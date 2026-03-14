using Microsoft.Data.SqlClient;

namespace FederatedQuery.LinkedServer;

/// <summary>
/// Option A – Linked Server.
/// L'application se connecte à Base Production (Base B).
/// Les tables du Référentiel (Base A) sont accédées via le Linked Server 'BaseReferentiel'.
/// SQL Server planifie et exécute la jointure distribuée en interne.
/// </summary>
public class LinkedServerQueryExecutor
{
    private readonly string _productionConnectionString;
    private readonly string _linkedServerName;

    public LinkedServerQueryExecutor(string productionConn, string linkedServerName)
    {
        _productionConnectionString = productionConn;
        _linkedServerName = linkedServerName;
    }

    /// <summary>
    /// T1 – Liste des SN depuis Base Production uniquement.
    /// </summary>
    public async Task<long> ExecuteT1_GetSNListAsync(bool activeOnly = true)
    {
        var sql = $@"
            SELECT p.SN, p.ID_SN, p.ID_PN
            FROM Piece p
            {(activeOnly ? "WHERE p.Is_Active = 1" : "")}
            ORDER BY p.ID_SN DESC;";

        return await CountRowsAsync(sql);
    }

    /// <summary>
    /// T2 – Mesures par pièce : jointure distribuée Production ↔ Référentiel.
    /// Piece + Mesures + Dérogations (Base B locale)
    /// ↔ Lien_type_mesure_type_piece + Type_mesure (Base A via Linked Server)
    /// </summary>
    public async Task<long> ExecuteT2_GetMesuresByPNAsync(string sn)
    {
        var sql = $@"
            SELECT
                CTM.Etape,
                CTM.Symbole,
                CASE
                    WHEN Derogations.ID_Derogation IS NULL THEN -1
                    ELSE Derogations.Accepted
                END AS Derog,
                Mesures.Valeur AS Result,
                Mesures.Commentaire,
                Mesures.Insert_Date,
                Mesures.ID_Operateur
            FROM Piece p
            JOIN [{_linkedServerName}].[Bench_Referentiel].dbo.Lien_type_mesure_type_piece LTMC
                ON LTMC.ID_PN = p.ID_PN
                AND LTMC.Is_Active = 1
            JOIN [{_linkedServerName}].[Bench_Referentiel].dbo.Type_mesure CTM
                ON CTM.ID_Type_Mesure = LTMC.ID_Type_Mesure
                AND CTM.Is_Active = 1
            LEFT JOIN Mesures
                ON Mesures.ID_SN = p.ID_SN
                AND Mesures.ID_Lien_Type_Mesure_Catalogue = LTMC.ID_Lien_Type_Mesure_Catalogue
                AND Mesures.Is_Active = 1
            LEFT JOIN Derogations
                ON Derogations.ID_Mesure = Mesures.ID_Mesure
                AND Derogations.Is_Active = 1
            WHERE p.SN = @sn
              AND p.Is_Active = 1
            ORDER BY LTMC.Numero_Etape, LTMC.Ordre_Mesure;";

        return await CountRowsAsync(sql, ("@sn", sn));
    }

    /// <summary>
    /// T3 – Historique complet avec CTE + fenêtrage sur données distribuées.
    /// </summary>
    public async Task<long> ExecuteT3_GetMesuresHistoriqueAsync(string sn)
    {
        var sql = $@"
            WITH SN_CTE AS (
                SELECT TOP (1) ID_SN, ID_PN
                FROM Piece
                WHERE SN = @sn AND Is_Active = 1
            ),
            MesuresActuelles AS (
                SELECT
                    M.ID_Mesure, M.ID_SN, M.ID_Lien_Type_Mesure_Catalogue,
                    M.Valeur, M.Commentaire, M.Insert_Date,
                    M.ID_Operateur, M.Is_Active, M.ID_Mesure AS CurrentMesureID
                FROM Mesures AS M
                JOIN SN_CTE S ON M.ID_SN = S.ID_SN
                WHERE M.Is_Active = 1
            ),
            MesuresHist AS (
                SELECT
                    MPrev.ID_Mesure, MPrev.ID_SN, MPrev.ID_Lien_Type_Mesure_Catalogue,
                    MPrev.Valeur, MPrev.Commentaire, MPrev.Insert_Date,
                    MPrev.ID_Operateur, MPrev.Is_Active, MA.ID_Mesure AS CurrentMesureID
                FROM Historique H
                INNER JOIN Mesures MPrev ON MPrev.ID_Mesure = H.ID_Source
                INNER JOIN MesuresActuelles MA ON H.ID_Destination = MA.ID_Mesure
                WHERE H.Nom_Table = 'Mesures' AND H.Statut = 'M'
            ),
            MesuresChain AS (
                SELECT * FROM MesuresActuelles
                UNION ALL
                SELECT * FROM MesuresHist
            ),
            Ordered AS (
                SELECT MC.*,
                    ROW_NUMBER() OVER (PARTITION BY MC.CurrentMesureID ORDER BY MC.Insert_Date) AS rn,
                    LAG(MC.ID_Mesure) OVER (PARTITION BY MC.CurrentMesureID ORDER BY MC.Insert_Date) AS Prev_ID
                FROM MesuresChain MC
            )
            SELECT
                LTMC.ID_Lien_Type_Mesure_Catalogue,
                LTMC.Numero_Etape,
                LTMC.Ordre_Mesure,
                CTM.Poste, CTM.Etape, CTM.Nom_Donnee, CTM.Symbole,
                COALESCE(CAST(CTM.Nominal           AS VARCHAR(100)), '') AS Nominal,
                COALESCE(CAST(CTM.Limite_Superieure AS VARCHAR(100)), '') AS Limite_Superieure,
                COALESCE(CAST(CTM.Limite_Inferieure AS VARCHAR(100)), '') AS Limite_Inferieure,
                CTM.Precision_Scale,
                LTMC.Warning_Level,
                CASE WHEN d.ID_Derogation IS NULL THEN -1 ELSE d.Accepted END AS Derog,
                O.Valeur AS Result,
                O.Commentaire,
                O.Insert_Date,
                CASE WHEN O.Prev_ID IS NULL THEN O.ID_Operateur ELSE Hprev.ID_Operateur END AS ID_Operateur,
                O.Is_Active
            FROM SN_CTE S
            INNER JOIN [{_linkedServerName}].[Bench_Referentiel].dbo.Lien_type_mesure_type_piece LTMC
                ON LTMC.ID_PN = S.ID_PN AND LTMC.Is_Active = 1
            INNER JOIN [{_linkedServerName}].[Bench_Referentiel].dbo.Type_mesure CTM
                ON CTM.ID_Type_Mesure = LTMC.ID_Type_Mesure AND CTM.Is_Active = 1
            LEFT JOIN Ordered O
                ON O.ID_Lien_Type_Mesure_Catalogue = LTMC.ID_Lien_Type_Mesure_Catalogue
            LEFT JOIN Historique Hprev
                ON Hprev.ID_Source      = O.Prev_ID
               AND Hprev.ID_Destination = O.CurrentMesureID
               AND Hprev.Nom_Table      = 'Mesures'
               AND Hprev.Statut         = 'M'
            LEFT JOIN Derogations d
                ON d.ID_Mesure = O.ID_Mesure AND d.Is_Active = 1
            ORDER BY LTMC.Numero_Etape, LTMC.Ordre_Mesure,
                CASE WHEN O.Prev_ID IS NULL THEN O.Insert_Date ELSE Hprev.Date_Insertion END;";

        return await CountRowsAsync(sql, ("@sn", sn));
    }

    /// <summary>
    /// T4 – Insertion mesure : locale sur Base Production, pas de Linked Server.
    /// </summary>
    public async Task<long> ExecuteT4_InsertMesureAsync(
        decimal valeur, int idSN, int idLien, int idOperateur, string? commentaire = null)
    {
        const string sql = @"
            INSERT INTO Mesures
                (Valeur, ID_SN, Commentaire, ID_Lien_Type_Mesure_Catalogue,
                 Insert_Date, ID_Operateur, Is_Active)
            OUTPUT INSERTED.ID_Mesure
            VALUES (@Valeur, @ID_SN, @Commentaire, @ID_Lien_Type_Mesure_Catalogue,
                    @Insert_Date, @ID_Operateur, 1);";

        await using var conn = new SqlConnection(_productionConnectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 30 };
        cmd.Parameters.AddWithValue("@Valeur", valeur);
        cmd.Parameters.AddWithValue("@ID_SN", idSN);
        cmd.Parameters.AddWithValue("@Commentaire", (object?)commentaire ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ID_Lien_Type_Mesure_Catalogue", idLien);
        cmd.Parameters.AddWithValue("@Insert_Date", DateTime.Now);
        cmd.Parameters.AddWithValue("@ID_Operateur", idOperateur);

        var result = await cmd.ExecuteScalarAsync();
        return result is int i ? i : Convert.ToInt64(result);
    }

    private async Task<long> CountRowsAsync(string sql,
        params (string Name, object Value)[] parameters)
    {
        await using var conn = new SqlConnection(_productionConnectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 300 };
        foreach (var (name, value) in parameters)
            cmd.Parameters.AddWithValue(name, value);

        long count = 0;
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) count++;
        return count;
    }
}