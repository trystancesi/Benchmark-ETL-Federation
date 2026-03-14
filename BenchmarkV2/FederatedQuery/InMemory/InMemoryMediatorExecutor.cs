using Microsoft.Data.SqlClient;

namespace FederatedQuery.InMemory;

// DTOs
public record SNRow(int IdSN, string SN, int IdPN);
public record LienRow(int IdLien, int IdPN, int IdTypeMesure, int NumeroEtape, int OrdreMesure, double WarningLevel);
public record TypeMesureRow(int IdTypeMesure, string Poste, string Etape, string Symbole,
    string NomDonnee, double? Nominal, double? LimSup, double? LimInf, int PrecisionScale);
public record MesureRow(int IdMesure, int IdSN, int IdLien, double? Valeur,
    string? Commentaire, DateTime InsertDate, int? IdOperateur);
public record DerogationRow(int IdDerogation, int IdMesure, int Accepted);
public record HistoriqueRow(int IdSource, int IdDestination, string NomTable,
    int? IdOperateur, DateTime DateInsertion, string Statut);

/// <summary>
/// Option B – Médiation C# en mémoire.
/// Fetch parallèle depuis Base A (Référentiel) et Base B (Production),
/// jointures et agrégations réalisées via LINQ.
/// Modélise le coût d'une jointure applicative vs jointure SQL Server.
/// </summary>
public class InMemoryMediatorExecutor
{
    private readonly string _referentielConn;
    private readonly string _productionConn;

    public InMemoryMediatorExecutor(string referentielConn, string productionConn)
    {
        _referentielConn = referentielConn;
        _productionConn = productionConn;
    }

    /// <summary>T1 – Liste SN : uniquement Base Production, pas de médiation.</summary>
    public async Task<long> ExecuteT1_GetSNListAsync(bool activeOnly = true)
    {
        var sql = $"SELECT ID_SN, SN, ID_PN FROM Piece {(activeOnly ? "WHERE Is_Active=1" : "")} ORDER BY ID_SN DESC";
        var rows = await FetchAsync(_productionConn, sql, r =>
            new SNRow(r.GetInt32(0), r.GetString(1), r.GetInt32(2)));
        return rows.Count;
    }

    /// <summary>
    /// T2 – Mesures par pièce : fetch parallèle Base A + Base B, jointure LINQ.
    /// </summary>
    public async Task<long> ExecuteT2_GetMesuresByPNAsync(string sn)
    {
        // Fetch Piece depuis Base B
        var snRows = await FetchAsync(_productionConn,
            "SELECT ID_SN, SN, ID_PN FROM Piece WHERE SN=@sn AND Is_Active=1",
            r => new SNRow(r.GetInt32(0), r.GetString(1), r.GetInt32(2)),
            ("@sn", sn));

        if (!snRows.Any()) return 0;
        var snRow = snRows.First();

        // Fetch parallèle : Référentiel (Base A) + Mesures/Dérogations (Base B)
        var liensTask = FetchAsync(_referentielConn,
            "SELECT ID_Lien_Type_Mesure_Catalogue, ID_PN, ID_Type_Mesure, Numero_Etape, Ordre_Mesure, Warning_Level FROM Lien_type_mesure_type_piece WHERE ID_PN=@pn AND Is_Active=1",
            r => new LienRow(r.GetInt32(0), r.GetInt32(1), r.GetInt32(2),
                             r.IsDBNull(3) ? 0 : r.GetInt32(3),
                             r.IsDBNull(4) ? 0 : r.GetInt32(4),
                             r.IsDBNull(5) ? 0 : (double)r.GetDecimal(5)),
            ("@pn", snRow.IdPN));

        var typesTask = FetchAsync(_referentielConn,
            "SELECT ID_Type_Mesure, Poste, Etape, Symbole, Nom_Donnee, Nominal, Limite_Superieure, Limite_Inferieure, Precision_Scale FROM Type_mesure WHERE Is_Active=1",
            r => new TypeMesureRow(r.GetInt32(0),
                r.IsDBNull(1) ? "" : r.GetString(1),
                r.IsDBNull(2) ? "" : r.GetString(2),
                r.IsDBNull(3) ? "" : r.GetString(3),
                r.IsDBNull(4) ? "" : r.GetString(4),
                r.IsDBNull(5) ? null : (double?)r.GetDecimal(5),
                r.IsDBNull(6) ? null : (double?)r.GetDecimal(6),
                r.IsDBNull(7) ? null : (double?)r.GetDecimal(7),
                r.IsDBNull(8) ? 3 : r.GetInt32(8)));

        var mesuresTask = FetchAsync(_productionConn,
            "SELECT ID_Mesure, ID_SN, ID_Lien_Type_Mesure_Catalogue, Valeur, Commentaire, Insert_Date, ID_Operateur FROM Mesures WHERE ID_SN=@sn AND Is_Active=1",
            r => new MesureRow(r.GetInt32(0), r.GetInt32(1), r.GetInt32(2),
                r.IsDBNull(3) ? null : (double?)r.GetDecimal(3),
                r.IsDBNull(4) ? null : r.GetString(4),
                r.GetDateTime(5),
                r.IsDBNull(6) ? null : r.GetInt32(6)),
            ("@sn", snRow.IdSN));

        var derogsTask = FetchAsync(_productionConn,
            "SELECT ID_Derogation, ID_Mesure, Accepted FROM Derogations WHERE Is_Active=1",
            r => new DerogationRow(r.GetInt32(0), r.GetInt32(1), r.GetInt32(2)));

        await Task.WhenAll(liensTask, typesTask, mesuresTask, derogsTask);

        // GroupBy pour éviter les doublons (mesures insérées par T4 lors des runs précédents)
        var liens = (await liensTask).ToDictionary(l => l.IdLien);
        var types = (await typesTask)
                        .GroupBy(t => t.IdTypeMesure)
                        .ToDictionary(g => g.Key, g => g.First());
        var mesures = (await mesuresTask)
                        .GroupBy(m => m.IdMesure)
                        .ToDictionary(g => g.Key, g => g.First());
        var derogs = (await derogsTask)
                        .GroupBy(d => d.IdMesure)
                        .ToDictionary(g => g.Key, g => g.First());

        // Jointure LINQ
        var result = (await liensTask)
            .Where(l => types.ContainsKey(l.IdTypeMesure))
            .Select(l =>
            {
                var tm = types[l.IdTypeMesure];
                var m = mesures.Values.FirstOrDefault(x => x.IdLien == l.IdLien);
                var d = m != null && derogs.ContainsKey(m.IdMesure) ? derogs[m.IdMesure] : null;
                return new
                {
                    tm.Etape,
                    tm.Symbole,
                    Derog = d?.Accepted ?? -1,
                    Result = m?.Valeur,
                    m?.Commentaire,
                    m?.InsertDate,
                    m?.IdOperateur
                };
            })
            .OrderBy(x => liens.Values.FirstOrDefault()?.NumeroEtape)
            .ToList();

        return result.Count;
    }

    /// <summary>
    /// T3 – Historique complet : fetch 4 sources en parallèle + reconstruction en mémoire.
    /// </summary>
    public async Task<long> ExecuteT3_GetMesuresHistoriqueAsync(string sn)
    {
        var snRows = await FetchAsync(_productionConn,
            "SELECT ID_SN, SN, ID_PN FROM Piece WHERE SN=@sn AND Is_Active=1",
            r => new SNRow(r.GetInt32(0), r.GetString(1), r.GetInt32(2)),
            ("@sn", sn));

        if (!snRows.Any()) return 0;
        var snRow = snRows.First();

        // Fetch parallèle des 4 sources
        var liensTask = FetchAsync(_referentielConn,
            "SELECT ID_Lien_Type_Mesure_Catalogue, ID_PN, ID_Type_Mesure, Numero_Etape, Ordre_Mesure, Warning_Level FROM Lien_type_mesure_type_piece WHERE ID_PN=@pn AND Is_Active=1",
            r => new LienRow(r.GetInt32(0), r.GetInt32(1), r.GetInt32(2),
                r.IsDBNull(3) ? 0 : r.GetInt32(3), r.IsDBNull(4) ? 0 : r.GetInt32(4),
                r.IsDBNull(5) ? 0 : (double)r.GetDecimal(5)),
            ("@pn", snRow.IdPN));

        var typesTask = FetchAsync(_referentielConn,
            "SELECT ID_Type_Mesure, Poste, Etape, Symbole, Nom_Donnee, Nominal, Limite_Superieure, Limite_Inferieure, Precision_Scale FROM Type_mesure WHERE Is_Active=1",
            r => new TypeMesureRow(r.GetInt32(0),
                r.IsDBNull(1) ? "" : r.GetString(1), r.IsDBNull(2) ? "" : r.GetString(2),
                r.IsDBNull(3) ? "" : r.GetString(3), r.IsDBNull(4) ? "" : r.GetString(4),
                r.IsDBNull(5) ? null : (double?)r.GetDecimal(5),
                r.IsDBNull(6) ? null : (double?)r.GetDecimal(6),
                r.IsDBNull(7) ? null : (double?)r.GetDecimal(7),
                r.IsDBNull(8) ? 3 : r.GetInt32(8)));

        var mesuresTask = FetchAsync(_productionConn,
            "SELECT ID_Mesure, ID_SN, ID_Lien_Type_Mesure_Catalogue, Valeur, Commentaire, Insert_Date, ID_Operateur FROM Mesures WHERE ID_SN=@sn AND Is_Active=1",
            r => new MesureRow(r.GetInt32(0), r.GetInt32(1), r.GetInt32(2),
                r.IsDBNull(3) ? null : (double?)r.GetDecimal(3),
                r.IsDBNull(4) ? null : r.GetString(4),
                r.GetDateTime(5), r.IsDBNull(6) ? null : r.GetInt32(6)),
            ("@sn", snRow.IdSN));

        var histTask = FetchAsync(_productionConn,
            "SELECT ID_Source, ID_Destination, Nom_Table, ID_Operateur, Date_Insertion, Statut FROM Historique WHERE Nom_Table='Mesures' AND Statut='M'",
            r => new HistoriqueRow(r.GetInt32(0), r.GetInt32(1), r.GetString(2),
                r.IsDBNull(3) ? null : r.GetInt32(3), r.GetDateTime(4), r.GetString(5)));

        var derogsTask = FetchAsync(_productionConn,
            "SELECT ID_Derogation, ID_Mesure, Accepted FROM Derogations WHERE Is_Active=1",
            r => new DerogationRow(r.GetInt32(0), r.GetInt32(1), r.GetInt32(2)));

        await Task.WhenAll(liensTask, typesTask, mesuresTask, histTask, derogsTask);

        var mesures = await mesuresTask;
        var hist = (await histTask).GroupBy(h => h.IdDestination)
                                      .ToDictionary(g => g.Key, g => g.ToList());

        // Reconstitution de la chaîne historique en mémoire
        var chain = mesures.SelectMany(m =>
        {
            var current = new[] { m };
            var historical = hist.ContainsKey(m.IdMesure)
                ? hist[m.IdMesure].Select(h => mesures.FirstOrDefault(prev => prev.IdMesure == h.IdSource))
                                  .Where(prev => prev != null)
                                  .Select(prev => prev!)
                : Array.Empty<MesureRow>();
            return current.Concat(historical);
        }).ToList();

        return chain.Count;
    }

    /// <summary>
    /// T4 – Insertion mesure : directe sur Base Production.
    /// </summary>
    public async Task<long> ExecuteT4_InsertMesureAsync(
        decimal valeur, int idSN, int idLien, int idOperateur, string? commentaire = null)
    {
        const string sql = @"
            INSERT INTO Mesures (Valeur, ID_SN, Commentaire, ID_Lien_Type_Mesure_Catalogue, Insert_Date, ID_Operateur, Is_Active)
            OUTPUT INSERTED.ID_Mesure
            VALUES (@v, @sn, @c, @l, @d, @op, 1);";

        await using var conn = new SqlConnection(_productionConn);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 30 };
        cmd.Parameters.AddWithValue("@v", valeur);
        cmd.Parameters.AddWithValue("@sn", idSN);
        cmd.Parameters.AddWithValue("@c", (object?)commentaire ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@l", idLien);
        cmd.Parameters.AddWithValue("@d", DateTime.Now);
        cmd.Parameters.AddWithValue("@op", idOperateur);

        var result = await cmd.ExecuteScalarAsync();
        return result is int i ? i : Convert.ToInt64(result);
    }

    // ── Helper fetch générique ────────────────────────────────────────────────

    private static async Task<List<T>> FetchAsync<T>(
        string connStr, string sql,
        Func<SqlDataReader, T> map,
        params (string Name, object Value)[] parameters)
    {
        var result = new List<T>();
        await using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 300 };
        foreach (var (name, value) in parameters)
            cmd.Parameters.AddWithValue(name, value);
        await using var reader = await cmd.ExecuteReaderAsync(
            System.Data.CommandBehavior.SequentialAccess);
        while (await reader.ReadAsync())
            result.Add(map(reader));
        return result;
    }
}