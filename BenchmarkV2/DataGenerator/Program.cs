using System.Data;
using Bogus;
using Microsoft.Data.SqlClient;

namespace DataGenerator;

/// <summary>
/// Générateur de données custom basé sur le vrai schéma de production.
/// Remplace dbgen — produit des données cohérentes et relationnellement valides.
/// 
/// Usage: DataGenerator.exe <referentiel_conn> <production_conn> <centralized_conn> <scale>
///   scale: small | medium | large
/// </summary>
class Program
{
    // Volumes par scale factor
    static readonly Dictionary<string, (int TypePieces, int SNCount, int MesuresParSN, int Operateurs, int Postes)> Scales = new()
    {
        ["small"] = (10, 1_000, 20, 10, 5),
        ["medium"] = (25, 10_000, 25, 25, 10),
        ["large"] = (50, 100_000, 30, 50, 15)
    };

    static async Task Main(string[] args)
    {
        string refConn = args.Length > 0 ? args[0] : "Server=localhost;Database=Bench_Referentiel;Trusted_Connection=True;TrustServerCertificate=True;";
        string prodConn = args.Length > 1 ? args[1] : "Server=localhost;Database=Bench_Production;Trusted_Connection=True;TrustServerCertificate=True;";
        string centConn = args.Length > 2 ? args[2] : "Server=localhost;Database=Bench_Centralized;Trusted_Connection=True;TrustServerCertificate=True;";
        string scale = args.Length > 3 ? args[3] : "small";

        if (!Scales.ContainsKey(scale))
        {
            Console.WriteLine($"Scale invalide: {scale}. Valeurs acceptées: small, medium, large");
            return;
        }

        var (typePieces, snCount, mesuresParSN, operateurs, postes) = Scales[scale];

        Console.WriteLine($"╔══════════════════════════════════════════════════╗");
        Console.WriteLine($"║  Générateur de données Benchmark – Scale: {scale,-6} ║");
        Console.WriteLine($"╚══════════════════════════════════════════════════╝");
        Console.WriteLine($"  Types de pièces : {typePieces}");
        Console.WriteLine($"  Numéros de série: {snCount:N0}");
        Console.WriteLine($"  Mesures par SN  : {mesuresParSN}");
        Console.WriteLine($"  Total mesures   : {(long)snCount * mesuresParSN:N0}");
        Console.WriteLine();

        var gen = new DataSeeder(refConn, prodConn, centConn);

        Console.WriteLine("→ Nettoyage des données existantes...");
        await gen.CleanAllAsync();

        Console.WriteLine("→ Génération Base Référentiel (Base A)...");
        var (typeIds, lienIds, pnIds) = await gen.SeedReferentielAsync(typePieces, postes, operateurs);

        Console.WriteLine("→ Génération Base Production (Base B)...");
        var snIds = await gen.SeedProductionAsync(snCount, mesuresParSN, pnIds, lienIds, operateurs);

        Console.WriteLine("→ Réplication vers Base Centralisée (destination ETL)...");
        await gen.SeedCentralizedAsync();

        Console.WriteLine($"\n✓ Génération terminée — {(long)snCount * mesuresParSN:N0} mesures insérées.");
    }
}

public class DataSeeder
{
    private readonly string _refConn;
    private readonly string _prodConn;
    private readonly string _centConn;
    private readonly Faker _faker = new("fr");
    private readonly Random _rng = new(42); // seed fixe pour reproductibilité

    public DataSeeder(string refConn, string prodConn, string centConn)
    {
        _refConn = refConn;
        _prodConn = prodConn;
        _centConn = centConn;
    }

    /// <summary>
    /// Nettoie toutes les données des 3 bases avant une nouvelle génération.
    /// Ordre respectant les FK : tables enfants avant parents.
    /// </summary>
    public async Task CleanAllAsync()
    {
        // Bench_Production — TRUNCATE pour les tables sans FK entrantes
        await using (var conn = new SqlConnection(_prodConn))
        {
            await conn.OpenAsync();
            var sqls = new[]
            {
                "TRUNCATE TABLE Historique",
                "TRUNCATE TABLE Derogations",
                "TRUNCATE TABLE Mesures",
                "TRUNCATE TABLE Etiquettes",
                "TRUNCATE TABLE EtiquettesLog",
                "DELETE FROM Piece",
                "DELETE FROM Infos",
            };
            foreach (var sql in sqls)
            {
                await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 };
                await cmd.ExecuteNonQueryAsync();
            }
        }
        Console.WriteLine("  ✓ Bench_Production nettoyée");

        // Bench_Referentiel — ordre FK : Lien → Type_mesure → Type_Piece → reste
        await using (var conn = new SqlConnection(_refConn))
        {
            await conn.OpenAsync();
            var sqls = new[]
            {
                "DELETE FROM Lien_type_mesure_type_piece",
                "DELETE FROM Nomenclature",
                "DELETE FROM Priorites",
                "DELETE FROM Type_mesure",
                "DELETE FROM Type_Piece",
                "DELETE FROM Postes",
                "DELETE FROM Personnes",
            };
            foreach (var sql in sqls)
            {
                await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 };
                await cmd.ExecuteNonQueryAsync();
            }
        }
        Console.WriteLine("  ✓ Bench_Referentiel nettoyée");

        // Bench_Centralized
        await using (var conn = new SqlConnection(_centConn))
        {
            await conn.OpenAsync();
            var sqls = new[]
            {
                "TRUNCATE TABLE Historique",
                "TRUNCATE TABLE Derogations",
                "TRUNCATE TABLE Mesures",
                "DELETE FROM Piece",
                "DELETE FROM Lien_type_mesure_type_piece",
                "DELETE FROM Type_mesure",
                "DELETE FROM Type_Piece",
                "DELETE FROM Postes",
                "DELETE FROM Personnes",
            };
            foreach (var sql in sqls)
            {
                await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 };
                await cmd.ExecuteNonQueryAsync();
            }
        }
        Console.WriteLine("  ✓ Bench_Centralized nettoyée");
    }

    /// <summary>
    /// Génère les tables de référentiel (Base A) :
    /// Postes, Type_Piece, Type_mesure, Lien_type_mesure_type_piece, Personnes
    /// </summary>
    public async Task<(List<int> TypeMesureIds, List<int> LienIds, List<int> PNIds)> SeedReferentielAsync(
        int typePiecesCount, int postesCount, int operateursCount)
    {
        await using var conn = new SqlConnection(_refConn);
        await conn.OpenAsync();

        // Postes
        var posteIds = new List<int>();
        for (int i = 1; i <= postesCount; i++)
        {
            var id = await InsertScalarAsync(conn,
                "INSERT INTO Postes (Descript, Is_Active) OUTPUT INSERTED.ID_Poste VALUES (@d, 1)",
                ("@d", $"Poste_{i:D2}"));
            posteIds.Add(id);
        }
        Console.WriteLine($"  ✓ {postesCount} postes");

        // Type_Piece
        var pnIds = new List<int>();
        var brands = new[] { "ALPHA", "BETA", "GAMMA", "DELTA", "OMEGA" };
        for (int i = 1; i <= typePiecesCount; i++)
        {
            var id = await InsertScalarAsync(conn,
                @"INSERT INTO Type_Piece (type_piece_number, Type_Article, Modele_Article, Nom_Client, Is_Active)
                  OUTPUT INSERTED.ID
                  VALUES (@pn, @ta, @ma, @nc, 1)",
                ("@pn", $"PN-{i:D4}"),
                ("@ta", brands[i % brands.Length]),
                ("@ma", $"Model-{_rng.Next(100, 999)}"),
                ("@nc", $"Client_{_rng.Next(1, 20)}"));
            pnIds.Add(id);
        }
        Console.WriteLine($"  ✓ {typePiecesCount} types de pièces");

        // Type_mesure (catalogue des points de mesure)
        var typeMesureIds = new List<int>();
        var etapes = new[] { "ENTREE", "USINAGE", "CONTROLE", "SORTIE" };
        var unites = new[] { "mm", "µm", "°", "N", "kg", "bar" };
        int totalTypeMesures = typePiecesCount * 5;
        for (int i = 0; i < totalTypeMesures; i++)
        {
            double nominal = Math.Round(_rng.NextDouble() * 100, 3);
            double tolerance = Math.Round(_rng.NextDouble() * 0.5, 3);
            var id = await InsertScalarAsync(conn,
                @"INSERT INTO Type_mesure (Poste, Etape, Symbole, Nom_Donnee, Nominal,
                    Limite_Superieure, Limite_Inferieure, Precision_Scale, Is_Active)
                  OUTPUT INSERTED.ID_Type_Mesure
                  VALUES (@po, @et, @sy, @nd, @nom, @ls, @li, @ps, 1)",
                ("@po", $"P{posteIds[i % posteIds.Count]}"),
                ("@et", etapes[i % etapes.Length]),
                ("@sy", unites[i % unites.Length]),
                ("@nd", $"Mesure_{i:D4}"),
                ("@nom", nominal),
                ("@ls", nominal + tolerance),
                ("@li", nominal - tolerance),
                ("@ps", _rng.Next(2, 5)));
            typeMesureIds.Add(id);
        }
        Console.WriteLine($"  ✓ {totalTypeMesures} types de mesures");

        // Lien_type_mesure_type_piece
        var lienIds = new List<int>();
        int lienCounter = 0;
        for (int pnIdx = 0; pnIdx < pnIds.Count; pnIdx++)
        {
            int nbMesures = _rng.Next(10, 30);
            for (int m = 0; m < nbMesures && lienCounter < typeMesureIds.Count; m++, lienCounter++)
            {
                var id = await InsertScalarAsync(conn,
                    @"INSERT INTO Lien_type_mesure_type_piece
                        (ID_PN, ID_Type_Mesure, Numero_Etape, Ordre_Mesure, Warning_Level, Is_Active)
                      OUTPUT INSERTED.ID_Lien_Type_Mesure_Catalogue
                      VALUES (@pn, @tm, @ne, @om, @wl, 1)",
                    ("@pn", pnIds[pnIdx]),
                    ("@tm", typeMesureIds[lienCounter % typeMesureIds.Count]),
                    ("@ne", m / 5 + 1),
                    ("@om", m + 1),
                    ("@wl", Math.Round(_rng.NextDouble() * 0.1, 3)));
                lienIds.Add(id);
            }
        }
        Console.WriteLine($"  ✓ {lienIds.Count} liens type_mesure ↔ type_piece");

        // Personnes (opérateurs)
        for (int i = 1; i <= operateursCount; i++)
        {
            await ExecuteAsync(conn,
                @"INSERT INTO Personnes (Nom, Prenom, Code, Is_Active)
                  VALUES (@n, @p, @c, 1)",
                ("@n", _faker.Name.LastName()),
                ("@p", _faker.Name.FirstName()),
                ("@c", $"OP{i:D3}"));
        }
        Console.WriteLine($"  ✓ {operateursCount} opérateurs");

        return (typeMesureIds, lienIds, pnIds);
    }

    /// <summary>
    /// Génère les données de production (Base B).
    /// </summary>
    public async Task<List<int>> SeedProductionAsync(
        int snCount, int mesuresParSN,
        List<int> pnIds, List<int> lienIds, int operateursCount)
    {
        await using var conn = new SqlConnection(_prodConn);
        await conn.OpenAsync();

        // Piece – numéros de série
        var snTable = new DataTable();
        snTable.Columns.Add("SN", typeof(string));
        snTable.Columns.Add("ID_PN", typeof(int));
        snTable.Columns.Add("Is_Active", typeof(bool));

        var snIds = new List<int>();
        for (int i = 0; i < snCount; i++)
            snTable.Rows.Add($"SN-{i:D8}", pnIds[i % pnIds.Count], true);

        await BulkInsertWithMappingAsync(_prodConn, snTable, "Piece",
            ("SN", "SN"), ("ID_PN", "ID_PN"), ("Is_Active", "Is_Active"));

        Console.WriteLine($"  ✓ {snCount:N0} pièces insérées");

        await using (var cmd = new SqlCommand("SELECT ID_SN FROM Piece ORDER BY ID_SN", conn))
        await using (var reader = await cmd.ExecuteReaderAsync())
            while (await reader.ReadAsync())
                snIds.Add(reader.GetInt32(0));

        if (!snIds.Any())
            throw new InvalidOperationException("Aucune Piece trouvée après insertion.");

        // Mesures
        var mesureTable = new DataTable();
        mesureTable.Columns.Add("Valeur", typeof(double));
        mesureTable.Columns.Add("ID_SN", typeof(int));
        mesureTable.Columns.Add("Commentaire", typeof(string));
        mesureTable.Columns.Add("ID_Lien_Type_Mesure_Catalogue", typeof(int));
        mesureTable.Columns.Add("Insert_Date", typeof(DateTime));
        mesureTable.Columns.Add("ID_Operateur", typeof(int));
        mesureTable.Columns.Add("Is_Active", typeof(bool));

        long totalMesures = (long)snCount * mesuresParSN;
        long batchSize = 50_000;
        long inserted = 0;

        for (int snIdx = 0; snIdx < snCount; snIdx++)
        {
            int snId = snIds[snIdx % snIds.Count];
            for (int m = 0; m < mesuresParSN; m++)
            {
                double valeur = Math.Round(_rng.NextDouble() * 100, 3);
                mesureTable.Rows.Add(
                    valeur,
                    snId,
                    _rng.Next(10) == 0 ? "OK avec réserve" : (object)DBNull.Value,
                    lienIds[_rng.Next(lienIds.Count)],
                    DateTime.Now.AddDays(-_rng.Next(365)).AddSeconds(-_rng.Next(86400)),
                    _rng.Next(1, operateursCount + 1),
                    true);
            }

            if (mesureTable.Rows.Count >= batchSize || snIdx == snCount - 1)
            {
                await BulkInsertWithMappingAsync(_prodConn, mesureTable, "Mesures",
                    ("Valeur", "Valeur"),
                    ("ID_SN", "ID_SN"),
                    ("Commentaire", "Commentaire"),
                    ("ID_Lien_Type_Mesure_Catalogue", "ID_Lien_Type_Mesure_Catalogue"),
                    ("Insert_Date", "Insert_Date"),
                    ("ID_Operateur", "ID_Operateur"),
                    ("Is_Active", "Is_Active"));
                inserted += mesureTable.Rows.Count;
                mesureTable.Rows.Clear();
                Console.Write($"\r  ✓ {inserted:N0} / {totalMesures:N0} mesures insérées...");
            }
        }
        Console.WriteLine();

        // Derogations – ~5% des mesures
        Console.WriteLine("  → Génération des dérogations (~5% des mesures)...");
        await ExecuteAsync(conn,
            @"INSERT INTO Derogations (ID_Mesure, Commentaire, Accepted, Is_Active)
              SELECT TOP (@n) ID_Mesure, 'Dérogation générée benchmark',
                     CASE WHEN ABS(CHECKSUM(NEWID())) % 2 = 0 THEN 1 ELSE 0 END, 1
              FROM Mesures
              ORDER BY NEWID()",
            ("@n", (int)(snCount * mesuresParSN * 0.05)));
        Console.WriteLine("  ✓ Dérogations insérées");

        // Historique – ~10% des mesures
        Console.WriteLine("  → Génération de l'historique (~10% des mesures)...");
        await ExecuteAsync(conn,
            @"INSERT INTO Historique (ID_Source, ID_Destination, Nom_Table, ID_Operateur, Date_Insertion, Statut)
              SELECT TOP (@n)
                  m1.ID_Mesure, m2.ID_Mesure, 'Mesures',
                  @op, GETDATE(), 'M'
              FROM Mesures m1
              CROSS JOIN (SELECT TOP 1 ID_Mesure FROM Mesures ORDER BY NEWID()) m2
              ORDER BY NEWID()",
            ("@n", (int)(snCount * mesuresParSN * 0.10)),
            ("@op", _rng.Next(1, operateursCount + 1)));
        Console.WriteLine("  ✓ Historique inséré");

        return snIds;
    }

    /// <summary>
    /// Réplique toutes les données vers la base centralisée (destination ETL initiale).
    /// </summary>
    public async Task SeedCentralizedAsync()
    {
        await using var conn = new SqlConnection(_centConn);
        await conn.OpenAsync();

        var copies = new[]
        {
            ("Bench_Referentiel", "Type_Piece",
             "SELECT type_piece_number, Type_Article, Modele_Article, Nom_Client, Is_Active FROM Bench_Referentiel.dbo.Type_Piece"),

            ("Bench_Referentiel", "Type_mesure",
             "SELECT Poste, Etape, Symbole, Nom_Donnee, Nominal, Limite_Superieure, Limite_Inferieure, Precision_Scale, Is_Active FROM Bench_Referentiel.dbo.Type_mesure"),

            ("Bench_Referentiel", "Lien_type_mesure_type_piece",
             "SELECT ID_PN, ID_Type_Mesure, Numero_Etape, Ordre_Mesure, Warning_Level, Is_Active FROM Bench_Referentiel.dbo.Lien_type_mesure_type_piece"),

            ("Bench_Referentiel", "Postes",
             "SELECT Descript, Is_Active FROM Bench_Referentiel.dbo.Postes"),

            ("Bench_Referentiel", "Personnes",
             "SELECT Nom, Prenom, Code, Is_Active FROM Bench_Referentiel.dbo.Personnes"),

            ("Bench_Production", "Piece",
             "SELECT SN, ID_PN, Is_Active FROM Bench_Production.dbo.Piece"),

            ("Bench_Production", "Mesures",
             "SELECT Valeur, ID_SN, Commentaire, ID_Lien_Type_Mesure_Catalogue, Insert_Date, ID_Operateur, Is_Active FROM Bench_Production.dbo.Mesures"),

            ("Bench_Production", "Derogations",
             "SELECT ID_Mesure, Commentaire, Accepted, Is_Active FROM Bench_Production.dbo.Derogations"),

            ("Bench_Production", "Historique",
             "SELECT ID_Source, ID_Destination, Nom_Table, ID_Operateur, Date_Insertion, Statut FROM Bench_Production.dbo.Historique"),
        };

        foreach (var (_, dstTable, selectSql) in copies)
        {
            var insertSql = $"INSERT INTO {dstTable} {selectSql}";
            await using var cmd = new SqlCommand(insertSql, conn) { CommandTimeout = 600 };
            var rows = await cmd.ExecuteNonQueryAsync();
            Console.WriteLine($"  ✓ {dstTable} → Centralisée ({rows:N0} lignes)");
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static async Task<int> InsertScalarAsync(
        SqlConnection conn, string sql, params (string Name, object Value)[] parameters)
    {
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 60 };
        foreach (var (name, value) in parameters)
            cmd.Parameters.AddWithValue(name, value);
        var result = await cmd.ExecuteScalarAsync();
        return result is int i ? i : Convert.ToInt32(result);
    }

    private static async Task ExecuteAsync(
        SqlConnection conn, string sql, params (string Name, object Value)[] parameters)
    {
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 300 };
        foreach (var (name, value) in parameters)
            cmd.Parameters.AddWithValue(name, value);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task BulkInsertWithMappingAsync(
        string connStr, DataTable table, string destinationTable,
        params (string Source, string Dest)[] mappings)
    {
        using var bulk = new SqlBulkCopy(connStr, SqlBulkCopyOptions.Default)
        {
            DestinationTableName = destinationTable,
            BatchSize = 10_000,
            BulkCopyTimeout = 600
        };
        foreach (var (src, dst) in mappings)
            bulk.ColumnMappings.Add(src, dst);
        await bulk.WriteToServerAsync(table);
    }

    private static async Task BulkInsertAsync(
        string connStr, DataTable table, string destinationTable)
    {
        using var bulk = new SqlBulkCopy(connStr, SqlBulkCopyOptions.Default)
        {
            DestinationTableName = destinationTable,
            BatchSize = 10_000,
            BulkCopyTimeout = 600
        };
        await bulk.WriteToServerAsync(table);
    }
}