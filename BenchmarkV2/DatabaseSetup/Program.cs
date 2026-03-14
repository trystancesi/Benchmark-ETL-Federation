using Microsoft.Data.SqlClient;

namespace DatabaseSetup;

/// <summary>
/// Crée et initialise les 3 bases de données du benchmark :
///   - Bench_Referentiel  : Base A – Type_Piece, Type_mesure, Lien_type_mesure_type_piece, Postes, Personnes
///   - Bench_Production   : Base B – Piece, Infos, Mesures, Historique, Derogations, Etiquettes
///   - Bench_Centralized  : Destination ETL – toutes les tables réunies
///
/// Usage : DatabaseSetup.exe [server] [--linked-server <server_secondaire>]
/// </summary>
class Program
{
    static async Task Main(string[] args)
    {
        string server = args.Length > 0 ? args[0] : "localhost";
        string? linkedServerHost = null;
        for (int i = 0; i < args.Length - 1; i++)
            if (args[i] == "--linked-server") linkedServerHost = args[i + 1];

        string masterConn = $"Server={server};Database=master;Trusted_Connection=True;TrustServerCertificate=True;";

        Console.WriteLine("╔══════════════════════════════════════════════════════╗");
        Console.WriteLine("║  DatabaseSetup – Benchmark ETL vs Fédéré            ║");
        Console.WriteLine($"║  Serveur : {server,-42}║");
        Console.WriteLine("╚══════════════════════════════════════════════════════╝");

        await CreateDatabasesAsync(masterConn);
        await CreateReferentielSchemaAsync(server);
        await CreateProductionSchemaAsync(server);
        await CreateCentralizedSchemaAsync(server);

        if (linkedServerHost != null)
            await ConfigureLinkedServerAsync(server, linkedServerHost);

        Console.WriteLine("\n✓ Configuration terminée.");
        Console.WriteLine("\nÉtapes suivantes :");
        Console.WriteLine("  1. DataGenerator.exe small|medium|large");
        Console.WriteLine("  2. BenchmarkRunner.exe");
    }

    static async Task CreateDatabasesAsync(string masterConn)
    {
        Console.WriteLine("\n→ Création des bases de données...");
        string[] dbs = { "Bench_Referentiel", "Bench_Production", "Bench_Centralized" };

        await using var conn = new SqlConnection(masterConn);
        await conn.OpenAsync();

        foreach (var db in dbs)
        {
            await using var cmd = new SqlCommand(
                $"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name='{db}') " +
                $"CREATE DATABASE [{db}];", conn);
            await cmd.ExecuteNonQueryAsync();
            Console.WriteLine($"  ✓ {db}");
        }
    }

    static async Task CreateReferentielSchemaAsync(string server)
    {
        Console.WriteLine("\n→ Schéma Bench_Referentiel (Base A)...");
        string conn = $"Server={server};Database=Bench_Referentiel;Trusted_Connection=True;TrustServerCertificate=True;";

        const string sql = @"
            -- Postes
            IF OBJECT_ID('Postes','U') IS NULL
            CREATE TABLE Postes (
                ID_Poste    INT IDENTITY(1,1) PRIMARY KEY,
                Descript    NVARCHAR(100)     NOT NULL,
                Is_Active   BIT               NOT NULL DEFAULT 1
            );

            -- Type_Piece
            IF OBJECT_ID('Type_Piece','U') IS NULL
            CREATE TABLE Type_Piece (
                ID                      INT IDENTITY(1,1) PRIMARY KEY,
                type_piece_number       NVARCHAR(50)  NOT NULL,
                Type_Article            NVARCHAR(50),
                Modele_Article          NVARCHAR(100),
                Mode_Article            NVARCHAR(50),
                Nom_Client              NVARCHAR(100),
                Fonction_AV             NVARCHAR(100),
                Fonction_AR             NVARCHAR(100),
                type_de_type_piece      NVARCHAR(50),
                Numero_Selle            NVARCHAR(50),
                Bouchage_Contact        NVARCHAR(50),
                Bouchage_Via            NVARCHAR(50),
                Via_Nbr                 INT,
                Contact_Nbr             INT,
                Is_Active               BIT NOT NULL DEFAULT 1
            );
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_TypePiece_Number')
                CREATE INDEX IX_TypePiece_Number ON Type_Piece(type_piece_number);

            -- Type_mesure
            IF OBJECT_ID('Type_mesure','U') IS NULL
            CREATE TABLE Type_mesure (
                ID_Type_Mesure      INT IDENTITY(1,1) PRIMARY KEY,
                Poste               NVARCHAR(50),
                Etape               NVARCHAR(100),
                Symbole             NVARCHAR(20),
                Nom_Donnee          NVARCHAR(200),
                Nominal             DECIMAL(18,4),
                Limite_Superieure   DECIMAL(18,4),
                Limite_Inferieure   DECIMAL(18,4),
                Precision_Scale     INT DEFAULT 3,
                Is_Active           BIT NOT NULL DEFAULT 1
            );

            -- Lien_type_mesure_type_piece
            IF OBJECT_ID('Lien_type_mesure_type_piece','U') IS NULL
            CREATE TABLE Lien_type_mesure_type_piece (
                ID_Lien_Type_Mesure_Catalogue   INT IDENTITY(1,1) PRIMARY KEY,
                ID_PN                           INT NOT NULL,
                ID_Type_Mesure                  INT NOT NULL,
                Numero_Etape                    INT,
                Ordre_Mesure                    INT,
                Certificat_Number               NVARCHAR(50),
                Warning_Level                   DECIMAL(18,4),
                Ordre_Mesure_Display            INT,
                Alias                           NVARCHAR(100),
                Expression                      NVARCHAR(500),
                Is_Active                       BIT NOT NULL DEFAULT 1,
                CONSTRAINT FK_Lien_PN FOREIGN KEY (ID_PN)          REFERENCES Type_Piece(ID),
                CONSTRAINT FK_Lien_TM FOREIGN KEY (ID_Type_Mesure) REFERENCES Type_mesure(ID_Type_Mesure)
            );
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_Lien_PN')
                CREATE INDEX IX_Lien_PN ON Lien_type_mesure_type_piece(ID_PN, Is_Active);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_Lien_TM')
                CREATE INDEX IX_Lien_TM ON Lien_type_mesure_type_piece(ID_Type_Mesure);

            -- Personnes
            IF OBJECT_ID('Personnes','U') IS NULL
            CREATE TABLE Personnes (
                ID_Personne     INT IDENTITY(1,1) PRIMARY KEY,
                ID_Clipper      NVARCHAR(50),
                Nom             NVARCHAR(100) NOT NULL,
                Prenom          NVARCHAR(100),
                Code            NVARCHAR(20),
                Mot_De_Passe    NVARCHAR(200),
                Is_Active       BIT NOT NULL DEFAULT 1
            );

            -- Nomenclature
            IF OBJECT_ID('Nomenclature','U') IS NULL
            CREATE TABLE Nomenclature (
                ID_Nomenclature INT IDENTITY(1,1) PRIMARY KEY,
                ID_PN           INT,
                ID_PN_Cor       INT,
                Is_Priority     BIT DEFAULT 0,
                Is_Active       BIT NOT NULL DEFAULT 1
            );

            -- Priorites
            IF OBJECT_ID('Priorites','U') IS NULL
            CREATE TABLE Priorites (
                ID_Priorite     INT IDENTITY(1,1) PRIMARY KEY,
                ID_PN           INT,
                ID_Alumine      INT,
                Priorite        INT,
                Date_Priorite   DATETIME,
                Is_Active       BIT NOT NULL DEFAULT 1
            );
        ";

        await ExecuteSqlAsync(conn, sql);
        Console.WriteLine("  ✓ Schéma Référentiel créé");
    }

    static async Task CreateProductionSchemaAsync(string server)
    {
        Console.WriteLine("\n→ Schéma Bench_Production (Base B)...");
        string conn = $"Server={server};Database=Bench_Production;Trusted_Connection=True;TrustServerCertificate=True;";

        const string sql = @"
            -- Piece (numéros de série)
            IF OBJECT_ID('Piece','U') IS NULL
            CREATE TABLE Piece (
                ID_SN       INT IDENTITY(1,1) PRIMARY KEY,
                SN          NVARCHAR(100) NOT NULL,
                ID_PN       INT           NOT NULL,
                Is_Active   BIT           NOT NULL DEFAULT 1
            );
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_Piece_SN')
                CREATE INDEX IX_Piece_SN ON Piece(SN, Is_Active);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_Piece_PN')
                CREATE INDEX IX_Piece_PN ON Piece(ID_PN);

            -- Infos (traçabilité logistique)
            IF OBJECT_ID('Infos','U') IS NULL
            CREATE TABLE Infos (
                ID                  INT IDENTITY(1,1) PRIMARY KEY,
                BL                  NVARCHAR(100),
                Date_BL             DATETIME,
                ID_Operateur        INT,
                ID_Usineur          INT,
                Numero_Commande     NVARCHAR(100),
                ID_SN               INT,
                ID_PN_Alumine       INT,
                Numero_Conformite   NVARCHAR(100),
                Numero_Lot_Matiere  NVARCHAR(100),
                Numero_Four         NVARCHAR(100),
                Numero_Lot_Usinage  NVARCHAR(100),
                Is_Active           BIT NOT NULL DEFAULT 1
            );
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_Infos_SN')
                CREATE INDEX IX_Infos_SN ON Infos(ID_SN);

            -- Mesures
            IF OBJECT_ID('Mesures','U') IS NULL
            CREATE TABLE Mesures (
                ID_Mesure                       INT IDENTITY(1,1) PRIMARY KEY,
                Valeur                          DECIMAL(18,4),
                ID_SN                           INT           NOT NULL,
                Commentaire                     NVARCHAR(500),
                ID_Lien_Type_Mesure_Catalogue   INT           NOT NULL,
                Insert_Date                     DATETIME      NOT NULL DEFAULT GETDATE(),
                ID_Operateur                    INT,
                Is_Active                       BIT           NOT NULL DEFAULT 1
            );
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_Mesures_SN')
                CREATE INDEX IX_Mesures_SN ON Mesures(ID_SN, Is_Active);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_Mesures_Lien')
                CREATE INDEX IX_Mesures_Lien ON Mesures(ID_Lien_Type_Mesure_Catalogue, Is_Active);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_Mesures_Date')
                CREATE INDEX IX_Mesures_Date ON Mesures(Insert_Date);

            -- Historique
            IF OBJECT_ID('Historique','U') IS NULL
            CREATE TABLE Historique (
                ID_Historique   INT IDENTITY(1,1) PRIMARY KEY,
                ID_Source       INT,
                ID_Destination  INT,
                Nom_Table       NVARCHAR(100),
                ID_Operateur    INT,
                Date_Insertion  DATETIME NOT NULL DEFAULT GETDATE(),
                Statut          NVARCHAR(10)
            );
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_Hist_Source')
                CREATE INDEX IX_Hist_Source ON Historique(ID_Source, Nom_Table, Statut);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_Hist_Dest')
                CREATE INDEX IX_Hist_Dest ON Historique(ID_Destination, Nom_Table, Statut);

            -- Derogations
            IF OBJECT_ID('Derogations','U') IS NULL
            CREATE TABLE Derogations (
                ID_Derogation   INT IDENTITY(1,1) PRIMARY KEY,
                ID_Mesure       INT NOT NULL,
                Commentaire     NVARCHAR(1000),
                Accepted        INT NOT NULL DEFAULT 0,
                Is_Active       BIT NOT NULL DEFAULT 1
            );
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_Derog_Mesure')
                CREATE INDEX IX_Derog_Mesure ON Derogations(ID_Mesure, Is_Active);

            -- Etiquettes
            IF OBJECT_ID('Etiquettes','U') IS NULL
            CREATE TABLE Etiquettes (
                ID_Etiquette    INT IDENTITY(1,1) PRIMARY KEY,
                ID_SN           INT NOT NULL,
                ID_Personne     INT
            );

            -- EtiquettesLog
            IF OBJECT_ID('EtiquettesLog','U') IS NULL
            CREATE TABLE EtiquettesLog (
                ID_Etiquette_Log    INT IDENTITY(1,1) PRIMARY KEY,
                ID_SN               INT,
                Date_Print          DATETIME,
                ID_Personne         INT,
                Is_Active           BIT NOT NULL DEFAULT 1
            );
        ";

        await ExecuteSqlAsync(conn, sql);
        Console.WriteLine("  ✓ Schéma Production créé");
    }

    static async Task CreateCentralizedSchemaAsync(string server)
    {
        Console.WriteLine("\n→ Schéma Bench_Centralized (destination ETL)...");
        string conn = $"Server={server};Database=Bench_Centralized;Trusted_Connection=True;TrustServerCertificate=True;";

        const string sql = @"
            IF OBJECT_ID('Postes','U') IS NULL
                CREATE TABLE Postes (ID_Poste INT IDENTITY(1,1) PRIMARY KEY, Descript NVARCHAR(100) NOT NULL, Is_Active BIT NOT NULL DEFAULT 1);

            IF OBJECT_ID('Type_Piece','U') IS NULL
                CREATE TABLE Type_Piece (ID INT IDENTITY(1,1) PRIMARY KEY, type_piece_number NVARCHAR(50), Type_Article NVARCHAR(50), Modele_Article NVARCHAR(100), Nom_Client NVARCHAR(100), Is_Active BIT NOT NULL DEFAULT 1);

            IF OBJECT_ID('Type_mesure','U') IS NULL
                CREATE TABLE Type_mesure (ID_Type_Mesure INT IDENTITY(1,1) PRIMARY KEY, Poste NVARCHAR(50), Etape NVARCHAR(100), Symbole NVARCHAR(20), Nom_Donnee NVARCHAR(200), Nominal DECIMAL(18,4), Limite_Superieure DECIMAL(18,4), Limite_Inferieure DECIMAL(18,4), Precision_Scale INT DEFAULT 3, Is_Active BIT NOT NULL DEFAULT 1);

            IF OBJECT_ID('Lien_type_mesure_type_piece','U') IS NULL
                CREATE TABLE Lien_type_mesure_type_piece (ID_Lien_Type_Mesure_Catalogue INT IDENTITY(1,1) PRIMARY KEY, ID_PN INT, ID_Type_Mesure INT, Numero_Etape INT, Ordre_Mesure INT, Warning_Level DECIMAL(18,4), Is_Active BIT NOT NULL DEFAULT 1);

            IF OBJECT_ID('Personnes','U') IS NULL
                CREATE TABLE Personnes (ID_Personne INT IDENTITY(1,1) PRIMARY KEY, Nom NVARCHAR(100), Prenom NVARCHAR(100), Code NVARCHAR(20), Is_Active BIT NOT NULL DEFAULT 1);

            IF OBJECT_ID('Piece','U') IS NULL
                CREATE TABLE Piece (ID_SN INT IDENTITY(1,1) PRIMARY KEY, SN NVARCHAR(100), ID_PN INT, Is_Active BIT NOT NULL DEFAULT 1);

            IF OBJECT_ID('Mesures','U') IS NULL
                CREATE TABLE Mesures (ID_Mesure INT IDENTITY(1,1) PRIMARY KEY, Valeur DECIMAL(18,4), ID_SN INT, Commentaire NVARCHAR(500), ID_Lien_Type_Mesure_Catalogue INT, Insert_Date DATETIME NOT NULL DEFAULT GETDATE(), ID_Operateur INT, Is_Active BIT NOT NULL DEFAULT 1);

            IF OBJECT_ID('Historique','U') IS NULL
                CREATE TABLE Historique (ID_Historique INT IDENTITY(1,1) PRIMARY KEY, ID_Source INT, ID_Destination INT, Nom_Table NVARCHAR(100), ID_Operateur INT, Date_Insertion DATETIME NOT NULL DEFAULT GETDATE(), Statut NVARCHAR(10));

            IF OBJECT_ID('Derogations','U') IS NULL
                CREATE TABLE Derogations (ID_Derogation INT IDENTITY(1,1) PRIMARY KEY, ID_Mesure INT, Commentaire NVARCHAR(1000), Accepted INT DEFAULT 0, Is_Active BIT NOT NULL DEFAULT 1);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_C_Piece_SN')    CREATE INDEX IX_C_Piece_SN  ON Piece(SN, Is_Active);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_C_M_SN')        CREATE INDEX IX_C_M_SN      ON Mesures(ID_SN, Is_Active);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_C_M_Lien')      CREATE INDEX IX_C_M_Lien    ON Mesures(ID_Lien_Type_Mesure_Catalogue, Is_Active);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_C_Lien_PN')     CREATE INDEX IX_C_Lien_PN   ON Lien_type_mesure_type_piece(ID_PN, Is_Active);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_C_Hist_Src')    CREATE INDEX IX_C_Hist_Src  ON Historique(ID_Source, Nom_Table, Statut);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_C_Hist_Dst')    CREATE INDEX IX_C_Hist_Dst  ON Historique(ID_Destination, Nom_Table, Statut);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_C_Derog_M')     CREATE INDEX IX_C_Derog_M   ON Derogations(ID_Mesure, Is_Active);
        ";

        await ExecuteSqlAsync(conn, sql);
        Console.WriteLine("  ✓ Schéma Centralisée créé");
    }

    static async Task ConfigureLinkedServerAsync(string primaryServer, string referentielServer)
    {
        Console.WriteLine($"\n→ Configuration Linked Server 'BaseReferentiel' → {referentielServer}...");
        string conn = $"Server={primaryServer};Database=master;Trusted_Connection=True;TrustServerCertificate=True;";

        var sql = $@"
            IF EXISTS (SELECT 1 FROM sys.servers WHERE name = 'BaseReferentiel' AND server_id != 0)
                EXEC sp_dropserver 'BaseReferentiel', 'droplogins';

            EXEC sp_addlinkedserver
                @server     = N'BaseReferentiel',
                @srvproduct = N'SQL Server',
                @provider   = N'SQLNCLI11',
                @datasrc    = N'{referentielServer}';

            EXEC sp_addlinkedsrvlogin
                @rmtsrvname = N'BaseReferentiel',
                @useself    = N'True';

            EXEC sp_serveroption 'BaseReferentiel', 'data access',  'true';
            EXEC sp_serveroption 'BaseReferentiel', 'rpc',          'true';
            EXEC sp_serveroption 'BaseReferentiel', 'rpc out',      'true';
        ";

        await ExecuteSqlAsync(conn, sql);
        Console.WriteLine("  ✓ Linked Server configuré");
    }

    static async Task ExecuteSqlAsync(string connStr, string sql)
    {
        await using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        var batches = sql.Split(new[] { "\r\nGO\r\n", "\nGO\n" },
            StringSplitOptions.RemoveEmptyEntries);
        foreach (var batch in batches)
        {
            if (string.IsNullOrWhiteSpace(batch)) continue;
            await using var cmd = new SqlCommand(batch, conn) { CommandTimeout = 120 };
            await cmd.ExecuteNonQueryAsync();
        }
    }
}