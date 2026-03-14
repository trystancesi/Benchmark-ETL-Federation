namespace BenchmarkCore.Models;

public enum Architecture { ETL, FederatedLinkedServer, FederatedInMemory }

public enum TestScenario
{
    T1_GetSNList,              // SELECT SN simple avec condition, ORDER BY ID_SN DESC
    T2_GetMesuresByPN,         // SELECT 4 jointures – mesures par type de pièce
    T3_GetMesuresHistorique,   // SELECT CTE + fenêtrage + historique complet
    T4_InsertMesure,           // INSERT Mesure avec OUTPUT INSERTED.ID_Mesure
    T5_BurstSelect,            // T2 × N exécutions parallèles (lecture simultanée)
    T6_ETLBatchIngestion,      // SqlBulkCopy Mesures Base Production → Centralisée
    T7_BurstInsert             // T4 × N exécutions parallèles (écriture simultanée)
}

public enum ScaleFactor
{
    Small = 1,  // ~1 000 SN,   ~20 000 mesures
    Medium = 2,  // ~10 000 SN,  ~250 000 mesures
    Large = 3   // ~100 000 SN, ~3 000 000 mesures
}

public class BenchmarkResult
{
    public Architecture Architecture { get; set; }
    public TestScenario Scenario { get; set; }
    public ScaleFactor Scale { get; set; }
    public int RunNumber { get; set; }
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;

    // ── Métrique 1 : temps end-to-end applicatif (Stopwatch) ──────────────
    public double EndToEndMs { get; set; }

    // ── Métrique 2 : temps serveur SQL (sys.dm_exec_query_stats delta) ────
    public double ServerCpuMs { get; set; }  // worker_time  / 1000
    public double ServerElapsedMs { get; set; }  // elapsed_time / 1000

    // ── Métrique 3 : CPU machine (PerformanceCounter Windows) ─────────────
    public double CpuPercent { get; set; }

    // ── Métrique 4 : mémoire ──────────────────────────────────────────────
    public double MemoryMb { get; set; }  // heap .NET (GC.GetTotalMemory)
    public double MemSqlServerMb { get; set; }  // Buffer Pool SQL Server (DMV)

    // ── Métrique 5 : lectures logiques / physiques (DMV delta) ────────────
    public long LogicalReads { get; set; }
    public long PhysicalReads { get; set; }

    // ── Lignes retournées / traitées ──────────────────────────────────────
    public long RowsReturned { get; set; }  // T1-T3
    public long? InsertedId { get; set; }  // T4

    // ── T5 / T7 – Burst ───────────────────────────────────────────────────
    public int? ConcurrentWorkers { get; set; }
    public double? ThroughputRequestsPerSec { get; set; }
    public int? ErrorCount { get; set; }

    // ── T6 – Ingestion batch ──────────────────────────────────────────────
    public long? RowsCopied { get; set; }
    public double? IngestionRowsPerSecond { get; set; }

    public bool IsWarmup { get; set; }
    public string? ErrorMessage { get; set; }
}

public class BenchmarkSummary
{
    public Architecture Architecture { get; set; }
    public TestScenario Scenario { get; set; }
    public ScaleFactor Scale { get; set; }
    public int SampleCount { get; set; }

    // End-to-end
    public double MedianEndToEndMs { get; set; }
    public double P25EndToEndMs { get; set; }
    public double P75EndToEndMs { get; set; }
    public double IQR => P75EndToEndMs - P25EndToEndMs;

    // Temps serveur SQL
    public double MedianServerCpuMs { get; set; }
    public double MedianServerElapsedMs { get; set; }

    // CPU machine
    public double MedianCpuPercent { get; set; }

    // Mémoire
    public double MedianMemoryMb { get; set; }  // .NET
    public double MedianMemSqlServerMb { get; set; } // SQL Server Buffer Pool

    // Lectures
    public double MedianLogicalReads { get; set; }
    public double MedianPhysicalReads { get; set; }

    // T5 / T7
    public int? ConcurrentWorkers { get; set; }
    public double? MedianThroughputRps { get; set; }
    public double? MedianErrorCount { get; set; }

    // T6
    public double? MedianIngestionRowsPerSecond { get; set; }
}