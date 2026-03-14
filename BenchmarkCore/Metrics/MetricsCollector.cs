using System.Diagnostics;
using Microsoft.Data.SqlClient;

namespace BenchmarkCore.Metrics;

/// <summary>
/// Collecte les métriques système et SQL Server pendant un run :
///   - CPU machine (PerformanceCounter Windows)
///   - Mémoire .NET process + SQL Server (sys.dm_os_performance_counters)
///   - Lectures logiques + physiques + temps serveur (sys.dm_exec_query_stats)
/// </summary>
public class MetricsCollector : IDisposable
{
    private readonly string _connectionString;

    // CPU sampling
    private PerformanceCounter? _cpuCounter;
    private readonly List<double> _cpuSamples = new();
    private CancellationTokenSource? _cts;
    private Task? _samplingTask;

    // Snapshot DMV pris AVANT le run pour isoler les stats de CE run
    private long _snapshotLogicalReads;
    private long _snapshotPhysicalReads;
    private long _snapshotWorkerTimeUs;   // temps CPU serveur en microsecondes
    private long _snapshotElapsedTimeUs;  // temps elapsed serveur en microsecondes

    public MetricsCollector(string connectionString)
    {
        _connectionString = connectionString;
        try
        {
            _cpuCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total");
            _cpuCounter.NextValue(); // premier appel toujours 0, nécessaire pour initialiser
        }
        catch
        {
            // Dégrade proprement si pas de droits PerformanceCounter
            _cpuCounter = null;
        }
    }

    // ── Démarrage du sampling ──────────────────────────────────────────────────

    /// <summary>
    /// Démarre le sampling CPU et prend un snapshot des DMVs SQL Server.
    /// À appeler juste avant l'exécution de la requête.
    /// </summary>
    public void StartSampling(int intervalMs = 200)
    {
        _cpuSamples.Clear();

        // Snapshot DMV synchrone — négligeable en latence (~1ms)
        TakeDmvSnapshot();

        _cts = new CancellationTokenSource();
        var token = _cts.Token;
        _samplingTask = Task.Run(async () =>
        {
            while (!token.IsCancellationRequested)
            {
                if (_cpuCounter != null)
                    _cpuSamples.Add(_cpuCounter.NextValue());
                try { await Task.Delay(intervalMs, token); }
                catch (TaskCanceledException) { break; }
            }
        }, token);
    }

    // ── Arrêt et collecte ─────────────────────────────────────────────────────

    /// <summary>
    /// Arrête le sampling et retourne toutes les métriques collectées.
    /// </summary>
    public async Task<MetricsSample> StopSamplingAsync()
    {
        _cts?.Cancel();
        if (_samplingTask != null)
        {
            try { await _samplingTask; }
            catch { /* ignoré */ }
        }

        double cpu = _cpuSamples.Any() ? _cpuSamples.Average() : 0;

        // Mémoire .NET process applicatif
        double memDotNetMb = GC.GetTotalMemory(false) / 1024.0 / 1024.0;

        // Mémoire SQL Server (Buffer Pool) via sys.dm_os_performance_counters
        double memSqlMb = await GetSqlServerMemoryMbAsync();

        // Lectures logiques/physiques + temps serveur (delta depuis snapshot)
        var (logicalReads, physicalReads, serverCpuMs, serverElapsedMs) =
            await GetDmvDeltaAsync();

        return new MetricsSample
        {
            CpuPercent = cpu,
            MemDotNetMb = memDotNetMb,
            MemSqlServerMb = memSqlMb,
            LogicalReads = logicalReads,
            PhysicalReads = physicalReads,
            ServerCpuMs = serverCpuMs,
            ServerElapsedMs = serverElapsedMs,
        };
    }

    // ── Cache clear ───────────────────────────────────────────────────────────

    public async Task ClearCacheAsync()
    {
        try
        {
            await using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            await using var cmd = new SqlCommand(
                "DBCC DROPCLEANBUFFERS; DBCC FREEPROCCACHE;", conn)
            { CommandTimeout = 60 };
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  [WARN] Cache clear ignoré (droits sysadmin requis): {ex.Message}");
        }
    }

    // ── DMV helpers ───────────────────────────────────────────────────────────

    /// <summary>
    /// Prend un snapshot des totaux agrégés de sys.dm_exec_query_stats.
    /// On agrège TOUT pour éviter le problème de corrélation par plan_handle
    /// (qui change entre runs après FREEPROCCACHE).
    /// </summary>
    private void TakeDmvSnapshot()
    {
        try
        {
            using var conn = new SqlConnection(_connectionString);
            conn.Open();
            using var cmd = new SqlCommand(@"
                SELECT
                    ISNULL(SUM(total_logical_reads),  0),
                    ISNULL(SUM(total_physical_reads), 0),
                    ISNULL(SUM(total_worker_time),    0),
                    ISNULL(SUM(total_elapsed_time),   0)
                FROM sys.dm_exec_query_stats", conn)
            { CommandTimeout = 10 };

            using var r = cmd.ExecuteReader();
            if (r.Read())
            {
                _snapshotLogicalReads = r.GetInt64(0);
                _snapshotPhysicalReads = r.GetInt64(1);
                _snapshotWorkerTimeUs = r.GetInt64(2);
                _snapshotElapsedTimeUs = r.GetInt64(3);
            }
        }
        catch
        {
            // Droits VIEW SERVER STATE manquants — tout reste à 0
            _snapshotLogicalReads = _snapshotPhysicalReads =
            _snapshotWorkerTimeUs = _snapshotElapsedTimeUs = 0;
        }
    }

    /// <summary>
    /// Calcule le delta lectures/temps entre le snapshot de départ et maintenant.
    /// </summary>
    private async Task<(long LogicalReads, long PhysicalReads, double ServerCpuMs, double ServerElapsedMs)>
        GetDmvDeltaAsync()
    {
        try
        {
            await using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            await using var cmd = new SqlCommand(@"
                SELECT
                    ISNULL(SUM(total_logical_reads),  0),
                    ISNULL(SUM(total_physical_reads), 0),
                    ISNULL(SUM(total_worker_time),    0),
                    ISNULL(SUM(total_elapsed_time),   0)
                FROM sys.dm_exec_query_stats", conn)
            { CommandTimeout = 10 };

            await using var r = await cmd.ExecuteReaderAsync();
            if (await r.ReadAsync())
            {
                long logReads = Math.Max(0, r.GetInt64(0) - _snapshotLogicalReads);
                long physReads = Math.Max(0, r.GetInt64(1) - _snapshotPhysicalReads);
                // worker_time et elapsed_time sont en microsecondes → convertir en ms
                double cpuMs = Math.Max(0, r.GetInt64(2) - _snapshotWorkerTimeUs) / 1000.0;
                double elapsedMs = Math.Max(0, r.GetInt64(3) - _snapshotElapsedTimeUs) / 1000.0;
                return (logReads, physReads, cpuMs, elapsedMs);
            }
        }
        catch { /* Droits insuffisants — retourne zéros */ }

        return (0, 0, 0, 0);
    }

    /// <summary>
    /// Lit la mémoire utilisée par SQL Server (Buffer Pool) via les compteurs de perf SQL.
    /// </summary>
    private async Task<double> GetSqlServerMemoryMbAsync()
    {
        try
        {
            await using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            // "Total Server Memory (KB)" = mémoire totale allouée par SQL Server
            await using var cmd = new SqlCommand(@"
                SELECT cntr_value
                FROM sys.dm_os_performance_counters
                WHERE counter_name = 'Total Server Memory (KB)'
                  AND object_name LIKE '%Memory Manager%'", conn)
            { CommandTimeout = 10 };

            var result = await cmd.ExecuteScalarAsync();
            if (result != null && result != DBNull.Value)
                return Convert.ToDouble(result) / 1024.0; // KB → MB
        }
        catch { /* Droits insuffisants */ }

        return 0;
    }

    // ── Dispose ───────────────────────────────────────────────────────────────

    public void Dispose()
    {
        _cpuCounter?.Dispose();
        _cts?.Dispose();
    }
}

/// <summary>
/// Toutes les métriques collectées pour un run.
/// Remplace le tuple (CpuPercent, MemoryMb) précédent.
/// </summary>
public record MetricsSample
{
    public double CpuPercent { get; init; }  // % CPU machine moyenne
    public double MemDotNetMb { get; init; }  // Mémoire heap .NET process
    public double MemSqlServerMb { get; init; }  // Mémoire Buffer Pool SQL Server
    public long LogicalReads { get; init; }  // Lectures logiques delta
    public long PhysicalReads { get; init; }  // Lectures physiques delta
    public double ServerCpuMs { get; init; }  // Temps CPU serveur (worker_time DMV)
    public double ServerElapsedMs { get; init; }  // Temps elapsed serveur (elapsed_time DMV)
}