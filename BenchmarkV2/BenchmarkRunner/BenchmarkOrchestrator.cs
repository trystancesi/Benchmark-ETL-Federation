using System.Diagnostics;
using BenchmarkCore.Configuration;
using BenchmarkCore.Metrics;
using BenchmarkCore.Models;
using ETLPipeline.Load;
using ETLPipeline.Queries;
using FederatedQuery.InMemory;
using FederatedQuery.LinkedServer;
using Microsoft.Data.SqlClient;

namespace BenchmarkRunner;

/// <summary>
/// Orchestre tous les scénarios selon le protocole :
/// 1 warmup → DBCC DROPCLEANBUFFERS → 10 mesures → médiane + IQR
/// </summary>
public class BenchmarkOrchestrator
{
    private readonly BenchmarkConfig _cfg;
    private readonly EtlQueryExecutor _etl;
    private readonly LinkedServerQueryExecutor _ls;
    private readonly InMemoryMediatorExecutor _im;
    private readonly MesuresBulkLoader _bulkLoader;
    private readonly MetricsCollector _metrics;

    private List<string> _testSNs = new();
    private List<(int IdSN, int IdLien, int IdOperateur)> _insertParams = new();

    public BenchmarkOrchestrator(BenchmarkConfig cfg)
    {
        _cfg = cfg;
        _etl = new EtlQueryExecutor(cfg.CentralizedConnectionString);
        _ls = new LinkedServerQueryExecutor(cfg.ProductionConnectionString, cfg.LinkedServerName);
        _im = new InMemoryMediatorExecutor(cfg.ReferentielConnectionString, cfg.ProductionConnectionString);
        _bulkLoader = new MesuresBulkLoader(cfg.ProductionConnectionString, cfg.CentralizedConnectionString);
        _metrics = new MetricsCollector(cfg.ProductionConnectionString);
    }

    /// <summary>Charge des SN et paramètres de test depuis la base générée.</summary>
    public async Task InitializeTestDataAsync()
    {
        Console.WriteLine("\n→ Chargement des données de test...");

        await using var conn = new SqlConnection(_cfg.ProductionConnectionString);
        await conn.OpenAsync();

        await using var cmd = new SqlCommand(
            "SELECT TOP 100 SN FROM Piece WHERE Is_Active=1 ORDER BY NEWID()", conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            _testSNs.Add(reader.GetString(0));

        // Paramètres pour les INSERT (T4 / T7)
        await using var cmd2 = new SqlCommand(@"
            SELECT TOP 100 s.ID_SN, l.ID_Lien_Type_Mesure_Catalogue, 1 AS ID_Operateur
            FROM Piece s
            CROSS JOIN (SELECT TOP 1 ID_Lien_Type_Mesure_Catalogue
                        FROM Bench_Referentiel.dbo.Lien_type_mesure_type_piece) l
            WHERE s.Is_Active=1
            ORDER BY NEWID()", conn);

        try
        {
            await using var r2 = await cmd2.ExecuteReaderAsync();
            while (await r2.ReadAsync())
                _insertParams.Add((r2.GetInt32(0), r2.GetInt32(1), 1));
        }
        catch
        {
            await using var connC = new SqlConnection(_cfg.CentralizedConnectionString);
            await connC.OpenAsync();
            await using var cmd3 = new SqlCommand(@"
                SELECT TOP 100 s.ID_SN, l.ID_Lien_Type_Mesure_Catalogue, 1
                FROM Piece s
                CROSS JOIN (SELECT TOP 1 ID_Lien_Type_Mesure_Catalogue
                            FROM Lien_type_mesure_type_piece) l
                WHERE s.Is_Active=1
                ORDER BY NEWID()", connC);
            await using var r3 = await cmd3.ExecuteReaderAsync();
            while (await r3.ReadAsync())
                _insertParams.Add((r3.GetInt32(0), r3.GetInt32(1), 1));
        }

        Console.WriteLine($"  ✓ {_testSNs.Count} SN de test chargés");
        Console.WriteLine($"  ✓ {_insertParams.Count} jeux de paramètres INSERT chargés");
    }

    // ─── T1 ────────────────────────────────────────────────────────────────

    public Task<List<BenchmarkResult>> RunT1_ETL(ScaleFactor sf) =>
        RunScenarioAsync(Architecture.ETL, TestScenario.T1_GetSNList, sf,
            () => _etl.ExecuteT1_GetSNListAsync());

    public Task<List<BenchmarkResult>> RunT1_LS(ScaleFactor sf) =>
        RunScenarioAsync(Architecture.FederatedLinkedServer, TestScenario.T1_GetSNList, sf,
            () => _ls.ExecuteT1_GetSNListAsync());

    public Task<List<BenchmarkResult>> RunT1_IM(ScaleFactor sf) =>
        RunScenarioAsync(Architecture.FederatedInMemory, TestScenario.T1_GetSNList, sf,
            () => _im.ExecuteT1_GetSNListAsync());

    // ─── T2 ────────────────────────────────────────────────────────────────

    public Task<List<BenchmarkResult>> RunT2_ETL(ScaleFactor sf) =>
        RunScenarioAsync(Architecture.ETL, TestScenario.T2_GetMesuresByPN, sf,
            () => _etl.ExecuteT2_GetMesuresByPNAsync(RandomSN()));

    public Task<List<BenchmarkResult>> RunT2_LS(ScaleFactor sf) =>
        RunScenarioAsync(Architecture.FederatedLinkedServer, TestScenario.T2_GetMesuresByPN, sf,
            () => _ls.ExecuteT2_GetMesuresByPNAsync(RandomSN()));

    public Task<List<BenchmarkResult>> RunT2_IM(ScaleFactor sf) =>
        RunScenarioAsync(Architecture.FederatedInMemory, TestScenario.T2_GetMesuresByPN, sf,
            () => _im.ExecuteT2_GetMesuresByPNAsync(RandomSN()));

    // ─── T3 ────────────────────────────────────────────────────────────────

    public Task<List<BenchmarkResult>> RunT3_ETL(ScaleFactor sf) =>
        RunScenarioAsync(Architecture.ETL, TestScenario.T3_GetMesuresHistorique, sf,
            () => _etl.ExecuteT3_GetMesuresHistoriqueAsync(RandomSN()));

    public Task<List<BenchmarkResult>> RunT3_LS(ScaleFactor sf) =>
        RunScenarioAsync(Architecture.FederatedLinkedServer, TestScenario.T3_GetMesuresHistorique, sf,
            () => _ls.ExecuteT3_GetMesuresHistoriqueAsync(RandomSN()));

    public Task<List<BenchmarkResult>> RunT3_IM(ScaleFactor sf) =>
        RunScenarioAsync(Architecture.FederatedInMemory, TestScenario.T3_GetMesuresHistorique, sf,
            () => _im.ExecuteT3_GetMesuresHistoriqueAsync(RandomSN()));

    // ─── T4 ────────────────────────────────────────────────────────────────

    public Task<List<BenchmarkResult>> RunT4_ETL(ScaleFactor sf) =>
        RunScenarioAsync(Architecture.ETL, TestScenario.T4_InsertMesure, sf,
            async () => {
                var p = RandomInsertParam(); return await _etl.ExecuteT4_InsertMesureAsync(
                (decimal)(new Random().NextDouble() * 100), p.IdSN, p.IdLien, p.IdOperateur);
            });

    public Task<List<BenchmarkResult>> RunT4_LS(ScaleFactor sf) =>
        RunScenarioAsync(Architecture.FederatedLinkedServer, TestScenario.T4_InsertMesure, sf,
            async () => {
                var p = RandomInsertParam(); return await _ls.ExecuteT4_InsertMesureAsync(
                (decimal)(new Random().NextDouble() * 100), p.IdSN, p.IdLien, p.IdOperateur);
            });

    public Task<List<BenchmarkResult>> RunT4_IM(ScaleFactor sf) =>
        RunScenarioAsync(Architecture.FederatedInMemory, TestScenario.T4_InsertMesure, sf,
            async () => {
                var p = RandomInsertParam(); return await _im.ExecuteT4_InsertMesureAsync(
                (decimal)(new Random().NextDouble() * 100), p.IdSN, p.IdLien, p.IdOperateur);
            });

    // ─── T5 – Burst SELECT (charge en lecture parallèle) ──────────────────

    public async Task<List<BenchmarkResult>> RunT5_BurstAsync(
        Architecture arch, ScaleFactor sf, int workerCount)
    {
        Console.Write($"  [T5-{arch} {sf} {workerCount} workers]");
        var results = new List<BenchmarkResult>();

        for (int run = 0; run <= _cfg.MeasurementRuns; run++)
        {
            bool isWarmup = run == 0;
            if (!isWarmup && _cfg.ClearCacheBetweenRuns)
                await _metrics.ClearCacheAsync();

            var result = new BenchmarkResult
            {
                Architecture = arch,
                Scenario = TestScenario.T5_BurstSelect,
                Scale = sf,
                RunNumber = run,
                IsWarmup = isWarmup,
                ConcurrentWorkers = workerCount,
                Timestamp = DateTime.UtcNow
            };

            try
            {
                _metrics.StartSampling();
                var sw = Stopwatch.StartNew();
                int errorCount = 0;

                var tasks = Enumerable.Range(0, workerCount).Select(async _ =>
                {
                    try
                    {
                        var sn = RandomSN();
                        return arch switch
                        {
                            Architecture.ETL => await _etl.ExecuteT2_GetMesuresByPNAsync(sn),
                            Architecture.FederatedLinkedServer => await _ls.ExecuteT2_GetMesuresByPNAsync(sn),
                            Architecture.FederatedInMemory => await _im.ExecuteT2_GetMesuresByPNAsync(sn),
                            _ => 0L
                        };
                    }
                    catch
                    {
                        Interlocked.Increment(ref errorCount);
                        return 0L;
                    }
                });

                await Task.WhenAll(tasks);
                sw.Stop();

                var m = await _metrics.StopSamplingAsync();
                double rps = workerCount / sw.Elapsed.TotalSeconds;

                result.EndToEndMs = sw.Elapsed.TotalMilliseconds;
                result.CpuPercent = m.CpuPercent;
                result.MemoryMb = m.MemDotNetMb;
                result.MemSqlServerMb = m.MemSqlServerMb;
                result.LogicalReads = m.LogicalReads;
                result.PhysicalReads = m.PhysicalReads;
                result.ServerCpuMs = m.ServerCpuMs;
                result.ServerElapsedMs = m.ServerElapsedMs;
                result.ThroughputRequestsPerSec = rps;
                result.ErrorCount = errorCount;

                if (!isWarmup)
                    Console.Write($" {sw.Elapsed.TotalMilliseconds:F0}ms({rps:F1}rps)");
            }
            catch (Exception ex)
            {
                result.ErrorMessage = ex.Message;
                await _metrics.StopSamplingAsync();
            }

            results.Add(result);
        }
        Console.WriteLine();
        return results;
    }

    // ─── T6 – ETL Batch Ingestion ──────────────────────────────────────────

    public async Task<List<BenchmarkResult>> RunT6_ETLBatchAsync(ScaleFactor sf)
    {
        Console.Write($"  [T6-ETL {sf}] Ingestion batch");
        var results = new List<BenchmarkResult>();

        for (int run = 0; run <= _cfg.MeasurementRuns; run++)
        {
            bool isWarmup = run == 0;
            var result = new BenchmarkResult
            {
                Architecture = Architecture.ETL,
                Scenario = TestScenario.T6_ETLBatchIngestion,
                Scale = sf,
                RunNumber = run,
                IsWarmup = isWarmup,
                Timestamp = DateTime.UtcNow
            };

            try
            {
                _metrics.StartSampling();
                var sw = Stopwatch.StartNew();

                var (rowsCopied, rps) = await _bulkLoader.RunAsync(
                    new Progress<long>(n => Console.Write($"\r  [T6] {n:N0} lignes...")));

                sw.Stop();
                var m = await _metrics.StopSamplingAsync();

                result.EndToEndMs = sw.Elapsed.TotalMilliseconds;
                result.CpuPercent = m.CpuPercent;
                result.MemoryMb = m.MemDotNetMb;
                result.MemSqlServerMb = m.MemSqlServerMb;
                result.LogicalReads = m.LogicalReads;
                result.PhysicalReads = m.PhysicalReads;
                result.ServerCpuMs = m.ServerCpuMs;
                result.ServerElapsedMs = m.ServerElapsedMs;
                result.RowsCopied = rowsCopied;
                result.IngestionRowsPerSecond = rps;

                if (!isWarmup)
                    Console.Write($" {rps:F0} lignes/sec");
            }
            catch (Exception ex)
            {
                result.ErrorMessage = ex.Message;
                await _metrics.StopSamplingAsync();
                Console.WriteLine($"\n  [ERROR] {ex.Message}");
            }

            results.Add(result);
        }
        Console.WriteLine();
        return results;
    }

    // ─── T7 – Burst INSERT (charge en écriture parallèle) ─────────────────

    /// <summary>
    /// Lance N insertions T4 simultanées via Task.WhenAll.
    /// Mesure le comportement sous contention d'écriture — verrous de table,
    /// gestion des transactions concurrentes, pression sur le log SQL.
    /// Architecturalement symétrique à T5 mais pour l'écriture.
    /// </summary>
    public async Task<List<BenchmarkResult>> RunT7_BurstInsertAsync(
        Architecture arch, ScaleFactor sf, int workerCount)
    {
        Console.Write($"  [T7-{arch} {sf} {workerCount} workers]");
        var results = new List<BenchmarkResult>();

        for (int run = 0; run <= _cfg.MeasurementRuns; run++)
        {
            bool isWarmup = run == 0;
            if (!isWarmup && _cfg.ClearCacheBetweenRuns)
                await _metrics.ClearCacheAsync();

            var result = new BenchmarkResult
            {
                Architecture = arch,
                Scenario = TestScenario.T7_BurstInsert,
                Scale = sf,
                RunNumber = run,
                IsWarmup = isWarmup,
                ConcurrentWorkers = workerCount,
                Timestamp = DateTime.UtcNow
            };

            try
            {
                _metrics.StartSampling();
                var sw = Stopwatch.StartNew();
                int errorCount = 0;

                // Lance N insertions T4 en parallèle avec des paramètres distincts
                var tasks = Enumerable.Range(0, workerCount).Select(async _ =>
                {
                    try
                    {
                        var p = RandomInsertParam();
                        var valeur = (decimal)(Random.Shared.NextDouble() * 100);
                        return arch switch
                        {
                            Architecture.ETL =>
                                await _etl.ExecuteT4_InsertMesureAsync(valeur, p.IdSN, p.IdLien, p.IdOperateur),
                            Architecture.FederatedLinkedServer =>
                                await _ls.ExecuteT4_InsertMesureAsync(valeur, p.IdSN, p.IdLien, p.IdOperateur),
                            Architecture.FederatedInMemory =>
                                await _im.ExecuteT4_InsertMesureAsync(valeur, p.IdSN, p.IdLien, p.IdOperateur),
                            _ => 0L
                        };
                    }
                    catch
                    {
                        Interlocked.Increment(ref errorCount);
                        return 0L;
                    }
                });

                await Task.WhenAll(tasks);
                sw.Stop();

                var m = await _metrics.StopSamplingAsync();
                double rps = workerCount / sw.Elapsed.TotalSeconds;

                result.EndToEndMs = sw.Elapsed.TotalMilliseconds;
                result.CpuPercent = m.CpuPercent;
                result.MemoryMb = m.MemDotNetMb;
                result.MemSqlServerMb = m.MemSqlServerMb;
                result.LogicalReads = m.LogicalReads;
                result.PhysicalReads = m.PhysicalReads;
                result.ServerCpuMs = m.ServerCpuMs;
                result.ServerElapsedMs = m.ServerElapsedMs;
                result.ThroughputRequestsPerSec = rps;
                result.ErrorCount = errorCount;

                if (!isWarmup)
                    Console.Write($" {sw.Elapsed.TotalMilliseconds:F0}ms({rps:F1}rps)");
            }
            catch (Exception ex)
            {
                result.ErrorMessage = ex.Message;
                await _metrics.StopSamplingAsync();
            }

            results.Add(result);
        }
        Console.WriteLine();
        return results;
    }

    // ─── Boucle de mesure générique ───────────────────────────────────────

    private async Task<List<BenchmarkResult>> RunScenarioAsync(
        Architecture arch, TestScenario scenario, ScaleFactor sf,
        Func<Task<long>> queryFunc)
    {
        Console.Write($"  [{scenario} | {arch} | {sf}]");
        var results = new List<BenchmarkResult>();

        for (int run = 0; run <= _cfg.MeasurementRuns; run++)
        {
            bool isWarmup = run == 0;
            if (!isWarmup && _cfg.ClearCacheBetweenRuns)
                await _metrics.ClearCacheAsync();

            var result = new BenchmarkResult
            {
                Architecture = arch,
                Scenario = scenario,
                Scale = sf,
                RunNumber = run,
                IsWarmup = isWarmup,
                Timestamp = DateTime.UtcNow
            };

            try
            {
                _metrics.StartSampling();
                var sw = Stopwatch.StartNew();
                long rows = await queryFunc();
                sw.Stop();
                var m = await _metrics.StopSamplingAsync();

                result.EndToEndMs = sw.Elapsed.TotalMilliseconds;
                result.CpuPercent = m.CpuPercent;
                result.MemoryMb = m.MemDotNetMb;
                result.MemSqlServerMb = m.MemSqlServerMb;
                result.LogicalReads = m.LogicalReads;
                result.PhysicalReads = m.PhysicalReads;
                result.ServerCpuMs = m.ServerCpuMs;
                result.ServerElapsedMs = m.ServerElapsedMs;
                result.RowsReturned = rows;

                if (!isWarmup)
                    Console.Write($" {result.EndToEndMs:F0}ms");
            }
            catch (Exception ex)
            {
                result.ErrorMessage = ex.Message;
                await _metrics.StopSamplingAsync();
                Console.WriteLine($"\n  [ERROR run {run}] {ex.Message}");
            }

            results.Add(result);
        }

        var valid = results.Where(r => !r.IsWarmup && r.ErrorMessage == null).ToList();
        if (valid.Any())
        {
            double median = valid.Select(r => r.EndToEndMs).OrderBy(x => x)
                               .ElementAt(valid.Count / 2);
            Console.WriteLine($" → Médiane: {median:F0}ms");
        }
        else Console.WriteLine(" → Aucune mesure valide");

        return results;
    }

    private string RandomSN()
    {
        if (!_testSNs.Any()) return "SN-00000001";
        return _testSNs[Random.Shared.Next(_testSNs.Count)];
    }

    private (int IdSN, int IdLien, int IdOperateur) RandomInsertParam()
    {
        if (!_insertParams.Any()) return (1, 1, 1);
        return _insertParams[Random.Shared.Next(_insertParams.Count)];
    }
}