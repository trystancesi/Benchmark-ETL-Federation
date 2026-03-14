using BenchmarkCore.Configuration;
using BenchmarkCore.Export;
using BenchmarkCore.Models;
using BenchmarkCore.Statistics;
using Microsoft.Extensions.Configuration;

namespace BenchmarkRunner;

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("╔══════════════════════════════════════════════════════╗");
        Console.WriteLine("║   BENCHMARK ETL vs ARCHITECTURE FÉDÉRÉE             ║");
        Console.WriteLine("║   Basé sur les requêtes de production réelles        ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════╝");

        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false)
            .AddCommandLine(args)
            .AddEnvironmentVariables()
            .Build();

        var benchConfig = config.GetSection("Benchmark").Get<BenchmarkConfig>()
            ?? throw new InvalidOperationException("Configuration Benchmark manquante.");

        var testConfig = config.GetSection("TestSelection");

        // Scale peut venir de args CLI : BenchmarkRunner.exe small
        var scaleStr = args.Length > 0 ? args[0]
                     : testConfig.GetValue<string>("Scale") ?? "small";

        var sf = scaleStr.ToLower() switch
        {
            "small" => ScaleFactor.Small,
            "medium" => ScaleFactor.Medium,
            "large" => ScaleFactor.Large,
            _ => ScaleFactor.Small
        };

        Console.WriteLine($"\nScale : {sf} | Runs : {benchConfig.MeasurementRuns} | Warmup : {benchConfig.WarmupRuns}");

        var orchestrator = new BenchmarkOrchestrator(benchConfig);
        await orchestrator.InitializeTestDataAsync();

        var allResults = new List<BenchmarkResult>();
        var exporter = new ResultExporter(benchConfig.OutputDirectory);

        // ─── ETL ──────────────────────────────────────────────────────────────
        if (testConfig.GetValue<bool>("RunETL"))
        {
            Console.WriteLine("\n══ ARCHITECTURE ETL ══════════════════════════════════");

            if (testConfig.GetValue<bool>("RunT1")) allResults.AddRange(await orchestrator.RunT1_ETL(sf));
            if (testConfig.GetValue<bool>("RunT2")) allResults.AddRange(await orchestrator.RunT2_ETL(sf));
            if (testConfig.GetValue<bool>("RunT3")) allResults.AddRange(await orchestrator.RunT3_ETL(sf));
            if (testConfig.GetValue<bool>("RunT4")) allResults.AddRange(await orchestrator.RunT4_ETL(sf));

            if (testConfig.GetValue<bool>("RunT5"))
                foreach (var workers in benchConfig.BurstWorkerCounts)
                    allResults.AddRange(await orchestrator.RunT5_BurstAsync(Architecture.ETL, sf, workers));

            if (testConfig.GetValue<bool>("RunT6"))
                allResults.AddRange(await orchestrator.RunT6_ETLBatchAsync(sf));

            if (testConfig.GetValue<bool>("RunT7"))
                foreach (var workers in benchConfig.BurstWorkerCounts)
                    allResults.AddRange(await orchestrator.RunT7_BurstInsertAsync(Architecture.ETL, sf, workers));
        }

        // ─── Linked Server ────────────────────────────────────────────────────
        if (testConfig.GetValue<bool>("RunLinkedServer"))
        {
            Console.WriteLine("\n══ FÉDÉRÉ – Option A (Linked Server) ════════════════");

            if (testConfig.GetValue<bool>("RunT1")) allResults.AddRange(await orchestrator.RunT1_LS(sf));
            if (testConfig.GetValue<bool>("RunT2")) allResults.AddRange(await orchestrator.RunT2_LS(sf));
            if (testConfig.GetValue<bool>("RunT3")) allResults.AddRange(await orchestrator.RunT3_LS(sf));
            if (testConfig.GetValue<bool>("RunT4")) allResults.AddRange(await orchestrator.RunT4_LS(sf));

            if (testConfig.GetValue<bool>("RunT5"))
                foreach (var workers in benchConfig.BurstWorkerCounts)
                    allResults.AddRange(await orchestrator.RunT5_BurstAsync(
                        Architecture.FederatedLinkedServer, sf, workers));

            if (testConfig.GetValue<bool>("RunT7"))
                foreach (var workers in benchConfig.BurstWorkerCounts)
                    allResults.AddRange(await orchestrator.RunT7_BurstInsertAsync(
                        Architecture.FederatedLinkedServer, sf, workers));
        }

        // ─── In-Memory Médiation ──────────────────────────────────────────────
        if (testConfig.GetValue<bool>("RunInMemory"))
        {
            Console.WriteLine("\n══ FÉDÉRÉ – Option B (Médiation C#) ════════════════");

            if (testConfig.GetValue<bool>("RunT1")) allResults.AddRange(await orchestrator.RunT1_IM(sf));
            if (testConfig.GetValue<bool>("RunT2")) allResults.AddRange(await orchestrator.RunT2_IM(sf));
            if (testConfig.GetValue<bool>("RunT3")) allResults.AddRange(await orchestrator.RunT3_IM(sf));
            if (testConfig.GetValue<bool>("RunT4")) allResults.AddRange(await orchestrator.RunT4_IM(sf));

            if (testConfig.GetValue<bool>("RunT5"))
                foreach (var workers in benchConfig.BurstWorkerCounts)
                    allResults.AddRange(await orchestrator.RunT5_BurstAsync(
                        Architecture.FederatedInMemory, sf, workers));

            if (testConfig.GetValue<bool>("RunT7"))
                foreach (var workers in benchConfig.BurstWorkerCounts)
                    allResults.AddRange(await orchestrator.RunT7_BurstInsertAsync(
                        Architecture.FederatedInMemory, sf, workers));
        }

        // ─── Export ───────────────────────────────────────────────────────────
        Console.WriteLine("\n══ EXPORT DES RÉSULTATS ══════════════════════════════════");
        var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");

        await exporter.ExportRawResultsAsync(allResults, $"raw_{timestamp}.csv");

        var valid = allResults.Where(r => !r.IsWarmup && r.ErrorMessage == null).ToList();
        var summaries = valid
            .GroupBy(r => new { r.Architecture, r.Scenario, r.Scale })
            .Select(g => StatisticsHelper.Summarize(g))
            .ToList();

        await exporter.ExportSummaryAsync(summaries, $"summary_{timestamp}.csv");
        await exporter.ExportMarkdownReportAsync(summaries, $"report_{timestamp}.md");

        // Résumé console
        Console.WriteLine("\n╔══════════ RÉSUMÉ ════════════════════════════════════╗");
        Console.WriteLine($"{"Architecture",-25} {"Scénario",-28} {"Médiane ms",10} {"IQR ms",8}");
        Console.WriteLine(new string('─', 75));
        foreach (var s in summaries.OrderBy(x => x.Scenario).ThenBy(x => x.Architecture))
            Console.WriteLine($"{s.Architecture,-25} {s.Scenario,-28} {s.MedianEndToEndMs,10:F1} {s.IQR,8:F1}");
        Console.WriteLine("╚══════════════════════════════════════════════════════╝");
        Console.WriteLine($"\n✓ Résultats exportés dans : {benchConfig.OutputDirectory}\\");
    }
}