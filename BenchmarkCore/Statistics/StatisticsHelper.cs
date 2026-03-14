using BenchmarkCore.Models;

namespace BenchmarkCore.Statistics;

public static class StatisticsHelper
{
    public static double Percentile(IEnumerable<double> values, double percentile)
    {
        var sorted = values.OrderBy(v => v).ToArray();
        if (sorted.Length == 0) return 0;
        double index = (percentile / 100.0) * (sorted.Length - 1);
        int lower = (int)Math.Floor(index);
        int upper = (int)Math.Ceiling(index);
        if (lower == upper) return sorted[lower];
        return sorted[lower] + (index - lower) * (sorted[upper] - sorted[lower]);
    }

    public static double Median(IEnumerable<double> values) => Percentile(values, 50);

    public static BenchmarkSummary Summarize(IEnumerable<BenchmarkResult> results)
    {
        var list = results
            .Where(r => !r.IsWarmup && r.ErrorMessage == null)
            .ToList();

        if (!list.Any())
            throw new InvalidOperationException("Aucun résultat valide à résumer.");

        var first = list.First();
        var e2e = list.Select(r => r.EndToEndMs).ToArray();

        return new BenchmarkSummary
        {
            Architecture = first.Architecture,
            Scenario = first.Scenario,
            Scale = first.Scale,
            SampleCount = list.Count,

            // End-to-end
            MedianEndToEndMs = Median(e2e),
            P25EndToEndMs = Percentile(e2e, 25),
            P75EndToEndMs = Percentile(e2e, 75),

            // Temps serveur SQL (DMV)
            MedianServerCpuMs = Median(list.Select(r => r.ServerCpuMs)),
            MedianServerElapsedMs = Median(list.Select(r => r.ServerElapsedMs)),

            // CPU machine
            MedianCpuPercent = Median(list.Select(r => r.CpuPercent)),

            // Mémoire
            MedianMemoryMb = Median(list.Select(r => r.MemoryMb)),
            MedianMemSqlServerMb = Median(list.Select(r => r.MemSqlServerMb)),

            // Lectures (DMV)
            MedianLogicalReads = Median(list.Select(r => (double)r.LogicalReads)),
            MedianPhysicalReads = Median(list.Select(r => (double)r.PhysicalReads)),

            // T5 / T7 – débit sous charge
            ConcurrentWorkers = list.FirstOrDefault(r => r.ConcurrentWorkers.HasValue)?.ConcurrentWorkers,

            MedianThroughputRps = list.Any(r => r.ThroughputRequestsPerSec.HasValue)
                ? Median(list.Where(r => r.ThroughputRequestsPerSec.HasValue)
                             .Select(r => r.ThroughputRequestsPerSec!.Value))
                : null,

            MedianErrorCount = list.Any(r => r.ErrorCount.HasValue)
                ? Median(list.Where(r => r.ErrorCount.HasValue)
                             .Select(r => (double)r.ErrorCount!.Value))
                : null,

            // T6 – débit ingestion
            MedianIngestionRowsPerSecond = list.Any(r => r.IngestionRowsPerSecond.HasValue)
                ? Median(list.Where(r => r.IngestionRowsPerSecond.HasValue)
                             .Select(r => r.IngestionRowsPerSecond!.Value))
                : null
        };
    }
}