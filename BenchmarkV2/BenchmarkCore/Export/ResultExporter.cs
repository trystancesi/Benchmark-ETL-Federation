using BenchmarkCore.Models;
using CsvHelper;
using System.Globalization;
using System.Text;

namespace BenchmarkCore.Export;

public class ResultExporter
{
    private readonly string _outputDirectory;

    public ResultExporter(string outputDirectory)
    {
        _outputDirectory = outputDirectory;
        Directory.CreateDirectory(outputDirectory);
    }

    public async Task ExportRawResultsAsync(IEnumerable<BenchmarkResult> results, string fileName)
    {
        var path = Path.Combine(_outputDirectory, fileName);
        await using var writer = new StreamWriter(path);
        using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);
        csv.WriteHeader<BenchmarkResult>();
        await csv.NextRecordAsync();
        foreach (var r in results)
        {
            csv.WriteRecord(r);
            await csv.NextRecordAsync();
        }
        Console.WriteLine($"[EXPORT] Résultats bruts → {path}");
    }

    public async Task ExportSummaryAsync(IEnumerable<BenchmarkSummary> summaries, string fileName)
    {
        var path = Path.Combine(_outputDirectory, fileName);
        await using var writer = new StreamWriter(path);
        using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);
        csv.WriteHeader<BenchmarkSummary>();
        await csv.NextRecordAsync();
        foreach (var s in summaries)
        {
            csv.WriteRecord(s);
            await csv.NextRecordAsync();
        }
        Console.WriteLine($"[EXPORT] Résumé → {path}");
    }

    public async Task ExportMarkdownReportAsync(
        IEnumerable<BenchmarkSummary> summaries, string fileName)
    {
        var path = Path.Combine(_outputDirectory, fileName);
        var sb = new StringBuilder();

        sb.AppendLine("# Rapport Benchmark – Architecture ETL vs Fédérée");
        sb.AppendLine($"*Généré le {DateTime.Now:dd/MM/yyyy HH:mm:ss}*");
        sb.AppendLine();
        sb.AppendLine("## Contexte");
        sb.AppendLine("Benchmark basé sur les requêtes réelles de production.");
        sb.AppendLine("Distribution : **Base Référentiel** (Type_Mesure, Lien, Pièce...) " +
                      "| **Base Production** (Mesures, SN, Historique, Dérogations...)");
        sb.AppendLine();

        var scenarioLabels = new Dictionary<TestScenario, string>
        {
            [TestScenario.T1_GetSNList] = "T1 – Liste SN (SELECT simple)",
            [TestScenario.T2_GetMesuresByPN] = "T2 – Mesures par pièce (4 jointures)",
            [TestScenario.T3_GetMesuresHistorique] = "T3 – Historique complet (CTE + fenêtrage)",
            [TestScenario.T4_InsertMesure] = "T4 – Insertion mesure (INSERT + OUTPUT)",
            [TestScenario.T5_BurstSelect] = "T5 – Charge en rafale SELECT (N requêtes parallèles)",
            [TestScenario.T6_ETLBatchIngestion] = "T6 – Ingestion batch ETL",
            [TestScenario.T7_BurstInsert] = "T7 – Charge en rafale INSERT (N insertions parallèles)",
        };

        foreach (var group in summaries.GroupBy(s => s.Scenario).OrderBy(g => g.Key))
        {
            // Sécurité : ignorer tout scénario inconnu plutôt que crasher
            if (!scenarioLabels.TryGetValue(group.Key, out var label))
                label = group.Key.ToString();

            sb.AppendLine($"## {label}");
            sb.AppendLine();
            sb.AppendLine("| Architecture | Volume | Médiane (ms) | P25 (ms) | P75 (ms) | IQR (ms) | CPU % | Mém .NET (Mo) | Mém SQL (Mo) | Lectures log. | Tps serveur (ms) |");
            sb.AppendLine("|---|---|---|---|---|---|---|---|---|---|---|");

            foreach (var s in group.OrderBy(x => x.Scale).ThenBy(x => x.Architecture))
            {
                sb.AppendLine(
                    $"| {s.Architecture} | {s.Scale} " +
                    $"| {s.MedianEndToEndMs:F1} | {s.P25EndToEndMs:F1} | {s.P75EndToEndMs:F1} " +
                    $"| {s.IQR:F1} | {s.MedianCpuPercent:F1} " +
                    $"| {s.MedianMemoryMb:F1} | {s.MedianMemSqlServerMb:F0} " +
                    $"| {s.MedianLogicalReads:F0} | {s.MedianServerElapsedMs:F1} |");
            }

            // Colonnes spéciales T5 et T7 — débit sous charge
            if (group.Key is TestScenario.T5_BurstSelect or TestScenario.T7_BurstInsert)
            {
                sb.AppendLine();
                sb.AppendLine("| Architecture | Volume | Workers | Débit (req/s) | Erreurs (médiane) |");
                sb.AppendLine("|---|---|---|---|---|");
                foreach (var s in group.OrderBy(x => x.Scale).ThenBy(x => x.Architecture))
                {
                    sb.AppendLine(
                        $"| {s.Architecture} | {s.Scale} " +
                        $"| {s.ConcurrentWorkers} " +
                        $"| {s.MedianThroughputRps:F1} | {s.MedianErrorCount:F0} |");
                }
            }

            // Colonne spéciale T6 — débit ingestion
            if (group.Key == TestScenario.T6_ETLBatchIngestion)
            {
                sb.AppendLine();
                sb.AppendLine("| Volume | Débit (lignes/sec) |");
                sb.AppendLine("|---|---|");
                foreach (var s in group.OrderBy(x => x.Scale))
                {
                    sb.AppendLine($"| {s.Scale} | {s.MedianIngestionRowsPerSecond:F0} |");
                }
            }

            sb.AppendLine();
        }

        await File.WriteAllTextAsync(path, sb.ToString());
        Console.WriteLine($"[EXPORT] Rapport Markdown → {path}");
    }
}