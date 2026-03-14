namespace BenchmarkCore.Configuration;

public class BenchmarkConfig
{
    // Base A – Référentiel (lien_type_mesure_type_pièce, Type_mesure, Pièce, Type_pièce...)
    public string ReferentielConnectionString { get; set; } = string.Empty;

    // Base B – Production (Mesures, SN, Infos, Historique, Dérogations, Personnes...)
    public string ProductionConnectionString { get; set; } = string.Empty;

    // Base Centralisée – Destination ETL (toutes les tables réunies)
    public string CentralizedConnectionString { get; set; } = string.Empty;

    // Nom du Linked Server configuré sur l'instance principale pointant vers Base A
    public string LinkedServerName { get; set; } = "BaseReferentiel";

    public int WarmupRuns { get; set; } = 1;
    public int MeasurementRuns { get; set; } = 10;
    public string OutputDirectory { get; set; } = "Results";
    public bool ClearCacheBetweenRuns { get; set; } = true;

    // Paramètres T5 – Burst
    public int[] BurstWorkerCounts { get; set; } = { 10, 25, 50 };

    // Paramètres T6 – ETL batch
    public int BulkCopyBatchSize { get; set; } = 10_000;
}
