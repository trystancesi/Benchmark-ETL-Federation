using Microsoft.Data.SqlClient;

namespace ETLPipeline.Load;

/// <summary>
/// T6 – Ingestion batch ETL : recopie les Mesures de la base Production
/// vers la base Centralisée via SqlBulkCopy.
/// Mesure le débit en lignes/seconde.
/// </summary>
public class MesuresBulkLoader
{
    private readonly string _sourceConnectionString;
    private readonly string _destinationConnectionString;
    private const int BatchSize = 10_000;

    public MesuresBulkLoader(string source, string destination)
    {
        _sourceConnectionString = source;
        _destinationConnectionString = destination;
    }

    public async Task<(long RowsCopied, double RowsPerSecond)> RunAsync(
        IProgress<long>? progress = null)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();

        // Truncate destination avant rechargement complet
        await TruncateAsync("Mesures");

        await using var srcConn = new SqlConnection(_sourceConnectionString);
        await srcConn.OpenAsync();

        const string extractSql = @"
            SELECT ID_Mesure, Valeur, ID_SN, Commentaire,
                   ID_Lien_Type_Mesure_Catalogue, Insert_Date,
                   ID_Operateur, Is_Active
            FROM Mesures
            WHERE Is_Active = 1;";

        await using var cmd = new SqlCommand(extractSql, srcConn) { CommandTimeout = 600 };
        await using var reader = await cmd.ExecuteReaderAsync(
            System.Data.CommandBehavior.SequentialAccess);

        long totalRows = 0;

        using var bulk = new SqlBulkCopy(_destinationConnectionString,
            SqlBulkCopyOptions.Default)
        {
            DestinationTableName = "Mesures",
            BatchSize = BatchSize,
            BulkCopyTimeout = 600,
            NotifyAfter = BatchSize
        };

        // Mapping explicite des colonnes
        bulk.ColumnMappings.Add("ID_Mesure",                        "ID_Mesure");
        bulk.ColumnMappings.Add("Valeur",                           "Valeur");
        bulk.ColumnMappings.Add("ID_SN",                            "ID_SN");
        bulk.ColumnMappings.Add("Commentaire",                      "Commentaire");
        bulk.ColumnMappings.Add("ID_Lien_Type_Mesure_Catalogue",    "ID_Lien_Type_Mesure_Catalogue");
        bulk.ColumnMappings.Add("Insert_Date",                      "Insert_Date");
        bulk.ColumnMappings.Add("ID_Operateur",                     "ID_Operateur");
        bulk.ColumnMappings.Add("Is_Active",                        "Is_Active");

        bulk.SqlRowsCopied += (_, e) =>
        {
            totalRows = e.RowsCopied;
            progress?.Report(totalRows);
        };

        await bulk.WriteToServerAsync(reader);
        totalRows = bulk.RowsCopied > 0 ? bulk.RowsCopied : totalRows;

        sw.Stop();
        double rps = totalRows / Math.Max(sw.Elapsed.TotalSeconds, 0.001);

        return (totalRows, rps);
    }

    private async Task TruncateAsync(string tableName)
    {
        await using var conn = new SqlConnection(_destinationConnectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand($"TRUNCATE TABLE {tableName}", conn)
        { CommandTimeout = 60 };
        await cmd.ExecuteNonQueryAsync();
    }
}
