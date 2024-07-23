// Copyright (c) Microsoft Corporation. All rights reserved.
namespace Microsoft.Fabric.SemanticLinkLabs
{
    using System;
    using System.Data;
    using System.IO;
    using Dax.Metadata;
    using Dax.Model.Extractor;
    using TOM = Microsoft.AnalysisServices.Tabular;

    /// <summary>
    /// Asynchronous writer of IDataReader output to a local parquet file.
    /// </summary>
    public static class VpaxTools
    {
        /// <summary>
        /// Main entry point.
        /// </summary>
        /// <param name="model">The model to extract.</param>
        /// <param name="connection">The connection to the data source.</param>
        /// <param name="serverName">The server name.</param>
        /// <param name="databaseName">The database name.</param>
        /// <param name="excludeVpa">Whether to exclude the VPA.</param>
        /// <param name="excludeTom">Whether to exclude the TOM.</param>
        /// <param name="destinationPath">The destination path.</param>
        public static void Export(
            TOM.Model model,
            IDbConnection connection,
            string serverName,
            string databaseName,
            bool excludeVpa,
            bool excludeTom,
            string destinationPath)
        {
            var thisAssembly = typeof(VpaxTools).Assembly;

            var applicationName = thisAssembly.GetName().Name;
            var applicationVersion = thisAssembly.GetName().Version.ToString();

            var daxModel = TomExtractor.GetDaxModel(model, applicationName, applicationVersion);

            var readStatisticsFromData = true;
            var sampleRows = 0; // RI violation sampling is not applicable to VPAX files
            var analyzeDirectQuery = true;
            var analyzeDirectLake = DirectLakeExtractionMode.Full;

            // Populate statistics from DMV
            DmvExtractor.PopulateFromDmv(daxModel, connection, serverName, databaseName, applicationName, applicationVersion);

            // Populate statistics by querying the data model
            if (readStatisticsFromData)
            {
#pragma warning disable CS0618 // Type or member is obsolete
                StatExtractor.UpdateStatisticsModel(daxModel, connection, sampleRows, analyzeDirectQuery, analyzeDirectLake);
#pragma warning restore CS0618 // Type or member is obsolete

                // If model has any DL partitions and we have forced all columns into memory then re-run the DMVs to update the data with the new values after everything has been transcoded.
                if (analyzeDirectLake > DirectLakeExtractionMode.ResidentOnly && daxModel.HasDirectLakePartitions())
                {
                    DmvExtractor.PopulateFromDmv(daxModel, connection, serverName, databaseName, applicationName, applicationVersion);
                }
            }

            var vpaModel = excludeVpa ? null : new Dax.ViewVpaExport.Model(daxModel);
            TOM.Database tomDatabase = null;

            if (!excludeTom)
            {
                // TODO: is this the right way to get the TOM database?
                tomDatabase = model.Database as TOM.Database;
            }

            using (var vpaxStream = new MemoryStream())
            {
                Dax.Vpax.Tools.VpaxTools.ExportVpax(vpaxStream, daxModel, vpaModel, tomDatabase);

                vpaxStream.Seek(0, SeekOrigin.Begin);

                using (var fileStream = new FileStream(destinationPath, FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    vpaxStream.CopyTo(fileStream);
                }
            }
        }
    }
}