import os
import sys
from typing import Optional
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
    resolve_lakehouse_name_and_id,
    resolve_lakehouse_id,
    resolve_workspace_id,
    create_abfss_path,
    _mount,
)
import sempy_labs._icons as icons
from sempy_labs.lakehouse._blobs import list_blobs
import sempy_labs._authentication as auth
import sempy_labs._utils as utils

_vpa_initialized = False


def init_vertipaq_analyzer() -> None:
    global _vpa_initialized
    if _vpa_initialized:
        return
    
    from clr_loader import get_coreclr
    from pythonnet import set_runtime

    assembly_path = f"{utils.current_dir}/dotnet_lib"
    rt = get_coreclr(runtime_config=os.fspath(f"{utils.current_dir}/dotnet.runtime.config.json"))
    set_runtime(rt)

    import clr

    sys.path.append(os.fspath(assembly_path))
    clr.AddReference("Dax.Metadata")
    clr.AddReference("Dax.Model.Extractor")
    clr.AddReference("Dax.ViewVpaExport")
    clr.AddReference("Dax.Vpax")
    clr.AddReference("System.IO")
    _vpa_initialized = True


def create_vpax(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    lakehouse: Optional[str | UUID] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
    file_path: Optional[str] = None,
    read_stats_from_data: bool = False,
    analyze_direct_query: bool = False,
    direct_lake_mode: str = "ResidentOnly",
    overwrite: bool = False,
):
    """
    Creates a .vpax file for a semantic model and saves it to a lakehouse.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    lakehouse : str | uuid.UUID, default=None
        The lakehouse name or ID.
        Defaults to None which resolves to the attached lakehouse.
    lakehouse_workspace : str | uuid.UUID, default=None
        The workspace name or ID of the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse.
    file_path : str, default=None
        The path where the .vpax file will be saved in the lakehouse.
        Defaults to None which resolves to the dataset name.
    read_stats_from_data : bool, default=False
        Whether to read statistics from the data.
    analyze_direct_query : bool, default=False
        Whether to analyze DirectQuery tables.
    direct_lake_mode : str, default='ResidentOnly'
        The Direct Lake extraction mode.
        Options are 'ResidentOnly' or 'Full'.
    overwrite : bool, default=False
        Whether to overwrite the .vpax file if it already exists in the lakehouse.
    """

    init_vertipaq_analyzer()

    import notebookutils
    from Dax.Metadata import DirectLakeExtractionMode
    from Dax.Model.Extractor import TomExtractor
    from Dax.Vpax.Tools import VpaxTools
    from Dax.ViewVpaExport import Model
    from System.IO import MemoryStream, FileMode, FileStream, FileAccess, FileShare

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)
    (lakehouse_workspace_name, lakehouse_workspace_id) = resolve_workspace_name_and_id(
        lakehouse_workspace
    )
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=lakehouse_workspace
    )

    token_provider = auth.token_provider.get()
    if token_provider is None:
        token = notebookutils.credentials.getToken("pbi")
    else:
        token = token_provider(audience="pbi")

    local_path = _mount(lakehouse=lakehouse, workspace=lakehouse_workspace)
    if file_path is None:
        file_path = dataset_name
    path = f"{local_path}/Files/{file_path}.vpax"

    # Check if the .vpax file already exists in the lakehouse
    df = list_blobs(lakehouse=lakehouse, workspace=lakehouse_workspace)
    df_filt = df[df["Blob Name"] == f"{lakehouse_id}/Files/{file_path}.vpax"]
    if not df_filt.empty and not overwrite:
        print(
            f"{icons.warning} The .vpax file already exists at {path}. Set overwrite=True to overwrite the file."
        )
        return

    vpax_stream = MemoryStream()
    extractor_app_name = "VPAX Notebook"
    extractor_app_version = "1.0"
    column_batch_size = 50
    connection_string = f"data source=powerbi://api.powerbi.com/v1.0/myorg/{workspace};initial catalog={dataset_name};User ID=;Password={token};Persist Security Info=True;Impersonation Level=Impersonate "

    print(f"{icons.in_progress} Extracting .vpax metadata...")

    dl_mode = (
        DirectLakeExtractionMode.Full
        if direct_lake_mode.capitalize() == "Full"
        else DirectLakeExtractionMode.ResidentOnly
    )

    dax_model = TomExtractor.GetDaxModel(
        connection_string,
        extractor_app_name,
        extractor_app_version,
        read_stats_from_data,
        0,
        analyze_direct_query,
        dl_mode,
        column_batch_size,
    )
    vpa_model = Model(dax_model)
    tom_database = TomExtractor.GetDatabase(connection_string)

    VpaxTools.ExportVpax(vpax_stream, dax_model, vpa_model, tom_database)
    print('stream...')
    print(vpax_stream)
    print('dax_model...')
    print(dax_model)
    print('vpa_model...')
    print(vpa_model)
    print('tom_database...')
    print(tom_database)

    # Direct Lake
    from sempy_labs.tom import connect_semantic_model
    #import Microsoft.AnalysisServices.Tabular as TOM
    import re
    import sempy.fabric as fabric

    def _get_column_aggregates(lakehouse, workspace, table_name, schema_name, columns):
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import countDistinct

        spark = SparkSession.builder.getOrCreate()

        if isinstance(columns, str):
            columns = [columns]

        workspace_id = resolve_workspace_id(workspace)
        lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)
        path = create_abfss_path(lakehouse_id, workspace_id, table_name, schema_name)

        df = spark.read.format("delta").load(path)

        agg_exprs = [countDistinct(col).alias(f"{col}") for col in columns]
        result_df = df.agg(*agg_exprs)

        return result_df.collect()[0].asDict()

    with connect_semantic_model(dataset=dataset, workspace=workspace) as tom:
        dl_tables = [
            t
            for t in tom.model.Tables
            if any(p.Mode == TOM.ModeType.DirectLake for p in t.Partitions)
        ]
        dl_tables = []
        for t in dl_tables:
            entity_name = next(p.Source.EntityName for p in t.Partitions)
            schema_name = next(p.Source.SchemaName for p in t.Partitions) or "dbo"
            expr_name = next(p.Source.ExpressionSource.Name for p in t.Partitions)
            expr = tom.model.Expressions[expr_name].Expression
            if "Sql.Database(" in expr:
                matches = re.findall(r'"([^"]+)"', expr)
                sql_endpoint_id = matches[1]
                dfI = fabric.list_items(type="SQLEndpoint", workspace=workspace)
                dfI_filt = dfI[dfI["Id"] == sql_endpoint_id]
                if dfI_filt.empty:
                    name = None
                else:
                    name = dfI_filt["Display Name"].iloc[0]

            if name:
                col_dict = {}
                for c in t.Columns:
                    col_dict[c.Name] = c.SourceColumn
                col_agg = _get_column_aggregates(
                    lakehouse=name,
                    workspace=workspace,
                    table_name=entity_name,
                    schema_name=schema_name,
                    columns=list(col_dict.keys()),
                )
                column_dc = {
                    column_name: col_agg[source_column]
                    for column_name, source_column in col_dict.items()
                    if source_column in col_agg
                }
                # Update the vpax file

            else:
                print(
                    f"{icons.info} Cannot determine the Direct Lake source of the '{t.Name}' table."
                )

    print(f"{icons.in_progress} Exporting .vpax file...")

    mode = FileMode.Create
    file_stream = FileStream(path, mode, FileAccess.Write, FileShare.Read)
    vpax_stream.CopyTo(file_stream)
    file_stream.Close()

    print(
        f"{icons.green_dot} The {file_path}.vpax file has been saved in the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace_name}' workspace."
    )
