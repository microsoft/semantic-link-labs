import os
import sempy
import re
from urllib.parse import urlparse
import sempy.fabric as fabric
import sys
from pathlib import Path
from typing import Optional
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
    resolve_lakehouse_name_and_id,
    _mount,
    _get_column_aggregate,
    resolve_item_type,
)
import sempy_labs._icons as icons
from sempy_labs.lakehouse._blobs import list_blobs
from sempy_labs.tom import connect_semantic_model

_vpa_initialized = False


def init_vertipaq_analyzer() -> None:
    global _vpa_initialized
    if _vpa_initialized:
        return

    sempy.fabric._client._utils._init_analysis_services()

    # from clr_loader import get_coreclr
    # from pythonnet import set_runtime

    current_dir = Path(__file__).parent
    assembly_path = current_dir / "dotnet_lib"
    # rt = get_coreclr(
    #    runtime_config=os.fspath(f"{assembly_path}/dotnet.runtime.config.json")
    # )
    # set_runtime(rt)

    import clr

    sys.path.append(os.fspath(assembly_path))
    clr.AddReference(os.fspath(assembly_path / "Dax.Metadata.dll"))
    clr.AddReference(os.fspath(assembly_path / "Dax.Model.Extractor.dll"))
    clr.AddReference(os.fspath(assembly_path / "Dax.ViewVpaExport.dll"))
    clr.AddReference(os.fspath(assembly_path / "Dax.Vpax.dll"))
    clr.AddReference(os.fspath(assembly_path / "System.IO.Packaging.dll"))
    # clr.AddReference("Dax.Metadata")
    # clr.AddReference("Dax.Model.Extractor")
    # clr.AddReference("Dax.ViewVpaExport")
    # clr.AddReference("Dax.Vpax")
    # clr.AddReference("System.IO")
    _vpa_initialized = True


def create_vpax(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    lakehouse: Optional[str | UUID] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
    file_path: Optional[str] = None,
    read_stats_from_data: bool = False,
    read_direct_query_stats: bool = False,
    direct_lake_stats_mode: str = "ResidentOnly",
    overwrite: bool = False,
):
    """
    Creates a .vpax file for a semantic model and saves it to a lakehouse. This is based on `SQL BI's VertiPaq Analyzer <https://www.sqlbi.com/tools/vertipaq-analyzer/>`_.

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
    read_direct_query_stats : bool, default=False
        Whether to analyze DirectQuery tables.
    direct_lake_stats_mode : str, default='ResidentOnly'
        The Direct Lake extraction mode. Options are 'ResidentOnly' or 'Full'. This parameter is ignored if read_stats_from_data is False. This parameter is only relevant for tables which use Direct Lake mode.
        If set to 'ResidentOnly', column statistics are obtained only for the columns which are in memory.
        If set to 'Full', column statistics are obtained for all columns - pending the proper identification of the Direct Lake source.
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

    direct_lake_stats_mode = direct_lake_stats_mode.capitalize()

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)
    (lakehouse_workspace_name, lakehouse_workspace_id) = resolve_workspace_name_and_id(
        lakehouse_workspace
    )
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=lakehouse_workspace
    )

    local_path = _mount(lakehouse=lakehouse_id, workspace=lakehouse_workspace_id)
    if file_path is None:
        file_path = dataset_name
    path = f"{local_path}/Files/{file_path}.vpax"

    # Check if the .vpax file already exists in the lakehouse
    df = list_blobs(
        lakehouse=lakehouse_id,
        workspace=lakehouse_workspace_id,
        container="Files",
        prefix=f"{file_path}.vpax",
    )
    if not df.empty and not overwrite:
        print(
            f"{icons.warning} The .vpax file already exists at {path}. Set overwrite=True to overwrite the file."
        )
        return

    vpax_stream = MemoryStream()
    extractor_app_name = "VPAX Notebook"
    extractor_app_version = "1.0"
    column_batch_size = 50
    token = notebookutils.credentials.getToken("pbi")
    connection_string = f"data source=powerbi://api.powerbi.com/v1.0/myorg/{workspace_name};initial catalog={dataset_name};User ID=;Password={token};Persist Security Info=True;Impersonation Level=Impersonate "

    print(f"{icons.in_progress} Extracting .vpax metadata...")

    # Get stats for the model; for direct lake only get is_resident
    dax_model = TomExtractor.GetDaxModel(
        connection_string,
        extractor_app_name,
        extractor_app_version,
        read_stats_from_data,
        0,
        read_direct_query_stats,
        DirectLakeExtractionMode.ResidentOnly,
        column_batch_size,
    )
    vpa_model = Model(dax_model)
    tom_database = TomExtractor.GetDatabase(connection_string)

    # Calculate Direct Lake stats for columns which are IsResident=False
    with connect_semantic_model(dataset=dataset, workspace=workspace) as tom:
        is_direct_lake = tom.is_direct_lake()
        if read_stats_from_data and is_direct_lake and direct_lake_stats_mode == "Full":
            from System import Array, Byte

            df_not_resident = fabric.evaluate_dax(
                dataset=dataset,
                workspace=workspace,
                dax_string=""" SELECT [DIMENSION_NAME] AS [TableName], [ATTRIBUTE_NAME] AS [ColumnName] FROM $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMNS WHERE NOT [ISROWNUMBER] AND NOT [DICTIONARY_ISRESIDENT]""",
            )

            import Microsoft.AnalysisServices.Tabular as TOM

            print(f"{icons.in_progress} Calculating Direct Lake statistics...")

            # For SQL endpoints (do once)
            dfI = fabric.list_items(workspace=workspace)

            # Get list of tables in Direct Lake mode which have columns that are not resident
            tbls = [
                t
                for t in tom.model.Tables
                if t.Name in df_not_resident["TableName"].values
                and any(p.Mode == TOM.ModeType.DirectLake for p in t.Partitions)
            ]
            for t in tbls:
                column_cardinalities = {}
                table_name = t.Name
                partition = next(p for p in t.Partitions)
                entity_name = partition.Source.EntityName
                schema_name = partition.Source.SchemaName
                if len(schema_name) == 0 or schema_name == "dbo":
                    schema_name = None
                expr_name = partition.Source.ExpressionSource.Name
                expr = tom.model.Expressions[expr_name].Expression
                item_id = None
                if "Sql.Database(" in expr:
                    matches = re.findall(r'"([^"]+)"', expr)
                    sql_endpoint_id = matches[1]
                    dfI_filt = dfI[dfI["Id"] == sql_endpoint_id]
                    item_name = (
                        dfI_filt["Display Name"].iloc[0] if not dfI_filt.empty else None
                    )
                    dfI_filt2 = dfI[
                        (dfI["Display Name"] == item_name)
                        & (dfI["Type"].isin(["Lakehouse", "Warehouse"]))
                    ]
                    item_id = dfI_filt2["Id"].iloc[0]
                    item_type = dfI_filt2["Type"].iloc[0]
                    item_workspace_id = workspace_id
                elif "AzureStorage.DataLake(" in expr:
                    match = re.search(r'AzureStorage\.DataLake\("([^"]+)"', expr)
                    if match:
                        url = match.group(1)
                        path_parts = urlparse(url).path.strip("/").split("/")
                        if len(path_parts) >= 2:
                            item_workspace_id, item_id = (
                                path_parts[0],
                                path_parts[1],
                            )
                            item_type = resolve_item_type(
                                item_id=item_id, workspace=workspace_id
                            )
                else:
                    raise NotImplementedError(
                        f"Direct Lake source '{expr}' is not supported. Please report this issue on GitHub (https://github.com/microsoft/semantic-link-labs/issues)."
                    )

                if not item_id:
                    print(
                        f"{icons.info} Cannot determine the Direct Lake source of the '{table_name}' table."
                    )
                elif item_type == "Warehouse":
                    print(
                        f"{icons.info} The '{table_name}' table references a warehouse. Warehouses are not yet supported for this method."
                    )
                else:
                    df_not_resident_cols = df_not_resident[
                        df_not_resident["TableName"] == table_name
                    ]
                    col_dict = {
                        c.Name: c.SourceColumn
                        for c in t.Columns
                        if c.Type != TOM.ColumnType.RowNumber
                        and c.Name in df_not_resident_cols["ColumnName"].values
                    }
                    col_agg = _get_column_aggregate(
                        lakehouse=item_id,
                        workspace=item_workspace_id,
                        table_name=entity_name,
                        schema_name=schema_name,
                        column_name=list(col_dict.values()),
                        function="distinctcount",
                    )
                    column_cardinalities = {
                        column_name: col_agg[source_column]
                        for column_name, source_column in col_dict.items()
                        if source_column in col_agg
                    }

                    # Update the dax_model file with column cardinalities
                    tbl = next(
                        table
                        for table in dax_model.Tables
                        if str(t.TableName) == table_name
                    )
                    cols = [
                        col
                        for col in tbl.Columns
                        if str(col.ColumnType) != "RowNumber"
                        and str(col.ColumnName) in column_cardinalities
                    ]
                    for col in cols:
                        col.ColumnCardinality = column_cardinalities.get(
                            str(col.ColumnName)
                        )

    VpaxTools.ExportVpax(vpax_stream, dax_model, vpa_model, tom_database)

    print(f"{icons.in_progress} Exporting .vpax file...")

    mode = FileMode.Create
    file_stream = FileStream(path, mode, FileAccess.Write, FileShare.Read)
    vpax_stream.CopyTo(file_stream)
    file_stream.Close()

    print(
        f"{icons.green_dot} The {file_path}.vpax file has been saved in the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace_name}' workspace."
    )

    return (vpax_stream, dax_model, vpa_model, tom_database)


def _dax_distinctcount(table_name, columns):

    dax = "EVALUATE\nROW("
    for c in columns:
        full_name = f"'{table_name}'[{c}]"
        dax += f"""\n"{c}", DISTINCTCOUNT({full_name}),"""

    return f"{dax.rstrip(',')}\n)"


def _move_key_to_index(d, key, index):

    items = list(d.items())
    # Remove the key from its current position
    value = d[key]
    items = [(k, v) for k, v in items if k != key]
    # Insert it at the desired index
    items.insert(index, (key, value))
    return dict(items)
