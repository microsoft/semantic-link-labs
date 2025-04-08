import os
import sempy
import re
import json
import sempy.fabric as fabric
import sys
from pathlib import Path
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
import zipfile
import io
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

    #init_vertipaq_analyzer()

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

    # token_provider = auth.token_provider.get()
    # if token_provider is None:
    token = notebookutils.credentials.getToken("pbi")
    # else:
    #    token = token_provider(audience="pbi")

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

    # Do not use TOMExtractor to get stats for Direct Lake
    with connect_semantic_model(dataset=dataset, workspace=workspace) as tom:
        is_direct_lake = tom.is_direct_lake()
        read_stats_using_tom_extractor = not is_direct_lake and read_stats_from_data

    dax_model = TomExtractor.GetDaxModel(
        connection_string,
        extractor_app_name,
        extractor_app_version,
        read_stats_using_tom_extractor,
        0,
        analyze_direct_query,
        dl_mode,
        column_batch_size,
    )
    vpa_model = Model(dax_model)
    tom_database = TomExtractor.GetDatabase(connection_string)

    VpaxTools.ExportVpax(vpax_stream, dax_model, vpa_model, tom_database)

    def _get_column_aggregates(lakehouse, workspace, table_name, schema_name, columns):
        from pyspark.sql.functions import countDistinct
        from sempy_labs._helper_functions import _read_delta_table

        if isinstance(columns, str):
            columns = [columns]

        workspace_id = resolve_workspace_id(workspace)
        lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)
        path = create_abfss_path(lakehouse_id, workspace_id, table_name, schema_name)

        df = _read_delta_table(path)

        agg_exprs = [countDistinct(col).alias(f"{col}") for col in columns]
        result_df = df.agg(*agg_exprs)

        return result_df.collect()[0].asDict()

    # Calculate Direct Lake stats
    if read_stats_from_data and is_direct_lake:
        from System import Array, Byte

        # Extract vertipaq json from the vpax stream
        vpax_stream.Position = 0
        buffer = Array.CreateInstance(Byte, vpax_stream.Length)
        vpax_stream.Read(buffer, 0, vpax_stream.Length)
        vpax_bytes = bytes(bytearray(buffer))
        vpax_zip = zipfile.ZipFile(io.BytesIO(vpax_bytes), mode="r")
        vpa_json = vpax_zip.read("DaxVpaView.json").decode("utf-8")
        cleaned_data = vpa_json.lstrip("\ufeff")
        data_dict = json.loads(cleaned_data)

        with connect_semantic_model(dataset=dataset, workspace=workspace) as tom:
            import Microsoft.AnalysisServices.Tabular as TOM

            print(f"{icons.in_progress} Calculating Direct Lake statistics...")

            for t in tom.model.Tables:
                column_cardinalities = {}
                table_name = t.Name
                # Direct Lake
                if any(p.Mode == TOM.ModeType.DirectLake for p in t.Partitions):
                    entity_name = next(p.Source.EntityName for p in t.Partitions)
                    schema_name = next(p.Source.SchemaName for p in t.Partitions)
                    if len(schema_name) == 0 or schema_name == "dbo":
                        schema_name = None
                    expr_name = next(
                        p.Source.ExpressionSource.Name for p in t.Partitions
                    )
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

                    if not name:
                        print(
                            f"{icons.info} Cannot determine the Direct Lake source of the '{table_name}' table."
                        )
                    else:
                        col_dict = {
                            c.Name: c.SourceColumn
                            for c in t.Columns
                            if c.Type != TOM.ColumnType.RowNumber
                        }
                        col_agg = _get_column_aggregates(
                            lakehouse=name,
                            workspace=workspace,
                            table_name=entity_name,
                            schema_name=schema_name,
                            columns=list(col_dict.keys()),
                        )
                        column_cardinalities = {
                            column_name: col_agg[source_column]
                            for column_name, source_column in col_dict.items()
                            if source_column in col_agg
                        }
                else:
                    columns = [
                        c.Name for c in t.Columns if c.Type != TOM.ColumnType.RowNumber
                    ]
                    dax = _dax_distinctcount(table_name, columns)
                    dax_result = fabric.evaluate_dax(
                        dataset=dataset, workspace=workspace, dax_string=dax
                    )
                    column_cardinalities = dax_result.iloc[0].to_dict()
                # Update the vpax file with column cardinalities
                for item_type, items in data_dict.items():
                    if item_type == "Columns":
                        for i, c in enumerate(items):  # get index and item
                            t_name = c.get("TableName")
                            c_name = c.get("ColumnName")
                            if t_name == t.Name and c.get('ColumnType') != "RowNumber":
                                c["ColumnCardinality"] = column_cardinalities.get(c_name)
                                items[i] = _move_key_to_index(c, 'ColumnCardinality', 3)  # update in-place

        # Update the vpax file with relationship cardinalities
        for item_type, items in data_dict.items():
            if item_type == "Relationships":
                for i, r in enumerate(items):
                    from_column_full = r.get("FromFullColumnName")
                    to_column_full = r.get("ToFullColumnName")

                    from_cardinality = next(
                        c.get("ColumnCardinality")
                        for c in data_dict["Columns"]
                        if f"'{c.get('TableName')}'[{c.get('ColumnName')}]" == from_column_full
                    )

                    to_cardinality = next(
                        c.get("ColumnCardinality")
                        for c in data_dict["Columns"]
                        if f"'{c.get('TableName')}'[{c.get('ColumnName')}]" == to_column_full
                    )

                    r["FromCardinality"] = from_cardinality
                    r["ToCardinality"] = to_cardinality

                    # Update the item in-place
                    r = _move_key_to_index(r, 'FromCardinality', 2)
                    r = _move_key_to_index(r, 'ToCardinality', 6)
                    items[i] = r  # <--- this line ensures the data_dict is updated
                    r["UsedSizeFrom"] = 0
                    r["UsedSize"] = 0
                    r["OneToManyRatio"] = from_cardinality / to_cardinality if to_cardinality else 0

        # Step 1: Convert data_dict to JSON string and encode it
        updated_json_str = json.dumps(data_dict, indent=2)
        updated_json_bytes = updated_json_str.encode('utf-8')

        # Step 2: Recreate the vpax ZIP archive in memory, replacing "DaxVpaView.json"
        new_vpax_stream = io.BytesIO()
        with zipfile.ZipFile(new_vpax_stream, mode="w", compression=zipfile.ZIP_DEFLATED) as new_zip:
            # Copy original contents except DaxVpaView.json
            for item in vpax_zip.infolist():
                if item.filename != "DaxVpaView.json":
                    new_zip.writestr(item, vpax_zip.read(item.filename))
            # Add updated DaxVpaView.json
            new_zip.writestr("DaxVpaView.json", updated_json_bytes)

        # Step 3: Reset the stream position
        new_vpax_stream.seek(0)
        vpax_stream = MemoryStream(new_vpax_stream.getvalue())

    print(f"{icons.in_progress} Exporting .vpax file...")

    mode = FileMode.Create
    file_stream = FileStream(path, mode, FileAccess.Write, FileShare.Read)
    vpax_stream.CopyTo(file_stream)
    file_stream.Close()

    print(
        f"{icons.green_dot} The {file_path}.vpax file has been saved in the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace_name}' workspace."
    )

    return vpax_stream


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
