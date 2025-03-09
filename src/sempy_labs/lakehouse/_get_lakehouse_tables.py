import sempy.fabric as fabric
import pandas as pd
import pyarrow.parquet as pq
import datetime
from sempy_labs._helper_functions import (
    _get_column_aggregate,
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    save_as_delta_table,
    _base_api,
    _create_dataframe,
    _create_spark_session,
    _mount,
    create_abfss_path,
    _read_delta_table,
    _get_parquet_file_infos,
)
from sempy_labs.directlake._guardrails import (
    get_sku_size,
    get_directlake_guardrails_for_sku,
)
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
from typing import Optional
import sempy_labs._icons as icons
from sempy._utils._log import log
from uuid import UUID


@log
def get_lakehouse_tables(
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    extended: bool = False,
    count_rows: bool = False,
    export: bool = False,
) -> pd.DataFrame:
    """
    Shows the tables of a lakehouse and their respective properties. Option to include additional properties relevant to Direct Lake guardrails.

    This is a wrapper function for the following API: `Tables - List Tables <https://learn.microsoft.com/rest/api/fabric/lakehouse/tables/list-tables>`_ plus extended capabilities.

    Parameters
    ----------
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    extended : bool, default=False
        Obtains additional columns relevant to the size of each table.
    count_rows : bool, default=False
        Obtains a row count for each lakehouse table.
    export : bool, default=False
        Exports the resulting dataframe to a delta table in the lakehouse.

    Returns
    -------
    pandas.DataFrame
        Shows the tables/columns within a lakehouse and their properties.
    """

    columns = {
        "Workspace Name": "string",
        "Lakehouse Name": "string",
        "Table Name": "string",
        "Format": "string",
        "Type": "string",
        "Location": "string",
    }
    df = _create_dataframe(columns=columns)

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    if count_rows:  # Setting countrows defaults to extended=True
        extended = True

    responses = _base_api(
        request=f"v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables",
        uses_pagination=True,
    )

    if not responses[0].get("data"):
        return df

    dfs = []
    for r in responses:
        for i in r.get("data", []):
            new_data = {
                "Workspace Name": workspace_name,
                "Lakehouse Name": lakehouse_name,
                "Table Name": i.get("name"),
                "Format": i.get("format"),
                "Type": i.get("type"),
                "Location": i.get("location"),
            }
            dfs.append(pd.DataFrame(new_data, index=[0]))

    if dfs:
        df = pd.concat(dfs, ignore_index=True)

    if extended:
        local_path = _mount(lakehouse=lakehouse, workspace=workspace)
        sku_value = get_sku_size(workspace_id)
        guardrail = get_directlake_guardrails_for_sku(sku_value)
        spark = _create_spark_session()
        df["Files"] = None
        df["Row Groups"] = None
        df["Table Size"] = None
        if count_rows:
            df["Row Count"] = 0
        for i, r in df.iterrows():
            table_name = r["Table Name"]
            delta_table_path = create_abfss_path(lakehouse_id, workspace_id, table_name)
            if r["Type"] == "Managed" and r["Format"] == "delta":

                from delta import DeltaTable

                delta_table = DeltaTable.forPath(spark, delta_table_path)
                table_df = delta_table.toDF()
                table_details = delta_table.detail().collect()[0].asDict()
                num_files = table_details.get("numFiles", 0)
                size_in_bytes = table_details.get("sizeInBytes", 0)
                latest_files = _read_delta_table(delta_table_path).inputFiles()
                all_parquet_files = _get_parquet_file_infos(delta_table_path)
                common_file_paths = set(
                    [file_info[0] for file_info in all_parquet_files]
                ).intersection(set(latest_files))
                latest_version_files = [
                    file_info
                    for file_info in all_parquet_files
                    if file_info[0] in common_file_paths
                ]

                delta_table_path = f"Tables/{table_name}"

                row_groups = 0
                for file_path, file_size in latest_version_files:
                    relative_path = file_path.split("Tables/")[1]
                    file_system_path = f"{local_path}/Tables/{relative_path}"
                    parquet_file = pq.ParquetFile(file_system_path)
                    row_groups += parquet_file.num_row_groups
                df.at[i, "Files"] = num_files
                df.at[i, "Row Groups"] = row_groups
                df.at[i, "Table Size"] = size_in_bytes
            if count_rows:
                num_rows = table_df.count()
                df.at[i, "Row Count"] = num_rows

    if extended:
        intColumns = ["Files", "Row Groups", "Table Size"]
        df[intColumns] = df[intColumns].astype(int)

        col_name = guardrail.columns[0]
        df["SKU"] = guardrail[col_name].iloc[0]
        df["Parquet File Guardrail"] = guardrail["Parquet files per table"].iloc[0]
        df["Row Group Guardrail"] = guardrail["Row groups per table"].iloc[0]
        df["Row Count Guardrail"] = (
            guardrail["Rows per table (millions)"].iloc[0] * 1000000
        )

        df["Parquet File Guardrail Hit"] = df["Files"] > df["Parquet File Guardrail"]
        df["Row Group Guardrail Hit"] = df["Row Groups"] > df["Row Group Guardrail"]
    if count_rows:
        df["Row Count"] = df["Row Count"].astype(int)
        df["Row Count Guardrail Hit"] = df["Row Count"] > df["Row Count Guardrail"]

    if export:
        if not lakehouse_attached():
            raise ValueError(
                f"{icons.red_dot} In order to save the report.json file, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook."
            )

        (current_lakehouse_name, current_lakehouse_id) = resolve_lakehouse_name_and_id()
        lakeTName = "lakehouse_table_details"
        lakeT_filt = df[df["Table Name"] == lakeTName]

        if len(lakeT_filt) == 0:
            run_id = 1
        else:
            max_run_id = _get_column_aggregate(
                lakehouse=current_lakehouse_name, table_name=lakeTName
            )
            run_id = max_run_id + 1

        export_df = df.copy()

        cols = [
            "Files",
            "Row Groups",
            "Row Count",
            "Table Size",
            "SKU",
            "Parquet File Guardrail",
            "Row Group Guardrail",
            "Row Count Guardrail",
            "Parquet File Guardrail Hit",
            "Row Group Guardrail Hit",
            "Row Count Guardrail Hit",
        ]

        for c in cols:
            if c not in export_df:
                if c in [
                    "Files",
                    "Row Groups",
                    "Row Count",
                    "Table Size",
                    "Parquet File Guardrail",
                    "Row Group Guardrail",
                    "Row Count Guardrail",
                ]:
                    export_df[c] = 0
                    export_df[c] = export_df[c].astype(int)
                elif c in ["SKU"]:
                    export_df[c] = None
                    export_df[c] = export_df[c].astype(str)
                elif c in [
                    "Parquet File Guardrail Hit",
                    "Row Group Guardrail Hit",
                    "Row Count Guardrail Hit",
                ]:
                    export_df[c] = False
                    export_df[c] = export_df[c].astype(bool)

        print(
            f"{icons.in_progress} Saving Lakehouse table properties to the '{lakeTName}' table in the lakehouse...\n"
        )
        export_df["Timestamp"] = datetime.datetime.now()
        export_df["RunId"] = run_id

        save_as_delta_table(
            dataframe=export_df, delta_table_name=lakeTName, write_mode="append"
        )

    return df
