import os
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime
from sempy_labs._helper_functions import (
    _get_column_aggregate,
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    save_as_delta_table,
    _base_api,
    _create_dataframe,
    resolve_workspace_id,
    resolve_lakehouse_id,
    _read_delta_table,
    _get_delta_table,
    _mount,
    create_abfss_path,
    _pure_python_notebook,
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

    This function can be executed in either a PySpark or pure Python notebook.

    This is a wrapper function for the following API: `Tables - List Tables <https://learn.microsoft.com/rest/api/fabric/lakehouse/tables/list-tables>`_ plus extended capabilities.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    if (
        workspace_id != resolve_workspace_id()
        and lakehouse_id != resolve_lakehouse_id()
        and count_rows
    ):
        raise ValueError(
            f"{icons.red_dot} If 'count_rows' is set to True, you must run this function against the default lakehouse attached to the notebook. "
            "Count rows runs a spark query and cross-workspace spark queries are currently not supported."
        )

    responses = _base_api(
        request=f"v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables",
        uses_pagination=True,
        client="fabric_sp",
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
        sku_value = get_sku_size(workspace_id)
        guardrail = get_directlake_guardrails_for_sku(sku_value)
        local_path = _mount()

        df["Files"], df["Row Groups"], df["Table Size"] = None, None, None
        if count_rows:
            df["Row Count"] = None

        for i, r in df.iterrows():
            table_name = r["Table Name"]
            if r["Type"] == "Managed" and r["Format"] == "delta":
                delta_table_path = create_abfss_path(
                    lakehouse_id, workspace_id, table_name
                )

                if _pure_python_notebook():
                    from deltalake import DeltaTable

                    delta_table = DeltaTable(delta_table_path)
                    latest_files = [
                        file["path"]
                        for file in delta_table.get_add_actions().to_pylist()
                    ]
                    size_in_bytes = 0
                    for f in latest_files:
                        local_file_path = os.path.join(
                            local_path, "Tables", table_name, os.path.basename(f)
                        )
                        if os.path.exists(local_file_path):
                            size_in_bytes += os.path.getsize(local_file_path)
                    num_latest_files = len(latest_files)
                else:
                    delta_table = _get_delta_table(delta_table_path)
                    latest_files = _read_delta_table(delta_table_path).inputFiles()
                    table_df = delta_table.toDF()
                    table_details = delta_table.detail().collect()[0].asDict()
                    num_latest_files = table_details.get("numFiles", 0)
                    size_in_bytes = table_details.get("sizeInBytes", 0)

                table_path = os.path.join(local_path, "Tables", table_name)
                file_paths = [os.path.basename(f) for f in latest_files]

                num_rowgroups = 0
                for filename in file_paths:
                    parquet_file = pq.ParquetFile(f"{table_path}/{filename}")
                    num_rowgroups += parquet_file.num_row_groups
                df.at[i, "Files"] = num_latest_files
                df.at[i, "Row Groups"] = num_rowgroups
                df.at[i, "Table Size"] = size_in_bytes
            if count_rows:
                if _pure_python_notebook():
                    row_count = delta_table.to_pyarrow_table().num_rows
                else:
                    row_count = table_df.count()
                df.at[i, "Row Count"] = row_count

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
                f"{icons.red_dot} In order to save the dataframe, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook."
            )

        lake_table_name = "lakehouse_table_details"
        df_filt = df[df["Table Name"] == lake_table_name]

        if df_filt.empty:
            run_id = 1
        else:
            max_run_id = _get_column_aggregate(table_name=lake_table_name)
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
            f"{icons.in_progress} Saving Lakehouse table properties to the '{lake_table_name}' table in the lakehouse...\n"
        )
        export_df["Timestamp"] = datetime.now()
        export_df["RunId"] = run_id

        save_as_delta_table(
            dataframe=export_df, delta_table_name=lake_table_name, write_mode="append"
        )

    return df
