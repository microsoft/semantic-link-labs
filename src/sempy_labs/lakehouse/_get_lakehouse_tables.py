import os
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime
from sempy_labs._helper_functions import (
    _get_column_aggregate,
    resolve_lakehouse_name_and_id,
    save_as_delta_table,
    resolve_workspace_id,
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
from sempy_labs.lakehouse._schemas import list_tables


@log
def get_lakehouse_tables(
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    extended: bool = False,
    count_rows: bool = False,
    export: bool = False,
    exclude_shortcuts: bool = False,
) -> pd.DataFrame:
    """
    Shows the tables of a lakehouse and their respective properties. Option to include additional properties relevant to Direct Lake guardrails.

    This function can be executed in either a PySpark or pure Python notebook.

    This is a wrapper function for the following API: `Tables - List Tables <https://learn.microsoft.com/rest/api/fabric/lakehouse/tables/list-tables>`_ plus extended capabilities.
    However, the above mentioned API does not support Lakehouse schemas (Preview) until it is in GA (General Availability). This version also supports schema
    enabled Lakehouses.

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
    exclude_shortcuts : bool, default=False
        If True, excludes shortcuts.

    Returns
    -------
    pandas.DataFrame
        Shows the tables/columns within a lakehouse and their properties.
    """

    workspace_id = resolve_workspace_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    df = list_tables(lakehouse=lakehouse, workspace=workspace)

    local_path = _mount(lakehouse=lakehouse_id, workspace=workspace_id)

    if extended:
        sku_value = get_sku_size(workspace_id)
        guardrail = get_directlake_guardrails_for_sku(sku_value)
        # Avoid mounting the lakehouse if is already mounted
        if not local_path:
            local_path = _mount(lakehouse=lakehouse_id, workspace=workspace_id)

        df["Files"], df["Row Groups"], df["Table Size"] = None, None, None
        if count_rows:
            df["Row Count"] = None

        for i, r in df.iterrows():
            schema_name = r["Schema Name"]
            table_name = r["Table Name"]
            if r["Type"] == "Managed" and r["Format"] == "delta":
                delta_table_path = (
                    create_abfss_path(
                        lakehouse_id, workspace_id, table_name, schema_name
                    )
                    .replace("//", "/")  # When schema_name = ""
                    .replace("abfss:/", "abfss://")  # Put back the // after abfss:
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
                            local_path, "Tables", schema_name, table_name, f
                        )

                        if os.path.exists(local_file_path):
                            size_in_bytes += os.path.getsize(local_file_path)
                    num_latest_files = len(latest_files)
                else:
                    delta_table = _get_delta_table(delta_table_path)

                    latest_files = _read_delta_table(delta_table_path).inputFiles()
                    table_df = delta_table.toDF()
                    table_details = delta_table.detail().collect()[0].asDict()
                    size_in_bytes = table_details.get("sizeInBytes", 0)
                    num_latest_files = table_details.get("numFiles", 0)

                table_path = os.path.join(local_path, "Tables", schema_name, table_name)

                file_paths = []
                for file in latest_files:
                    if _pure_python_notebook():
                        file_paths.append(file)
                    else:
                        # Append the <Partition folder>/<filename> or <filename>
                        find_table = file.find(table_name)
                        len_file = len(file)
                        len_table = len(table_name)
                        last_chars = len_file - (find_table + len_table + 1)
                        file_paths.append(file[-last_chars:])

                num_rowgroups = 0
                for filename in file_paths:
                    parquet_file_path = f"{table_path}/{filename}"
                    if os.path.exists(parquet_file_path):
                        parquet_file = pq.ParquetFile(parquet_file_path)
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

            # Set "Schema Name" = "dbo" when it is ""
            df.loc[df["Schema Name"] == "", "Schema Name"] = "dbo"

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

    if exclude_shortcuts:
        from sempy_labs.lakehouse._shortcuts import list_shortcuts

        # Exclude shortcuts
        shortcuts = (
            list_shortcuts(lakehouse=lakehouse, workspace=workspace)
            .query("`Shortcut Path`.str.startswith('/Tables')", engine="python")
            .assign(
                FullPath=lambda df: df["Shortcut Path"].str.rstrip("/")
                + "/"
                + df["Shortcut Name"]
            )["FullPath"]
            .tolist()
        )

        df["FullPath"] = df.apply(
            lambda x: (
                f"/Tables/{x['Table Name']}"
                if pd.isna(x["Schema Name"]) or x["Schema Name"] == ""
                else f"/Tables/{x['Schema Name']}/{x['Table Name']}"
            ),
            axis=1,
        )

        df = df[~df["FullPath"].isin(shortcuts)].reset_index(drop=True)

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
