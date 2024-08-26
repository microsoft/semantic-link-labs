import sempy.fabric as fabric
import pandas as pd
from pyspark.sql import SparkSession
import pyarrow.parquet as pq
import datetime
from sempy_labs._helper_functions import (
    resolve_lakehouse_id,
    resolve_lakehouse_name,
    resolve_workspace_name_and_id,
)
from sempy_labs.directlake._guardrails import (
    get_sku_size,
    get_directlake_guardrails_for_sku,
)
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
from typing import Optional
import sempy_labs._icons as icons
from sempy._utils._log import log
from sempy.fabric.exceptions import FabricHTTPException


@log
def get_lakehouse_tables(
    lakehouse: Optional[str] = None,
    workspace: Optional[str] = None,
    extended: Optional[bool] = False,
    count_rows: Optional[bool] = False,
    export: Optional[bool] = False,
) -> pd.DataFrame:
    """
    Shows the tables of a lakehouse and their respective properties. Option to include additional properties relevant to Direct Lake guardrails.

    Parameters
    ----------
    lakehouse : str, default=None
        The Fabric lakehouse.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str, default=None
        The Fabric workspace used by the lakehouse.
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

    from sempy_labs._helper_functions import pagination

    df = pd.DataFrame(
        columns=[
            "Workspace Name",
            "Lakehouse Name",
            "Table Name",
            "Format",
            "Type",
            "Location",
        ]
    )

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, workspace)
    else:
        lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)

    if count_rows:  # Setting countrows defaults to extended=True
        extended = True

    if (
        workspace_id != fabric.get_workspace_id()
        and lakehouse_id != fabric.get_lakehouse_id()
        and count_rows
    ):
        raise ValueError(
            f"{icons.red_dot} If 'count_rows' is set to True, you must run this function against the default lakehouse attached to the notebook. "
            "Count rows runs a spark query and cross-workspace spark queries are currently not supported."
        )

    client = fabric.FabricRestClient()
    response = client.get(
        f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    dfs = []
    for r in responses:
        for i in r.get("data", []):
            new_data = {
                "Workspace Name": workspace,
                "Lakehouse Name": lakehouse,
                "Table Name": i.get("name"),
                "Format": i.get("format"),
                "Type": i.get("type"),
                "Location": i.get("location"),
            }
            dfs.append(pd.DataFrame(new_data, index=[0]))
    df = pd.concat(dfs, ignore_index=True)

    if extended:
        sku_value = get_sku_size(workspace)
        guardrail = get_directlake_guardrails_for_sku(sku_value)
        spark = SparkSession.builder.getOrCreate()
        df["Files"] = None
        df["Row Groups"] = None
        df["Table Size"] = None
        if count_rows:
            df["Row Count"] = None
        for i, r in df.iterrows():
            tName = r["Table Name"]
            if r["Type"] == "Managed" and r["Format"] == "delta":
                detail_df = spark.sql(f"DESCRIBE DETAIL `{tName}`").collect()[0]
                num_files = detail_df.numFiles
                size_in_bytes = detail_df.sizeInBytes

                delta_table_path = f"Tables/{tName}"
                latest_files = (
                    spark.read.format("delta").load(delta_table_path).inputFiles()
                )
                file_paths = [f.split("/")[-1] for f in latest_files]

                # Handle FileNotFoundError
                num_rowgroups = 0
                for filename in file_paths:
                    try:
                        num_rowgroups += pq.ParquetFile(
                            f"/lakehouse/default/{delta_table_path}/{filename}"
                        ).num_row_groups
                    except FileNotFoundError:
                        continue
                df.at[i, "Files"] = num_files
                df.at[i, "Row Groups"] = num_rowgroups
                df.at[i, "Table Size"] = size_in_bytes
            if count_rows:
                num_rows = spark.table(tName).count()
                df.at[i, "Row Count"] = num_rows

    if extended:
        intColumns = ["Files", "Row Groups", "Table Size"]
        df[intColumns] = df[intColumns].astype(int)
        df["SKU"] = guardrail["Fabric SKUs"].iloc[0]
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

        spark = SparkSession.builder.getOrCreate()

        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(
            lakehouse_id=lakehouse_id, workspace=workspace
        )
        lakeTName = "lakehouse_table_details"
        lakeT_filt = df[df["Table Name"] == lakeTName]

        query = f"SELECT MAX(RunId) FROM {lakehouse}.{lakeTName}"

        if len(lakeT_filt) == 0:
            runId = 1
        else:
            dfSpark = spark.sql(query)
            maxRunId = dfSpark.collect()[0][0]
            runId = maxRunId + 1

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
        now = datetime.datetime.now()
        export_df["Timestamp"] = now
        export_df["RunId"] = runId

        export_df.columns = export_df.columns.str.replace(" ", "_")
        spark_df = spark.createDataFrame(export_df)
        spark_df.write.mode("append").format("delta").saveAsTable(lakeTName)
        print(
            f"{icons.bullet} Lakehouse table properties have been saved to the '{lakeTName}' delta table."
        )

    return df
