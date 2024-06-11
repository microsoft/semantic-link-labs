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


def get_lakehouse_tables(
    lakehouse: Optional[str] = None,
    workspace: Optional[str] = None,
    extended: Optional[bool] = False,
    count_rows: Optional[bool] = False,
    export: Optional[bool] = False,
):
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

    if lakehouse == None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, workspace)
    else:
        lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)

    if count_rows:  # Setting countrows defaults to extended=True
        extended = True

    client = fabric.FabricRestClient()
    response = client.get(
        f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables"
    )

    for i in response.json()["data"]:
        tName = i["name"]
        tType = i["type"]
        tFormat = i["format"]
        tLocation = i["location"]
        if extended == False:
            new_data = {
                "Workspace Name": workspace,
                "Lakehouse Name": lakehouse,
                "Table Name": tName,
                "Format": tFormat,
                "Type": tType,
                "Location": tLocation,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        else:
            sku_value = get_sku_size(workspace)
            guardrail = get_directlake_guardrails_for_sku(sku_value)

            spark = SparkSession.builder.getOrCreate()

            intColumns = ["Files", "Row Groups", "Table Size"]
            if tType == "Managed" and tFormat == "delta":
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

                if count_rows:
                    num_rows = spark.table(tName).count()
                    intColumns.append("Row Count")
                    new_data = {
                        "Workspace Name": workspace,
                        "Lakehouse Name": lakehouse,
                        "Table Name": tName,
                        "Format": tFormat,
                        "Type": tType,
                        "Location": tLocation,
                        "Files": num_files,
                        "Row Groups": num_rowgroups,
                        "Row Count": num_rows,
                        "Table Size": size_in_bytes,
                    }
                else:
                    new_data = {
                        "Workspace Name": workspace,
                        "Lakehouse Name": lakehouse,
                        "Table Name": tName,
                        "Format": tFormat,
                        "Type": tType,
                        "Location": tLocation,
                        "Files": num_files,
                        "Row Groups": num_rowgroups,
                        "Table Size": size_in_bytes,
                    }

                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
                df[intColumns] = df[intColumns].astype(int)

            df["SKU"] = guardrail["Fabric SKUs"].iloc[0]
            df["Parquet File Guardrail"] = guardrail["Parquet files per table"].iloc[0]
            df["Row Group Guardrail"] = guardrail["Row groups per table"].iloc[0]
            df["Row Count Guardrail"] = (
                guardrail["Rows per table (millions)"].iloc[0] * 1000000
            )

            df["Parquet File Guardrail Hit"] = (
                df["Files"] > df["Parquet File Guardrail"]
            )
            df["Row Group Guardrail Hit"] = df["Row Groups"] > df["Row Group Guardrail"]

            if count_rows:
                df["Row Count Guardrail Hit"] = (
                    df["Row Count"] > df["Row Count Guardrail"]
                )

    if export:
        lakeAttach = lakehouse_attached()
        if lakeAttach == False:
            print(
                f"In order to save the report.json file, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook."
            )
            return
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
            f"Saving Lakehouse table properties to the '{lakeTName}' table in the lakehouse...\n"
        )
        now = datetime.datetime.now()
        export_df["Timestamp"] = now
        export_df["RunId"] = runId

        export_df.columns = export_df.columns.str.replace(" ", "_")
        spark_df = spark.createDataFrame(export_df)
        spark_df.write.mode("append").format("delta").saveAsTable(lakeTName)
        print(
            f"\u2022 Lakehouse table properties have been saved to the '{lakeTName}' delta table."
        )

    return df
