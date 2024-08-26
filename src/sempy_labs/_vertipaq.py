import sempy.fabric as fabric
import pandas as pd
from IPython.display import display, HTML
import zipfile
import os
import shutil
import datetime
import warnings
from pyspark.sql import SparkSession
from sempy_labs._helper_functions import (
    format_dax_object_name,
    resolve_lakehouse_name,
    resolve_dataset_id,
    save_as_delta_table,
    resolve_workspace_capacity,
)
from sempy_labs._list_functions import list_relationships
from sempy_labs.lakehouse import lakehouse_attached, get_lakehouse_tables
from sempy_labs.directlake import get_direct_lake_source
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def vertipaq_analyzer(
    dataset: str,
    workspace: Optional[str] = None,
    export: Optional[str] = None,
    read_stats_from_data: Optional[bool] = False,
    **kwargs,
):
    """
    Displays an HTML visualization of the Vertipaq Analyzer statistics from a semantic model.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name in which the semantic model exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    export : str, default=None
        Specifying 'zip' will export the results to a zip file in your lakehouse (which can be imported using the import_vertipaq_analyzer function.
        Specifying 'table' will export the results to delta tables (appended) in your lakehouse.
        Default value: None.
    read_stats_from_data : bool, default=False
        Setting this parameter to true has the function get Column Cardinality and Missing Rows using DAX (Direct Lake semantic models achieve this using a Spark query to the lakehouse).

    Returns
    -------

    """

    from sempy_labs.tom import connect_semantic_model

    if "lakehouse_workspace" in kwargs:
        print(
            f"{icons.info} The 'lakehouse_workspace' parameter has been deprecated as it is no longer necessary. Please remove this parameter from the function going forward."
        )
        del kwargs["lakehouse_workspace"]

    pd.options.mode.copy_on_write = True
    warnings.filterwarnings(
        "ignore", message="createDataFrame attempted Arrow optimization*"
    )

    workspace = fabric.resolve_workspace_name(workspace)

    dfT = fabric.list_tables(dataset=dataset, extended=True, workspace=workspace)
    dfT.rename(columns={"Name": "Table Name"}, inplace=True)
    dfC = fabric.list_columns(dataset=dataset, extended=True, workspace=workspace)
    dfC["Column Object"] = format_dax_object_name(dfC["Table Name"], dfC["Column Name"])
    dfC.rename(columns={"Column Cardinality": "Cardinality"}, inplace=True)
    dfH = fabric.list_hierarchies(dataset=dataset, extended=True, workspace=workspace)
    dfR = list_relationships(dataset=dataset, extended=True, workspace=workspace)
    dfR["From Object"] = format_dax_object_name(dfR["From Table"], dfR["From Column"])
    dfR["To Object"] = format_dax_object_name(dfR["To Table"], dfR["To Column"])
    dfP = fabric.list_partitions(dataset=dataset, extended=True, workspace=workspace)
    artifact_type, lakehouse_name, lakehouse_id, lakehouse_workspace_id = (
        get_direct_lake_source(dataset=dataset, workspace=workspace)
    )

    with connect_semantic_model(
        dataset=dataset, readonly=True, workspace=workspace
    ) as tom:
        compat_level = tom.model.Model.Database.CompatibilityLevel
        is_direct_lake = tom.is_direct_lake()
        def_mode = tom.model.DefaultMode
        table_count = tom.model.Tables.Count
        column_count = len(list(tom.all_columns()))

    dfR["Missing Rows"] = None

    # Direct Lake
    if read_stats_from_data:
        if is_direct_lake and artifact_type == "Lakehouse":
            dfC = pd.merge(
                dfC,
                dfP[["Table Name", "Query", "Source Type"]],
                on="Table Name",
                how="left",
            )
            dfC_flt = dfC[
                (dfC["Source Type"] == "Entity")
                & (~dfC["Column Name"].str.startswith("RowNumber-"))
            ]

            object_workspace = fabric.resolve_workspace_name(lakehouse_workspace_id)
            current_workspace_id = fabric.get_workspace_id()
            if current_workspace_id != lakehouse_workspace_id:
                lakeTables = get_lakehouse_tables(
                    lakehouse=lakehouse_name, workspace=object_workspace
                )

            sql_statements = []
            spark = SparkSession.builder.getOrCreate()
            # Loop through tables
            for lakeTName in dfC_flt["Query"].unique():
                query = "SELECT "
                columns_in_table = dfC_flt.loc[
                    dfC_flt["Query"] == lakeTName, "Source"
                ].unique()

                # Loop through columns within those tables
                for scName in columns_in_table:
                    query = query + f"COUNT(DISTINCT(`{scName}`)) AS `{scName}`, "

                query = query[:-2]
                if lakehouse_workspace_id == current_workspace_id:
                    query = query + f" FROM {lakehouse_name}.{lakeTName}"
                else:
                    lakeTables_filt = lakeTables[lakeTables["Table Name"] == lakeTName]
                    tPath = lakeTables_filt["Location"].iloc[0]

                    df = spark.read.format("delta").load(tPath)
                    tempTableName = "delta_table_" + lakeTName
                    df.createOrReplaceTempView(tempTableName)
                    query = query + f" FROM {tempTableName}"
                sql_statements.append((lakeTName, query))

            for o in sql_statements:
                tName = o[0]
                query = o[1]

                df = spark.sql(query)

                for column in df.columns:
                    x = df.collect()[0][column]
                    for i, r in dfC.iterrows():
                        if r["Query"] == tName and r["Source"] == column:
                            dfC.at[i, "Cardinality"] = x

            # Remove column added temporarily
            dfC.drop(columns=["Query", "Source Type"], inplace=True)

            # Direct Lake missing rows
            dfR = pd.merge(
                dfR,
                dfP[["Table Name", "Query"]],
                left_on="From Table",
                right_on="Table Name",
                how="left",
            )
            dfR.rename(columns={"Query": "From Lake Table"}, inplace=True)
            dfR.drop(columns=["Table Name"], inplace=True)
            dfR = pd.merge(
                dfR,
                dfP[["Table Name", "Query"]],
                left_on="To Table",
                right_on="Table Name",
                how="left",
            )
            dfR.rename(columns={"Query": "To Lake Table"}, inplace=True)
            dfR.drop(columns=["Table Name"], inplace=True)
            dfR = pd.merge(
                dfR,
                dfC[["Column Object", "Source"]],
                left_on="From Object",
                right_on="Column Object",
                how="left",
            )
            dfR.rename(columns={"Source": "From Lake Column"}, inplace=True)
            dfR.drop(columns=["Column Object"], inplace=True)
            dfR = pd.merge(
                dfR,
                dfC[["Column Object", "Source"]],
                left_on="To Object",
                right_on="Column Object",
                how="left",
            )
            dfR.rename(columns={"Source": "To Lake Column"}, inplace=True)
            dfR.drop(columns=["Column Object"], inplace=True)

            spark = SparkSession.builder.getOrCreate()
            for i, r in dfR.iterrows():
                fromTable = r["From Lake Table"]
                fromColumn = r["From Lake Column"]
                toTable = r["To Lake Table"]
                toColumn = r["To Lake Column"]

                if lakehouse_workspace_id == current_workspace_id:
                    query = f"select count(f.{fromColumn}) as {fromColumn}\nfrom {fromTable} as f\nleft join {toTable} as c on f.{fromColumn} = c.{toColumn}\nwhere c.{toColumn} is null"
                else:
                    tempTableFrom = f"delta_table_{fromTable}"
                    tempTableTo = f"delta_table_{toTable}"

                    query = f"select count(f.{fromColumn}) as {fromColumn}\nfrom {tempTableFrom} as f\nleft join {tempTableTo} as c on f.{fromColumn} = c.{toColumn}\nwhere c.{toColumn} is null"

                # query = f"select count(f.{fromColumn}) as {fromColumn}\nfrom {fromTable} as f\nleft join {toTable} as c on f.{fromColumn} = c.{toColumn}\nwhere c.{toColumn} is null"

                df = spark.sql(query)
                missingRows = df.collect()[0][0]
                dfR.at[i, "Missing Rows"] = missingRows

            dfR["Missing Rows"] = dfR["Missing Rows"].astype(int)
        elif not is_direct_lake:
            # Calculate missing rows using DAX for non-direct lake
            for i, r in dfR.iterrows():
                fromTable = r["From Table"]
                fromColumn = r["From Column"]
                toTable = r["To Table"]
                toColumn = r["To Column"]
                isActive = bool(r["Active"])
                fromObject = format_dax_object_name(fromTable, fromColumn)
                toObject = format_dax_object_name(toTable, toColumn)

                missingRows = 0

                query = f"evaluate\nsummarizecolumns(\n\"1\",calculate(countrows('{fromTable}'),isblank({toObject}))\n)"

                if isActive is False:  # add userelationship
                    query = f"evaluate\nsummarizecolumns(\n\"1\",calculate(countrows('{fromTable}'),userelationship({fromObject},{toObject}),isblank({toObject}))\n)"

                result = fabric.evaluate_dax(
                    dataset=dataset, dax_string=query, workspace=workspace
                )

                try:
                    missingRows = result.iloc[0, 0]
                except Exception:
                    pass

                dfR.at[i, "Missing Rows"] = missingRows
            dfR["Missing Rows"] = dfR["Missing Rows"].astype(int)

    dfTP = dfP.groupby("Table Name")["Partition Name"].count().reset_index()
    dfTP.rename(columns={"Partition Name": "Partitions"}, inplace=True)
    dfTC = dfC.groupby("Table Name")["Column Name"].count().reset_index()
    dfTC.rename(columns={"Column Name": "Columns"}, inplace=True)

    total_size = dfC["Total Size"].sum()
    table_sizes = dfC.groupby("Table Name")["Total Size"].sum().reset_index()
    table_sizes.rename(columns={"Total Size": "Table Size"}, inplace=True)

    # Columns
    dfC_filt = dfC[~dfC["Column Name"].str.startswith("RowNumber-")]
    dfC_filt["% DB"] = round((dfC_filt["Total Size"] / total_size) * 100, 2)
    dfC_filt = pd.merge(dfC_filt, table_sizes, on="Table Name", how="left")
    dfC_filt["% Table"] = round(
        (dfC_filt["Total Size"] / dfC_filt["Table Size"]) * 100, 2
    )
    columnList = [
        "Table Name",
        "Column Name",
        "Type",
        "Cardinality",
        "Total Size",
        "Data Size",
        "Dictionary Size",
        "Hierarchy Size",
        "% Table",
        "% DB",
        "Data Type",
        "Encoding",
        "Is Resident",
        "Temperature",
        "Last Accessed",
    ]

    colSize = dfC_filt[columnList].sort_values(by="Total Size", ascending=False)
    temp = dfC_filt[columnList].sort_values(by="Temperature", ascending=False)
    colSize.reset_index(drop=True, inplace=True)
    temp.reset_index(drop=True, inplace=True)

    export_Col = colSize.copy()

    intList = [
        "Cardinality",
        "Total Size",
        "Data Size",
        "Dictionary Size",
        "Hierarchy Size",
    ]
    pctList = ["% Table", "% DB"]
    colSize[intList] = colSize[intList].applymap("{:,}".format)
    temp[intList] = temp[intList].applymap("{:,}".format)
    colSize[pctList] = colSize[pctList].applymap("{:.2f}%".format)
    temp[pctList] = temp[pctList].applymap("{:.2f}%".format)

    # Tables
    intList = ["Total Size", "Data Size", "Dictionary Size", "Hierarchy Size"]
    dfCSum = dfC.groupby(["Table Name"])[intList].sum().reset_index()
    dfCSum["% DB"] = round((dfCSum["Total Size"] / total_size) * 100, 2)

    dfTable = pd.merge(
        dfT[["Table Name", "Type", "Row Count"]], dfCSum, on="Table Name", how="inner"
    )
    dfTable = pd.merge(dfTable, dfTP, on="Table Name", how="left")
    dfTable = pd.merge(dfTable, dfTC, on="Table Name", how="left")
    dfTable = dfTable.sort_values(by="Total Size", ascending=False)
    dfTable.reset_index(drop=True, inplace=True)
    export_Table = dfTable.copy()

    intList.extend(["Row Count", "Partitions", "Columns"])
    dfTable[intList] = dfTable[intList].applymap("{:,}".format)
    pctList = ["% DB"]
    dfTable[pctList] = dfTable[pctList].applymap("{:.2f}%".format)

    #  Relationships
    # dfR.drop(columns=['Max From Cardinality', 'Max To Cardinality'], inplace=True)
    dfR = pd.merge(
        dfR,
        dfC[["Column Object", "Cardinality"]],
        left_on="From Object",
        right_on="Column Object",
        how="left",
    )
    dfR.rename(columns={"Cardinality": "Max From Cardinality"}, inplace=True)
    dfR = pd.merge(
        dfR,
        dfC[["Column Object", "Cardinality"]],
        left_on="To Object",
        right_on="Column Object",
        how="left",
    )
    dfR.rename(columns={"Cardinality": "Max To Cardinality"}, inplace=True)
    dfR = dfR[
        [
            "From Object",
            "To Object",
            "Multiplicity",
            "Used Size",
            "Max From Cardinality",
            "Max To Cardinality",
            "Missing Rows",
        ]
    ].sort_values(by="Used Size", ascending=False)
    dfR.reset_index(drop=True, inplace=True)
    export_Rel = dfR.copy()
    intList = [
        "Used Size",
        "Max From Cardinality",
        "Max To Cardinality",
        "Missing Rows",
    ]
    if read_stats_from_data is False:
        intList.remove("Missing Rows")
    dfR[intList] = dfR[intList].applymap("{:,}".format)

    # Partitions
    dfP = dfP[
        [
            "Table Name",
            "Partition Name",
            "Mode",
            "Record Count",
            "Segment Count",
            # "Records per Segment",
        ]
    ].sort_values(by="Record Count", ascending=False)
    dfP["Records per Segment"] = round(
        dfP["Record Count"] / dfP["Segment Count"], 2
    )  # Remove after records per segment is fixed
    dfP.reset_index(drop=True, inplace=True)
    export_Part = dfP.copy()
    intList = ["Record Count", "Segment Count", "Records per Segment"]
    dfP[intList] = dfP[intList].applymap("{:,}".format)

    # Hierarchies
    dfH_filt = dfH[dfH["Level Ordinal"] == 0]
    dfH_filt = dfH_filt[["Table Name", "Hierarchy Name", "Used Size"]].sort_values(
        by="Used Size", ascending=False
    )
    dfH_filt.reset_index(drop=True, inplace=True)
    dfH_filt.fillna({"Used Size": 0}, inplace=True)
    dfH_filt["Used Size"] = dfH_filt["Used Size"].astype(int)
    export_Hier = dfH_filt.copy()
    intList = ["Used Size"]
    dfH_filt[intList] = dfH_filt[intList].applymap("{:,}".format)

    # Model
    if total_size >= 1000000000:
        y = total_size / (1024**3) * 1000000000
    elif total_size >= 1000000:
        y = total_size / (1024**2) * 1000000
    elif total_size >= 1000:
        y = total_size / (1024) * 1000
    y = round(y)

    dfModel = pd.DataFrame(
        {
            "Dataset Name": dataset,
            "Total Size": y,
            "Table Count": table_count,
            "Column Count": column_count,
            "Compatibility Level": compat_level,
            "Default Mode": def_mode,
        },
        index=[0],
    )
    dfModel.reset_index(drop=True, inplace=True)
    dfModel["Default Mode"] = dfModel["Default Mode"].astype(str)
    export_Model = dfModel.copy()
    intList = ["Total Size", "Table Count", "Column Count"]
    dfModel[intList] = dfModel[intList].applymap("{:,}".format)

    dataFrames = {
        "dfModel": dfModel,
        "dfTable": dfTable,
        "dfP": dfP,
        "colSize": colSize,
        "temp": temp,
        "dfR": dfR,
        "dfH_filt": dfH_filt,
    }

    dfs = {}
    for fileName, df in dataFrames.items():
        dfs[fileName] = df

    visualize_vertipaq(dfs)

    # Export vertipaq to delta tables in lakehouse
    if export in ["table", "zip"]:
        if not lakehouse_attached():
            raise ValueError(
                f"{icons.red_dot} In order to save the Vertipaq Analyzer results, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook."
            )

    if export == "table":
        spark = SparkSession.builder.getOrCreate()

        lakehouse_id = fabric.get_lakehouse_id()
        lake_workspace = fabric.resolve_workspace_name()
        lakehouse = resolve_lakehouse_name(
            lakehouse_id=lakehouse_id, workspace=lake_workspace
        )
        lakeTName = "vertipaq_analyzer_model"

        lakeT = get_lakehouse_tables(lakehouse=lakehouse, workspace=lake_workspace)
        lakeT_filt = lakeT[lakeT["Table Name"] == lakeTName]

        query = f"SELECT MAX(RunId) FROM {lakehouse}.{lakeTName}"

        if len(lakeT_filt) == 0:
            runId = 1
        else:
            dfSpark = spark.sql(query)
            maxRunId = dfSpark.collect()[0][0]
            runId = maxRunId + 1

        dfMap = {
            "export_Col": ["Columns", export_Col],
            "export_Table": ["Tables", export_Table],
            "export_Part": ["Partitions", export_Part],
            "export_Rel": ["Relationships", export_Rel],
            "export_Hier": ["Hierarchies", export_Hier],
            "export_Model": ["Model", export_Model],
        }

        print(
            f"{icons.in_progress} Saving Vertipaq Analyzer to delta tables in the lakehouse...\n"
        )
        now = datetime.datetime.now()
        dfD = fabric.list_datasets(workspace=workspace, mode="rest")
        dfD_filt = dfD[dfD["Dataset Name"] == dataset]
        configured_by = dfD_filt["Configured By"].iloc[0]
        capacity_id, capacity_name = resolve_workspace_capacity(workspace=workspace)

        for key, (obj, df) in dfMap.items():
            df["Capacity Name"] = capacity_name
            df["Capacity Id"] = capacity_id
            df["Configured By"] = configured_by
            df["Workspace Name"] = workspace
            df["Workspace Id"] = fabric.resolve_workspace_id(workspace)
            df["Dataset Name"] = dataset
            df["Dataset Id"] = resolve_dataset_id(dataset, workspace)
            df["RunId"] = runId
            df["Timestamp"] = now

            colName = "Capacity Name"
            df.insert(0, colName, df.pop(colName))
            colName = "Capacity Id"
            df.insert(1, colName, df.pop(colName))
            colName = "Workspace Name"
            df.insert(2, colName, df.pop(colName))
            colName = "Workspace Id"
            df.insert(3, colName, df.pop(colName))
            colName = "Dataset Name"
            df.insert(4, colName, df.pop(colName))
            colName = "Dataset Id"
            df.insert(5, colName, df.pop(colName))
            colName = "Configured By"
            df.insert(6, colName, df.pop(colName))

            df.columns = df.columns.str.replace(" ", "_")

            delta_table_name = f"VertipaqAnalyzer_{obj}".lower()
            save_as_delta_table(
                dataframe=df,
                delta_table_name=delta_table_name,
                write_mode="append",
                merge_schema=True,
            )

    # Export vertipaq to zip file within the lakehouse
    if export == "zip":
        dataFrames = {
            "dfModel": dfModel,
            "dfTable": dfTable,
            "dfP": dfP,
            "colSize": colSize,
            "temp": temp,
            "dfR": dfR,
            "dfH_filt": dfH_filt,
        }

        zipFileName = f"{workspace}.{dataset}.zip"

        folderPath = "/lakehouse/default/Files"
        subFolderPath = os.path.join(folderPath, "VertipaqAnalyzer")
        ext = ".csv"
        if not os.path.exists(subFolderPath):
            os.makedirs(subFolderPath, exist_ok=True)
        zipFilePath = os.path.join(subFolderPath, zipFileName)

        # Create CSV files based on dataframes
        for fileName, df in dataFrames.items():
            filePath = os.path.join(subFolderPath, f"{fileName}{ext}")
            df.to_csv(filePath, index=False)

        # Create a zip file and add CSV files to it
        with zipfile.ZipFile(zipFilePath, "w") as zipf:
            for fileName in dataFrames:
                filePath = os.path.join(subFolderPath, f"{fileName}{ext}")
                zipf.write(filePath, os.path.basename(filePath))

        # Clean up: remove the individual CSV files
        for fileName, df in dataFrames.items():
            filePath = os.path.join(subFolderPath, fileName) + ext
            if os.path.exists(filePath):
                os.remove(filePath)
        print(
            f"{icons.green_dot} The Vertipaq Analyzer info for the '{dataset}' semantic model in the '{workspace}' workspace has been saved "
            f"to the 'Vertipaq Analyzer/{zipFileName}' in the default lakehouse attached to this notebook."
        )


def visualize_vertipaq(dataframes):

    # Tooltips for columns within the visual
    data = [
        {
            "ViewName": "Model",
            "ColumnName": "Dataset Name",
            "Tooltip": "The name of the semantic model",
        },
        {
            "ViewName": "Model",
            "ColumnName": "Total Size",
            "Tooltip": "The size of the model (in bytes)",
        },
        {
            "ViewName": "Model",
            "ColumnName": "Table Count",
            "Tooltip": "The number of tables in the semantic model",
        },
        {
            "ViewName": "Model",
            "ColumnName": "Column Count",
            "Tooltip": "The number of columns in the semantic model",
        },
        {
            "ViewName": "Model",
            "ColumnName": "Compatibility Level",
            "Tooltip": "The compatibility level of the semantic model",
        },
        {
            "ViewName": "Model",
            "ColumnName": "Default Mode",
            "Tooltip": "The default query mode of the semantic model",
        },
        {
            "ViewName": "Table",
            "ColumnName": "Table Name",
            "Tooltip": "The name of the table",
        },
        {"ViewName": "Table", "ColumnName": "Type", "Tooltip": "The type of table"},
        {
            "ViewName": "Table",
            "ColumnName": "Row Count",
            "Tooltip": "The number of rows in the table",
        },
        {
            "ViewName": "Table",
            "ColumnName": "Total Size",
            "Tooltip": "Data Size + Dictionary Size + Hierarchy Size (in bytes)",
        },
        {
            "ViewName": "Table",
            "ColumnName": "Data Size",
            "Tooltip": "The size of the data for all the columns in this table (in bytes)",
        },
        {
            "ViewName": "Table",
            "ColumnName": "Dictionary Size",
            "Tooltip": "The size of the column's dictionary for all columns in this table (in bytes)",
        },
        {
            "ViewName": "Table",
            "ColumnName": "Hierarchy Size",
            "Tooltip": "The size of hierarchy structures for all columns in this table (in bytes)",
        },
        {
            "ViewName": "Table",
            "ColumnName": "% DB",
            "Tooltip": "The size of the table relative to the size of the semantic model",
        },
        {
            "ViewName": "Table",
            "ColumnName": "Partitions",
            "Tooltip": "The number of partitions in the table",
        },
        {
            "ViewName": "Table",
            "ColumnName": "Columns",
            "Tooltip": "The number of columns in the table",
        },
        {
            "ViewName": "Partition",
            "ColumnName": "Table Name",
            "Tooltip": "The name of the table",
        },
        {
            "ViewName": "Partition",
            "ColumnName": "Partition Name",
            "Tooltip": "The name of the partition within the table",
        },
        {
            "ViewName": "Partition",
            "ColumnName": "Mode",
            "Tooltip": "The query mode of the partition",
        },
        {
            "ViewName": "Partition",
            "ColumnName": "Record Count",
            "Tooltip": "The number of rows in the partition",
        },
        {
            "ViewName": "Partition",
            "ColumnName": "Segment Count",
            "Tooltip": "The number of segments within the partition",
        },
        {
            "ViewName": "Partition",
            "ColumnName": "Records per Segment",
            "Tooltip": "The number of rows per segment",
        },
        {
            "ViewName": "Column",
            "ColumnName": "Table Name",
            "Tooltip": "The name of the table",
        },
        {
            "ViewName": "Column",
            "ColumnName": "Column Name",
            "Tooltip": "The name of the column",
        },
        {"ViewName": "Column", "ColumnName": "Type", "Tooltip": "The type of column"},
        {
            "ViewName": "Column",
            "ColumnName": "Cardinality",
            "Tooltip": "The number of unique rows in the column",
        },
        {
            "ViewName": "Column",
            "ColumnName": "Total Size",
            "Tooltip": "Data Size + Dictionary Size + Hierarchy Size (in bytes)",
        },
        {
            "ViewName": "Column",
            "ColumnName": "Data Size",
            "Tooltip": "The size of the data for the column (in bytes)",
        },
        {
            "ViewName": "Column",
            "ColumnName": "Dictionary Size",
            "Tooltip": "The size of the column's dictionary (in bytes)",
        },
        {
            "ViewName": "Column",
            "ColumnName": "Hierarchy Size",
            "Tooltip": "The size of hierarchy structures (in bytes)",
        },
        {
            "ViewName": "Column",
            "ColumnName": "% Table",
            "Tooltip": "The size of the column relative to the size of the table",
        },
        {
            "ViewName": "Column",
            "ColumnName": "% DB",
            "Tooltip": "The size of the column relative to the size of the semantic model",
        },
        {
            "ViewName": "Column",
            "ColumnName": "Data Type",
            "Tooltip": "The data type of the column",
        },
        {
            "ViewName": "Column",
            "ColumnName": "Encoding",
            "Tooltip": "The encoding type for the column",
        },
        {
            "ViewName": "Column",
            "ColumnName": "Is Resident",
            "Tooltip": "Indicates whether the column is in memory or not",
        },
        {
            "ViewName": "Column",
            "ColumnName": "Temperature",
            "Tooltip": "A decimal indicating the frequency and recency of queries against the column",
        },
        {
            "ViewName": "Column",
            "ColumnName": "Last Accessed",
            "Tooltip": "The time the column was last queried",
        },
        {
            "ViewName": "Hierarchy",
            "ColumnName": "Table Name",
            "Tooltip": "The name of the table",
        },
        {
            "ViewName": "Hierarchy",
            "ColumnName": "Hierarchy Name",
            "Tooltip": "The name of the hierarchy",
        },
        {
            "ViewName": "Hierarchy",
            "ColumnName": "Used Size",
            "Tooltip": "The size of user hierarchy structures (in bytes)",
        },
        {
            "ViewName": "Relationship",
            "ColumnName": "From Object",
            "Tooltip": "The from table/column in the relationship",
        },
        {
            "ViewName": "Relationship",
            "ColumnName": "To Object",
            "Tooltip": "The to table/column in the relationship",
        },
        {
            "ViewName": "Relationship",
            "ColumnName": "Multiplicity",
            "Tooltip": "The cardinality on each side of the relationship",
        },
        {
            "ViewName": "Relationship",
            "ColumnName": "Used Size",
            "Tooltip": "The size of the relationship (in bytes)",
        },
        {
            "ViewName": "Relationship",
            "ColumnName": "Max From Cardinality",
            "Tooltip": "The number of unique values in the column used in the from side of the relationship",
        },
        {
            "ViewName": "Relationship",
            "ColumnName": "Max To Cardinality",
            "Tooltip": "The number of unique values in the column used in the to side of the relationship",
        },
        {
            "ViewName": "Relationship",
            "ColumnName": "Missing Rows",
            "Tooltip": "The number of rows in the 'from' table which do not map to the key column in the 'to' table",
        },
    ]

    # Create DataFrame
    tooltipDF = pd.DataFrame(data)

    # define the dictionary with {"Tab name":df}
    df_dict = {
        "Model Summary": dataframes["dfModel"],
        "Tables": dataframes["dfTable"],
        "Partitions": dataframes["dfP"],
        "Columns (Total Size)": dataframes["colSize"],
        "Columns (Temperature)": dataframes["temp"],
        "Relationships": dataframes["dfR"],
        "Hierarchies": dataframes["dfH_filt"],
    }

    mapping = {
        "Model Summary": "Model",
        "Tables": "Table",
        "Partitions": "Partition",
        "Columns (Total Size)": "Column",
        "Columns (Temperature)": "Column",
        "Relationships": "Relationship",
        "Hierarchies": "Hierarchy",
    }

    # Basic styles for the tabs and tab content
    styles = """
    <style>
        .tab { overflow: hidden; border: 1px solid #ccc; background-color: #f1f1f1; }
        .tab button { background-color: inherit; float: left; border: none; outline: none; cursor: pointer; padding: 14px 16px; transition: 0.3s; }
        .tab button:hover { background-color: #ddd; }
        .tab button.active { background-color: #ccc; }
        .tabcontent { display: none; padding: 6px 12px; border: 1px solid #ccc; border-top: none; }
    </style>
    """
    # JavaScript for tab functionality
    script = """
    <script>
    function openTab(evt, tabName) {
        var i, tabcontent, tablinks;
        tabcontent = document.getElementsByClassName("tabcontent");
        for (i = 0; i < tabcontent.length; i++) {
            tabcontent[i].style.display = "none";
        }
        tablinks = document.getElementsByClassName("tablinks");
        for (i = 0; i < tablinks.length; i++) {
            tablinks[i].className = tablinks[i].className.replace(" active", "");
        }
        document.getElementById(tabName).style.display = "block";
        evt.currentTarget.className += " active";
    }
    </script>
    """

    # HTML for tabs
    tab_html = '<div class="tab">'
    content_html = ""
    for i, (title, df) in enumerate(df_dict.items()):
        tab_id = f"tab{i}"
        tab_html += f'<button class="tablinks" onclick="openTab(event, \'{tab_id}\')">{title}</button>'

        vw = mapping.get(title)

        df_html = df.to_html()
        for col in df.columns:
            tt = None
            try:
                tooltipDF_filt = tooltipDF[
                    (tooltipDF["ViewName"] == vw) & (tooltipDF["ColumnName"] == col)
                ]
                tt = tooltipDF_filt["Tooltip"].iloc[0]
            except Exception:
                pass
            df_html = df_html.replace(f"<th>{col}</th>", f'<th title="{tt}">{col}</th>')
        content_html += (
            f'<div id="{tab_id}" class="tabcontent"><h3>{title}</h3>{df_html}</div>'
        )
    tab_html += "</div>"

    # Display the tabs, tab contents, and run the script
    display(HTML(styles + tab_html + content_html + script))
    # Default to open the first tab
    display(
        HTML("<script>document.getElementsByClassName('tablinks')[0].click();</script>")
    )


@log
def import_vertipaq_analyzer(folder_path: str, file_name: str):
    """
    Imports and visualizes the vertipaq analyzer info from a saved .zip file in your lakehouse.

    Parameters
    ----------
    folder_path : str
        The folder within your lakehouse in which the .zip file containing the vertipaq analyzer info has been saved.
    file_name : str
        The file name of the file which contains the vertipaq analyzer info.

    Returns
    -------
    str
       A visualization of the Vertipaq Analyzer statistics.
    """

    pd.options.mode.copy_on_write = True

    zipFilePath = os.path.join(folder_path, file_name)
    extracted_dir = os.path.join(folder_path, "extracted_dataframes")

    with zipfile.ZipFile(zipFilePath, "r") as zip_ref:
        zip_ref.extractall(extracted_dir)

    # Read all CSV files into a dictionary of DataFrames
    dfs = {}
    for file_name in zip_ref.namelist():
        df = pd.read_csv(extracted_dir + "/" + file_name)
        dfs[file_name] = df

    visualize_vertipaq(dfs)

    # Clean up: remove the extracted directory
    shutil.rmtree(extracted_dir)
