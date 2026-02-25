import sempy.fabric as fabric
import pandas as pd
from IPython.display import display, HTML
import zipfile
import os
import uuid
import shutil
import datetime
from sempy_labs._helper_functions import (
    format_dax_object_name,
    save_as_delta_table,
    resolve_workspace_capacity,
    _get_column_aggregate,
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
    _create_spark_session,
    resolve_workspace_id,
    resolve_workspace_name,
)
from sempy_labs._list_functions import list_relationships, list_tables
from sempy_labs.lakehouse import lakehouse_attached, get_lakehouse_tables
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from pathlib import Path
from uuid import UUID


@log
def vertipaq_analyzer(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    export: Optional[str] = None,
    read_stats_from_data: bool = False,
) -> dict[str, pd.DataFrame]:
    """
    Displays an HTML visualization of the `Vertipaq Analyzer <https://www.sqlbi.com/tools/vertipaq-analyzer/>`_ statistics from a semantic model.

    `Vertipaq Analyzer <https://www.sqlbi.com/tools/vertipaq-analyzer/>`_ is an open-sourced tool built by SQLBI. It provides a detailed analysis of the VertiPaq engine, which is the in-memory engine used by Power BI and Analysis Services Tabular models.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str| uuid.UUID, default=None
        The Fabric workspace name or ID in which the semantic model exists.
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
    dict[str, pandas.DataFrame]
        A dictionary of pandas dataframes showing the vertipaq analyzer statistics.
    """

    from sempy_labs.tom import connect_semantic_model

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    fabric.refresh_tom_cache(workspace=workspace)

    vertipaq_map = {
        "Model": {
            "Dataset Name": [icons.data_type_string, icons.no_format],
            "Total Size": [icons.data_type_long, icons.int_format],
            "Table Count": [icons.data_type_long, icons.int_format],
            "Column Count": [icons.data_type_long, icons.int_format],
            "Compatibility Level": [icons.data_type_long, icons.no_format],
            "Default Mode": [icons.data_type_string, icons.no_format],
        },
        "Tables": {
            "Table Name": [icons.data_type_string, icons.no_format],
            "Type": [icons.data_type_string, icons.no_format],
            "Row Count": [icons.data_type_long, icons.int_format],
            "Total Size": [icons.data_type_long, icons.int_format],
            "Dictionary Size": [icons.data_type_long, icons.int_format],
            "Data Size": [icons.data_type_long, icons.int_format],
            "Hierarchy Size": [icons.data_type_long, icons.int_format],
            "Relationship Size": [icons.data_type_long, icons.int_format],
            "User Hierarchy Size": [icons.data_type_long, icons.int_format],
            "Partitions": [icons.data_type_long, icons.int_format],
            "Columns": [icons.data_type_long, icons.int_format],
            "% DB": [icons.data_type_double, icons.pct_format],
        },
        "Partitions": {
            "Table Name": [icons.data_type_string, icons.no_format],
            "Partition Name": [icons.data_type_string, icons.no_format],
            "Mode": [icons.data_type_string, icons.no_format],
            "Record Count": [icons.data_type_long, icons.int_format],
            "Segment Count": [icons.data_type_long, icons.int_format],
            "Records per Segment": [icons.data_type_double, icons.int_format],
        },
        "Columns": {
            "Table Name": [icons.data_type_string, icons.no_format],
            "Column Name": [icons.data_type_string, icons.no_format],
            "Type": [icons.data_type_string, icons.no_format],
            "Cardinality": [icons.data_type_long, icons.int_format],
            "Total Size": [icons.data_type_long, icons.int_format],
            "Data Size": [icons.data_type_long, icons.int_format],
            "Dictionary Size": [icons.data_type_long, icons.int_format],
            "Hierarchy Size": [icons.data_type_long, icons.int_format],
            "% Table": [icons.data_type_double, icons.pct_format],
            "% DB": [icons.data_type_double, icons.pct_format],
            "Data Type": [icons.data_type_string, icons.no_format],
            "Encoding": [icons.data_type_string, icons.no_format],
            "Is Resident": [icons.data_type_bool, icons.no_format],
            "Temperature": [icons.data_type_double, icons.int_format],
            "Last Accessed": [icons.data_type_timestamp, icons.no_format],
        },
        "Hierarchies": {
            "Table Name": [icons.data_type_string, icons.no_format],
            "Hierarchy Name": [icons.data_type_string, icons.no_format],
            "Used Size": [icons.data_type_long, icons.int_format],
        },
        "Relationships": {
            "From Object": [icons.data_type_string, icons.no_format],
            "To Object": [icons.data_type_string, icons.no_format],
            "Multiplicity": [icons.data_type_string, icons.no_format],
            "Used Size": [icons.data_type_long, icons.int_format],
            "Max From Cardinality": [icons.data_type_long, icons.int_format],
            "Max To Cardinality": [icons.data_type_long, icons.int_format],
            "Missing Rows": [icons.data_type_long, icons.int_format],
        },
    }

    with connect_semantic_model(
        dataset=dataset_id, workspace=workspace_id, readonly=True
    ) as tom:
        compat_level = tom.model.Model.Database.CompatibilityLevel
        is_direct_lake = tom.is_direct_lake()
        def_mode = tom.model.DefaultMode
        table_count = tom.model.Tables.Count
        column_count = len(list(tom.all_columns()))
        if table_count == 0:
            print(
                f"{icons.warning} The '{dataset_name}' semantic model within the '{workspace_name}' workspace has no tables. Vertipaq Analyzer can only be run if the semantic model has tables."
            )
            return

    dfT = list_tables(dataset=dataset_id, extended=True, workspace=workspace_id)

    dfT.rename(columns={"Name": "Table Name"}, inplace=True)
    columns_to_keep = list(vertipaq_map["Tables"].keys())
    dfT = dfT[dfT.columns.intersection(columns_to_keep)]

    dfC = fabric.list_columns(dataset=dataset_id, extended=True, workspace=workspace_id)
    dfC["Column Object"] = format_dax_object_name(dfC["Table Name"], dfC["Column Name"])
    dfC.rename(columns={"Column Cardinality": "Cardinality"}, inplace=True)
    dfH = fabric.list_hierarchies(
        dataset=dataset_id, extended=True, workspace=workspace_id
    )
    dfR = list_relationships(dataset=dataset_id, extended=True, workspace=workspace_id)
    dfP = fabric.list_partitions(
        dataset=dataset_id, extended=True, workspace=workspace_id
    )

    artifact_type = None
    lakehouse_workspace_id = None
    lakehouse_name = None
    # if is_direct_lake:
    #    artifact_type, lakehouse_name, lakehouse_id, lakehouse_workspace_id = (
    #        get_direct_lake_source(dataset=dataset_id, workspace=workspace_id)
    #    )

    dfR["Missing Rows"] = 0
    dfR["Missing Rows"] = dfR["Missing Rows"].astype(int)

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

            object_workspace = resolve_workspace_name(
                workspace_id=lakehouse_workspace_id
            )
            current_workspace_id = resolve_workspace_id()
            if current_workspace_id != lakehouse_workspace_id:
                lakeTables = get_lakehouse_tables(
                    lakehouse=lakehouse_name, workspace=object_workspace
                )

            sql_statements = []
            spark = _create_spark_session()
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

            spark = _create_spark_session()
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

                if not isActive:
                    query = f"evaluate\nsummarizecolumns(\n\"1\",calculate(countrows('{fromTable}'),userelationship({fromObject},{toObject}),isblank({toObject}))\n)"

                result = fabric.evaluate_dax(
                    dataset=dataset_id, dax_string=query, workspace=workspace_id
                )

                try:
                    missingRows = result.iloc[0, 0]
                except Exception:
                    pass

                dfR.at[i, "Missing Rows"] = missingRows
            dfR["Missing Rows"] = dfR["Missing Rows"].astype(int)

    table_totals = dfC.groupby("Table Name")["Total Size"].transform("sum")
    db_total_size = dfC["Total Size"].sum()
    dfC["% Table"] = round((dfC["Total Size"] / table_totals) * 100, 2)
    dfC["% DB"] = round((dfC["Total Size"] / db_total_size) * 100, 2)
    columnList = list(vertipaq_map["Columns"].keys())

    dfC = dfC[dfC["Type"] != "RowNumber"].reset_index(drop=True)

    colSize = dfC[columnList].sort_values(by="Total Size", ascending=False)
    temp = dfC[columnList].sort_values(by="Temperature", ascending=False)
    colSize.reset_index(drop=True, inplace=True)
    temp.reset_index(drop=True, inplace=True)

    export_Col = colSize.copy()
    export_Table = dfT.copy()

    #  Relationships
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

    # Hierarchies
    dfH_filt = dfH[dfH["Level Ordinal"] == 0]
    dfH_filt = dfH_filt[["Table Name", "Hierarchy Name", "Used Size"]].sort_values(
        by="Used Size", ascending=False
    )
    dfH_filt.reset_index(drop=True, inplace=True)
    dfH_filt.fillna({"Used Size": 0}, inplace=True)
    dfH_filt["Used Size"] = dfH_filt["Used Size"].astype(int)
    export_Hier = dfH_filt.copy()

    # Model
    # Converting to KB/MB/GB necessitates division by 1024 * 1000.
    if db_total_size >= 1000000000:
        y = db_total_size / (1024**3) * 1000000000
    elif db_total_size >= 1000000:
        y = db_total_size / (1024**2) * 1000000
    elif db_total_size >= 1000:
        y = db_total_size / (1024) * 1000
    else:
        y = db_total_size
    y = round(y)

    dfModel = pd.DataFrame(
        {
            "Dataset Name": dataset_name,
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

    def _style_columns_based_on_types(dataframe: pd.DataFrame, column_type_mapping):
        # Define formatting functions based on the type mappings
        format_funcs = {
            "int": lambda x: "{:,}".format(x) if pd.notnull(x) else "",
            "pct": lambda x: "{:.2f}%".format(x) if pd.notnull(x) else "",
            "": lambda x: "{}".format(x),
        }

        # Apply the formatting function to each column based on its specified type
        for col, dt in column_type_mapping.items():
            if dt in format_funcs:
                dataframe[col] = dataframe[col].map(format_funcs[dt])

        return dataframe

    dfModel = _style_columns_based_on_types(
        dfModel,
        column_type_mapping={
            key: values[1] for key, values in vertipaq_map["Model"].items()
        },
    )
    dfT = _style_columns_based_on_types(
        dfT,
        column_type_mapping={
            key: values[1] for key, values in vertipaq_map["Tables"].items()
        },
    )
    dfP = _style_columns_based_on_types(
        dfP,
        column_type_mapping={
            key: values[1] for key, values in vertipaq_map["Partitions"].items()
        },
    )
    colSize = _style_columns_based_on_types(
        colSize,
        column_type_mapping={
            key: values[1] for key, values in vertipaq_map["Columns"].items()
        },
    )
    temp = _style_columns_based_on_types(
        temp,
        column_type_mapping={
            key: values[1] for key, values in vertipaq_map["Columns"].items()
        },
    )
    dfR = _style_columns_based_on_types(
        dfR,
        column_type_mapping={
            key: values[1] for key, values in vertipaq_map["Relationships"].items()
        },
    )
    dfH_filt = _style_columns_based_on_types(
        dfH_filt,
        column_type_mapping={
            key: values[1] for key, values in vertipaq_map["Hierarchies"].items()
        },
    )

    dataFrames = {
        "dfModel": dfModel,
        "dfT": dfT,
        "dfP": dfP,
        "colSize": colSize,
        "temp": temp,
        "dfR": dfR,
        "dfH_filt": dfH_filt,
    }

    dfs = {}
    for fileName, df in dataFrames.items():
        dfs[fileName] = df

    if export is None:
        visualize_vertipaq(dfs)
        return {
            "Model Summary": export_Model,
            "Tables": export_Table,
            "Partitions": export_Part,
            "Columns": export_Col,
            "Relationships": export_Rel,
            "Hierarchies": export_Hier,
        }

    # Export vertipaq to delta tables in lakehouse
    if export in ["table", "zip"]:
        if not lakehouse_attached():
            raise ValueError(
                f"{icons.red_dot} In order to save the Vertipaq Analyzer results, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook."
            )

    if export == "table":
        lakeTName = "vertipaqanalyzer_model"

        lakeT = get_lakehouse_tables()
        lakeT_filt = lakeT[lakeT["Table Name"] == lakeTName]

        if len(lakeT_filt) == 0:
            runId = 1
        else:
            max_run_id = _get_column_aggregate(table_name=lakeTName)
            runId = max_run_id + 1

        dfMap = {
            "Columns": ["Columns", export_Col],
            "Tables": ["Tables", export_Table],
            "Partitions": ["Partitions", export_Part],
            "Relationships": ["Relationships", export_Rel],
            "Hierarchies": ["Hierarchies", export_Hier],
            "Model": ["Model", export_Model],
        }

        print(
            f"{icons.in_progress} Saving Vertipaq Analyzer to delta tables in the lakehouse...\n"
        )
        now = datetime.datetime.now()
        dfD = fabric.list_datasets(workspace=workspace_id, mode="rest")
        dfD_filt = dfD[dfD["Dataset Id"] == dataset_id]
        configured_by = dfD_filt["Configured By"].iloc[0]
        capacity_id, capacity_name = resolve_workspace_capacity(workspace=workspace_id)

        for key_name, (obj, df) in dfMap.items():
            df["Capacity Name"] = capacity_name
            df["Capacity Id"] = capacity_id
            df["Configured By"] = configured_by
            df["Workspace Name"] = workspace_name
            df["Workspace Id"] = workspace_id
            df["Dataset Name"] = dataset_name
            df["Dataset Id"] = dataset_id
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

            schema = {
                "Capacity_Name": icons.data_type_string,
                "Capacity_Id": icons.data_type_string,
                "Workspace_Name": icons.data_type_string,
                "Workspace_Id": icons.data_type_string,
                "Dataset_Name": icons.data_type_string,
                "Dataset_Id": icons.data_type_string,
                "Configured_By": icons.data_type_string,
            }

            schema.update(
                {
                    key.replace(" ", "_"): value[0]
                    for key, value in vertipaq_map[key_name].items()
                }
            )
            schema["RunId"] = icons.data_type_long
            schema["Timestamp"] = icons.data_type_timestamp

            delta_table_name = f"VertipaqAnalyzer_{obj}".lower()
            save_as_delta_table(
                dataframe=df,
                delta_table_name=delta_table_name,
                write_mode="append",
                schema=schema,
                merge_schema=True,
            )

    # Export vertipaq to zip file within the lakehouse
    if export == "zip":
        dataFrames = {
            "dfModel": dfModel,
            "dfT": dfT,
            "dfP": dfP,
            "colSize": colSize,
            "temp": temp,
            "dfR": dfR,
            "dfH_filt": dfH_filt,
        }

        zipFileName = f"{workspace_name}.{dataset_name}.zip"

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
            f"{icons.green_dot} The Vertipaq Analyzer info for the '{dataset_name}' semantic model in the '{workspace_name}' workspace has been saved "
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
        {
            "ViewName": "Column",
            "ColumnName": "Type",
            "Tooltip": "The type of column",
        },
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
        "Tables": dataframes["dfT"],
        "Partitions": dataframes["dfP"],
        "Columns": dataframes["colSize"],
        "Relationships": dataframes["dfR"],
        "Hierarchies": dataframes["dfH_filt"],
    }

    mapping = {
        "Model Summary": "Model",
        "Tables": "Table",
        "Partitions": "Partition",
        "Columns": "Column",
        "Relationships": "Relationship",
        "Hierarchies": "Hierarchy",
    }

    uid = uuid.uuid4().hex[:8]

    # Build tooltip lookup for fast access
    tooltip_lookup = {}
    for _, row in tooltipDF.iterrows():
        tooltip_lookup[(row["ViewName"], row["ColumnName"])] = row["Tooltip"]

    # ── CSS ──────────────────────────────────────────────────────────────
    styles = f"""
    <style>
    .vpx-{uid} {{
        --vpx-accent: #0071e3;
        --vpx-accent-hover: #0077ED;
        --vpx-bg: #ffffff;
        --vpx-bg-secondary: #f5f5f7;
        --vpx-bg-tertiary: #fbfbfd;
        --vpx-border: rgba(0, 0, 0, 0.06);
        --vpx-border-strong: rgba(0, 0, 0, 0.12);
        --vpx-text: #1d1d1f;
        --vpx-text-secondary: #6e6e73;
        --vpx-text-tertiary: #86868b;
        --vpx-shadow-sm: 0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.06);
        --vpx-shadow-md: 0 4px 14px rgba(0,0,0,0.08), 0 2px 6px rgba(0,0,0,0.04);
        --vpx-shadow-lg: 0 12px 40px rgba(0,0,0,0.12), 0 4px 12px rgba(0,0,0,0.06);
        --vpx-radius: 12px;
        --vpx-radius-sm: 8px;
        --vpx-transition: 0.25s cubic-bezier(0.4, 0, 0.2, 1);
        font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "Helvetica Neue", Helvetica, Arial, sans-serif;
        color: var(--vpx-text);
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
        max-width: 100%;
        margin: 0;
        padding: 0;
    }}
    .vpx-{uid} *, .vpx-{uid} *::before, .vpx-{uid} *::after {{
        box-sizing: border-box;
    }}
    /* ── Container ── */
    .vpx-{uid} .vpx-container {{
        background: var(--vpx-bg);
        border-radius: var(--vpx-radius);
        box-shadow: var(--vpx-shadow-lg);
        overflow: hidden;
        border: 1px solid var(--vpx-border);
    }}
    /* ── Header ── */
    .vpx-{uid} .vpx-header {{
        padding: 20px 24px 0 24px;
        background: var(--vpx-bg);
    }}
    .vpx-{uid} .vpx-title {{
        font-size: 22px;
        font-weight: 700;
        letter-spacing: -0.02em;
        color: var(--vpx-text);
        margin: 0 0 16px 0;
        line-height: 1.2;
    }}
    /* ── Tab Navigation ── */
    .vpx-{uid} .vpx-tab-bar {{
        display: flex;
        gap: 2px;
        padding: 0 24px;
        background: var(--vpx-bg);
        border-bottom: 1px solid var(--vpx-border);
        overflow-x: auto;
        scrollbar-width: none;
        -ms-overflow-style: none;
    }}
    .vpx-{uid} .vpx-tab-bar::-webkit-scrollbar {{
        display: none;
    }}
    .vpx-{uid} .vpx-tab-btn {{
        position: relative;
        padding: 10px 16px;
        font-size: 13px;
        font-weight: 500;
        color: var(--vpx-text-secondary);
        background: none;
        border: none;
        border-radius: 0;
        cursor: pointer;
        transition: color var(--vpx-transition);
        white-space: nowrap;
        letter-spacing: -0.01em;
        outline: none;
        font-family: inherit;
    }}
    .vpx-{uid} .vpx-tab-btn::after {{
        content: '';
        position: absolute;
        bottom: -1px;
        left: 0;
        right: 0;
        height: 2px;
        background: var(--vpx-accent);
        border-radius: 2px 2px 0 0;
        transform: scaleX(0);
        transition: transform var(--vpx-transition);
    }}
    .vpx-{uid} .vpx-tab-btn:hover {{
        color: var(--vpx-text);
    }}
    .vpx-{uid} .vpx-tab-btn.vpx-active {{
        color: var(--vpx-accent);
        font-weight: 600;
    }}
    .vpx-{uid} .vpx-tab-btn.vpx-active::after {{
        transform: scaleX(1);
    }}
    /* ── Toolbar ── */
    .vpx-{uid} .vpx-toolbar {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 12px 24px;
        background: var(--vpx-bg-tertiary);
        border-bottom: 1px solid var(--vpx-border);
    }}
    .vpx-{uid} .vpx-search-wrap {{
        position: relative;
        width: 260px;
    }}
    .vpx-{uid} .vpx-search-icon {{
        position: absolute;
        left: 10px;
        top: 50%;
        transform: translateY(-50%);
        width: 14px;
        height: 14px;
        color: var(--vpx-text-tertiary);
        pointer-events: none;
    }}
    .vpx-{uid} .vpx-search {{
        width: 100%;
        padding: 7px 12px 7px 32px;
        font-size: 13px;
        font-family: inherit;
        background: var(--vpx-bg);
        border: 1px solid var(--vpx-border-strong);
        border-radius: var(--vpx-radius-sm);
        color: var(--vpx-text);
        outline: none;
        transition: border-color var(--vpx-transition), box-shadow var(--vpx-transition);
    }}
    .vpx-{uid} .vpx-search::placeholder {{
        color: var(--vpx-text-tertiary);
    }}
    .vpx-{uid} .vpx-search:focus {{
        border-color: var(--vpx-accent);
        box-shadow: 0 0 0 3px rgba(0, 113, 227, 0.15);
    }}
    .vpx-{uid} .vpx-row-count {{
        font-size: 12px;
        font-weight: 500;
        color: var(--vpx-text-tertiary);
        letter-spacing: -0.01em;
    }}
    .vpx-{uid} .vpx-row-count span {{
        font-variant-numeric: tabular-nums;
    }}
    /* ── Tab Content ── */
    .vpx-{uid} .vpx-panel {{
        display: none;
        animation: vpxFadeIn{uid} 0.3s ease;
    }}
    .vpx-{uid} .vpx-panel.vpx-visible {{
        display: block;
    }}
    @keyframes vpxFadeIn{uid} {{
        from {{ opacity: 0; transform: translateY(4px); }}
        to {{ opacity: 1; transform: translateY(0); }}
    }}
    /* ── Table ── */
    .vpx-{uid} .vpx-table-wrap {{
        overflow-x: auto;
        overflow-y: auto;
        max-height: 520px;
    }}
    .vpx-{uid} table {{
        width: 100%;
        border-collapse: separate;
        border-spacing: 0;
        font-size: 13px;
        line-height: 1.4;
    }}
    .vpx-{uid} thead {{
        position: sticky;
        top: 0;
        z-index: 2;
    }}
    .vpx-{uid} thead th {{
        padding: 10px 20px 10px 16px;
        text-align: left;
        font-weight: 600;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        color: var(--vpx-text-secondary);
        background: var(--vpx-bg-secondary);
        border-bottom: 1px solid var(--vpx-border-strong);
        cursor: pointer;
        user-select: none;
        white-space: nowrap;
        position: relative;
        overflow: visible;
        min-width: fit-content;
        transition: color var(--vpx-transition), background var(--vpx-transition);
    }}
    .vpx-{uid} thead th:hover {{
        color: var(--vpx-text);
        background: #ececee;
    }}
    .vpx-{uid} thead th .vpx-sort-arrow {{
        display: inline-block;
        margin-left: 4px;
        font-size: 10px;
        opacity: 0.3;
        transition: opacity var(--vpx-transition);
    }}
    .vpx-{uid} thead th.vpx-sort-asc .vpx-sort-arrow,
    .vpx-{uid} thead th.vpx-sort-desc .vpx-sort-arrow {{
        opacity: 1;
        color: var(--vpx-accent);
    }}
    .vpx-{uid} tbody td {{
        padding: 9px 16px;
        border-bottom: 1px solid var(--vpx-border);
        color: var(--vpx-text);
        white-space: nowrap;
        text-align: left;
        transition: background var(--vpx-transition);
    }}
    .vpx-{uid} tbody tr {{
        transition: background var(--vpx-transition);
    }}
    .vpx-{uid} tbody tr:nth-child(even) {{
        background: var(--vpx-bg-tertiary);
    }}
    .vpx-{uid} tbody tr:hover {{
        background: rgba(0, 113, 227, 0.04);
    }}
    .vpx-{uid} tbody tr:hover td {{
        color: var(--vpx-text);
    }}
    .vpx-{uid} tbody td.vpx-numeric {{
        font-variant-numeric: tabular-nums;
        text-align: left;
        font-feature-settings: "tnum";
    }}
    .vpx-{uid} thead th.vpx-numeric {{
        text-align: left;
    }}
    /* ── Resize handle ── */
    .vpx-{uid} thead th .vpx-resize-handle {{
        position: absolute;
        right: 0;
        top: 0;
        bottom: 0;
        width: 5px;
        cursor: col-resize;
        background: transparent;
        z-index: 3;
    }}
    .vpx-{uid} thead th .vpx-resize-handle:hover,
    .vpx-{uid} thead th .vpx-resize-handle.vpx-resizing {{
        background: var(--vpx-accent);
        opacity: 0.5;
    }}
    /* ── Tooltip ── */
    .vpx-{uid} thead th[data-vpx-tip]:hover::before {{
        content: attr(data-vpx-tip);
        position: absolute;
        bottom: calc(100% + 6px);
        left: 50%;
        transform: translateX(-50%);
        padding: 6px 12px;
        font-size: 11px;
        font-weight: 400;
        text-transform: none;
        letter-spacing: 0;
        color: #fff;
        background: rgba(29, 29, 31, 0.92);
        backdrop-filter: blur(8px);
        -webkit-backdrop-filter: blur(8px);
        border-radius: 6px;
        white-space: nowrap;
        pointer-events: none;
        z-index: 10;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }}
    .vpx-{uid} thead th[data-vpx-tip]:hover::after {{
        content: '';
        position: absolute;
        bottom: calc(100% + 2px);
        left: 50%;
        transform: translateX(-50%);
        border: 4px solid transparent;
        border-top-color: rgba(29, 29, 31, 0.92);
        pointer-events: none;
        z-index: 10;
    }}
    /* ── Empty state ── */
    .vpx-{uid} .vpx-empty {{
        text-align: center;
        padding: 40px 24px;
        color: var(--vpx-text-tertiary);
        font-size: 14px;
    }}
    /* ── Footer ── */
    .vpx-{uid} .vpx-footer {{
        padding: 10px 24px;
        font-size: 11px;
        color: var(--vpx-text-tertiary);
        text-align: right;
        border-top: 1px solid var(--vpx-border);
        background: var(--vpx-bg-tertiary);
    }}
    </style>
    """

    # ── Build HTML ────────────────────────────────────────────────────────
    search_svg = (
        '<svg class="vpx-search-icon" viewBox="0 0 20 20" fill="currentColor">'
        '<path fill-rule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 '
        '1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z"'
        ' clip-rule="evenodd"/></svg>'
    )

    model_name = ""
    if "dfModel" in dataframes and not dataframes["dfModel"].empty:
        try:
            model_name = str(dataframes["dfModel"]["Dataset Name"].iloc[0])
        except Exception:
            pass

    header_title = f"Vertipaq Analyzer &mdash; {model_name}" if model_name else "Vertipaq Analyzer"

    html_parts = []
    html_parts.append(f'<div class="vpx-{uid}">')
    html_parts.append('<div class="vpx-container">')
    html_parts.append(f'<div class="vpx-header"><div class="vpx-title">{header_title}</div></div>')

    # Tab bar
    html_parts.append(f'<div class="vpx-tab-bar" id="vpx-tabbar-{uid}">')
    for i, title in enumerate(df_dict.keys()):
        active = " vpx-active" if i == 0 else ""
        html_parts.append(
            f'<button class="vpx-tab-btn{active}" '
            f'data-vpx-target="vpx-{uid}-p{i}" '
            f'onclick="vpxSwitch_{uid}(this)">{title}</button>'
        )
    html_parts.append('</div>')

    # Panels
    for i, (title, df) in enumerate(df_dict.items()):
        visible = " vpx-visible" if i == 0 else ""
        panel_id = f"vpx-{uid}-p{i}"
        vw = mapping.get(title)
        row_count = len(df)

        html_parts.append(f'<div id="{panel_id}" class="vpx-panel{visible}">')

        # Toolbar with search and row count
        html_parts.append('<div class="vpx-toolbar">')
        html_parts.append(
            f'<div class="vpx-search-wrap">{search_svg}'
            f'<input type="text" class="vpx-search" '
            f'placeholder="Filter {title.lower()}\u2026" '
            f'oninput="vpxFilter_{uid}(this, \'{panel_id}\')" />'
            f'</div>'
        )
        html_parts.append(
            f'<div class="vpx-row-count" id="{panel_id}-rc">'
            f'<span>{row_count:,}</span> row{"s" if row_count != 1 else ""}'
            f'</div>'
        )
        html_parts.append('</div>')  # toolbar

        # Table
        html_parts.append('<div class="vpx-table-wrap">')

        if df.empty:
            html_parts.append('<div class="vpx-empty">No data available</div>')
        else:
            html_parts.append('<table>')

            # Determine numeric columns
            numeric_cols = set()
            for col in df.columns:
                if df[col].dtype.kind in ('i', 'f', 'u'):
                    numeric_cols.add(col)

            # Header
            html_parts.append('<thead><tr>')
            for col in df.columns:
                tt = tooltip_lookup.get((vw, col), "")
                num_cls = ' vpx-numeric' if col in numeric_cols else ''
                tip_attr = f' data-vpx-tip="{tt}"' if tt else ''
                html_parts.append(
                    f'<th class="{num_cls.strip()}"{tip_attr} '
                    f'onclick="vpxSort_{uid}(this)">'
                    f'{col}<span class="vpx-sort-arrow">&#x25B2;</span>'
                    f'<div class="vpx-resize-handle" '
                    f'onmousedown="vpxResizeStart_{uid}(event, this)"></div></th>'
                )
            html_parts.append('</tr></thead>')

            # Body
            html_parts.append('<tbody>')
            for _, row_data in df.iterrows():
                html_parts.append('<tr>')
                for col in df.columns:
                    val = row_data[col]
                    num_cls = ' class="vpx-numeric"' if col in numeric_cols else ''
                    cell_val = "" if pd.isna(val) else str(val)
                    html_parts.append(f'<td{num_cls}>{cell_val}</td>')
                html_parts.append('</tr>')
            html_parts.append('</tbody>')

            html_parts.append('</table>')

        html_parts.append('</div>')  # table-wrap
        html_parts.append('</div>')  # panel

    html_parts.append(f'<div class="vpx-footer">Powered by Semantic Link Labs &bull; Vertipaq Analyzer</div>')
    html_parts.append('</div>')  # container
    html_parts.append('</div>')  # root

    # ── JavaScript ────────────────────────────────────────────────────────
    script = f"""
    <script>
    (function() {{
        /* Tab switching */
        window.vpxSwitch_{uid} = function(btn) {{
            var bar = btn.parentElement;
            var container = bar.closest('.vpx-{uid}');
            bar.querySelectorAll('.vpx-tab-btn').forEach(function(b) {{ b.classList.remove('vpx-active'); }});
            btn.classList.add('vpx-active');
            container.querySelectorAll('.vpx-panel').forEach(function(p) {{ p.classList.remove('vpx-visible'); }});
            var target = container.querySelector('#' + btn.getAttribute('data-vpx-target'));
            if (target) target.classList.add('vpx-visible');
        }};

        /* Filtering */
        window.vpxFilter_{uid} = function(input, panelId) {{
            var panel = document.getElementById(panelId);
            if (!panel) return;
            var q = input.value.toLowerCase();
            var rows = panel.querySelectorAll('tbody tr');
            var shown = 0;
            rows.forEach(function(tr) {{
                var text = tr.textContent.toLowerCase();
                var match = !q || text.indexOf(q) !== -1;
                tr.style.display = match ? '' : 'none';
                if (match) shown++;
            }});
            var rc = document.getElementById(panelId + '-rc');
            if (rc) {{
                var total = rows.length;
                rc.innerHTML = '<span>' + shown.toLocaleString() + '</span>' +
                    (shown !== total ? ' of <span>' + total.toLocaleString() + '</span>' : '') +
                    ' row' + (shown !== 1 ? 's' : '');
            }}
        }};

        /* Column resizing */
        var _vpxResizing_{uid} = false;
        window.vpxResizeStart_{uid} = function(evt, handle) {{
            evt.stopPropagation();
            evt.preventDefault();
            _vpxResizing_{uid} = true;
            var th = handle.parentElement;
            var table = th.closest('table');
            var startX = evt.pageX;
            var startW = th.offsetWidth;
            handle.classList.add('vpx-resizing');
            /* measure natural header widths before freezing layout */
            var ths = table.querySelectorAll('thead th');
            ths.forEach(function(h) {{
                if (!h.getAttribute('data-vpx-minw')) {{
                    h.setAttribute('data-vpx-minw', h.scrollWidth);
                }}
            }});
            /* freeze all column widths so layout is stable */
            ths.forEach(function(h) {{ h.style.width = h.offsetWidth + 'px'; }});
            table.style.tableLayout = 'fixed';
            var minW = parseInt(th.getAttribute('data-vpx-minw')) || 40;
            function onMove(e) {{
                var diff = e.pageX - startX;
                var newW = Math.max(minW, startW + diff);
                th.style.width = newW + 'px';
                th.style.minWidth = newW + 'px';
            }}
            function onUp() {{
                handle.classList.remove('vpx-resizing');
                document.removeEventListener('mousemove', onMove);
                document.removeEventListener('mouseup', onUp);
                setTimeout(function() {{ _vpxResizing_{uid} = false; }}, 0);
            }}
            document.addEventListener('mousemove', onMove);
            document.addEventListener('mouseup', onUp);
        }};

        /* Sorting */
        window.vpxSort_{uid} = function(th) {{
            if (_vpxResizing_{uid}) return;
            var table = th.closest('table');
            if (!table) return;
            var idx = Array.from(th.parentElement.children).indexOf(th);
            var tbody = table.querySelector('tbody');
            var rows = Array.from(tbody.querySelectorAll('tr'));
            var asc = !th.classList.contains('vpx-sort-asc');

            /* Clear sort classes from all headers */
            th.parentElement.querySelectorAll('th').forEach(function(h) {{
                h.classList.remove('vpx-sort-asc', 'vpx-sort-desc');
                h.querySelector('.vpx-sort-arrow').innerHTML = '&#x25B2;';
            }});
            th.classList.add(asc ? 'vpx-sort-asc' : 'vpx-sort-desc');
            th.querySelector('.vpx-sort-arrow').innerHTML = asc ? '&#x25B2;' : '&#x25BC;';

            rows.sort(function(a, b) {{
                var aVal = a.children[idx] ? a.children[idx].textContent.trim() : '';
                var bVal = b.children[idx] ? b.children[idx].textContent.trim() : '';
                /* Try numeric comparison */
                var aNum = parseFloat(aVal.replace(/,/g, '').replace(/%/g, ''));
                var bNum = parseFloat(bVal.replace(/,/g, '').replace(/%/g, ''));
                if (!isNaN(aNum) && !isNaN(bNum)) {{
                    return asc ? aNum - bNum : bNum - aNum;
                }}
                return asc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
            }});
            rows.forEach(function(r) {{ tbody.appendChild(r); }});
        }};
    }})();
    </script>
    """

    display(HTML(styles + "\n".join(html_parts) + script))


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
        file_path = Path(file_name)
        df_name = file_path.stem
        dfs[df_name] = df

    visualize_vertipaq(dfs)

    # Clean up: remove the extracted directory
    shutil.rmtree(extracted_dir)
