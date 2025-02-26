import pandas as pd
import re
from typing import Dict, Optional
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from sempy_labs._helper_functions import (
    create_abfss_path,
    save_as_delta_table,
    _get_column_aggregate,
    _create_dataframe,
    _update_dataframe_datatypes,
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    _read_delta_table,
    _delta_table_row_count,
    _mount,
    _create_spark_session,
)
from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
import sempy_labs._icons as icons
from tqdm.auto import tqdm
from uuid import UUID
from datetime import datetime
import os
import json
from notebookutils import mssparkutils

def delta_analyzer_history(
    table_name: str,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,  

)-> pd.DataFrame:
    """
    Analyzes the transaction log for a specified delta table and shows the results in dataframe.  One row per data modification operation

    Keeps track on the number of Parquet files, rowgroups, file size and #rows impacted by each change.

    Incremental Framing effect: 100% = highly effective, 0% = no benefit at all

    Parameters
    ----------
    table_name : str
        The delta table name.

    Returns
    -------
    pandas.DataFrame

    Displayslus gantt visual showing timeline for individual parquet files

    Example Syntax
    -------
    import sempy_labs as labs
    df=labs.delta_analyzer_history('fact_myevents_1bln2')
    display(df)
    """
    from notebookutils import mssparkutils
    from IPython.display import display, HTML

    now = datetime.now()


    table_path = f"lakehouse/default/Tables/{table_name}/_delta_log"
    
    files = mssparkutils.fs.ls("file:/" + table_path)
    json_files = [file.name for file in files if file.name.endswith('.json')]

    totalSize: int = 0
    totalRows: int = 0
    totalFiles: int = 0
    totalRowgroups: int = 0

    changesArray = [] 
    parquetFiles = []
    elementVersion=0

    myDateTimeFormat = "%Y-%m-%d %H:%M:%S.%f"

    nowToEpoch = datetime.now().strftime(myDateTimeFormat)

    num_latest_files = len(json_files)
    for idx, file in enumerate(
        bar := tqdm(json_files), start=1
        ):
    
        bar.set_description(
            f"Analyzing the '{file}' parquet file ({idx}/{num_latest_files})..."
        )

        changeTimestamp = datetime.strptime("2001-01-01 12:00:00.000", myDateTimeFormat)

        df = pd.read_json(f"file:/lakehouse/default/Tables/{table_name}/_delta_log/{file}", lines = True )
        

        rowsAdded: int = 0
        sizeAdded: int = 0
        rowsDeleted: int = 0
        sizeDeleted: int = 0
        filesAdded: int = 0
        filesRemoved: int = 0

        rowGroupsAdded: int = 0
        rowGroupsRemoved: int = 0

        totalFilesBeforeChange: int = totalFiles
        totalRowGroupsBeforeChange: int = totalRowgroups
        operation: str = ""
        predicate: str = ""
        tags: str=""

        for index,row in df.iterrows():

            if df.get('add') is not None:
                add_row = row['add']

                if type(add_row)==dict:

                    file_name = add_row["path"]
                    sizeAdded = sizeAdded + add_row["size"]
                    filesAdded = filesAdded + 1
                    
                    fileRowsAdded: int = 0

                    fs_filename = f"file:/lakehouse/default/Tables/{table_name}/{file_name}"
                    if mssparkutils.fs.exists(fs_filename):
                        parquet_file = pq.ParquetFile(f"file:/lakehouse/default/Tables/{table_name}/{file_name}")
                        for i in range(parquet_file.num_row_groups):
                            row_group = parquet_file.metadata.row_group(i)
                            num_rows = row_group.num_rows
                            fileRowsAdded = fileRowsAdded + num_rows

                            rowsAdded=rowsAdded+num_rows

                        rowGroupsAdded = rowGroupsAdded + parquet_file.num_row_groups

                        start = str(datetime.fromtimestamp(add_row["modificationTime"]/1000.0))
                        parquetFiles.append({"file":file_name,"start":start,"end":nowToEpoch,"rows":fileRowsAdded,"isCurrent":1})


            if df.get('remove') is not None:
                remove_row = row['remove']
                if type(remove_row)==dict:
                    file_name = remove_row["path"]
                    ### CHECK IF FILE EXISTS!!!
                    fs_filename = f"file:/lakehouse/default/Tables/{table_name}/{file_name}"
                    if mssparkutils.fs.exists(fs_filename):
                        parquet_file = pq.ParquetFile(f"file:/lakehouse/default/Tables/{table_name}/{file_name}")
                        for i in range(parquet_file.num_row_groups):
                            row_group = parquet_file.metadata.row_group(i)
                            num_rows = row_group.num_rows
                            rowsDeleted=rowsDeleted+num_rows

                        filesRemoved = filesRemoved + 1
                        sizeDeleted = sizeDeleted + remove_row["size"]

                        rowGroupsRemoved = rowGroupsRemoved + parquet_file.num_row_groups

                        result = next((row for row in parquetFiles if row["file"] == file_name), None)
                        if result is not None:
                            result["isCurrent"] = 0
                            result["end"] = str(datetime.fromtimestamp(remove_row["deletionTimestamp"]/1000.0))
                

            if df.get('commitInfo') is not None:
                commit_row = row['commitInfo']
                if type(commit_row)==dict:
                    operation = commit_row["operation"]
                    
                    if "tags" in commit_row:
                        tags = commit_row["tags"]

                    if "operationParameters" in commit_row:
                        operationParameters= commit_row["operationParameters"]
                        if "predicate" in operationParameters:
                            predicate=operationParameters["predicate"]

                    if operation=="VACUUM START":
                        totalFiles = totalFiles - int(commit_row["operationMetrics"]["numFilesToDelete"])
                        totalSize = totalSize - int(commit_row["operationMetrics"]["sizeOfDataToDelete"])

                    changeTimestamp = datetime.fromtimestamp(commit_row["timestamp"]/1000.0)



        totalSize = totalSize + sizeAdded - sizeDeleted
        totalRows = totalRows + rowsAdded - rowsDeleted
        totalFiles = totalFiles + filesAdded - filesRemoved
        totalRowgroups = totalRowgroups + rowGroupsAdded - rowGroupsRemoved


        incrementalFramingEffect = 1
        if sizeDeleted != 0:
            incrementalFramingEffect = round( (totalSize-sizeAdded * 1.0) /totalSize,4)

        changesArray.append(
                [
                    elementVersion,
                    operation,
                    predicate,
                    changeTimestamp,
                    incrementalFramingEffect * 100,

                    filesAdded,
                    filesRemoved,
                    totalFilesBeforeChange-filesRemoved,
                    totalFiles ,

                    sizeAdded ,
                    sizeDeleted,
                    totalSize,

                    rowGroupsAdded,
                    rowGroupsRemoved,
                    totalRowGroupsBeforeChange-rowGroupsRemoved,
                    totalRowgroups,

                    rowsAdded,
                    rowsDeleted,
                    rowsAdded-rowsDeleted,
                    totalRows,

                    tags

                ]
            )

        elementVersion=elementVersion+1


    #  /********************************************************************************************************************
    #      Display Gantt Chart of files
    #  ********************************************************************************************************************/
    spec:str = """{
    "$$schema": 'https://vega.github.io/schema/vega-lite/v2.json',
    "description": "A simple bar chart with ranged data (aka Gantt Chart).",
    "width" : 1024 ,
    "data": {
        "values": %s
    },
    "layer":[
        {"mark": "bar"},
        {"mark": {
        "type": "text",
        "align": "center",
        "baseline": "middle",
        "dx": 40
        },
        "encoding": {
        "text": {"field": "rows", "type": "quantitative", "format":","},
        "color":{
        "condition": {"test": "datum['isCurrent'] == 1", "value": "black"},
        "value": "black"
            }
        }
        }],
    "encoding": {
        "y": {"field": "file", "type": "ordinal","sort": "isCurrent","title":null,"axis":{"labelPadding":15,"labelLimit":360}},
        "x": {"field": "start", "type": "temporal","title":null},
        "x2": {"field": "end", "type": "temporal","title":null},
            "color": {
            "field": "isCurrent",
            "scale": {"range": ["silver", "#ca8861"]}
            }
    }
    }""" % (parquetFiles)


    display(HTML(
        """
        <!DOCTYPE html>
        <html>
            <head>
                <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
                <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
                <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
            </head>
            <body>
                <div id="vis"></div>
                <script type="text/javascript">
                    var spec = """ + spec + """;
                    var opt = {"renderer": "canvas", "actions": false};
                    vegaEmbed("#vis", spec, opt);
                </script>
            </body>
        </html>"""
    ))



    changesDF = pd.DataFrame(changesArray,columns=[
                "Change Number","Change Type","Predicate","Modification Time","Incremental Effect",
                "Files Added","Files Removed","Files Preserved","Files after change",
                "Size Added","Sized Removed","Size after change",
                "Rowgroups Added","Rowgroups Removed","Rowgroups Preserved","Rowgroups after change",
                "Rows Added","Rows Removed","Rows Delta" , "Rows after change","Tags"])   

    return changesDF

def delta_analyzer(
    table_name: str,
    approx_distinct_count: bool = True,
    skip_cardinalitiy: bool = True,
    export: bool = False,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Analyzes a delta table and shows the results in dictionary containing a set of 5 dataframes. If 'export' is set to True, the results will be saved to delta tables in the lakehouse attached to the notebook.

    The 5 dataframes returned by this function are:

    * Summary
    * Parquet Files
    * Row Groups
    * Column Chunks
    * Columns

    Read more about Delta Analyzer `here <https://github.com/microsoft/Analysis-Services/tree/master/DeltaAnalyzer>`_.

    Parameters
    ----------
    table_name : str
        The delta table name.
    approx_distinct_count: bool, default=True
        If True, uses approx_count_distinct to calculate the cardinality of each column. If False, uses COUNT(DISTINCT) instead.
    skip_cardinalitiy: bool, default=True
        IF True, skips cardinality check on the 'Column' dataset which can speed up the output.  Set to False if you need to see cardinalities of each column
    export : bool, default=False
        If True, exports the resulting dataframes to delta tables in the lakehouse attached to the notebook.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    Dict[str, pandas.DataFrame]
        A dictionary of pandas dataframes showing semantic model objects which violated the best practice analyzer rules.
    
    Example Syntax
    -------
    import sempy_labs as labs
    dataFrames = labs.delta_analyzer(table_name='fact_myevents_1bln2', export=False)

    display(dataFrames["Summary"])
    display(dataFrames["Parquet Files"])
    display(dataFrames["Row Groups"])
    display(dataFrames["Column Chunks"])
    display(dataFrames["Columns"])
    """
    import notebookutils

    # display_toggle = notebookutils.common.configs.pandas_display

    # Turn off notebookutils display
    # if display_toggle is True:
    #    notebookutils.common.configs.pandas_display = False


    prefix = "SLL_DeltaAnalyzer_"
    now = datetime.now()
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace=workspace)
   
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace
    )
    path = create_abfss_path(lakehouse_id, workspace_id, table_name)
    local_path = _mount(lakehouse=lakehouse, workspace=workspace)
    table_path = f"{local_path}/Tables/{table_name}"
    delta_table_path = create_abfss_path(lakehouse_id, workspace_id, table_name)

    # Set back to original value
    # notebookutils.common.configs.pandas_display = display_toggle

    parquet_file_df_columns = {
        "Dataset": "string",
        "ParquetFile": "string",
        "RowCount": "int",
        "RowGroups": "int",
        "CreatedBy": "string" ,
        "TotalTableRows": "int",
        "TotalTableRowGroups": "int",
    }
    row_group_df_columns = {
        "Dataset": "string",
        "ParquetFile": "string",
        "RowGroupID": "int",
        "RowCount": "int",
        "CompressedSize": "int",
        "UncompressedSize": "int",
        "CompressionRatio": "float",
        "TotalTableRows": "int",
        "RatioOfTotalTableRows": "float",
        "TotalTableRowGroups": "int",

    }
    column_chunk_df_columns = {
        "Dataset": "string",
        "ParquetFile": "string",
        "ColumnID": "int",
        "ColumnName": "string",
        "ColumnType": "string",
        "CompressedSize": "int",
        "UncompressedSize": "int",
        "HasDict": "bool",
        "DictOffset": "int_fillna",
        "ValueCount": "int",
        "Encodings": "string",
        "Statistics": "string" ,
        "PrimativeType": "string",
    }

    parquet_file_df = _create_dataframe(columns=parquet_file_df_columns)
    row_group_df = _create_dataframe(columns=row_group_df_columns)
    column_chunk_df = _create_dataframe(columns=column_chunk_df_columns)


    row_groups = 0
    max_rows_per_row_group = 0
    min_rows_per_row_group = float("inf")
    schema = ds.dataset(table_path).schema.metadata
    is_vorder = any(b"vorder" in key for key in schema.keys())

        # Get the common details of the Delta table
    spark = _create_spark_session()

    from delta import DeltaTable

    delta_table = DeltaTable.forPath(spark, delta_table_path)
    table_df = delta_table.toDF()

    delta_table = DeltaTable.forPath(spark, delta_table_path)
    table_df = delta_table.toDF()
    # total_partition_count = table_df.rdd.getNumPartitions()
    row_count = table_df.count()
    table_details = delta_table.detail().collect()[0].asDict()
    # created_at = table_details.get("createdAt")
    # last_modified = table_details.get("lastModified")
    # partition_columns = table_details.get("partitionColumns")
    # clustering_columns = table_details.get("clusteringColumns")
    num_latest_files = table_details.get("numFiles", 0)
    # size_in_bytes = table_details.get("sizeInBytes")
    # min_reader_version = table_details.get("minReaderVersion")
    # min_writer_version = table_details.get("minWriterVersion")

    latest_files = _read_delta_table(path).inputFiles()
    file_paths = [f.split("/")[-1] for f in latest_files]
    row_count = _delta_table_row_count(table_name)
    row_groups = 0
    max_rows_per_row_group = 0
    min_rows_per_row_group = float("inf")

    schema = ds.dataset(table_path).schema.metadata
    is_vorder = any(b"vorder" in key for key in schema.keys())

    for idx, file_path in enumerate(
        bar := tqdm(latest_files), start=1
    ):
        file_name = os.path.basename(file_path)
        bar.set_description(
            f"Analyzing the '{file_name}' parquet file ({idx}/{num_latest_files})..."
        )

    #for file_name in file_paths:
        parquet_file = pq.ParquetFile(f"{table_path}/{file_name}")
        row_groups += parquet_file.num_row_groups

        # Generate parquet dataframe
        new_data = {
            "Dataset": "Parquet Files",
            "ParquetFile": file_name,
            "RowCount": parquet_file.metadata.num_rows,
            "RowGroups": parquet_file.num_row_groups,
            "CreatedBy": parquet_file.metadata.created_by,
            "TotalTableRows": -1,
            "TotalTableRowGroups": -1,

        }

        parquet_file_df = pd.concat(
            [parquet_file_df, pd.DataFrame(new_data, index=[0])], ignore_index=True
        )

        for i in range(parquet_file.num_row_groups):
            row_group = parquet_file.metadata.row_group(i)
            num_rows = row_group.num_rows

            max_rows_per_row_group = max(max_rows_per_row_group, num_rows)
            min_rows_per_row_group = min(min_rows_per_row_group, num_rows)

            total_compressed_size = 0
            total_uncompressed_size = 0

            for j in range(row_group.num_columns):
                column_chunk = row_group.column(j)
                total_compressed_size += column_chunk.total_compressed_size
                total_uncompressed_size += column_chunk.total_uncompressed_size

                # Generate Column Chunk Dataframe
                new_data = {
                    "Dataset": "Column Chunks",
                    "ParquetFile": file_name,
                    "ColumnID": j,
                    "ColumnName": column_chunk.path_in_schema,
                    "ColumnType": column_chunk.physical_type,
                    "CompressedSize": column_chunk.total_compressed_size,
                    "UncompressedSize": column_chunk.total_uncompressed_size,
                    "HasDict": column_chunk.has_dictionary_page,
                    "DictOffset": column_chunk.dictionary_page_offset,
                    "ValueCount": column_chunk.num_values,
                    "Encodings": str(column_chunk.encodings),        
                    "Statistics": column_chunk.statistics,
                    "PrimativeType": column_chunk.physical_type,
                }

                column_chunk_df = pd.concat(
                    [column_chunk_df, pd.DataFrame(new_data, index=[0])],
                    ignore_index=True,
                )

            # Generate rowgroup dataframe
            new_data = {
                "Dataset": "RowGroups",
                "ParquetFile": file_name,
                "RowGroupID": i + 1,
                "RowCount": num_rows,
                "CompressedSize": total_compressed_size,
                "UncompressedSize": total_uncompressed_size,
                "CompressionRatio": total_compressed_size / total_uncompressed_size,
                "TotalTableRows": -1,
                "TotalTableRowGroups": -1,

            }

            if not row_group_df.empty:
                row_group_df = pd.concat(
                    [row_group_df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
            else:
                row_group_df = pd.DataFrame(new_data, index=[0])

    avg_rows_per_row_group = row_count / row_groups

    # Generate summary dataframe
    summary_df = pd.DataFrame(
        [
            {
                "Dataset": "Summary",
                "RowCount": row_count,
                "RowGroups": row_groups,
                "ParquetFiles": len(file_paths),
                "MaxRowsPerRowGroup": max_rows_per_row_group,
                "MinRowsPerRowGroup": min_rows_per_row_group,
                "AvgRowsPerRowGroup": avg_rows_per_row_group,
                "VOrderEnabled": is_vorder,
                # "VOrderLevel": v_order_level,
            }
        ]
    )

    # Clean up data types
    _update_dataframe_datatypes(
        dataframe=column_chunk_df, column_map=column_chunk_df_columns
    )
    _update_dataframe_datatypes(dataframe=row_group_df, column_map=row_group_df_columns)
    _update_dataframe_datatypes(
        dataframe=parquet_file_df, column_map=parquet_file_df_columns
    )

    # Generate column dataframe
    column_df = column_chunk_df.groupby(
        ["ColumnName", "ColumnType"], as_index=False
    ).agg({"CompressedSize": "sum", "UncompressedSize": "sum"})
    column_df["Dataset"] = "Columns"


    # Add distinct count to column_df
    if not skip_cardinalitiy:
        for ind, r in column_df.iterrows():
            col_name = r["ColumnName"]
            if approx_distinct_count:
                dc = _get_column_aggregate(
                    table_name=table_name,
                    column_name=col_name,
                    function="approx",
                    lakehouse=lakehouse,
                    workspace=workspace,
                )
            else:
                dc = _get_column_aggregate(
                    table_name=table_name,
                    column_name=col_name,
                    function="distinctcount",
                    lakehouse=lakehouse,
                    workspace=workspace,
                )

            if "Cardinality" not in column_df.columns:
                column_df["Cardinality"] = None

            column_df.at[ind, "Cardinality"] = dc
    else:
        column_df["Cardinality"] = None

    #column_df["Cardinality"] = column_df["Cardinality"].astype(int)
    summary_df["TotalSize"] = column_df["CompressedSize"].sum()

    parquet_file_df["TotalTableRows"] = parquet_file_df["RowCount"].sum()
    parquet_file_df["TotalTableRowGroups"] = parquet_file_df["RowGroups"].sum()

    row_group_df["TotalTableRows"] = parquet_file_df["RowCount"].sum()
    row_group_df["TotalTableRowGroups"] = parquet_file_df["RowGroups"].sum()
    total_rows = row_group_df["RowCount"].sum()
    row_group_df["RatioOfTotalTableRows"] = row_group_df["RowCount"] / total_rows  * 100.0

    column_df["TotalTableRows"] = parquet_file_df["RowCount"].sum()
    column_df["TableSize"] =column_df["CompressedSize"].sum()
    column_df["CardinalityOfTotalRows"] = column_df["Cardinality"] / column_df["TotalTableRows"] * 100.0
    column_df["SizePercentOfTable"] = column_df["CompressedSize"] / column_df["TableSize"] * 100.0

    dataframes = {
        "Summary": summary_df,
        "Parquet Files": parquet_file_df,
        "Row Groups": row_group_df,
        "Column Chunks": column_chunk_df,
        "Columns": column_df,
    }

    save_table = f"{prefix}Summary"

    if export:
        if not lakehouse_attached():
            raise ValueError(
                f"{icons.red_dot} No lakehouse is attached to this notebook. Please attach a lakehouse to the notebook before running the Delta Analyzer."
            )
        dfL = get_lakehouse_tables()
        dfL_filt = dfL[dfL["Table Name"] == save_table]
        if dfL_filt.empty:
            runId = 1
        else:
            max_run_id = _get_column_aggregate(
                table_name=save_table,
            )
            runId = max_run_id + 1

    for name, df in dataframes.items():
        name = name.replace(" ", "")
        cols = {
            "WorkspaceName": workspace_name,
            "WorkspaceId": workspace_id,
            "LakehouseName": lakehouse_name,
            "LakehouseId": lakehouse_id,
            "TableName": table_name,
        }
        for i, (col, param) in enumerate(cols.items()):
            df[col] = param
            df.insert(i, col, df.pop(col))

        df["Timestamp"] = now
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])

        if export:
            df["RunId"] = runId
            df["RunId"] = df["RunId"].astype(int)
            save_as_delta_table(
                dataframe=df,
                delta_table_name=f"{prefix}{name}",
                write_mode="append",
                merge_schema=True,
            )

    return dataframes
