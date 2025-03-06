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
    import notebookutils

    now = datetime.now()
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace=workspace)

    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace
    )
    path = create_abfss_path(lakehouse_id, workspace_id, table_name)
    table_path = create_abfss_path(lakehouse_id, workspace_id)
    local_path = _mount(lakehouse=lakehouse, workspace=workspace)
    table_path_local = f"{local_path}/Tables/{table_name}"
    delta_table_path = path + "/_delta_log"

    files = notebookutils.fs.ls(delta_table_path)
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

        #df = pd.read_json(f"{table_path}/{table_name}/_delta_log/{file}", lines = True )
        df = pd.read_json(f"{table_path}/Tables/{table_name}/_delta_log/{file}", lines = True )

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

       # for index,row in df.iterrows():
        for index, row in enumerate(
            bar := tqdm(df.iterrows(), leave=False), start=1
            ):

            bar.set_description(
                f"Analyzing change ({index} of {len(df)})..."
            )

            if df.get('add') is not None:
                add_row = row[1]['add']

                if type(add_row)==dict:

                    file_name = add_row["path"]
                    sizeAdded = sizeAdded + add_row["size"]
                    filesAdded = filesAdded + 1

                    fileRowsAdded: int = 0

                    fs_filename = f"{table_path}/Tables/{table_name}/{file_name}"

                    if notebookutils.fs.exists(fs_filename):
                        #parquet_file = pq.ParquetFile(f"{table_path}/Tables/{table_name}/{file_name}")
                        parquet_file = pq.ParquetFile(table_path_local + f"/{file_name}")
                        for i in range(parquet_file.num_row_groups):
                            row_group = parquet_file.metadata.row_group(i)
                            num_rows = row_group.num_rows
                            fileRowsAdded = fileRowsAdded + num_rows

                            rowsAdded=rowsAdded+num_rows

                        rowGroupsAdded = rowGroupsAdded + parquet_file.num_row_groups

                        start = str(datetime.fromtimestamp(add_row["modificationTime"]/1000.0))
                        parquetFiles.append({"file":file_name,"start":start,"end":nowToEpoch,"rows":fileRowsAdded,"isCurrent":1})


            if df.get('remove') is not None:
                remove_row = row[1]['remove']
                if type(remove_row)==dict:
                    file_name = remove_row["path"]
                    ### CHECK IF FILE EXISTS!!!
                    fs_filename = f"{table_path}/Tables/{table_name}/{file_name}"

                    if notebookutils.fs.exists(fs_filename):
                        #parquet_file = pq.ParquetFile(f"{table_path}/Tables/{table_name}/{file_name}")
                        parquet_file = pq.ParquetFile(table_path_local + f"/{file_name}")
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
                commit_row = row[1]['commitInfo']
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

