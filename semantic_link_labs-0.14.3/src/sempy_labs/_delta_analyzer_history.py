import pandas as pd
from typing import Optional
import pyarrow.parquet as pq
from sempy_labs._helper_functions import (
    create_abfss_path,
    resolve_workspace_id,
    resolve_lakehouse_id,
    _mount,
)
from sempy._utils._log import log
from tqdm.auto import tqdm
from uuid import UUID
from datetime import datetime


@log
def delta_analyzer_history(
    table_name: str,
    schema: Optional[str] = None,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Analyzes the transaction log for a specified delta table and shows the results in dataframe.  One row per data modification operation.

    Keeps track on the number of Parquet files, rowgroups, file size and #rows impacted by each change.

    Incremental Framing effect: 100% = highly effective, 0% = no benefit at all

    Parameters
    ----------
    table_name : str
        The delta table name.
    schema : str, default=None
        The schema name of the delta table.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Displays a gantt visual showing a timeline for individual parquet files.
    """

    import notebookutils
    from IPython.display import display, HTML

    workspace_id = resolve_workspace_id(workspace=workspace)
    lakehouse_id = resolve_lakehouse_id(lakehouse=lakehouse, workspace=workspace)

    table_path = create_abfss_path(lakehouse_id, workspace_id, table_name, schema)
    local_path = _mount(lakehouse=lakehouse, workspace=workspace)
    if schema:  # use schema if specified
        table_path_local = f"{local_path}/Tables/{schema}/{table_name}"
    else:
        table_path_local = f"{local_path}/Tables/{table_name}"
    delta_table_path = f"{table_path}/_delta_log"

    files = notebookutils.fs.ls(delta_table_path)
    json_files = [file.name for file in files if file.name.endswith(".json")]

    element_version = total_size = total_rows = total_files = total_rowgroups = 0
    changes_array = []
    parquet_files = []
    my_date_time_format = "%Y-%m-%d %H:%M:%S.%f"
    now_to_epoch = datetime.now().strftime(my_date_time_format)
    num_latest_files = len(json_files)

    for idx, file in enumerate(bar := tqdm(json_files), start=1):
        bar.set_description(
            f"Analyzing the '{file}' parquet file ({idx}/{num_latest_files})..."
        )

        change_timestamp = datetime.strptime(
            "2001-01-01 12:00:00.000", my_date_time_format
        )
        df = pd.read_json(f"{delta_table_path}/{file}", lines=True)

        rows_added = size_added = rows_deleted = size_deleted = files_added = (
            files_removed
        ) = row_groups_added = row_groups_removed = 0
        total_files_before_change = total_files
        total_row_groups_before_change = total_rowgroups
        operation = predicate = tags = ""

        for _, row in df.iterrows():
            add_row = row.get("add")
            remove_row = row.get("remove")
            commit_row = row.get("commitInfo")

            if isinstance(add_row, dict):
                file_name = add_row["path"]
                fs_filename = f"{table_path}/{file_name}"
                size_added += add_row["size"]
                files_added += 1
                filerows_added = 0

                if notebookutils.fs.exists(fs_filename):
                    parquet_file = pq.ParquetFile(table_path_local + f"/{file_name}")
                    for i in range(parquet_file.num_row_groups):
                        row_group = parquet_file.metadata.row_group(i)
                        num_rows = row_group.num_rows
                        filerows_added += num_rows
                        rows_added += num_rows

                    row_groups_added += parquet_file.num_row_groups

                    start = str(
                        datetime.fromtimestamp(add_row["modificationTime"] / 1000.0)
                    )
                    parquet_files.append(
                        {
                            "file": file_name,
                            "start": start,
                            "end": now_to_epoch,
                            "rows": filerows_added,
                            "isCurrent": 1,
                        }
                    )

            if isinstance(remove_row, dict):
                file_name = remove_row["path"]
                fs_filename = f"{table_path}/{file_name}"

                if notebookutils.fs.exists(fs_filename):
                    parquet_file = pq.ParquetFile(table_path_local + f"/{file_name}")
                    for i in range(parquet_file.num_row_groups):
                        row_group = parquet_file.metadata.row_group(i)
                        num_rows = row_group.num_rows
                        rows_deleted += num_rows

                    files_removed += 1
                    size_deleted += remove_row.get("size", 0)
                    row_groups_removed += parquet_file.num_row_groups

                    result = next(
                        (row for row in parquet_files if row["file"] == file_name), None
                    )
                    if result:
                        result.update(
                            {
                                "isCurrent": 0,
                                "end": str(
                                    datetime.fromtimestamp(
                                        remove_row["deletionTimestamp"] / 1000.0
                                    )
                                ),
                            }
                        )

            if isinstance(commit_row, dict):
                operation = commit_row.get("operation")
                tags = commit_row.get("tags")
                predicate = commit_row.get("operationParameters", {}).get("predicate")

                if operation == "VACUUM START":
                    operation_metrics = commit_row.get("operationMetrics", {})
                    total_files -= int(operation_metrics.get("numFilesToDelete", 0))
                    total_size -= int(operation_metrics.get("sizeOfDataToDelete", 0))

                change_timestamp = datetime.fromtimestamp(
                    commit_row["timestamp"] / 1000.0
                )

        total_size += size_added - size_deleted
        total_rows += rows_added - rows_deleted
        total_files += files_added - files_removed
        total_rowgroups += row_groups_added - row_groups_removed

        incremental_framing_effect = 1
        if size_deleted != 0:
            incremental_framing_effect = (
                int((total_size - size_added * 1.0) / total_size * 100000) / 1000
            )
            # incrementalFramingEffect = round(
            #     (totalSize - sizeAdded * 1.0) / totalSize, 4
            # )

        changes_array.append(
            [
                element_version,
                operation,
                predicate,
                change_timestamp,
                incremental_framing_effect,
                files_added,
                files_removed,
                total_files_before_change - files_removed,
                total_files,
                size_added,
                size_deleted,
                total_size,
                row_groups_added,
                row_groups_removed,
                total_row_groups_before_change - row_groups_removed,
                total_rowgroups,
                rows_added,
                rows_deleted,
                rows_added - rows_deleted,
                total_rows,
                tags,
            ]
        )

        element_version += 1

    #  /********************************************************************************************************************
    #      Display Gantt Chart of files
    #  ********************************************************************************************************************/
    spec: str = (
        """{
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
    }"""
        % (parquet_files)
    )

    display(
        HTML(
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
                    var spec = """
            + spec
            + """;
                    var opt = {"renderer": "canvas", "actions": false};
                    vegaEmbed("#vis", spec, opt);
                </script>
            </body>
        </html>"""
        )
    )

    return pd.DataFrame(
        changes_array,
        columns=[
            "Change Number",
            "Change Type",
            "Predicate",
            "Modification Time",
            "Incremental Effect",
            "Files Added",
            "Files Removed",
            "Files Preserved",
            "Files After Change",
            "Size Added",
            "Sized Removed",
            "Size After Change",
            "Rowgroups Added",
            "Rowgroups Removed",
            "Rowgroups Preserved",
            "Rowgroups After Change",
            "Rows Added",
            "Rows Removed",
            "Rows Delta",
            "Rows After Change",
            "Tags",
        ],
    )
