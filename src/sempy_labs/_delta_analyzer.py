import pandas as pd
import re
import datetime
import os
from uuid import UUID
from typing import Dict, Optional
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from delta import DeltaTable
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
from sempy._utils._log import log
from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
import sempy_labs._icons as icons


def get_parquet_file_infos(path):

    import notebookutils

    files = []
    items = notebookutils.fs.ls(path)
    for item in items:
        if item.isDir:
            # Ignore the _delta_log directory
            if "_delta_log" not in item.path:
                files.extend(get_parquet_file_infos(item.path))
        else:
            # Filter out non-Parquet files and files with size 0
            if item.path.endswith('.parquet') and item.size > 0:
                files.append((item.path, item.size))
    return files


@log
def delta_analyzer(
    table_name: str,
    approx_distinct_count: bool = True,
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
    """

    # display_toggle = notebookutils.common.configs.pandas_display

    # Turn off notebookutils display
    # if display_toggle is True:
    #    notebookutils.common.configs.pandas_display = False

    prefix = "SLL_DeltaAnalyzer_"
    now = datetime.datetime.now()
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
        "ParquetFile": "string",
        "RowCount": "int",
        "RowGroups": "int",
    }
    row_group_df_columns = {
        "ParquetFile": "string",
        "RowGroupID": "int",
        "RowCount": "int",
        "CompressedSize": "int",
        "UncompressedSize": "int",
        "CompressionRatio": "float",
    }
    column_chunk_df_columns = {
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
    }

    parquet_file_df = _create_dataframe(columns=parquet_file_df_columns)
    row_group_df = _create_dataframe(columns=row_group_df_columns)
    column_chunk_df = _create_dataframe(columns=column_chunk_df_columns)

    row_count = _delta_table_row_count(table_name)
    row_groups = 0
    max_rows_per_row_group = 0
    min_rows_per_row_group = float("inf")

    schema = ds.dataset(table_path).schema.metadata
    is_vorder = any(b"vorder" in key for key in schema.keys())

    # Get the common details of the Delta table
    spark = _create_spark_session()
    delta_table = DeltaTable.forPath(spark, delta_table_path)
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
    # file_paths = [f.split("/")[-1] for f in latest_files]
    all_parquet_files = get_parquet_file_infos(delta_table_path)
    common_file_paths = set([file_info[0] for file_info in all_parquet_files]).intersection(set(latest_files))
    latest_version_files = [file_info for file_info in all_parquet_files if file_info[0] in common_file_paths]

    for file_path, file_size in latest_version_files:
        file_name = os.path.basename(file_path)
        relative_path = file_path.split("Tables/")[1]
        file_system_path = f"{local_path}/Tables/{relative_path}"
        parquet_file = pq.ParquetFile(file_system_path)

        row_groups += parquet_file.num_row_groups

        # Generate rowgroup dataframe
        new_data = {
            "ParquetFile": file_name,
            "RowCount": parquet_file.metadata.num_rows,
            "RowGroups": parquet_file.num_row_groups,
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
                }

                column_chunk_df = pd.concat(
                    [column_chunk_df, pd.DataFrame(new_data, index=[0])],
                    ignore_index=True,
                )

            # Generate rowgroup dataframe
            new_data = {
                "ParquetFile": file_name,
                "RowGroupID": i + 1,
                "RowCount": num_rows,
                "CompressedSize": total_compressed_size,
                "UncompressedSize": total_uncompressed_size,
                "CompressionRatio": total_compressed_size / total_uncompressed_size,
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
                "RowCount": row_count,
                "RowGroups": row_groups,
                "ParquetFiles": num_latest_files,
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

    # Add distinct count to column_df
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

    column_df["Cardinality"] = column_df["Cardinality"].astype(int)
    summary_df["TotalSize"] = column_df["CompressedSize"].sum()

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


@log
def get_delta_table_history(table_name: str, lakehouse: Optional[str | UUID] = None, workspace: Optional[str | UUID] = None) -> pd.DataFrame:

    """
    Returns the history of a delta table as a pandas dataframe.

    Parameters
    ----------
    table_name : str
        The delta table name.
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
        A dataframe showing the history of the delta table.
    """

    def camel_to_title(text):
        return re.sub(r'([a-z])([A-Z])', r'\1 \2', text).title()

    spark = _create_spark_session()
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace=workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace
    )
    path = create_abfss_path(lakehouse_id, workspace_id, table_name)
    delta_table = DeltaTable.forPath(spark, path)
    delta_table = DeltaTable.forPath(spark, path)
    df = delta_table.history().toPandas()

    df.rename(columns=lambda col: camel_to_title(col), inplace=True)

    return df


