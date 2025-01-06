import pandas as pd
import datetime
import sempy.fabric as fabric
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from sempy_labs._helper_functions import (
    _get_fabric_context_setting,
    resolve_lakehouse_name,
    save_as_delta_table,
    _get_max_run_id,
)
from sempy_labs.lakehouse import get_lakehouse_tables
from typing import Tuple


def delta_analyzer(
    table_name: str, export: bool = False
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Analyzes a delta table and shows the results in a set of 5 dataframes. The table analyzed must be in the lakehouse attached to the notebook.

    Parameters
    ----------
    table_name : str
        The delta table name.
    export : bool, default=False
        If True, exports the resulting dataframes to delta tables in the lakehouse attached to the notebook.

    Returns
    -------
    Tuple[pandas.DataFrame, pandas.DataFrame, pandas.DataFrame, pandas.DataFrame, pandas.DataFrame]
        A pandas dataframe in HTML format showing semantic model objects which violated the best practice analyzer rules.
    """

    prefix = "SLL_DeltaAnalyzer_"
    now = datetime.datetime.now()
    workspace_id = fabric.get_workspace_id()
    workspace_name = fabric.resolve_workspace_name(workspace=workspace_id)
    lakehouse_id = fabric.get_lakehouse_id()
    lakehouse_name = resolve_lakehouse_name()
    default_file_storage = _get_fabric_context_setting(name="fs.defaultFS")
    fs = default_file_storage.split("@")[-1][:-1]

    delta_table_path = f"Tables/{table_name}"
    path = f"abfss://{workspace_id}@{fs}/{lakehouse_id}/{delta_table_path}"

    spark = SparkSession.builder.getOrCreate()
    # delta_table = DeltaTable.forPath(spark, path)
    # detail_df = spark.sql(f"DESCRIBE DETAIL `{table_name}`").collect()[0]

    def _get_distinct_count(table_name: str, column_name: str) -> int:

        query = f"SELECT COUNT(DISTINCT({column_name})) FROM {table_name}"
        dfSpark = spark.sql(query)
        return dfSpark.collect()[0][0] or 0

    # num_files = detail_df.numFiles
    # size_in_bytes = detail_df.sizeInBytes
    latest_files = spark.read.format("delta").load(path).inputFiles()
    file_paths = [f.split("/")[-1] for f in latest_files]
    row_count = spark.table(table_name).count()
    row_groups = 0
    max_rows_per_row_group = 0
    min_rows_per_row_group = float("inf")
    schema = ds.dataset(f"/lakehouse/default/Tables/{table_name}").schema.metadata
    is_vorder = any(b"vorder" in key for key in schema.keys())
    v_order_level = (
        int(schema.get(b"com.microsoft.parquet.vorder.level").decode("utf-8"))
        if is_vorder
        else None
    )

    parquet_file_df = pd.DataFrame(columns=["ParquetFile", "RowCount", "RowGroups"])
    row_group_df = pd.DataFrame(columns=["ParquetFile", "RowGroupID", "RowCount"])
    column_chunk_df = pd.DataFrame(
        columns=[
            "ParquetFile",
            "ColumnID",
            "ColumnName",
            "ColumnType",
            "CompressedSize",
            "UncompressedSize",
            "HasDict",
            "DictOffset",
            "ValueCount",
            "Encodings",
        ]
    )
    for file_name in file_paths:
        parquet_file = pq.ParquetFile(
            f"/lakehouse/default/Tables/{table_name}/{file_name}"
        )
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

            row_group_df = pd.concat(
                [row_group_df, pd.DataFrame(new_data, index=[0])], ignore_index=True
            )

    avg_rows_per_row_group = row_count / row_groups

    # Generate summary dataframe
    summary_df = pd.DataFrame(
        [
            {
                "RowCount": row_count,
                "RowGroups": row_groups,
                "ParquetFiles": len(file_paths),
                "MaxRowsPerRowGroup": max_rows_per_row_group,
                "MinRowsPerRowGroup": min_rows_per_row_group,
                "AvgRowsPerRowGroup": avg_rows_per_row_group,
                "VOrderEnabled": is_vorder,
                "VOrderLevel": v_order_level,
            }
        ]
    )

    # Clean up data types
    bool_cols = ["HasDict"]
    int_cols = [
        "CompressedSize",
        "UncompressedSize",
        "DictOffset",
        "ValueCount",
        "ColumnID",
    ]
    column_chunk_df[int_cols] = column_chunk_df[int_cols].astype(int)
    column_chunk_df[bool_cols] = column_chunk_df[bool_cols].astype(bool)

    int_cols = ["RowGroupID", "RowCount", "CompressedSize", "UncompressedSize"]
    float_cols = ["CompressionRatio"]
    row_group_df[int_cols] = row_group_df[int_cols].astype(int)
    row_group_df[float_cols] = row_group_df[float_cols].astype(float)

    int_cols = ["RowCount", "RowGroups"]
    parquet_file_df[int_cols] = parquet_file_df[int_cols].astype(int)

    # Generate column dataframe
    column_df = column_chunk_df.groupby(
        ["ColumnName", "ColumnType"], as_index=False
    ).agg({"CompressedSize": "sum", "UncompressedSize": "sum"})

    # Add distinct count to column_df
    for i, r in column_df.iterrows():
        col_name = r["ColumnName"]
        dc = _get_distinct_count(table_name=table_name, column_name=col_name)

        if "Cardinality" not in column_df.columns:
            column_df["Cardinality"] = None

        column_df.at[i, "Cardinality"] = dc

    column_df["Cardinality"] = column_df["Cardinality"].astype(int)
    summary_df["TotalSize"] = column_df["CompressedSize"].sum()

    dataframes = {
        "Summary": summary_df,
        "ParquetFiles": parquet_file_df,
        "RowGroups": row_group_df,
        "ColumnChunks": column_chunk_df,
        "Columns": column_df,
    }

    save_table = f"{prefix}Summary"

    if export:
        dfL = get_lakehouse_tables()
        dfL_filt = dfL[dfL["Table Name"] == save_table]
        if len(dfL_filt) == 0:
            runId = 1
        else:
            max_run_id = _get_max_run_id(
                lakehouse=lakehouse_name, table_name=save_table
            )
            runId = max_run_id + 1

    for name, df in dataframes.items():
        cols = {
            "WorkspaceName": workspace_name,
            "LakehouseName": lakehouse_name,
            "TableName": table_name,
        }
        for i, (col, param) in enumerate(cols.items()):
            df[col] = param
            df.insert(i, col, df.pop(col))

        df["Timestamp"] = now
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])

        if export:
            df["RunId"] = runId
            save_as_delta_table(
                dataframe=df,
                delta_table_name=f"{prefix}{name}",
                write_mode="append",
            )

    return (*dataframes.values(),)
