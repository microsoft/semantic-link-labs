import pandas as pd
import re
from datetime import datetime
import os
from uuid import UUID
from typing import Dict, Optional
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
    _mount,
    _read_delta_table_history,
    resolve_workspace_id,
    resolve_lakehouse_id,
    _get_delta_table,
)
from sempy._utils._log import log
from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
from sempy_labs.lakehouse._lakehouse import (
    lakehouse_attached,
)
from sempy_labs.lakehouse._helper import (
    is_v_ordered,
)
import sempy_labs._icons as icons
from tqdm.auto import tqdm


@log
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
            if item.path.endswith(".parquet") and item.size > 0:
                files.append((item.path, item.size))
    return files


@log
def delta_analyzer(
    table_name: str,
    approx_distinct_count: bool = True,
    export: bool = False,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    column_stats: bool = True,
    skip_cardinality: bool = True,
    schema: Optional[str] = None,
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
    column_stats : bool, default=True
        If True, collects data about column chunks and columns. If False, skips that step and only returns the other 3 dataframes.
    skip_cardinality : bool, default=True
        If True, skips the cardinality calculation for each column. If False, calculates the cardinality for each column.
    schema : str, default=None
        The name of the schema to which the table belongs (for schema-enabled lakehouses). If None, the default schema is used.

    Returns
    -------
    Dict[str, pandas.DataFrame]
        A dictionary of pandas dataframes showing semantic model objects which violated the best practice analyzer rules.
    """

    # Must calculate column stats if calculating cardinality
    if not skip_cardinality:
        column_stats = True

    prefix = "SLL_DeltaAnalyzer_"
    now = datetime.now()
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace=workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace
    )

    delta_table_path = create_abfss_path(
        lakehouse_id, workspace_id, table_name, schema=schema
    )
    local_path = _mount(lakehouse=lakehouse, workspace=workspace)

    parquet_file_df_columns = {
        # "Dataset": "string",
        "Parquet File": "string",
        "Row Count": "int",
        "Row Groups": "int",
        "Created By": "string",
        "Total Table Rows": "int",
        "Total Table Row Groups": "int",
    }
    row_group_df_columns = {
        # "Dataset": "string",
        "Parquet File": "string",
        "Row Group ID": "int",
        "Row Count": "int",
        "Compressed Size": "int",
        "Uncompressed Size": "int",
        "Compression Ratio": "float",
        "Total Table Rows": "int",
        "Ratio Of Total Table Rows": "float",
        "Total Table Row Groups": "int",
    }
    column_chunk_df_columns = {
        # "Dataset": "string",
        "Parquet File": "string",
        "Column ID": "int",
        "Column Name": "string",
        "Column Type": "string",
        "Compressed Size": "int",
        "Uncompressed Size": "int",
        "Has Dict": "bool",
        "Dict Offset": "int_fillna",
        "Value Count": "int",
        "Encodings": "string",
        "Statistics": "string",
        "Primative Type": "string",
    }

    parquet_file_df = _create_dataframe(columns=parquet_file_df_columns)
    row_group_df = _create_dataframe(columns=row_group_df_columns)
    column_chunk_df = _create_dataframe(columns=column_chunk_df_columns)

    row_groups = 0
    max_rows_per_row_group = 0
    min_rows_per_row_group = float("inf")

    is_vorder = is_v_ordered(
        table_name=table_name, lakehouse=lakehouse, workspace=workspace, schema=schema
    )

    # Get the common details of the Delta table
    delta_table = _get_delta_table(delta_table_path)
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

    latest_files = _read_delta_table(delta_table_path).inputFiles()
    # file_paths = [f.split("/")[-1] for f in latest_files]
    all_parquet_files = get_parquet_file_infos(delta_table_path)
    common_file_paths = set(
        [file_info[0] for file_info in all_parquet_files]
    ).intersection(set(latest_files))
    latest_version_files = [
        file_info
        for file_info in all_parquet_files
        if file_info[0] in common_file_paths
    ]

    for idx, (file_path, file_size) in enumerate(
        bar := tqdm(latest_version_files), start=1
    ):
        file_name = os.path.basename(file_path)
        bar.set_description(
            f"Analyzing the '{file_name}' parquet file ({idx}/{num_latest_files})..."
        )

        relative_path = file_path.split("Tables/")[1]
        file_system_path = f"{local_path}/Tables/{relative_path}"
        parquet_file = pq.ParquetFile(file_system_path)

        row_groups += parquet_file.num_row_groups

        # Generate rowgroup dataframe
        new_data = {
            # "Dataset": "Parquet Files",
            "Parquet File": file_name,
            "Row Count": parquet_file.metadata.num_rows,
            "Row Groups": parquet_file.num_row_groups,
            "Created By": parquet_file.metadata.created_by,
            "Total Table Rows": -1,
            "Total Table Row Groups": -1,
        }

        parquet_file_df = pd.concat(
            [parquet_file_df, pd.DataFrame(new_data, index=[0])], ignore_index=True
        )

        # Loop through the row groups
        for i in range(parquet_file.num_row_groups):
            row_group = parquet_file.metadata.row_group(i)
            num_rows = row_group.num_rows

            max_rows_per_row_group = max(max_rows_per_row_group, num_rows)
            min_rows_per_row_group = min(min_rows_per_row_group, num_rows)

            total_compressed_size = 0
            total_uncompressed_size = 0

            # Loop through the columns
            if column_stats:
                for j in range(row_group.num_columns):
                    column_chunk = row_group.column(j)
                    total_compressed_size += column_chunk.total_compressed_size
                    total_uncompressed_size += column_chunk.total_uncompressed_size

                    # Generate Column Chunk Dataframe
                    new_data = {
                        # "Dataset": "Column Chunks",
                        "Parquet File": file_name,
                        "Column ID": j,
                        "Column Name": column_chunk.path_in_schema,
                        "Column Type": column_chunk.physical_type,
                        "Compressed Size": column_chunk.total_compressed_size,
                        "Uncompressed Size": column_chunk.total_uncompressed_size,
                        "Has Dict": column_chunk.has_dictionary_page,
                        "Dict Offset": column_chunk.dictionary_page_offset,
                        "Value Count": column_chunk.num_values,
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
                # "Dataset": "Row Groups",
                "Parquet File": file_name,
                "Row Group ID": i + 1,
                "Row Count": num_rows,
                "Compressed Size": total_compressed_size,
                "Uncompressed Size": total_uncompressed_size,
                "Compression Ratio": (
                    total_compressed_size / total_uncompressed_size
                    if column_stats
                    else 0
                ),
                "Total Table Rows": -1,
                "Total Table Row Groups": -1,
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
                # "Dataset": "Summary",
                "Row Count": row_count,
                "Row Groups": row_groups,
                "Parquet Files": num_latest_files,
                "Max Rows Per Row Group": max_rows_per_row_group,
                "Min Rows Per Row Group": min_rows_per_row_group,
                "Avg Rows Per Row Group": avg_rows_per_row_group,
                "VOrder Enabled": is_vorder,
                # "VOrderLevel": v_order_level,
            }
        ]
    )

    # Clean up data types
    _update_dataframe_datatypes(dataframe=row_group_df, column_map=row_group_df_columns)
    _update_dataframe_datatypes(
        dataframe=parquet_file_df, column_map=parquet_file_df_columns
    )

    # Generate column dataframe
    if column_stats:
        _update_dataframe_datatypes(
            dataframe=column_chunk_df, column_map=column_chunk_df_columns
        )
        column_df = column_chunk_df.groupby(
            ["Column Name", "Column Type"], as_index=False
        ).agg({"Compressed Size": "sum", "Uncompressed Size": "sum"})

        # Add distinct count to column_df
        if not skip_cardinality:
            for ind, r in column_df.iterrows():
                col_name = r["Column Name"]
                if approx_distinct_count:
                    function = "approx"
                else:
                    function = "distinctcount"
                dc = _get_column_aggregate(
                    table_name=table_name,
                    column_name=col_name,
                    function=function,
                    lakehouse=lakehouse,
                    workspace=workspace,
                )

                if "Cardinality" not in column_df.columns:
                    column_df["Cardinality"] = None

                column_df.at[ind, "Cardinality"] = dc

        summary_df["Total Size"] = column_df["Compressed Size"].sum()

    parquet_file_df["Total Table Rows"] = parquet_file_df["Row Count"].sum()
    parquet_file_df["Total Table Row Groups"] = parquet_file_df["Row Groups"].sum()

    row_group_df["Total Table Rows"] = parquet_file_df["Row Count"].sum()
    row_group_df["Total Table Row Groups"] = parquet_file_df["Row Groups"].sum()
    total_rows = row_group_df["Row Count"].sum()
    row_group_df["Ratio Of Total Table Rows"] = (
        row_group_df["Row Count"] / total_rows * 100.0
    )

    if column_stats:
        column_df["Total Table Rows"] = parquet_file_df["Row Count"].sum()
        column_df["Table Size"] = column_df["Compressed Size"].sum()
        column_df["Size Percent Of Table"] = (
            column_df["Compressed Size"] / column_df["Table Size"] * 100.0
        )
    if not skip_cardinality and column_stats:
        column_df["Cardinality"] = column_df["Cardinality"].fillna(0).astype(int)
        column_df["Cardinality Of Total Rows"] = (
            column_df["Cardinality"] / column_df["Total Table Rows"] * 100.0
        )

    dataframes = {
        "Summary": summary_df,
        "Parquet Files": parquet_file_df,
        "Row Groups": row_group_df,
    }

    if column_stats:
        dataframes["Column Chunks"] = column_chunk_df
        dataframes["Columns"] = column_df

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
            "Workspace Name": workspace_name,
            "Workspace Id": workspace_id,
            "Lakehouse Name": lakehouse_name,
            "Lakehouse Id": lakehouse_id,
            "Table Name": table_name,
        }
        for i, (col, param) in enumerate(cols.items()):
            df[col] = param
            df.insert(i, col, df.pop(col))

        df["Timestamp"] = now
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])

        if export:
            df["Run Id"] = runId
            df["Run Id"] = df["Run Id"].astype(int)

            df.columns = df.columns.str.replace(" ", "")
            save_as_delta_table(
                dataframe=df,
                delta_table_name=f"{prefix}{name}",
                write_mode="append",
                merge_schema=True,
            )

    return dataframes


@log
def get_delta_table_history(
    table_name: str,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    schema: Optional[str] = None,
) -> pd.DataFrame:
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
    schema : str, default=None
        The name of the schema to which the table belongs (for schema-enabled lakehouses). If None, the default schema is used.

    Returns
    -------
    pandas.DataFrame
        A dataframe showing the history of the delta table.
    """

    def camel_to_title(text):
        return re.sub(r"([a-z])([A-Z])", r"\1 \2", text).title()

    workspace_id = resolve_workspace_id(workspace=workspace)
    lakehouse_id = resolve_lakehouse_id(lakehouse=lakehouse, workspace=workspace_id)
    path = create_abfss_path(lakehouse_id, workspace_id, table_name, schema)
    df = _read_delta_table_history(path=path)
    df.rename(columns=lambda col: camel_to_title(col), inplace=True)

    return df
