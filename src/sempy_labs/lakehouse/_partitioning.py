from typing import Optional, List
from uuid import UUID
import pandas as pd
from sempy_labs._helper_functions import (
    _create_dataframe,
    _get_delta_table,
)
from sempy._utils._log import log

BYTES_PER_GB = 1024**3
DELTA_TABLE_DETAIL_COLUMNS = [
    "Table Name",
    "Schema Name",
    "Size In Bytes",
    "Size In GB",
    "Files",
    "Partition Columns",
    "Is Partitioned",
]
DELTA_TABLE_DETAIL_COLUMN_TYPES = {
    "Table Name": "str",
    "Schema Name": "str",
    "Size In Bytes": "int",
    "Size In GB": "float",
    "Files": "int",
    "Partition Columns": "object",
    "Is Partitioned": "bool",
}


def _is_over_partitioned_by_details(
    details: dict, total_table_size_gb: int, average_partition_size_gb: int
) -> bool:
    total_size_gb = details["Size In GB"]
    partitioned = details["Is Partitioned"]
    num_files = details["Files"]

    # Only check if the table is partitioned
    if partitioned and num_files > 0:
        avg_partition_size_gb = total_size_gb / num_files

        return (
            total_size_gb < total_table_size_gb
            or avg_partition_size_gb < average_partition_size_gb
        )

    return False


@log
def _get_partitions(
    table_name: str,
    schema_name: Optional[str] = None,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
):

    workspace_id = resolve_workspace_id(workspace)
    lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)
    path = create_abfss_path(lakehouse_id, workspace_id, table_name, schema_name)

    delta_table = _get_delta_table(path)
    details_df = delta_table.detail()

    return details_df.collect()[0].asDict()


@log
def is_partitioned(
    table: str,
    schema: Optional[str] = None,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
) -> bool:
    """
    Checks if a delta table is partitioned.

    Parameters
    ----------
    table : str
        The name of the delta table.
    schema : str, optional
        The schema of the table to check. If not provided, the default schema is used.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    bool
        True if the table is partitioned, False otherwise.
    """

    details = _get_partitions(
        table_name=table, schema_name=schema, lakehouse=lakehouse, workspace=workspace
    )
    return len(details["partitionColumns"]) > 0


@log
def list_partitioned_columns(
    table: str,
    schema: Optional[str] = None,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
) -> List[str]:
    """
    Lists the partitioned columns of a delta table.

    Parameters
    ----------
    table : str
        The name of the delta table.
    schema : str, optional
        The schema of the table to check. If not provided, the default schema is used.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    typing.List[str]
        The list of partitioned columns.
    """

    details = _get_partitions(
        table_name=table, schema_name=schema, lakehouse=lakehouse, workspace=workspace
    )

    return details["partitionColumns"]


@log
def get_delta_table_details(
    table: str,
    schema: Optional[str] = None,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
) -> dict:
    """
    Gets size and partition details for a delta table.

    Parameters
    ----------
    table : str
        The name of the delta table.
    schema : str, optional
        The schema of the table to check. If not provided, the default schema is used.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    dict
        A dictionary containing table size, file count and partition details.
    """

    details = _get_partitions(
        table_name=table, schema_name=schema, lakehouse=lakehouse, workspace=workspace
    )
    partition_columns = details.get("partitionColumns", [])
    size_bytes = details.get("sizeInBytes", 0)
    files = details.get("numFiles", 0)

    return {
        "Table Name": table,
        "Schema Name": schema,
        "Size In Bytes": size_bytes,
        "Size In GB": size_bytes / BYTES_PER_GB,
        "Files": files,
        "Partition Columns": partition_columns,
        "Is Partitioned": len(partition_columns) > 0,
    }


@log
def is_over_partitioned(
    table: str,
    schema: Optional[str] = None,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    total_table_size_gb: int = 1000,
    average_partition_size_gb: int = 1,
) -> bool:
    """
    Checks if a delta table is over-partitioned.

    Parameters
    ----------
    table : str
        The name of the delta table.
    schema : str, optional
        The schema of the table to check. If not provided, the default schema is used.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    total_table_size_gb : int, default=1000
        Threshold for total table size in GB (default 1TB).
    average_partition_size_gb : int, default=1
        Threshold for average partition size in GB.

    Returns
    -------
    bool
        True if the table is over-partitioned, False otherwise.
    """

    details = get_delta_table_details(
        table=table, schema=schema, lakehouse=lakehouse, workspace=workspace
    )
    return _is_over_partitioned_by_details(
        details=details,
        total_table_size_gb=total_table_size_gb,
        average_partition_size_gb=average_partition_size_gb,
    )


@log
def list_over_partitioned_tables(
    schema: Optional[str | List[str]] = None,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    total_table_size_gb: int = 1000,
    average_partition_size_gb: int = 1,
) -> pd.DataFrame:
    """
    Lists over-partitioned delta tables in a lakehouse.

    Parameters
    ----------
    schema : str | typing.List[str], optional
        The schema name(s) used to filter tables.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    total_table_size_gb : int, default=1000
        Threshold for total table size in GB (default 1TB).
    average_partition_size_gb : int, default=1
        Threshold for average partition size in GB.

    Returns
    -------
    pandas.DataFrame
        A table of over-partitioned delta tables.
    """
    from sempy_labs.lakehouse._schemas import list_tables

    df = list_tables(lakehouse=lakehouse, workspace=workspace, schema=schema)
    over_partitioned_rows = []

    for _, row in df.query("Format == 'delta'").iterrows():
        table = row["Table Name"]
        table_schema = row["Schema Name"]
        details = get_delta_table_details(
            table=table,
            schema=table_schema,
            lakehouse=lakehouse,
            workspace=workspace,
        )
        if _is_over_partitioned_by_details(
            details=details,
            total_table_size_gb=total_table_size_gb,
            average_partition_size_gb=average_partition_size_gb,
        ):
            over_partitioned_rows.append(details)

    if not over_partitioned_rows:
        return _create_dataframe(columns=DELTA_TABLE_DETAIL_COLUMN_TYPES)

    return pd.DataFrame(over_partitioned_rows, columns=DELTA_TABLE_DETAIL_COLUMNS)
