from typing import Optional, List
from uuid import UUID
from sempy_labs._helper_functions import (
    _create_spark_session,
    create_abfss_path,
    resolve_workspace_id,
    resolve_lakehouse_id,
    _get_delta_table,
)
from sempy._utils._log import log


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
    List[str]
        The list of partitioned columns.
    """

    details = _get_partitions(
        table_name=table, schema_name=schema, lakehouse=lakehouse, workspace=workspace
    )

    return details["partitionColumns"]


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

    workspace_id = resolve_workspace_id(workspace)
    lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)
    path = create_abfss_path(lakehouse_id, workspace_id, table, schema)
    # Get DeltaTable details
    spark = _create_spark_session()
    details_df = spark.sql(f"DESCRIBE DETAIL delta.`{path}`")
    details = details_df.collect()[0].asDict()

    # Extract relevant fields
    size_bytes = details["sizeInBytes"]
    partition_cols = details["partitionColumns"]
    num_files = details["numFiles"]

    total_size_gb = size_bytes / (1024**3)

    # Only check if the table is partitioned
    if len(partition_cols) > 0 and num_files > 0:
        avg_partition_size_gb = total_size_gb / num_files

        if (
            total_size_gb < total_table_size_gb
            or avg_partition_size_gb < average_partition_size_gb
        ):
            return True

    return False
