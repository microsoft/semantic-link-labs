import sempy.fabric as fabric
from tqdm.auto import tqdm
from sempy_labs._helper_functions import (
    resolve_lakehouse_name,
    resolve_workspace_name_and_id,
)
from typing import List, Optional, Union
from sempy._utils._log import log
from uuid import UUID


def lakehouse_attached() -> bool:
    """
    Identifies if a lakehouse is attached to the notebook.

    Returns
    -------
    bool
        Returns True if a lakehouse is attached to the notebook.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    lakeId = spark.conf.get("trident.lakehouse.id")

    if len(lakeId) > 0:
        return True
    else:
        return False


@log
def optimize_lakehouse_tables(
    tables: Optional[Union[str, List[str]]] = None,
    lakehouse: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Runs the `OPTIMIZE <https://docs.delta.io/latest/optimizations-oss.html>`_ function over the specified lakehouse tables.

    Parameters
    ----------
    tables : str | List[str], default=None
        The table(s) to optimize.
        Defaults to None which resovles to optimizing all tables within the lakehouse.
    lakehouse : str, default=None
        The Fabric lakehouse.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    from pyspark.sql import SparkSession
    from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
    from delta import DeltaTable

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, workspace_id)

    lakeTables = get_lakehouse_tables(lakehouse=lakehouse, workspace=workspace_id)
    lakeTablesDelta = lakeTables[lakeTables["Format"] == "delta"]

    if isinstance(tables, str):
        tables = [tables]

    if tables is not None:
        tables_filt = lakeTablesDelta[lakeTablesDelta["Table Name"].isin(tables)]
    else:
        tables_filt = lakeTablesDelta.copy()

    spark = SparkSession.builder.getOrCreate()

    for _, r in (bar := tqdm(tables_filt.iterrows())):
        tableName = r["Table Name"]
        tablePath = r["Location"]
        bar.set_description(f"Optimizing the '{tableName}' table...")
        deltaTable = DeltaTable.forPath(spark, tablePath)
        deltaTable.optimize().executeCompaction()


@log
def vacuum_lakehouse_tables(
    tables: Optional[Union[str, List[str]]] = None,
    lakehouse: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    retain_n_hours: Optional[int] = None,
):
    """
    Runs the `VACUUM <https://docs.delta.io/latest/delta-utility.html#remove-files-no-longer-referenced-by-a-delta-table>`_ function over the specified lakehouse tables.

    Parameters
    ----------
    tables : str | List[str] | None
        The table(s) to vacuum. If no tables are specified, all tables in the lakehouse will be optimized.
    lakehouse : str, default=None
        The Fabric lakehouse.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    retain_n_hours : int, default=None
        The number of hours to retain historical versions of Delta table files.
        Files older than this retention period will be deleted during the vacuum operation.
        If not specified, the default retention period configured for the Delta table will be used.
        The default retention period is 168 hours (7 days) unless manually configured via table properties.
    """

    from pyspark.sql import SparkSession
    from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
    from delta import DeltaTable

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, workspace_id)

    lakeTables = get_lakehouse_tables(lakehouse=lakehouse, workspace=workspace_id)
    lakeTablesDelta = lakeTables[lakeTables["Format"] == "delta"]

    if isinstance(tables, str):
        tables = [tables]

    if tables is not None:
        tables_filt = lakeTablesDelta[lakeTablesDelta["Table Name"].isin(tables)]
    else:
        tables_filt = lakeTablesDelta.copy()

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")

    for _, r in (bar := tqdm(tables_filt.iterrows())):
        tableName = r["Table Name"]
        tablePath = r["Location"]
        bar.set_description(f"Vacuuming the '{tableName}' table...")
        deltaTable = DeltaTable.forPath(spark, tablePath)

        if retain_n_hours is None:
            deltaTable.vacuum()
        else:
            deltaTable.vacuum(retain_n_hours)
