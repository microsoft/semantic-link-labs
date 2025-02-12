from tqdm.auto import tqdm
from typing import List, Optional, Union
from sempy._utils._log import log
from uuid import UUID
from sempy_labs._helper_functions import (
    _base_api,
    resolve_lakehouse_name_and_id,
    resolve_workspace_name_and_id,
    _create_spark_session,
)
import sempy_labs._icons as icons
import re


def lakehouse_attached() -> bool:
    """
    Identifies if a lakehouse is attached to the notebook.

    Returns
    -------
    bool
        Returns True if a lakehouse is attached to the notebook.
    """

    from sempy_labs._helper_functions import _get_fabric_context_setting

    lake_id = _get_fabric_context_setting(name="trident.lakehouse.id")

    if len(lake_id) > 0:
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

    from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
    from delta import DeltaTable

    lakeTables = get_lakehouse_tables(lakehouse=lakehouse, workspace=workspace)
    lakeTablesDelta = lakeTables[lakeTables["Format"] == "delta"]

    if isinstance(tables, str):
        tables = [tables]

    if tables is not None:
        tables_filt = lakeTablesDelta[lakeTablesDelta["Table Name"].isin(tables)]
    else:
        tables_filt = lakeTablesDelta.copy()

    spark = _create_spark_session()

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

    lakeTables = get_lakehouse_tables(lakehouse=lakehouse, workspace=workspace)
    lakeTablesDelta = lakeTables[lakeTables["Format"] == "delta"]

    if isinstance(tables, str):
        tables = [tables]

    if tables is not None:
        tables_filt = lakeTablesDelta[lakeTablesDelta["Table Name"].isin(tables)]
    else:
        tables_filt = lakeTablesDelta.copy()

    spark = _create_spark_session()
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


def run_table_maintenance(
    table_name: str,
    optimize: bool = False,
    v_order: bool = False,
    vacuum: bool = False,
    retention_period: Optional[str] = None,
    schema: Optional[str] = None,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Runs table maintenance operations on the specified table within the lakehouse.

    This is a wrapper function for the following API: `Background Jobs - Run On Demand Table Maintenance <https://learn.microsoft.com/rest/api/fabric/lakehouse/background-jobs/run-on-demand-table-maintenance>`_.

    Parameters
    ----------
    table_name : str
        Name of the delta table on which to run maintenance operations.
    optimize : bool, default=False
        If True, the `OPTIMIZE <https://docs.delta.io/latest/optimizations-oss.html>`_ function will be run on the table.
    v_order : bool, default=False
        If True, v-order will be enabled for the table.
    vacuum : bool, default=False
        If True, the `VACUUM <https://docs.delta.io/latest/delta-utility.html#remove-files-no-longer-referenced-by-a-delta-table>`_ function will be run on the table.
    retention_period : str, default=None
        If specified, the retention period for the vacuum operation. Must be in the 'd:hh:mm:ss' format.
    schema : str, default=None
        The schema of the tables within the lakehouse.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    if not optimize and not vacuum:
        raise ValueError(
            f"{icons.warning} At least one of 'optimize' or 'vacuum' must be set to True."
        )
    if not vacuum and retention_period is not None:
        raise ValueError(
            f"{icons.warning} The 'retention_period' parameter can only be set if 'vacuum' is set to True."
        )
    if retention_period is not None:

        def is_valid_format(time_string):
            pattern = r"^\d+:[0-2][0-9]:[0-5][0-9]:[0-5][0-9]$"
            return bool(re.match(pattern, time_string))

        if not is_valid_format(retention_period):
            raise ValueError(
                f"{icons.red_dot} The 'retention_period' parameter must be in the 'd:hh:mm:ss' format."
            )

    payload = {
        "executionData": {
            "tableName": table_name,
        }
    }
    if schema is not None:
        payload["executionData"]["schemaName"] = schema
    if optimize:
        payload["executionData"]["optimizeSettings"] = {}
    if v_order:
        payload["executionData"]["optimizeSettings"] = {"vorder": True}
    if vacuum:
        payload["executionData"]["vacuumSettings"] = {}
    if vacuum and retention_period is not None:
        payload["executionData"]["vacuumSettings"]["retentionPeriod"] = retention_period

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/jobs/instances?jobType=TableMaintenance",
        method="post",
        payload=payload,
        status_codes=202,
    )

    print(
        f"{icons.green_dot} The table maintenance job for the '{table_name}' table in the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has been initiated."
    )
