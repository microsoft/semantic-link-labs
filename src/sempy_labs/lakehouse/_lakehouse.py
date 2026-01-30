from tqdm.auto import tqdm
from typing import List, Optional, Union
from sempy._utils._log import log
from uuid import UUID
from sempy_labs._helper_functions import (
    _base_api,
    resolve_lakehouse_name_and_id,
    resolve_workspace_name_and_id,
    _create_spark_session,
    _pure_python_notebook,
    _create_dataframe,
    _update_dataframe_datatypes,
    resolve_workspace_id,
)
import sempy_labs._icons as icons
import re
import pandas as pd


@log
def list_lakehouses(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the lakehouses within a workspace.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the lakehouses within a workspace.
    """

    columns = {
        "Lakehouse Name": "string",
        "Lakehouse ID": "string",
        "Description": "string",
        "OneLake Tables Path": "string",
        "OneLake Files Path": "string",
        "SQL Endpoint Connection String": "string",
        "SQL Endpoint ID": "string",
        "SQL Endpoint Provisioning Status": "string",
        "Schema Enabled": "bool",
        "Default Schema": "string",
        "Sensitivity Label Id": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/lakehouses",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            prop = v.get("properties", {})
            sqlEPProp = prop.get("sqlEndpointProperties", {})
            default_schema = prop.get("defaultSchema", None)

            rows.append(
                {
                    "Lakehouse Name": v.get("displayName"),
                    "Lakehouse ID": v.get("id"),
                    "Description": v.get("description"),
                    "OneLake Tables Path": prop.get("oneLakeTablesPath"),
                    "OneLake Files Path": prop.get("oneLakeFilesPath"),
                    "SQL Endpoint Connection String": sqlEPProp.get("connectionString"),
                    "SQL Endpoint ID": sqlEPProp.get("id"),
                    "SQL Endpoint Provisioning Status": sqlEPProp.get(
                        "provisioningStatus"
                    ),
                    "Schema Enabled": True if default_schema else False,
                    "Default Schema": default_schema,
                    "Sensitivity Label Id": v.get("sensitivityLabel", {}).get(
                        "sensitivityLabelId"
                    ),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
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
def _optimize_table(path):

    if _pure_python_notebook():
        from deltalake import DeltaTable

        DeltaTable(path).optimize.compact()
    else:
        from delta import DeltaTable

        spark = _create_spark_session()
        DeltaTable.forPath(spark, path).optimize().executeCompaction()


@log
def _vacuum_table(path, retain_n_hours):

    if _pure_python_notebook():
        from deltalake import DeltaTable

        DeltaTable(path).vacuum(retention_hours=retain_n_hours)
    else:
        from delta import DeltaTable

        spark = _create_spark_session()
        spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
        DeltaTable.forPath(spark, path).vacuum(retain_n_hours)


@log
def optimize_lakehouse_tables(
    tables: Optional[Union[str, List[str]]] = None,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Runs the `OPTIMIZE <https://docs.delta.io/latest/optimizations-oss.html>`_ function over the specified lakehouse tables.

    Parameters
    ----------
    tables : str | List[str], default=None
        The table(s) to optimize.
        Defaults to None which resovles to optimizing all tables within the lakehouse.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables

    df = get_lakehouse_tables(
        lakehouse=lakehouse, workspace=workspace, exclude_shortcuts=True
    )
    df_delta = df[df["Format"] == "delta"]

    if isinstance(tables, str):
        tables = [tables]

    df_tables = df_delta[df_delta["Table Name"].isin(tables)] if tables else df_delta
    df_tables.reset_index(drop=True, inplace=True)

    total = len(df_tables)
    for idx, r in (bar := tqdm(df_tables.iterrows(), total=total, bar_format="{desc}")):
        table_name = r["Table Name"]
        path = r["Location"]
        bar.set_description(
            f"Optimizing the '{table_name}' table ({idx + 1}/{total})..."
        )
        _optimize_table(path=path)


@log
def vacuum_lakehouse_tables(
    tables: Optional[Union[str, List[str]]] = None,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    retain_n_hours: Optional[int] = None,
):
    """
    Runs the `VACUUM <https://docs.delta.io/latest/delta-utility.html#remove-files-no-longer-referenced-by-a-delta-table>`_ function over the specified lakehouse tables.

    Parameters
    ----------
    tables : str | List[str] | None
        The table(s) to vacuum. If no tables are specified, all tables in the lakehouse will be vacuumed.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
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

    from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables

    df = get_lakehouse_tables(
        lakehouse=lakehouse, workspace=workspace, exclude_shortcuts=True
    )
    df_delta = df[df["Format"] == "delta"]

    if isinstance(tables, str):
        tables = [tables]

    df_tables = df_delta[df_delta["Table Name"].isin(tables)] if tables else df_delta
    df_tables.reset_index(drop=True, inplace=True)

    total = len(df_tables)
    for idx, r in (bar := tqdm(df_tables.iterrows(), total=total, bar_format="{desc}")):
        table_name = r["Table Name"]
        path = r["Location"]
        bar.set_description(f"Vacuuming the '{table_name}' table ({idx}/{total})...")
        _vacuum_table(path=path, retain_n_hours=retain_n_hours)


@log
def run_table_maintenance(
    table_name: str,
    optimize: bool = False,
    v_order: bool = False,
    vacuum: bool = False,
    retention_period: Optional[str] = None,
    schema: Optional[str] = None,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
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

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the job instance details of the table maintenance operation.
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
        payload["executionData"]["optimizeSettings"] = {"vOrder": True}
    if vacuum:
        payload["executionData"]["vacuumSettings"] = {}
    if vacuum and retention_period is not None:
        payload["executionData"]["vacuumSettings"]["retentionPeriod"] = retention_period

    print(
        f"{icons.in_progress} The table maintenance job for the '{table_name}' table in the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has been initiated."
    )

    df = _base_api(
        request=f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/jobs/instances?jobType=TableMaintenance",
        method="post",
        payload=payload,
        status_codes=[200, 202],
        client="fabric_sp",
        lro_return_df=True,
    )

    status = df["Status"].iloc[0]

    if status == "Completed":
        print(
            f"{icons.green_dot} The table maintenance job for the '{table_name}' table in the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has succeeded."
        )
    else:
        print(status)
        print(
            f"{icons.red_dot} The table maintenance job for the '{table_name}' table in the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has failed."
        )

    return df
