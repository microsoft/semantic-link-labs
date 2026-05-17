from typing import List, Optional
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    _base_api,
    _create_dataframe,
    _create_spark_session,
)
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons
import pandas as pd


@log
def refresh_materialized_lake_views(
    lakehouse: Optional[str | UUID] = None, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Run on-demand Refresh MaterializedLakeViews job instance.

    This is a wrapper function for the following API: `Background Jobs - Run On Demand Refresh Materialized Lake Views <https://learn.microsoft.com/rest/api/fabric/lakehouse/background-jobs/run-on-demand-refresh-materialized-lake-views>`_.

    Parameters
    ----------
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
        A DataFrame containing the job instance details of the refresh materialized lake views operation.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    print(
        f"{icons.in_progress} The refresh materialized lake views job for the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has been initiated."
    )

    df = _base_api(
        request=f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/jobs/instances?jobType=RefreshMaterializedLakeViews",
        lro_return_df=True,
        method="post",
        status_codes=[200, 202],
    )

    status = df["Status"].iloc[0]

    if status == "Completed":
        print(
            f"{icons.green_dot} The refresh materialized lake views job for the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has succeeded."
        )
    else:
        print(status)
        print(
            f"{icons.red_dot} The refresh materialized lake views job for the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has failed."
        )

    return df


def _get_materialized_lake_views_schedule(
    lakehouse: Optional[str | UUID] = None, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Get the schedule details for the MaterializedLakeViews job instance.

    Parameters
    ----------
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
        A DataFrame containing the schedule details of the materialized lake views job instance.
    """

    workspace_id = resolve_workspace_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    columns = {
        "Job Schedule Id": "string",
        "Enabled": "bool",
        "Created DateTime": "datetime",
        "Type": "string",
        "Start DateTime": "datetime",
        "End DateTime": "datetime",
        "Local TimeZoneId": "string",
        "Interval": "int",
        "Owner Id": "string",
        "Owner Type": "string",
    }

    df = _create_dataframe(columns=columns)

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/jobs/RefreshMaterializedLakeViews/schedules",
    )

    df = pd.json_normalize(response.json().get("value", []))

    return df


@log
def _delete_materialized_lake_view_schedule(
    schedule_id: UUID,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Delete an existing Refresh MaterializedLakeViews schedule for a lakehouse.

    This is a wrapper function for the following API: `Background Jobs - Delete Refresh Materialized Lake Views Schedule <https://learn.microsoft.com/rest/api/fabric/lakehouse/background-jobs/delete-refresh-materialized-lake-views-schedule>`_.

    Parameters
    ----------
    schedule_id : uuid.UUID
        The ID of the job schedule to delete.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_id = resolve_workspace_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/jobs/RefreshMaterializedLakeViews/schedules/{schedule_id}",
        method="delete",
    )

    print(
        f"{icons.green_dot} The materialized lake view schedule with ID '{schedule_id}' has been deleted from the '{lakehouse_name}' lakehouse within the '{workspace_id}' workspace."
    )


@log
def create_materialized_lake_view(
    name: str,
    query: str,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    replace: bool = False,
    partition_columns: List[str] = None,
    test_run: bool = False,
) -> bool | None:
    """
    Creates a materialized lake view within a lakehouse.

    Requirements: This function must be executed in a PySpark notebook.

    Parameters
    ----------
    name : str
        The name of the materialized lake view (not including the lakehouse or workspace names). This must be in schema_name.view_name format.
    query : str
        The SQL query that defines the materialized lake view. The query must be a valid SQL query that can be executed in the context of the lakehouse.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    replace : bool, default=False
        If True, it will replace the existing materialized lake view if one exists.
    partition_columns : typing.List[str], default=None
        The columns to partition the materialized lake view by.
    test_run : bool, default=False
        If True, the function will indicate whether the materialized lake view would be created successfully without actually creating it. This is useful for validating the input parameters and the SQL query before executing the creation of the materialized lake view.
    """

    import fmlv
    from sempy_labs.lakehouse._schemas import create_schema

    spark = _create_spark_session()

    if test_run:
        try:
            spark.sql(query).limit(1).collect()
            is_valid = True
        except Exception:
            is_valid = False
        return is_valid

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    default_workspace_id = resolve_workspace_id()
    if workspace_id != default_workspace_id:
        raise ValueError(
            f"{icons.red_dot} A materialized lake view may only be created if the notebook's default lakehouse is running in a workspace that is the same as the workspace of the materialized lake view. Please ensure that your notebook is attached to a cluster in the same workspace to avoid any connectivity issues."
        )

    name = name.strip()
    parts = name.split(".")

    if len(parts) == 2:
        schema_name, _ = parts
    elif len(parts) == 1:
        schema_name = None
        #  table_name = parts[0]
    else:
        raise ValueError(f"Invalid table name format: {name}")

    if schema_name:
        create_schema(name=schema_name, lakehouse=lakehouse_id, workspace=workspace_id)

    @fmlv.materialized_lake_view(
        name=f"`{workspace_name}`.`{lakehouse_name}`.{name}",
        partition_cols=partition_columns,
        #  table_properties={"delta.enableChangeDataFeed": "true"}
        replace=replace,
    )
    def view():
        df = spark.sql(query)
        return df

    print(
        f"{icons.green_dot} The materialized lake view '{name}' has been created in the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace."
    )
