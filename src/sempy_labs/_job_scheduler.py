from sempy._utils._log import log
import pandas as pd
from typing import Optional, List
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    _update_dataframe_datatypes,
    _base_api,
    _create_dataframe,
)
from uuid import UUID
import sempy_labs._icons as icons


@log
def list_item_job_instances(
    item: str | UUID, type: Optional[str] = None, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Returns a list of job instances for the specified item.

    This is a wrapper function for the following API: `Job Scheduler - List Item Job Instances <https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/list-item-job-instances>`_.

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID
    type : str, default=None
        The item `type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_. If specifying the item name as the item, the item type is required.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Shows a list of job instances for the specified item.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=item, type=type, workspace=workspace
    )

    columns = {
        "Job Instance Id": "string",
        "Item Name": "string",
        "Item Id": "string",
        "Item Type": "string",
        "Job Type": "string",
        "Invoke Type": "string",
        "Status": "string",
        "Root Activity Id": "string",
        "Start Time UTC": "datetime",
        "End Time UTC": "string",
        "Error Message": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances",
        uses_pagination=True,
    )

    if not responses[0].get("value"):
        return df

    dfs = []
    for r in responses:
        for v in r.get("value", []):
            fail = v.get("failureReason", {})
            new_data = {
                "Job Instance Id": v.get("id"),
                "Item Name": item_name,
                "Item Id": v.get("itemId"),
                "Item Type": type,
                "Job Type": v.get("jobType"),
                "Invoke Type": v.get("invokeType"),
                "Status": v.get("status"),
                "Root Activity Id": v.get("rootActivityId"),
                "Start Time UTC": v.get("startTimeUtc"),
                "End Time UTC": v.get("endTimeUtc"),
                "Error Message": fail.get("message") if fail is not None else "",
            }
            dfs.append(pd.DataFrame(new_data, index=[0]))

    if dfs:
        df = pd.concat(dfs, ignore_index=True)

    df = _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def list_item_schedules(
    item: str | UUID,
    type: Optional[str] = None,
    job_type: str = "DefaultJob",
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Get scheduling settings for one specific item.

    This is a wrapper function for the following API: `Job Scheduler - List Item Schedules <https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/list-item-schedules>`_.

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID
    type : str, default=None
        The item `type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_. If specifying the item name as the item, the item type is required.
    job_type : str, default="DefaultJob"
        The job type.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Shows a list of scheduling settings for one specific item.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=item, type=type, workspace=workspace
    )

    columns = {
        "Job Schedule Id": "string",
        "Enabled": "bool",
        "Created Date Time": "datetime",
        "Start Date Time": "datetime",
        "End Date Time": "string",
        "Local Time Zone Id": "string",
        "Type": "string",
        "Interval": "string",
        "Weekdays": "string",
        "Times": "string",
        "Owner Id": "string",
        "Owner Type": "string",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(
        request=f"v1/workspaces/{workspace_id}/items/{item_id}/jobs/{job_type}/schedules"
    )

    for v in response.json().get("value", []):
        config = v.get("configuration", {})
        own = v.get("owner", {})
        new_data = {
            "Job Schedule Id": v.get("id"),
            "Enabled": v.get("enabled"),
            "Created Date Time": v.get("createdDateTime"),
            "Start Date Time": config.get("startDateTime"),
            "End Date Time": config.get("endDateTime"),
            "Local Time Zone Id": config.get("localTimeZoneId"),
            "Type": config.get("type"),
            "Interval": config.get("interval"),
            "Weekdays": config.get("weekdays"),
            "Times": config.get("times"),
            "Owner Id": own.get("id"),
            "Owner Type": own.get("type"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def run_on_demand_item_job(
    item: str | UUID,
    type: Optional[str] = None,
    job_type: str = "DefaultJob",
    workspace: Optional[str | UUID] = None,
):
    """
    Run on-demand item job instance.

    This is a wrapper function for the following API: `Job Scheduler - Run On Demand Item Job <https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/run-on-demand-item-job>`_.

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID.
    type : str, default=None
        The item `type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_. If specifying the item name as the item, the item type is required.
    job_type : str, default="DefaultJob"
        The job type.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=item, type=type, workspace=workspace
    )

    _base_api(
        request=f"v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances?jobType={job_type}",
        method="post",
        lro_return_status_code=True,
        status_codes=202,
    )

    print(f"{icons.green_dot} The '{item_name}' {type.lower()} has been executed.")


def create_item_schedule_cron(
    item: str | UUID,
    type: str,
    start_date_time: str,
    end_date_time: str,
    local_time_zone: str,
    job_type: str = "DefaultJob",
    interval_minutes: int = 10,
    enabled: bool = True,
    workspace: Optional[str | UUID] = None,
):
    """
    Create a new schedule for an item based on a `chronological time <https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/create-item-schedule?tabs=HTTP#cronscheduleconfig>`_.

    This is a wrapper function for the following API: `Job Scheduler - Create Item Schedule <https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/create-item-schedule>`_.

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID.
    type : str
        The item `type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_. If specifying the item name as the item, the item type is required.
    start_date_time: str
        The start date and time of the schedule. Example: "2024-04-28T00:00:00".
    end_date_time: str
        The end date and time of the schedule. Must be later than the start_date_time. Example: "2024-04-30T23:59:00".
    local_time_zone: str
        The `time zone <https://learn.microsoft.com/windows-hardware/manufacture/desktop/default-time-zones?view=windows-11>`_ of the schedule. Example: "Central Standard Time".
    job_type : str, default="DefaultJob"
        The job type.
    interval_minutes: int, default=10
        The schedule interval (in minutes).
    enabled: bool, default=True
        Whether the schedule is enabled.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=item, type=type, workspace=workspace
    )

    payload = {
        "enabled": enabled,
        "configuration": {
            "startDateTime": start_date_time,
            "endDateTime": end_date_time,
            "localTimeZoneId": local_time_zone,
            "type": "Cron",
            "interval": interval_minutes,
        },
    }

    _base_api(
        request=f"v1/workspaces/{workspace_id}/items/{item_id}/jobs/{job_type}/schedules",
        method="post",
        payload=payload,
        status_codes=201,
    )

    print(
        f"{icons.green_dot} The schedule for the '{item_name}' {type.lower()} has been created."
    )


def create_item_schedule_daily(
    item: str | UUID,
    type: str,
    start_date_time: str,
    end_date_time: str,
    local_time_zone: str,
    times: List[str],
    job_type: str = "DefaultJob",
    enabled: bool = True,
    workspace: Optional[str | UUID] = None,
):
    """
    Create a new daily schedule for an item.

    This is a wrapper function for the following API: `Job Scheduler - Create Item Schedule <https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/create-item-schedule>`_.

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID.
    type : str
        The item `type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_. If specifying the item name as the item, the item type is required.
    start_date_time: str
        The start date and time of the schedule. Example: "2024-04-28T00:00:00".
    end_date_time: str
        The end date and time of the schedule. Must be later than the start_date_time. Example: "2024-04-30T23:59:00".
    local_time_zone: str
        The `time zone <https://learn.microsoft.com/windows-hardware/manufacture/desktop/default-time-zones?view=windows-11>`_ of the schedule. Example: "Central Standard Time".
    times : List[str]
        A list of time slots in hh:mm format, at most 100 elements are allowed. Example: ["00:00", "12:00"].
    job_type : str, default="DefaultJob"
        The job type.
    enabled: bool, default=True
        Whether the schedule is enabled.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=item, type=type, workspace=workspace
    )

    payload = {
        "enabled": enabled,
        "configuration": {
            "startDateTime": start_date_time,
            "endDateTime": end_date_time,
            "localTimeZoneId": local_time_zone,
            "type": "Daily",
            "times": times,
        },
    }

    _base_api(
        request=f"v1/workspaces/{workspace_id}/items/{item_id}/jobs/{job_type}/schedules",
        method="post",
        payload=payload,
        status_codes=201,
    )

    print(
        f"{icons.green_dot} The schedule for the '{item_name}' {type.lower()} has been created."
    )


def create_item_schedule_weekly(
    item: str | UUID,
    type: str,
    start_date_time: str,
    end_date_time: str,
    local_time_zone: str,
    times: List[str],
    weekdays: List[str],
    job_type: str = "DefaultJob",
    enabled: bool = True,
    workspace: Optional[str | UUID] = None,
):
    """
    Create a new daily schedule for an item.

    This is a wrapper function for the following API: `Job Scheduler - Create Item Schedule <https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/create-item-schedule>`_.

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID.
    type : str
        The item `type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_. If specifying the item name as the item, the item type is required.
    start_date_time: str
        The start date and time of the schedule. Example: "2024-04-28T00:00:00".
    end_date_time: str
        The end date and time of the schedule. Must be later than the start_date_time. Example: "2024-04-30T23:59:00".
    local_time_zone: str
        The `time zone <https://learn.microsoft.com/windows-hardware/manufacture/desktop/default-time-zones?view=windows-11>`_ of the schedule. Example: "Central Standard Time".
    times : List[str]
        A list of time slots in hh:mm format, at most 100 elements are allowed. Example: ["00:00", "12:00"].
    weekdays : List[str]
        A list of weekdays. Example: ["Monday", "Tuesday"].
    job_type : str, default="DefaultJob"
        The job type.
    enabled: bool, default=True
        Whether the schedule is enabled.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=item, type=type, workspace=workspace
    )

    weekdays = [w.capitalize() for w in weekdays]
    weekday_list = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
    ]
    for weekday in weekdays:
        if weekday not in weekday_list:
            raise ValueError(
                f"{icons.red_dot} Invalid weekday: {weekday}. Must be one of {weekday_list}."
            )

    payload = {
        "enabled": enabled,
        "configuration": {
            "startDateTime": start_date_time,
            "endDateTime": end_date_time,
            "localTimeZoneId": local_time_zone,
            "type": "Weekly",
            "times": times,
            "weekdays": weekdays,
        },
    }

    _base_api(
        request=f"v1/workspaces/{workspace_id}/items/{item_id}/jobs/{job_type}/schedules",
        method="post",
        payload=payload,
        status_codes=201,
    )

    print(
        f"{icons.green_dot} The schedule for the '{item_name}' {type.lower()} has been created."
    )
