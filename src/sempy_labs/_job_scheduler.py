from sempy._utils._log import log
import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    resolve_item_type,
    _update_dataframe_datatypes,
    _base_api,
    _print_success,
    _create_dataframe,
)
from uuid import UUID
import sempy_labs._icons as icons
import json


def __create_item_schedule_payload(
    enabled: bool = True,
    start_date_time: str = None,
    end_date_time: str = None,
    local_time_zone_id: str = None,
    schedule_config_type: str = None,
    interval: int = None,
    weekdays: str = None,
    times: str | list = None,
) -> dict:
    """
    Returns an API payload for creating or updating a item job schedule.

    There are three types of schedules configuration: "CronScheduleConfig", "DailySchedulConfig", "WeeklyScheduleConfig".
    Each configuration type has different properties. Depending on the configuration type, the payload will be different.

    Returns
    -------
    dict
    """
    payload = {
                "enabled": enabled,
                "configuration": {
                    "startDateTime": start_date_time,
                    "endDateTime": end_date_time,
                    "localTimeZoneId": local_time_zone_id,
                    "type": schedule_config_type
                }
            }

    match schedule_config_type.lower():
        case 'cron':
            payload["configuration"]["interval"] = interval
        case 'daily':
            payload["configuration"]["times"] = str(times)
        case 'weekly':
            payload["configuration"]["times"] = times
            payload["configuration"]["weekdays"] = weekdays

    return payload


@log
def list_item_job_instances(
    item: str | UUID,
    item_type: Optional[str] = None,
    workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Returns a list of job instances for the specified item.

    This is a wrapper function for the following API: `Job Scheduler - List Item Job Instances <https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/list-item-job-instances>`_.

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID.
    item_type : str, default=None
        The `item type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_ is required when specifying the item name instead of the item id.
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
        item=item, type=item_type, workspace=workspace
    )
    if item_type is None:
        item_type = resolve_item_type(item_id=item_id, workspace_id=workspace_id)

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
                "Item Type": item_type,
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
    item_type: Optional[str] = None,
    job_type: str = None,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Get scheduling settings for one specific item.

    This is a wrapper function for the following API: `Job Scheduler - List Item Schedules <https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/list-item-schedules>`_.

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID.
    item_type : str, default=None
        The `item type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_ is required when specifying the item name instead of the item id.
    job_type : str, default=None
        The job type. Available options: "Pipeline", "RunNotebook", "CopyJob", "sparkjob".
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
        item=item, type=item_type, workspace=workspace
    )

    columns = {
        "Job Schedule Id": "string",
        "Enabled": "bool",
        "Created Date Time": "datetime",
        "Start Date Time": "datetime",
        "End Date Time": "string",
        "Local Time Zone Id": "string",
        "Type": "string",
        "Interval": "int_fillna",
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
            "Times": json.dumps(config.get("times")),
            "Owner Id": own.get("id"),
            "Owner Type": own.get("type"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def run_on_demand_item_job(
    item: str | UUID,
    item_type: Optional[str] = None,
    job_type: str = None,
    workspace: Optional[str | UUID] = None,
) -> UUID:
    """
    Run on-demand item job instance. Returns the job instance id for the on-demand job.

    This is a wrapper function for the following API: `Job Scheduler - Run On Demand Item Job <https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/run-on-demand-item-job>`_.

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID
    item_type : str, default=None
        The `item type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_ is required when specifying the item name instead of the item id.
    job_type : str, default=None
        The job type. Available options: "Pipeline", "RunNotebook", "CopyJob", "sparkjob".
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    UUID
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=item, type=item_type, workspace=workspace
    )
    if item_type is None:
        item_type = resolve_item_type(item_id=item_id, workspace_id=workspace_id)

    response = _base_api(
        request=f"v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances?jobType={job_type}",
        method="post",
        status_codes=202,
    )

    print(f"{icons.green_dot} The '{item_name}' {item_type.lower()} has been executed.")
    job_instance_id = response.headers.get("Location").split("/")[-1]
    return job_instance_id


@log
def get_item_schedule(
    item: str | UUID,
    schedule_id: str | UUID,
    item_type: Optional[str] = None,
    job_type: str = None,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Get scheduling settings for one specific item.

    This is a wrapper function for the following API: `Job Scheduler - Get Item Schedule <https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/get-item-schedule>`_.

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID.
    schedule_id : str | uuid.UUID
        The schedule ID.
    item_type : str, default=None
        The `item type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_ is required when specifying the item name instead of the item id.
    job_type : str, default=None
        The job type. Available options: "Pipeline", "RunNotebook", "CopyJob", "sparkjob".
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Shows a list of scheduling settings for one specific item.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=item, type=item_type, workspace=workspace
    )

    columns = {
        "Job Schedule Id": "string",
        "Enabled": "bool",
        "Created Date Time": "datetime",
        "Start Date Time": "datetime",
        "End Date Time": "string",
        "Local Time Zone Id": "string",
        "Type": "string",
        "Interval": "int_fillna",
        "Weekdays": "string",
        "Times": "string",
        "Owner Id": "string",
        "Owner Type": "string",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(
        request=f"v1/workspaces/{workspace_id}/items/{item_id}/jobs/{job_type}/schedules/{schedule_id}"
    )

    v = response.json()
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
        "Times": json.dumps(config.get("times")),
        "Owner Id": own.get("id"),
        "Owner Type": own.get("type"),
    }
    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    _update_dataframe_datatypes(dataframe=df, column_map=columns)
    return df


@log
def create_item_schedule(
    item: str | UUID,
    item_type: Optional[str] = None,
    job_type: str = None,
    workspace: Optional[str | UUID] = None,
    start_date_time: str = None,
    end_date_time: str = None,
    local_time_zone_id: str = None,
    schedule_config_type: str = None,
    enabled: bool = True,
    interval: int = None,
    weekdays: str = None,
    times: str | list = None,
) -> UUID:
    """
    Create a new scheduling setting for one specific item. Return the ID of the created schedule.

    This is a wrapper function for the following API: `Job Scheduler - Create Item Schedule <https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/create-item-schedule>`_.

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID.
    item_type : str, default=None
        The `item type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_ is required when specifying the item name instead of the item id.
    schedule_config_type : str, default=None
        The schedule configuration type. Available options: "Cron", "Daily", "Weekly"
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    job_type : str, default=None
        The job type. Currently available options: "Pipeline", "RunNotebook", "CopyJob", "sparkjob"
    enabled : bool, default=True
        The schedule is enabled.
    start_date_time : str, default=None
        The start time for this schedule. If the start time is in the past, it will trigger a job instantly.
    end_date_time : str, default=None
        The end time for this schedule. The end time must be later than the start time.
    local_time_zone_id : str, default=None
        The time zone identifier registry on local computer for windows, see <https://learn.microsoft.com/en-us/windows-hardware/manufacture/desktop/default-time-zones>.
    interval : int, default=None
        The time interval in minutes. A number between 1 and 5270400 (10 years).
    weekdays : str, default=None
        The weekdays. Available options: "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"
    times : str | list, default=None
        A list of time slots in hh:mm format, at most 100 elements are allowed.

    Returns
    -------
    UUID
    """

    valid_types = {'cron', 'daily', 'weekly'}
    if schedule_config_type.lower() not in valid_types:
        raise ValueError("Type must be one of these values: %r." % valid_types)
    if interval is not None and interval < 1 or interval > 5270400:
        raise ValueError("Interval must be a number between 1 and 5270400.")
    if len(str(times).split('.')) > 100:
        raise ValueError("At most 100 elements are allowed in the times list.")

    payload = __create_item_schedule_payload(
        enabled=enabled, start_date_time=start_date_time, end_date_time=end_date_time, local_time_zone_id=local_time_zone_id,
        schedule_config_type=schedule_config_type, interval=interval, weekdays=weekdays, times=times
    )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=item, type=item_type, workspace=workspace
    )

    response = _base_api(
        request=f"v1/workspaces/{workspace_id}/items/{item_id}/jobs/{job_type}/schedules",
        method="post",
        status_codes=201,
        payload=payload,
    )
    _print_success(
        item_name=item_name,
        item_type="schedule",
        workspace_name=workspace_name,
        action="created",
    )

    schedule_id = response.headers.get("Location").split("/")[-1]
    return schedule_id

@log
def update_item_schedule(
    item: str | UUID,
    schedule_id: str | UUID,
    item_type: Optional[str] = None,
    job_type: str = None,
    workspace: Optional[str | UUID] = None,
    start_date_time: str = None,
    end_date_time: str = None,
    local_time_zone_id: str = None,
    schedule_config_type: str = None,
    enabled: bool = True,
    interval: int = None,
    weekdays: str = None,
    times: str | list = None,
):
    """
    Update an existing scheduling setting for one specific item.

    This is a wrapper function for the following API: `Job Scheduler - Update Item Schedule <https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/update-item-schedule>`_.

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID
    schedule_id : str | uuid.UUID
        The schedule ID.
    item_type : str, default=None
        The `item type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_ is required when specifying the item name instead of the item id.
    schedule_config_type : str, default=None
        The schedule configuration type. Available options: "Cron", "Daily", "Weekly"
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    job_type : str, default=True
        The job type. Currently available options: "Pipeline", "RunNotebook", "CopyJob", "sparkjob"
    enabled : bool, default=True
        The schedule is enabled.
    start_date_time : str, default=None
        The start time for this schedule. If the start time is in the past, it will trigger a job instantly.
    end_date_time : str, default=None
        The end time for this schedule. The end time must be later than the start time.
    local_time_zone_id : str, default=None
        The time zone identifier registry on local computer for windows, see <https://learn.microsoft.com/en-us/windows-hardware/manufacture/desktop/default-time-zones>.
    interval : int, default=None
        The time interval in minutes. A number between 1 and 5270400 (10 years).
    weekdays : str, default=None
        The weekdays. Available options: "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"
    times : str | list, default=None
        A list of time slots in hh:mm format, at most 100 elements are allowed.
    """

    valid_types = {'cron', 'daily', 'weekly'}
    if schedule_config_type.lower() not in valid_types:
        raise ValueError("Type must be one of these values: %r." % valid_types)
    if interval is not None and interval < 1 or interval > 5270400:
        raise ValueError("Interval must be a number between 1 and 5270400.")
    if len(str(times).split('.')) > 100:
        raise ValueError("At most 100 elements are allowed in the times list.")

    payload = __create_item_schedule_payload(
        enabled=enabled, start_date_time=start_date_time, end_date_time=end_date_time, local_time_zone_id=local_time_zone_id,
        schedule_config_type=schedule_config_type, interval=interval, weekdays=weekdays, times=times
    )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=item, type=item_type, workspace=workspace
    )

    _base_api(
        request=f"v1/workspaces/{workspace_id}/items/{item_id}/jobs/{job_type}/schedules/{schedule_id}",
        method="patch",
        status_codes=200,
        payload=payload,
    )

    print(f"{icons.green_dot} The '{item_name}' schedule in workspace {workspace_name} has been updated.")


@log
def get_item_job_instance(
    item: str | UUID,
    job_instance_id: str | UUID,
    item_type: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Get a specific job instance for the specified item.

    This is a wrapper function for the following API: `Job Scheduler - Get Item Job Instance <https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/get-item-job-instance>`_.

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID
    job_instance_id : str | uuid.UUID
        The job instance ID.
    item_type : str, default=None
        The `item type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_ is required when specifying the item name instead of the item id.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Shows a specific job instance for the specified item.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=item, type=item_type, workspace=workspace
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

    response = _base_api(
        request=f"v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances/{job_instance_id}"
    )

    v = response.json()
    fail = v.get("failureReason", {})
    new_data = {
        "Job Instance Id": v.get("id"),
        "Item Name": item_name,
        "Item Id": v.get("itemId"),
        "Item Type": item_type,
        "Job Type": v.get("jobType"),
        "Invoke Type": v.get("invokeType"),
        "Status": v.get("status"),
        "Root Activity Id": v.get("rootActivityId"),
        "Start Time UTC": v.get("startTimeUtc"),
        "End Time UTC": v.get("endTimeUtc"),
        "Error Message": fail.get("message") if fail is not None else "",
    }
    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    _update_dataframe_datatypes(dataframe=df, column_map=columns)
    return df


@log
def cancel_item_job_instance(
    item: str | UUID,
    job_instance_id: str | UUID,
    item_type: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Cancel an existing job instance for the specified item.

    This is a wrapper function for the following API: `Job Scheduler - Cancel Item Job Instance <https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/cancel-item-job-instance>`_.

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID
    job_instance_id : str | uuid.UUID
        The job instance ID.
    item_type : str, default=None
        The `item type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_ is required when specifying the item name instead of the item id.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=item, type=item_type, workspace=workspace
    )

    _base_api(
        request=f"v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances/{job_instance_id}/cancel",
        method="post",
        status_codes=202,
    )
    print(f"{icons.green_dot} The '{item_name}' job instance has been cancelled.")