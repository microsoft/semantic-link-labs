import time
import pandas as pd
from typing import List, Literal, Optional
from uuid import UUID
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    resolve_item_id,
    resolve_item_name_and_id,
    resolve_workspace_id,
    resolve_workspace_name_and_id,
)
import sempy_labs._icons as icons

_JOB_INSTANCE_COLUMNS = {
    "Job Instance Id": "string",
    "Item Id": "string",
    "Job Type": "string",
    "Invoke Type": "string",
    "Status": "string",
    "Root Activity Id": "string",
    "Start Time UTC": "datetime",
    "End Time UTC": "datetime",
    "Error Code": "string",
    "Error Message": "string",
}

_SCHEDULE_COLUMNS = {
    "Schedule Id": "string",
    "Enabled": "bool",
    "Created Date Time": "datetime",
    "Type": "string",
    "Start Date Time": "datetime",
    "End Date Time": "datetime",
    "Local Time Zone Id": "string",
    "Interval Minutes": "string",
    "Times": "string",
    "Weekdays": "string",
    "Recurrence": "string",
    "Occurrence Type": "string",
    "Day Of Month": "string",
    "Week Index": "string",
    "Weekday": "string",
    "Owner Id": "string",
    "Owner Type": "string",
}

_SCHEDULE_TYPES = ["Cron", "Daily", "Weekly", "Monthly"]
_TERMINAL_JOB_STATUSES = ["Completed", "Failed", "Cancelled", "Deduped"]


def _jobs_url(workspace_id: UUID, item_id: UUID) -> str:
    return f"/v1/workspaces/{workspace_id}/copyJobs/{item_id}/jobs/execute"


def _job_instance_row(v: dict) -> dict:
    failure = v.get("failureReason") or {}
    return {
        "Job Instance Id": v.get("id"),
        "Item Id": v.get("itemId"),
        "Job Type": v.get("jobType"),
        "Invoke Type": v.get("invokeType"),
        "Status": v.get("status"),
        "Root Activity Id": v.get("rootActivityId"),
        "Start Time UTC": v.get("startTimeUtc"),
        "End Time UTC": v.get("endTimeUtc"),
        "Error Code": failure.get("errorCode"),
        "Error Message": failure.get("message"),
    }


def _job_instances_df(rows: List[dict]) -> pd.DataFrame:
    if not rows:
        return _create_dataframe(columns=_JOB_INSTANCE_COLUMNS)

    df = pd.DataFrame(rows, columns=list(_JOB_INSTANCE_COLUMNS.keys()))
    _update_dataframe_datatypes(
        dataframe=df,
        column_map={"Start Time UTC": "datetime", "End Time UTC": "datetime"},
    )
    return df


def _schedule_row(v: dict) -> dict:
    config = v.get("configuration") or {}
    occurrence = config.get("occurrence") or {}
    owner = v.get("owner") or {}
    return {
        "Schedule Id": v.get("id"),
        "Enabled": v.get("enabled"),
        "Created Date Time": v.get("createdDateTime"),
        "Type": config.get("type"),
        "Start Date Time": config.get("startDateTime"),
        "End Date Time": config.get("endDateTime"),
        "Local Time Zone Id": config.get("localTimeZoneId"),
        "Interval Minutes": config.get("interval"),
        "Times": config.get("times"),
        "Weekdays": config.get("weekdays"),
        "Recurrence": config.get("recurrence"),
        "Occurrence Type": occurrence.get("occurrenceType"),
        "Day Of Month": occurrence.get("dayOfMonth"),
        "Week Index": occurrence.get("weekIndex"),
        "Weekday": occurrence.get("weekday"),
        "Owner Id": owner.get("id"),
        "Owner Type": owner.get("type"),
    }


def _schedules_df(rows: List[dict]) -> pd.DataFrame:
    if not rows:
        return _create_dataframe(columns=_SCHEDULE_COLUMNS)

    df = pd.DataFrame(rows, columns=list(_SCHEDULE_COLUMNS.keys()))
    _update_dataframe_datatypes(
        dataframe=df,
        column_map={
            "Enabled": "bool",
            "Created Date Time": "datetime",
            "Start Date Time": "datetime",
            "End Date Time": "datetime",
        },
    )
    return df


def _as_list(value: Optional[str | List[str]]) -> Optional[List[str]]:
    if value is None:
        return None
    return [value] if isinstance(value, str) else list(value)


def _build_schedule_configuration(
    schedule_type: str,
    start_date_time: str,
    end_date_time: str,
    local_time_zone: str,
    interval_minutes: Optional[int] = None,
    times: Optional[str | List[str]] = None,
    weekdays: Optional[str | List[str]] = None,
    recurrence: Optional[int] = None,
    day_of_month: Optional[int] = None,
    week_index: Optional[str] = None,
    weekday: Optional[str] = None,
) -> dict:

    schedule_type = next(
        (t for t in _SCHEDULE_TYPES if t.lower() == str(schedule_type).lower()), None
    )
    if schedule_type is None:
        raise ValueError(
            f"{icons.red_dot} Invalid schedule type. Valid options: {_SCHEDULE_TYPES}."
        )

    missing = [
        name
        for name, value in {
            "start_date_time": start_date_time,
            "end_date_time": end_date_time,
            "local_time_zone": local_time_zone,
        }.items()
        if not value
    ]

    config = {
        "type": schedule_type,
        "startDateTime": start_date_time,
        "endDateTime": end_date_time,
        "localTimeZoneId": local_time_zone,
    }

    if schedule_type == "Cron":
        if interval_minutes is None:
            missing.append("interval_minutes")
        config["interval"] = interval_minutes
    else:
        times = _as_list(times)
        if not times:
            missing.append("times")
        config["times"] = times

    if schedule_type == "Weekly":
        weekdays = _as_list(weekdays)
        if not weekdays:
            missing.append("weekdays")
        config["weekdays"] = weekdays

    if schedule_type == "Monthly":
        if recurrence is None:
            missing.append("recurrence")
        config["recurrence"] = recurrence

        if day_of_month is not None and (week_index or weekday):
            raise ValueError(
                f"{icons.red_dot} Specify either 'day_of_month' or both 'week_index' and 'weekday' for a monthly schedule, not both."
            )
        if day_of_month is not None:
            config["occurrence"] = {
                "occurrenceType": "DayOfMonth",
                "dayOfMonth": day_of_month,
            }
        elif week_index and weekday:
            config["occurrence"] = {
                "occurrenceType": "OrdinalWeekday",
                "weekIndex": week_index,
                "weekday": weekday,
            }
        else:
            missing.append("day_of_month' or 'week_index' and 'weekday")

    if missing:
        missing_str = "', '".join(missing)
        raise ValueError(
            f"{icons.red_dot} The following parameters are required for a '{schedule_type}' schedule: '{missing_str}'."
        )

    return config


@log
def run_copy_job(
    copy_job: str | UUID,
    workspace: Optional[str | UUID] = None,
    wait: bool = True,
) -> pd.DataFrame:
    """
    Runs an on-demand execute job instance of the specified copy job.

    This is a wrapper function for the following API: `Background Jobs - Run On Demand Execute <https://learn.microsoft.com/rest/api/fabric/copyjob/background-jobs/run-on-demand-execute>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    copy_job : str | uuid.UUID
        The copy job name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    wait : bool, default=True
        If True, waits for the job instance to reach a terminal status (Completed, Failed, Cancelled or Deduped).
        If False, returns immediately after the job instance is submitted.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the job instance.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=copy_job, type="CopyJob", workspace=workspace_id
    )
    url = _jobs_url(workspace_id, item_id)

    response = _base_api(
        request=f"{url}/instances",
        method="post",
        status_codes=[202],
        client="fabric_sp",
    )
    job_instance_id = response.headers.get("Location", "").rstrip("/").split("/")[-1]
    retry_after = int(response.headers.get("Retry-After", 5) or 5)

    print(
        f"{icons.in_progress} The execute job for the '{item_name}' copy job within the '{workspace_name}' workspace has been initiated (job instance '{job_instance_id}')."
    )

    while True:
        response = _base_api(
            request=f"{url}/instances/{job_instance_id}", client="fabric_sp"
        )
        result = response.json()
        retry_after = int(
            response.headers.get("Retry-After", retry_after) or retry_after
        )
        status = result.get("status")
        if not wait or status in _TERMINAL_JOB_STATUSES:
            break
        time.sleep(retry_after)

    if status == "Completed":
        print(
            f"{icons.green_dot} The execute job for the '{item_name}' copy job within the '{workspace_name}' workspace has completed."
        )
    elif status in _TERMINAL_JOB_STATUSES:
        message = (result.get("failureReason") or {}).get("message")
        print(
            f"{icons.red_dot} The execute job for the '{item_name}' copy job within the '{workspace_name}' workspace finished with status '{status}'."
            + (f" Error: {message}" if message else "")
        )

    return _job_instances_df([_job_instance_row(result)])


@log
def list_copy_job_instances(
    copy_job: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows the execute job instances of the specified copy job.

    This is a wrapper function for the following API: `Background Jobs - List Execute Job Instances <https://learn.microsoft.com/rest/api/fabric/copyjob/background-jobs/list-execute-job-instances>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    copy_job : str | uuid.UUID
        The copy job name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the execute job instances of the copy job.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=copy_job, type="CopyJob", workspace=workspace_id)

    responses = _base_api(
        request=f"{_jobs_url(workspace_id, item_id)}/instances",
        uses_pagination=True,
        client="fabric_sp",
    )

    return _job_instances_df(
        [_job_instance_row(v) for r in responses for v in r.get("value", [])]
    )


@log
def get_copy_job_instance(
    copy_job: str | UUID,
    job_instance_id: UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the specified execute job instance of a copy job.

    This is a wrapper function for the following API: `Background Jobs - Get Execute Job Instance <https://learn.microsoft.com/rest/api/fabric/copyjob/background-jobs/get-execute-job-instance>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    copy_job : str | uuid.UUID
        The copy job name or ID.
    job_instance_id : uuid.UUID
        The job instance ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the execute job instance.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=copy_job, type="CopyJob", workspace=workspace_id)

    result = _base_api(
        request=f"{_jobs_url(workspace_id, item_id)}/instances/{job_instance_id}",
        client="fabric_sp",
    ).json()

    return _job_instances_df([_job_instance_row(result)])


@log
def create_copy_job_schedule(
    copy_job: str | UUID,
    schedule_type: Literal["Cron", "Daily", "Weekly", "Monthly"],
    start_date_time: str,
    end_date_time: str,
    local_time_zone: str,
    interval_minutes: Optional[int] = None,
    times: Optional[str | List[str]] = None,
    weekdays: Optional[str | List[str]] = None,
    recurrence: Optional[int] = None,
    day_of_month: Optional[int] = None,
    week_index: Optional[Literal["First", "Second", "Third", "Fourth", "Fifth"]] = None,
    weekday: Optional[str] = None,
    enabled: bool = True,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Creates a new execute schedule for the specified copy job.

    This is a wrapper function for the following API: `Background Jobs - Schedule Execute <https://learn.microsoft.com/rest/api/fabric/copyjob/background-jobs/schedule-execute>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    copy_job : str | uuid.UUID
        The copy job name or ID.
    schedule_type : typing.Literal["Cron", "Daily", "Weekly", "Monthly"]
        The type of schedule.

        - Cron: requires 'interval_minutes'.
        - Daily: requires 'times'.
        - Weekly: requires 'times' and 'weekdays'.
        - Monthly: requires 'times', 'recurrence' and either 'day_of_month' or both 'week_index' and 'weekday'.
    start_date_time : str
        The start date and time of the schedule (UTC). Example: "2026-04-28T00:00:00".
        If the start time is in the past, a job is triggered instantly.
    end_date_time : str
        The end date and time of the schedule (UTC). Must be later than the start_date_time. Example: "2026-04-30T23:59:00".
    local_time_zone : str
        The `time zone <https://learn.microsoft.com/windows-hardware/manufacture/desktop/default-time-zones>`_ of the schedule. Example: "Central Standard Time".
    interval_minutes : int, default=None
        The time interval in minutes (between 1 and 5270400). Used by 'Cron' schedules.
    times : str | typing.List[str], default=None
        Time slot(s) in hh:mm format (at most 100). Example: ["00:00", "12:00"]. Used by 'Daily', 'Weekly' and 'Monthly' schedules.
    weekdays : str | typing.List[str], default=None
        Day(s) of the week. Example: ["Monday", "Friday"]. Used by 'Weekly' schedules.
    recurrence : int, default=None
        The monthly repeat interval (between 1 and 12). For example, 1 triggers the job every month. Used by 'Monthly' schedules.
    day_of_month : int, default=None
        The day of the month (between 1 and 31) on which to trigger the job. Used by 'Monthly' schedules.
    week_index : typing.Literal["First", "Second", "Third", "Fourth", "Fifth"], default=None
        The week of the month on which to trigger the job. Used with 'weekday' by 'Monthly' schedules.
    weekday : str, default=None
        The day of the week on which to trigger the job. Example: "Monday". Used with 'week_index' by 'Monthly' schedules.
    enabled : bool, default=True
        Whether the schedule is enabled.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the created schedule.
    """

    payload = {
        "enabled": enabled,
        "configuration": _build_schedule_configuration(
            schedule_type=schedule_type,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            local_time_zone=local_time_zone,
            interval_minutes=interval_minutes,
            times=times,
            weekdays=weekdays,
            recurrence=recurrence,
            day_of_month=day_of_month,
            week_index=week_index,
            weekday=weekday,
        ),
    }

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=copy_job, type="CopyJob", workspace=workspace_id
    )

    result = _base_api(
        request=f"{_jobs_url(workspace_id, item_id)}/schedules",
        method="post",
        payload=payload,
        status_codes=[201],
        client="fabric_sp",
    ).json()

    print(
        f"{icons.green_dot} The execute schedule for the '{item_name}' copy job within the '{workspace_name}' workspace has been created."
    )

    return _schedules_df([_schedule_row(result)])


@log
def list_copy_job_schedules(
    copy_job: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows the execute schedules of the specified copy job.

    This is a wrapper function for the following API: `Background Jobs - List Execute Schedules <https://learn.microsoft.com/rest/api/fabric/copyjob/background-jobs/list-execute-schedules>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    copy_job : str | uuid.UUID
        The copy job name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the execute schedules of the copy job.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=copy_job, type="CopyJob", workspace=workspace_id)

    responses = _base_api(
        request=f"{_jobs_url(workspace_id, item_id)}/schedules",
        uses_pagination=True,
        client="fabric_sp",
    )

    return _schedules_df(
        [_schedule_row(v) for r in responses for v in r.get("value", [])]
    )


@log
def get_copy_job_schedule(
    copy_job: str | UUID,
    schedule_id: UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the specified execute schedule of a copy job.

    This is a wrapper function for the following API: `Background Jobs - Get Execute Schedule <https://learn.microsoft.com/rest/api/fabric/copyjob/background-jobs/get-execute-schedule>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    copy_job : str | uuid.UUID
        The copy job name or ID.
    schedule_id : uuid.UUID
        The schedule ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the execute schedule.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=copy_job, type="CopyJob", workspace=workspace_id)

    result = _base_api(
        request=f"{_jobs_url(workspace_id, item_id)}/schedules/{schedule_id}",
        client="fabric_sp",
    ).json()

    return _schedules_df([_schedule_row(result)])


@log
def update_copy_job_schedule(
    copy_job: str | UUID,
    schedule_id: UUID,
    schedule_type: Optional[Literal["Cron", "Daily", "Weekly", "Monthly"]] = None,
    start_date_time: Optional[str] = None,
    end_date_time: Optional[str] = None,
    local_time_zone: Optional[str] = None,
    interval_minutes: Optional[int] = None,
    times: Optional[str | List[str]] = None,
    weekdays: Optional[str | List[str]] = None,
    recurrence: Optional[int] = None,
    day_of_month: Optional[int] = None,
    week_index: Optional[Literal["First", "Second", "Third", "Fourth", "Fifth"]] = None,
    weekday: Optional[str] = None,
    enabled: Optional[bool] = None,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Updates an existing execute schedule of the specified copy job.

    Only the specified parameters are changed; all other settings are retained from the existing schedule.

    This is a wrapper function for the following API: `Background Jobs - Update Execute Schedule <https://learn.microsoft.com/rest/api/fabric/copyjob/background-jobs/update-execute-schedule>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    copy_job : str | uuid.UUID
        The copy job name or ID.
    schedule_id : uuid.UUID
        The schedule ID.
    schedule_type : typing.Literal["Cron", "Daily", "Weekly", "Monthly"], default=None
        The type of schedule. See :func:`sempy_labs.copy_job.create_copy_job_schedule` for the parameters required by each type.
        Defaults to None which retains the existing schedule type.
    start_date_time : str, default=None
        The start date and time of the schedule (UTC). Example: "2026-04-28T00:00:00".
        Defaults to None which retains the existing value.
    end_date_time : str, default=None
        The end date and time of the schedule (UTC). Must be later than the start_date_time.
        Defaults to None which retains the existing value.
    local_time_zone : str, default=None
        The `time zone <https://learn.microsoft.com/windows-hardware/manufacture/desktop/default-time-zones>`_ of the schedule.
        Defaults to None which retains the existing value.
    interval_minutes : int, default=None
        The time interval in minutes (between 1 and 5270400). Used by 'Cron' schedules.
        Defaults to None which retains the existing value.
    times : str | typing.List[str], default=None
        Time slot(s) in hh:mm format (at most 100). Used by 'Daily', 'Weekly' and 'Monthly' schedules.
        Defaults to None which retains the existing value.
    weekdays : str | typing.List[str], default=None
        Day(s) of the week. Used by 'Weekly' schedules.
        Defaults to None which retains the existing value.
    recurrence : int, default=None
        The monthly repeat interval (between 1 and 12). Used by 'Monthly' schedules.
        Defaults to None which retains the existing value.
    day_of_month : int, default=None
        The day of the month (between 1 and 31) on which to trigger the job. Used by 'Monthly' schedules.
        Defaults to None which retains the existing value.
    week_index : typing.Literal["First", "Second", "Third", "Fourth", "Fifth"], default=None
        The week of the month on which to trigger the job. Used with 'weekday' by 'Monthly' schedules.
        Defaults to None which retains the existing value.
    weekday : str, default=None
        The day of the week on which to trigger the job. Used with 'week_index' by 'Monthly' schedules.
        Defaults to None which retains the existing value.
    enabled : bool, default=None
        Whether the schedule is enabled.
        Defaults to None which retains the existing value.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the updated schedule.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=copy_job, type="CopyJob", workspace=workspace_id
    )
    url = f"{_jobs_url(workspace_id, item_id)}/schedules/{schedule_id}"

    existing = _base_api(request=url, client="fabric_sp").json()
    config = existing.get("configuration") or {}
    occurrence = config.get("occurrence") or {}

    # Only inherit the existing monthly occurrence if no occurrence parameter was provided, so a
    # DayOfMonth occurrence can be swapped for an OrdinalWeekday one (and vice versa).
    if day_of_month is None and week_index is None and weekday is None:
        day_of_month = occurrence.get("dayOfMonth")
        week_index = occurrence.get("weekIndex")
        weekday = occurrence.get("weekday")

    payload = {
        "enabled": existing.get("enabled") if enabled is None else enabled,
        "configuration": _build_schedule_configuration(
            schedule_type=schedule_type or config.get("type"),
            start_date_time=start_date_time or config.get("startDateTime"),
            end_date_time=end_date_time or config.get("endDateTime"),
            local_time_zone=local_time_zone or config.get("localTimeZoneId"),
            interval_minutes=(
                config.get("interval") if interval_minutes is None else interval_minutes
            ),
            times=config.get("times") if times is None else times,
            weekdays=config.get("weekdays") if weekdays is None else weekdays,
            recurrence=config.get("recurrence") if recurrence is None else recurrence,
            day_of_month=day_of_month,
            week_index=week_index,
            weekday=weekday,
        ),
    }

    result = _base_api(
        request=url,
        method="patch",
        payload=payload,
        client="fabric_sp",
    ).json()

    print(
        f"{icons.green_dot} The '{schedule_id}' execute schedule for the '{item_name}' copy job within the '{workspace_name}' workspace has been updated."
    )

    return _schedules_df([_schedule_row(result)])


@log
def delete_copy_job_schedule(
    copy_job: str | UUID,
    schedule_id: UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Deletes the specified execute schedule of a copy job.

    This is a wrapper function for the following API: `Background Jobs - Delete Execute Schedule <https://learn.microsoft.com/rest/api/fabric/copyjob/background-jobs/delete-execute-schedule>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    copy_job : str | uuid.UUID
        The copy job name or ID.
    schedule_id : uuid.UUID
        The schedule ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=copy_job, type="CopyJob", workspace=workspace_id
    )

    _base_api(
        request=f"{_jobs_url(workspace_id, item_id)}/schedules/{schedule_id}",
        method="delete",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{schedule_id}' execute schedule for the '{item_name}' copy job within the '{workspace_name}' workspace has been deleted."
    )
