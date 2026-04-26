import pandas as pd
from typing import Optional
from dateutil.parser import parse as dtparser
from datetime import datetime, timedelta
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    execute_in_timeslots,
)
import sempy_labs._icons as icons


@log
def list_activity_events(
    start_time: str,
    end_time: str,
    activity_filter: Optional[str] = None,
    user_id_filter: Optional[str] = None,
    return_dataframe: bool = True,
) -> pd.DataFrame | dict:
    """
    Shows a list of audit activity events for a tenant.

    This is a wrapper function for the following API: `Admin - Get Activity Events <https://learn.microsoft.com/rest/api/power-bi/admin/get-activity-events>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    start_time : str
        Start date and time of the window for audit event results. Example: "2024-09-25T07:55:00".
    end_time : str
        End date and time of the window for audit event results. Example: "2024-09-25T08:55:00".
    activity_filter : str, default=None
        Filter value for activities. Example: 'viewreport'.
    user_id_filter : str, default=None
        Email address of the user.
    return_dataframe : bool, default=True
        If True the response is a pandas.DataFrame. If False returns the original Json. Default True

    Returns
    -------
    pandas.DataFrame | dict
        A pandas dataframe or json showing a list of audit activity events for a tenant.
    """
    start_dt = dtparser(start_time)
    end_dt = dtparser(end_time)

    if not start_dt.date() == end_dt.date():
        raise ValueError(
            f"{icons.red_dot} Start and End Times must be within the same UTC day. Please refer to the documentation here: https://learn.microsoft.com/rest/api/power-bi/admin/get-activity-events#get-audit-activity-events-within-a-time-window-and-for-a-specific-activity-type-and-user-id-example"
        )

    columns = {
        "Id": "string",
        "Record Type": "string",
        "Creation Time": "datetime",
        "Operation": "string",
        "Organization Id": "string",
        "User Type": "string",
        "User Key": "string",
        "Workload": "string",
        "Result Status": "string",
        "User Id": "string",
        "Client IP": "string",
        "User Agent": "string",
        "Activity": "string",
        "Workspace Name": "string",
        "Workspace Id": "string",
        "Workspaces Semicolon Delimited List": "string",
        "Object Id": "string",
        "Request Id": "string",
        "Object Type": "string",
        "Object Display Name": "string",
        "Experience": "string",
        "Refresh Enforcement Policy": "string",
        "Is Success": "bool",
        "Activity Id": "string",
        "Item Name": "string",
        "Dataset Name": "string",
        "Report Name": "string",
        "Capacity Id": "string",
        "Capacity Name": "string",
        "App Name": "string",
        "Dataset Id": "string",
        "Report Id": "string",
        "Artifact Id": "string",
        "Artifact Name": "string",
        "Report Type": "string",
        "App Report Id": "string",
        "Distribution Method": "string",
        "Consumption Method": "string",
        "Artifact Kind": "string",
    }
    df = _create_dataframe(columns=columns)

    response_json = {"activityEventEntities": []}
    url = f"/v1.0/myorg/admin/activityevents?startDateTime='{start_time}'&endDateTime='{end_time}'"

    conditions = []
    if activity_filter is not None:
        conditions.append(f"Activity eq '{activity_filter}'")
    if user_id_filter is not None:
        conditions.append(f"UserId eq '{user_id_filter}'")

    if conditions:
        url += f"&$filter={f' and '.join(conditions)}"

    responses = _base_api(request=url, client="fabric_sp", uses_pagination=True)

    rows = []
    for r in responses:
        if return_dataframe:
            for i in r.get("activityEventEntities", []):
                rows.append(
                    {
                        "Id": i.get("Id"),
                        "Record Type": i.get("RecordType"),
                        "Creation Time": i.get("CreationTime"),
                        "Operation": i.get("Operation"),
                        "Organization Id": i.get("OrganizationId"),
                        "User Type": i.get("UserType"),
                        "User Key": i.get("UserKey"),
                        "Workload": i.get("Workload"),
                        "Result Status": i.get("ResultStatus"),
                        "User Id": i.get("UserId"),
                        "Client IP": i.get("ClientIP"),
                        "User Agent": i.get("UserAgent"),
                        "Activity": i.get("Activity"),
                        "Workspace Name": i.get("WorkSpaceName"),
                        "Workspace Id": i.get("WorkspaceId"),
                        "Workspaces Semicolon Delimited List": i.get(
                            "WorkspacesSemicolonDelimitedList"
                        ),
                        "Object Id": i.get("ObjectId"),
                        "Request Id": i.get("RequestId"),
                        "Object Type": i.get("ObjectType"),
                        "Object Display Name": i.get("ObjectDisplayName"),
                        "Experience": i.get("Experience"),
                        "Refresh Enforcement Policy": i.get("RefreshEnforcementPolicy"),
                        "Is Success": i.get("IsSuccess"),
                        "Activity Id": i.get("ActivityId"),
                        "Item Name": i.get("ItemName"),
                        "Dataset Name": i.get("DatasetName"),
                        "Report Name": i.get("ReportName"),
                        "Capacity Id": i.get("CapacityId"),
                        "Capacity Name": i.get("CapacityName"),
                        "App Name": i.get("AppName"),
                        "Dataset Id": i.get("DatasetId"),
                        "Report Id": i.get("ReportId"),
                        "Artifact Id": i.get("ArtifactId"),
                        "Artifact Name": i.get("ArtifactName"),
                        "Report Type": i.get("ReportType"),
                        "App Report Id": i.get("AppReportId"),
                        "Distribution Method": i.get("DistributionMethod"),
                        "Consumption Method": i.get("ConsumptionMethod"),
                        "Artifact Kind": i.get("ArtifactKind"),
                    }
                )
        else:
            response_json["activityEventEntities"].extend(
                r.get("activityEventEntities")
            )

    if return_dataframe:
        if rows:
            df = pd.DataFrame(rows, columns=list(columns.keys()))
            _update_dataframe_datatypes(dataframe=df, column_map=columns)
        return df
    else:
        return response_json


@log
def list_activity_events_multiple_days(
    start_day: str,
    num_days: int,
    inc_days: int = 1,
    activity_filter: Optional[str] = None,
    user_id_filter: Optional[str] = None,
    return_dataframe: bool = True,
) -> pd.DataFrame | dict:
    """
    Retrieves audit activity events for multiple days by repeatedly calling the
    `Admin - Get Activity Events` API for each day in the generated window.

    This is a wrapper around the single-day `list_activity_events` function.
    For each iteration, it constructs a full-day ISO8601 time window
    (00:00:00.000Z → 23:59:59.999Z) and executes the calls in rate-limited
    timeslots.

    Parameters
    ----------
    start_day : str
        The first day to query, in format ``"YYYY-MM-DD"``.
        Example: ``"2024-09-25"``.

    num_days : int
        Number of day-windows to retrieve. A value of 3 means three separate
        24-hour windows will be queried.

    inc_days : int, default=1
        Step size (in days) between each window.
        Example: ``inc_days=2`` queries every other day.

    activity_filter : str, optional
        Filter for activity types.
        Example: ``"viewreport"``.

    user_id_filter : str, optional
        Email address of the user to filter on.

    return_dataframe : bool, default=True
        If True, returns a pandas DataFrame.
        If False, returns a dict.

    Returns
    -------
    pandas.DataFrame or dict
        Aggregated results from all generated day-windows, returned either as a
        concatenated DataFrame or a dict depending on `return_dataframe`.
    """

    func_name = "list_activity_events"
    parameters_list = []
    max_per_slot = 200
    slot_seconds = 60
    namespace = globals()

    # Normalize start_day
    start_date = datetime.strptime(start_day, "%Y-%m-%d").date()

    # Build the list of tasks
    for i in range(num_days):
        day = start_date + timedelta(days=i * inc_days)

        start_iso = day.strftime("%Y-%m-%dT00:00:00.000Z")
        end_iso = day.strftime("%Y-%m-%dT23:59:59.999Z")

        parameters_list.append(
            {
                "start_time": start_iso,
                "end_time": end_iso,
                "activity_filter": activity_filter,
                "user_id_filter": user_id_filter,
                "return_dataframe": return_dataframe,
            }
        )

    results = execute_in_timeslots(
        func_name, parameters_list, max_per_slot, slot_seconds, namespace
    )

    return results
