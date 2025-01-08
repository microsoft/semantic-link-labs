import sempy.fabric as fabric
import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    pagination,
    lro,
)
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID
import sempy_labs._icons as icons


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

    client = fabric.FabricRestClient()
    response = client.get(
        f"v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(
        columns=[
            "Job Instance Id",
            "Item Name",
            "Item Id",
            "Item Type",
            "Job Type",
            "Invoke Type",
            "Status",
            "Root Activity Id",
            "Start Time UTC",
            "End Time UTC",
            "Failure Reason",
        ]
    )

    responses = pagination(client, response)

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

    return df


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

    df = pd.DataFrame(
        columns=[
            "Job Schedule Id",
            "Enabled",
            "Created Date Time",
            "Start Date Time",
            "End Date Time",
            "Local Time Zone Id",
            "Type",
            "Interval",
            "Weekdays",
            "Times",
            "Owner Id",
            "Owner Type",
        ]
    )

    client = fabric.FabricRestClient()
    response = client.get(
        f"v1/workspaces/{workspace_id}/items/{item_id}/jobs/{job_type}/schedules"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

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

    df["Enabled"] = df["Enabled"].astype(bool)
    df["Created Date Time"] = pd.to_datetime(df["Created Date Time"])
    df["Start Date Time"] = pd.to_datetime(df["Start Date Time"])

    return df


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
        The item name or ID
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

    client = fabric.FabricRestClient()
    response = client.post(
        f"v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances?jobType={job_type}"
    )

    lro(client, response, return_status_code=True)

    print(f"{icons.green_dot} The '{item_name}' {type.lower()} has been executed.")
