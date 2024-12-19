import sempy.fabric as fabric
import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    pagination,
)
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID


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
        The item type. If specifying the item name as the item, the item type is required.
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
            "Root Activity Id" "Start Time UTC",
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
                "Failure Reason": v.get("failureReason"),
            }
            dfs.append(pd.DataFrame(new_data, index=[0]))

    if dfs:
        df = pd.concat(dfs, ignore_index=True)

    return df
