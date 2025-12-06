from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_lakehouse_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
)
import pandas as pd
from typing import Optional
from uuid import UUID
from sempy._utils._log import log


@log
def list_livy_sessions(
    lakehouse: Optional[str | UUID] = None, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a list of livy sessions from the specified item identifier.

    This is a wrapper function for the following API: `Livy Sessions - List Livy Sessions <https://learn.microsoft.com/rest/api/fabric/lakehouse/livy-sessions/list-livy-sessions>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of livy sessions from the specified item identifier.
    """

    columns = {
        "Spark Application Id": "string",
        "State:": "string",
        "Livy Id": "string",
        "Origin": "string",
        "Attempt Number": "int",
        "Max Number Of Attempts": "int",
        "Livy Name": "string",
        "Submitter Id": "string",
        "Submitter Type": "string",
        "Item Workspace Id": "string",
        "Item Id": "string",
        "Item Reference Type": "string",
        "Item Name": "string",
        "Item Type": "string",
        "Job Type": "string",
        "Submitted Date Time": "str",
        "Start Date Time": "str",
        "End Date Time": "string",
        "Queued Duration Value": "int",
        "Queued Duration Time Unit": "string",
        "Running Duration Value": "int",
        "Running Duration Time Unit": "string",
        "Total Duration Value": "int",
        "Total Duration Time Unit": "string",
        "Job Instance Id": "string",
        "Creator Item Workspace Id": "string",
        "Creator Item Id": "string",
        "Creator Item Reference Type": "string",
        "Creator Item Name": "string",
        "Creator Item Type": "string",
        "Cancellation Reason": "string",
        "Capacity Id": "string",
        "Operation Name": "string",
        "Runtime Version": "string",
        "Livy Session Item Resource Uri": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)
    lakehouse_id = resolve_lakehouse_id(lakehouse, workspace_id)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/livySessions",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            queued_duration = v.get("queuedDuration", {})
            running_duration = v.get("runningDuration", {})
            total_duration = v.get("totalDuration", {})
            rows.append(
                {
                    "Spark Application Id": v.get("sparkApplicationId"),
                    "State:": v.get("state"),
                    "Livy Id": v.get("livyId"),
                    "Origin": v.get("origin"),
                    "Attempt Number": v.get("attemptNumber"),
                    "Max Number Of Attempts": v.get("maxNumberOfAttempts"),
                    "Livy Name": v.get("livyName"),
                    "Submitter Id": v["submitter"].get("id"),
                    "Submitter Type": v["submitter"].get("type"),
                    "Item Workspace Id": v["item"].get("workspaceId"),
                    "Item Id": v["item"].get("itemId"),
                    "Item Reference Type": v["item"].get("referenceType"),
                    "Item Name": v.get("itemName"),
                    "Item Type": v.get("itemType"),
                    "Job Type": v.get("jobType"),
                    "Submitted Date Time": v.get("submittedDateTime"),
                    "Start Date Time": v.get("startDateTime"),
                    "End Date Time": v.get("endDateTime"),
                    "Queued Duration Value": queued_duration.get("value"),
                    "Queued Duration Time Unit": queued_duration.get("timeUnit"),
                    "Running Duration Value": running_duration.get("value"),
                    "Running Duration Time Unit": running_duration.get("timeUnit"),
                    "Total Duration Value": total_duration.get("value"),
                    "Total Duration Time Unit": total_duration.get("timeUnit"),
                    "Job Instance Id": v.get("jobInstanceId"),
                    "Creator Item Workspace Id": v["creatorItem"].get("workspaceId"),
                    "Creator Item Id": v["creatorItem"].get("itemId"),
                    "Creator Item Reference Type": v["creatorItem"].get(
                        "referenceType"
                    ),
                    "Creator Item Name": v.get("creatorItemName"),
                    "Creator Item Type": v.get("creatorItemType"),
                    "Cancellation Reason": v.get("cancellationReason"),
                    "Capacity Id": v.get("capacityId"),
                    "Operation Name": v.get("operationName"),
                    "Runtime Version": v.get("runtimeVersion"),
                    "Livy Session Item Resource Uri": v.get(
                        "livySessionItemResourceUri"
                    ),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df
