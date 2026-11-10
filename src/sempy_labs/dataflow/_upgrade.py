import pandas as pd
from typing import List, Optional
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
)
from sempy_labs.dataflow._items import list_dataflows


@log
def list_dataflow_upgrade_readiness(
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the upgrade readiness results for all Gen1 dataflows in the specified workspace. Gen2 dataflows are excluded.

    This is a wrapper function for the following API: `Upgrade - List Readiness <https://learn.microsoft.com/rest/api/fabric/dataflow/upgrade/list-readiness>`_.

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
        A pandas dataframe showing the upgrade readiness of each Gen1 dataflow in the workspace.
        The 'Reasons' column contains a list of dictionaries (with 'status', 'code' and 'message' keys)
        explaining the readiness verdict.
    """

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Dataflow Id": "string",
        "Dataflow Name": "string",
        "Status": "string",
        "Reason Codes": "list",
        "Reasons": "list",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/dataflows/gen1UpgradeReadinessResults",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            reasons = v.get("reasons", [])
            rows.append(
                {
                    "Dataflow Id": v.get("id"),
                    "Dataflow Name": v.get("displayName"),
                    "Status": v.get("status"),
                    "Reason Codes": [reason.get("code") for reason in reasons],
                    "Reasons": reasons,
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def upgrade_dataflow(
    dataflow: str | UUID | List[str | UUID],
    workspace: Optional[str | UUID] = None,
    new_dataflow_name: Optional[str] = None,
    new_dataflow_workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Upgrades one or more Gen1 dataflows to Dataflow Gen2 in place. Each successful upgrade preserves the dataflow ID and workspace.

    Use :func:`sempy_labs.dataflow.list_dataflow_upgrade_readiness` to check whether the dataflows are ready to be upgraded.

    This is a wrapper function for the following API: `Upgrade - Upgrade Dataflows <https://learn.microsoft.com/rest/api/fabric/dataflow/upgrade/upgrade-dataflows>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataflow : str | uuid.UUID | List[str | uuid.UUID]
        The name or ID of the Gen1 dataflow, or a list of Gen1 dataflow names or IDs (up to 50).
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    new_dataflow_name : str, default=None
        Deprecated. The upgrade runs in place so this parameter is ignored.
    new_dataflow_workspace : str | uuid.UUID, default=None
        Deprecated. The upgrade runs in place and does not support cross-workspace targets so this parameter is ignored.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the upgrade result for each dataflow.
        The 'Required Actions' column contains a list of dictionaries (with 'status', 'code' and 'message' keys)
        describing the changes required before the dataflow can be upgraded.
    """

    if new_dataflow_name or new_dataflow_workspace:
        print(
            f"{icons.warning} The 'new_dataflow_name' and 'new_dataflow_workspace' parameters are deprecated and ignored. Dataflows are now upgraded in place."
        )

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)

    columns = {
        "Dataflow Id": "string",
        "Dataflow Name": "string",
        "Target Dataflow Name": "string",
        "Workspace Id": "string",
        "Workspace Name": "string",
        "Assessment Status": "string",
        "Migration Status": "string",
        "Migration End Date Time": "datetime",
        "Required Actions": "list",
        "Error Code": "string",
        "Error Message": "string",
        "Request Id": "string",
        "Is Retriable": "bool",
    }
    df = _create_dataframe(columns=columns)

    if isinstance(dataflow, (str, UUID)):
        dataflow = [dataflow]

    # Resolve names/IDs using a single listing call rather than one call per dataflow
    dfD = list_dataflows(workspace=workspace_id)
    dataflow_ids = []
    for d in dataflow:
        d_str = str(d)
        dfD_filt = dfD[dfD["Dataflow Id"] == d_str]
        if dfD_filt.empty:
            dfD_filt = dfD[dfD["Dataflow Name"] == d_str]
        if dfD_filt.empty:
            raise ValueError(
                f"{icons.red_dot} The '{d_str}' dataflow does not exist within the '{workspace_name}' workspace."
            )

        dataflow_name = dfD_filt["Dataflow Name"].iloc[0]
        generation = dfD_filt["Generation"].iloc[0]
        if generation != "Gen1":
            print(
                f"{icons.info} The '{dataflow_name}' dataflow is a {generation} dataflow. Only Gen1 dataflows can be upgraded so it will be skipped."
            )
            continue

        dataflow_id = dfD_filt["Dataflow Id"].iloc[0]
        if dataflow_id not in dataflow_ids:
            dataflow_ids.append(dataflow_id)

    if not dataflow_ids:
        print(f"{icons.info} No Gen1 dataflows to upgrade. No changes made.")
        return df

    if len(dataflow_ids) > 50:
        raise ValueError(
            f"{icons.red_dot} A maximum of 50 dataflows can be upgraded per request. {len(dataflow_ids)} Gen1 dataflows were provided."
        )

    payload = {"dataflows": [{"id": i} for i in dataflow_ids]}

    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/dataflows/gen1Upgrade",
        method="post",
        payload=payload,
        status_codes=[200, 202],
        lro_return_json=True,
        client="fabric_sp",
    )

    rows = []
    for m in result.get("migrations", []):
        error = m.get("migrationError", {})
        rows.append(
            {
                "Dataflow Id": m.get("dataflowId"),
                "Dataflow Name": m.get("dataflowName"),
                "Target Dataflow Name": m.get("targetDataflowName"),
                "Workspace Id": m.get("workspaceId"),
                "Workspace Name": m.get("workspaceName"),
                "Assessment Status": m.get("assessmentStatus"),
                "Migration Status": m.get("migrationStatus"),
                "Migration End Date Time": m.get("migrationEndDateTime"),
                "Required Actions": m.get("requiredActions", []),
                "Error Code": error.get("errorCode"),
                "Error Message": error.get("message"),
                "Request Id": error.get("requestId"),
                "Is Retriable": error.get("isRetriable", False),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    summary = result.get("summary", {})
    succeeded = summary.get("succeededCount", 0)
    failed = summary.get("failedCount", 0)
    total = summary.get("totalCount", len(dataflow_ids))
    icon = icons.green_dot if failed == 0 else icons.yellow_dot
    print(
        f"{icon} {succeeded} of {total} Gen1 dataflows in the '{workspace_name}' workspace were upgraded successfully. {failed} failed."
    )

    return df
