import pandas as pd
import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
    _conv_b64,
    delete_item,
    resolve_workspace_id,
)
from uuid import UUID
from sempy._utils._log import log


@log
def list_kql_dashboards(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the KQL dashboards within a workspace.

    This is a wrapper function for the following API: `Items - List KQL Dashboards <https://learn.microsoft.com/rest/api/fabric/kqldatabase/items/list-kql-dashboards>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the KQL dashboards within a workspace.
    """

    columns = {
        "KQL Dashboard Name": "string",
        "KQL Dashboard Id": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"v1/workspaces/{workspace_id}/kqlDashboards",
        uses_pagination=True,
        lient="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "KQL Dashboard Name": v.get("displayName"),
                    "KQL Dashboard Id": v.get("id"),
                    "Description": v.get("description"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df


@log
def create_kql_dashboard(
    name: str,
    content: Optional[str] = None,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a KQL dashboard.

    This is a wrapper function for the following API: `Items - Create KQL Dashboard <https://learn.microsoft.com/rest/api/fabric/kqldatabase/items/create-kql-dashboard>`_.

    Parameters
    ----------
    name: str
        Name of the KQL dashboard.
    content: Optional[dict], default=None
        The RealTimeDashboard.json content.
    description : str, default=None
        A description of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {"displayName": name}

    if description:
        payload["description"] = description

    if content:
        payload["definition"] = {
            "format": None,
            "parts": [
                {
                    "path": "RealTimeDashboard.json",
                    "payload": _conv_b64(content),
                    "payloadType": "InlineBase64",
                },
            ],
        }

    _base_api(
        request=f"v1/workspaces/{workspace_id}/kqlDashboards",
        method="post",
        payload=payload,
        status_codes=201,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{name}' KQL dashboard has been created within the '{workspace_name}' workspace."
    )


@log
def delete_kql_dashboard(
    kql_dashboard: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Deletes a KQL dashboard.

    This is a wrapper function for the following API: `Items - Delete KQL Dashboard <https://learn.microsoft.com/rest/api/fabric/kqldatabase/items/delete-kql-dashboard>`_.

    Parameters
    ----------
    kql_dashboard: str | uuid.UUID
        Name or ID of the KQL dashboard.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=kql_dashboard, item_type="KQLDashboard", workspace=workspace)
