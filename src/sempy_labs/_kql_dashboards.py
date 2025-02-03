import sempy.fabric as fabric
import pandas as pd
import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
)
from uuid import UUID
import requests
import json


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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    responses = _base_api(
        request=f"v1/workspaces/{workspace_id}/kqlDashboards", uses_pagination=True
    )

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "KQL Dashboard Name": v.get("displayName"),
                "KQL Dashboard Id": v.get("id"),
                "Description": v.get("description"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


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
        # platform_payload = ''
        payload["definition"] = {
            "format": None,
            "parts": [
                {
                    "path": "RealTimeDashboard.json",
                    "payload": content,
                    "payloadType": "InlineBase64",
                },
                # {
                #    "path": ".platform",
                #    "payload": platform_payload,
                #    "payloadType": "InlineBase64"
                # }
            ],
        }

    _base_api(
        request=f"v1/workspaces/{workspace_id}/kqlDashboards",
        method="post",
        payload=payload,
        status_codes=201,
    )

    print(
        f"{icons.green_dot} The '{name}' KQL dashboard has been created within the '{workspace_name}' workspace."
    )


def create_workspace_montiring_dashboard(
    name: str = "Fabric Workspace Monitoring Dashboard",
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a workspace monitoring dashboard based on `this <https://github.com/microsoft/fabric-toolbox/tree/main/monitoring/workspace-monitoring-dashboards>`_ template.

    This function requires the workspace to have workspace monitoring enabled.

    Parameters
    ----------
    name : str, default="Fabric Workspace Monitoring Dashboard"
        The name of the workspace monitoring dashboard.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """
    from sempy_labs._kql_databases import list_kql_databases

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    db_name = "Monitoring KQL database"

    url = "https://raw.githubusercontent.com/microsoft/fabric-toolbox/refs/heads/main/monitoring/workspace-monitoring-dashboards/Fabric%20Workspace%20Monitoring%20Dashboard.json"
    response = requests.get(url)
    content = json.loads(response.content)

    # Resolve the cluster URI and database ID
    dfK = list_kql_databases(workspace=workspace)
    dfK_filt = dfK[dfK["KQL Database Name"] == db_name]
    if dfK_filt.empty:
        raise ValueError(
            f"{icons.red_dot} Workspace monitoring is not set up for the '{workspace_name}' workspace."
        )
    cluster_uri = dfK_filt["Query Service URI"].iloc[0]
    database_id = dfK_filt["KQL Database Id"].iloc[0]

    content["dataSources"] = [
        {
            "kind": "kusto-trident",
            "scopeId": "kusto-trident",
            "clusterUri": cluster_uri,
            "database": database_id,
            "name": db_name,
            "id": "b7d7ce56-c612-4d4f-ab1a-1a6f6212efd0",
            "workspace": workspace_id,
        }
    ]

    create_kql_dashboard(name=name, content=content, workspace=workspace)


def delete_kql_dashboard(
    kql_dashboard: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Deletes a KQL database.

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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = fabric.resolve_item_id(
        item_name=kql_dashboard, type="KQLDashboard", workspace=workspace_id
    )

    fabric.delete_item(item_id=item_id, workspace=workspace)
    print(
        f"{icons.green_dot} The '{kql_dashboard}' KQL database within the '{workspace_name}' workspace has been deleted."
    )
