import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    generate_guid,
)
from uuid import UUID
from sempy._utils._log import log
import requests
import json
from sempy_labs.kql_dashboard._items import create_kql_dashboard
from sempy_labs.kql_database._items import list_kql_databases


@log
def create_workspace_monitoring_dashboard(
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
            "id": generate_guid(),
            "workspace": workspace_id,
        }
    ]

    create_kql_dashboard(name=name, content=content, workspace=workspace)
