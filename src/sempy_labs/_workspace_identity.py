import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    lro,
)
from typing import Optional
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID


def provision_workspace_identity(workspace: Optional[str | UUID] = None):
    """
    Provisions a workspace identity for a workspace.

    This is a wrapper function for the following API: `Workspaces - Provision Identity <https://learn.microsoft.com/rest/api/fabric/core/workspaces/provision-identity>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/provisionIdentity")

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)

    lro(client, response)

    print(
        f"{icons.green_dot} A workspace identity has been provisioned for the '{workspace_name}' workspace."
    )


def deprovision_workspace_identity(workspace: Optional[str | UUID] = None):
    """
    Deprovisions a workspace identity for a workspace.

    This is a wrapper function for the following API: `Workspaces - Derovision Identity <https://learn.microsoft.com/rest/api/fabric/core/workspaces/deprovision-identity>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/deprovisionIdentity")

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)

    lro(client, response)

    print(
        f"{icons.green_dot} The workspace identity has been deprovisioned from the '{workspace_name}' workspace."
    )
