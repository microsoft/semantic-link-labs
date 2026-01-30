from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
)
from typing import Optional
import sempy_labs._icons as icons
from uuid import UUID
from sempy._utils._log import log


@log
def provision_workspace_identity(workspace: Optional[str | UUID] = None):
    """
    Provisions a workspace identity for a workspace.

    This is a wrapper function for the following API: `Workspaces - Provision Identity <https://learn.microsoft.com/rest/api/fabric/core/workspaces/provision-identity>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/provisionIdentity",
        method="post",
        lro_return_status_code=True,
        status_codes=None,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} A workspace identity has been provisioned for the '{workspace_name}' workspace."
    )


@log
def deprovision_workspace_identity(workspace: Optional[str | UUID] = None):
    """
    Deprovisions a workspace identity for a workspace.

    This is a wrapper function for the following API: `Workspaces - Derovision Identity <https://learn.microsoft.com/rest/api/fabric/core/workspaces/deprovision-identity>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/deprovisionIdentity",
        method="post",
        lro_return_status_code=True,
        status_codes=None,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The workspace identity has been deprovisioned from the '{workspace_name}' workspace."
    )
