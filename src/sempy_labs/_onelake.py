from sempy_labs._helper_functions import (
    _base_api,
    resolve_workspace_id,
    resolve_lakehouse_name_and_id,
    resolve_workspace_name_and_id,
)
from sempy._utils._log import log
from uuid import UUID
from typing import Optional
import pandas as pd
import sempy_labs._icons as icons


@log
def get_onelake_settings(workspace: Optional[str | UUID] = None):
    """
    Obtains the workspace OneLake settings.

    This is a wrapper function for the following API: `OneLake Settings - Get Settings <https://learn.microsoft.com/rest/api/fabric/core/onelake-settings/get-settings>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        The workspace OneLake settings.
    """

    workspace_id = resolve_workspace_id(workspace)
    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/onelake/settings", client="fabric_sp"
    ).json()

    d = result.get("diagnostics", {})
    enabled = True if d.get("status", {}) == "Enabled" else False
    rows = []
    rows.append(
        {
            "Enabled": enabled,
            "Destination Type": (
                d.get("destination", {}).get("type", {}) if enabled else None
            ),
            "Destination Id": (
                d.get("destination", {}).get("lakehouse", {}).get("itemId", {})
                if enabled
                else None
            ),
            "Destination Workspace Id": (
                d.get("destination", {}).get("lakehouse", {}).get("workspaceId", {})
                if enabled
                else None
            ),
        }
    )

    return pd.DataFrame(rows)


def modify_onelake_diagnostics(
    workspace: Optional[str | UUID] = None,
    enabled: bool = True,
    destination_lakehouse: Optional[str | UUID] = None,
    destination_workspace: Optional[str | UUID] = None,
):
    """
    Obtains the workspace OneLake settings.

    This is a wrapper function for the following API: `OneLake Settings - Modify Diagnostics <https://learn.microsoft.com/rest/api/fabric/core/onelake-settings/modify-diagnostics>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    enabled : bool, default=True
        Whether to enable or disable OneLake diagnostics.
    destination_lakehouse : str | uuid.UUID, default=None
        The name or ID of the destination lakehouse.
        Defaults to None which resolves to the lakehouse of the attached lakehouse
        or if no lakehouse attached, resolves to the lakehouse of the notebook.
    destination_workspace : str | uuid.UUID, default=None
        The name or ID of the destination workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_id = resolve_workspace_id(workspace)
    (destination_workspace_name, destination_workspace_id) = (
        resolve_workspace_name_and_id(destination_workspace)
    )
    (destination_lakehouse_name, destination_lakehouse_id) = (
        resolve_lakehouse_name_and_id(destination_lakehouse, destination_workspace_id)
    )

    if enabled:
        payload = {
            "status": "Enabled",
            "destination": {
                "type": "Lakehouse",
                "lakehouse": {
                    "referenceType": "ById",
                    "itemId": destination_lakehouse_id,
                    "workspaceId": destination_workspace_id,
                },
            },
        }
    else:
        payload = {"status": "Disabled"}
    _base_api(
        request=f"/v1/workspaces/{workspace_id}/onelake/settings/modifyDiagnostics",
        client="fabric_sp",
        method="post",
        payload=payload,
    )

    if enabled:
        print(
            f"{icons.green_dot} OneLake diagnostics have been enabled and updated to use the '{destination_lakehouse_name}' lakehouse in the '{destination_workspace_name}' workspace as the destination."
        )
    else:
        print(f"{icons.green_dot} OneLake diagnostics have been disabled.")
