from sempy_labs._helper_functions import (
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
    delete_item,
)
from uuid import UUID
from sempy._utils._log import log
import pandas as pd
from typing import Optional


@log
def list_policy_sets(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the policy sets within a workspace.

    This is a wrapper function for the following API: `Items - List Policy Sets <https://learn.microsoft.com/rest/api/fabric/policyset/items/list-policy-sets>`_.

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
        A pandas dataframe showing the policy sets within a workspace.
    """

    columns = {
        "Policy Set Name": "string",
        "Policy Set Id": "string",
        "Description": "string",
        "Folder Id": "string",
        "Scope Type": "string",
        "Scope Id": "string",
        "Status": "string",
    }

    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/policySets",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            props = v.get("properties", {}) or {}
            scope = props.get("scope", {}) or {}
            rows.append(
                {
                    "Policy Set Name": v.get("displayName"),
                    "Policy Set Id": v.get("id"),
                    "Description": v.get("description"),
                    "Folder Id": v.get("folderId"),
                    "Scope Type": scope.get("type"),
                    "Scope Id": scope.get("id"),
                    "Status": props.get("status"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def delete_policy_set(policy_set: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Deletes a policy set within a workspace.

    This is a wrapper function for the following API: `Items - Delete Policy Set <https://learn.microsoft.com/rest/api/fabric/policyset/items/delete-policy-set>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    policy_set : str | uuid.UUID
        The ID of the policy set to delete.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """
    delete_item(item=policy_set, type="PolicySet", workspace=workspace)
