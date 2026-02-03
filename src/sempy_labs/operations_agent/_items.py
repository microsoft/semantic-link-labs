import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    delete_item,
    resolve_workspace_id,
)
from uuid import UUID
from sempy._utils._log import log


@log
def list_operations_agents(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Returns a list of Operations Agents from the specified workspace.

    This is a wrapper function for the following API: `Items - List Operations Agents <https://learn.microsoft.com/rest/api/fabric/operationsagent/items/list-operations-agents>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the Operations Agents within a workspace.
    """
    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Operations Agent Name": "str",
        "Operations Agent Id": "str",
        "Description": "str",
        "State": "str",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"v1/workspaces/{workspace_id}/OperationsAgents",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []

    for r in responses:
        for v in r.get("value", []):
            row = {
                "Operations Agent Name": v.get("displayName"),
                "Operations Agent Id": v.get("id"),
                "Description": v.get("description"),
                "State": v.get("properties", {}).get("state"),
            }
            rows.append(row)

    return df


@log
def delete_operations_agent(
    operations_agent: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Deletes an Operations Agent from the specified workspace.

    This is a wrapper function for the following API: `Items - Delete Operations Agent <https://learn.microsoft.com/rest/api/fabric/operationsagent/items/delete-operations-agent>`_.

    Parameters
    ----------
    operations_agent : str | uuid.UUID
        The Operations Agent name or ID to delete.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=operations_agent, type="OperationsAgent", workspace=workspace)
