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
def list_event_schema_sets(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Returns a list of Event Schema Sets from the specified workspace.

    This is a wrapper function for the following API: `Items - List Event Schema Sets <https://learn.microsoft.com/rest/api/fabric/eventschemaset/items/list-event-schema-sets>`_.
    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the Event Schema Sets within a workspace.
    """
    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Event Schema Set Name": "str",
        "Event Schema Set Id": "str",
        "Description": "str",
        "OneLake Root Path": "str",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"v1/workspaces/{workspace_id}/eventSchemaSets",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []

    for r in responses:
        for v in r.get("value", []):
            row = {
                "Event Schema Set Name": v.get("displayName"),
                "Event Schema Set Id": v.get("id"),
                "Description": v.get("description"),
                "OneLake Root Path": v.get("properties", {}).get("oneLakeRootPath"),
            }
            rows.append(row)

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def delete_event_schema_set(
    event_schema_set: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Deletes an Event Schema Set from the specified workspace.

    This is a wrapper function for the following API: `Items - Delete Event Schema Set <https://learn.microsoft.com/rest/api/fabric/eventschemaset/items/delete-event-schema-set>`_.

    Parameters
    ----------
    event_schema_set : str | uuid.UUID
        The Event Schema Set name or ID to delete.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=event_schema_set, type="EventSchemaSet", workspace=workspace)
