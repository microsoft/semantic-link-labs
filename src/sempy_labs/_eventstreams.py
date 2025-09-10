import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    _base_api,
    delete_item,
    _create_dataframe,
    create_item,
    resolve_workspace_id,
)
from uuid import UUID
import sempy_labs._icons as icons
from sempy._utils._log import log


@log
def list_eventstreams(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the eventstreams within a workspace.

    This is a wrapper function for the following API: `Items - List Eventstreams <https://learn.microsoft.com/rest/api/fabric/environment/items/list-eventstreams>`_.

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
        A pandas dataframe showing the eventstreams within a workspace.
    """

    columns = {
        "Eventstream Name": "string",
        "Eventstream Id": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)
    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Eventstream Name": v.get("displayName"),
                    "Eventstream Id": v.get("id"),
                    "Description": v.get("description"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def create_eventstream(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a Fabric eventstream.

    This is a wrapper function for the following API: `Items - Create Eventstream <https://learn.microsoft.com/rest/api/fabric/environment/items/create-eventstream>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name: str
        Name of the eventstream.
    description : str, default=None
        A description of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    create_item(
        name=name, description=description, type="Eventstream", workspace=workspace
    )


@log
def delete_eventstream(
    eventstream: str | UUID, workspace: Optional[str | UUID] = None, **kwargs
):
    """
    Deletes a Fabric eventstream.

    This is a wrapper function for the following API: `Items - Delete Eventstream <https://learn.microsoft.com/rest/api/fabric/environment/items/delete-eventstream>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    eventstream: str | uuid.UUID
        Name or ID of the eventstream.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if "name" in kwargs:
        eventstream = kwargs["name"]
        print(
            f"{icons.warning} The 'name' parameter is deprecated. Please use 'eventstream' instead."
        )

    delete_item(item=eventstream, type="Eventstream", workspace=workspace)
