import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    resolve_item_id,
    _create_dataframe,
    _conv_b64,
    _decode_b64,
    delete_item,
    create_item,
    get_item_definition,
)
from uuid import UUID
import sempy_labs._icons as icons


def create_eventhouse(
    name: str,
    definition: Optional[dict],
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a Fabric eventhouse.

    This is a wrapper function for the following API: `Items - Create Eventhouse <https://learn.microsoft.com/rest/api/fabric/environment/items/create-eventhouse>`_.

    Parameters
    ----------
    name: str
        Name of the eventhouse.
    definition : dict
        The definition (EventhouseProperties.json) of the eventhouse.
    description : str, default=None
        A description of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if definition is not None and not isinstance(definition, dict):
        raise ValueError(f"{icons.red_dot} The definition must be a dictionary.")

    definition_payload = (
        {
            "parts": [
                {
                    "path": "EventhouseProperties.json",
                    "payload": _conv_b64(definition),
                    "payloadType": "InlineBase64",
                }
            ]
        }
        if definition is not None
        else None
    )

    create_item(
        name=name,
        type="Eventhouse",
        workspace=workspace,
        description=description,
        definition=definition_payload,
    )


def list_eventhouses(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the eventhouses within a workspace.

    This is a wrapper function for the following API: `Items - List Eventhouses <https://learn.microsoft.com/rest/api/fabric/environment/items/list-eventhouses>`_.

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
        A pandas dataframe showing the eventhouses within a workspace.
    """

    columns = {
        "Eventhouse Name": "string",
        "Eventhouse Id": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventhouses",
        uses_pagination=True,
        client="fabric_sp",
    )

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Eventhouse Name": v.get("displayName"),
                "Eventhouse Id": v.get("id"),
                "Description": v.get("description"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def delete_eventhouse(name: str, workspace: Optional[str | UUID] = None):
    """
    Deletes a Fabric eventhouse.

    This is a wrapper function for the following API: `Items - Delete Eventhouse <https://learn.microsoft.com/rest/api/fabric/environment/items/delete-eventhouse>`_.

    Parameters
    ----------
    name: str
        Name of the eventhouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=name, type="Eventhouse", workspace=workspace)


def get_eventhouse_definition(
    eventhouse: str | UUID,
    workspace: Optional[str | UUID] = None,
    return_dataframe: bool = False,
) -> dict | pd.DataFrame:
    """
    Gets the eventhouse definition.

    This is a wrapper function for the following API: `Items - Get Eventhouse Definition <https://learn.microsoft.com/rest/api/fabric/eventhouse/items/get-eventhouse-definition>`_.

    Parameters
    ----------
    eventhouse : str
        Name of the eventhouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the eventhouse resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    return_dataframe : bool, default=False
        If True, returns a dataframe. If False, returns a json dictionary.

    Returns
    -------
    dict | pandas.DataFrame
        The eventhouse definition in .json format or as a pandas dataframe.
    """

    return get_item_definition(
        item=eventhouse,
        type="Eventhouse",
        workspace=workspace,
        return_dataframe=return_dataframe,
    )
