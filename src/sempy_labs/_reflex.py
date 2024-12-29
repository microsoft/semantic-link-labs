import sempy.fabric as fabric
import pandas as pd
import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    lro,
    _conv_b64,
    _decode_b64,
)
from sempy.fabric.exceptions import FabricHTTPException
import json
from uuid import UUID


def list_activators(workspace: Optional[str] = None) -> pd.DataFrame:
    """
    Shows the activators (reflexes) within a workspace.

    This is a wrapper function for the following API: `Items - List Reflexes <https://learn.microsoft.com/rest/api/fabric/reflex/items/list-reflexes>`_.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the activators (reflexes) within a workspace.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/reflexes")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(columns=["Activator Name", "Activator Id", "Description"])

    for v in response.json().get("value"):
        new_data = {
            "Activator Name": v.get("displayName"),
            "Activator Id": v.get("id"),
            "Description": v.get("description"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def delete_activator(activator: str | UUID, workspace: Optional[str] = None):
    """
    Deletes an activator (reflex).

    This is a wrapper function for the following API: `Items - Delete Reflex <https://learn.microsoft.com/rest/api/fabric/reflex/items/delete-reflex>`_.

    Parameters
    ----------
    activator : str | uuid.UUID
        The name or ID of the activator/reflex.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(item=activator, type='Reflex', workspace=workspace_id)
    fabric.delete_item(item_id=item_id, workspace=workspace_id)

    print(
        f"{icons.green_dot} The '{item_name}' activator within the '{workspace_name}' workspace has been deleted."
    )


def _create_activator(
    name: str,
    definition: Optional[dict] = None,
    description: Optional[str] = None,
    workspace: Optional[str] = None,
):
    """
    Creates an activator (reflex).

    This is a wrapper function for the following API: `Items - Create Reflex <https://learn.microsoft.com/rest/api/fabric/reflex/items/create-reflex>`_.

    Parameters
    ----------
    name : str
        The name of the activator/reflex.
    definition : dict, default=None
        The .json definition of an activator/reflex.
    description : str, default=None
        The description of the activator/reflex.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {"displayName": name}

    if description is not None:
        payload["description"] = description

    if definition is not None:
        reflex_payload = _conv_b64(definition)
        # platform_payload = ''
        payload["definition"] = {
            "format": "json",
            "parts": [
                {
                    "path": "ReflexEntities.json",
                    "payload": reflex_payload,
                    "payloadType": "InlineBase64",
                },
                # {
                #    "path": ".platform",
                #    "payload": platform_payload,
                #    "payloadType": "InlineBase64"
                # }
            ],
        }

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/reflexes", json=payload)

    lro(client, response, status_codes=[201, 202], return_status_code=True)

    print(
        f"{icons.green_dot} The '{name}' activator has been created within the '{workspace_name}' workspace."
    )


def _update_activator_definition(
    activator: str, definition: dict, workspace: Optional[str] = None
):
    """
    Updates the definition of an activator (reflex).

    This is a wrapper function for the following API: `Items - Update Reflex Definition <https://learn.microsoft.com/rest/api/fabric/reflex/items/update-reflex-definition>`_.

    Parameters
    ----------
    activator : str
        The name of the activator/reflex.
    definition : dict, default=None
        The .json definition of an activator/reflex.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(item=activator, type='Reflex', workspace=workspace_id)
    reflex_payload = _conv_b64(definition)

    client = fabric.FabricRestClient()
    payload = {
        "definition": {
            "parts": [
                {
                    "path": "ReflexEntities.json",
                    "payload": reflex_payload,
                    "payloadType": "InlineBase64",
                }
            ]
        }
    }
    response = client.post(
        f"/v1/workspaces/{workspace_id}/reflexes/{item_id}/updateDefinition",
        json=payload,
    )

    lro(client, response, status_codes=[200, 202], return_status_code=True)

    print(
        f"{icons.green_dot} The '{item_name}' activator has been updated within the '{workspace_name}' workspace."
    )


def _get_activator_definition(
    activator: str, workspace: Optional[str] = None, decode: bool = True,
) -> dict:
    """
    Gets the definition of an activator (reflex).

    This is a wrapper function for the following API: `Items - Update Reflex Definition <https://learn.microsoft.com/rest/api/fabric/reflex/items/update-reflex-definition>`_.

    Parameters
    ----------
    activator : str
        The name of the activator/reflex.
    definition : dict, default=None
        The .json definition of an activator/reflex.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    decode : bool, default=True
        If True, decodes the activator definition file into .json format.
        If False, obtains the activator definition file in base64 format.

    Returns
    -------
    dict
        The activator definition.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(item=activator, type='Reflex', workspace=workspace_id)
    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/reflexes/{item_id}/getDefinition",
    )

    result = lro(client, response).json()
    df_items = pd.json_normalize(result["definition"]["parts"])
    df_items_filt = df_items[df_items["path"] == "ReflexEntities.json"]
    payload = df_items_filt["payload"].iloc[0]

    if decode:
        result = json.loads(_decode_b64(payload))
    else:
        result = payload

    return result
