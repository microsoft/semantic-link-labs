import sempy.fabric as fabric
import pandas as pd
import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    lro,
    _conv_b64,
)
from sempy.fabric.exceptions import FabricHTTPException


def list_activators(workspace: Optional[str] = None) -> pd.DataFrame:

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/reflexes")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(columns=['Activator Name', 'Activator Id', 'Description'])

    for v in response.json().get('value'):
        new_data = {
            "Activator Name": v.get('displayName'),
            "Activator Id": v.get('id'),
            "Description": v.get('description'),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def delete_activator(activator: str, workspace: Optional[str] = None):

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(item_name=activator, type='Reflex', workspace=workspace_id)
    fabric.delete_item(item_id=item_id, workspace=workspace_id)

    print(f"{icons.green_dot} The '{activator}' activator within the '{workspace}' workspace has been deleted.")


def create_activator(name: str, definition: Optional[str] = None, description: Optional[str] = None, workspace: Optional[str] = None):

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {
        "displayName": name
    }

    if description is not None:
        payload['description'] = description

    if definition is not None:
        reflex_payload = _conv_b64(definition)
        # platform_payload = ''
        payload['definition'] = {
            "format": "json",
            "parts": [
                {
                    "path": "ReflexEntities.json",
                    "payload": reflex_payload,
                    "payloadType": "InlineBase64"
                },
                # {
                #    "path": ".platform",
                #    "payload": platform_payload,
                #    "payloadType": "InlineBase64"
                # }
            ]
        }

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/reflexes", json=payload)

    lro(client, response, status_codes=[201, 202])

    print(
        f"{icons.green_dot} The '{name}' activator has been created within the '{workspace}' workspace."
    )


def update_activator_definition(activator: str, definition: str, workspace: Optional[str] = None):

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = fabric.resolve_item_id(item_name=activator, type='Reflex', workspace=workspace_id)
    reflex_payload = _conv_b64(definition)

    client = fabric.FabricRestClient()
    payload = {
        "definition": {
            "parts": [
                {
                    "path": "ReflexEntities.json",
                    "payload": reflex_payload,
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }
    response = client.post(f"/v1/workspaces/{workspace_id}/reflexes/{item_id}/updateDefinition", json=payload)

    lro(client, response, status_codes=[200, 202], return_status_code=True)

    print(
        f"{icons.green_dot} The '{activator}' activator has been updated within the '{workspace}' workspace."
    )
