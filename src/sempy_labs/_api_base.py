import sempy.fabric as fabric
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    lro,
)
import sempy_labs._icons as icons

item_type_map = {
    "DataPipeline": 'data pipeline',
    "Environment": 'environment',
    "Eventhouse": 'eventhouse',
    "Eventstream": 'eventstream',
    "Lakehouse": 'lakehouse',
    "MLExperiment": 'ML experiment',
    "MLModel": 'ML model',
    "Warehouse": 'warehouse',
}


def create_item(item_name: str, type: str, description: Optional[str] = None, workspace: Optional[str] = None):

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    if type not in list(item_type_map.keys()):
        raise ValueError(f"{icons.red_dot} Invalid item type.")

    item_type_printout = item_type_map.get(type)

    rest_type = f"{type[0].lower()}{type[1:]}s"

    payload = {'displayName': item_name}

    if description is not None:
        payload['description'] = description

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/{rest_type}")

    lro(client, response, status_codes=[201, 202])

    print(f"{icons.green_dot} The '{item_name}' {item_type_printout} has been created within the '{workspace}' workspace.")
