import sempy
import sempy.fabric as fabric
import pandas as pd
import json, os, time, base64
from .HelperFunctions import resolve_lakehouse_name
from .Lakehouse import lakehouse_attached
from typing import List, Optional, Union

def get_semantic_model_bim(dataset: str, workspace: Optional[str] = None, save_to_file_name: Optional[str] = None):

    """
    Extracts the Model.bim file for a given semantic model.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    save_to_file_name : str, default=None
        If specified, saves the Model.bim as a file in the lakehouse attached to the notebook.

    Returns
    -------
    Model.bim
        The Model.bim file for the semantic model.
    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)
    
    objType = 'SemanticModel'
    client = fabric.FabricRestClient()
    itemList = fabric.list_items(workspace = workspace, type = objType)
    itemListFilt = itemList[(itemList['Display Name'] == dataset)]
    itemId = itemListFilt['Id'].iloc[0]
    response = client.post(f"/v1/workspaces/{workspace_id}/items/{itemId}/getDefinition")
        
    if response.status_code == 200:
        res = response.json()
    elif response.status_code == 202:
        operationId = response.headers['x-ms-operation-id']
        response = client.get(f"/v1/operations/{operationId}")
        response_body = json.loads(response.content) 
        while response_body['status'] != 'Succeeded':
            time.sleep(3)
            response = client.get(f"/v1/operations/{operationId}")
            response_body = json.loads(response.content)
        response = client.get(f"/v1/operations/{operationId}/result")
        res = response.json()
    df_items = pd.json_normalize(res['definition']['parts'])
    df_items_filt = df_items[df_items['path'] == 'model.bim']
    payload = df_items_filt['payload'].iloc[0]
    bimFile = base64.b64decode(payload).decode('utf-8')
    bimJson = json.loads(bimFile)

    if save_to_file_name is not None:        
        lakeAttach = lakehouse_attached()
        if lakeAttach == False:
            print(f"In order to save the model.bim file, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook.")
            return
        
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, workspace)
        folderPath = '/lakehouse/default/Files'
        fileExt = '.bim'
        if not save_to_file_name.endswith(fileExt):
            save_to_file_name = save_to_file_name + fileExt
        filePath = os.path.join(folderPath, save_to_file_name)
        with open(filePath, "w") as json_file:
            json.dump(bimJson, json_file, indent=4)
        print(f"The .bim file for the '{dataset}' semantic model has been saved to the '{lakehouse}' in this location: '{filePath}'.\n\n")

    return bimJson