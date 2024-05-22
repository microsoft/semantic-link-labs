import sempy
import sempy.fabric as fabric
import pandas as pd
import json, os, time, base64
from .HelperFunctions import resolve_lakehouse_name
from .Lakehouse import lakehouse_attached

def get_semantic_model_bim(dataset: str, workspace: str | None = None, save_to_file_name: str | None = None):

    """
    
    Documentation is available here: https://github.com/m-kovalsky/fabric_cat_tools?tab=readme-ov-file#get_semantic_model_bim

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)
    
    objType = 'SemanticModel'
    client = fabric.FabricRestClient()
    itemList = fabric.list_items(workspace = workspace)
    itemListFilt = itemList[(itemList['Display Name'] == dataset) & (itemList['Type'] == objType)]
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