import sempy
import sempy.fabric as fabric
import json, base64, time
from .GetSemanticModelBim import get_semantic_model_bim
from sempy._utils._log import log

def create_semantic_model_from_bim(dataset: str, bim_file, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#create_semantic_model_from_bim

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    objectType = 'SemanticModel'

    dfI = fabric.list_items(workspace = workspace, type = objectType)
    dfI_filt = dfI[(dfI['Display Name'] == dataset)]

    if len(dfI_filt) > 0:
        print(f"WARNING: '{dataset}' already exists as a semantic model in the '{workspace}' workspace.")
        return

    client = fabric.FabricRestClient()
    defPBIDataset = {
    "version": "1.0",
    "settings": {}
    }

    def conv_b64(file):
        
        loadJson = json.dumps(file)
        f = base64.b64encode(loadJson.encode('utf-8')).decode('utf-8')
        
        return f

    payloadPBIDefinition = conv_b64(defPBIDataset)
    payloadBim = conv_b64(bim_file)

    request_body = {
            'displayName': dataset,
            'type': objectType,
            'definition': {
        "parts": [
            {
                "path": "model.bim",
                "payload": payloadBim,
                "payloadType": "InlineBase64"
            },
            {
                "path": "definition.pbidataset",
                "payload": payloadPBIDefinition,
                "payloadType": "InlineBase64"
            }
        ]

            }
        }

    response = client.post(f"/v1/workspaces/{workspace_id}/items",json=request_body)

    if response.status_code == 201:
        print(f"The '{dataset}' semantic model has been created within the '{workspace}' workspace.")
        print(response.json())
    elif response.status_code == 202:
        operationId = response.headers['x-ms-operation-id']
        response = client.get(f"/v1/operations/{operationId}")
        response_body = json.loads(response.content) 
        while response_body['status'] != 'Succeeded':
            time.sleep(3)
            response = client.get(f"/v1/operations/{operationId}")
            response_body = json.loads(response.content)
        response = client.get(f"/v1/operations/{operationId}/result")
        print(f"The '{dataset}' semantic model has been created within the '{workspace}' workspace.")
        print(response.json())

def deploy_semantic_model(dataset: str, new_dataset: str | None = None, workspace: str | None = None, new_dataset_workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#deploy_semantic_model

    """    

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    if new_dataset_workspace == None:
        new_dataset_workspace = workspace

    if new_dataset is None:
        new_dataset = dataset

    if new_dataset == dataset and new_dataset_workspace == workspace:
        print(f"The 'dataset' and 'new_dataset' parameters have the same value. And, the 'workspace' and 'new_dataset_workspace' parameters have the same value. At least one of these must be different. Please update the parameters.")
        return

    bim = get_semantic_model_bim(dataset = dataset, workspace = workspace)

    create_semantic_model_from_bim(dataset = new_dataset, bim_file = bim, workspace = new_dataset_workspace)