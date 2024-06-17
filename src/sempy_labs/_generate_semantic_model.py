import sempy
import sempy.fabric as fabric
import pandas as pd
import json, base64, time, os
from typing import List, Optional, Union
from sempy_labs._helper_functions import (
    resolve_lakehouse_name,
    resolve_workspace_name_and_id,
)
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
import sempy_labs._icons as icons


def create_blank_semantic_model(
    dataset: str,
    compatibility_level: int = 1605,
    workspace: Optional[str] = None,
):
    """
    Creates a new blank semantic model (no tables/columns etc.).

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    compatibility_level : int, default=1605
        The compatibility level of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    if compatibility_level < 1500:
        print(f"{icons.red_dot} Compatiblity level must be at least 1500.")
        return

    tmsl = f"""
  {{
    "createOrReplace": {{
      "object": {{
        "database": '{dataset}'
      }},
      "database": {{
        "name": '{dataset}',
        "compatibilityLevel": {compatibility_level},
        "model": {{
          "culture": "en-US",
          "defaultPowerBIDataSourceVersion": "powerBI_V3"
        }}
      }}
    }}
  }}
  """

    fabric.execute_tmsl(script=tmsl, workspace=workspace)

    return print(
        f"{icons.green_dot} The '{dataset}' semantic model was created within the '{workspace}' workspace."
    )


def create_semantic_model_from_bim(
    dataset: str, bim_file: str, workspace: Optional[str] = None
):
    """
    Creates a new semantic model based on a Model.bim file.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    bim_file : str
        The model.bim file.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    objectType = "SemanticModel"

    dfI = fabric.list_items(workspace=workspace, type=objectType)
    dfI_filt = dfI[(dfI["Display Name"] == dataset)]

    if len(dfI_filt) > 0:
        print(
            f"WARNING: '{dataset}' already exists as a semantic model in the '{workspace}' workspace."
        )
        return

    client = fabric.FabricRestClient()
    defPBIDataset = {"version": "1.0", "settings": {}}

    def conv_b64(file):

        loadJson = json.dumps(file)
        f = base64.b64encode(loadJson.encode("utf-8")).decode("utf-8")

        return f

    payloadPBIDefinition = conv_b64(defPBIDataset)
    payloadBim = conv_b64(bim_file)

    request_body = {
        "displayName": dataset,
        "type": objectType,
        "definition": {
            "parts": [
                {
                    "path": "model.bim",
                    "payload": payloadBim,
                    "payloadType": "InlineBase64",
                },
                {
                    "path": "definition.pbidataset",
                    "payload": payloadPBIDefinition,
                    "payloadType": "InlineBase64",
                },
            ]
        },
    }

    response = client.post(f"/v1/workspaces/{workspace_id}/items", json=request_body)

    if response.status_code == 201:
        print(
            f"The '{dataset}' semantic model has been created within the '{workspace}' workspace."
        )
        print(response.json())
    elif response.status_code == 202:
        operationId = response.headers["x-ms-operation-id"]
        response = client.get(f"/v1/operations/{operationId}")
        response_body = json.loads(response.content)
        while response_body["status"] != "Succeeded":
            time.sleep(3)
            response = client.get(f"/v1/operations/{operationId}")
            response_body = json.loads(response.content)
        response = client.get(f"/v1/operations/{operationId}/result")
        print(
            f"The '{dataset}' semantic model has been created within the '{workspace}' workspace."
        )
        print(response.json())


def deploy_semantic_model(
    dataset: str,
    new_dataset: Optional[str] = None,
    workspace: Optional[str] = None,
    new_dataset_workspace: Optional[str] = None,
):
    """
    Deploys a semantic model based on an existing semantic model.

    Parameters
    ----------
    dataset : str
        Name of the semantic model to deploy.
    new_dataset: str
        Name of the new semantic model to be created.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    new_dataset_workspace : str, default=None
        The Fabric workspace name in which the new semantic model will be deployed.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------

    """

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    if new_dataset_workspace is None:
        new_dataset_workspace = workspace

    if new_dataset is None:
        new_dataset = dataset

    if new_dataset == dataset and new_dataset_workspace == workspace:
        print(
            f"The 'dataset' and 'new_dataset' parameters have the same value. And, the 'workspace' and 'new_dataset_workspace' parameters have the same value. At least one of these must be different. Please update the parameters."
        )
        return

    bim = get_semantic_model_bim(dataset=dataset, workspace=workspace)

    create_semantic_model_from_bim(
        dataset=new_dataset, bim_file=bim, workspace=new_dataset_workspace
    )


def get_semantic_model_bim(
    dataset: str,
    workspace: Optional[str] = None,
    save_to_file_name: Optional[str] = None,
):
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
    str
        The Model.bim file for the semantic model.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    objType = "SemanticModel"
    client = fabric.FabricRestClient()
    itemList = fabric.list_items(workspace=workspace, type=objType)
    itemListFilt = itemList[(itemList["Display Name"] == dataset)]
    itemId = itemListFilt["Id"].iloc[0]
    response = client.post(
        f"/v1/workspaces/{workspace_id}/items/{itemId}/getDefinition"
    )

    if response.status_code == 200:
        res = response.json()
    elif response.status_code == 202:
        operationId = response.headers["x-ms-operation-id"]
        response = client.get(f"/v1/operations/{operationId}")
        response_body = json.loads(response.content)
        while response_body["status"] != "Succeeded":
            time.sleep(3)
            response = client.get(f"/v1/operations/{operationId}")
            response_body = json.loads(response.content)
        response = client.get(f"/v1/operations/{operationId}/result")
        res = response.json()
    df_items = pd.json_normalize(res["definition"]["parts"])
    df_items_filt = df_items[df_items["path"] == "model.bim"]
    payload = df_items_filt["payload"].iloc[0]
    bimFile = base64.b64decode(payload).decode("utf-8")
    bimJson = json.loads(bimFile)

    if save_to_file_name is not None:
        lakeAttach = lakehouse_attached()
        if lakeAttach is False:
            print(
                f"In order to save the model.bim file, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook."
            )
            return

        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, workspace)
        folderPath = "/lakehouse/default/Files"
        fileExt = ".bim"
        if not save_to_file_name.endswith(fileExt):
            save_to_file_name = save_to_file_name + fileExt
        filePath = os.path.join(folderPath, save_to_file_name)
        with open(filePath, "w") as json_file:
            json.dump(bimJson, json_file, indent=4)
        print(
            f"The .bim file for the '{dataset}' semantic model has been saved to the '{lakehouse}' in this location: '{filePath}'.\n\n"
        )

    return bimJson
