import sempy.fabric as fabric
import pandas as pd
import json
import os
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_lakehouse_name,
    resolve_workspace_name_and_id,
    resolve_dataset_id,
    _conv_b64,
    _decode_b64,
    lro,
)
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
import sempy_labs._icons as icons
from sempy_labs._refresh_semantic_model import refresh_semantic_model


def create_blank_semantic_model(
    dataset: str,
    compatibility_level: int = 1605,
    workspace: Optional[str] = None,
    overwrite: bool = True,
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
    overwrite : bool, default=False
        If set to True, overwrites the existing semantic model in the workspace if it exists.
    """

    workspace = fabric.resolve_workspace_name(workspace)
    dfD = fabric.list_datasets(workspace=workspace, mode="rest")
    dfD_filt = dfD[dfD["Dataset Name"] == dataset]

    if len(dfD_filt) > 0 and not overwrite:
        raise ValueError(
            f"{icons.warning} The '{dataset}' semantic model already exists within the '{workspace}' workspace. The 'overwrite' parameter is set to False so the blank new semantic model was not created."
        )

    min_compat = 1500
    if compatibility_level < min_compat:
        raise ValueError(
            f"{icons.red_dot} Compatiblity level must be at least {min_compat}."
        )

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
    dataset: str, bim_file: dict, workspace: Optional[str] = None
):
    """
    Creates a new semantic model based on a Model.bim file.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    bim_file : dict
        The model.bim file.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfI = fabric.list_datasets(workspace=workspace, mode="rest")
    dfI_filt = dfI[(dfI["Dataset Name"] == dataset)]

    if len(dfI_filt) > 0:
        raise ValueError(
            f"{icons.red_dot} The '{dataset}' semantic model already exists as a semantic model in the '{workspace}' workspace."
        )

    client = fabric.FabricRestClient()
    defPBIDataset = {"version": "1.0", "settings": {}}

    payloadPBIDefinition = _conv_b64(defPBIDataset)
    payloadBim = _conv_b64(bim_file)

    request_body = {
        "displayName": dataset,
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

    response = client.post(
        f"/v1/workspaces/{workspace_id}/semanticModels",
        json=request_body,
    )

    lro(client, response, status_codes=[201, 202])

    print(
        f"{icons.green_dot} The '{dataset}' semantic model has been created within the '{workspace}' workspace."
    )


def update_semantic_model_from_bim(
    dataset: str, bim_file: dict, workspace: Optional[str] = None
):
    """
    Updates a semantic model definition based on a Model.bim file.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    bim_file : dict
        The model.bim file.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfD = fabric.list_datasets(workspace=workspace, mode="rest")
    dfD_filt = dfD[dfD["Dataset Name"] == dataset]
    if len(dfD_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{dataset}' semantic model within the '{workspace}' workspace does not exist."
        )
    dataset_id = dfD_filt["Dataset Id"].iloc[0]

    client = fabric.FabricRestClient()
    defPBIDataset = {"version": "1.0", "settings": {}}

    payloadPBIDefinition = _conv_b64(defPBIDataset)
    payloadBim = _conv_b64(bim_file)

    request_body = {
        "displayName": dataset,
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

    response = client.post(
        f"/v1/workspaces/{workspace_id}/semanticModels/{dataset_id}/updateDefinition",
        json=request_body,
    )

    lro(client, response, status_codes=[200, 202])

    print(
        f"{icons.green_dot} The '{dataset}' semantic model has been updated within the '{workspace}' workspace."
    )


def deploy_semantic_model(
    source_dataset: str,
    source_workspace: Optional[str] = None,
    target_dataset: Optional[str] = None,
    target_workspace: Optional[str] = None,
    refresh_target_dataset: bool = True,
    overwrite: bool = False,
):
    """
    Deploys a semantic model based on an existing semantic model.

    Parameters
    ----------
    source_dataset : str
        Name of the semantic model to deploy.
    source_workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    target_dataset: str
        Name of the new semantic model to be created.
    target_workspace : str, default=None
        The Fabric workspace name in which the new semantic model will be deployed.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    refresh_target_dataset : bool, default=True
        If set to True, this will initiate a full refresh of the target semantic model in the target workspace.
    overwrite : bool, default=False
        If set to True, overwrites the existing semantic model in the workspace if it exists.
    """

    source_workspace = fabric.resolve_workspace_name(source_workspace)

    if target_workspace is None:
        target_workspace = source_workspace

    if target_dataset is None:
        target_dataset = source_dataset

    if target_dataset == source_dataset and target_workspace == source_workspace:
        raise ValueError(
            f"{icons.red_dot} The 'dataset' and 'new_dataset' parameters have the same value. And, the 'workspace' and 'new_dataset_workspace' "
            f"parameters have the same value. At least one of these must be different. Please update the parameters."
        )

    dfD = fabric.list_datasets(workspace=target_workspace, mode="rest")
    dfD_filt = dfD[dfD["Dataset Name"] == target_dataset]
    if len(dfD_filt) > 0 and not overwrite:
        raise ValueError(
            f"{icons.warning} The '{target_dataset}' semantic model already exists within the '{target_workspace}' workspace. The 'overwrite' parameter is set to False so the source semantic model was not deployed to the target destination."
        )

    bim = get_semantic_model_bim(dataset=source_dataset, workspace=source_workspace)

    # Create the semantic model if the model does not exist
    if len(dfD_filt) == 0:
        create_semantic_model_from_bim(
            dataset=target_dataset,
            bim_file=bim,
            workspace=target_workspace,
        )
    # Update the semantic model if the model exists
    else:
        update_semantic_model_from_bim(
            dataset=target_dataset, bim_file=bim, workspace=target_workspace
        )

    if refresh_target_dataset:
        refresh_semantic_model(dataset=target_dataset, workspace=target_workspace)


def get_semantic_model_bim(
    dataset: str,
    workspace: Optional[str] = None,
    save_to_file_name: Optional[str] = None,
    lakehouse_workspace: Optional[str] = None,
) -> dict:
    """
    Extracts the Model.bim file for a given semantic model.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    save_to_file_name : str, default=None
        If specified, saves the Model.bim as a file in the lakehouse attached to the notebook.
    lakehouse_workspace : str, default=None
        The Fabric workspace name in which the lakehouse attached to the workspace resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    dict
        The Model.bim file for the semantic model.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    fmt = "TMSL"
    client = fabric.FabricRestClient()
    dataset_id = resolve_dataset_id(dataset=dataset, workspace=workspace)
    response = client.post(
        f"/v1/workspaces/{workspace_id}/semanticModels/{dataset_id}/getDefinition?format={fmt}",
    )
    result = lro(client, response).json()
    df_items = pd.json_normalize(result["definition"]["parts"])
    df_items_filt = df_items[df_items["path"] == "model.bim"]
    payload = df_items_filt["payload"].iloc[0]
    bimFile = _decode_b64(payload)
    bimJson = json.loads(bimFile)

    if save_to_file_name is not None:
        if not lakehouse_attached():
            raise ValueError(
                f"{icons.red_dot} In order to save the model.bim file, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook."
            )

        lakehouse_id = fabric.get_lakehouse_id()
        lake_workspace = fabric.resolve_workspace_name()
        lakehouse = resolve_lakehouse_name(lakehouse_id, lake_workspace)
        folderPath = "/lakehouse/default/Files"
        fileExt = ".bim"
        if not save_to_file_name.endswith(fileExt):
            save_to_file_name = f"{save_to_file_name}{fileExt}"
        filePath = os.path.join(folderPath, save_to_file_name)
        with open(filePath, "w") as json_file:
            json.dump(bimJson, json_file, indent=4)
        print(
            f"{icons.green_dot} The .bim file for the '{dataset}' semantic model has been saved to the '{lakehouse}' in this location: '{filePath}'.\n\n"
        )

    return bimJson


def get_semantic_model_size(dataset: str, workspace: Optional[str] = None):

    workspace = fabric.resolve_workspace_name(workspace)

    dict = fabric.evaluate_dax(
        dataset=dataset,
        workspace=workspace,
        dax_string="""
        EVALUATE SELECTCOLUMNS(FILTER(INFO.STORAGETABLECOLUMNS(), [COLUMN_TYPE] = "BASIC_DATA"),[DICTIONARY_SIZE])
        """,
    )

    used_size = fabric.evaluate_dax(
        dataset=dataset,
        workspace=workspace,
        dax_string="""
        EVALUATE SELECTCOLUMNS(INFO.STORAGETABLECOLUMNSEGMENTS(),[USED_SIZE])
        """,
    )
    dict_size = dict["[DICTIONARY_SIZE]"].sum()
    used_size = used_size["[USED_SIZE]"].sum()
    model_size = dict_size + used_size
    # Calculate proper bytes size by dividing by 1024 and multiplying by 1000 - per 1000
    if model_size >= 10**9:
        result = model_size / (1024**3) * 10**9
    elif model_size >= 10**6:
        result = model_size / (1024**2) * 10**6
    elif model_size >= 10**3:
        result = model_size / (1024) * 10**3

    return result
