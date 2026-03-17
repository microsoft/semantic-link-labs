import sempy.fabric as fabric
import pandas as pd
import json
import os
from typing import Optional, List
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
    _conv_b64,
    _decode_b64,
    _base_api,
    _mount,
)
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
import sempy_labs._icons as icons
from sempy_labs._refresh_semantic_model import refresh_semantic_model
from uuid import UUID


@log
def create_blank_semantic_model(
    dataset: str,
    compatibility_level: int = 1702,
    workspace: Optional[str | UUID] = None,
    overwrite: bool = True,
):
    """
    Creates a new blank semantic model (no tables/columns etc.).

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    compatibility_level : int, default=1702
        The compatibility level of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    overwrite : bool, default=False
        If set to True, overwrites the existing semantic model in the workspace if it exists.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    dfD = fabric.list_datasets(workspace=workspace_id, mode="rest")
    dfD_filt = dfD[dfD["Dataset Name"] == dataset]

    if len(dfD_filt) > 0 and not overwrite:
        raise ValueError(
            f"{icons.warning} The '{dataset}' semantic model already exists within the '{workspace_name}' workspace. The 'overwrite' parameter is set to False so the blank new semantic model was not created."
        )

    min_compat = 1500
    if compatibility_level < min_compat:
        raise ValueError(
            f"{icons.red_dot} Compatiblity level must be at least {min_compat}."
        )

    # If the model does not exist
    if len(dfD_filt) == 0:
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
                    "cultures": [
                        {{
                            "name": "en-US",
                            "linguisticMetadata": {{
                                "content": {{
                                    "Version": "1.0.0",
                                    "Language": "en-US"
                                }},
                                "contentType": "json"
                            }}
                        }}
                    ],
                    "collation": "Latin1_General_100_BIN2_UTF8",
                    "dataAccessOptions": {{
                        "legacyRedirects": true,
                        "returnErrorValuesAsNull": true,
                    }},
                    "defaultPowerBIDataSourceVersion": "powerBI_V3",
                    "sourceQueryCulture": "en-US",
                    }}
                }}
            }}
        }}
        """
    else:
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

    fabric.execute_tmsl(script=tmsl, workspace=workspace_id)

    return print(
        f"{icons.green_dot} The '{dataset}' semantic model was created within the '{workspace_name}' workspace."
    )


@log
def create_semantic_model_from_bim(
    dataset: str, bim_file: dict, workspace: Optional[str | UUID] = None
):
    """
    Creates a new semantic model based on a Model.bim file.

    This is a wrapper function for the following API: `Items - Create Semantic Model <https://learn.microsoft.com/rest/api/fabric/semanticmodel/items/create-semantic-model>`_.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    bim_file : dict
        The model.bim file.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfI = fabric.list_datasets(workspace=workspace_id, mode="rest")
    dfI_filt = dfI[(dfI["Dataset Name"] == dataset)]

    if not dfI_filt.empty:
        raise ValueError(
            f"{icons.red_dot} The '{dataset}' semantic model already exists as a semantic model in the '{workspace_name}' workspace."
        )

    defPBIDataset = {"version": "1.0", "settings": {}}
    payloadPBIDefinition = _conv_b64(defPBIDataset)
    payloadBim = _conv_b64(bim_file)

    payload = {
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

    _base_api(
        request=f"v1/workspaces/{workspace_id}/semanticModels",
        payload=payload,
        method="post",
        lro_return_status_code=True,
        status_codes=[201, 202],
    )

    print(
        f"{icons.green_dot} The '{dataset}' semantic model has been created within the '{workspace_name}' workspace."
    )


@log
def update_semantic_model_from_bim(
    dataset: str | UUID, bim_file: dict, workspace: Optional[str | UUID] = None
):
    """
    Updates a semantic model definition based on a Model.bim file.

    This is a wrapper function for the following API: `Items - Update Semantic Model Definition <https://learn.microsoft.com/rest/api/fabric/semanticmodel/items/update-semantic-model-definition>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    bim_file : dict
        The model.bim file.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    defPBIDataset = {"version": "1.0", "settings": {}}
    payloadPBIDefinition = _conv_b64(defPBIDataset)
    payloadBim = _conv_b64(bim_file)

    payload = {
        "displayName": dataset_name,
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

    _base_api(
        request=f"v1/workspaces/{workspace_id}/semanticModels/{dataset_id}/updateDefinition",
        payload=payload,
        method="post",
        lro_return_status_code=True,
        status_codes=None,
    )

    print(
        f"{icons.green_dot} The '{dataset_name}' semantic model has been updated within the '{workspace_name}' workspace."
    )


@log
def deploy_semantic_model(
    source_dataset: str,
    source_workspace: Optional[str | UUID] = None,
    target_dataset: Optional[str] = None,
    target_workspace: Optional[str | UUID] = None,
    refresh_target_dataset: bool = True,
    overwrite: bool = False,
    perspective: Optional[str] = None,
):
    """
    Deploys a semantic model based on an existing semantic model.

    Parameters
    ----------
    source_dataset : str
        Name of the semantic model to deploy.
    source_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    target_dataset: str
        Name of the new semantic model to be created.
    target_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the new semantic model will be deployed.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    refresh_target_dataset : bool, default=True
        If set to True, this will initiate a full refresh of the target semantic model in the target workspace.
    overwrite : bool, default=False
        If set to True, overwrites the existing semantic model in the workspace if it exists.
    perspective : str, default=None
        Set this to the name of a perspective in the model and it will reduce the deployed model down to the tables/columns/measures/hierarchies within that perspective.
    """

    (source_workspace_name, source_workspace_id) = resolve_workspace_name_and_id(
        source_workspace
    )

    (target_workspace_name, target_workspace_id) = resolve_workspace_name_and_id(
        target_workspace
    )

    if target_dataset is None:
        target_dataset = source_dataset

    if (
        target_dataset == source_dataset
        and target_workspace_name == source_workspace_name
    ):
        raise ValueError(
            f"{icons.red_dot} The 'dataset' and 'new_dataset' parameters have the same value. And, the 'workspace' and 'new_dataset_workspace' "
            f"parameters have the same value. At least one of these must be different. Please update the parameters."
        )

    dfD = fabric.list_datasets(workspace=target_workspace_id, mode="rest")
    dfD_filt = dfD[dfD["Dataset Name"] == target_dataset]
    if not dfD_filt.empty and not overwrite:
        raise ValueError(
            f"{icons.warning} The '{target_dataset}' semantic model already exists within the '{target_workspace_name}' workspace. The 'overwrite' parameter is set to False so the source semantic model was not deployed to the target destination."
        )

    if perspective is not None:
        from sempy_labs.tom import connect_semantic_model

        with connect_semantic_model(
            dataset=source_dataset, workspace=source_workspace, readonly=True
        ) as tom:

            df_added = tom._reduce_model(perspective_name=perspective)
            bim = tom.get_bim()

    else:
        bim = get_semantic_model_bim(
            dataset=source_dataset, workspace=source_workspace_id
        )

    # Create the semantic model if the model does not exist
    if dfD_filt.empty:
        create_semantic_model_from_bim(
            dataset=target_dataset,
            bim_file=bim,
            workspace=target_workspace_id,
        )
    # Update the semantic model if the model exists
    else:
        update_semantic_model_from_bim(
            dataset=target_dataset, bim_file=bim, workspace=target_workspace_id
        )

    if refresh_target_dataset:
        refresh_semantic_model(dataset=target_dataset, workspace=target_workspace_id)

    if perspective is not None:
        return df_added


@log
def get_semantic_model_bim(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    save_to_file_name: Optional[str] = None,
) -> dict:
    """
    Extracts the Model.bim file for a given semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    save_to_file_name : str, default=None
        If specified, saves the Model.bim as a file in the lakehouse attached to the notebook.

    Returns
    -------
    dict
        The Model.bim file for the semantic model.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    bimJson = get_semantic_model_definition(
        dataset=dataset_id,
        workspace=workspace_id,
        format="TMSL",
        return_dataframe=False,
    )

    if save_to_file_name is not None:
        if not lakehouse_attached():
            raise ValueError(
                f"{icons.red_dot} In order to save the model.bim file, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook."
            )

        local_path = _mount()
        save_folder = f"{local_path}/Files"
        file_ext = ".bim"
        if not save_to_file_name.endswith(file_ext):
            save_to_file_name = f"{save_to_file_name}{file_ext}"
        file_path = os.path.join(save_folder, save_to_file_name)
        with open(file_path, "w") as json_file:
            json.dump(bimJson, json_file, indent=4)
        print(
            f"{icons.green_dot} The {file_ext} file for the '{dataset_name}' semantic model has been saved to the lakehouse attached to the notebook within: 'Files/{save_to_file_name}'.\n\n"
        )

    return bimJson


@log
def get_semantic_model_definition(
    dataset: str | UUID,
    format: str = "TMSL",
    workspace: Optional[str | UUID] = None,
    return_dataframe: bool = True,
) -> pd.DataFrame | dict | List:
    """
    Extracts the semantic model definition.

    This is a wrapper function for the following API: `Items - Get Semantic Model Definition <https://learn.microsoft.com/rest/api/fabric/semanticmodel/items/get-semantic-model-definition>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    format : str, default="TMSL"
        The output format. Valid options are "TMSL" or "TMDL". "TMSL" returns the .bim file whereas "TMDL" returns the collection of TMDL files. Can also enter 'bim' for the TMSL version.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    return_dataframe : bool, default=True
        If True, returns a dataframe.
        If False, returns the .bim file for TMSL format. Returns a list of the TMDL files (decoded) for TMDL format.

    Returns
    -------
    pandas.DataFrame | dict | List
        A pandas dataframe with the semantic model definition or the file or files comprising the semantic model definition.
    """

    valid_formats = ["TMSL", "TMDL"]

    format = format.upper()
    if format == "BIM":
        format = "TMSL"
    if format not in valid_formats:
        raise ValueError(
            f"{icons.red_dot} Invalid format. Valid options: {valid_formats}."
        )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    result = _base_api(
        request=f"v1/workspaces/{workspace_id}/semanticModels/{dataset_id}/getDefinition?format={format}",
        method="post",
        lro_return_json=True,
        status_codes=None,
    )

    files = result["definition"]["parts"]

    if return_dataframe:
        return pd.json_normalize(files)
    elif format == "TMSL":
        payload = next(
            (part["payload"] for part in files if part["path"] == "model.bim"), None
        )
        return json.loads(_decode_b64(payload))
    else:
        decoded_parts = [
            {"file_name": part["path"], "content": _decode_b64(part["payload"])}
            for part in files
        ]

        return decoded_parts


@log
def get_semantic_model_size(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Gets size of the semantic model in bytes.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    int
        The size of the semantic model in bytes
    """

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
    else:
        result = model_size

    return result
