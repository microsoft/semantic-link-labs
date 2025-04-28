import sempy.fabric as fabric
import os
import pandas as pd
from sempy_labs._helper_functions import (
    _base_api,
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    _mount,
)
from uuid import UUID
from typing import Optional
import json
import sempy_labs._icons as icons
from sempy_labs.lakehouse._blobs import list_blobs
from sempy_labs._folders import (
    list_folders,
    create_folder,
)
import re


# Item types which have definitions
item_list = [
    "CopyJob",
    "Eventhouse",
    "DataPipeline",
    "KQLDatabase",
    "KQLDashboard",
    "KQLQueryset",
    "MirroredDatabase",
    "MountedDataFactory",
    "Environment",
    "Notebook",
    "Report",
    "SemanticModel",
    "Eventstream",
    # "Reflex", # This API is not working
    "SparkJobDefinition",
    "VariableLibrary",
    # Dataflow,
    # GraphQLApi,
]


def backup_item_definitions(
    workspace: Optional[str | UUID] = None,
    lakehouse: Optional[str | UUID] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
):
    """
    Backups the item definitions of a workspace to a lakehouse.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_workspace_name, lakehouse_workspace_id) = resolve_workspace_name_and_id(
        lakehouse_workspace
    )
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=lakehouse_workspace_id
    )
    local_path = _mount(lakehouse=lakehouse_id, workspace=lakehouse_workspace_id)
    path_prefix = f"{local_path}/Files/SLL_backup_item_definitions/{workspace_name}"

    # dfI = fabric.list_items(workspace=workspace)
    response = _base_api(request=f"/v1/workspaces/{workspace_id}/items?recursive=True")
    df = pd.json_normalize(response.json()["value"])
    dfI_filt = df[df["type"].isin(item_list)]
    # dfI_filt = dfI[dfI["Type"].isin(items)]

    dfF = list_folders(workspace=workspace)

    for _, r in dfI_filt.iterrows():
        item_name = r["displayName"]
        item_id = r["id"]
        description = r["description"]
        folder_id = r.get("folderId")
        item_type = r["type"]
        print(f"{item_name} : {item_type}")
        definition = _base_api(
            request=f"/v1/workspaces/{workspace_id}/items/{item_id}/getDefinition",
            method="post",
            lro_return_json=True,
            status_codes=None,
        )

        # Obtain the folder path
        folder_path = ""
        if folder_id:
            df_filt = dfF[dfF["Folder Id"] == folder_id]
            if not df_filt.empty:
                folder_path = df_filt["Folder Path"].iloc[0]

        definition["description"] = description
        definition["folderPath"] = folder_path

        file_path = f"{path_prefix}/{item_type}/{item_name}.json"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as json_file:
            json.dump(definition, json_file, indent=4)

        print(
            f"{icons.green_dot} The '{item_name}' {item_type}' definition has been backed up to the Files section of the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace_name}' workspace."
        )


def restore_item_definitions(
    backup_file_path: str,
    target_workspace: Optional[str | UUID] = None,
):
    """
    Creates items based on an item definition backup file path.

    Parameters
    ----------
    backup_file_path : str
        The path to the backup file. For example: "abfss://{lakehouse_id}@onelake.dfs.fabric.microsoft.com/{workspace_id}/Files/SLL_backup_item_definitions/My Workspace Name"
    target_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (target_workspace_name, target_workspace_id) = resolve_workspace_name_and_id(
        target_workspace
    )

    lakehouse_workspace_id = backup_file_path.split("abfss://")[1].split("@")[0]
    lakehouse_id = backup_file_path.split("microsoft.com/")[1].split("/")[0]
    folder_path = backup_file_path.split(f"microsoft.com/{lakehouse_id}/")[1]

    blobs = list_blobs(
        lakehouse=lakehouse_id, workspace=lakehouse_workspace_id, container="Files"
    )
    blobs_filt = blobs[
        (blobs["Blob Name"].str.startswith(f"{lakehouse_id}/{folder_path}"))
        & (blobs["Blob Name"].str.endswith(".json"))
    ]

    local_path = _mount(lakehouse=lakehouse_id, workspace=lakehouse_workspace_id)

    # Creating the folder structure
    def ensure_folder_path_exists(folder_path):
        # Normalize the paths if necessary
        existing_paths = set(
            dfF["Folder Path"].str.strip("/")
        )  # remove leading/trailing slashes for easier comparison

        parts = folder_path.strip("/").split("/")
        current_path = ""

        for part in parts:
            if current_path:
                current_path += "/" + part
            else:
                current_path = part

            if current_path not in existing_paths:
                # Create the folder since it does not exist
                parent_folder = (
                    "/" + "/".join(current_path.split("/")[:-1])
                    if "/" in current_path
                    else "/"
                )
                # creation_folder = '/' + current_path
                create_folder(
                    name=part,
                    workspace=target_workspace_id,
                    parent_folder=parent_folder,
                )
                existing_paths.add(current_path)

    for _, r in blobs_filt.iterrows():
        blob_name = r["Blob Name"]
        blob_file = blob_name.split(f"{lakehouse_id}")[1][1:]
        file_name = os.path.basename(blob_file)
        # directory = os.path.dirname(blob_file)
        # folder_structure = os.path.dirname(directory)
        item_type = os.path.basename(os.path.dirname(blob_file))
        item_name = os.path.splitext(file_name)[0]
        definition_file_path = f"{local_path}/{blob_file}"
        with open(definition_file_path, "r", encoding="utf-8") as file:
            definition = json.load(file)

        description = definition.get("description")
        folder_path = definition.get("folderPath")
        raw_definition = definition.get('definition')

        payload = {
            "displayName": item_name,
            "type": item_type,
            "definition": raw_definition,
        }

        if description:
            payload["description"] = description
        if folder_path:
            dfF = list_folders(workspace=target_workspace_id)
            dfF_filt = dfF[dfF["Folder Path"] == folder_path]
            if not dfF_filt.empty:
                folder_id = dfF_filt["Folder Id"].iloc[0]
            else:
                folder_id = None
                # Create the folder if it does not exist
                ensure_folder_path_exists(folder_path)
                # Get the folder ID again after creating it
                dfF = list_folders(workspace=target_workspace_id)
                dfF_filt = dfF[dfF["Folder Path"] == folder_path]
                if not dfF_filt.empty:
                    folder_id = dfF_filt["Folder Id"].iloc[0]

            payload["folderId"] = folder_id

        # Create items...
        _base_api(
            request=f"/v1/workspaces/{target_workspace_id}/items",
            method="post",
            payload=payload,
            status_codes=[201, 202],
            lro_return_status_code=True,
        )

        print(f"{icons.green_dot} Created the '{item_name}' {_split_camel_case(item_type)} within the '{target_workspace_name}' workspace")


def _split_camel_case(text):
    # Find acronym groups or normal words
    matches = re.finditer(r'([A-Z]+(?=[A-Z][a-z])|[A-Z][a-z]*)', text)
    words = [m.group(0) for m in matches]

    # Lowercase normal words, keep acronyms as-is
    words = [w if w.isupper() else w.lower() for w in words]

    return ' '.join(words)
