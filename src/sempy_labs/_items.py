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
from sempy_labs._folders import list_folders


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
    path_prefix = f"{local_path}/Files/{workspace_name}/"

    items = ["Report", "SemanticModel"]

    dfI = fabric.list_items(workspace=workspace)
    dfI_filt = dfI[dfI["Type"].isin([items])]

    for _, r in dfI_filt.iterrows():
        item_name = r["Display Name"]
        item_id = r["Id"]
        description = r["Description"]
        folder_id = r["Folder Id"]
        item_type = r["Type"]
        definition = _base_api(
            request=f"/v1/workspaces/{workspace_id}/items/{item_id}/getDefinition",
            method="post",
            lro_return_json=True,
            status_codes=None,
        )

        df_filt = dfF[dfF["Folder Id"] == folder_id]
        folder_path = df_filt["Folder Path"].iloc[0]

        file_path = f"{path_prefix}/{item_type}/{item_name}.json"

        definition["description"] = description
        definition["folderPath"] = folder_path

        with open(file_path, "w") as json_file:
            json.dump(definition, json_file, indent=4)

        print(
            f"{icons.green_dot} The '{item_name}' {item_type}' definition has been backed up to the Files section of the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace_name}' workspace."
        )

    # Save folder structure
    dfF = list_folders(workspace=workspace)
    with open(f"{path_prefix}/folderStructure.json", "w") as json_file:
        json.dump(dfF.to_json(), json_file, indent=4)


def restore_item_definitions(
    source_workspace: str,
    target_workspace: Optional[str | UUID] = None,
    lakehouse: Optional[str | UUID] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
):

    (target_workspace_name, target_workspace_id) = resolve_workspace_name_and_id(
        target_workspace
    )
    (lakehouse_workspace_name, lakehouse_workspace_id) = resolve_workspace_name_and_id(
        lakehouse_workspace
    )
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=lakehouse_workspace_id
    )

    blobs = list_blobs(
        lakehouse=lakehouse_id, workspace=lakehouse_workspace_id, container="Files"
    )
    blobs_filt = blobs[
        (blobs["Blob Name"].str.startswith(f"{lakehouse_id}/Files/{source_workspace}"))
        & (blobs["Blob Name"].str.endswith(".json"))
    ]

    local_path = _mount(lakehouse=lakehouse_id, workspace=lakehouse_workspace_id)

    # Create the folder structure
    with open(
        f"{local_path}/Files/folderStructure.json", "r", encoding="utf-8"
    ) as file:
        df_folders = pd.json_normalize(json.load(file))
        for _, r in df_folders.iterrows():
            folder_name = r["Folder Name"]
            folder_path = r["Folder Path"]

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

        description = definition.get("definition")
        folder_path = definition.get("folderPath")

        definition.pop("description")
        definition.pop("folderPath")

        payload = {
            "displayName": item_name,
            "type": item_type,
            "description": description,
            "definition": json.dumps(definition, indent=2),
        }

        # Create items...
        _base_api(
            request=f"/v1/workspaces/{target_workspace_id}/items",
            method="post",
            payload=payload,
            status_codes=[201, 202],
            lro_return_status_code=True,
        )
