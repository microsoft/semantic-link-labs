import sempy.fabric as fabric
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


def backup_item_definitions(
    workspace: Optional[str | UUID] = None,
    lakehouse: Optional[str | UUID] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
):

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_workspace_name, lakehouse_workspace_id) = resolve_workspace_name_and_id(
        lakehouse_workspace
    )
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=lakehouse_workspace_id
    )
    local_path = _mount(lakehouse=lakehouse_id, workspace=lakehouse_workspace_id)

    items = ["Report", "SemanticModel"]

    dfI = fabric.list_items(workspace=workspace)
    dfI_filt = dfI[dfI["Type"].isin([items])]

    for _, r in dfI_filt.iterrows():
        item_name = r["Display Name"]
        item_id = r["Id"]
        item_type = r["Type"]
        result = _base_api(
            request=f"/v1/workspaces/{workspace_id}/items/{item_id}/getDefinition",
            method="post",
            lro_return_json=True,
            status_codes=None,
        )

        file_path = f"{local_path}/Files/{workspace_name}/{item_type}/{item_name}.json"

        with open(file_path, "w") as json_file:
            json.dump(result, json_file, indent=4)
        print(
            f"{icons.green_dot} The '{item_name}' {item_type}' definition has been backed up to the Files section of the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace_name}' workspace."
        )


def restore_item_definitions(
    source_workspace: str,
    target_workspace: Optional[str | UUID] = None,
    lakehouse: Optional[str | UUID] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
):

    from sempy_labs.lakehouse._blobs import list_blobs

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

    for _, r in blobs_filt.iterrows():
        blob_name = r["Blob Name"]
        blob_file = blob_name.split(f"{lakehouse_id}")[1][1:]
        split = blob_name.split("/")
        item_type = split[-2]
        item_name = split[-1].replace(".json", "")
        file_path = f"{local_path}/{blob_file}"
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)

        payload = {
            "displayName": item_name,
            "type": item_type,
            "description": "",
            "definition": json.dumps(data, indent=2),
        }

        _base_api(
            request=f"/v1/workspaces/{target_workspace_id}/items",
            method="post",
            payload=payload,
            status_codes=[201, 202],
            lro_return_status_code=True,
        )
    # Create items...
