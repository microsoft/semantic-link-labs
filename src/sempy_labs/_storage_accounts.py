from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    _get_adls_client,
    _get_blob_client,
)
from uuid import UUID
from typing import Optional
import sempy_labs._icons as icons


def restore_lakehouse_object(
    file_path: str,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Restores a delta table in a lakehouse from a deleted state.

    Parameters
    ----------
    file_path : str
        The file path of the object to restore. For example: "Tables/my_delta_table".
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse, workspace_id
    )
    blob_path_prefix = f"{lakehouse_id}/{file_path}"

    container = file_path.split("/")[0]
    if container not in ["Tables", "Files"]:
        raise ValueError(
            f"{icons.red_dot} Invalid container '{container}' within the file_path parameter. Expected 'Tables' or 'Files'."
        )

    bsc = _get_blob_client(workspace_id=workspace_id, item_id=lakehouse_id)
    ccli = bsc.get_container_client(container=container)
    blobs = ccli.list_blobs(include=["deleted"])
    for b in blobs:
        blob_name = b.name
        if blob_name.startswith(blob_path_prefix) and b.deleted:
            print(f"{icons.in_progress} Restoring the '{blob_name}' blob...")
            blob_client = ccli.get_blob_client(blob_name)
            blob_client.undelete_blob()
            print(f"{icons.green_dot} The '{blob_name}' blob has been restored.")
