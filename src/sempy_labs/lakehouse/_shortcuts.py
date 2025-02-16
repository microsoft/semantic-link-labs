import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_lakehouse_name,
    resolve_lakehouse_id,
    resolve_workspace_name_and_id,
    _base_api,
)
from typing import Optional
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID


def create_shortcut_onelake(
    table_name: str,
    source_lakehouse: str,
    source_workspace: str | UUID,
    destination_lakehouse: str,
    destination_workspace: Optional[str | UUID] = None,
    shortcut_name: Optional[str] = None,
    source_folder: str = "Tables",
    destination_folder: str = "Tables",
):
    """
    Creates a `shortcut <https://learn.microsoft.com/fabric/onelake/onelake-shortcuts>`_ to a delta table in OneLake.

    This is a wrapper function for the following API: `OneLake Shortcuts - Create Shortcut <https://learn.microsoft.com/rest/api/fabric/core/onelake-shortcuts/create-shortcut>`_.

    Parameters
    ----------
    table_name : str
        The table name for which a shortcut will be created.
    source_lakehouse : str
        The Fabric lakehouse in which the table resides.
    source_workspace : str | uuid.UUID
        The name or ID of the Fabric workspace in which the source lakehouse exists.
    destination_lakehouse : str
        The Fabric lakehouse in which the shortcut will be created.
    destination_workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace in which the shortcut will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    shortcut_name : str, default=None
        The name of the shortcut 'table' to be created. This defaults to the 'table_name' parameter value.
    source_folder : str, default="Tables"
        A string representing the full path to the table/file in the source lakehouse.
    destination_folder: str, default="Tables"
        A string representing the full path where the shortcut is created, including either "Files" or "Tables".
    """

    if not (source_folder.startswith("Files") or source_folder.startswith("Tables")):
        raise ValueError(
            f"{icons.red_dot} The 'source_folder' parameter must be either 'Files' or 'Tables'."
        )
    if not (
        destination_folder.startswith("Files")
        or destination_folder.startswith("Tables")
    ):
        raise ValueError(
            f"{icons.red_dot} The 'destination_folder' parameter must be either 'Files' or 'Tables'."
        )

    (source_workspace_name, source_workspace_id) = resolve_workspace_name_and_id(
        source_workspace
    )
    source_lakehouse_id = resolve_lakehouse_id(source_lakehouse, source_workspace_id)
    source_lakehouse_name = fabric.resolve_item_name(
        item_id=source_lakehouse_id, type="Lakehouse", workspace=source_workspace_id
    )

    if destination_workspace is None:
        destination_workspace_name = source_workspace_name
        destination_workspace_id = source_workspace_id
    else:
        destination_workspace_name = destination_workspace
        destination_workspace_id = fabric.resolve_workspace_id(
            destination_workspace_name
        )

    destination_workspace_id = fabric.resolve_workspace_id(destination_workspace)
    destination_lakehouse_id = resolve_lakehouse_id(
        destination_lakehouse, destination_workspace
    )
    destination_lakehouse_name = fabric.resolve_item_name(
        item_id=destination_lakehouse_id,
        type="Lakehouse",
        workspace=destination_workspace_id,
    )

    if shortcut_name is None:
        shortcut_name = table_name

    source_path = f"{source_folder}/{table_name}"

    payload = {
        "path": destination_folder,
        "name": shortcut_name.replace(" ", ""),
        "target": {
            "oneLake": {
                "workspaceId": source_workspace_id,
                "itemId": source_lakehouse_id,
                "path": source_path,
            }
        },
    }

    _base_api(
        request=f"/v1/workspaces/{destination_workspace_id}/items/{destination_lakehouse_id}/shortcuts",
        payload=payload,
        status_codes=201,
        method="post",
    )

    print(
        f"{icons.green_dot} The shortcut '{shortcut_name}' was created in the '{destination_lakehouse_name}' lakehouse within the '{destination_workspace_name} workspace. It is based on the '{table_name}' table in the '{source_lakehouse_name}' lakehouse within the '{source_workspace_name}' workspace."
    )


def create_shortcut(
    shortcut_name: str,
    location: str,
    subpath: str,
    source: str,
    connection_id: str,
    lakehouse: Optional[str] = None,
    workspace: Optional[str] = None,
):
    """
    Creates a `shortcut <https://learn.microsoft.com/fabric/onelake/onelake-shortcuts>`_ to an ADLS Gen2 or Amazon S3 source.

    Parameters
    ----------
    shortcut_name : str
    location : str
    subpath : str
    source : str
    connection_id: str
    lakehouse : str
        The Fabric lakehouse in which the shortcut will be created.
    workspace : str, default=None
        The name of the Fabric workspace in which the shortcut will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    source_titles = {"adlsGen2": "ADLS Gen2", "amazonS3": "Amazon S3"}

    sourceValues = list(source_titles.keys())

    if source not in sourceValues:
        raise ValueError(
            f"{icons.red_dot} The 'source' parameter must be one of these values: {sourceValues}."
        )

    sourceTitle = source_titles[source]

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
    else:
        lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)

    client = fabric.FabricRestClient()
    shortcutActualName = shortcut_name.replace(" ", "")

    request_body = {
        "path": "Tables",
        "name": shortcutActualName,
        "target": {
            source: {
                "location": location,
                "subpath": subpath,
                "connectionId": connection_id,
            }
        },
    }

    try:
        response = client.post(
            f"/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts",
            json=request_body,
        )
        if response.status_code == 201:
            print(
                f"{icons.green_dot} The shortcut '{shortcutActualName}' was created in the '{lakehouse}' lakehouse within"
                f" the '{workspace} workspace. It is based on the '{subpath}' table in '{sourceTitle}'."
            )
        else:
            print(response.status_code)
    except Exception as e:
        raise ValueError(
            f"{icons.red_dot} Failed to create a shortcut for the '{shortcut_name}' table."
        ) from e


def delete_shortcut(
    shortcut_name: str,
    lakehouse: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Deletes a shortcut.

    This is a wrapper function for the following API: `OneLake Shortcuts - Delete Shortcut <https://learn.microsoft.com/rest/api/fabric/core/onelake-shortcuts/delete-shortcut>`_.

    Parameters
    ----------
    shortcut_name : str
        The name of the shortcut.
    lakehouse : str, default=None
        The Fabric lakehouse name in which the shortcut resides.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | UUID, default=None
        The name or ID of the Fabric workspace in which lakehouse resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, workspace_id)
    else:
        lakehouse_id = resolve_lakehouse_id(lakehouse, workspace_id)

    client = fabric.FabricRestClient()
    response = client.delete(
        f"/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts/Tables/{shortcut_name}"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{shortcut_name}' shortcut in the '{lakehouse}' within the '{workspace_name}' workspace has been deleted."
    )


def reset_shortcut_cache(workspace: Optional[str | UUID] = None):
    """
    Deletes any cached files that were stored while reading from shortcuts.

    This is a wrapper function for the following API: `OneLake Shortcuts - Reset Shortcut Cache <https://learn.microsoft.com/rest/api/fabric/core/onelake-shortcuts/reset-shortcut-cache>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/onelake/resetShortcutCache",
        method="post",
        lro_return_status_code=True,
        status_codes=None,
    )

    print(
        f"{icons.green_dot} The shortcut cache has been reset for the '{workspace_name}' workspace."
    )
