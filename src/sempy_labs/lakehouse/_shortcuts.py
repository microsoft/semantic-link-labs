import sempy.fabric as fabric
import pandas as pd
from sempy_labs._helper_functions import (
    resolve_lakehouse_name_and_id,
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
    resolve_workspace_name,
)
from sempy._utils._log import log
from typing import Optional
import sempy_labs._icons as icons
from uuid import UUID
from sempy.fabric.exceptions import FabricHTTPException


@log
def create_shortcut_onelake(
    table_name: str,
    source_lakehouse: str | UUID,
    source_workspace: str | UUID,
    destination_lakehouse: Optional[str | UUID] = None,
    destination_workspace: Optional[str | UUID] = None,
    shortcut_name: Optional[str] = None,
    source_path: str = "Tables",
    destination_path: str = "Tables",
    shortcut_conflict_policy: Optional[str] = None,
):
    """
    Creates a `shortcut <https://learn.microsoft.com/fabric/onelake/onelake-shortcuts>`_ to a delta table in OneLake.

    This is a wrapper function for the following API: `OneLake Shortcuts - Create Shortcut <https://learn.microsoft.com/rest/api/fabric/core/onelake-shortcuts/create-shortcut>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    table_name : str
        The table name for which a shortcut will be created.
    source_lakehouse : str | uuid.UUID
        The Fabric lakehouse in which the table resides.
    source_workspace : str | uuid.UUID
        The name or ID of the Fabric workspace in which the source lakehouse exists.
    destination_lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse in which the shortcut will be created.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    destination_workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace in which the shortcut will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    shortcut_name : str, default=None
        The name of the shortcut 'table' to be created. This defaults to the 'table_name' parameter value.
    source_path : str, default="Tables"
        A string representing the full path to the table/file in the source lakehouse, including either "Files" or "Tables". Examples: Tables/FolderName/SubFolderName; Files/FolderName/SubFolderName.
    destination_path: str, default="Tables"
        A string representing the full path where the shortcut is created, including either "Files" or "Tables". Examples: Tables/FolderName/SubFolderName; Files/FolderName/SubFolderName.
    shortcut_conflict_policy : str, default=None
        When provided, it defines the action to take when a shortcut with the same name and path already exists. The default action is 'Abort'. Additional ShortcutConflictPolicy types may be added over time.
    """

    if not (source_path.startswith("Files") or source_path.startswith("Tables")):
        raise ValueError(
            f"{icons.red_dot} The 'source_path' parameter must be either 'Files' or 'Tables'."
        )
    if not (
        destination_path.startswith("Files") or destination_path.startswith("Tables")
    ):
        raise ValueError(
            f"{icons.red_dot} The 'destination_path' parameter must be either 'Files' or 'Tables'."
        )

    (source_workspace_name, source_workspace_id) = resolve_workspace_name_and_id(
        source_workspace
    )

    (source_lakehouse_name, source_lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=source_lakehouse, workspace=source_workspace_id
    )

    (destination_workspace_name, destination_workspace_id) = (
        resolve_workspace_name_and_id(destination_workspace)
    )
    (destination_lakehouse_name, destination_lakehouse_id) = (
        resolve_lakehouse_name_and_id(
            lakehouse=destination_lakehouse, workspace=destination_workspace_id
        )
    )

    if shortcut_name is None:
        shortcut_name = table_name

    source_full_path = f"{source_path}/{table_name}"

    actual_shortcut_name = shortcut_name.replace(" ", "")

    payload = {
        "path": destination_path,
        "name": actual_shortcut_name,
        "target": {
            "oneLake": {
                "itemId": source_lakehouse_id,
                "path": source_full_path,
                "workspaceId": source_workspace_id,
            }
        },
    }

    # Check if the shortcut already exists
    try:
        response = _base_api(
            request=f"/v1/workspaces/{destination_workspace_id}/items/{destination_lakehouse_id}/shortcuts/{destination_path}/{actual_shortcut_name}",
            client="fabric_sp",
        )
        response_json = response.json()
        del response_json["target"]["type"]
        if response_json.get("target") == payload.get("target"):
            print(
                f"{icons.info} The '{actual_shortcut_name}' shortcut already exists in the '{destination_lakehouse_name}' lakehouse within the '{destination_workspace_name}' workspace."
            )
            return
        else:
            raise ValueError(
                f"{icons.red_dot} The '{actual_shortcut_name}' shortcut already exists in the '{destination_lakehouse_name} lakehouse within the '{destination_workspace_name}' workspace but has a different source."
            )
    except FabricHTTPException:
        pass

    url = f"/v1/workspaces/{destination_workspace_id}/items/{destination_lakehouse_id}/shortcuts"

    if shortcut_conflict_policy:
        if shortcut_conflict_policy not in ["Abort", "GenerateUniqueName"]:
            raise ValueError(
                f"{icons.red_dot} The 'shortcut_conflict_policy' parameter must be either 'Abort' or 'GenerateUniqueName'."
            )
        url += f"?shortcutConflictPolicy={shortcut_conflict_policy}"

    _base_api(
        request=url,
        payload=payload,
        status_codes=201,
        method="post",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The shortcut '{shortcut_name}' was created in the '{destination_lakehouse_name}' lakehouse within the '{destination_workspace_name}' workspace. It is based on the '{table_name}' table in the '{source_lakehouse_name}' lakehouse within the '{source_workspace_name}' workspace."
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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    shortcutActualName = shortcut_name.replace(" ", "")

    payload = {
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

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts",
        method="post",
        payload=payload,
        status_codes=201,
        client="fabric_sp",
    )
    print(
        f"{icons.green_dot} The shortcut '{shortcutActualName}' was created in the '{lakehouse_name}' lakehouse within"
        f" the '{workspace_name}' workspace. It is based on the '{subpath}' table in '{sourceTitle}'."
    )


@log
def delete_shortcut(
    shortcut_name: str,
    shortcut_path: str = "Tables",
    lakehouse: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Deletes a shortcut.

    This is a wrapper function for the following API: `OneLake Shortcuts - Delete Shortcut <https://learn.microsoft.com/rest/api/fabric/core/onelake-shortcuts/delete-shortcut>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    shortcut_name : str
        The name of the shortcut.
    shortcut_path : str = "Tables"
        The path of the shortcut to be deleted. Must start with either "Files" or "Tables". Examples: Tables/FolderName/SubFolderName; Files/FolderName/SubFolderName.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name in which the shortcut resides.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | UUID, default=None
        The name or ID of the Fabric workspace in which lakehouse resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts/{shortcut_path}/{shortcut_name}",
        method="delete",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{shortcut_name}' shortcut in the '{lakehouse}' within the '{workspace_name}' workspace has been deleted."
    )


@log
def reset_shortcut_cache(workspace: Optional[str | UUID] = None):
    """
    Deletes any cached files that were stored while reading from shortcuts.

    This is a wrapper function for the following API: `OneLake Shortcuts - Reset Shortcut Cache <https://learn.microsoft.com/rest/api/fabric/core/onelake-shortcuts/reset-shortcut-cache>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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
        client="fabric_sp",
        lro_return_status_code=True,
        status_codes=None,
    )

    print(
        f"{icons.green_dot} The shortcut cache has been reset for the '{workspace_name}' workspace."
    )


@log
def list_shortcuts(
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    path: Optional[str] = None,
) -> pd.DataFrame:
    """
    Shows all shortcuts which exist in a Fabric lakehouse and their properties.

    Parameters
    ----------
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace in which lakehouse resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    path: str, default=None
        The path within lakehouse where to look for shortcuts. If provided, must start with either "Files" or "Tables". Examples: Tables/FolderName/SubFolderName; Files/FolderName/SubFolderName.
        Defaults to None which will retun all shortcuts on the given lakehouse

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing all the shortcuts which exist in the specified lakehouse.
    """

    workspace_id = resolve_workspace_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    columns = {
        "Shortcut Name": "string",
        "Shortcut Path": "string",
        "Source Type": "string",
        "Source Workspace Id": "string",
        "Source Workspace Name": "string",
        "Source Item Id": "string",
        "Source Item Name": "string",
        "Source Item Type": "string",
        "OneLake Path": "string",
        "Connection Id": "string",
        "Location": "string",
        "Bucket": "string",
        "SubPath": "string",
        "Source Properties Raw": "string",
    }
    df = _create_dataframe(columns=columns)

    # To improve performance create a dataframe to cache all items for a given workspace
    itm_clms = {
        "Id": "string",
        "Display Name": "string",
        "Description": "string",
        "Type": "string",
        "Workspace Id": "string",
    }
    source_items_df = _create_dataframe(columns=itm_clms)

    url = f"/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts"

    if path is not None:
        url += f"?parentPath={path}"

    responses = _base_api(
        request=url,
        uses_pagination=True,
        client="fabric_sp",
    )

    sources = {
        "AdlsGen2": "adlsGen2",
        "AmazonS3": "amazonS3",
        "Dataverse": "dataverse",
        "ExternalDataShare": "externalDataShare",
        "GoogleCloudStorage": "googleCloudStorage",
        "OneLake": "oneLake",
        "S3Compatible": "s3Compatible",
    }

    rows = []
    for r in responses:
        for i in r.get("value", []):
            tgt = i.get("target", {})
            tgt_type = tgt.get("type")
            connection_id = tgt.get(sources.get(tgt_type), {}).get("connectionId")
            location = tgt.get(sources.get(tgt_type), {}).get("location")
            sub_path = tgt.get(sources.get(tgt_type), {}).get("subpath")
            source_workspace_id = tgt.get(sources.get(tgt_type), {}).get("workspaceId")
            source_item_id = tgt.get(sources.get(tgt_type), {}).get("itemId")
            bucket = tgt.get(sources.get(tgt_type), {}).get("bucket")
            source_workspace_name = (
                resolve_workspace_name(
                    workspace_id=source_workspace_id, throw_error=False
                )
                if source_workspace_id is not None
                else None
            )
            # Cache and use it to getitem type and name
            source_item_type = None
            source_item_name = None
            try:
                dfI = source_items_df[
                    source_items_df["Workspace Id"] == source_workspace_id
                ]
                if dfI.empty:
                    dfI = fabric.list_items(workspace=source_workspace_id)
                    source_items_df = pd.concat(
                        [source_items_df, dfI], ignore_index=True
                    )

                dfI_filt = dfI[dfI["Id"] == source_item_id]
                if not dfI_filt.empty:
                    source_item_type = dfI_filt["Type"].iloc[0]
                    source_item_name = dfI_filt["Display Name"].iloc[0]
            except Exception:
                pass

            rows.append(
                {
                    "Shortcut Name": i.get("name"),
                    "Shortcut Path": i.get("path"),
                    "Source Type": tgt_type,
                    "Source Workspace Id": source_workspace_id,
                    "Source Workspace Name": source_workspace_name,
                    "Source Item Id": source_item_id,
                    "Source Item Name": source_item_name,
                    "Source Item Type": source_item_type,
                    "OneLake Path": tgt.get(sources.get("oneLake"), {}).get("path"),
                    "Connection Id": connection_id,
                    "Location": location,
                    "Bucket": bucket,
                    "SubPath": sub_path,
                    "Source Properties Raw": str(tgt),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df
