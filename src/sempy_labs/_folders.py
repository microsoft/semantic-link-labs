import pandas as pd
from typing import Optional
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    _is_valid_uuid,
)
import sempy_labs._icons as icons


def list_folders(
    workspace: Optional[str | UUID] = None,
    recursive: bool = True,
    root_folder: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows a list of folders from the specified workspace.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    recursive : bool, default=True
        Lists folders in a folder and its nested folders, or just a folder only. True - All folders in the folder and its nested folders are listed, False - Only folders in the folder are listed.
    root_folder : str | uuid.UUID, default=None
        This parameter allows users to filter folders based on a specific root folder. If not provided, the workspace is used as the root folder.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of folders from the specified workspace.
    """

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Folder Name": "string",
        "Folder Id": "string",
        "Parent Folder Id": "string",
    }

    df = _create_dataframe(columns=columns)

    url = f"/v1/workspaces/{workspace_id}/folders?recursive={recursive}"

    if _is_valid_uuid(root_folder):
        url += f"&rootFolderId={root_folder}"

    responses = _base_api(
        request=url,
        client="fabric_sp",
        uses_pagination=True,
    )

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Folder Name": v.get("displayName"),
                "Folder Id": v.get("id"),
                "Parent Folder Id": v.get("parentFolderId"),
            }

            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    # Add folder path
    folder_map = {row["Folder Id"]: row["Folder Name"] for _, row in df.iterrows()}

    def get_folder_path(folder_id):
        if folder_id not in folder_map:
            return ""

        row = df.loc[df["Folder Id"] == folder_id].iloc[0]
        if "Parent Folder Id" in row:
            return get_folder_path(row["Parent Folder Id"]) + "/" + row["Folder Name"]
        return row["Folder Name"]

    # Apply function to create the path column
    df["Folder Path"] = df["Folder Id"].apply(get_folder_path)

    # Filter the folders if specified
    if root_folder is not None and not _is_valid_uuid(root_folder):
        root = df[df["Folder Name"] == root_folder]
        if root.empty:
            raise ValueError(f"Folder name '{root_folder}' not found.")
        root_folder_id = root["Folder Id"].iloc[0]
        df = df[df["Parent Folder Id"] == root_folder_id]

    return df


def create_folder(
    name,
    workspace: Optional[str | UUID] = None,
    parent_folder: Optional[str | UUID] = None,
):
    """
    Creates a new folder in the specified workspace.

    Parameters
    ----------
    name : str
        The name of the folder to create.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    parent_folder : str | uuid.UUID, default=None
        The ID of the parent folder. If not provided, the folder will be created in the root folder of the workspace.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    url = f"/v1/workspaces/{workspace_id}/folders"

    payload = {
        "displayName": name,
    }

    if parent_folder:
        parent_folder_id = resolve_folder_id(folder=parent_folder, workspace=workspace)
        payload["parentFolderId"] = parent_folder_id

    _base_api(
        request=url,
        client="fabric_sp",
        method="post",
        payload=payload,
        status_codes=201,
    )

    print(
        f"{icons.green_dot} The '{name}' folder has been successfully created within the '{workspace_name}' workspace."
    )


def resolve_folder_id(
    folder: str | UUID, workspace: Optional[str | UUID] = None
) -> UUID:

    if _is_valid_uuid(folder):
        return folder
    else:
        df = list_folders(workspace=workspace)
        if not folder.startswith("/"):
            folder_path = f"/{folder}"
        else:
            folder_path = folder
        df_filt = df[df["Folder Path"] == folder_path]
        if df_filt.empty:
            (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
            raise ValueError(
                f"{icons.red_dot} The '{folder}' folder does not exist within the '{workspace_name}' workspace."
            )
        return df_filt["Folder Id"].iloc[0]


def delete_folder(folder: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Deletes a folder from the specified workspace.

    Parameters
    ----------
    folder : str | uuid.UUID
        The name or ID of the folder to move. If the folder is a subfolder, specify the path (i.e. "/folder/subfolder").
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    folder_id = resolve_folder_id(folder=folder, workspace=workspace)

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/folders/{folder_id}",
        client="fabric_sp",
        method="delete",
    )

    print(
        f"{icons.green_dot} The '{folder}' folder has been successfully deleted from the '{workspace_name}' workspace."
    )


def move_folder(
    folder: str | UUID,
    target_folder: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Moves a folder to a new location in the workspace.

    Parameters
    ----------
    folder : str | uuid.UUID
        The name or ID of the folder to move. If the folder is a subfolder, specify the path (i.e. "/folder/subfolder").
    target_folder : str | uuid.UUID
        The name or ID of the target folder where the folder will be moved. If the folder is a subfolder, specify the path (i.e. "/folder/subfolder").
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    target_folder_id = resolve_folder_id(folder=target_folder, workspace=workspace)
    folder_id = resolve_folder_id(folder=folder, workspace=workspace)

    payload = {
        "targetFolderId": target_folder_id,
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/folders/{folder_id}/move",
        client="fabric_sp",
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The '{folder}' folder has been successfully moved to the '{target_folder}' folder within the '{workspace_name}' workspace."
    )


def update_folder(
    folder: str | UUID, name: str, workspace: Optional[str | UUID] = None
):
    """
    Updates the name of a folder in the specified workspace.

    Parameters
    ----------
    folder : str | uuid.UUID
        The name/path or ID of the folder to update. If the folder is a subfolder, specify the path (i.e. "/folder/subfolder").
    name : str
        The new name for the folder. Must meet the `folder name requirements <https://learn.microsoft.com/fabric/fundamentals/workspaces-folders#folder-name-requirements>`_.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    folder_id = resolve_folder_id(folder=folder, workspace=workspace)

    payload = {
        "displayName": name,
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/folders/{folder_id}",
        client="fabric_sp",
        method="patch",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The '{folder}' folder has been successfully updated to '{name}' within the '{workspace_name}' workspace."
    )
