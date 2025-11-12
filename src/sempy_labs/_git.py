import pandas as pd
import sempy_labs._icons as icons
from typing import Optional, List
from sempy_labs._helper_functions import (
    _update_dataframe_datatypes,
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
)
from uuid import UUID
from sempy._utils._log import log


@log
def connect_workspace_to_azure_dev_ops(
    organization_name: str,
    project_name: str,
    repository_name: str,
    branch_name: str,
    directory_name: str,
    workspace: Optional[str | UUID] = None,
):
    """
    Connects a workspace to an Azure DevOps git repository.

    This is a wrapper function for the following API: `Git - Connect <https://learn.microsoft.com/rest/api/fabric/core/git/connect>`_.

    Parameters
    ----------
    organization_name : str
        The organization name.
    project_name : str
        The project name.
    repository_name : str
        The repository name.
    branch_name : str
        The branch name.
    directory_name : str
        The directory name.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {
        "gitProviderDetails": {
            "organizationName": organization_name,
            "projectName": project_name,
            "gitProviderType": "AzureDevOps",
            "repositoryName": repository_name,
            "branchName": branch_name,
            "directoryName": directory_name,
        }
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/git/connect",
        payload=payload,
        method="post",
    )

    print(
        f"{icons.green_dot} The '{workspace_name}' workspace has been connected to the '{project_name}' Git project in Azure DevOps within the '{repository_name}' repository."
    )


@log
def connect_workspace_to_github(
    owner_name: str,
    repository_name: str,
    branch_name: str,
    directory_name: str,
    connection_id: UUID,
    source: str = "ConfiguredConnection",
    workspace: Optional[str | UUID] = None,
):
    """
    Connects a workspace to a GitHub git repository.

    This is a wrapper function for the following API: `Git - Connect <https://learn.microsoft.com/rest/api/fabric/core/git/connect>`_.

    Parameters
    ----------
    owner_name : str
        The owner name.
    repository_name : str
        The repository name.
    branch_name : str
        The branch name.
    directory_name : str
        The directory name.
    source : str, default="ConfiguredConnection"
        The Git credentials source.
    connection_id : uuid.UUID
        The object ID of the connection.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {
        "gitProviderDetails": {
            "ownerName": owner_name,
            "gitProviderType": "GitHub",
            "repositoryName": repository_name,
            "branchName": branch_name,
            "directoryName": directory_name,
        },
        "myGitCredentials": {
            "source": source,
            "connectionId": connection_id,
        },
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/git/connect",
        payload=payload,
        method="post",
    )

    print(
        f"{icons.green_dot} The '{workspace_name}' workspace has been connected to the '{repository_name}' GitHub repository."
    )


@log
def disconnect_workspace_from_git(workspace: Optional[str | UUID] = None):
    """
    Disconnects a workspace from a git repository.

    This is a wrapper function for the following API: `Git - Disconnect <https://learn.microsoft.com/rest/api/fabric/core/git/disconnect>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    _base_api(request=f"/v1/workspaces/{workspace_id}/git/disconnect", method="post")

    print(
        f"{icons.green_dot} The '{workspace_name}' workspace has been disconnected from Git."
    )


@log
def get_git_status(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Obtains the Git status of items in the workspace, that can be committed to Git.

    This is a wrapper function for the following API: `Git - Get Status <https://learn.microsoft.com/rest/api/fabric/core/git/get-status>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the Git status of items in the workspace.
    """

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Workspace Head": "str",
        "Remote Commit Hash": "str",
        "Object ID": "str",
        "Logical ID": "str",
        "Item Type": "str",
        "Item Name": "str",
        "Workspace Change": "str",
        "Remote Change": "str",
        "Conflict Type": "str",
    }

    df = _create_dataframe(columns=columns)

    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/git/status",
        lro_return_json=True,
        status_codes=None,
    )

    rows = []
    for changes in result.get("changes", []):
        item_metadata = changes.get("itemMetadata", {})
        item_identifier = item_metadata.get("itemIdentifier", {})

        rows.append(
            {
                "Workspace Head": result.get("workspaceHead"),
                "Remote Commit Hash": result.get("remoteCommitHash"),
                "Object ID": item_identifier.get("objectId"),
                "Logical ID": item_identifier.get("logicalId"),
                "Item Type": item_metadata.get("itemType"),
                "Item Name": item_metadata.get("displayName"),
                "Remote Change": changes.get("remoteChange"),
                "Workspace Change": changes.get("workspaceChange"),
                "Conflict Type": changes.get("conflictType"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df


@log
def get_git_connection(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Obtains the Git status of items in the workspace, that can be committed to Git.

    This is a wrapper function for the following API: `Git - Get Connection <https://learn.microsoft.com/rest/api/fabric/core/git/get-connection>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the Git status of items in the workspace.
    """

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Organization Name": "str",
        "Project Name": "str",
        "Git Provider Type": "str",
        "Repository Name": "str",
        "Branch Name": "str",
        "Directory Name": "str",
        "Workspace Head": "str",
        "Last Sync Time": "datetime",
        "Git Connection State": "str",
    }

    df = _create_dataframe(columns=columns)

    response = _base_api(request=f"/v1/workspaces/{workspace_id}/git/connection")

    r = response.json()
    provider_details = r.get("gitProviderDetails", {})
    sync_details = r.get("gitSyncDetails", {})

    new_data = {
        "Organization Name": provider_details.get("organizationName"),
        "Project Name": provider_details.get("projectName"),
        "Git Provider Type": provider_details.get("gitProviderType"),
        "Repository Name": provider_details.get("repositoryName"),
        "Branch Name": provider_details.get("branchName"),
        "Directory Name": provider_details.get("directoryName"),
        "Workspace Head": sync_details.get("head"),
        "Last Sync Time": sync_details.get("lastSyncTime"),
        "Git Connection State": r.get("gitConnectionState"),
    }
    df = pd.DataFrame([new_data], columns=columns.keys())
    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def initialize_git_connection(workspace: Optional[str | UUID] = None) -> str:
    """
    Initializes a connection for a workspace that is connected to Git.

    This is a wrapper function for the following API: `Git - Initialize Connection <https://learn.microsoft.com/rest/api/fabric/core/git/initialize-connection>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        Remote full SHA commit hash.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    response_json = _base_api(
        request=f"/v1/workspaces/{workspace_id}/git/initializeConnection",
        method="post",
        lro_return_json=True,
        status_codes=None,
    )

    print(
        f"{icons.green_dot} The '{workspace_name}' workspace git connection has been initialized."
    )

    return response_json.get("remoteCommitHash")


@log
def commit_to_git(
    comment: str,
    item_ids: UUID | List[UUID] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Commits all or a selection of items within a workspace to Git.

    This is a wrapper function for the following API: `Git - Commit to Git <https://learn.microsoft.com/rest/api/fabric/core/git/commit-to-git>`_.

    Parameters
    ----------
    comment : str
        The Git commit comment.
    item_ids : uuid.UUID | List[uuid.UUID], default=None
        A list of item Ids to commit to Git.
        Defaults to None which commits all items to Git.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    gs = get_git_status(workspace=workspace_id)
    if not gs.empty:
        workspace_head = gs["Workspace Head"].iloc[0]

        if item_ids is None:
            commit_mode = "All"
        else:
            commit_mode = "Selective"

        if isinstance(item_ids, str):
            item_ids = [item_ids]

        payload = {
            "mode": commit_mode,
            "workspaceHead": workspace_head,
            "comment": comment,
        }

        if item_ids is not None:
            payload["items"] = [{"objectId": item_id} for item_id in item_ids]

        _base_api(
            request=f"/v1/workspaces/{workspace_id}/git/commitToGit",
            method="post",
            payload=payload,
            lro_return_status_code=True,
            status_codes=None,
        )

        if commit_mode == "All":
            print(
                f"{icons.green_dot} All items within the '{workspace_name}' workspace have been committed to Git."
            )
        else:
            print(
                f"{icons.green_dot} The {item_ids} items within the '{workspace_name}' workspace have been committed to Git."
            )
    else:
        print(
            f"{icons.info} Git already up to date: no modified items found within the '{workspace_name}' workspace."
        )


@log
def update_from_git(
    remote_commit_hash: str,
    conflict_resolution_policy: str,
    workspace_head: Optional[str] = None,
    allow_override: bool = False,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates the workspace with commits pushed to the connected branch.

    This is a wrapper function for the following API: `Git - Update From Git <https://learn.microsoft.com/rest/api/fabric/core/git/update-from-git>`_.

    Parameters
    ----------
    remote_commit_hash : str
        Remote full SHA commit hash.
    confilict_resolution_policy : str
        The `conflict resolution policy <https://learn.microsoft.com/rest/api/fabric/core/git/update-from-git?tabs=HTTP#conflictresolutionpolicy>`_.
    workspace_head : str
        Full SHA hash that the workspace is synced to. This value may be null only after Initialize Connection.
        In other cases, the system will validate that the given value is aligned with the head known to the system.
    allow_override : bool, default=False
        User consent to override incoming items during the update from Git process. When incoming items are present and the allow override items is not specified or is provided as false, the update operation will not start. Default value is false.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    conflict_resolution_policies = ["PreferWorkspace", "PreferRemote"]
    if "remote" in [policy.lower() for policy in conflict_resolution_policies]:
        conflict_resolution_policies = "PreferRemote"
    elif "workspace" in [policy.lower() for policy in conflict_resolution_policies]:
        conflict_resolution_policies = "PreferWorkspace"

    if conflict_resolution_policy not in conflict_resolution_policies:
        raise ValueError(
            f"{icons.red_dot} Invalid conflict resolution policy. Valid options: {conflict_resolution_policies}."
        )

    payload = {}
    payload["remoteCommitHash"] = remote_commit_hash
    payload["conflictResolution"] = {
        "conflictResolutionType": "Workspace",
        "conflictResolutionPolicy": conflict_resolution_policy,
    }

    if workspace_head is not None:
        payload["workspaceHead"] = workspace_head
    if allow_override is not None:
        payload["options"] = {"allowOverrideItems": allow_override}

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/git/updateFromGit",
        method="post",
        payload=payload,
        lro_return_status_code=True,
        status_codes=None,
    )

    print(
        f"{icons.green_dot} The '{workspace_name}' workspace has been updated with commits pushed to the connected branch."
    )


@log
def get_my_git_credentials(
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns the user's Git credentials configuration details.

    This is a wrapper function for the following API: `Git - Get My Git Credentials <https://learn.microsoft.com/rest/api/fabric/core/git/get-my-git-credentials>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the user's Git credentials configuration details.
    """

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Source": "string",
    }

    df = _create_dataframe(columns)

    response = _base_api(request=f"/v1/workspaces/{workspace_id}/git/myGitCredentials")

    r = response.json()
    new_data = {
        "Source": r.get("source"),
        "Connection Id": r.get("connectionId"),
    }
    df = pd.DataFrame([new_data], columns=columns.keys())

    return df


@log
def update_my_git_credentials(
    source: str,
    connection_id: Optional[UUID] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates the user's Git credentials configuration details.

    This is a wrapper function for the following API: `Git - Update My Git Credentials <https://learn.microsoft.com/rest/api/fabric/core/git/update-my-git-credentials>`_.

    Parameters
    ----------
    source : str
        The Git credentials source. Valid options: 'Automatic', 'ConfiguredConnection', 'None'.
    connection_id : UUID, default=None
        The object ID of the connection. Valid only for the 'ConfiguredConnection' source.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if source == "ConfiguredConnection" and connection_id is None:
        raise ValueError(
            f"{icons.red_dot} The 'ConfiguredConnection' source requires a connection_id."
        )

    payload = {
        "source": source,
    }

    if connection_id is not None:
        payload["connectionId"] = connection_id

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/git/myGitCredentials",
        method="patch",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The user's Git credentials have been updated accordingly."
    )
