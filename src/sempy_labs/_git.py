import sempy.fabric as fabric
import pandas as pd
import sempy_labs._icons as icons
from typing import Optional, List
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    lro,
)
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID


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

    request_body = {
        "gitProviderDetails": {
            "organizationName": organization_name,
            "projectName": project_name,
            "gitProviderType": "AzureDevOps",
            "repositoryName": repository_name,
            "branchName": branch_name,
            "directoryName": directory_name,
        }
    }

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/git/connect", json=request_body
    )
    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{workspace_name}' workspace has been connected to the '{project_name}' Git project in Azure DevOps within the '{repository_name}' repository."
    )


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

    request_body = {
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

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/git/connect", json=request_body
    )
    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{workspace_name}' workspace has been connected to the '{repository_name}' GitHub repository."
    )


def disconnect_workspace_from_git(workspace: Optional[str | UUID] = None):
    """
    Disconnects a workpsace from a git repository.

    This is a wrapper function for the following API: `Git - Disconnect <https://learn.microsoft.com/rest/api/fabric/core/git/disconnect>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/git/disconnect")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{workspace_name}' workspace has been disconnected from Git."
    )


def get_git_status(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Obtains the Git status of items in the workspace, that can be committed to Git.

    This is a wrapper function for the following API: `Git - Get Status <https://learn.microsoft.com/rest/api/fabric/core/git/get-status>.

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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = pd.DataFrame(
        columns=[
            "Workspace Head",
            "Remote Commit Hash",
            "Object ID",
            "Logical ID",
            "Item Type",
            "Item Name",
            "Workspace Change",
            "Remote Change",
            "Conflict Type",
        ]
    )

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/git/status")

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)

    result = lro(client, response).json()

    for changes in result.get("changes", []):
        item_metadata = changes.get("itemMetadata", {})
        item_identifier = item_metadata.get("itemIdentifier", {})

        new_data = {
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
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = pd.DataFrame(
        columns=[
            "Organization Name",
            "Project Name",
            "Git Provider Type",
            "Repository Name",
            "Branch Name",
            "Directory Name",
            "Workspace Head",
            "Last Sync Time",
            "Git Connection State",
        ]
    )

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/git/connection")

    if response.status_code != 200:
        raise FabricHTTPException(response)

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
    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


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

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/git/initializeConnection")

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)

    lro(client, response)

    print(
        f"{icons.green_dot} The '{workspace_name}' workspace git connection has been initialized."
    )

    return response.json().get("remoteCommitHash")


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

        request_body = {
            "mode": commit_mode,
            "workspaceHead": workspace_head,
            "comment": comment,
        }

        if item_ids is not None:
            request_body["items"] = [{"objectId": item_id} for item_id in item_ids]

        client = fabric.FabricRestClient()
        response = client.post(
            f"/v1/workspaces/{workspace_id}/git/commitToGit",
            json=request_body,
        )

        if response.status_code not in [200, 202]:
            raise FabricHTTPException(response)

        lro(client=client, response=response, return_status_code=True)

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

    request_body = {}
    request_body["remoteCommitHash"] = remote_commit_hash
    request_body["conflictResolution"] = {
        "conflictResolutionType": "Workspace",
        "conflictResolutionPolicy": conflict_resolution_policy,
    }

    if workspace_head is not None:
        request_body["workspaceHead"] = workspace_head
    if allow_override is not None:
        request_body["options"] = {"allowOverrideItems": allow_override}

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/git/updateFromGit",
        json=request_body,
    )

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)

    lro(client, response, return_status_code=True)

    print(
        f"{icons.green_dot} The '{workspace_name}' workspace has been updated with commits pushed to the connected branch."
    )
