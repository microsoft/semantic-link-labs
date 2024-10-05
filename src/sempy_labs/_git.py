import sempy.fabric as fabric
import pandas as pd
import sempy_labs._icons as icons
from typing import Optional, List
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    lro,
)
from sempy.fabric.exceptions import FabricHTTPException


def connect_workspace_to_git(
    organization_name: str,
    project_name: str,
    repository_name: str,
    branch_name: str,
    directory_name: str,
    git_provider_type: str = "AzureDevOps",
    workspace: Optional[str] = None,
):
    """
    Connects a workspace to a git repository.

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
    git_provider_type : str, default="AzureDevOps"
        A `Git provider type <https://learn.microsoft.com/rest/api/fabric/core/git/connect?tabs=HTTP#gitprovidertype>`_.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/git/connect?tabs=HTTP

    workspace, workspace_id = resolve_workspace_name_and_id(workspace)

    request_body = {
        "gitProviderDetails": {
            "organizationName": organization_name,
            "projectName": project_name,
            "gitProviderType": git_provider_type,
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
        f"{icons.green_dot} The '{workspace}' workspace has been connected to the '{project_name}' Git project within the '{repository_name}' repository."
    )


def disconnect_workspace_from_git(workspace: Optional[str] = None):
    """
    Disconnects a workpsace from a git repository.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/git/disconnect?tabs=HTTP

    workspace, workspace_id = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/git/disconnect")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{workspace}' workspace has been disconnected from Git."
    )


def get_git_status(workspace: Optional[str] = None) -> pd.DataFrame:
    """
    Obtains the Git status of items in the workspace, that can be committed to Git.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the Git status of items in the workspace.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/git/get-status?tabs=HTTP

    workspace, workspace_id = resolve_workspace_name_and_id(workspace)

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


def get_git_connection(workspace: Optional[str] = None) -> pd.DataFrame:
    """
    Obtains the Git status of items in the workspace, that can be committed to Git.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the Git status of items in the workspace.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/git/get-status?tabs=HTTP

    workspace, workspace_id = resolve_workspace_name_and_id(workspace)

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


def initialize_git_connection(workspace: Optional[str] = None):
    """
    Initializes a connection for a workspace that is connected to Git.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/git/initialize-connection?tabs=HTTP

    workspace, workspace_id = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/git/initializeConnection")

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)

    lro(client, response)

    print(
        f"{icons.green_dot} The '{workspace}' workspace git connection has been initialized."
    )


def commit_to_git(
    comment: str, item_ids: str | List[str] = None, workspace: Optional[str] = None
):
    """
    Commits all or a selection of items within a workspace to Git.

    Parameters
    ----------
    comment : str
        The Git commit comment.
    item_ids : str | List[str], default=None
        A list of item Ids to commit to Git.
        Defaults to None which commits all items to Git.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/git/commit-to-git?tabs=HTTP

    workspace, workspace_id = resolve_workspace_name_and_id(workspace)

    gs = get_git_status(workspace=workspace)
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

    lro(client, response)

    if commit_mode == "All":
        print(
            f"{icons.green_dot} All items within the '{workspace}' workspace have been committed to Git."
        )
    else:
        print(
            f"{icons.green_dot} The {item_ids} items ithin the '{workspace}' workspace have been committed to Git."
        )


def update_from_git(
    remote_commit_hash: str,
    conflict_resolution_policy: str,
    workspace_head: Optional[str] = None,
    allow_override: bool = False,
    workspace: Optional[str] = None,
):
    """
    Updates the workspace with commits pushed to the connected branch.

    Parameters
    ----------
    workspace_head : str
        Full SHA hash that the workspace is synced to. This value may be null only after Initialize Connection.
        In other cases, the system will validate that the given value is aligned with the head known to the system.
    remove_commit_hash : str
        Remote full SHA commit hash.
    confilict_resolution_policy : str
        The `conflict resolution policy <https://learn.microsoft.com/rest/api/fabric/core/git/update-from-git?tabs=HTTP#conflictresolutionpolicy>`_.
    allow_override : bool, default=False
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/git/update-from-git?tabs=HTTP

    workspace, workspace_id = resolve_workspace_name_and_id(workspace)

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
        f"{icons.green_dot} The '{workspace}' workspace has been updated with commits pushed to the connected branch."
    )
