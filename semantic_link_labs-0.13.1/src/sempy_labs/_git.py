import pandas as pd
from typing import Optional, List
from uuid import UUID
from sempy._utils._log import log
import sempy_labs.git as git


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

    git.connect_workspace_to_azure_dev_ops(
        organization_name=organization_name,
        project_name=project_name,
        repository_name=repository_name,
        branch_name=branch_name,
        directory_name=directory_name,
        workspace=workspace,
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

    git.connect_workspace_to_github(
        owner_name=owner_name,
        repository_name=repository_name,
        branch_name=branch_name,
        directory_name=directory_name,
        connection_id=connection_id,
        source=source,
        workspace=workspace,
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

    git.disconnect_workspace_from_git(workspace=workspace)


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

    return git.get_status(workspace=workspace)


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

    return git.get_connection(workspace=workspace)


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

    return git.initialize_connection(workspace=workspace)


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

    git.commit_to_git(comment=comment, item_ids=item_ids, workspace=workspace)


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

    git.update_from_git(
        remote_commit_hash=remote_commit_hash,
        conflict_resolution_policy=conflict_resolution_policy,
        workspace_head=workspace_head,
        allow_override=allow_override,
        workspace=workspace,
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

    return git.get_my_git_credentials(workspace=workspace)


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

    git.update_my_git_credentials(
        source=source, connection_id=connection_id, workspace=workspace
    )
