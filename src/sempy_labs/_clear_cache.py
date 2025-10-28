import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    is_default_semantic_model,
    _get_adls_client,
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
    _update_dataframe_datatypes,
    _base_api,
)
from typing import List, Optional
import sempy_labs._icons as icons
from sempy._utils._log import log
import pandas as pd
from uuid import UUID


@log
def clear_cache(dataset: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Clears the cache of a semantic model.
    See `here <https://learn.microsoft.com/analysis-services/instances/clear-the-analysis-services-caches?view=asallproducts-allversions>`_ for documentation.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    if is_default_semantic_model(dataset=dataset, workspace=workspace):
        raise ValueError(
            f"{icons.red_dot} Cannot run XMLA operations against a default semantic model. Please choose a different semantic model. "
            "See here for more information: https://learn.microsoft.com/fabric/data-warehouse/semantic-models"
        )

    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    xmla = f"""
            <ClearCache xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">
                <Object>
                    <DatabaseID>{dataset_id}</DatabaseID>
                </Object>
            </ClearCache>
            """
    fabric.execute_xmla(dataset=dataset_id, xmla_command=xmla, workspace=workspace_id)
    print(
        f"{icons.green_dot} Cache cleared for the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
    )


@log
def cancel_spid(dataset: str | UUID, spid: int, workspace: Optional[str | UUID] = None):
    """
    Cancels a given SPID (query) against a semantic model.
    See `here <https://learn.microsoft.com/analysis-services/xmla/xml-elements-commands/cancel-element-xmla?view=asallproducts-allversions>`_ for documentation.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    spid : int
        The SPID of the query to cancel. Can be found using the 'list_running_queries' function.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    xmla = f"""
            <Cancel xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">
                <SPID>{spid}</SPID>
            </Cancel>
            """
    fabric.execute_xmla(xmla_command=xmla, dataset=dataset, workspace=workspace)
    print(
        f"{icons.green_dot} The query with SPID '{spid}' has been cancelled in the '{dataset_name}' semantic model within the the '{workspace_name}' workspace."
    )


@log
def list_semantic_model_sessions(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    user_name: Optional[str | List[str]] = None,
    spid: Optional[int] = None,
) -> pd.DataFrame:
    """
    Lists the active sessions against a semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    user_name : str | List[str], default=None
        The user name or list of user names to filter by.
    spid : int, default=None
        The SPID of the query to filter by.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the active sessions against a semantic model.
    """

    if user_name and isinstance(user_name, str):
        user_name = [user_name]

    sessions = fabric.evaluate_dax(
        dataset=dataset,
        workspace=workspace,
        dax_string="""select * from $system.discover_sessions""",
    )
    if user_name:
        sessions = sessions[sessions["SESSION_USER_NAME"].isin(user_name)]
    if spid:
        sessions = sessions[sessions["SESSION_SPID"] == spid]
    return sessions


@log
def list_semantic_model_commands(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    min_elapsed_time_seconds: Optional[int] = None,
    spid: Optional[int] = None,
) -> pd.DataFrame:
    """
    Lists the active commands against a semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    min_elapsed_time_seconds : int, default=None
        The minimum elapsed time in seconds to filter by.
    spid : int, default=None
        The SPID of the query to filter by.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the active commands against a semantic model.
    """

    commands = fabric.evaluate_dax(
        dataset=dataset,
        workspace=workspace,
        dax_string="""select * from $system.discover_commands""",
    )

    if min_elapsed_time_seconds:
        commands = commands[
            commands["COMMAND_ELAPSED_TIME_MS"] >= min_elapsed_time_seconds * 1000
        ]
    if spid:
        commands = commands[commands["SESSION_SPID"] == spid]
    return commands


@log
def _cancel_refresh(dataset: str | UUID, workspace: Optional[str | UUID] = None):

    dataset_id = resolve_dataset_name_and_id(dataset, workspace)[1]

    df = list_semantic_model_commands(dataset=dataset, workspace=workspace)
    # Find refresh operations
    df_filt = df[
        (
            df["COMMAND_TEXT"].str.contains(
                '<Batch Transaction="false" xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">'
            )
        )
        & (
            df["COMMAND_TEXT"].str.contains(
                '<Refresh xmlns="http://schemas.microsoft.com/analysisservices/2014/engine">'
            )
        )
        & (df["COMMAND_TEXT"].str.contains(f"<DatabaseID>{dataset_id}</DatabaseID>"))
    ]
    if df_filt:
        spids = df_filt["SESSION_SPID"].values.tolist()
        for spid in spids:
            cancel_spid(dataset=dataset, spid=spid, workspace=workspace)
    else:
        print(f"{icons.info} No refresh operations found to cancel.")


@log
def cancel_long_running_queries(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    min_elapsed_time_seconds: int = 60,
    skip_users: str | List[str] = None,
    include_refresh_operations: bool = False,
):
    """
    Cancels long-running queries against a semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    min_elapsed_time_seconds : int, default=60
        The minimum elapsed time in seconds which defines a long-running query.
    skip_users : str | List[str], default=None
        The user name or list of user names to skip when cancelling long-running queries.
    include_refresh_operations : bool, default=False
        If True, includes refresh operations when cancelling long-running queries.
    """

    df = list_semantic_model_sessions(dataset=dataset, workspace=workspace)
    # Filter out refresh operations unless specified to include
    if not include_refresh_operations:
        df = df[
            ~(
                (
                    df["COMMAND_TEXT"].str.contains(
                        '<Batch Transaction="false" xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">'
                    )
                )
                & (
                    df["COMMAND_TEXT"].str.contains(
                        '<Refresh xmlns="http://schemas.microsoft.com/analysisservices/2014/engine">'
                    )
                )
            )
        ]

    if skip_users and isinstance(skip_users, str):
        skip_users = [skip_users]
    if skip_users:
        df = df[~df["SESSION_USER_NAME"].isin(skip_users)]
    df = df[df["SESSION_ELAPSED_TIME_MS"] >= min_elapsed_time_seconds * 1000]
    spids = df["SESSION_SPID"].values.tolist()

    for spid in spids:
        cancel_spid(dataset=dataset, spid=spid, workspace=workspace)


@log
def backup_semantic_model(
    dataset: str | UUID,
    file_path: str,
    allow_overwrite: bool = True,
    apply_compression: bool = True,
    workspace: Optional[str | UUID] = None,
    password: Optional[str] = None,
):
    """
    `Backs up <https://learn.microsoft.com/azure/analysis-services/analysis-services-backup>`_ a semantic model to the ADLS Gen2 storage account connected to the workspace.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    file_path : str
        The ADLS Gen2 storage account location in which to backup the semantic model. Always saves within the 'power-bi-backup/<workspace name>' folder.
        Must end in '.abf'.
        Example 1: file_path = 'MyModel.abf'
        Example 2: file_path = 'MyFolder/MyModel.abf'
    password : Optional[str], default=None
        Password to encrypt the backup file. If None, no password is used.
    allow_overwrite : bool, default=True
        If True, overwrites backup files of the same name. If False, the file you are saving cannot have the same name as a file that already exists in the same location.
    apply_compression : bool, default=True
        If True, compresses the backup file. Compressed backup files save disk space, but require slightly higher CPU utilization.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if not file_path.endswith(".abf"):
        raise ValueError(
            f"{icons.red_dot} The backup file for restoring must be in the .abf format."
        )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    tmsl = {
        "backup": {
            "database": dataset_name,
            "file": file_path,
            "allowOverwrite": allow_overwrite,
            "applyCompression": apply_compression,
        }
    }

    if password:
        tmsl["backup"]["password"] = password  # Add password only if provided

    fabric.execute_tmsl(script=tmsl, workspace=workspace_id)
    print(
        f"{icons.green_dot} The '{dataset_name}' semantic model within the '{workspace_name}' workspace has been backed up to the '{file_path}' location."
    )


@log
def restore_semantic_model(
    dataset: str,
    file_path: str,
    allow_overwrite: bool = True,
    ignore_incompatibilities: bool = True,
    force_restore: bool = False,
    workspace: Optional[str | UUID] = None,
    password: Optional[str] = None,
):
    """
    `Restores <https://learn.microsoft.com/power-bi/enterprise/service-premium-backup-restore-dataset>`_ a semantic model based on a backup (.abf) file
    within the ADLS Gen2 storage account connected to the workspace.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    file_path : str
        The location in which to backup the semantic model. Must end in '.abf'.
        Example 1: file_path = 'MyModel.abf'
        Example 2: file_path = 'MyFolder/MyModel.abf'
    password : Optional[str], default=None
        Password to decrypt the backup file. If None, no password is used.
    allow_overwrite : bool, default=True
        If True, overwrites backup files of the same name. If False, the file you are saving cannot have the same name as a file that already exists in the same location.
    ignore_incompatibilities : bool, default=True
        If True, ignores incompatibilities between Azure Analysis Services and Power BI Premium.
    force_restore: bool, default=False
        If True, restores the semantic model with the existing semantic model unloaded and offline.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if not file_path.endswith(".abf"):
        raise ValueError(
            f"{icons.red_dot} The backup file for restoring must be in the .abf format."
        )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    tmsl = {
        "restore": {
            "database": dataset,
            "file": file_path,
            "allowOverwrite": allow_overwrite,
            "security": "copyAll",
            "ignoreIncompatibilities": ignore_incompatibilities,
        }
    }

    if password:
        tmsl["restore"]["password"] = password

    if force_restore:
        tmsl["restore"]["forceRestore"] = force_restore

    fabric.execute_tmsl(script=tmsl, workspace=workspace_id)

    print(
        f"{icons.green_dot} The '{dataset}' semantic model has been restored to the '{workspace_name}' workspace based on the '{file_path}' backup file."
    )


@log
def copy_semantic_model_backup_file(
    source_workspace: str,
    target_workspace: str,
    source_file_name: str,
    target_file_name: str,
    storage_account: str,
    source_file_system: str = "power-bi-backup",
    target_file_system: str = "power-bi-backup",
):
    """
    Copies a semantic model backup file (.abf) from an Azure storage account to another location within the Azure storage account.

    Requirements:
        1. Must have an Azure storage account and connect it to both the source and target workspace.
        2. Must be a 'Storage Blob Data Contributor' for the storage account.
            Steps:
                1. Navigate to the storage account within the Azure Portal
                2. Navigate to 'Access Control (IAM)'
                3. Click '+ Add' -> Add Role Assignment
                4. Search for 'Storage Blob Data Contributor', select it and click 'Next'
                5. Add yourself as a member, click 'Next'
                6. Click 'Review + assign'

    Parameters
    ----------
    source_workspace : str
        The workspace name of the source semantic model backup file.
    target_workspace : str
        The workspace name of the target semantic model backup file destination.
    source_file_name : str
        The name of the source backup file (i.e. MyModel.abf).
    target_file_name : str
        The name of the target backup file (i.e. MyModel.abf).
    storage_account : str
        The name of the storage account.
    source_file_system : str, default="power-bi-backup"
        The container in which the source backup file is located.
    target_file_system : str, default="power-bi-backup"
        The container in which the target backup file will be saved.
    """

    suffix = ".abf"

    if not source_file_name.endswith(suffix):
        source_file_name = f"{source_file_name}{suffix}"
    if not target_file_name.endswith(suffix):
        target_file_name = f"{target_file_name}{suffix}"

    source_path = f"/{source_workspace}/{source_file_name}"
    target_path = f"/{target_workspace}/{target_file_name}"

    client = _get_adls_client(account_name=storage_account)

    source_file_system_client = client.get_file_system_client(
        file_system=source_file_system
    )
    destination_file_system_client = client.get_file_system_client(
        file_system=target_file_system
    )

    source_file_client = source_file_system_client.get_file_client(source_path)
    destination_file_client = destination_file_system_client.get_file_client(
        target_path
    )

    download = source_file_client.download_file()
    file_content = download.readall()

    # Upload the content to the destination file
    destination_file_client.create_file()  # Create the destination file
    destination_file_client.append_data(
        data=file_content, offset=0, length=len(file_content)
    )
    destination_file_client.flush_data(len(file_content))

    print(
        f"{icons.green_dot} The backup file of the '{source_file_name}' semantic model from the '{source_workspace}' workspace has been copied as the '{target_file_name}' semantic model backup file within the '{target_workspace}'."
    )


@log
def list_backups(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows a list of backup files contained within a workspace's ADLS Gen2 storage account.
    Requirement: An ADLS Gen2 storage account must be `connected to the workspace <https://learn.microsoft.com/power-bi/transform-model/dataflows/dataflows-azure-data-lake-storage-integration#connect-to-an-azure-data-lake-gen-2-at-a-workspace-level>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of backup files contained within a workspace's ADLS Gen2 storage account.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    response = _base_api(
        request=f"/v1.0/myorg/resources?resourceType=StorageAccount&folderObjectId={workspace_id}"
    )

    v = response.json().get("value", [])
    if not v:
        raise ValueError(
            f"{icons.red_dot} A storage account is not associated with the '{workspace_name}' workspace."
        )
    storage_account = v[0]["resourceName"]

    df = list_storage_account_files(storage_account=storage_account)
    colName = "Storage Account Name"
    df.insert(0, colName, df.pop(colName))

    return df


@log
def list_storage_account_files(
    storage_account: str, container: str = "power-bi-backup"
) -> pd.DataFrame:
    """
    Shows a list of files within an ADLS Gen2 storage account.

    Parameters
    ----------
    storage_account: str
        The name of the ADLS Gen2 storage account.
    container : str, default='power-bi-backup'
        The name of the container.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of files contained within an ADLS Gen2 storage account.
    """

    df = pd.DataFrame(
        columns=[
            "File Path",
            "File Size",
            "Creation Time",
            "Last Modified",
            "Expiry Time",
            "Encryption Scope",
        ]
    )

    client = _get_adls_client(storage_account)
    fs = client.get_file_system_client(container)

    for x in list(fs.get_paths()):
        if not x.is_directory:
            new_data = {
                "File Path": x.name,
                "File Size": x.content_length,
                "Creation Time": x.creation_time,
                "Last Modified": x.last_modified,
                "Expiry Time": x.expiry_time,
                "Encryption Scope": x.encryption_scope,
            }

            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    column_map = {
        "File Size": "int",
    }

    _update_dataframe_datatypes(dataframe=df, column_map=column_map)

    return df
