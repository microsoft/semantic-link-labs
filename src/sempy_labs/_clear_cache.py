import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_dataset_id,
    is_default_semantic_model,
    _get_adls_client,
)
from typing import Optional
import sempy_labs._icons as icons
from sempy._utils._log import log
import pandas as pd
from sempy.fabric.exceptions import FabricHTTPException


def clear_cache(dataset: str, workspace: Optional[str] = None):
    """
    Clears the cache of a semantic model.
    See `here <https://learn.microsoft.com/analysis-services/instances/clear-the-analysis-services-caches?view=asallproducts-allversions>`_ for documentation.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace = fabric.resolve_workspace_name(workspace)
    if is_default_semantic_model(dataset=dataset, workspace=workspace):
        raise ValueError(
            f"{icons.red_dot} Cannot run XMLA operations against a default semantic model. Please choose a different semantic model. "
            "See here for more information: https://learn.microsoft.com/fabric/data-warehouse/semantic-models"
        )

    dataset_id = resolve_dataset_id(dataset=dataset, workspace=workspace)

    xmla = f"""
            <ClearCache xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">
                <Object>
                    <DatabaseID>{dataset_id}</DatabaseID>
                </Object>
            </ClearCache>
            """
    fabric.execute_xmla(dataset=dataset, xmla_command=xmla, workspace=workspace)
    print(
        f"{icons.green_dot} Cache cleared for the '{dataset}' semantic model within the '{workspace}' workspace."
    )


@log
def backup_semantic_model(
    dataset: str,
    file_path: str,
    allow_overwrite: bool = True,
    apply_compression: bool = True,
    workspace: Optional[str] = None,
):
    """
    `Backs up <https://learn.microsoft.com/azure/analysis-services/analysis-services-backup>`_ a semantic model to the ADLS Gen2 storage account connected to the workspace.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    file_path : str
        The ADLS Gen2 storage account location in which to backup the semantic model. Always saves within the 'power-bi-backup/<workspace name>' folder.
        Must end in '.abf'.
        Example 1: file_path = 'MyModel.abf'
        Example 2: file_path = 'MyFolder/MyModel.abf'
    allow_overwrite : bool, default=True
        If True, overwrites backup files of the same name. If False, the file you are saving cannot have the same name as a file that already exists in the same location.
    apply_compression : bool, default=True
        If True, compresses the backup file. Compressed backup files save disk space, but require slightly higher CPU utilization.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if not file_path.endswith(".abf"):
        raise ValueError(
            f"{icons.red_dot} The backup file for restoring must be in the .abf format."
        )

    workspace = fabric.resolve_workspace_name(workspace)

    tmsl = {
        "backup": {
            "database": dataset,
            "file": file_path,
            "allowOverwrite": allow_overwrite,
            "applyCompression": apply_compression,
        }
    }

    fabric.execute_tmsl(script=tmsl, workspace=workspace)
    print(
        f"{icons.green_dot} The '{dataset}' semantic model within the '{workspace}' workspace has been backed up to the '{file_path}' location."
    )


@log
def restore_semantic_model(
    dataset: str,
    file_path: str,
    allow_overwrite: bool = True,
    ignore_incompatibilities: bool = True,
    force_restore: bool = False,
    workspace: Optional[str] = None,
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
    allow_overwrite : bool, default=True
        If True, overwrites backup files of the same name. If False, the file you are saving cannot have the same name as a file that already exists in the same location.
    ignore_incompatibilities : bool, default=True
        If True, ignores incompatibilities between Azure Analysis Services and Power BI Premium.
    force_restore: bool, default=False
        If True, restores the semantic model with the existing semantic model unloaded and offline.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """
    # https://learn.microsoft.com/en-us/power-bi/enterprise/service-premium-backup-restore-dataset

    if not file_path.endswith(".abf"):
        raise ValueError(
            f"{icons.red_dot} The backup file for restoring must be in the .abf format."
        )

    workspace = fabric.resolve_workspace_name(workspace)

    tmsl = {
        "restore": {
            "database": dataset,
            "file": file_path,
            "allowOverwrite": allow_overwrite,
            "security": "copyAll",
            "ignoreIncompatibilities": ignore_incompatibilities,
        }
    }

    if force_restore:
        tmsl["restore"]["forceRestore"] = force_restore

    fabric.execute_tmsl(script=tmsl, workspace=workspace)

    print(
        f"{icons.green_dot} The '{dataset}' semantic model has been restored to the '{workspace}' workspace based on the '{file_path}' backup file."
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
def list_backups(workspace: Optional[str] = None) -> pd.DataFrame:
    """
    Shows a list of backup files contained within a workspace's ADLS Gen2 storage account.
    Requirement: An ADLS Gen2 storage account must be `connected to the workspace <https://learn.microsoft.com/power-bi/transform-model/dataflows/dataflows-azure-data-lake-storage-integration#connect-to-an-azure-data-lake-gen-2-at-a-workspace-level>`_.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of backup files contained within a workspace's ADLS Gen2 storage account.
    """

    client = fabric.PowerBIRestClient()
    workspace = fabric.resolve_workspace_name(workspace)
    workspace_id = fabric.resolve_workspace_id(workspace)
    response = client.get(
        f"/v1.0/myorg/resources?resourceType=StorageAccount&folderObjectId={workspace_id}"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    v = response.json().get("value", [])
    if not v:
        raise ValueError(
            f"{icons.red_dot} A storage account is not associated with the '{workspace}' workspace."
        )
    storage_account = v[0]["resourceName"]
    df = list_storage_account_files(storage_account=storage_account)
    df["Storage Account Name"] = storage_account

    return df


@log
def list_storage_account_files(
    storage_account: str, container: str = "power-bi-backup"
) -> pd.DataFrame:
    """
    Shows a list of files within an ADLS Gen2 storage account, including subfolders.

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

    onelake = _get_adls_client(storage_account)
    fs = onelake.get_file_system_client(container)

    def traverse_dirs(path, df):
        for x in list(fs.get_paths(path)):
            if x.is_directory:
                df = traverse_dirs(x.name, df)
            else:
                new_data = {
                    "File Path": x.name,
                    "File Size": x.content_length,
                    "Creation Time": x.creation_time,
                    "Last Modified": x.last_modified,
                    "Expiry Time": x.expiry_time,
                    "Encryption Scope": x.encryption_scope,
                }

                df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        return df

    df = traverse_dirs("", df)

    df["File Size"] = df["File Size"].astype(int)

    return df
