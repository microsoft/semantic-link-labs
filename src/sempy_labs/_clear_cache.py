import sempy.fabric as fabric
from ._helper_functions import resolve_dataset_id, is_default_semantic_model
from typing import Optional
import sempy_labs._icons as icons


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
            "Cannot run XMLA operations against a `default semantic model <https://learn.microsoft.com/fabric/data-warehouse/semantic-models>`_."
            " Please choose a different semantic model."
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

    outputtext = f"{icons.green_dot} Cache cleared for the '{dataset}' semantic model within the '{workspace}' workspace."

    return outputtext


def backup_semantic_model(
    dataset: str,
    file_path: str,
    allow_overwrite: Optional[bool] = True,
    apply_compression: Optional[bool] = True,
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


def restore_semantic_model(
    dataset: str,
    file_path: str,
    allow_overwrite: Optional[bool] = True,
    ignore_incompatibilities: Optional[bool] = True,
    force_restore: Optional[bool] = False,
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
        f"{icons.green_dot} The '{dataset}' semantic model has been restored to the '{workspace}' workspace based on teh '{file_path}' backup file."
    )


def copy_semantic_model_backup_file(
    source_workspace: str,
    target_workspace: str,
    source_file_name: str,
    target_file_name: str,
    storage_account_url: str,
    key_vault_uri: str,
    key_vault_account_key: str,
    source_file_system: Optional[str] = "power-bi-backup",
    target_file_system: Optional[str] = "power-bi-backup",
):

    from notebookutils import mssparkutils
    from azure.storage.filedatalake import DataLakeServiceClient

    account_key = mssparkutils.credentials.getSecret(
        key_vault_uri, key_vault_account_key
    )

    if not source_file_name.endswith(".abf"):
        source_file_name = f"{source_file_name}.abf"
    if not target_file_name.endswith(".abf"):
        target_file_name = f"{target_file_name}.abf"

    source_path = f"/{source_workspace}/{source_file_name}"
    target_path = f"/{target_workspace}/{target_file_name}"
    service_client = DataLakeServiceClient(
        account_url=storage_account_url, credential=account_key
    )

    source_file_system_client = service_client.get_file_system_client(
        file_system=source_file_system
    )
    destination_file_system_client = service_client.get_file_system_client(
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


def create_azure_storage_account(
    azure_subscription_id: str,
    resource_group: str,
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
    storage_account: str,
    region: str,
    sku: Optional[str] = "Standard_GRS",
):
    "https://learn.microsoft.com/en-us/rest/api/storagerp/storage-sample-create-account"
    "https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/create?view=rest-storageservices-datalakestoragegen2-2019-12-12"

    import requests
    from sempy.fabric.exceptions import FabricHTTPException
    from sempy_labs._capacities import get_azure_token_credentials

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Storage/storageAccounts/{storage_account}?api-version=2018-02-01"

    payload = {
        "sku": {"name": sku},
        "kind": "StorageV2",
        "location": region,
    }

    azure_token, credential, headers = get_azure_token_credentials(
        key_vault_uri=key_vault_uri,
        key_vault_tenant_id=key_vault_tenant_id,
        key_vault_client_id=key_vault_client_id,
        key_vault_client_secret=key_vault_client_secret,
    )

    response = requests.put(url, headers=headers, json=payload)

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{storage_account}' storage account has been created."
    )
