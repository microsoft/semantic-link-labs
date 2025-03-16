from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    _get_adls_client,
    _get_blob_client,
)
from uuid import UUID
from typing import Optional


def restore_lakehouse_table(
    table_name,
    storage_account: str,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    schema: Optional[str] = None,
):
    """
    Restores a delta table in a lakehouse from a deleted state.

    Parameters
    ----------
    table_name : str
        The name of the table to restore.
    storage_account: str
        The name of the ADLS Gen2 storage account.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    schema : str, default=None
        The name of the schema to which the table belongs (for schema-enabled lakehouses). If None, the default schema is used.
    """

    (workspace_name, workspace_id) = resolve_lakehouse_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_workspace_name_and_id(
        lakehouse, workspace_id
    )

    data_path = f"{lakehouse_name}.Lakehouse/Tables"
    if schema:
        data_path += f"/{schema}"
    data_path += f"/{table_name}"

    client = _get_adls_client(storage_account)
    fs = client.get_file_system_client(workspace_name)
    # paths = fs.get_paths(path=data_path)
    deleted_paths = fs.list_deleted_paths(path_prefix=data_path)
    bsc = _get_blob_client(account_name=storage_account)
    ccli = bsc.get_container_client(container=workspace_name)
    for path in deleted_paths:
        blob_client = ccli.get_blob_client(path)
        blob_client.undelete_blob()
    return deleted_paths
