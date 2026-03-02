from uuid import UUID
from typing import Optional, Literal
from sempy_labs._helper_functions import (
    _base_api,
    resolve_workspace_name_and_id,
    delete_item,
)
import sempy_labs._icons as icons
from sempy._utils._log import log


auto_sync_map = {True: "Enabled", False: "Disabled"}


@log
def create_mirrored_azure_databricks_catalog(
    name: str,
    catalog_name: str,
    databricks_workspace_connection_id: UUID,
    auto_sync: Optional[bool] = None,
    mirroring_mode: Literal["Full", "Partial"] = "Full",
    storage_connection_id: Optional[UUID] = None,
    mirror_configuration: Optional[dict] = None,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
) -> UUID:
    """
    Creates a mirrored Azure Databricks Catalog within a specified workspace.

    This is a wrapper function for the following API: `Items - Create Mirrored Azure Databricks Catalog <https://learn.microsoft.com/rest/api/fabric/mirroredazuredatabrickscatalog/items/create-mirrored-azure-databricks-catalog>`_.

    Parameters
    ----------
    name : str
        The display name of the mirrored Azure Databricks Catalog.
    catalog_name : str
        Azure databricks catalog name.
    databricks_workspace_connection_id : uuid.UUID
        The Azure databricks workspace connection id.
    auto_sync : bool, Default=None
        Enable or disable automatic synchronization for the catalog. Defaults to None, which means autoSync will be disabled.
    mirroring_mode : typing.Literal["Full", "Partial"], Default="Full"
        The mirroring mode for the catalog. Can be either "Full" or "Partial".
    storage_connection_id : uuid.UUID, default=None
        The storage connection id. This is required when mirroring_mode is set to "Full".
    mirror_configuration : dict, default=None
        The mirror configuration for the catalog. This is required when mirroring_mode is set to "Partial". See `here <https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/mirrored-azuredatabricks-unitycatalog-definition#contentdetails-example-1>`_ for examples.
    description : str, default=None
        The description of the mirrored Azure Databricks Catalog.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    uuid.UUID
        The ID of the created mirrored Azure Databricks Catalog.
    """
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {
        "displayName": name,
        "creationPayload": {
            "catalogName": catalog_name,
            "databricksWorkspaceConnectionId": str(databricks_workspace_connection_id),
            "mirroringMode": mirroring_mode,
        },
    }
    if description:
        payload["description"] = description
    if auto_sync is not None:
        payload["creationPayload"]["autoSync"] = auto_sync_map.get(auto_sync)
    if mirror_configuration:
        payload["creationPayload"]["mirrorConfiguration"] = mirror_configuration
    if storage_connection_id:
        payload["creationPayload"]["storageConnectionId"] = str(storage_connection_id)

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/mirroredAzureDatabricksCatalogs",
        method="post",
        payload=payload,
        status_codes=[201, 202],
        lro_return_json=True,
    )

    print(
        f"{icons.green_dot} The '{name}' mirrored Azure Databricks Catalog has been succesfully created within the '{workspace_name}' workspace."
    )
    return response.json().get("id")


@log
def delete_mirrored_azure_databricks_catalog(
    mirrored_azure_databricks_catalog: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Deletes a mirrored Azure Databricks Catalog.

    This is a wrapper function for the following API: `Items - Delete Mirrored Azure Databricks Catalog <https://learn.microsoft.com/rest/api/fabric/mirroredazuredatabrickscatalog/items/delete-mirrored-azure-databricks-catalog>`_.

    Parameters
    ----------
    mirrored_azure_databricks_catalog : str | uuid.UUID
        The name or ID of the mirrored Azure Databricks catalog to be deleted.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook
    """

    delete_item(
        item=mirrored_azure_databricks_catalog,
        type="MirroredAzureDatabricksCatalog",
        workspace=workspace,
    )


@log
def update_mirrored_azure_databricks_catalog(
    mirrored_azure_databricks_catalog: str | UUID,
    name: Optional[str] = None,
    auto_sync: Optional[bool] = None,
    mirroring_mode: Optional[Literal["Full", "Partial"]] = None,
    storage_connection_id: Optional[UUID] = None,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
) -> dict:
    """
    Updates the definition of a mirrored Azure Databricks Catalog within a specified workspace.

    This is a wrapper function for the following API: `Items - Update Mirrored Azure Databricks Catalog <https://learn.microsoft.com/rest/api/fabric/mirroredazuredatabrickscatalog/items/update-mirrored-azure-databricks-catalog>`_.

    Parameters
    ----------
    name : str
        The display name of the mirrored Azure Databricks Catalog.
    auto_sync : bool, Default=None
        Enable or disable automatic synchronization for the catalog. Defaults to None, which means autoSync will be disabled.
    mirroring_mode : typing.Literal["Full", "Partial"], Default="Full"
        The mirroring mode for the catalog. Can be either "Full" or "Partial".
    storage_connection_id : uuid.UUID, default=None
        The storage connection id. This is required when mirroring_mode is set to "Full".
    description : str, default=None
        The description of the mirrored Azure Databricks Catalog.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    dict
        The updated mirrored Azure Databricks Catalog item definition.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {"publicUpdateableExtendedProperties": {}}

    if name:
        payload["displayName"] = name
    if description:
        payload["description"] = description
    if auto_sync is not None:
        payload["publicUpdateableExtendedProperties"]["autoSync"] = auto_sync_map.get(
            auto_sync
        )
    if mirroring_mode:
        payload["publicUpdateableExtendedProperties"]["mirroringMode"] = mirroring_mode
    if storage_connection_id:
        payload["publicUpdateableExtendedProperties"]["storageConnectionId"] = str(
            storage_connection_id
        )

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/mirroredAzureDatabricksCatalogs/{mirrored_azure_databricks_catalog}",
        method="patch",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The '{mirrored_azure_databricks_catalog}' mirrored Azure Databricks Catalog has been succesfully updated within the '{workspace_name}' workspace."
    )
    return response.json()
