from uuid import UUID
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    _base_api,
)
import sempy_labs._icons as icons
from sempy._utils._log import log


@log
def refresh_catalog_metadata(
    mirrored_azure_databricks_catalog: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Refresh Databricks catalog metadata in mirroredAzureDatabricksCatalogs Item.

    This is a wrapper function for the following API: `Refresh Metadata - Items RefreshCatalogMetadata <https://learn.microsoft.com/rest/api/fabric/mirroredazuredatabrickscatalog/refresh-metadata/items-refresh-catalog-metadata>`_.

    Parameters
    ----------
    mirrored_azure_databricks_catalog : str | uuid.UUID
        The name or ID of the mirrored Azure Databricks catalog.
    workspace : str | uuie.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (catalog_name, catalog_id) = resolve_item_name_and_id(
        mirrored_azure_databricks_catalog
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/mirroredAzureDatabricksCatalogs/{catalog_id}/refreshCatalogMetadata",
        method="post",
        lro_return_status_code=True,
    )

    print(
        f"{icons.green_dot} The '{catalog_name}' Databricks Catalog metadata within the '{workspace_name}' workspace has been refreshed."
    )
