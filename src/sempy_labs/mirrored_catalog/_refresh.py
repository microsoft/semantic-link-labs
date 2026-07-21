from typing import Optional
from sempy_labs._helper_functions import (
    _base_api,
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
)
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def refresh_catalog_metadata(
    mirrored_catalog: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Refreshes the metadata of a mirrored catalog.

    This is a wrapper function for the following API: `Refresh - Refresh Catalog Metadata <https://learn.microsoft.com/rest/api/fabric/mirroredcatalog/refresh/refresh-catalog-metadata(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_catalog : str | uuid.UUID
        The name or ID of the mirrored catalog.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """
    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    mirrored_catalog_name, mirrored_catalog_id = resolve_item_name_and_id(
        item=mirrored_catalog, type="MirroredCatalog", workspace=workspace_id
    )

    _base_api(
        f"/v1/workspaces/{workspace_id}/mirroredCatalogs/{mirrored_catalog_id}/refreshCatalogMetadata?beta=True",
        method="post",
        lro_return_status_code=True,
        status_codes=[200, 202],
    )

    print(
        f"{icons.green_dot} The mirrored catalog metadata refresh for the '{mirrored_catalog_name}' within the '{workspace_name}' workspace has been triggered successfully."
    )
