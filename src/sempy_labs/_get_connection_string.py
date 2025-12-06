from sempy_labs._helper_functions import (
    resolve_item_id,
    _base_api,
    resolve_workspace_id,
)
from typing import Optional, Literal
import sempy_labs._icons as icons
from uuid import UUID
from sempy._utils._log import log


@log
def get_connection_string(
    item: str | UUID,
    type: Literal["Lakehouse", "Warehouse", "SQLEndpoint"],
    workspace: Optional[str | UUID] = None,
    guest_tenant_id: Optional[UUID] = None,
    private_link_type: Optional[str] = None,
) -> str:
    """
    Returns the SQL connection string of the specified item.

    Parameters
    ----------
    item : str | uuid.UUID
        The name or ID of the item (Lakehouse or MirroredDatabase).
    type : Literal['Lakehouse', 'Warehouse', 'SQLEndpoint']
        The type of the item. Must be 'Lakehouse' or 'MirroredDatabase'.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    guest_tenant_id : uuid.UUID, default=None
        The guest tenant ID if the end user's tenant is different from the warehouse tenant.
    private_link_type : str, default=None
        Indicates the type of private link this connection string uses. Must be either 'Workspace' or 'None' or left as None.

    Returns
    -------
    str
        Returns the SQL connection string of the specified item.
    """
    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=item, type=type, workspace=workspace)

    type_dict = {
        "Warehouse": "warehouses",
        "SQLEndpoint": "sqlEndpoints",
    }
    type_for_url = type_dict.get(type)

    if type == "Lakehouse":
        response = _base_api(
            f"/v1/workspaces/{workspace_id}/lakehouses/{item_id}", client="fabric_sp"
        ).json()
        return (
            response.get("properties", {})
            .get("sqlEndpointProperties", {})
            .get("connectionString", {})
        )
    if type in ["SQLEndpoint", "Warehouse"]:
        url = f"/v1/workspaces/{workspace_id}/{type_for_url}/{item_id}/connectionString"
    else:
        raise ValueError(
            f"{icons.red_dot} The type must be 'Lakehouse', 'Warehouse' or 'SQLEndpoint'."
        )

    if private_link_type is not None and private_link_type not in ["Workspace", "None"]:
        raise ValueError(
            f"{icons.red_dot} private_link_type must be 'Workspace' or 'None' or left as None."
        )

    if guest_tenant_id or private_link_type:
        params = []
        if guest_tenant_id:
            params.append(f"guestTenantId={guest_tenant_id}")
        if private_link_type:
            params.append(f"privateLinkType={private_link_type}")
        param_str = "?" + "&".join(params)
        url += param_str

    response = _base_api(request=url, client="fabric_sp")

    return response.json().get("connectionString")
