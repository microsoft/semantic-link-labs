import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_lakehouse_name,
    resolve_lakehouse_id,
    resolve_warehouse_id,
)
from typing import Optional
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException


def generate_shared_expression(
    item_name: Optional[str] = None,
    item_type: Optional[str] = "Lakehouse",
    workspace: Optional[str] = None,
    direct_lake_over_onelake: Optional[bool] = False,
) -> str:
    """
    Dynamically generates the M expression used by a Direct Lake model for a given lakehouse/warehouse.

    Parameters
    ----------
    item_name : str, default=None
        The Fabric lakehouse or warehouse name.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    item_type : str, default="Lakehouse"
        The Fabric item name. Valid options: 'Lakehouse', 'Warehouse'.
    workspace : str, default=None
        The Fabric workspace used by the item.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    direct_lake_over_onelake : bool, defualt=False
        Generates an expression required for a Direct Lake over OneLake semantic mode. Only available for lakehouses, not warehouses.

    Returns
    -------
    str
        Shows the expression which can be used to connect a Direct Lake semantic model to its SQL Endpoint.
    """

    workspace = fabric.resolve_workspace_name(workspace)
    workspace_id = fabric.resolve_workspace_id(workspace)
    item_types = ["Lakehouse", "Warehouse"]
    item_type = item_type.capitalize()
    if item_type not in item_types:
        raise ValueError(
            f"{icons.red_dot} Invalid item type. Valid options: {item_types}."
        )

    if item_type == 'Warehouse' and direct_lake_over_onelake:
        raise ValueError(f"{icons.red_dot} Direct Lake over OneLake is only supported for lakehouses, not warehouses.")

    if item_name is None:
        item_id = fabric.get_lakehouse_id()
        item_name = resolve_lakehouse_name(item_id, workspace)
    elif item_name is not None and item_type == "Lakehouse":
        item_id = resolve_lakehouse_id(lakehouse=item_name, workspace=workspace)
    elif item_type == "Warehouse":
        item_id = resolve_warehouse_id(warehouse=item_name, workspace=workspace)

    client = fabric.FabricRestClient()
    item_type_rest = f"{item_type.lower()}s"
    response = client.get(f"/v1/workspaces/{workspace_id}/{item_type_rest}/{item_id}")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    prop = response.json().get("properties")

    if item_type == "Lakehouse":
        sqlprop = prop.get("sqlEndpointProperties")
        sqlEPCS = sqlprop.get("connectionString")
        sqlepid = sqlprop.get("id")
        provStatus = sqlprop.get("provisioningStatus")
    elif item_type == "Warehouse":
        sqlEPCS = prop.get("connectionString")
        sqlepid = item_id
        provStatus = None

    if provStatus == "InProgress":
        raise ValueError(
            f"{icons.red_dot} The SQL Endpoint for the '{item_name}' lakehouse within the '{workspace}' workspace has not yet been provisioned. Please wait until it has been provisioned."
        )

    start_expr = 'let\n\tdatabase = '
    end_expr = '\nin\n\tdatabase'
    if not direct_lake_over_onelake:
        mid_expr = f'Sql.Database("{sqlEPCS}", "{sqlepid}")'
    else:
        url = prop.get('oneLakeTablesPath').rstrip('/Tables')
        mid_expr = f'AzureStorage.DataLake(\\"{url}\\")"'

    sh = f"{start_expr}{mid_expr}{end_expr}"

    return sh
