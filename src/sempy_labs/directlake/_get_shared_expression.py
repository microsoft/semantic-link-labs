import sempy.fabric as fabric
from sempy_labs._list_functions import list_lakehouses, list_warehouses
import sempy_labs._icons as icons

from typing import Optional

def get_shared_expression(
    item_name: Optional[str] = None, workspace: Optional[str] = None, item_type: Optional[str] = 'lakehouse'
) -> str:

    """
    Dynamically generates the M expression used by a Direct Lake model for a given supported item.

    Parameters
    ----------

    item_name : str, default=None
        The Fabric item used by the Direct Lake semantic model.
        Defaults to None which resolves to the lakehouse attached to the notebook.

    workspace : str, default=None
        The Fabric workspace used by the item.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    item_type : str, default=lakehouse
        The Fabric item type used by the Direct Lake semantic model [lakehouse, warehouse]
        Defaults to lakehouse if no item type specificed.

    Returns
    -------
    str
        Shows the expression which can be used to connect a Direct Lake semantic model to its SQL Endpoint.
    """

    workspace = fabric.resolve_workspace_name(workspace)

    if item_type == 'lakehouse' and item_name is None:
        id = fabric.get_lakehouse_id()
        # Resolve item name: https://learn.microsoft.com/python/api/semantic-link-sempy/sempy.fabric?view=semantic-link-python#sempy-fabric-resolve-item-name
        item_name = fabric.resolve_item_name(id, workspace)

    # Array for supported item types
    supported_items = ['lakehouse', 'warehouse']

    if item_type not in supported_items:
        raise ValueError(
            f"{icons.red_dot} The {item_type} is not a supported item type for a Direct Lake semantic model."
        )    

    def get_details(workspace, item_type, item_name):
        if item_type == 'lakehouse':
            df = list_lakehouses(workspace=workspace)
            name_col = "Lakehouse Name"
            sqlEPCS_col = "SQL Endpoint Connection String"
            sqlepid_col = "SQL Endpoint ID"
            provStatus_col = "SQL Endpoint Provisioning Status"
        elif item_type == 'warehouse':
            df = list_warehouses(workspace=workspace)
            name_col = "Warehouse Name"
            sqlEPCS_col = "Connection Info"
            sqlepid_col = "Warehouse ID"
            provStatus_col = None

        detail = df[df[name_col] == item_name]

        sqlEPCS = detail[sqlEPCS_col].iloc[0]
        sqlepid = detail[sqlepid_col].iloc[0]
        provStatus = detail[provStatus_col].iloc[0] if provStatus_col else None

        return sqlEPCS, sqlepid, provStatus

    # Example usage
    sqlEPCS, sqlepid, provStatus = get_details(workspace, item_type, item_name)

    if provStatus == "InProgress":
        raise ValueError(
            f"{icons.red_dot} The SQL Endpoint for the '{item_name}' '{item_type}' within the '{workspace}' workspace has not yet been provisioned. Please wait until it has been provisioned."
        )

    sh = (
        'let\n\tdatabase = Sql.Database("'
        + sqlEPCS
        + '", "'
        + sqlepid
        + '")\nin\n\tdatabase'
    )

    return sh
