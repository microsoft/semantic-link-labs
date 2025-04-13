from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    resolve_lakehouse_name_and_id,
    resolve_item_name_and_id,
)
from typing import Optional
import sempy_labs._icons as icons
from uuid import UUID


def generate_shared_expression(
    item_name: Optional[str] = None,
    item_type: str = "Lakehouse",
    workspace: Optional[str | UUID] = None,
    use_sql_endpoint: bool = True,
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
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the item.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    use_sql_endpoint : bool, default=True
        Whether to use the SQL Endpoint for the lakehouse/warehouse.
        If False, the expression will be generated without using the SQL Endpoint.

    Returns
    -------
    str
        Shows the expression which can be used to connect a Direct Lake semantic model to its SQL Endpoint.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_types = ["Lakehouse", "Warehouse"]
    item_type = item_type.capitalize()
    if item_type not in item_types:
        raise ValueError(
            f"{icons.red_dot} Invalid item type. Valid options: {item_types}."
        )

    if item_type == "Lakehouse":
        (item_name, item_id) = resolve_lakehouse_name_and_id(
            lakehouse=item_name, workspace=workspace_id
        )
    else:
        (item_name, item_id) = resolve_item_name_and_id(
            item=item_name, type=item_type, workspace=workspace_id
        )

    item_type_rest = f"{item_type.lower()}s"
    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/{item_type_rest}/{item_id}"
    )

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
            f"{icons.red_dot} The SQL Endpoint for the '{item_name}' lakehouse within the '{workspace_name}' workspace has not yet been provisioned. Please wait until it has been provisioned."
        )

    start_expr = "let\n\tdatabase = "
    end_expr = "\nin\n\tdatabase"
    mid_expr = f'Sql.Database("{sqlEPCS}", "{sqlepid}")'

    # Build DL/OL expression
    if not use_sql_endpoint and item_type == "Lakehouse":
        return f'AzureDataLakeStorage{{"server":"onelake.dfs.fabric.microsoft.com","path":"/{workspace_id}/{item_id}/"}}'
    else:
        return f"{start_expr}{mid_expr}{end_expr}"
