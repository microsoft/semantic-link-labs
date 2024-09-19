import sempy.fabric as fabric
import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    pagination,
)
from sempy.fabric.exceptions import FabricHTTPException
from sempy_labs._api_base import create_item


def create_warehouse(
    warehouse: str, description: Optional[str] = None, workspace: Optional[str] = None
):
    """
    Creates a Fabric warehouse.

    Parameters
    ----------
    warehouse: str
        Name of the warehouse.
    description : str, default=None
        A description of the warehouse.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    create_item(item_name=warehouse, type='Warehouse', description=description, workspace=workspace)


def list_warehouses(workspace: Optional[str] = None) -> pd.DataFrame:
    """
    Shows the warehouses within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the warehouses within a workspace.
    """

    df = pd.DataFrame(
        columns=[
            "Warehouse Name",
            "Warehouse ID",
            "Description",
            "Connection Info",
            "Created Date",
            "Last Updated Time",
        ]
    )

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/warehouses")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            prop = v.get("properties", {})

            new_data = {
                "Warehouse Name": v.get("displayName"),
                "Warehouse ID": v.get("id"),
                "Description": v.get("description"),
                "Connection Info": prop.get("connectionInfo"),
                "Created Date": prop.get("createdDate"),
                "Last Updated Time": prop.get("lastUpdatedTime"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df