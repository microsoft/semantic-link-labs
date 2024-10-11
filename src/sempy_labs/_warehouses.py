import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    pagination,
    lro,
)
import pandas as pd
from typing import Optional
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException


def create_warehouse(
    warehouse: str,
    description: Optional[str] = None,
    case_insensitive_collation: bool = False,
    workspace: Optional[str] = None,
):
    """
    Creates a Fabric warehouse.

    Parameters
    ----------
    warehouse: str
        Name of the warehouse.
    description : str, default=None
        A description of the warehouse.
    case_insensitive_collation: bool, default=False
        If True, creates the warehouse with case-insensitive collation.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    request_body = {"displayName": warehouse}

    if description:
        request_body["description"] = description
    if case_insensitive_collation:
        request_body.setdefault("creationPayload", {})
        request_body["creationPayload"][
            "defaultCollation"
        ] = "Latin1_General_100_CI_AS_KS_WS_SC_UTF8"

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/warehouses/", json=request_body
    )

    lro(client, response, status_codes=[201, 202])

    print(
        f"{icons.green_dot} The '{warehouse}' warehouse has been created within the '{workspace}' workspace."
    )


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
            "Warehouse Id",
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
                "Warehouse Id": v.get("id"),
                "Description": v.get("description"),
                "Connection Info": prop.get("connectionInfo"),
                "Created Date": prop.get("createdDate"),
                "Last Updated Time": prop.get("lastUpdatedTime"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def delete_warehouse(name: str, workspace: Optional[str] = None):
    """
    Deletes a Fabric warehouse.

    Parameters
    ----------
    name: str
        Name of the warehouse.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(
        item_name=name, type="Warehouse", workspace=workspace
    )

    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/workspaces/{workspace_id}/warehouses/{item_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{name}' warehouse within the '{workspace}' workspace has been deleted."
    )
