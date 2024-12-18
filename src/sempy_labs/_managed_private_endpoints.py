import sempy.fabric as fabric
import pandas as pd
import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    lro,
    pagination,
)
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID


def create_managed_private_endpoint(
    name: str,
    target_private_link_resource_id: UUID,
    target_subresource_type: str,
    request_message: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a managed private endpoint.

    This is a wrapper function for the following API: `Managed Private Endpoints - Create Workspace Managed Private Endpoint <https://learn.microsoft.com/rest/api/fabric/core/managed-private-endpoints/create-workspace-managed-private-endpoint>`.

    Parameters
    ----------
    name: str
        Name of the managed private endpoint.
    target_private_link_resource_id: uuid.UUID
        Resource Id of data source for which private endpoint needs to be created.
    target_subresource_type : str
        Sub-resource pointing to Private-link resoure.
    request_message : str, default=None
        Message to approve private endpoint request. Should not be more than 140 characters.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    request_body = {
        "name": name,
        "targetPrivateLinkResourceId": target_private_link_resource_id,
        "targetSubresourceType": target_subresource_type,
    }

    if request_message is not None:
        if len(request_message) > 140:
            raise ValueError(
                f"{icons.red_dot} The request message cannot be more than 140 characters."
            )
        request_body["requestMessage"] = request_message

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/managedPrivateEndpoints", json=request_body
    )

    lro(client, response, status_codes=[201, 202])

    print(
        f"{icons.green_dot} The '{name}' managed private endpoint has been created within the '{workspace_name}' workspace."
    )


def list_managed_private_endpoints(
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the managed private endpoints within a workspace.

    This is a wrapper function for the following API: `Managed Private Endpoints - List Workspace Managed Private Endpoints <https://learn.microsoft.com/rest/api/fabric/core/managed-private-endpoints/list-workspace-managed-private-endpoints>`.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the managed private endpoints within a workspace.
    """

    df = pd.DataFrame(
        columns=[
            "Managed Private Endpoint Name",
            "Managed Private Endpoint Id",
            "Target Private Link Resource Id",
            "Provisioning State",
            "Connection Status",
            "Connection Description",
            "Target Subresource Type",
        ]
    )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/managedPrivateEndpoints")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            conn = v.get("connectionState", {})
            new_data = {
                "Managed Private Endpoint Name": v.get("name"),
                "Managed Private Endpoint Id": v.get("id"),
                "Target Private Link Resource Id": v.get("targetPrivateLinkResourceId"),
                "Provisioning State": v.get("provisioningState"),
                "Connection Status": conn.get("status"),
                "Connection Description": conn.get("description"),
                "Target Subresource Type": v.get("targetSubresourceType"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def delete_managed_private_endpoint(
    managed_private_endpoint: str, workspace: Optional[str | UUID] = None
):
    """
    Deletes a Fabric managed private endpoint.

    This is a wrapper function for the following API: `Managed Private Endpoints - Delete Workspace Managed Private Endpoint <https://learn.microsoft.com/rest/api/fabric/core/managed-private-endpoints/delete-workspace-managed-private-endpoint>`.

    Parameters
    ----------
    managed_private_endpoint: str
        Name of the managed private endpoint.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = list_managed_private_endpoints(workspace=workspace_id)
    df_filt = df[df["Managed Private Endpoint Name"] == managed_private_endpoint]

    if len(df_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{managed_private_endpoint}' managed private endpoint does not exist within the '{workspace_name}' workspace."
        )

    item_id = df_filt["Managed Private Endpoint Id"].iloc[0]

    client = fabric.FabricRestClient()
    response = client.delete(
        f"/v1/workspaces/{workspace_id}/managedPrivateEndpoints/{item_id}"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{managed_private_endpoint}' managed private endpoint within the '{workspace_name}' workspace has been deleted."
    )
