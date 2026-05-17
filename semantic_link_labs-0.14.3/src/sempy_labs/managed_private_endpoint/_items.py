import pandas as pd
import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _is_valid_uuid,
    _base_api,
    _print_success,
    _create_dataframe,
    resolve_workspace_id,
)
from uuid import UUID
from sempy._utils._log import log


@log
def create_managed_private_endpoint(
    name: str,
    target_private_link_resource_id: UUID,
    target_subresource_type: str,
    request_message: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a managed private endpoint.

    This is a wrapper function for the following API: `Managed Private Endpoints - Create Workspace Managed Private Endpoint <https://learn.microsoft.com/rest/api/fabric/core/managed-private-endpoints/create-workspace-managed-private-endpoint>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/managedPrivateEndpoints",
        method="post",
        status_codes=[201, 202],
        payload=request_body,
        lro_return_status_code=True,
        client="fabric_sp",
    )
    _print_success(
        item_name=name,
        item_type="managed private endpoint",
        workspace_name=workspace_name,
        action="created",
    )


@log
def list_managed_private_endpoints(
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the managed private endpoints within a workspace.

    This is a wrapper function for the following API: `Managed Private Endpoints - List Workspace Managed Private Endpoints <https://learn.microsoft.com/rest/api/fabric/core/managed-private-endpoints/list-workspace-managed-private-endpoints>`.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    columns = {
        "Managed Private Endpoint Name": "string",
        "Managed Private Endpoint Id": "string",
        "Target Private Link Resource Id": "string",
        "Provisioning State": "string",
        "Connection Status": "string",
        "Connection Description": "string",
        "Target Subresource Type": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/managedPrivateEndpoints",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            conn = v.get("connectionState", {})
            rows.append(
                {
                    "Managed Private Endpoint Name": v.get("name"),
                    "Managed Private Endpoint Id": v.get("id"),
                    "Target Private Link Resource Id": v.get(
                        "targetPrivateLinkResourceId"
                    ),
                    "Provisioning State": v.get("provisioningState"),
                    "Connection Status": conn.get("status"),
                    "Connection Description": conn.get("description"),
                    "Target Subresource Type": v.get("targetSubresourceType"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def delete_managed_private_endpoint(
    managed_private_endpoint: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Deletes a Fabric managed private endpoint.

    This is a wrapper function for the following API: `Managed Private Endpoints - Delete Workspace Managed Private Endpoint <https://learn.microsoft.com/rest/api/fabric/core/managed-private-endpoints/delete-workspace-managed-private-endpoint>`.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    managed_private_endpoint: str | uuid.UUID
        Name or ID of the managed private endpoint.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if _is_valid_uuid(managed_private_endpoint):
        item_id = managed_private_endpoint
    else:
        df = list_managed_private_endpoints(workspace=workspace)
        df_filt = df[df["Managed Private Endpoint Name"] == managed_private_endpoint]

        if df_filt.empty:
            raise ValueError(
                f"{icons.red_dot} The '{managed_private_endpoint}' managed private endpoint does not exist within the '{workspace_name}' workspace."
            )

        item_id = df_filt["Managed Private Endpoint Id"].iloc[0]

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/managedPrivateEndpoints/{item_id}",
        method="delete",
        client="fabric_sp",
    )

    _print_success(
        item_name=managed_private_endpoint,
        item_type="managed private endpoint",
        workspace_name=workspace_name,
        action="deleted",
    )


@log
def list_managed_private_endpoint_fqdns(
    managed_private_endpoint: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a list of fully qualified domain names (FQDNs) associated with the specified managed private endpoint.

    This is a wrapper function for the following API: `Managed Private Endpoints - List FQDNs <https://learn.microsoft.com/rest/api/fabric/core/managed-private-endpoints/list-fqd-ns>`.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    managed_private_endpoint : str | uuid.UUID
        The managed private endpoint name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of fully qualified domain names (FQDNs) associated with the specified managed private endpoint.
    """

    workspace_id = resolve_workspace_id(workspace)
    if _is_valid_uuid(managed_private_endpoint):
        item_id = managed_private_endpoint
    else:
        df = list_managed_private_endpoints(workspace=workspace_id)
        df_filt = df[df["Managed Private Endpoint Name"] == managed_private_endpoint]
        if df_filt.empty:
            raise ValueError(
                f"{icons.red_dot} The '{managed_private_endpoint}' managed private endpoint does not exist within the workspace."
            )
        item_id = df_filt["Managed Private Endpoint Id"].iloc[0]

    columns = {"FQDN": "str"}
    df = _create_dataframe(columns=columns)
    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/managedPrivateEndpoints/{item_id}/targetFQDNs",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "FQDN": v,
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df
