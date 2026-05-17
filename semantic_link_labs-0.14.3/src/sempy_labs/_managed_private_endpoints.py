import pandas as pd
from typing import Optional
from uuid import UUID
from sempy._utils._log import log
import sempy_labs.managed_private_endpoint as mpe


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

    mpe.create_managed_private_endpoint(
        name=name,
        target_private_link_resource_id=target_private_link_resource_id,
        target_subresource_type=target_subresource_type,
        request_message=request_message,
        workspace=workspace,
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

    return mpe.list_managed_private_endpoints(workspace=workspace)


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

    mpe.delete_managed_private_endpoint(
        managed_private_endpoint=managed_private_endpoint,
        workspace=workspace,
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

    return mpe.list_managed_private_endpoint_fqdns(
        managed_private_endpoint=managed_private_endpoint,
        workspace=workspace,
    )
