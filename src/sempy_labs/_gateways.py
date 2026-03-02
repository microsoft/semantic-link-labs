from sempy._utils._log import log
import pandas as pd
from typing import Optional
from uuid import UUID
import sempy_labs.gateway as g


@log
def list_gateways() -> pd.DataFrame:
    """
    Returns a list of all gateways the user has permission for, including on-premises, on-premises (personal mode), and virtual network gateways.

    This is a wrapper function for the following API: `Gateways - List Gateways <https://learn.microsoft.com/rest/api/fabric/core/gateways/list-gateways>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of all gateways the user has permission for, including on-premises, on-premises (personal mode), and virtual network gateways.
    """

    return g.list_gateways()


@log
def resolve_gateway_id(gateway: str | UUID) -> UUID:

    return g.resolve_gateway_id(gateway)


@log
def delete_gateway(gateway: str | UUID):
    """
    Deletes a gateway.

    This is a wrapper function for the following API: `Gateways - Delete Gateway <https://learn.microsoft.com/rest/api/fabric/core/gateways/delete-gateway>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    gateway : str | uuid.UUID
        The gateway name or ID.
    """

    g.delete_gateway(gateway)


@log
def list_gateway_role_assigments(gateway: str | UUID) -> pd.DataFrame:
    """
    Returns a list of gateway role assignments.

    This is a wrapper function for the following API: `Gateways - List Gateway Role Assignments <https://learn.microsoft.com/rest/api/fabric/core/gateways/list-gateway-role-assignments>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    gateway : str | uuid.UUID
        The gateway name or ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of gateway role assignments.
    """

    return g.list_gateway_role_assigments(gateway)


@log
def delete_gateway_role_assignment(gateway: str | UUID, role_assignment_id: UUID):
    """
    Delete the specified role assignment for the gateway.

    This is a wrapper function for the following API: `Gateways - Delete Gateway Role Assignment <https://learn.microsoft.com/rest/api/fabric/core/gateways/delete-gateway-role-assignment>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    gateway : str | uuid.UUID
        The gateway name or ID.
    role_assignment_id : uuid.UUID
        The role assignment ID.
    """

    g.delete_gateway_role_assignment(gateway, role_assignment_id)


@log
def resolve_gateway_member_id(gateway: str | UUID, gateway_member: str | UUID) -> UUID:

    return g.resolve_gateway_member_id(gateway, gateway_member)


@log
def delete_gateway_member(gateway: str | UUID, gateway_member: str | UUID):
    """
    Delete gateway member of an on-premises gateway.

    This is a wrapper function for the following API: `Gateways - Delete Gateway Member <https://learn.microsoft.com/rest/api/fabric/core/gateways/delete-gateway-member>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    gateway : str | uuid.UUID
        The gateway name or ID.
    gateway_member : str | uuid.UUID
        The gateway member name or ID.
    """

    g.delete_gateway_member(gateway, gateway_member)


@log
def list_gateway_members(gateway: str | UUID) -> pd.DataFrame:
    """
    Lists gateway members of an on-premises gateway.

    This is a wrapper function for the following API: `Gateways - List Gateway Members <https://learn.microsoft.com/rest/api/fabric/core/gateways/list-gateway-members>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    gateway : str | uuid.UUID
        The gateway name or ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of gateway members of an on-premises gateway.
    """

    return g.list_gateway_members(gateway)


@log
def create_vnet_gateway(
    name: str,
    capacity: str | UUID,
    inactivity_minutes_before_sleep: int,
    number_of_member_gateways: int,
    subscription_id: str,
    resource_group: str,
    virtual_network: str,
    subnet: str,
):
    """
    Creates a virtual network gateway.

    This is a wrapper function for the following API: `Gateways - Create Gateway <https://learn.microsoft.com/rest/api/fabric/core/gateways/create-gateway>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str
        The gateway name.
    capacity : str | uuid.UUID
        The capacity name or Id.
    inactivity_minutes_before_sleep : int
        The minutes of inactivity before the virtual network gateway goes into auto-sleep. Must be one of the following values: 30, 60, 90, 120, 150, 240, 360, 480, 720, 1440.
    number_of_member_gateways: int
        The number of member gateways. A number between 1 and 7.
    subscription_id : str
        The subscription ID.
    resource_group : str
        The name of the resource group.
    virtual_network : str
        The name of the virtual network.
    subnet : str
        The name of the subnet.
    """

    g.create_vnet_gateway(
        name,
        capacity,
        inactivity_minutes_before_sleep,
        number_of_member_gateways,
        subscription_id,
        resource_group,
        virtual_network,
        subnet,
    )


@log
def update_on_premises_gateway(
    gateway: str | UUID,
    allow_cloud_connection_refresh: Optional[bool] = None,
    allow_custom_connectors: Optional[bool] = None,
    load_balancing_setting: Optional[str] = None,
):
    """
    Updates an on-premises gateway.

    This is a wrapper function for the following API: `Gateways - Update Gateway <https://learn.microsoft.com/rest/api/fabric/core/gateways/update-gateway>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    gateway : str | uuid.UUID
        The gateway name or ID.
    allow_cloud_connection_refresh : bool, default=None
        Whether to allow cloud connections to refresh through this on-premises gateway. True - Allow, False - Do not allow.
    allow_custom_connectors : bool, default=None
        Whether to allow custom connectors to be used with this on-premises gateway. True - Allow, False - Do not allow.
    load_balancing_setting : str, default=None
        The `load balancing setting <https://learn.microsoft.com/rest/api/fabric/core/gateways/update-gateway?tabs=HTTP#loadbalancingsetting>`_ of the on-premises gateway.
    """

    g.update_on_premises_gateway(
        gateway,
        allow_cloud_connection_refresh,
        allow_custom_connectors,
        load_balancing_setting,
    )


@log
def update_vnet_gateway(
    gateway: str | UUID,
    capacity: str | UUID,
    inactivity_minutes_before_sleep: Optional[int] = None,
    number_of_member_gateways: Optional[int] = None,
):
    """
    Updates a virtual network gateway.

    This is a wrapper function for the following API: `Gateways - Update Gateway <https://learn.microsoft.com/rest/api/fabric/core/gateways/update-gateway>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    gateway : str | uuid.UUID
        The gateway name or ID.
    capacity: str | uuid.UUID
        The capacity name or ID.
    inactivity_minutes_before_sleep : int, default=None
        The minutes of inactivity before the virtual network gateway goes into auto-sleep. Must be one of the following values: 30, 60, 90, 120, 150, 240, 360, 480, 720, 1440.
    number_of_member_gateways : int, default=None
        The number of member gateways. A number between 1 and 7.
    """

    g.update_vnet_gateway(
        gateway,
        capacity,
        inactivity_minutes_before_sleep,
        number_of_member_gateways,
    )


@log
def bind_semantic_model_to_gateway(
    dataset: str | UUID,
    gateway: str | UUID,
    workspace: Optional[str | UUID] = None,
    data_source_object_ids: Optional[list[UUID]] = None,
):
    """
    Binds the specified dataset from the specified workspace to the specified gateway.

    This is a wrapper function for the following API: `Datasets - Bind To Gateway In Group <https://learn.microsoft.com/rest/api/power-bi/datasets/bind-to-gateway-in-group>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataset : str | uuid.UUID
        The name or ID of the semantic model.
    gateway : str | uuid.UUID
        The name or ID of the gateway.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    data_source_object_ids : list[uuid.UUID], default=None
        A list of data source object IDs to bind to the gateway.
    """

    g.bind_semantic_model_to_gateway(
        dataset, gateway, workspace, data_source_object_ids
    )
