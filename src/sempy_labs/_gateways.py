from sempy._utils._log import log
import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    _is_valid_uuid,
    resolve_capacity_id,
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
    _update_dataframe_datatypes,
    _base_api,
    _create_dataframe,
)
from uuid import UUID
import sempy_labs._icons as icons


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

    columns = {
        "Gateway Name": "string",
        "Gateway Id": "string",
        "Type": "string",
        "Public Key Exponent": "string",
        "Public Key Modulus": "string",
        "Version": "string",
        "Number Of Member Gateways": "int",
        "Load Balancing Setting": "string",
        "Allow Cloud Connection Refresh": "bool",
        "Allow Custom Connectors": "bool",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request="/v1/gateways", client="fabric_sp", uses_pagination=True
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Gateway Name": v.get("displayName"),
                    "Gateway Id": v.get("id"),
                    "Type": v.get("type"),
                    "Public Key Exponent": v.get("publicKey", {}).get("exponent"),
                    "Public Key Modulus": v.get("publicKey", {}).get("modulus"),
                    "Version": v.get("version"),
                    "Number Of Member Gateways": v.get("numberOfMemberGateways", 0),
                    "Load Balancing Setting": v.get("loadBalancingSetting"),
                    "Allow Cloud Connection Refresh": v.get(
                        "allowCloudConnectionRefresh"
                    ),
                    "Allow Custom Connectors": v.get("allowCustomConnectors"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def _resolve_gateway_id(gateway: str | UUID) -> UUID:

    if _is_valid_uuid(gateway):
        return gateway
    else:
        dfG = list_gateways()
        dfG_filt = dfG[dfG["Gateway Name"] == gateway]

        if dfG_filt.empty:
            raise ValueError(f"{icons.red_dot} The '{gateway}' gateway does not exist.")

        return dfG_filt["Gateway Id"].iloc[0]


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

    gateway_id = _resolve_gateway_id(gateway)
    _base_api(request=f"/v1/gateways/{gateway_id}", client="fabric_sp", method="delete")
    print(f"{icons.green_dot} The '{gateway}' gateway has been deleted.")


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

    columns = {
        "Gateway Role Assignment Id": "string",
        "Principal Id": "string",
        "Principal Type": "string",
        "Role": "string",
    }
    df = _create_dataframe(columns=columns)
    gateway_id = _resolve_gateway_id(gateway)
    responses = _base_api(
        request=f"/v1/gateways/{gateway_id}/roleAssignments",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Gateway Role Assignment Id": v.get("id"),
                    "Principal Id": v.get("principal", {}).get("id"),
                    "Principal Type": v.get("principal", {}).get("type"),
                    "Role": v.get("role"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


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

    gateway_id = _resolve_gateway_id(gateway)
    _base_api(
        request=f"/v1/gateways/{gateway_id}/roleAssignments/{role_assignment_id}",
        client="fabric_sp",
        method="delete",
    )

    print(
        f"{icons.green_dot} The '{role_assignment_id}' role assignment for the '{gateway}' gateway has been deleted."
    )


@log
def _resolve_gateway_member_id(gateway: str | UUID, gateway_member: str | UUID) -> UUID:

    gateway_id = _resolve_gateway_id(gateway)
    dfM = list_gateway_members(gateway=gateway_id)

    if _is_valid_uuid(gateway_member):
        dfM_filt = dfM[dfM["Member Id"] == gateway_member]
    else:
        dfM_filt = dfM[dfM["Member Name"] == gateway_member]
    if len(dfM_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{gateway_member}' gateway member does not exist within the '{gateway}' gateway."
        )

    return dfM_filt["Member Id"].iloc[0]


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

    gateway_id = _resolve_gateway_id(gateway)
    member_id = _resolve_gateway_member_id(
        gateway=gateway_id, gateway_member=gateway_member
    )

    _base_api(
        request=f"/v1/gateways/{gateway_id}/members/{member_id}",
        client="fabric_sp",
        method="delete",
    )
    print(
        f"{icons.green_dot} The '{member_id}' member for the '{gateway}' gateway has been deleted."
    )


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

    gateway_id = _resolve_gateway_id(gateway)

    columns = {
        "Member Id": "string",
        "Member Name": "string",
        "Public Key Exponent": "string",
        "Public Key Modulus": "string",
        "Version": "string",
        "Enabled": "bool",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(
        request=f"/v1/gateways/{gateway_id}/members", client="fabric_sp"
    )

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
                "Member Id": v.get("id"),
                "Member Name": v.get("displayName"),
                "Public Key Exponent": v.get("publicKey", {}).get("exponent"),
                "Public Key Modulus": v.get("publicKey", {}).get("modulus"),
                "Version": v.get("version"),
                "Enabled": v.get("enabled"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


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

    capacity_id = resolve_capacity_id(capacity=capacity)
    payload = {
        "type": "VirtualNetwork",
        "displayName": name,
        "capacityId": capacity_id,
        "virtualNetworkAzureResource": {
            "subscriptionId": subscription_id,
            "resourceGroupName": resource_group,
            "virtualNetworkName": virtual_network,
            "subnetName": subnet,
        },
        "inactivityMinutesBeforeSleep": inactivity_minutes_before_sleep,
        "numberOfMemberGateways": number_of_member_gateways,
    }

    _base_api(
        request="/v1/gateways",
        client="fabric_sp",
        method="post",
        payload=payload,
        status_codes=201,
    )

    print(
        f"{icons.green_dot} The '{name}' gateway was created within the '{capacity}' capacity."
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

    gateway_id = _resolve_gateway_id(gateway)

    payload = {}

    if allow_cloud_connection_refresh is not None:
        payload["allowCloudConnectionRefresh"] = allow_cloud_connection_refresh
    if allow_custom_connectors is not None:
        payload["allowCustomConnectors"] = allow_custom_connectors
    if load_balancing_setting is not None:
        payload["loadBalancingSetting"] = load_balancing_setting

    if not payload:
        raise ValueError(
            f"{icons.yellow_dot} The '{gateway}' gateway has not been update as no valid settings were provided."
        )

    payload["type"] = "OnPremises"

    _base_api(
        request=f"/v1/gateways/{gateway_id}",
        client="fabric_sp",
        method="patch",
        payload=payload,
    )

    print(f"{icons.green_dot} The '{gateway}' has been updated accordingly.")


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

    gateway_id = _resolve_gateway_id(gateway)

    payload = {}

    if capacity is not None:
        capacity_id = resolve_capacity_id(capacity=capacity)
        payload["capacityId"] = capacity_id
    if inactivity_minutes_before_sleep is not None:
        payload["inactivityMinutesBeforeSleep"] = inactivity_minutes_before_sleep
    if number_of_member_gateways is not None:
        payload["numberOfMemberGateways"] = number_of_member_gateways

    if not payload:
        raise ValueError(
            f"{icons.yellow_dot} The '{gateway}' gateway has not been update as no valid settings were provided."
        )

    payload["type"] = "VirtualNetwork"

    _base_api(
        request=f"/v1/gateways/{gateway_id}",
        client="fabric_sp",
        method="patch",
        payload=payload,
    )
    print(f"{icons.green_dot} The '{gateway}' has been updated accordingly.")


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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(
        dataset, workspace=workspace_id
    )

    gateway_id = _resolve_gateway_id(gateway)
    payload = {
        "gatewayObjectId": gateway_id,
    }
    if data_source_object_ids is not None:
        payload["datasourceObjectIds"] = data_source_object_ids

    _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/Default.BindToGateway",
        client="fabric_sp",
        method="post",
        payload=payload,
    )
    print(
        f"{icons.green_dot} The '{dataset_name}' semantic model within the '{workspace_name}' workspace has been binded to the '{gateway_id}' gateway."
    )
