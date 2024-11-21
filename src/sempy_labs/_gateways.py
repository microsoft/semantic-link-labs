import sempy.fabric as fabric
import pandas as pd
from typing import Optional
from sempy.fabric.exceptions import FabricHTTPException
from sempy_labs._helper_functions import (
    pagination,
    _is_valid_uuid,
    resolve_capacity_id,
)
from uuid import UUID
import sempy_labs._icons as icons


def list_gateways() -> pd.DataFrame:

    client = fabric.FabricRestClient()
    response = client.get("/v1/gateways")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    df = pd.DataFrame(
        columns=[
            "Gateway Name",
            "Gateway Id",
            "Type",
            "Public Key Exponent",
            "Public Key Modulus",
            "Version",
            "Number Of Member Gateways",
            "Load Balancing Setting",
            "Allow Cloud Connection Refresh",
            "Allow Custom Connectors",
        ]
    )

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Gateway Name": v.get("displayName"),
                "Gateway Id": v.get("id"),
                "Type": v.get("type"),
                "Public Key Exponent": v.get("publicKey", {}).get("exponent"),
                "Public Key Modulus": v.get("publicKey", {}).get("modulus"),
                "Version": v.get("version"),
                "Number Of Member Gateways": v.get("numberOfMemberGateways", 0),
                "Load Balancing Setting": v.get("loadBalancingSetting"),
                "Allow Cloud Connection Refresh": v.get("allowCloudConnectionRefresh"),
                "Allow Custom Connectors": v.get("allowCustomConnectors"),
            }

            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    int_cols = ["Number Of Member Gateways"]
    bool_cols = ["Allow Cloud Connection Refresh", "Allow Custom Connectors"]
    df[bool_cols] = df[bool_cols].astype(bool)
    df[int_cols] = df[int_cols].astype(int)

    return df


def resolve_gateway_id(gateway: str | UUID) -> UUID:

    dfG = list_gateways()
    if _is_valid_uuid(gateway):
        dfG_filt = dfG[dfG["Gateway Id"] == gateway]
    else:
        dfG_filt = dfG[dfG["Gateway Name"] == gateway]

    if len(dfG_filt) == 0:
        raise ValueError(f"{icons.red_dot} The '{gateway}' does not exist.")

    return dfG_filt["Gateway Id"].iloc[0]


def delete_gateway(gateway: str | UUID):

    gateway_id = resolve_gateway_id(gateway)
    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/gateways/{gateway_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{gateway}' gateway has been deleted.")


def list_gateway_role_assigments(gateway: str | UUID) -> pd.DataFrame:

    gateway_id = resolve_gateway_id(gateway)
    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/gateways/{gateway_id}/roleAssignments")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(columns=[])

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Gateway Role Assignment Id": v.get("id"),
                "Principal Id": v.get("principal", {}).get("id"),
                "Principal Type": v.get("principal", {}).get("type"),
                "Role": v.get("role"),
            }

            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def delete_gateway_role_assignment(gateway: str | UUID, role_assignement_id: UUID):

    gateway_id = resolve_gateway_id(gateway)
    client = fabric.FabricRestClient()
    response = client.delete(
        f"/v1/gateways/{gateway_id}/roleAssignments/{role_assignement_id}"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{role_assignement_id}' role assignment for the '{gateway}' gateway has been deleted."
    )


def delete_gateway_member(gateway: str | UUID, gateway_member: UUID):

    gateway_id = resolve_gateway_id(gateway)
    dfM = list_gateway_members(gateway=gateway_id)

    if _is_valid_uuid(gateway_member):
        dfM_filt = dfM[dfM["Member Id"] == gateway_member]
    else:
        dfM_filt = dfM[dfM["Member Name"] == gateway_member]
    if len(dfM_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{gateway_member}' gateway member does not exist within the '{gateway}' gateway."
        )
    member_id = dfM_filt["Member Id"].iloc[0]

    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/gateways/{gateway_id}/members/{member_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{member_id}' member for the '{gateway}' gateway has been deleted."
    )


def list_gateway_members(gateway: str | UUID) -> pd.DataFrame:

    gateway_id = resolve_gateway_id(gateway)
    client = fabric.FabricRestClient()
    response = client.get(f"/v1/gateways/{gateway_id}/members")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(
        columns=[
            "Member Id",
            "Member Name",
            "Public Key Exponent",
            "Public Key Modulus",
            "Version",
            "Enabled",
        ]
    )

    for v in response.json().get("value", []):
        new_data = {
            "Member Id": v.get("id"),
            "Member Name": v.get("displayName"),
            "Public Key Exponent": v.get("publicKey", {}).get("exponent"),
            "Public Key Modulus": v.get("publicKey", {}).get("modulus"),
            "Version": v.get("version"),
            "Enabled": v.get("enabled"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    bool_cols = ["Enabled"]
    df[bool_cols] = df[bool_cols].astype(bool)


def create_vnet_gateway(
    name: str,
    capacity: str | UUID,
    inactivity_minutes_before_sleep: int,
    number_of_member_gateways: int,
    subscription_id: UUID,
    resource_group: str,
    virtual_network: str,
    subnet: str,
):

    client = fabric.FabricRestClient()

    capacity_id = resolve_capacity_id(capacity)
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
    response = client.post("/v1/gateways", json=payload)

    if response.status_code != 201:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{name}' gateway was created within the '{capacity}' capacity."
    )


# def add_gateway_role_assignment(gateway : str | UUID)\:


def update_on_premises_gateway(
    gateway: str,
    allow_cloud_connection_refresh: Optional[bool] = None,
    allow_custom_connectors: Optional[bool] = None,
    load_balancing_setting: Optional[str] = None,
):

    gateway_id = resolve_gateway_id(gateway)

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

    client = fabric.FabricRestClient()
    response = client.patch(f"/v1/gateways/{gateway_id}", json=payload)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{gateway}' has been updated accordingly.")


def update_vnet_gateway(
    gateway: str,
    capacity: str | UUID,
    inactivity_minutes_before_sleep: Optional[int] = None,
    number_of_member_gateways: Optional[int] = None,
):

    gateway_id = resolve_gateway_id(gateway)

    payload = {}

    if capacity is not None:
        capacity_id = resolve_capacity_id(capacity)
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

    client = fabric.FabricRestClient()
    response = client.patch(f"/v1/gateways/{gateway_id}", json=payload)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{gateway}' has been updated accordingly.")
