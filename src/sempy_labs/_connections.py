import sempy.fabric as fabric
import pandas as pd
from sempy.fabric.exceptions import FabricHTTPException
from typing import Optional
from sempy_labs._helper_functions import pagination


def list_connections() -> pd.DataFrame:
    """
    Lists all available connections.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing all available connections.
    """

    client = fabric.FabricRestClient()
    response = client.get("/v1/connections")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(
        columns=[
            "Connection Id",
            "Connection Name",
            "Gateway Id",
            "Connectivity Type",
            "Connection Path",
            "Connection Type",
            "Privacy Level",
            "Credential Type",
            "Single Sign on Type",
            "Connection Encyrption",
            "Skip Test Connection",
        ]
    )

    for i in response.json().get("value", []):
        connection_details = i.get("connectionDetails", {})
        credential_details = i.get("credentialDetails", {})

        new_data = {
            "Connection Id": i.get("id"),
            "Connection Name": i.get("displayName"),
            "Gateway Id": i.get("gatewayId"),
            "Connectivity Type": i.get("connectivityType"),
            "Connection Path": connection_details.get("path"),
            "Connection Type": connection_details.get("type"),
            "Privacy Level": i.get("privacyLevel"),
            "Credential Type": (
                credential_details.get("credentialType") if credential_details else None
            ),
            "Single Sign On Type": (
                credential_details.get("singleSignOnType")
                if credential_details
                else None
            ),
            "Connection Encryption": (
                credential_details.get("connectionEncryption")
                if credential_details
                else None
            ),
            "Skip Test Connection": (
                credential_details.get("skipTestConnection")
                if credential_details
                else None
            ),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    bool_cols = ["Skip Test Connection"]
    df[bool_cols] = df[bool_cols].astype(bool)

    return df


def list_item_connections(
    item_name: str, item_type: str, workspace: Optional[str] = None
) -> pd.DataFrame:
    """
    Shows the list of connections that the specified item is connected to.

    Parameters
    ----------
    item_name : str
        The item name.
    item_type : str
        The `item type <https://learn.microsoft.com/rest/api/fabric/core/items/update-item?tabs=HTTP#itemtype>`_.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the list of connections that the specified item is connected to.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-item-connections?tabs=HTTP

    workspace = fabric.resolve_workspace_name(workspace)
    workspace_id = fabric.resolve_workspace_id(workspace)
    item_type = item_type[0].upper() + item_type[1:]
    item_id = fabric.resolve_item_id(
        item_name=item_name, type=item_type, workspace=workspace
    )

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/items/{item_id}/connections")

    df = pd.DataFrame(
        columns=[
            "Connection Name",
            "Connection Id",
            "Connectivity Type",
            "Connection Type",
            "Connection Path",
            "Gateway Id",
        ]
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Connection Name": v.get("displayName"),
                "Connection Id": v.get("id"),
                "Connectivity Type": v.get("connectivityType"),
                "Connection Type": v.get("connectionDetails", {}).get("type"),
                "Connection Path": v.get("connectionDetails", {}).get("path"),
                "Gateway Id": v.get("gatewayId"),
            }

            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def create_connection_cloud(
    name: str,
    server_name: str,
    database_name: str,
    user_name: str,
    password: str,
    privacy_level: str,
) -> pd.DataFrame:

    # https://review.learn.microsoft.com/en-us/rest/api/fabric/core/connections/create-connection?branch=features%2Fdmts&tabs=HTTP

    df = pd.DataFrame(
        columns=[
            "Connection ID",
            "Connection Name",
            "Connectivity Type",
            "Connection Type",
            "Connection Path",
            "Privacy Level",
            "Credential Type",
            "Single Sign On Type",
            "Connection Encryption",
            "Skip Test Connection",
        ]
    )

    client = fabric.FabricRestClient()

    request_body = {
        "connectivityType": "ShareableCloud",
        "name": name,
        "connectionDetails": {
            "type": "SQL",
            "parameters": [
                {"name": "server", "value": server_name},
                {"name": "database", "value": database_name},
            ],
        },
        "privacyLevel": privacy_level,
        "credentialDetails": {
            "singleSignOnType": "None",
            "connectionEncryption": "NotEncrypted",
            "skipTestConnection": False,
            "credentials": {
                "credentialType": "Basic",
                "username": user_name,
                "password": password,
            },
        },
    }

    response = client.post("/v1/connections", json=request_body)

    if response.status_code != 200:
        raise FabricHTTPException(response)
    o = response.json()
    new_data = {
        "Connection Id": o.get("id"),
        "Connection Name": o.get("name"),
        "Connectivity Type": o.get("connectivityType"),
        "Connection Type": o.get("connectionDetails", {}).get("type"),
        "Connection Path": o.get("connectionDetails", {}).get("path"),
        "Privacy Level": o.get("privacyLevel"),
        "Credential Type": o.get("credentialDetails", {}).get("credentialType"),
        "Single Sign On Type": o.get("credentialDetails", {}).get("singleSignOnType"),
        "Connection Encryption": o.get("credentialDetails", {}).get(
            "connectionEncryption"
        ),
        "Skip Test Connection": o.get("credentialDetails", {}).get(
            "skipTestConnection"
        ),
    }
    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df["Skip Test Connection"] = df["Skip Test Connection"].astype(bool)

    return df


def create_connection_on_prem(
    name: str,
    gateway_id: str,
    server_name: str,
    database_name: str,
    credentials: str,
    privacy_level: str,
) -> pd.DataFrame:

    df = pd.DataFrame(
        columns=[
            "Connection ID",
            "Connection Name",
            "Gateway ID",
            "Connectivity Type",
            "Connection Type",
            "Connection Path",
            "Privacy Level",
            "Credential Type",
            "Single Sign On Type",
            "Connection Encryption",
            "Skip Test Connection",
        ]
    )

    client = fabric.FabricRestClient()

    request_body = {
        "connectivityType": "OnPremisesDataGateway",
        "gatewayId": gateway_id,
        "name": name,
        "connectionDetails": {
            "type": "SQL",
            "parameters": [
                {"name": "server", "value": server_name},
                {"name": "database", "value": database_name},
            ],
        },
        "privacyLevel": privacy_level,
        "credentialDetails": {
            "singleSignOnType": "None",
            "connectionEncryption": "NotEncrypted",
            "skipTestConnection": False,
            "credentials": {
                "credentialType": "Windows",
                "values": [{"gatewayId": gateway_id, "credentials": credentials}],
            },
        },
    }

    response = client.post("/v1/connections", json=request_body)

    if response.status_code != 200:
        raise FabricHTTPException(response)
    o = response.json()
    new_data = {
        "Connection Id": o.get("id"),
        "Connection Name": o.get("name"),
        "Gateway ID": o.get("gatewayId"),
        "Connectivity Type": o.get("connectivityType"),
        "Connection Type": o.get("connectionDetails", {}).get("type"),
        "Connection Path": o.get("connectionDetails", {}).get("path"),
        "Privacy Level": o.get("privacyLevel"),
        "Credential Type": o.get("credentialDetails", {}).get("credentialType"),
        "Single Sign On Type": o.get("credentialDetails", {}).get("singleSignOnType"),
        "Connection Encryption": o.get("credentialDetails", {}).get(
            "connectionEncryption"
        ),
        "Skip Test Connection": o.get("credentialDetails", {}).get(
            "skipTestConnection"
        ),
    }
    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df["Skip Test Connection"] = df["Skip Test Connection"].astype(bool)

    return df


def create_connection_vnet(
    name: str,
    gateway_id: str,
    server_name: str,
    database_name: str,
    user_name: str,
    password: str,
    privacy_level: str,
) -> pd.DataFrame:

    df = pd.DataFrame(
        columns=[
            "Connection ID",
            "Connection Name",
            "Gateway ID",
            "Connectivity Type",
            "Connection Type",
            "Connection Path",
            "Privacy Level",
            "Credential Type",
            "Single Sign On Type",
            "Connection Encryption",
            "Skip Test Connection",
        ]
    )

    client = fabric.FabricRestClient()

    request_body = {
        "connectivityType": "VirtualNetworkDataGateway",
        "gatewayId": gateway_id,
        "name": name,
        "connectionDetails": {
            "type": "SQL",
            "parameters": [
                {"name": "server", "value": server_name},
                {"name": "database", "value": database_name},
            ],
        },
        "privacyLevel": privacy_level,
        "credentialDetails": {
            "singleSignOnType": "None",
            "connectionEncryption": "Encrypted",
            "skipTestConnection": False,
            "credentials": {
                "credentialType": "Basic",
                "username": user_name,
                "password": password,
            },
        },
    }

    response = client.post("/v1/connections", json=request_body)

    if response.status_code != 200:
        raise FabricHTTPException(response)
    o = response.json()
    new_data = {
        "Connection Id": o.get("id"),
        "Connection Name": o.get("name"),
        "Gateway ID": o.get("gatewayId"),
        "Connectivity Type": o.get("connectivityType"),
        "Connection Type": o.get("connectionDetails", {}).get("type"),
        "Connection Path": o.get("connectionDetails", {}).get("path"),
        "Privacy Level": o.get("privacyLevel"),
        "Credential Type": o.get("credentialDetails", {}).get("credentialType"),
        "Single Sign On Type": o.get("credentialDetails", {}).get("singleSignOnType"),
        "Connection Encryption": o.get("credentialDetails", {}).get(
            "connectionEncryption"
        ),
        "Skip Test Connection": o.get("credentialDetails", {}).get(
            "skipTestConnection"
        ),
    }
    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df["Skip Test Connection"] = df["Skip Test Connection"].astype(bool)

    return df
