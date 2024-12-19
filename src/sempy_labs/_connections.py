import sempy.fabric as fabric
import pandas as pd
from sempy.fabric.exceptions import FabricHTTPException
from typing import Optional
from sempy_labs._helper_functions import (
    pagination,
    _is_valid_uuid,
    resolve_workspace_name_and_id,
)
from uuid import UUID
import sempy_labs._icons as icons
from sempy_labs._gateways import _resolve_gateway_id


def delete_connection(connection: str | UUID):
    """
    Delete a connection.

    This is a wrapper function for the following API: `Connections - Delete Connection <https://learn.microsoft.com/rest/api/fabric/core/connections/delete-connection>`_.

    Parameters
    ----------
    connection : str | uuid.UUID
        The connection name or ID.
    """

    connection_id = _resolve_connection_id(connection)

    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/connections/{connection_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{connection}' connection has been deleted.")


def delete_connection_role_assignment(connection: str | UUID, role_assignment_id: UUID):
    """
    Delete the specified role assignment for the connection.

    This is a wrapper function for the following API: `Connections - Delete Connection Role Assignment <https://learn.microsoft.com/rest/api/fabric/core/connections/delete-connection-role-assignment>`_.

    Parameters
    ----------
    connection : str | uuid.UUID
        The connection name or ID.
    role_assignment_id : uuid.UUID
        The role assignment ID.
    """

    connection_id = _resolve_connection_id(connection)

    client = fabric.FabricRestClient()
    response = client.delete(
        f"/v1/connections/{connection_id}/roleAssignments/{role_assignment_id}"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{role_assignment_id}' role assignment Id has been deleted from the '{connection}' connection."
    )


def _resolve_connection_id(connection: str | UUID) -> UUID:

    dfC = list_connections()
    if _is_valid_uuid(connection):
        dfC_filt = dfC[dfC["Connection Id"] == connection]
    else:
        dfC_filt = dfC[dfC["Connection Name"] == connection]

    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{connection}' is not a valid connection."
        )

    return dfC_filt["Connection Id"].iloc[0]


def list_connection_role_assignments(connection: str | UUID) -> pd.DataFrame:
    """
    Returns a list of connection role assignments.

    This is a wrapper function for the following API: `Connections - List Connection Role Assignments <https://learn.microsoft.com/rest/api/fabric/core/connections/list-connection-role-assignments>`_.

    Parameters
    ----------
    connection : str | uuid.UUID
        The connection name or ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of connection role assignments.
    """

    connection_id = _resolve_connection_id(connection)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/connections/{connection_id}/roleAssignments")

    df = pd.DataFrame(
        columns=[
            "Connection Role Assignment Id",
            "Principal Id",
            "Principal Type",
            "Role",
        ]
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Connection Role Assignment Id": v.get("id"),
                "Principal Id": v.get("principal", {}).get("id"),
                "Principal Type": v.get("principal", {}).get("type"),
                "Role": v.get("role"),
            }

            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


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

    responses = pagination(client, response)

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
    for r in responses:
        for i in r.get("value", []):
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
                    credential_details.get("credentialType")
                    if credential_details
                    else None
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
    item_name: str, item_type: str, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows the list of connections that the specified item is connected to.

    This is a wrapper function for the following API: `Items - List Item Connections <https://learn.microsoft.com/rest/api/fabric/core/items/list-item-connections>`_.

    Parameters
    ----------
    item_name : str
        The item name.
    item_type : str
        The `item type <https://learn.microsoft.com/rest/api/fabric/core/items/update-item?tabs=HTTP#itemtype>`_.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the list of connections that the specified item is connected to.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_type = item_type[0].upper() + item_type[1:]
    item_id = fabric.resolve_item_id(
        item_name=item_name, type=item_type, workspace=workspace_id
    )

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/items/{item_id}/connections")

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


def _list_supported_connection_types(
    gateway: Optional[str | UUID] = None, show_all_creation_methods: bool = False
) -> pd.DataFrame:

    url = f"/v1/connections/supportedConnectionTypes?showAllCreationMethods={show_all_creation_methods}&"
    if gateway is not None:
        gateway_id = _resolve_gateway_id(gateway)
        url += f"gatewayId={gateway_id}"

    df = pd.DataFrame(
        columns=[
            "Connection Type",
            "Creation Method",
            "Supported Credential Types",
            "Supported Connection Encryption Types",
            "Supports Skip Test Connection",
        ]
    )

    url = url.rstrip("&")
    client = fabric.FabricRestClient()
    response = client.get(url)
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    records = []
    for r in responses:
        for v in r.get("value", []):
            records.append(
                {
                    "Connection Type": v.get("type"),
                    "Creation Method": v["creationMethods"][0]["name"],
                    "Supported Credential Types": v.get("supportedCredentialTypes"),
                    "Supported Connection Encryption Types": v.get(
                        "supportedConnectionEncryptionTypes"
                    ),
                    "Supports Skip Test Connection": v.get(
                        "supportsSkipTestConnection"
                    ),
                }
            )

    if records:
        df = pd.DataFrame(records)

    return df


def create_cloud_connection(
    name: str,
    server_name: str,
    database_name: str,
    user_name: str,
    password: str,
    privacy_level: str,
    connection_encryption: str = "NotEncrypted",
    skip_test_connection: bool = False,
):
    """
    Creates a shared cloud connection.

    This is a wrapper function for the following API: `Connections - Create Connection <https://learn.microsoft.com/rest/api/fabric/core/connections/create-connection>`_.

    Parameters
    ----------
    name : str
        The name of the connection.
    server_name : str
        The name of the server.
    database_name : str
        The name of the database.
    user_name : str
        The username.
    password : str
        The password.
    privacy_level : str
        The `privacy level <https://learn.microsoft.com/rest/api/fabric/core/connections/create-connection?tabs=HTTP#privacylevel>`_ of the connection.
    connection_encryption : str, default="NotEncrypted"
        The connection encrpytion.
    skip_test_connection: bool, default=False
        If True, skips the test connection.
    """

    request_body = {
        "connectivityType": "ShareableCloud",
        "displayName": name,
        "connectionDetails": {
            "type": "SQL",
            "creationMethod": "SQL",
            "parameters": [
                {
                    "dataType": "Text",
                    "name": "server",
                    "value": server_name,
                },
                {
                    "dataType": "Text",
                    "name": "database",
                    "value": database_name,
                },
            ],
        },
        "privacyLevel": privacy_level,
        "credentialDetails": {
            "singleSignOnType": "None",
            "connectionEncryption": connection_encryption,
            "skipTestConnection": skip_test_connection,
            "credentials": {
                "credentialType": "Basic",
                "username": user_name,
                "password": password,
            },
        },
    }

    client = fabric.FabricRestClient()
    response = client.post("/v1/connections", json=request_body)

    if response.status_code != 201:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{name}' cloud connection has been created.")


def create_on_prem_connection(
    name: str,
    gateway: str | UUID,
    server_name: str,
    database_name: str,
    credentials: str,
    privacy_level: str,
    connection_encryption: str = "NotEncrypted",
    skip_test_connection: bool = False,
):
    """
    Creates an on-premises connection.

    This is a wrapper function for the following API: `Connections - Create Connection <https://learn.microsoft.com/rest/api/fabric/core/connections/create-connection>`_.

    Parameters
    ----------
    name : str
        The name of the connection.
    gateway : str | uuid.UUID
        The name or Id of the gateway.
    server_name : str
        The name of the server.
    database_name : str
        The name of the database.
    user_name : str
        The username.
    password : str
        The password.
    privacy_level : str
        The `privacy level <https://learn.microsoft.com/rest/api/fabric/core/connections/create-connection?tabs=HTTP#privacylevel>`_ of the connection.
    connection_encryption : str, default="NotEncrypted"
        The connection encrpytion.
    skip_test_connection: bool, default=False
        If True, skips the test connection.
    """

    gateway_id = _resolve_gateway_id(gateway)

    request_body = {
        "connectivityType": "OnPremisesGateway",
        "gatewayId": gateway_id,
        "displayName": name,
        "connectionDetails": {
            "type": "SQL",
            "creationMethod": "SQL",
            "parameters": [
                {
                    "dataType": "Text",
                    "name": "server",
                    "value": server_name,
                },
                {
                    "dataType": "Text",
                    "name": "database",
                    "value": database_name,
                },
            ],
        },
        "privacyLevel": privacy_level,
        "credentialDetails": {
            "singleSignOnType": "None",
            "connectionEncryption": connection_encryption,
            "skipTestConnection": skip_test_connection,
            "credentials": {
                "credentialType": "Windows",
                "values": [{"gatewayId": gateway_id, "credentials": credentials}],
            },
        },
    }

    client = fabric.FabricRestClient()
    response = client.post("/v1/connections", json=request_body)

    if response.status_code != 201:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{name}' on-prem connection has been created.")


def create_vnet_connection(
    name: str,
    gateway: str | UUID,
    server_name: str,
    database_name: str,
    user_name: str,
    password: str,
    privacy_level: str,
    connection_encryption: str = "NotEncrypted",
    skip_test_connection: bool = False,
):
    """
    Creates a virtual network gateway connection.

    This is a wrapper function for the following API: `Connections - Create Connection <https://learn.microsoft.com/rest/api/fabric/core/connections/create-connection>`_.

    Parameters
    ----------
    name : str
        The name of the connection.
    gateway : str | uuid.UUID
        The name or Id of the gateway.
    server_name : str
        The name of the server.
    database_name : str
        The name of the database.
    user_name : str
        The username.
    password : str
        The password.
    privacy_level : str
        The `privacy level <https://learn.microsoft.com/rest/api/fabric/core/connections/create-connection?tabs=HTTP#privacylevel>`_ of the connection.
    connection_encryption : str, default="NotEncrypted"
        The connection encrpytion.
    skip_test_connection: bool, default=False
        If True, skips the test connection.
    """

    gateway_id = _resolve_gateway_id(gateway)

    request_body = {
        "connectivityType": "VirtualNetworkGateway",
        "gatewayId": gateway_id,
        "displayName": name,
        "connectionDetails": {
            "type": "SQL",
            "creationMethod": "SQL",
            "parameters": [
                {
                    "dataType": "Text",
                    "name": "server",
                    "value": server_name,
                },
                {
                    "dataType": "Text",
                    "name": "database",
                    "value": database_name,
                },
            ],
        },
        "privacyLevel": privacy_level,
        "credentialDetails": {
            "singleSignOnType": "None",
            "connectionEncryption": connection_encryption,
            "skipTestConnection": skip_test_connection,
            "credentials": {
                "credentialType": "Basic",
                "username": user_name,
                "password": password,
            },
        },
    }

    client = fabric.FabricRestClient()
    response = client.post("/v1/connections", json=request_body)

    if response.status_code != 201:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{name}' virtual network gateway connection has been created."
    )
