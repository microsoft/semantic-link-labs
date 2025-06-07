import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    _is_valid_uuid,
    resolve_workspace_name_and_id,
    _update_dataframe_datatypes,
    _base_api,
    _create_dataframe,
    resolve_item_id,
)
from uuid import UUID
import sempy_labs._icons as icons
from sempy_labs._gateways import _resolve_gateway_id


def delete_connection(connection: str | UUID):
    """
    Delete a connection.

    This is a wrapper function for the following API: `Connections - Delete Connection <https://learn.microsoft.com/rest/api/fabric/core/connections/delete-connection>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    connection : str | uuid.UUID
        The connection name or ID.
    """

    connection_id = _resolve_connection_id(connection)
    _base_api(
        request=f"/v1/connections/{connection_id}", client="fabric_sp", method="delete"
    )
    print(f"{icons.green_dot} The '{connection}' connection has been deleted.")


def delete_connection_role_assignment(connection: str | UUID, role_assignment_id: UUID):
    """
    Delete the specified role assignment for the connection.

    This is a wrapper function for the following API: `Connections - Delete Connection Role Assignment <https://learn.microsoft.com/rest/api/fabric/core/connections/delete-connection-role-assignment>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    connection : str | uuid.UUID
        The connection name or ID.
    role_assignment_id : uuid.UUID
        The role assignment ID.
    """

    connection_id = _resolve_connection_id(connection)
    _base_api(
        request=f"/v1/connections/{connection_id}/roleAssignments/{role_assignment_id}",
        client="fabric_sp",
        method="delete",
    )

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

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    columns = {
        "Connection Role Assignment Id": "string",
        "Principal Id": "string",
        "Principal Type": "string",
        "Role": "string",
    }

    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1/connections/{connection_id}/roleAssignments",
        client="fabric_sp",
        uses_pagination=True,
    )

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

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing all available connections.
    """

    columns = {
        "Connection Id": "string",
        "Connection Name": "string",
        "Gateway Id": "string",
        "Connectivity Type": "string",
        "Connection Path": "string",
        "Connection Type": "string",
        "Privacy Level": "string",
        "Credential Type": "string",
        "Single Sign On Type": "string",
        "Connection Encryption": "string",
        "Skip Test Connection": "bool",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request="/v1/connections", client="fabric_sp", uses_pagination=True
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

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


def list_item_connections(
    item_name: str, item_type: str, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows the list of connections that the specified item is connected to.

    This is a wrapper function for the following API: `Items - List Item Connections <https://learn.microsoft.com/rest/api/fabric/core/items/list-item-connections>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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
    item_id = resolve_item_id(item=item_name, type=item_type, workspace=workspace_id)

    columns = {
        "Connection Name": "string",
        "Connection Id": "string",
        "Connectivity Type": "string",
        "Connection Type": "string",
        "Connection Path": "string",
        "Gateway Id": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/items/{item_id}/connections",
        client="fabric_sp",
        uses_pagination=True,
    )

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

    columns = {
        "Connection Type": "string",
        "Creation Method": "string",
        "Supported Credential Types": "string",
        "Supported Connection Encryption Types": "string",
        "Supports Skip Test Connection": "bool",
    }
    df = _create_dataframe(columns=columns)

    url = url.rstrip("&")
    responses = _base_api(request=url, client="fabric_sp", uses_pagination=True)

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

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

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

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    payload = {
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

    _base_api(
        request="/v1/connections",
        client="fabric_sp",
        method="post",
        payload=payload,
        status_codes=201,
    )

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

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    payload = {
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

    _base_api(
        request="/v1/connections",
        client="fabric_sp",
        method="post",
        payload=payload,
        status_codes=201,
    )

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

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    payload = {
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

    _base_api(
        request="/v1/connections",
        client="fabric_sp",
        method="post",
        payload=payload,
        status_codes=201,
    )

    print(
        f"{icons.green_dot} The '{name}' virtual network gateway connection has been created."
    )
