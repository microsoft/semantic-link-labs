import pandas as pd
from typing import Literal, Optional
from uuid import UUID
from sempy._utils._log import log
import sempy_labs.connection as conn


@log
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

    conn.delete_connection(connection)


@log
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

    conn.delete_connection_role_assignment(connection, role_assignment_id)


@log
def resolve_connection_id(connection: str | UUID) -> UUID:

    return conn.resolve_connection_id(connection)


@log
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

    return conn.list_connection_role_assignments(connection)


@log
def list_connections() -> pd.DataFrame:
    """
    Lists all available connections.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing all available connections.
    """

    return conn.list_connections()


@log
def list_item_connections(
    item: Optional[str | UUID] = None,
    type: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Shows the list of connections that the specified item is connected to.

    This is a wrapper function for the following API: `Items - List Item Connections <https://learn.microsoft.com/rest/api/fabric/core/items/list-item-connections>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID.
    type : str
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

    return conn.list_item_connections(
        item=item, type=type, workspace=workspace, **kwargs
    )


@log
def list_supported_connection_types(
    gateway: Optional[str | UUID] = None, show_all_creation_methods: bool = False
) -> pd.DataFrame:

    return conn.list_supported_connection_types(
        gateway=gateway, show_all_creation_methods=show_all_creation_methods
    )


@log
def create_cloud_connection(
    name: str,
    server_name: str,
    database_name: str,
    user_name: str,
    password: str,
    privacy_level: str,
    connection_encryption: Literal["Encrypted", "Any", "NotEncrypted"] = "NotEncrypted",
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
    connection_encryption : typing.Literal["Encrypted", "Any", "NotEncrypted"], default="NotEncrypted"
        The connection encryption type.
    skip_test_connection: bool, default=False
        If True, skips the test connection.
    """

    conn.create_cloud_connection(
        name=name,
        server_name=server_name,
        database_name=database_name,
        user_name=user_name,
        password=password,
        privacy_level=privacy_level,
        connection_encryption=connection_encryption,
        skip_test_connection=skip_test_connection,
    )


@log
def create_on_prem_connection(
    name: str,
    gateway: str | UUID,
    server_name: str,
    database_name: str,
    credentials: str,
    privacy_level: str,
    connection_encryption: Literal["Encrypted", "Any", "NotEncrypted"] = "NotEncrypted",
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
    credentials : str
        The encrypted credentials obtained from the gateway.
    privacy_level : str
        The `privacy level <https://learn.microsoft.com/rest/api/fabric/core/connections/create-connection?tabs=HTTP#privacylevel>`_ of the connection.
    connection_encryption : typing.Literal["Encrypted", "Any", "NotEncrypted"], default="NotEncrypted"
        The connection encryption type.
    skip_test_connection: bool, default=False
        If True, skips the test connection.
    """

    conn.create_on_prem_connection(
        name=name,
        gateway=gateway,
        server_name=server_name,
        database_name=database_name,
        credentials=credentials,
        privacy_level=privacy_level,
        connection_encryption=connection_encryption,
        skip_test_connection=skip_test_connection,
    )


@log
def create_vnet_connection(
    name: str,
    gateway: str | UUID,
    server_name: str,
    database_name: str,
    user_name: str,
    password: str,
    privacy_level: str,
    connection_encryption: Literal["Encrypted", "Any", "NotEncrypted"] = "NotEncrypted",
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
    connection_encryption : typing.Literal["Encrypted", "Any", "NotEncrypted"], default="NotEncrypted"
        The connection encryption type.
    skip_test_connection: bool, default=False
        If True, skips the test connection.
    """

    conn.create_vnet_connection(
        name=name,
        gateway=gateway,
        server_name=server_name,
        database_name=database_name,
        user_name=user_name,
        password=password,
        privacy_level=privacy_level,
        connection_encryption=connection_encryption,
        skip_test_connection=skip_test_connection,
    )
