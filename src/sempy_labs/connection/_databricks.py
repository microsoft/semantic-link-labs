from typing import Optional, Literal
from uuid import UUID
import sempy_labs._icons as icons
from sempy_labs._helper_functions import _base_api
from sempy._utils._log import log


@log
def create_databricks_connection(
    name: str,
    server_hostname: str,
    http_path: str,
    databricks_token: str,
    catalog: Optional[str] = None,
    privacy_level: Optional[
        Literal["None", "Public", "Organizational", "Private"]
    ] = None,
    connection_encryption: Optional[Literal["Encrypted", "NotEncrypted", "Any"]] = None,
) -> UUID:
    """
    Create a connection to an Azure Databricks workspace in Microsoft Fabric.

    Parameters
    ----------
    name : str
        The display name of the connection.
    server_hostname : str
        The server hostname of the Azure Databricks workspace. Example: "dbc-12345x67-8xx9.cloud.databricks.com"
    http_path : str
        The HTTP path for the Azure Databricks workspace. Example: "/sql/1.0/endpoints/12345678-90ab-cdef-1234-567890abcdef"
    databricks_token : str
        The personal access token for authenticating with the Azure Databricks REST API.
    catalog : str, default=None
        The default catalog to use for the connection. This is optional and can be set to None.
    privacy_level : typing.Literal["None", "Public", "Organizational", "Private"], default=None
        The privacy level for the connection. Allowed values are "None", "Public", "Organizational", "Private". This is optional and can be set to None.
    connection_encryption : typing.Literal["Encrypted", "NotEncrypted", "Any"], default=None
        The connection encryption setting for the connection. Allowed values are "Encrypted", "NotEncrypted", "Any". This is optional and can be set to None.

    Returns
    -------
    uuid.UUID
        The unique identifier of the created connection.
    """

    if privacy_level not in ["None", "Public", "Organizational", "Private", None]:
        raise ValueError(
            "Invalid privacy level. Allowed values are: 'None', 'Public', 'Organizational', 'Private'."
        )
    if connection_encryption not in ["Encrypted", "NotEncrypted", "Any", None]:
        raise ValueError(
            "Invalid connection encryption. Allowed values are: 'Encrypted', 'NotEncrypted', 'Any'."
        )

    payload = {
        "connectivityType": "ShareableCloud",
        "connectionDetails": {
            "type": "Databricks",
            "CreationMethod": "Databricks.Catalogs",
            "parameters": [
                {
                    "name": "host",
                    "dataType": "Text",
                    "required": True,
                    "value": server_hostname,
                },
                {
                    "name": "httpPath",
                    "dataType": "Text",
                    "required": True,
                    "value": http_path,
                },
            ],
        },
        "displayName": name,
        "credentialDetails": {
            "credentials": {"credentialType": "Key", "key": databricks_token},
            "singleSignOnType": "None",
            "skipTestConnection": False,
        },
    }
    if catalog:
        payload["connectionDetails"]["parameters"].append(
            {
                "name": "Catalog",
                "dataType": "Text",
                "required": False,
                "value": catalog,
            }
        )
    if privacy_level:
        payload["privacyLevel"] = privacy_level
    if connection_encryption:
        payload["credentialDetails"]["connectionEncryption"] = connection_encryption

    response = _base_api(
        request="/v1/connections", payload=payload, method="post", status_codes=201
    )

    print(f"{icons.green_dot} The '{name}' connection has been succesfully created.")

    return response.json().get("id")


@log
def create_azure_databricks_workspace_connection(
    name: str,
    url: str,
    databricks_token: str,
    privacy_level: Optional[
        Literal["None", "Public", "Organizational", "Private"]
    ] = None,
    connection_encryption: Optional[Literal["Encrypted", "NotEncrypted", "Any"]] = None,
) -> UUID:
    """
    Create a connection to an Azure Databricks workspace in Microsoft Fabric.

    Parameters
    ----------
    name : str
        The display name of the connection.
    url : str
        The URL of the Azure Databricks workspace. Example: "https://dbc-12345x67-8xx9.cloud.databricks.com"
    databricks_token : str
        The personal access token for authenticating with the Azure Databricks REST API.
    privacy_level : typing.Literal["None", "Public", "Organizational", "Private"], default=None
        The privacy level for the connection. Allowed values are "None", "Public", "Organizational", "Private". This is optional and can be set to None.
    connection_encryption : typing.Literal["Encrypted", "NotEncrypted", "Any"], default=None
        The connection encryption setting for the connection. Allowed values are "Encrypted", "NotEncrypted", "Any". This is optional and can be set to None.

    Returns
    -------
    uuid.UUID
        The unique identifier of the created connection.
    """

    if privacy_level not in ["None", "Public", "Organizational", "Private", None]:
        raise ValueError(
            "Invalid privacy level. Allowed values are: 'None', 'Public', 'Organizational', 'Private'."
        )
    if connection_encryption not in ["Encrypted", "NotEncrypted", "Any", None]:
        raise ValueError(
            "Invalid connection encryption. Allowed values are: 'Encrypted', 'NotEncrypted', 'Any'."
        )

    payload = {
        "connectivityType": "ShareableCloud",
        "connectionDetails": {
            "type": "AzureDatabricksWorkspace",
            "CreationMethod": "AzureDatabricksWorkspace.Actions",
            "parameters": [
                {
                    "name": "url",
                    "dataType": "Text",
                    "required": True,
                    "value": url,
                },
            ],
        },
        "displayName": name,
        "credentialDetails": {
            "credentials": {"credentialType": "Key", "key": databricks_token},
            "singleSignOnType": "None",
            "skipTestConnection": False,
        },
    }
    if privacy_level:
        payload["privacyLevel"] = privacy_level
    if connection_encryption:
        payload["credentialDetails"]["connectionEncryption"] = connection_encryption

    response = _base_api(
        request="/v1/connections", payload=payload, method="post", status_codes=201
    )

    print(f"{icons.green_dot} The '{name}' connection has been succesfully created.")

    return response.json().get("id")
