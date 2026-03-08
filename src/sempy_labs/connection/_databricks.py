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
    #server_hostname: str,
    http_path: str,
    databricks_token: str,
    privacy_level: Optional[
        Literal["None", "Public", "Organizational", "Private"]
    ] = None,
    connection_encryption: Optional[Literal["Encrypted", "NotEncrypted", "Any"]] = None,
) -> UUID:

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
    if privacy_level:
        payload["privacyLevel"] = privacy_level
    if connection_encryption:
        payload["credentialDetails"]["connectionEncryption"] = connection_encryption

    response = _base_api(
        request="/v1/connections", payload=payload, method="post", status_codes=201
    )

    print(f"{icons.green_dot} The '{name}' connection has been succesfully created.")

    return response.json().get("id")
