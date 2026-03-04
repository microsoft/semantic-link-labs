import requests
import yaml
from sempy.fabric.exceptions import FabricHTTPException
from sempy._utils._log import log
from typing import Optional, Literal
from uuid import UUID
import sempy_labs._icons as icons
from sempy_labs._helper_functions import _base_api


@log
def list_databricks_metric_views(
    databricks_workspace: str, unity_catalog: str, schema: str, databricks_token: str
):
    """
    Lists all metric views in a specified Unity Catalog and schema within an Azure Databricks workspace.

    Parameters
    ----------
    databricks_workspace : str
        The URL of the Azure Databricks workspace. Example: "https://dbc-12345x67-8xx9.cloud.databricks.com"
    unity_catalog : str
        The name of the Unity Catalog.
    schema : str
        The name of the schema within the Unity Catalog.
    databricks_token : str
        The personal access token for authenticating with the Azure Databricks REST API.
    """

    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json",
    }
    response = requests.get(
        f"{databricks_workspace}/api/2.1/unity-catalog/tables?catalog_name={unity_catalog}&schema_name={schema}",
        headers=headers,
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    rows = []
    for t in response.json().get("tables"):
        name = t.get("name")
        table_type = t.get("table_type")
        view_definition = t.get("view_definition")
        if table_type == "METRIC_VIEW":
            yaml_dict = yaml.safe_load(view_definition)
            rows.append(
                {
                    "Name": name,
                    "Definition": yaml_dict,
                }
            )

    return rows


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
                {
                    "name": "Catalog",
                    "dataType": "Text",
                    "required": False,
                    "value": catalog,
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
