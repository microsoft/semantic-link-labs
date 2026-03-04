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
                    "View Definition": yaml_dict,
                    "Columns": yaml_dict.get("columns", []),
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



from sempy_labs.tom import connect_semantic_model
import json

type_mapping = {
    "string": "",
    "bigint": "",
    "timestamp": "",
}

with connect_semantic_model(dataset='bob', workspace=None) as tom:

    def add_table_from_metric_view(name: str, databricks_workspace: str, unity_catalog: str, schema: str, databricks_token: str, metric_view: str):

        mvs = list_databricks_metric_views(databricks_workspace=databricks_workspace, unity_catalog=unity_catalog, schema=schema, databricks_token=databricks_token)
        # Find the first matching metric view
        mv_match = next((mv for mv in mvs if mv.get('Name') == metric_view), None)

        if not mv_match:
            print("None...")
            return

        # Safely extract definition and columns
        definition = mv_match.get("Definition")
        objects = mv_match.get("Columns")
        source = definition.get('source')
        joins = definition.get('joins')
        #tom.add_table(name=name)
        #tom.add_entity_partition(table_name=name, entity_name='', expression='', schema_name='')
        columns = {}
        measures = {}
        for c in objects:
            type_json = json.loads(c.get('type_json'))
            name = type_json.get('name')
            type_text = c.get('type_text')
            metadata = type_json.get('metadata')
            obj_type = metadata.get('metric_view.type') # dimension/measure
            obj_expression = metadata.get('metric_view.expr')
            raw_semantic = metadata.get('semantic_metadata')
            semantic_metadata = json.loads(raw_semantic) if raw_semantic else {}
            display_name = semantic_metadata.get('display_name', name)
            format = semantic_metadata.get('format')
            synonyms = semantic_metadata.get('synonyms', [])
            if obj_type == 'dimension':
                columns[name] = {
                    "displayName": display_name,
                    "type": type_text,
                    "format": format,
                    "expression": obj_expression,
                    "synonyms": synonyms,
                }
            elif obj_type == "measure":
                measures[name] = {
                    "expression": obj_expression,
                    "format": format,
                    "synonyms": synonyms,
                } 
            else:
                raise ValueError()
        
        for col_name, col_info in columns.items():
            type = col_info.get('type') # map to Power BI data type
            display_name = col_info.get('displayName')
            expr = col_info.get('expression')
            format = col_info.get('format')
            synonyms = col_info.get('synonyms')
            if expr != f"source.{col_name}":
                print(f"Skipping column '{col_name}' as it is not a direct mapping to the source column.")
                continue
            #tom.add_data_column(table_name=name, column_name=display_name, source_column=col_name, data_type=type_mapping.get(type))

        for measure_name, measure_info in measures.items():
            format = measure_info.get('format')
            synonyms = measure_info.get('synonyms')
            expr = measure_info.get('expression')
            # map expr to DAX
            tom.add_measure(table_name=name, measure_name=measure_name, expression="")