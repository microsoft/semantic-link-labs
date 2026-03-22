import requests
import yaml
import json
from sempy.fabric.exceptions import FabricHTTPException
from sempy._utils._log import log
from sempy_labs.tom import connect_semantic_model
from typing import List

@log
def list_databricks_metric_views(
    databricks_workspace: str, unity_catalog: str, schema: str, databricks_token: str
) -> List[dict]:
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

    Returns
    -------
    List[dict]
        A list of dictionaries, each containing details about a metric view, including its name, view definition, and columns.
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


TYPE_MAPPING = {
    "boolean": "Boolean",
    "tinyint": "Int64",
    "smallint": "Int64",
    "int": "Int64",
    "integer": "Int64",
    "bigint": "Int64",
    "long": "Int64",
    "float": "Double",
    "double": "Double",
    "decimal": "Decimal",
    "string": "String",
    "char": "String",
    "varchar": "String",
    "binary": "Binary",
    "date": "DateTime",
    "timestamp": "DateTime",
    "timestamp_ntz": "DateTime",
}

with connect_semantic_model(dataset="bob", workspace=None) as tom:

    def add_table_from_metric_view(
        name: str,
        databricks_workspace: str,
        unity_catalog: str,
        schema: str,
        databricks_token: str,
        metric_view: str,
    ):

        mvs = list_databricks_metric_views(
            databricks_workspace=databricks_workspace,
            unity_catalog=unity_catalog,
            schema=schema,
            databricks_token=databricks_token,
        )
        # Find the first matching metric view
        mv_match = next((mv for mv in mvs if mv.get("Name") == metric_view), None)

        if not mv_match:
            print("The metrics view does not exist...")
            return

        # Safely extract definition and columns
        definition = mv_match.get("Definition")
        objects = mv_match.get("Columns")
        source = definition.get("source")
        joins = definition.get("joins") # if joins exist - create a materialized view?
        # tom.add_table(name=name)
        # tom.add_entity_partition(table_name=name, entity_name='', expression='', schema_name='')

        # Collect columns and measures
        columns = {}
        measures = {}
        for c in objects:
            type_json = json.loads(c.get("type_json"))
            name = type_json.get("name")
            type_text = c.get("type_text")
            metadata = type_json.get("metadata")
            obj_type = metadata.get("metric_view.type")  # dimension/measure
            description = metadata.get("comment")
            obj_expression = metadata.get("metric_view.expr")
            raw_semantic = metadata.get("semantic_metadata")
            semantic_metadata = json.loads(raw_semantic) if raw_semantic else {}
            display_name = semantic_metadata.get("display_name", name)
            format = semantic_metadata.get("format")
            synonyms = semantic_metadata.get("synonyms", [])
            if obj_type == "dimension":
                columns[name] = {
                    "displayName": display_name,
                    "type": type_text,
                    "format": format,
                    "expression": obj_expression,
                    "description": description,
                    "synonyms": synonyms,
                }
            elif obj_type == "measure":
                measures[name] = {
                    "expression": obj_expression,
                    "format": format,
                    "description": description,
                    "synonyms": synonyms,
                }
            else:
                raise NotImplementedError()

        for col_name, col_info in columns.items():
            type = col_info.get("type")
            data_type = TYPE_MAPPING.get(type)
            display_name = col_info.get("displayName")
            desc = col_info.get("description")
            expr = col_info.get("expression")
            format = col_info.get("format")
            synonyms = col_info.get("synonyms")
            if expr != f"source.{col_name}":
                print(
                    f"Skipping column '{col_name}' as it is not a direct mapping to the source column."
                )
                continue
            tom.add_data_column(
                table_name=name,
                column_name=display_name,
                source_column=col_name,
                data_type=data_type,
                description=desc,
            )

        for measure_name, measure_info in measures.items():
            format = measure_info.get("format")
            synonyms = measure_info.get("synonyms")
            expr = measure_info.get("expression")
            description = measure_info.get("description")
            dax_expr = expr.lower()  # TODO: map expression to dax
            tom.add_measure(
                table_name=name, measure_name=measure_name, expression=dax_expr
            )

        # TODO: add synonyms to both columns and measures in TOM
