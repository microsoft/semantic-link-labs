import requests
import yaml
import json
from sempy.fabric.exceptions import FabricHTTPException
from sempy._utils._log import log
from sempy_labs.tom import connect_semantic_model
from typing import List
from sempy_labs._generate_semantic_model import create_blank_semantic_model
from sempy_labs.directlake._generate_shared_expression import generate_shared_expression
from sempy_labs.connection._databricks import (
    create_azure_databricks_workspace_connection,
)
from sempy_labs.mirrored_azure_databricks_catalog._items import (
    create_mirrored_azure_databricks_catalog,
)


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
                    "Columns": t.get("columns", []),
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


def sql_to_dax(expr: str) -> str:

    return expr


def convert_format(fmt: dict) -> str:
    """
    Converts the format from Databricks to a Power BI format string.
    """

    def decimals(dp):
        if dp["type"] == "EXACT":
            places = dp.get("places", 0)
            return "." + ("0" * places) if places > 0 else ""
        elif dp["type"] == "ALL":
            return ".################"
        return ""

    def grouping(hide):
        return "" if hide else ","

    def abbreviation(abbrev):
        if abbrev == "COMPACT":
            return ",,"  # millions
        return ""

    symbol_map = {"USD": "$", "EUR": "€", "ILS": "₪"}

    if "currency" in fmt:
        f = fmt["currency"]
        symbol = symbol_map.get(f.get("currency_code"))
        if not symbol:
            print(
                f"Currency code '{f.get('currency_code')}' not recognized. Defaulting to no symbol."
            )
            return None
        return (
            f"{symbol}#"
            + grouping(f["hide_group_separator"])
            + "0"
            + abbreviation(f["abbreviation"])
            + decimals(f["decimal_places"])
        )

    if "number" in fmt:
        f = fmt["number"]
        return (
            "#"
            + grouping(f["hide_group_separator"])
            + "0"
            + abbreviation(f["abbreviation"])
            + decimals(f["decimal_places"])
        )

    return None


def _collect_data_from_metric_view(
    metric_view: str,
    databricks_workspace: str,
    unity_catalog: str,
    schema: str,
    databricks_token: str,
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
    definition = mv_match.get("View Definition")
    objects = mv_match.get("Columns")
    source = definition.get("source")
    joins = definition.get("joins")
    source_database, source_schema, source_table = source.split(".")

    tables = []
    relationships = []
    columns = {}
    measures = {}
    tables.append(
        {
            "databaseName": source_database,
            "schemaName": source_schema,
            "tableName": source_table,
            "isSource": True,
        }
    )

    # Determine relationships
    for join in joins:
        database, schema, table = join["source"].split(".")
        tables.append(
            {
                "databaseName": database,
                "schemaName": schema,
                "tableName": table,
                "isSource": False,
            }
        )
        left, right = join["on"].split("=")

        left = left.strip()
        right = right.strip()

        from_table_alias, from_column = left.split(".")
        to_table_alias, to_column = right.split(".")

        if from_table_alias == "source":
            from_table_alias = source_table
        if to_table_alias == "source":
            to_table_alias = source_table

        relationships.append(
            {
                "from_database": database,
                "from_schema": schema,
                "from_table": from_table_alias,
                "from_column": from_column,
                "to_database": database,  # assuming same DB for now
                "to_schema": schema,  # ⚠️ adjust if different joins possible
                "to_table": to_table_alias,
                "to_column": to_column,
            }
        )

    # TODO: create mirrors for each catalog
    table_names = {t["tableName"] for t in tables}
    source_table = next((t["tableName"] for t in tables if t["isSource"]), None)

    def extract_table_name(expression: str) -> str | None:
        parts = expression.split(".")

        # Must be exactly "xxx.yyyy"
        if len(parts) != 2:
            return None

        left_part, _ = parts

        if left_part.lower() == "source":
            return source_table if source_table in table_names else None

        return left_part if left_part in table_names else None

    # Collect columns and measures
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
                "tableName": extract_table_name(obj_expression),
                "displayName": display_name,
                "type": type_text,
                "format": format,
                "expression": obj_expression,
                "description": description,
                "synonyms": synonyms,
            }
        elif obj_type == "measure":
            measures[name] = {
                "tableName": source_table,  # Default to source table
                "expression": obj_expression,
                "format": format,
                "description": description,
                "synonyms": synonyms,
            }
        else:
            raise NotImplementedError()

    return tables, relationships, columns, measures


def gen_sm(name: str, workspace):

    create_blank_semantic_model(dataset=name, workspace=workspace)

    (tables, relationships, columns, measures) = _collect_data_from_metric_view()

    expr = generate_shared_expression()

    # Generate semantic model
    with connect_semantic_model(dataset=name, workspace=workspace) as tom:

        # Add expression
        expression_name = "DLMirror"
        tom.add_expression(name=expression_name, expression=expr)
        for t in tables:
            table_name = t.get("tableName")
            schema_name = t.get("schemaName")
            tom.add_table(name=table_name)
            tom.add_entity_partition(
                table_name=table_name,
                entity_name=table_name,
                expression=expression_name,
                schema_name=schema_name,
            )

        for col_name, col_info in columns.items():
            table_name = col_info.get("tableName")
            type = col_info.get("type")
            data_type = TYPE_MAPPING.get(type)
            display_name = col_info.get("displayName")
            desc = col_info.get("description")
            expr = col_info.get("expression")
            format = col_info.get("format")
            #  synonyms = col_info.get("synonyms")
            if not table_name:
                print(
                    f"Skipping column {col_name} as its definition could not be determined from the expression."
                )
                continue
            tom.add_data_column(
                table_name=table_name,
                column_name=display_name,
                source_column=col_name,
                data_type=data_type,
                description=desc,
            )

        for measure_name, measure_info in measures.items():
            table_name = measure_info.get("tableName")
            format = measure_info.get("format")
            #  synonyms = measure_info.get("synonyms")
            expr = measure_info.get("expression")
            desc = measure_info.get("description")
            dax = sql_to_dax(expr)
            converted_format = convert_format(format) if format else None
            tom.add_measure(
                table_name=table_name,
                measure_name=measure_name,
                expression=dax,
                format_string=converted_format,
                description=desc,
            )

        for r in relationships:
            tom.add_relationship(
                from_table=r["from_table"],
                from_column=r["from_column"],
                to_table=r["to_table"],
                to_column=r["to_column"],
            )

        # TODO: add synonyms to both columns and measures in TOM
