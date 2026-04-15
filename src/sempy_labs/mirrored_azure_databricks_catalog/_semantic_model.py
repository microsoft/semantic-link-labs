from collections import defaultdict
from itertools import combinations
import re
import json
from sempy_labs._helper_functions import (
    convert_column_data_type,
)
from sempy_labs.tom import connect_semantic_model
from sempy_labs._generate_semantic_model import create_blank_semantic_model
from sempy_labs.directlake._generate_shared_expression import (
    generate_shared_expression,
)
from sempy_labs.mirrored_azure_databricks_catalog._list_objects import (
    list_databricks_metric_views,
)
from sempy_labs._sql import ConnectMirroredAzureDatabricksCatalog


def convert_sql_to_dax(expression: str, source_table_name: str) -> str:
    """
    Convert a simple Databricks SQL measure expression to a DAX expression.

    Parameters:
        expression (str): SQL expression from Databricks metric view
        source_table_name (str): Table name to use in DAX

    Returns:
        str: Converted DAX expression
    """

    expr = expression.strip()

    # 1. Handle COUNT(*)
    if re.fullmatch(r"COUNT\s*\(\s*\*\s*\)", expr, re.IGNORECASE):
        return f"COUNTROWS('{source_table_name}')"

    # 2. Replace source.column -> 'table'[column]
    def replace_source_column(match):
        column = match.group(1)
        return f"'{source_table_name}'[{column}]"

    expr = re.sub(
        r"\bsource\.([a-zA-Z_][a-zA-Z0-9_]*)\b",
        replace_source_column,
        expr,
        flags=re.IGNORECASE,
    )

    # 3. Normalize common aggregations (optional but useful)
    # e.g. SUM(source.col) -> SUM('table'[col])
    # (Already handled by replacement above)

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

    symbol_map = {
        "USD": "$",  # US Dollar
        "EUR": "€",  # Euro
        "GBP": "£",  # British Pound
        "ILS": "₪",  # Israeli Shekel
        "JPY": "¥",  # Japanese Yen
        "CNY": "¥",  # Chinese Yuan
        "INR": "₹",  # Indian Rupee
        "KRW": "₩",  # South Korean Won
        "RUB": "₽",  # Russian Ruble
        "TRY": "₺",  # Turkish Lira
    }

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
    joins = definition.get("joins", [])
    source_catalog, source_schema, source_table = source.split(".")

    tables = []
    relationships = []
    columns = {}
    measures = {}
    tables.append(
        {
            "catalogName": source_catalog,
            "schemaName": source_schema,
            "tableName": source_table,
            "isSource": True,
        }
    )

    # Determine relationships
    for join in joins:
        catalog, schema, table = join["source"].split(".")
        tables.append(
            {
                "catalogName": catalog,
                "schemaName": schema,
                "tableName": table,
                "isSource": False,
            }
        )

        r = join.get('on')
        join_source = join.get('source')
        join_name = join.get('name')
        left, right = r.split('=')
        from_object = left.strip()
        to_object = right.strip()
        from_table_alias, from_column = from_object.split('.')
        to_table_alias, to_column = to_object.split('.')

        from_source = None
        to_source = None
        if from_table_alias == "source":
            from_source = source
        elif from_table_alias == join_name:
            from_source = join_source
        if to_table_alias == "source":
            to_source = source
        elif to_table_alias == join_name:
            to_source = join_name

        if from_source is None or to_source is None:
            raise ValueError()

        from_catalog, from_schema, from_table = from_source.split('.')
        to_catalog, to_schema, to_table = to_source.split('.')

        relationships.append(
            {
                "from_catalog": from_catalog,
                "from_schema": from_schema,
                "from_table": from_table,
                "from_column": from_column,
                "to_catalog": to_catalog,
                "to_schema": to_schema,
                "to_table": to_table,
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
            data_type = convert_column_data_type(type)
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
            dax = convert_sql_to_dax(expr, table_name)
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


def infer_model_relationships(column_list, workspace):

    candidates = [c for c in column_list if c.get("columnName", "").endswith("ID")]

    def find_column_relationships(candidates):
        groups = defaultdict(list)

        for c in candidates:
            groups[(c["columnName"], c["dataType"])].append(c)

        return [
            (c1, c2)
            for cols in groups.values()
            if len(cols) > 1
            for c1, c2 in combinations(cols, 2)
            if (c1["tableName"], c1["sourceSchema"], c1["sourceCatalog"])
            != (c2["tableName"], c2["sourceSchema"], c2["sourceCatalog"])
        ]

    # ✅ Cache results to avoid repeated DB hits
    cardinality_cache = {}

    def is_unique(source, schema, table, column, workspace):
        key = (source, schema, table, column)

        if key not in cardinality_cache:
            with ConnectMirroredAzureDatabricksCatalog(
                mirrored_azure_databricks_catalog=source, workspace=workspace
            ) as sql:
                df = sql.query(
                    f"""SELECT COUNT(DISTINCT {column}) AS ct_col,
                            COUNT({column}) AS ct_tbl
                        FROM {schema}.{table}"""
                )
            ct_col, ct_tbl = df.iloc[0]
            cardinality_cache[key] = ct_col == ct_tbl

        return cardinality_cache[key]

    # ✅ Build relationships
    relationships = []

    for left, right in find_column_relationships(candidates):

        left_key = (
            left["sourceCatalog"],
            left["sourceSchema"],
            left["tableName"],
            left["columnName"],
        )
        right_key = (
            right["sourceCatalog"],
            right["sourceSchema"],
            right["tableName"],
            right["columnName"],
        )

        # Check uniqueness (dimension side)
        if is_unique(*left_key, workspace=workspace):
            relationships.append(
                {
                    "fromTable": right["tableName"],
                    "fromColumn": right["columnName"],
                    "toTable": left["tableName"],
                    "toColumn": left["columnName"],
                }
            )
        elif is_unique(*right_key, workspace=workspace):
            relationships.append(
                {
                    "fromTable": left["tableName"],
                    "fromColumn": left["columnName"],
                    "toTable": right["tableName"],
                    "toColumn": right["columnName"],
                }
            )

    return relationships
