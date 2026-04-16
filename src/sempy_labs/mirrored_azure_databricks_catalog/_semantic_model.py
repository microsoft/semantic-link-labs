from typing import Optional
from collections import defaultdict
from itertools import combinations
from uuid import UUID
import json
from sempy_labs._helper_functions import (
    convert_column_data_type,
    resolve_item_id,
    resolve_workspace_id,
    convert_sql_to_dax,
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
import sempy_labs._icons as icons
from sempy_labs.mirrored_azure_databricks_catalog._items import (
    get_mirrored_azure_databricks_catalog
)
from sempy._utils._log import log


def _collect_data_from_metric_view(
    metric_view: str,
    databricks_workspace: str,
    databricks_token: str,
):
    """
    Generates a model map of sources, tables, columns, measures, relationships based on a Databricks Metric View.
    """

    parts = metric_view.split(".")

    if len(parts) != 3:
        raise ValueError(f"Invalid metric_view format: '{metric_view}' (expected 'catalog.schema.metric')")

    catalog, schema, metric_view_name = parts

    mvs = list_databricks_metric_views(
        databricks_workspace=databricks_workspace,
        unity_catalog=catalog,
        schema=schema,
        databricks_token=databricks_token,
    )
    # Find the first matching metric view
    mv_match = next((mv for mv in mvs if mv.get("Name") == metric_view_name), None)

    if not mv_match:
        raise ValueError(f"The '{metric_view}' metric view does not exist or could not be found.")

    # Safely extract definition and columns
    definition = mv_match.get("View Definition")
    objects = mv_match.get("Columns")
    source = definition.get("source")
    joins = definition.get("joins", [])
    source_catalog, source_schema, source_table = source.split(".")

    model_map = {"sources": {}, "relationships": {}, "tables": {}}
    model_map["sources"][source] = {
        "catalogName": source_catalog,
        "schemaName": source_schema,
        "tableName": source_table,
        "isSource": True,
    }

    # Determine relationships
    for idx, join in enumerate(joins):
        join_name = join.get("name")
        join_source = join.get("source")
        join_on = join.get("on")
        if ' AND ' in join_on:
            print(f"Joins with multiple conditions are not supported. Adjust the tables so that joins only have a single condition. Multi-conditional join: {join_on}.")
            return
        catalog, schema, table = join_source.split(".")
        model_map["sources"][join_source] = {
            "catalogName": catalog,
            "schemaName": schema,
            "tableName": table,
            "isSource": False,
        }

        left, right = join_on.split("=")
        from_object = left.strip()
        to_object = right.strip()
        from_table_alias, from_column = from_object.split(".")
        to_table_alias, to_column = to_object.split(".")

        alias_to_source = {"source": source, join_name: join_source}

        from_source = alias_to_source.get(from_table_alias)
        to_source = alias_to_source.get(to_table_alias)

        if from_source is None or to_source is None:
            raise ValueError(f"Could not resolve table aliases in the join condition: '{from_table_alias}' or '{to_table_alias}' not found among source and joins.")

        from_catalog, from_schema, from_table = from_source.split(".")
        to_catalog, to_schema, to_table = to_source.split(".")

        model_map["relationships"][f"Relationship_{idx}"] = {
            "from_catalog": from_catalog,
            "from_schema": from_schema,
            "from_table": from_table,
            "from_column": from_column,
            "to_catalog": to_catalog,
            "to_schema": to_schema,
            "to_table": to_table,
            "to_column": to_column,
        }

    # TODO: create mirrors for each catalog
    table_names = [s["tableName"] for s in model_map["sources"].values()]

    duplicates = {t for t in table_names if table_names.count(t) > 1}
    if duplicates:
        raise ValueError(f"Duplicate table names found: {duplicates}")

    # ✅ Build tables map
    for s in model_map["sources"].values():
        table_name = s["tableName"]

        model_map["tables"][table_name] = {
            "catalogName": s["catalogName"],
            "schemaName": s["schemaName"],
            "columns": [],
            "measures": [],
        }

    source_table = next(
        (s.get("tableName") for s in model_map["sources"].values() if s["isSource"]),
        None,
    )

    def extract_table_name(expression: str, source_table: str) -> str | None:
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
        data_type = c.get("type_text")
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
            table_name = extract_table_name(obj_expression, source_table)
            if not table_name:
                print(
                    f"{icons.warning} Skipping the '{display_name}' column as it could not be determined from the expression."
                )
                continue
            model_map["tables"][table_name]["columns"].append(
                {
                    "columnName": display_name,
                    "sourceColumn": name,
                    "dataType": convert_column_data_type(data_type),
                    "format": format,
                    "description": description,
                    "expression": obj_expression,
                    "synonyms": synonyms,
                }
            )
        elif obj_type == "measure":
            model_map["tables"][source_table]["measures"].append(
                {
                    "measureName": display_name,
                    "format": format,
                    "description": description,
                    "expression": convert_sql_to_dax(obj_expression, source_table),
                    "synonyms": synonyms,
                }
            )
        else:
            raise NotImplementedError(
                f"Please raise a support issue that the '{obj_type}' object type is not supported."
            )

    return model_map


@log
def generate_semantic_model_from_metric_view(
    name: str,
    metric_view: str,
    databricks_workspace: str,
    databricks_token: str,
    sources: dict,
    workspace: Optional[str | UUID] = None,
):
    """

    Parameters
    ----------
    name : str
        Name of the semantic model to create.
    metric_view : str
        In the format of catalog.schema.metric_view_name. This metric view will be used as the source to generate the semantic model.
    databricks_workspace : str
        The Databricks workspace URL (e.g. "https://adb-1234567890123456.7.azuredatabricks.net").
    databricks_token : str
        A Databricks personal access token with permissions to read the metric view and its underlying tables.
    sources : dict

        Example:

        sources = {
            "catalog1": {
                "mirror": "",
                "workspace": "",
            },
            "catalog2": {
                "mirror": "",
                "workspace": "",
            }
        }
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """
    model_map = _collect_data_from_metric_view(metric_view=metric_view, databricks_workspace=databricks_workspace, databricks_token=databricks_token)

    create_blank_semantic_model(dataset=name, workspace=workspace, overwrite=False)

    # Validate catalogs
    catalogs = {s["catalogName"] for s in model_map["sources"].values()}
    mirrored_catalogs = {}
    for catalog, items in sources.items():
        mirror = items.get("mirror")
        ws = items.get("workspace")
        if not mirror or not ws:
            raise ValueError(
                f"{icons.warning} Skipping catalog '{catalog}' as it does not have a mirror and workspace defined in the sources."
            )
        mirror_workspace_id = resolve_workspace_id(workspace=ws)
        mirror_id = resolve_item_id(item=mirror, type="MirroredAzureDatabricksCatalog", workspace=mirror_workspace_id)
        mirror_catalog = get_mirrored_azure_databricks_catalog(mirrored_azure_databricks_catalog=mirror_id, workspace=mirror_workspace_id, return_dataframe=False).get('properties', {}).get('catalogName')
        if catalog != mirror_catalog:
            raise ValueError(
                f"{icons.warning} The catalog name '{catalog}' in the model map does not match the catalog name '{mirror_catalog}' of the mirrored Azure Databricks Catalog item."
            )
        if catalog in mirrored_catalogs:
            raise ValueError(
                f"{icons.warning} Duplicate catalog '{catalog}' found in sources. Each catalog should only be listed once."
            )
        mirrored_catalogs[catalog] = {
            "mirror_id": mirror_id,
            "workspace_id": mirror_workspace_id,
        }

    if catalogs != mirrored_catalogs.keys():
        raise ValueError(
            f"{icons.red_dot} The catalogs in the model map do not match the mirrored catalogs in the sources."
        )

    # Generate semantic model
    with connect_semantic_model(dataset=name, workspace=workspace, readonly=False) as tom:

        # Add expression
        for catalog, info in mirrored_catalogs.items():
            mirror_id = info["mirror_id"]
            mirror_workspace_id = info["workspace_id"]
            mirror_expr = generate_shared_expression(
                item=mirror_id,
                type="MirroredAzureDatabricksCatalog",
                workspace=mirror_workspace_id,
                use_sql_endpoint=False,
            )
            tom.add_expression(name=f"Mirror_{catalog}", expression=mirror_expr)

        for table_name, table_info in model_map["tables"].items():
            catalog = table_info["catalogName"]
            schema_name = table_info["schemaName"]
            mirror_expr_name = f"Mirror_{catalog}"
            tom.add_table(
                name=table_name,
            )
            tom.add_entity_partition(
                table_name=table_name,
                entity_name=table_name,
                expression=mirror_expr_name,
                schema_name=schema_name,
            )

            for column in table_info["columns"]:
                column_name = column["columnName"]
                source_column = column["sourceColumn"]
                data_type = column["dataType"]
                # format = column.get("format")
                # synonyms = column.get("synonyms")
                desc = column.get("description")
                tom.add_data_column(
                    table_name=table_name,
                    column_name=column_name,
                    source_column=source_column,
                    data_type=data_type,
                    # dformat_string=convert_format(format) if format else None,
                    description=desc,
                )

            for measure in table_info['measures'].items():
                table_name = measure.get("tableName")
                #format = measure.get("format")
                #  synonyms = measure_info.get("synonyms")
                expr = measure.get("expression")
                desc = measure.get("description")
                #converted_format = convert_format(format) if format else None
                tom.add_measure(
                    table_name=table_name,
                    measure_name=measure,
                    expression=expr,
                    #format_string=converted_format,
                    description=desc,
                )

        for r in model_map['relationships']:
            tom.add_relationship(
                from_table=r["from_table"],
                from_column=r["from_column"],
                to_table=r["to_table"],
                to_column=r["to_column"],
            )


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
