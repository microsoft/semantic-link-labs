from typing import Optional, List
from collections import defaultdict
from itertools import combinations
from uuid import UUID
import json
from sempy_labs._helper_functions import (
    convert_column_data_type,
    resolve_item_id,
    resolve_workspace_id,
    convert_sql_to_dax,
    retry,
    create_abfss_path,
    list_columns_from_path,
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
    get_mirrored_azure_databricks_catalog,
)
from sempy._utils._log import log
from sempy_labs.connection._items import list_connections
from sempy_labs._refresh_semantic_model import refresh_semantic_model


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
        raise ValueError(
            f"Invalid metric_view format: '{metric_view}' (expected 'catalog.schema.metric')"
        )

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
        raise ValueError(
            f"The '{metric_view}' metric view does not exist or could not be found."
        )

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
        "sourceName": 'source',
        "isSource": True,
        "mirrorId": None,
        "mirrorWorkspaceId": None,
        "columns": [],
    }

    # Determine relationships
    for idx, join in enumerate(joins):
        join_name = join.get("name")
        join_source = join.get("source")
        join_on = join.get("on", {})
        join_using = join.get('using', [])

        catalog, schema, table = join_source.split(".")
        model_map["sources"][join_source] = {
            "catalogName": catalog,
            "schemaName": schema,
            "tableName": table,
            "sourceName": join_name,
            "isSource": False,
            "mirrorId": None,
            "mirrorWorkspaceId": None,
            "columns": [],
        }

        if join_on:
            if " AND " in join_on:
                print(
                    f"Joins with multiple conditions are not supported. Adjust the tables so that joins only have a single condition. Multi-conditional join: {join_on}."
                )
                return

            left, right = join_on.split("=")
            from_object = left.strip()
            to_object = right.strip()
            from_table_alias, from_column = from_object.split(".")
            to_table_alias, to_column = to_object.split(".")

            alias_to_source = {"source": source, join_name: join_source}

            from_source = alias_to_source.get(from_table_alias)
            to_source = alias_to_source.get(to_table_alias)

            if from_source is None or to_source is None:
                raise ValueError(
                    f"Could not resolve table aliases in the join condition: '{from_table_alias}' or '{to_table_alias}' not found among source and joins."
                )

            from_catalog, from_schema, from_table = from_source.split(".")
            to_catalog, to_schema, to_table = to_source.split(".")
        elif join_using:
            from_catalog, from_schema, from_table = source_catalog, source_schema, source_table
            to_catalog, to_schema, to_table = catalog, schema, table
            if len(join_using) > 1:
                print(
                    f"Joins with multiple conditions are not supported. Adjust the tables so that joins only have a single condition. Multi-conditional join: {join_on}."
                )
                return
            from_column = join_using[0]
            to_column = join_using[0]
        else:
            raise NotImplementedError("Unsupported join type.")

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

    def extract_table_name(expression: str) -> str | None:
        parts = expression.split(".")

        # Must be exactly "xxx.yyyy"
        if len(parts) != 2:
            return None

        left_part, _ = parts

        for join_source, properties in model_map['sources'].items():
            table_name = properties.get('tableName')
            source_name = properties.get('sourceName')
            if left_part == source_name:
                return table_name

        return None

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
            table_name = extract_table_name(obj_expression)
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
                    "expression": obj_expression,
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
    sources: dict | List[dict],
    workspace: Optional[str | UUID] = None,
    refresh: bool = True,
):
    """
    Creates a Direct Lake semantic model based on an Azure Databricks metric view. Before running this function, ensure that the Mirrored Azure Databricks Catalog(s) have been set up in Fabric for all catalogs referenced by the metric view.

    Limitations:
        * Calculated columns are ignored and not added to the semantic model
        * Measures based on calculated columns are ignored and not added to the semantic model.
        * Relationships based on multiple columns per table are not supported.
        * Metric Views which rely on Materialized Views in Databricks are not supported as they do not show up in Azure Mirrored Databricks Catalogs.
        * Metric views use SQL for measure expressions. This is converted to DAX. Not all measures may properly convert to DAX

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
    sources : dict | typing.List[dict]
        A dictionary or list of dictionaries of the Mirrored Azure Databricks Catalog(s) used as source(s) for the metric view.

        Example 1:

        sources = {
                    "mirror": "mkcat",
                    "workspace": None,
                }

        Example 2:

        sources = [
            {
                "mirror": "mkcat",
                "workspace": None,
            },
            {
                "mirror": "mkcatdim",
                "workspace": 'Workspace B',
            }
        ]
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """
    workspace_id = resolve_workspace_id(workspace)

    if isinstance(sources, dict):
        sources = [sources]

    databricks_workspace = databricks_workspace.rstrip("/")
    model_map = _collect_data_from_metric_view(
        metric_view=metric_view,
        databricks_workspace=databricks_workspace,
        databricks_token=databricks_token,
    )

    df = list_connections()

    # Validate catalogs
    catalogs = {s["catalogName"] for s in model_map["sources"].values()}
    for source in sources:
        mirror = source.get("mirror")
        ws = source.get("workspace")
        if "mirror" not in source or "workspace" not in source:
            raise ValueError(
                f"{icons.warning} Skipping catalog '{source}' as it does not have a mirror and workspace defined in the sources."
            )
        mirror_workspace_id = resolve_workspace_id(workspace=ws)
        mirror_id = resolve_item_id(
            item=mirror,
            type="MirroredAzureDatabricksCatalog",
            workspace=mirror_workspace_id,
        )
        mirror_catalog = get_mirrored_azure_databricks_catalog(
            mirrored_azure_databricks_catalog=mirror_id,
            workspace=mirror_workspace_id,
            return_dataframe=False,
        )

        mirror_catalog_name = mirror_catalog.get("properties", {}).get("catalogName")
        mirror_connection_id = mirror_catalog.get("properties", {}).get(
            "databricksWorkspaceConnectionId"
        )

        df_filt = df[df["Connection Id"] == mirror_connection_id]
        if df_filt.empty:
            raise ValueError(
                f"{icons.red_dot} No connection found for mirror catalog '{mirror_catalog_name}'."
            )
        mirror_connection_workspace = df_filt["Connection Path"].iloc[0].rstrip("/")

        if mirror_connection_workspace != databricks_workspace:
            raise ValueError()
        if mirror_catalog_name not in catalogs:
            raise ValueError()

        for source, items in model_map.get("sources").items():
            cat = items.get("catalogName")
            if cat == mirror_catalog_name:
                model_map["sources"][source]["mirrorId"] = mirror_id
                model_map["sources"][source]["mirrorWorkspaceId"] = mirror_workspace_id

    for source, items in model_map["sources"].items():
        catalog_name = items.get("catalogName")
        mirror_id = items.get("mirrorId")
        mirror_workspace_id = items.get("mirrorWorkspaceId")
        if mirror_id is None or mirror_workspace_id is None:
            raise ValueError(
                f"{icons.red_dot} No mirror defined for source catalog '{catalog_name}'. Please ensure all catalogs have a corresponding mirror and workspace defined in the sources."
            )

    seen = set()
    mirrors = [
        {"catalogName": c, "mirrorId": m, "mirrorWorkspaceId": w}
        for items in model_map["sources"].values()
        for (c, m, w) in [
            (
                items.get("catalogName"),
                items.get("mirrorId"),
                items.get("mirrorWorkspaceId"),
            )
        ]
        if not ((c, m, w) in seen or seen.add((c, m, w)))
    ]

    for source, properties in model_map['sources'].items():
        mirror_id = properties.get('mirrorId')
        mirror_workspace_id = properties.get('mirrorWorkspaceId')
        table_name = properties.get('tableName')
        schema_name = properties.get('schemaName')
        path = create_abfss_path(
                lakehouse_id=mirror_id,
                lakehouse_workspace_id=mirror_workspace_id,
                delta_table_name=table_name,
                schema=schema_name,
            )

        df = list_columns_from_path(path=path)
        if df.empty:
            raise ValueError(
                f"{icons.red_dot} The table '{table_name}' does not exist in the source or has no columns."
            )

        # Add columns
        for _, row in df.iterrows():
            column_name = row["Column Name"]
            data_type = row["Data Type"]
            converted_data_type = convert_column_data_type(data_type)
            if converted_data_type is None:
                raise ValueError(
                    f"{icons.red_dot} The data type '{data_type}' of column '{column_name}' in table '{table_name}' is not supported."
                )
            if converted_data_type == "Binary":
                print(
                    f"{icons.warning} The column '{column_name}' in table '{table_name}' has data type 'Binary' which is not supported in Direct Lake semantic models. This column will be skipped."
                )
                continue
            model_map['sources'][source]["columns"].append(
                {"columnName": column_name, "dataType": converted_data_type}
            )

    # Generate semantic model
    model_id = create_blank_semantic_model(
        dataset=name, workspace=workspace_id, overwrite=False
    )

    @retry(
        sleep_time=1,
        timeout_error_message=f"{icons.red_dot} Function timed out after 1 minute",
    )
    def dyn_connect():
        with connect_semantic_model(
            dataset=model_id, readonly=True, workspace=workspace_id
        ) as tom:

            tom.model

    dyn_connect()
    with connect_semantic_model(
        dataset=model_id, workspace=workspace_id, readonly=False
    ) as tom:

        source_table_name = None

        # Add expressions
        for mirror in mirrors:
            catalog_name = mirror.get("catalogName")
            mirror_id = mirror.get("mirrorId")
            mirror_workspace_id = mirror.get("mirrorWorkspaceId")
            mirror_expr = generate_shared_expression(
                item=mirror_id,
                item_type="MirroredAzureDatabricksCatalog",
                workspace=mirror_workspace_id,
                use_sql_endpoint=False,
            )
            tom.add_expression(name=f"Mirror_{catalog_name}", expression=mirror_expr)

        for source, properties in model_map['sources'].items():

            table_name = properties.get('tableName')
            schema_name = properties.get('schemaName')
            catalog_name = properties.get('catalogName')
            is_source = properties.get('isSource')
            if is_source:
                source_table_name = table_name
            columns = properties.get('columns', [])
            mirror_expr_name = f"Mirror_{catalog_name}"

            tom.add_table(
                name=table_name,
            )
            tom.add_entity_partition(
                table_name=table_name,
                entity_name=table_name,
                expression=mirror_expr_name,
                schema_name=schema_name,
            )

            for column in columns:
                column_name = column.get("columnName")
                data_type = column.get("dataType")
                tom.add_data_column(
                    table_name=table_name,
                    column_name=column_name,
                    source_column=column_name,
                    data_type=data_type,
                    is_hidden=True,
                )

        for table_name, table_info in model_map["tables"].items():
            #catalog = table_info["catalogName"]
            #schema_name = table_info["schemaName"]
            #mirror_expr_name = f"Mirror_{catalog}"
            #tom.add_table(
            #    name=table_name,
            #)
            #tom.add_entity_partition(
            #    table_name=table_name,
            #    entity_name=table_name,
            #    expression=mirror_expr_name,
            #    schema_name=schema_name,
            #)

            for column in table_info.get("columns", []):
                column_name = column.get("columnName")
                source_column = column.get("sourceColumn")
                data_type = column.get("dataType")
                desc = column.get("description")
                # format = column.get("format")
                # synonyms = column.get("synonyms")
                c = tom.model.Tables[table_name].Columns[source_column]
                c.Name = column_name
                c.IsHidden = False
                c.Description = desc
                #tom.add_data_column(
                #    table_name=table_name,
                #    column_name=column_name,
                #    source_column=source_column,
                #    data_type=data_type,
                    # dformat_string=convert_format(format) if format else None,
                #    description=desc,
                #)

        # Add measures afterwards
        column_table_map = {{c.Name: c.Parent.Name} for c in tom.all_columns()}
        for table_name, table_info in model_map["tables"].items():
            for measure in table_info.get("measures", []):
                measure_name = measure.get("measureName")
                expr = measure.get("expression")
                desc = measure.get("description")
                dax = convert_sql_to_dax(sql=expr, column_table_map=column_table_map, default_table=source_table_name),
                # format = measure.get("format")
                #  synonyms = measure_info.get("synonyms")
                # converted_format = convert_format(format) if format else None
                tom.add_measure(
                    table_name=table_name,
                    measure_name=measure_name,
                    expression=dax,
                    # format_string=converted_format,
                    description=desc,
                )

        column_lookup = {c for c in tom.all_columns()}
        relationships = model_map.get("relationships", {})
        for name, properties in relationships.items():
            from_table = properties.get("from_table")
            from_source_column = properties.get("from_column")

            to_table = properties.get("to_table")
            to_source_column = properties.get("to_column")

            from_column = next(
                (
                    c
                    for c in column_lookup
                    if c.Parent.Name == from_table
                    and c.SourceColumn == from_source_column
                ),
                None,
            )
            to_column = next(
                (
                    c
                    for c in column_lookup
                    if c.Parent.Name == to_table and c.SourceColumn == to_source_column
                ),
                None,
            )

            if not to_column:
                from_column_data_type = next(
                    (
                        c.DataType
                        for c in column_lookup
                        if c.Parent.Name == from_table and c.SourceColumn == from_source_column
                    ),
                    None,
                )
                to_column = tom.add_data_column(
                    table_name=to_table,
                    column_name=to_source_column,
                    source_column=to_source_column,
                    data_type=str(from_column_data_type),
                )

            if not from_column or not to_column:
                raise ValueError(
                    f"Column not found for relationship '{name}': "
                    f"{from_table}.{from_source_column} -> {to_table}.{to_source_column}"
                )

            # Hide key columns
            from_column.IsHidden = True
            to_column.IsHidden = True

            tom.add_relationship(
                from_table=from_table,
                from_column=from_column.Name,
                to_table=to_table,
                to_column=to_column.Name,
                from_cardinality="Many",
                to_cardinality="One",
            )

    if refresh:
        refresh_semantic_model(dataset=model_id, workspace=workspace_id)


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
