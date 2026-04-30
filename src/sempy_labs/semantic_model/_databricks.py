from typing import Optional, List
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
from sempy_labs._databricks import (
    list_databricks_metric_views,
    list_databricks_tables,
)
import sempy_labs._icons as icons
from sempy_labs.mirrored_azure_databricks_catalog._items import (
    get_mirrored_azure_databricks_catalog,
)
from sempy._utils._log import log
from sempy_labs.connection._items import list_connections
from sempy_labs._refresh_semantic_model import refresh_semantic_model


def _validate_metric_view_format(metric_view: str):
    parts = metric_view.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Invalid metric_view format: '{metric_view}' (expected 'catalog.schema.metric')"
        )
    return parts


def _validate_metric_view_exists(
    catalog: str,
    schema: str,
    metric_view_name: str,
    databricks_workspace: str,
    databricks_token: str,
):

    mvs = list_databricks_metric_views(
        databricks_workspace=databricks_workspace,
        unity_catalog=catalog,
        schema=schema,
        databricks_token=databricks_token,
    )
    # Find the first matching metric view
    mv = next((mv for mv in mvs if mv.get("Name") == metric_view_name), None)

    if not mv:
        raise ValueError(
            f"{icons.red_dot} The '{metric_view_name}' metric view does not exist or could not be found."
        )
    return mv


def _validate_tables(model_map: dict, databricks_workspace: str, databricks_token: str):

    table_names = [s["tableName"] for s in model_map["sources"].values()]

    duplicates = {t for t in table_names if table_names.count(t) > 1}
    if duplicates:
        raise ValueError(f"Duplicate table names found: {duplicates}")

    # Check table type
    for source, properties in model_map["sources"].items():
        cat = properties.get("catalogName")
        sch = properties.get("schemaName")
        tbl = properties.get("tableName")
        df = list_databricks_tables(
            databricks_workspace=databricks_workspace,
            databricks_token=databricks_token,
            unity_catalog=cat,
            schema=sch,
            table_name=tbl,
        )
        if df.empty:
            raise ValueError(
                f"{icons.red_dot} The '{source}' table was not found in the source."
            )
        data_source_format = df["Data Source Format"].iloc[0]
        table_type = df["Table Type"].iloc[0]
        if data_source_format != "DELTA":
            raise ValueError(
                f"{icons.red_dot} Only delta tables are supported. The '{source}' table is a '{table_type}' which is not in delta format and is not supported."
            )


def _extract_table_name(expression: str, model_map: dict) -> str | None:
    parts = expression.split(".")

    # Must be exactly "xxx.yyyy"
    if len(parts) != 2:
        return None

    left_part, _ = parts

    for _, properties in model_map["sources"].items():
        table_name = properties.get("tableName")
        source_name = properties.get("sourceName")
        if left_part == source_name:
            return table_name

    return None


def _collect_data_from_metric_view(
    metric_view: str,
    databricks_workspace: str,
    databricks_token: str,
):
    """
    Generates a model map of sources, tables, columns, measures, relationships based on a Databricks Metric View.
    """

    catalog, schema, metric_view_name = _validate_metric_view_format(metric_view)

    mv = _validate_metric_view_exists(
        catalog=catalog,
        schema=schema,
        metric_view_name=metric_view_name,
        databricks_workspace=databricks_workspace,
        databricks_token=databricks_token,
    )

    # Safely extract definition and columns
    definition = mv.get("View Definition")
    objects = mv.get("Columns")
    source = definition.get("source")
    dimensions = definition.get("dimensions", [])
    joins = definition.get("joins", [])
    source_catalog, source_schema, source_table = source.split(".")

    model_map = {"sources": {}, "relationships": {}, "tables": {}}

    # Add primary source to model map
    model_map["sources"][source] = {
        "catalogName": source_catalog,
        "schemaName": source_schema,
        "tableName": source_table,
        "sourceName": "source",
        "isSource": True,
        "mirrorId": None,
        "mirrorWorkspaceId": None,
        "columns": [],
        "measures": [],
    }

    # Determine relationships
    for idx, join in enumerate(joins):
        join_name = join.get("name")
        join_source = join.get("source")
        join_on = join.get("on", {})
        join_using = join.get("using", [])

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
            "measures": [],
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
            from_catalog, from_schema, from_table = (
                source_catalog,
                source_schema,
                source_table,
            )
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

    _validate_tables(
        model_map=model_map,
        databricks_workspace=databricks_workspace,
        databricks_token=databricks_token,
    )

    # ✅ Build tables map
    for s in model_map["sources"].values():
        table_name = s["tableName"]

        model_map["tables"][table_name] = {
            "catalogName": s["catalogName"],
            "schemaName": s["schemaName"],
            "columns": [],
            "measures": [],
        }

    sources = {
        s.get("sourceName"): s.get("tableName") for s in model_map["sources"].values()
    }

    source_table = next(
        (s.get("tableName") for s in model_map["sources"].values() if s["isSource"]),
        None,
    )

    def get_table(expr, alias_map):
        import re

        matches = re.findall(r"\b(\w+)\.", expr)
        tables = [alias_map[m] for m in matches if m in alias_map]

        return tables[0] if len(tables) == 1 else tables

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
        is_calc_column = False

        if obj_type == "dimension":
            table_name = _extract_table_name(
                expression=obj_expression, model_map=model_map
            )
            if not table_name:
                print(
                    f"{icons.warning} Skipping the '{display_name}' column as it could not be determined from the expression."
                )
                table_name = get_table(obj_expression, sources)
                is_calc_column = True
                source_column = display_name.replace(" ", "")
            else:
                source_column = obj_expression.split(".")[1]

            model_map["tables"][table_name]["columns"].append(
                {
                    "columnName": display_name,
                    "sourceColumn": source_column,
                    "dataType": convert_column_data_type(data_type),
                    "format": format,
                    "description": description,
                    "expression": obj_expression,
                    "synonyms": synonyms,
                    "isCalcColumn": is_calc_column,
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
    test_run: bool = False,
):
    """
    Creates a Direct Lake semantic model based on an Azure Databricks metric view. Before running this function, ensure that the Mirrored Azure Databricks Catalog(s) have been set up in Fabric for all catalogs referenced by the metric view.

    Limitations:
        * Calculated columns are ignored and not added to the semantic model
        * Measures based on calculated columns are ignored and not added to the semantic model.
        * Relationships based on multiple columns per table are not supported.
        * Metric Views which are based on tables which are not in Delta format are not supported. As such, Materialized Views and Streaming Tables are not supported.
        * Metric Views use SQL for measure expressions. This is converted to DAX. Not all measures may properly convert to DAX. Complex expressions may not convert properly and may require manual adjustment after the semantic model is generated.

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
                        "mirror": "MirrorA", # Name or ID of the Mirrored Azure Databricks Catalog
                        "workspace": None, # Name or ID of the workspace where the Mirrored Azure Databricks Catalog is located. If None, it will resolve to the workspace of the attached lakehouse or if no lakehouse attached, resolves to the workspace of the notebook.
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
    refresh : bool, default=True
        If True, refreshes the semantic model upon creation.
    test_run : bool, default=False
        If True, does not create the semantic model. It returns the blueprint for creating the semantic model.
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
                f"{icons.red_dot} No connection found for the mirrored catalog from the '{source}' source."
            )
        mirror_connection_workspace = df_filt["Connection Path"].iloc[0].rstrip("/")

        if mirror_connection_workspace != databricks_workspace:
            raise ValueError(
                f"{icons.red_dot} The mirrored catalog from the '{source}' source is connected to a different Databricks workspace than the one specified in the function parameters. Please ensure the mirrored catalog is connected to the correct Databricks workspace or update the databricks_workspace parameter to match the workspace of the mirrored catalog. Mirror catalog workspace: {mirror_connection_workspace}, specified databricks_workspace: {databricks_workspace}."
            )
        if mirror_catalog_name not in catalogs:
            raise ValueError(
                f"{icons.red_dot} The mirrored catalog '{mirror_catalog_name}' from the '{source}' source is not found in the list of catalogs."
            )

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

    source_table_name = None
    source_full_table_name = None
    for source, properties in model_map["sources"].items():
        mirror_id = properties.get("mirrorId")
        mirror_workspace_id = properties.get("mirrorWorkspaceId")
        table_name = properties.get("tableName")
        schema_name = properties.get("schemaName")
        is_source = properties.get("isSource")
        source_name = properties.get("sourceName")
        if is_source:
            source_table_name = table_name
            source_full_table_name = source
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

        # Get columns for the specific table directly
        cols = model_map.get("tables", {}).get(table_name, {}).get("columns", [])

        # Index columns by sourceColumn for O(1) lookup
        cols_lookup = {c.get("sourceColumn"): c for c in cols}

        # Add columns
        for _, row in df.iterrows():
            source_column = row["Column Name"]
            data_type = row["Data Type"]
            converted_data_type = convert_column_data_type(data_type)
            col_prop = cols_lookup.get(source_column)

            is_hidden = col_prop is None
            desc = col_prop.get("description") if col_prop else None
            column_name = (
                col_prop.get("columnName")
                if col_prop and col_prop.get("columnName")
                else source_column
            )
            synonyms = col_prop.get("synonyms", []) if col_prop else []

            if converted_data_type is None:
                raise ValueError(
                    f"{icons.red_dot} The data type '{data_type}' of column '{source_column}' in table '{table_name}' is not supported."
                )
            if converted_data_type == "Binary":
                print(
                    f"{icons.warning} The column '{source_column}' in table '{table_name}' has data type 'Binary' which is not supported in Direct Lake semantic models. This column will be skipped."
                )
                continue

            def fixed_column_name(column_name: str):
                if " " in column_name:
                    return f"`{column_name}`"
                else:
                    return column_name

            model_map["sources"][source]["columns"].append(
                {
                    "columnName": column_name,
                    "dataType": converted_data_type,
                    "sourceColumn": source_column,
                    "isHidden": is_hidden,
                    "description": desc,
                    "synonyms": synonyms,
                    "fullSourceColumnName": (
                        f"{source_name}.{fixed_column_name(source_column)}"
                        if source_name
                        else source_column
                    ),
                    "fullColumnName": f"{table_name}.{fixed_column_name(column_name)}",
                }
            )

    # Add measures
    column_map = {}
    for source, prop in model_map["sources"].items():
        columns = prop.get("columns")
        table_name = prop.get("tableName")
        for c in columns:
            column_name = c.get("columnName")
            is_hidden = c.get("isHidden")
            full_source_column_name = c.get("fullSourceColumnName")
            full_column_name = c.get("fullColumnName")
            fcn = f"'{table_name}'[{column_name}]"
            if not is_hidden:
                column_map[column_name] = fcn
                column_map[full_source_column_name] = fcn
                column_map[full_column_name] = fcn

    for source, prop in model_map["sources"].items():
        columns = prop.get("columns")
        table_name = prop.get("tableName")
        for c in columns:
            column_name = c.get("columnName")
            is_hidden = c.get("isHidden")
            full_source_column_name = c.get("fullSourceColumnName")
            full_column_name = c.get("fullColumnName")
            fcn = f"'{table_name}'[{column_name}]"
            lower_keys = {k.lower() for k in column_map}
            if column_name.lower() not in lower_keys:
                column_map[column_name] = fcn
                column_map[full_source_column_name] = fcn
                column_map[full_column_name] = fcn

    calc_columns = []
    for table_name, prop in model_map["tables"].items():
        columns = prop.get("columns", [])
        for c in columns:
            column_name = c.get("columnName")
            source_column = c.get("sourceColumn")
            fcn = f"'{table_name}'[{column_name}]"
            if c.get("isCalcColumn"):
                calc_columns.append(fcn)
                column_map[column_name] = fcn
                column_map[source_column] = fcn
                column_map[f"{table_name}.{source_column}"] = fcn

    for _, prop in model_map["tables"].items():
        measures = prop.get("measures", [])
        for m in measures:
            m_name = m.get("measureName")
            desc = m.get("description")
            expr = m.get("expression")
            syn = m.get("synonyms", [])
            is_hidden = False
            dax = convert_sql_to_dax(
                sql=expr,
                column_map=column_map,
                default_table=source_table_name,
            )
            for c in calc_columns:
                if c in dax:
                    dax = f""" /* NOT SUPPORTED AS THIS MEASURE USES A CALCULATED COLUMN. {dax} */ """
                    is_hidden = True
                    break
            model_map["sources"][source_full_table_name]["measures"].append(
                {
                    "measureName": m_name,
                    "description": desc,
                    "expression_sql": expr,
                    "expression_dax": dax,
                    "synonyms": syn,
                    "isHidden": is_hidden,
                }
            )

    # Delete tables from model map as they are not needed for model generation and to avoid confusion. The necessary table information has already been incorporated into the sources.
    del model_map["tables"]

    if test_run:
        return model_map

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

        for source, properties in model_map["sources"].items():

            table_name = properties.get("tableName")
            schema_name = properties.get("schemaName")
            catalog_name = properties.get("catalogName")
            is_source = properties.get("isSource")
            if is_source:
                source_table_name = table_name
            columns = properties.get("columns", [])
            measures = properties.get("measures", [])
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
                source_column = column.get("sourceColumn")
                description = column.get("description")
                is_hidden = column.get("isHidden")
                tom.add_data_column(
                    table_name=table_name,
                    column_name=column_name,
                    source_column=source_column,
                    data_type=data_type,
                    hidden=is_hidden,
                    description=description,
                )

            for measure in measures:
                tom.add_measure(
                    table_name=table_name,
                    measure_name=measure.get("measureName"),
                    expression=measure.get("expression_dax"),
                    description=measure.get("description"),
                    is_hidden=measure.get("isHidden"),
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

            # if not to_column:
            #    from_column_data_type = next(
            #        (
            #            c.DataType
            #            for c in column_lookup
            #            if c.Parent.Name == from_table
            #            and c.SourceColumn == from_source_column
            #        ),
            #        None,
            #    )
            #    to_column = tom.add_data_column(
            #        table_name=to_table,
            #        column_name=to_source_column,
            #        source_column=to_source_column,
            #        data_type=str(from_column_data_type),
            #    )

            # if not from_column or not to_column:
            #    raise ValueError(
            #        f"Column not found for relationship '{name}': "
            #        f"{from_table}.{from_source_column} -> {to_table}.{to_source_column}"
            #    )

            # Hide key columns
            # from_column.IsHidden = True
            # to_column.IsHidden = True

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

    return model_map
