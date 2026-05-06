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
    convert_format_from_databricks,
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

    table_names = [s["tableName"] for s in model_map["tables"]]

    duplicates = {t for t in table_names if table_names.count(t) > 1}
    if duplicates:
        raise ValueError(f"Duplicate table names found: {duplicates}")

    # Check table type
    for t in model_map["tables"]:
        source_object = t.get('sourceObject')
        cat, sch, tbl = source_object.split('.')
        df = list_databricks_tables(
            databricks_workspace=databricks_workspace,
            databricks_token=databricks_token,
            unity_catalog=cat,
            schema=sch,
            table_name=tbl,
        )
        if df.empty:
            raise ValueError(
                f"{icons.red_dot} The '{source_object}' table was not found in the source."
            )
        data_source_format = df["Data Source Format"].iloc[0]
        table_type = df["Table Type"].iloc[0]
        if data_source_format != "DELTA":
            raise ValueError(
                f"{icons.red_dot} Only delta tables are supported. The '{source_object}' table is a '{table_type}' which is not in delta format and is not supported."
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


def _add_table_to_model_map(source_object: str, model_map: dict, is_source = False, source_name = 'source', source_list = None) -> dict:

    # Check if table already exists in model map

    catalog, _, table = source_object.split(".")

    source = next((s for s in source_list if s.get('catalogName') == catalog), None)
    if not source:
        raise ValueError(
            f"{icons.red_dot} No mirror found for catalog '{catalog}' in the sources list. Please ensure all catalogs have a corresponding mirror and workspace defined in the sources."
        )
    source_item_id, source_workspace_id = source.get("itemId"), source.get("workspaceId")

    model_map['tables'].append({
        "tableName": table,
        "sourceObject": source_object,
        "isSource": is_source,
        "sourceName": source_name,
        "sourceItemId": source_item_id,
        "sourceWorkspaceId": source_workspace_id,
        "columns": [],
        "measures": [],
    })

    return model_map


def _create_model_map(
    metric_view: str,
    databricks_workspace: str,
    databricks_token: str,
    sources: dict | List[dict],
) -> dict:
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
    objects = mv.get("Objects", [])
    source_object = definition.get("source")
    joins = definition.get("joins", [])
    source_catalog, source_schema, source_table = source_object.split(".")

    # Collect object properties
    object_list = []
    for o in objects:
        name = o.get('name')
        metadata = json.loads(o.get('type_json', {})).get('metadata', {})
        format = o.get('metadata', {}).get('format')
        object_type = metadata.get('metric_view.type')
        synonyms = o.get('metadata', {}).get('synonyms', [])
        description = o.get('comment')
        display_name = o.get('metadata', {}).get('display_name') or name
        obj_expression = metadata.get("metric_view.expr")

        object_list.append({
            "name": name,
            "type": object_type,
            "displayName": display_name,
            "expression": obj_expression,
            "description": description,
            "format": format,
            "dataType": o.get('type_text'),
            "synonyms": synonyms,
            "identified": False,
            "tableName": None,
        })


    # Map sources to IDs
    df_conn = list_connections()

    if isinstance(sources, dict):
        sources = [sources]

    source_list = []
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

        df_filt = df_conn[df_conn["Connection Id"] == mirror_connection_id]
        if df_filt.empty:
            raise ValueError(
                f"{icons.red_dot} No connection found for the mirrored catalog from the '{source}' source."
            )
        mirror_connection_workspace = df_filt["Connection Path"].iloc[0].rstrip("/")

        if mirror_connection_workspace != databricks_workspace:
            raise ValueError(
                f"{icons.red_dot} The mirrored catalog from the '{source}' source is connected to a different Databricks workspace than the one specified in the function parameters. Please ensure the mirrored catalog is connected to the correct Databricks workspace or update the databricks_workspace parameter to match the workspace of the mirrored catalog. Mirror catalog workspace: {mirror_connection_workspace}, specified databricks_workspace: {databricks_workspace}."
            )
        source_list.append({
            "itemId": mirror_id,
            "workspaceId": mirror_workspace_id,
            "catalogName": mirror_catalog_name,
        })

    # Establish model map
    model_map = {"tables": [], "relationships": []}

    # Add source table to model map    
    model_map = _add_table_to_model_map(source_object=source_object, model_map=model_map, is_source=True, source_name='source', source_list=source_list)

    # Determine relationships and add joined tables to model map
    for join in joins:
        join_name = join.get("name")
        join_source = join.get("source")
        join_on = join.get("on", {})
        join_using = join.get("using", [])

        catalog, schema, table = join_source.split(".")

        model_map = _add_table_to_model_map(source_object=join_source, model_map=model_map, is_source=False, source_name=join_name, source_list=source_list)

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

            alias_to_source = {"source": source_object, join_name: join_source}

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

        model_map['relationships'].append({
            "from_table": from_table,
            "from_column": from_column,
            "to_table": to_table,
            "to_column": to_column,
        })

    _validate_tables(
        model_map=model_map,
        databricks_workspace=databricks_workspace,
        databricks_token=databricks_token,
    )

    cols = [o for o in object_list if o.get('type') == 'dimension']
    measures = [o for o in object_list if o.get('type') == 'measure']

    # Add columns to model map
    for t in model_map.get('tables', []):
        #print(t)
        src_obj = t.get('sourceObject')
        src_name = t.get('sourceName')
        #print(src_name)
        src_item_id = t.get("sourceItemId")
        src_workspace_id = t.get("sourceWorkspaceId")
        t_name = t.get('tableName')
        
        sch_name = src_obj.split('.')[1]
  
        path = create_abfss_path(
                lakehouse_id=src_item_id,
                lakehouse_workspace_id=src_workspace_id,
                delta_table_name=t_name,
                schema=sch_name,
            )

        df_table = list_columns_from_path(path=path)
        if df_table.empty:
            raise ValueError(
                f"{icons.red_dot} The table '{t_name}' does not exist in the source or has no columns."
            )
        
        def fix_source_column(column_name: str):
            if " " in column_name:
                return f"`{column_name}`"
            else:
                return column_name
        # Add columns
        for _, row in df_table.iterrows():
            source_column = row["Column Name"]
            data_type = row["Data Type"]
            converted_data_type = convert_column_data_type(data_type)

            if converted_data_type is None:
                raise ValueError(
                    f"{icons.red_dot} The data type '{data_type}' of column '{source_column}' in table '{t_name}' is not supported."
                )
            if converted_data_type == "Binary":
                print(
                    f"{icons.warning} The column '{source_column}' in table '{t_name}' has data type 'Binary' which is not supported in Direct Lake semantic models. This column will be skipped."
                )
                continue
            column_name = source_column
            expression, description, format_string = None, None, None
            fixed_source_column = fix_source_column(source_column)
            is_hidden = True
            for col in cols:
                if col.get('expression') == f"{src_name}.{fixed_source_column}":
                    column_name = col.get('displayName') or source_column
                    expression = col.get('expression')
                    format_string = col.get('format')
                    description = col.get('description')
                    is_hidden = False
                    col['identified'] = True
                    break

            fixed_column_name = fix_source_column(column_name)
            t['columns'].append({
                "columnName": column_name,
                "sourceColumn": source_column,
                "fixedSourceColumn": fixed_source_column,
                "expression": expression,
                "dataType": converted_data_type,
                "isHidden": is_hidden,
                "description": description,
                "format_dbx": format_string,
                "format_pbi": convert_format_from_databricks(format_string),
                "isCalcColumn": False,
                "daxFullObjectName": f"'{t_name}'[{column_name}]",
                "columnReferences": list(set([f"{src_name}.{fixed_source_column}", fixed_source_column, fixed_column_name])),
            })
    
    # Add calculated columns
    calc_columns = []
    for col in cols:
        if not col.get('identified'):
            print(f"{icons.warning} Skipping the '{col.get('name')}' column as calculated columns are not supported.")
            
            expr = col.get('expression') or ""
            source_column = col.get('name')
            column_name = col.get('displayName') or source_column
            fixed_source_column = fix_source_column(source_column)
            tbl_name = None
            calc_columns.append(fixed_source_column)

            # Find matching table
            for t in model_map['tables']:
                for t_col in t.get('columns', []):
                    full_ref = f"{t.get('sourceName')}.{t_col.get('fixedSourceColumn')}"
                    if full_ref in expr:
                        tbl_name = t.get('tableName')
                        break
                if tbl_name:
                    break  # <-- break outer loop too

            if not tbl_name:
                print(f"{icons.warning} Could not determine table for column '{column_name}'")
                continue

            # Append calculated column
            for t in model_map['tables']:
                src_name = t.get('sourceName')
                if t['tableName'] == tbl_name:
                    t.setdefault('columns', []).append({
                        "columnName": column_name,
                        "sourceColumn": col.get('name'),
                        "expression": col.get('expression'),
                        "dataType": convert_column_data_type(col.get('dataType')),
                        "isHidden": False,
                        "description": col.get('description'),
                        "format_dbx": col.get('format'),
                        "format_pbi": convert_format_from_databricks(col.get('format')),
                        "isCalcColumn": True,
                        "daxFullObjectName": f"'{tbl_name}'[{column_name}]",
                        "columnReferences": list(set([f"{src_name}.{fixed_source_column}", fixed_source_column])),
                    })
                    break

    # Create column map for DAX references
    column_map = {}
    for t in model_map['tables']:
        for c in t['columns']:
            fcn = c['daxFullObjectName']
            for ref in c.get('columnReferences', []):
                column_map[ref] = fcn

    # Add measures
    for t in model_map['tables']:
        if t.get('isSource'):
            for m in measures:
                measure_name = m.get('displayName') or m.get('name')
                expression = m.get('expression')
                dax = convert_sql_to_dax(sql=expression, column_map=column_map, default_table=t.get('tableName'))
                for calc_column in calc_columns:
                    if calc_column in expression:
                        dax = f"""/* NOT SUPPORTED AS THIS MEASURE USES A CALCULATED COLUMN. {dax} */ """
                        break
                format = m.get('format')
                is_hidden = False
                t['measures'].append({
                    "measureName": measure_name,
                    "description": m.get('description'),
                    "expression_sql": expression,
                    "expression_dax": dax,
                    "isHidden": is_hidden,
                    "format_dbx": format,
                    "format_pbi": convert_format_from_databricks(format),
                    "synonyms": m.get('synonyms', []),
                })
    
    return model_map

    # Set Roles according to DBX permissions
    #catalog, schema, table = metric_view.split('.')

    #if infer_row_level_security:
    #    df_perm = list_permissions(object=metric_view, databricks_workspace=databricks_workspace, databricks_token=databricks_token)
    #    view_permissions = df_perm[df_perm['Privilege'].isin(['SELECT'])]['Principal'].tolist()
    #    df_perm = list_permissions(object=f"{catalog}.{schema}", databricks_workspace=databricks_workspace, databricks_token=databricks_token)
    #    schema_permissions = df_perm[df_perm['Privilege'].isin(['USE_SCHEMA'])]['Principal'].tolist()
    #    df_perm = list_permissions(object=catalog, databricks_workspace=databricks_workspace, databricks_token=databricks_token)
    #    catalog_permissions = df_perm[df_perm['Privilege'].isin(['USE_CATALOG'])]['Principal'].tolist()

    #    inferred_permissions = set(view_permissions) | set(schema_permissions) | set(catalog_permissions)


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
        * Calculated columns are ignored and not added to the semantic model.
        * Measures based on calculated columns are ignored and not added to the semantic model.
        * Relationships based on multiple columns per table are not supported.
        * Metric Views which are based on tables which are not in Delta format are not supported. As such, Materialized Views and Streaming Tables are not supported.
        * Metric Views use SQL for measure expressions. This is converted to DAX. Complex expressions may not convert properly and may require manual adjustment after the semantic model is generated.

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

    databricks_workspace = databricks_workspace.rstrip("/")
    model_map = _create_model_map(
        metric_view=metric_view,
        databricks_workspace=databricks_workspace,
        databricks_token=databricks_token,
        sources=sources,
    )

    if test_run:
        return model_map

    # Extract list of mirrors
    seen = set()
    mirrors = []

    for t in model_map['tables']:
        key = (t.get('sourceItemId'), t.get('sourceWorkspaceId'))
        
        if key not in seen:
            seen.add(key)
            mirrors.append({
                "mirrorId": key[0],
                "workspaceId": key[1],
            })

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

        # Add expressions
        for t in model_map['tables']:
            tbl_name = t.get('tableName')
            mirror_id = t.get('sourceItemId')
            mirror_workspace_id = t.get('sourceWorkspaceId')
            catalog_name, schema_name, t_name = t.get('sourceObject').split('.')
            mirror_expr = generate_shared_expression(
                item=mirror_id,
                item_type="MirroredAzureDatabricksCatalog",
                workspace=mirror_workspace_id,
                use_sql_endpoint=False,
            )
            expr_name = f"Mirror_{catalog_name}"
            if not any(e for e in tom.model.Expressions if e.Name == expr_name):
                tom.add_expression(name=expr_name, expression=mirror_expr)

            tom.add_table(name=tbl_name)
            tom.add_entity_partition(
                table_name=tbl_name,
                entity_name=tbl_name,
                expression=expr_name,
                schema_name=schema_name,
            )

            for column in t.get('columns', []):
                column_name = column.get("columnName")
                data_type = column.get("dataType")
                source_column = column.get("sourceColumn")
                description = column.get("description")
                is_hidden = column.get("isHidden")
                format_string = column.get("format_pbi")
                if column.get('isCalcColumn', False) == False:
                    tom.add_data_column(
                        table_name=tbl_name,
                        column_name=column_name,
                        source_column=source_column,
                        data_type=data_type,
                        hidden=is_hidden,
                        description=description,
                        format_string=format_string,
                    )

            for measure in t.get('measures', []):
                tom.add_measure(
                    table_name=tbl_name,
                    measure_name=measure.get("measureName"),
                    expression=measure.get("expression_dax"),
                    description=measure.get("description"),
                    hidden=measure.get("isHidden"),
                    format_string=measure.get("format_pbi"),
                )

        column_lookup = {c for c in tom.all_columns()}
        relationships = model_map.get("relationships", {})
        for r in relationships:
            from_table = r.get("from_table")
            from_source_column = r.get("from_column")
            to_table = r.get("to_table")
            to_source_column = r.get("to_column")

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

            tom.add_relationship(
                from_table=from_table,
                from_column=from_column.Name,
                to_table=to_table,
                to_column=to_column.Name,
                from_cardinality="Many",
                to_cardinality="One",
            )

            tom.hide_key_columns()
            tom.mark_primary_keys()

    if refresh:
        refresh_semantic_model(dataset=model_id, workspace=workspace_id)

    return model_map
