import json
import re
import yaml
from uuid import UUID
from typing import List, Optional, Any, Dict, Literal
from sempy_labs.semantic_model._helper import convert_sql_to_dax
from sempy_labs._helper_functions import (
    resolve_item_id,
    resolve_workspace_id,
)
from sempy_labs._snowflake import (
    list_snowflake_columns,
)
from sempy._utils._log import log
from sempy_labs.semantic_model._convert_model_map import convert_model_map_to_bim
from sempy_labs._generate_semantic_model import (
    create_blank_semantic_model,
    create_semantic_model_from_bim,
)


def _get_synonyms(node: Optional[dict]) -> List[str]:
    """Extract synonyms from a Snowflake semantic view node."""
    if not node:
        return []
    syns = node.get("synonyms") or []
    return [s for s in syns if isinstance(s, str)]


def _build_source_name(base_table: Optional[dict]) -> str:
    """Build a fully qualified ``database.schema.table`` source name."""
    if not base_table:
        return ""
    parts = [
        base_table.get("database", "") or "",
        base_table.get("schema", "") or "",
        base_table.get("table", "") or "",
    ]
    return ".".join([p for p in parts if p])


def _resolve_metric_table(expression: str, table_names: List[str]) -> Optional[str]:
    """Heuristically find the table a metric expression refers to by looking
    for ``table_name.`` references in the expression."""
    if not expression:
        return None
    for tbl in table_names:
        if re.search(rf"\b{re.escape(tbl)}\.", expression):
            return tbl
    return None


# Mapping of Snowflake data types to Power BI data types. Keys are normalized
# to upper-case base type names (no parameters/precision/scale).
# Reference:
#   * https://docs.snowflake.com/en/sql-reference/data-types
#   * https://learn.microsoft.com/analysis-services/tabular-models/data-types-supported-ssas-tabular
_SNOWFLAKE_TO_PBI_DATA_TYPE: Dict[str, str] = {
    # Numeric / fixed-point
    "NUMBER": "Decimal",
    "DECIMAL": "Decimal",
    "NUMERIC": "Decimal",
    "INT": "Int64",
    "INTEGER": "Int64",
    "BIGINT": "Int64",
    "SMALLINT": "Int64",
    "TINYINT": "Int64",
    "BYTEINT": "Int64",
    # Floating-point
    "FLOAT": "Double",
    "FLOAT4": "Double",
    "FLOAT8": "Double",
    "DOUBLE": "Double",
    "DOUBLE PRECISION": "Double",
    "REAL": "Double",
    # String
    "VARCHAR": "String",
    "CHAR": "String",
    "CHARACTER": "String",
    "STRING": "String",
    "TEXT": "String",
    # Boolean
    "BOOLEAN": "Boolean",
    "BOOL": "Boolean",
    # Date / time
    "DATE": "DateTime",
    "DATETIME": "DateTime",
    "TIME": "DateTime",
    "TIMESTAMP": "DateTime",
    "TIMESTAMP_LTZ": "DateTime",
    "TIMESTAMP_NTZ": "DateTime",
    "TIMESTAMP_TZ": "DateTime",
    # Binary
    "BINARY": "Binary",
    "VARBINARY": "Binary",
    # Semi-structured / other (best-effort)
    "VARIANT": "String",
    "OBJECT": "String",
    "ARRAY": "String",
    "GEOGRAPHY": "String",
    "GEOMETRY": "String",
}


# Note added to measures which cannot be converted because they reference a
# calculated column (calculated columns are not supported by the conversion).
_CALCULATED_DEPENDENCY_NOTE = (
    "This measure could not be converted because it depends on the following "
    "calculated column(s) which are not supported: {columns}."
)


def _convert_snowflake_data_type(data_type: Optional[str]) -> str:
    """Convert a Snowflake data type string to its Power BI equivalent.

    Strips any parameters (e.g. ``NUMBER(10,2)`` -> ``NUMBER``) before
    looking up the mapping. Returns an empty string if ``data_type`` is
    falsy and ``"String"`` as a safe default for unrecognized types.
    """
    if not data_type:
        return ""
    base = re.split(r"[\(\s]", str(data_type).strip(), maxsplit=1)[0].upper()
    return _SNOWFLAKE_TO_PBI_DATA_TYPE.get(base, "String")


@log
def convert_from_snowflake(
    yaml_file: str,
    account: str,
    token: str,
    source_item: str | UUID,
    source_type: Literal["Lakehouse", "Warehouse"] = "Lakehouse",
    source_workspace: Optional[str | UUID] = None,
    semantic_model_workspace: Optional[str | UUID] = None,
    semantic_model_name: Optional[str] = None,
    test_run: bool = True,
) -> dict:
    """
    Converts a Snowflake semantic view YAML definition into the a Power BI semantic model in Direct Lake mode.

    Limitations:
        * Calculated columns (fields whose 'expr' is not a plain column reference) are not supported. They are
          excluded from the semantic model and are instead documented as annotations on their table.
        * Metrics which reference a calculated column cannot be converted. Such measures are created with an
          expression of ``BLANK()`` and a note explaining why the conversion was not possible.

    Parameters
    ----------
    yaml_file : str
        A YAML string containing a Snowflake semantic view definition (see here `<https://docs.snowflake.com/en/user-guide/views-semantic/semantic-view-yaml-spec>`_ for the specification).
    account : str
        The Snowflake account identifier. For example: XXXXXXX-XXX00000.snowflakecomputing.com
    token : str
        The authentication token for the Snowflake account.
    source_item : str | uuid.UUID
        The Fabric source item ID or name.
    source_type : typing.Literal["Lakehouse", "Warehouse"], default="Lakehouse"
        The type of the source item.
    source_workspace : str | uuid.UUID, default=None
        The source workspace name or ID. If not provided, defaults to the workspace of the attached lakehouse or the workspace of the notebook.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    semantic_model_workspace : str | uuid.UUID, default=None
        The workspace name or ID for the Power BI semantic model. If not provided, defaults to the workspace of the attached lakehouse or the workspace of the notebook.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    semantic_model_name : typing.Optional[str], default=None
        The name of the Power BI semantic model. If not provided, defaults to the name specified in the YAML file.
    test_run : bool, default=True
        If True, the conversion will be performed in test mode without making any changes to the actual Power BI semantic model.

    Returns
    -------
    dict
        The model.bim file of the Power BI semantic model.
    """

    if hasattr(yaml_file, "read"):
        data = yaml.safe_load(yaml_file)
    else:
        data = yaml.safe_load(yaml_file)

    data = data or {}

    source_workspace_id = resolve_workspace_id(source_workspace)
    semantic_model_workspace_id = resolve_workspace_id(semantic_model_workspace)
    source_item_id = resolve_item_id(
        item=source_item, type=source_type, workspace=source_workspace_id
    )

    model_name = semantic_model_name or data.get("name", "")
    if model_name is None:
        raise ValueError(
            "Semantic model name must be provided either as an argument or in the YAML file."
        )
    model_description = data.get("description", "") or ""

    sf_tables = data.get("tables") or []
    if not sf_tables:
        raise ValueError("Snowflake semantic view does not contain a 'tables' entry.")

    table_names = [t.get("name", "") for t in sf_tables if t.get("name")]

    # Pre-build a minimal relationships list (fromTable / toTable only) so it
    # can be passed to ``convert_sql_to_dax`` while generating measure DAX.
    # The full relationships list (with column info) is built later for the
    # output payload.
    rel_hints: List[Dict[str, str]] = []
    for rel in data.get("relationships", []) or []:
        ft = rel.get("left_table", "") or ""
        tt = rel.get("right_table", "") or ""
        if ft and tt:
            rel_hints.append({"fromTable": ft, "toTable": tt})

    # Build column maps for use by ``convert_sql_to_dax``:
    #   * ``column_map`` (global): maps ``table.column`` and bare ``column``
    #     references (using the field name, the source column, and the raw
    #     ``expr`` identifier) to the DAX form ``'table'[column]``.
    #   * ``per_table_bare`` (per-table overlay): maps bare column references
    #     to the DAX form, scoped to a single table. Used to bias bare
    #     references inside a measure's expression toward the measure's own
    #     table when the same column name exists in multiple tables.
    column_map: Dict[str, str] = {}
    per_table_bare: Dict[str, Dict[str, str]] = {}

    def _bare_identifier(expr: str) -> Optional[str]:
        """Return the unquoted identifier if ``expr`` is a bare column
        reference, else None."""
        if not expr:
            return None
        s = expr.strip()
        if re.fullmatch(r'(?:"[^"]+"|`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)', s):
            return s.strip('"').strip("`")
        return None

    # Calculated columns are not supported and are excluded from the model, so
    # they must not be registered in the column maps.
    calculated_column_names: Dict[str, set] = {}
    for t in sf_tables:
        tbl = t.get("name", "") or ""
        if not tbl:
            continue
        calc_names = set()
        for kind in ("dimensions", "time_dimensions", "facts"):
            for field in t.get(kind, []) or []:
                col = field.get("name", "") or ""
                expr = field.get("expr", "") or ""
                if col and expr and _bare_identifier(expr) is None:
                    calc_names.add(col.lower())
        calculated_column_names[tbl] = calc_names

    for t in sf_tables:
        tbl = t.get("name", "") or ""
        if not tbl:
            continue
        bare_for_table: Dict[str, str] = {}
        for kind in ("dimensions", "time_dimensions", "facts"):
            for field in t.get(kind, []) or []:
                col = field.get("name", "") or ""
                if not col or col.lower() in calculated_column_names.get(tbl, set()):
                    continue
                dax_ref = f"'{tbl}'[{col}]"
                # Logical/field name references.
                column_map[f"{tbl}.{col}"] = dax_ref
                column_map.setdefault(col, dax_ref)
                bare_for_table[col] = dax_ref
                # Source-column / expr-identifier references (e.g. measures
                # written against the underlying base table column name).
                src_id = _bare_identifier(field.get("expr", "") or "")
                if src_id and src_id != col:
                    column_map[f"{tbl}.{src_id}"] = dax_ref
                    column_map.setdefault(src_id, dax_ref)
                    bare_for_table[src_id] = dax_ref
        # Pre-register table-scoped metrics so that other measure expressions
        # (including view-level derived metrics) can reference them as
        # ``table.metric`` or bare ``metric`` and have them resolve to a DAX
        # measure reference ``[MetricName]``.
        for metric in t.get("metrics", []) or []:
            mname = metric.get("name", "") or ""
            if not mname:
                continue
            measure_ref = f"[{mname}]"
            column_map[f"{tbl}.{mname}"] = measure_ref
            column_map.setdefault(mname, measure_ref)
            bare_for_table[mname] = measure_ref
        per_table_bare[tbl] = bare_for_table

    # View-level (derived) metrics — also register them as measure references.
    for metric in data.get("metrics", []) or []:
        mname = metric.get("name", "") or ""
        if not mname:
            continue
        column_map.setdefault(mname, f"[{mname}]")

    # Pre-register columns referenced via ``table.column`` in metric
    # expressions but not declared as dimensions/facts. The Snowflake
    # semantic view often references the underlying base-table column names
    # directly (e.g. ``SUM(store_sales.ss_sales_price * store_sales.ss_quantity)``).
    # Resolving these to DAX column references makes the resulting metric
    # expression valid; the columns themselves still need to exist in the
    # final Power BI model.
    qualified_ref_re = re.compile(
        r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b"
    )
    table_names_lower = {tn.lower(): tn for tn in table_names}

    def _scan_expr(expr: str, owning_table: Optional[str]) -> None:
        if not expr:
            return
        for m in qualified_ref_re.finditer(expr):
            tbl_ref, col_ref = m.group(1), m.group(2)
            tbl = table_names_lower.get(tbl_ref.lower())
            if not tbl:
                continue
            key = f"{tbl_ref}.{col_ref}"
            if key in column_map:
                continue
            if col_ref.lower() in calculated_column_names.get(tbl, set()):
                continue
            dax_ref = f"'{tbl}'[{col_ref}]"
            column_map[key] = dax_ref
            column_map.setdefault(col_ref, dax_ref)
            if owning_table and owning_table == tbl:
                per_table_bare.setdefault(owning_table, {}).setdefault(col_ref, dax_ref)

    for t in sf_tables:
        tbl_name = t.get("name", "") or ""
        for metric in t.get("metrics", []) or []:
            _scan_expr(metric.get("expr", "") or "", tbl_name)
    for metric in data.get("metrics", []) or []:
        _scan_expr(metric.get("expr", "") or "", None)

    def _column_map_for_table(table_name: str) -> Dict[str, str]:
        """Return a column map biased toward ``table_name`` for bare refs."""
        if not table_name or table_name not in per_table_bare:
            return column_map
        return {**column_map, **per_table_bare[table_name]}

    def _calculated_dependencies(
        expression: str, owning_table: Optional[str]
    ) -> List[str]:
        """Return the calculated columns referenced by a SQL expression."""
        if not expression:
            return []
        deps = set()
        for m in qualified_ref_re.finditer(expression):
            tbl = table_names_lower.get(m.group(1).lower())
            if tbl and m.group(2).lower() in calculated_column_names.get(tbl, set()):
                deps.add(f"{tbl}.{m.group(2)}")
        bare_calc = calculated_column_names.get(owning_table or "", set())
        if bare_calc:
            # Qualified references are removed first so they are not counted twice.
            unqualified = qualified_ref_re.sub(" ", expression)
            for token in re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", unqualified):
                if token.lower() in bare_calc:
                    deps.add(f"{owning_table}.{token}")
        return sorted(deps)

    def _metric_dax(expression: str, table_name: str) -> str:
        """Convert a metric expression to DAX, returning ``BLANK()`` with a note
        when the metric depends on an unsupported calculated column."""
        if not expression:
            return ""
        deps = _calculated_dependencies(expression, table_name)
        if deps:
            note = _CALCULATED_DEPENDENCY_NOTE.format(columns=", ".join(deps))
            return f"// {note}\nBLANK()"
        return convert_sql_to_dax(
            expression,
            column_map=_column_map_for_table(table_name),
            default_table=table_name,
            relationships=rel_hints,
        )

    def _build_column(field: dict, table_name: str, pk_columns: set) -> Dict[str, Any]:
        col_name = field.get("name", "") or ""
        expression = field.get("expr", "") or ""
        # If the expression is just a bare column name (an identifier,
        # optionally quoted with double quotes or backticks), treat it as a
        # plain source column rather than a calculated column.
        expr_stripped = expression.strip()
        is_bare_identifier = (
            bool(expr_stripped)
            and re.fullmatch(
                r'(?:"[^"]+"|`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)', expr_stripped
            )
            is not None
        )
        is_calculated = bool(expression) and not is_bare_identifier
        if is_calculated:
            source_column = ""
        elif is_bare_identifier:
            # Strip surrounding quotes/backticks if present.
            source_column = expr_stripped.strip('"').strip("`")
        else:
            source_column = col_name
        is_key = (col_name in pk_columns) or (
            bool(source_column) and source_column in pk_columns
        )
        return {
            "name": col_name,
            "sourceColumn": source_column,
            "sourceDataType": field.get("data_type", "") or "",
            "pbiDataType": _convert_snowflake_data_type(field.get("data_type")),
            "sourceFormat": None,
            "pbiFormat": None,
            "description": field.get("description", "") or "",
            "expression": expression if is_calculated else "",
            "synonyms": _get_synonyms(field),
            "fullDAXObjectName": f"'{table_name}'[{col_name}]",
            "isCalculated": is_calculated,
            "isKey": is_key,
            "isHidden": False,
        }

    tables: List[Dict[str, Any]] = []
    for t in sf_tables:
        table_name = t.get("name", "") or ""
        source_name = _build_source_name(t.get("base_table"))

        # Collect the set of primary key column names for this table. The
        # Snowflake semantic view schema declares them under
        # ``primary_key.columns``.
        pk_block = t.get("primary_key") or {}
        pk_columns = set(pk_block.get("columns") or [])

        columns: List[Dict[str, Any]] = []
        calculated_columns: List[Dict[str, Any]] = []
        for kind in ("dimensions", "time_dimensions", "facts"):
            for field in t.get(kind, []) or []:
                column = _build_column(field, table_name, pk_columns)
                # Calculated columns are not supported; they are documented as
                # table annotations instead of being added to the model.
                if column["isCalculated"]:
                    calculated_columns.append(column)
                else:
                    columns.append(column)

        # Augment with all base-table columns from Snowflake. Columns already
        # declared in the YAML keep ``isHidden=False``; columns added solely
        # from the Snowflake base table are marked ``isHidden=True``.
        base_table = t.get("base_table") or {}
        bt_database = base_table.get("database") or ""
        bt_schema = base_table.get("schema") or ""
        bt_table = base_table.get("table") or ""
        if bt_database and bt_schema and bt_table:
            existing_source_cols = {
                (c.get("sourceColumn") or "").upper()
                for c in columns
                if c.get("sourceColumn")
            }
            sf_cols_df = list_snowflake_columns(
                account=account,
                token=token,
                database=bt_database,
                schema=bt_schema,
                table=bt_table,
            )
            for _, row in sf_cols_df.iterrows():
                sf_col_name = row["Column Name"] or ""
                if not sf_col_name or sf_col_name.upper() in existing_source_cols:
                    continue
                sf_data_type = row["Data Type"] or ""
                columns.append(
                    {
                        "name": sf_col_name,
                        "sourceColumn": sf_col_name,
                        "sourceDataType": sf_data_type,
                        "pbiDataType": _convert_snowflake_data_type(sf_data_type),
                        "sourceFormat": None,
                        "pbiFormat": None,
                        "description": "",
                        "expression": "",
                        "synonyms": [],
                        "fullDAXObjectName": f"'{table_name}'[{sf_col_name}]",
                        "isCalculated": False,
                        "isKey": sf_col_name in pk_columns,
                        "isHidden": True,
                    }
                )

        measures: List[Dict[str, Any]] = []
        for metric in t.get("metrics", []) or []:
            metric_name = metric.get("name", "") or ""
            expression = metric.get("expr", "") or ""
            measures.append(
                {
                    "name": metric_name,
                    "sourceExpression": expression,
                    "daxExpression": _metric_dax(expression, table_name),
                    "sourceFormat": None,
                    "pbiFormat": None,
                    "description": metric.get("description", "") or "",
                    "synonyms": _get_synonyms(metric),
                }
            )

        table_entry: Dict[str, Any] = {
            "tableName": table_name,
            "description": t.get("description", "") or "",
            "sourceName": source_name,
            "sourceItemId": source_item_id,
            "sourceWorkspaceId": source_workspace_id,
            "columns": columns,
            "measures": measures,
        }
        if calculated_columns:
            table_entry["annotations"] = [
                {
                    "name": f"CalculatedColumn_{c['name']}",
                    "value": json.dumps(
                        {
                            k: c[k]
                            for k in (
                                "name",
                                "expression",
                                "sourceDataType",
                                "pbiDataType",
                                "description",
                                "synonyms",
                                "isKey",
                            )
                        }
                    ),
                }
                for c in calculated_columns
            ]
        tables.append(table_entry)

    # Map view-level (derived) metrics onto the table they reference.
    table_lookup = {t["tableName"]: t for t in tables}
    for metric in data.get("metrics", []) or []:
        metric_name = metric.get("name", "") or ""
        expression = metric.get("expr", "") or ""
        target_table = _resolve_metric_table(expression, table_names)
        if target_table is None and tables:
            target_table = tables[0]["tableName"]
        if target_table is None:
            continue

        table_lookup[target_table]["measures"].append(
            {
                "name": metric_name,
                "sourceExpression": expression,
                "daxExpression": _metric_dax(expression, target_table),
                "sourceFormat": None,
                "pbiFormat": None,
                "description": metric.get("description", "") or "",
                "synonyms": _get_synonyms(metric),
            }
        )

    relationships: List[Dict[str, Any]] = []
    for rel in data.get("relationships", []) or []:
        rel_columns = rel.get("relationship_columns") or []
        if len(rel_columns) > 1:
            raise ValueError(
                "Multi-column relationships are not supported. The relationship "
                f"'{rel.get('name', '')}' from '{rel.get('left_table', '')}' to "
                f"'{rel.get('right_table', '')}' has "
                f"relationship_columns={rel_columns}."
            )
        left_column = rel_columns[0].get("left_column", "") if rel_columns else ""
        right_column = rel_columns[0].get("right_column", "") if rel_columns else ""

        relationships.append(
            {
                "name": rel.get("name") or None,
                "fromTable": rel.get("left_table", "") or "",
                "fromColumn": left_column,
                "toTable": rel.get("right_table", "") or "",
                "toColumn": right_column,
                "fromCardinality": "Many",
                "toCardinality": "One",
            }
        )

    # Composite primary keys (multiple columns flagged with isKey=True on the
    # same table) are not supported by the model_map format. When detected,
    # clear the isKey flag on all columns of the affected table.
    for t in tables:
        if sum(1 for c in t["columns"] if c.get("isKey")) > 1:
            for c in t["columns"]:
                c["isKey"] = False

    model_map = {
        "model": {
            "name": model_name,
            "description": model_description,
            "tables": tables,
            "relationships": relationships,
        }
    }

    bim = convert_model_map_to_bim(model_map=model_map)

    if not test_run:
        # create_blank_semantic_model(dataset=model_name, workspace=semantic_model_workspace_id)
        create_semantic_model_from_bim(
            dataset=model_name, bim_file=bim, workspace=semantic_model_workspace_id
        )

    return bim
