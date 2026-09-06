from typing import Any, Dict, List, Optional, Tuple
from sempy._utils._log import log
from sempy_labs.semantic_model._helper import convert_sql_to_dax
import sempy_labs._icons as icons

# Maps the model_map 'pbiDataType' values to their TMSL (.bim) equivalent.
_TMSL_DATA_TYPES: Dict[str, str] = {
    "String": "string",
    "Int64": "int64",
    "Double": "double",
    "Decimal": "decimal",
    "DateTime": "dateTime",
    "Boolean": "boolean",
    "Binary": "binary",
    "Variant": "variant",
}


def _generate_onelake_expression(workspace_id: str, item_id: str) -> str:
    """Generate the M expression used by a Direct Lake on OneLake model."""

    return f'let\n\tSource = AzureStorage.DataLake("https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{item_id}")\nin\n\tSource'


def _split_source_name(source_name: Optional[str]) -> Tuple[Optional[str], str]:
    """Split a ``database.schema.table`` source name into (schema, entity)."""

    parts = [p for p in (source_name or "").split(".") if p]
    if not parts:
        return None, ""
    return (parts[-2] if len(parts) > 1 else None), parts[-1]


def _build_annotations(values: Dict[str, Any]) -> List[Dict[str, str]]:
    """Build a TMSL annotation collection, skipping empty values."""

    return [{"name": name, "value": str(v)} for name, v in values.items() if v]


def _format_synonyms(synonyms: Any) -> str:
    """Format a list of synonyms as a comma-separated annotation value."""

    if not synonyms:
        return ""
    if isinstance(synonyms, str):
        return synonyms
    return ", ".join(str(s) for s in synonyms if s)


def _build_column_maps(
    model_tables: List[Dict[str, Any]],
) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
    """Build the column maps used to convert SQL column expressions to DAX.

    Returns the global map (``table.column`` and bare ``column`` references) and
    a per-table overlay which biases bare references toward the table owning the
    expression.
    """

    column_map: Dict[str, str] = {}
    per_table: Dict[str, Dict[str, str]] = {}
    for t in model_tables:
        table_name = t.get("tableName") or ""
        bare: Dict[str, str] = {}
        for c in t.get("columns", []) or []:
            column_name = c.get("name") or ""
            if not column_name:
                continue
            dax_ref = f"'{table_name}'[{column_name}]"
            for key in (column_name, c.get("sourceColumn") or ""):
                if not key:
                    continue
                column_map[f"{table_name}.{key}"] = dax_ref
                column_map.setdefault(key, dax_ref)
                bare[key] = dax_ref
        per_table[table_name] = bare

    return column_map, per_table


def _summarize_by(column_name: str, data_type: str) -> str:
    if data_type not in ["int64", "double", "decimal"]:
        return "none"
    if column_name.lower().endswith("key") or column_name.lower().endswith("id"):
        return "count"
    return "sum"


@log
def convert_model_map_to_bim(
    model_map: dict,
    compatibility_level: int = 1702,
) -> dict:
    """
    Converts a model_map dictionary into a Direct Lake semantic model in the Model.bim (TMSL) format.

    Each table's 'sourceWorkspaceId' and 'sourceItemId' are used to generate a Direct Lake on OneLake
    expression. Tables which share the same source item share the same expression. The 'sourceDataType',
    'sourceExpression', 'sourceFormat' and 'synonyms' properties of the model_map are retained as
    annotations on the corresponding semantic model object.

    Columns which have an 'expression' are created as calculated columns; the expression is translated
    from SQL to DAX and the original SQL expression is retained as an annotation.

    Limitations:
        * Columns with a 'Binary' data type are not supported in Direct Lake and are skipped.

    Parameters
    ----------
    model_map : dict
        A dictionary of a semantic model in the model_map format (see ``sempy_labs.semantic_model._model_map``).
    compatibility_level : int, default=1702
        The compatibility level of the semantic model.

    Returns
    -------
    dict
        The Model.bim file of the semantic model.
    """

    model = (model_map or {}).get("model")
    if not model:
        raise ValueError(
            f"{icons.red_dot} The 'model_map' parameter must contain a 'model' key."
        )

    expressions: List[Dict[str, Any]] = []
    expression_names: Dict[Tuple[str, str], str] = {}

    def _resolve_expression_name(workspace_id: str, item_id: str) -> str:
        key = (workspace_id, item_id)
        if key not in expression_names:
            name = (
                "DirectLake"
                if not expression_names
                else f"DirectLake{len(expression_names) + 1}"
            )
            expression_names[key] = name
            expressions.append(
                {
                    "name": name,
                    "kind": "m",
                    "expression": _generate_onelake_expression(workspace_id, item_id),
                }
            )
        return expression_names[key]

    tables: List[Dict[str, Any]] = []
    # Maps each table to its {source column | column name: column name} so relationships,
    # which are defined based on source columns, can be resolved to the column names.
    column_lookup: Dict[str, Dict[str, str]] = {}

    model_tables = model.get("tables", []) or []
    column_map, per_table_columns = _build_column_maps(model_tables)
    # Table pairs are used by the SQL -> DAX conversion to add RELATED() where needed.
    relationship_hints = [
        {"fromTable": r.get("fromTable") or "", "toTable": r.get("toTable") or ""}
        for r in model.get("relationships", []) or []
    ]

    for t in model_tables:
        table_name = t.get("tableName") or ""
        source_workspace_id = t.get("sourceWorkspaceId")
        source_item_id = t.get("sourceItemId")
        if not source_workspace_id or not source_item_id:
            raise ValueError(
                f"{icons.red_dot} The '{table_name}' table must have a 'sourceWorkspaceId' and a 'sourceItemId' in order to generate a Direct Lake semantic model."
            )
        expression_name = _resolve_expression_name(source_workspace_id, source_item_id)
        schema_name, entity_name = _split_source_name(t.get("sourceName"))
        table_column_map = {**column_map, **per_table_columns.get(table_name, {})}

        columns: List[Dict[str, Any]] = []
        lookup: Dict[str, str] = {}
        for c in t.get("columns", []) or []:
            column_name = c.get("name") or ""
            data_type = _TMSL_DATA_TYPES.get(c.get("pbiDataType") or "", "string")
            if data_type == "binary":
                print(
                    f"{icons.warning} The '{column_name}' column in the '{table_name}' table has the 'Binary' data type which is not supported in Direct Lake semantic models. This column is skipped."
                )
                continue
            source_column = c.get("sourceColumn") or ""
            expression = c.get("expression") or ""
            column: Dict[str, Any] = {
                "name": column_name,
                "dataType": data_type,
                "summarizeBy": _summarize_by(column_name, data_type),
            }
            if expression:
                column["type"] = "calculated"
                column["expression"] = convert_sql_to_dax(
                    expression,
                    column_map=table_column_map,
                    default_table=table_name,
                    relationships=relationship_hints,
                )
                column["isDataTypeInferred"] = False
            else:
                source_column = source_column or column_name
                column["sourceColumn"] = source_column
            if c.get("description"):
                column["description"] = c.get("description")
            if c.get("pbiFormat"):
                column["formatString"] = c.get("pbiFormat")
            if c.get("isHidden"):
                column["isHidden"] = True
            if c.get("isKey"):
                column["isKey"] = True
            annotations = _build_annotations(
                {
                    "SourceDataType": c.get("sourceDataType"),
                    "SourceExpression": expression,
                    "SourceFormat": c.get("sourceFormat"),
                    "Synonyms": _format_synonyms(c.get("synonyms")),
                }
            )
            if annotations:
                column["annotations"] = annotations
            columns.append(column)
            if source_column:
                lookup[source_column] = column_name
            lookup.setdefault(column_name, column_name)
        column_lookup[table_name] = lookup

        measures: List[Dict[str, Any]] = []
        for m in t.get("measures", []) or []:
            measure: Dict[str, Any] = {
                "name": m.get("name") or "",
                "expression": m.get("daxExpression") or "",
            }
            if m.get("description"):
                measure["description"] = m.get("description")
            if m.get("pbiFormat"):
                measure["formatString"] = m.get("pbiFormat")
            if m.get("isHidden"):
                measure["isHidden"] = True
            annotations = _build_annotations(
                {
                    "SourceExpression": m.get("sourceExpression"),
                    "SourceFormat": m.get("sourceFormat"),
                    "Synonyms": _format_synonyms(m.get("synonyms")),
                }
            )
            if annotations:
                measure["annotations"] = annotations
            measures.append(measure)

        source: Dict[str, Any] = {
            "type": "entity",
            "entityName": entity_name,
            "expressionSource": expression_name,
        }
        if schema_name:
            source["schemaName"] = schema_name

        table: Dict[str, Any] = {
            "name": table_name,
            "columns": columns,
            "partitions": [
                {
                    "name": table_name,
                    "mode": "directLake",
                    "source": source,
                }
            ],
        }
        if t.get("description"):
            table["description"] = t.get("description")
        if measures:
            table["measures"] = measures
        table_annotations = _build_annotations(
            {"Synonyms": _format_synonyms(t.get("synonyms"))}
        )
        if table_annotations:
            table["annotations"] = table_annotations
        tables.append(table)

    relationships: List[Dict[str, Any]] = []
    for r in model.get("relationships", []) or []:
        from_table = r.get("fromTable") or ""
        to_table = r.get("toTable") or ""
        from_column = column_lookup.get(from_table, {}).get(r.get("fromColumn") or "")
        to_column = column_lookup.get(to_table, {}).get(r.get("toColumn") or "")
        if not from_column or not to_column:
            raise ValueError(
                f"{icons.red_dot} The relationship from '{from_table}'[{r.get('fromColumn')}] to '{to_table}'[{r.get('toColumn')}] could not be resolved to columns within the semantic model."
            )
        relationships.append(
            {
                "name": r.get("name")
                or f"{from_table}_{from_column}_{to_table}_{to_column}",
                "fromTable": from_table,
                "fromColumn": from_column,
                "toTable": to_table,
                "toColumn": to_column,
                "fromCardinality": (r.get("fromCardinality") or "Many").lower(),
                "toCardinality": (r.get("toCardinality") or "One").lower(),
            }
        )

    bim: Dict[str, Any] = {
        "name": model.get("name") or "",
        "compatibilityLevel": compatibility_level,
        "model": {
            "culture": "en-US",
            "collation": "Latin1_General_100_BIN2_UTF8",
            "dataAccessOptions": {
                "legacyRedirects": True,
                "returnErrorValuesAsNull": True,
            },
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "sourceQueryCulture": "en-US",
            "expressions": expressions,
            "tables": tables,
            "relationships": relationships,
        },
    }
    if model.get("description"):
        bim["model"]["description"] = model.get("description")

    return bim
