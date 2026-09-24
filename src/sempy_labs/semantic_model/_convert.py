import json
import re
from typing import Any, Callable, Dict, List, Optional
from uuid import UUID
from sempy._utils._log import log
from sempy_labs._helper_functions import resolve_workspace_name_and_id
from sempy_labs._ui_components import (
    ICONS as _UI_ICONS,
    LIGHT_THEME_VARS as _UI_LIGHT_VARS,
    DARK_THEME_VARS as _UI_DARK_VARS,
    SEARCH_SELECT_CSS as _UI_SEARCH_SELECT_CSS,
    SEARCH_SELECT_JS as _UI_SEARCH_SELECT_JS,
    SYNTAX_HIGHLIGHT_VARS as _UI_SYNTAX_VARS,
    _picker_items_from_api,
    fullscreen_css as _ui_fullscreen_css,
    fullscreen_setup_js as _ui_fullscreen_setup_js,
    list_picker_datasets as _list_picker_datasets,
    list_picker_lakehouses as _list_picker_lakehouses,
    list_picker_warehouses as _list_picker_warehouses,
    list_picker_workspaces as _list_picker_workspaces,
    render_attribution_html as _ui_render_attribution_html,
    scoped_attribution_css as _ui_scoped_attribution_css,
    scoped_button_press_css as _ui_scoped_button_press_css,
    scoped_header_css as _ui_scoped_header_css,
)


def _convert_snowflake(
    values: Dict[str, str],
    source_item: str,
    source_type: str,
    source_workspace: str,
    workspace: str,
    name: Optional[str],
    test_run: bool,
) -> dict:
    from sempy_labs.semantic_model._snowflake import convert_from_snowflake

    account = re.sub(r"^https://", "", values["account"].strip(), flags=re.I)

    return convert_from_snowflake(
        yaml_file=values["yaml_file"],
        account=account.rstrip("/"),
        token=values["token"].strip(),
        source_item=source_item,
        source_type=source_type,
        source_workspace=source_workspace,
        semantic_model_workspace=workspace,
        semantic_model_name=name,
        test_run=test_run,
    )


# Properties of a Snowflake semantic view which the conversion uses, per element.
_SF_KNOWN_KEYS: Dict[str, set] = {
    "view": {"name", "description", "tables", "relationships", "metrics"},
    "table": {
        "name",
        "description",
        "base_table",
        "primary_key",
        "dimensions",
        "time_dimensions",
        "facts",
        "metrics",
        "synonyms",
    },
    "base_table": {"database", "schema", "table"},
    "primary_key": {"columns"},
    "field": {"name", "description", "expr", "data_type", "synonyms"},
    "metric": {"name", "description", "expr", "synonyms"},
    "relationship": {"name", "left_table", "right_table", "relationship_columns"},
    "relationship_column": {"left_column", "right_column"},
}

# Snowflake properties with no semantic model equivalent: key -> (element, explanation).
_SF_UNSUPPORTED: Dict[str, tuple] = {
    "unique_keys": ("Unique keys", "Semantic models have no unique key constraints."),
    "unique": ("Unique keys", "Semantic models have no unique key constraints."),
    "definition": (
        "SQL query tables",
        "Logical tables defined by a SQL query have no Direct Lake source table.",
    ),
    "filters": ("Filters", "Named filters have no semantic model equivalent."),
    "labels": (
        "Filter labels",
        "Dimensions and facts labelled as filters become regular columns.",
    ),
    "tags": ("Tags", "Snowflake object tags are not carried over."),
    "sample_values": ("Sample values", "Sample values are not carried over."),
    "is_enum": ("Enumerations", "'is_enum' is not carried over."),
    "cortex_search_service": (
        "Cortex Search services",
        "Cortex Search services are not carried over.",
    ),
    "cortex_search_service_name": (
        "Cortex Search services",
        "Cortex Search services are not carried over.",
    ),
    "access_modifier": (
        "Private access",
        "Private facts and metrics are not hidden in the semantic model.",
    ),
    "non_additive_dimensions": (
        "Non-additive dimensions",
        "Measures aggregate across all dimensions; semi-additive behavior is not converted.",
    ),
    "using_relationships": (
        "Relationship paths",
        "Measures use the model's active relationships instead of 'using_relationships'.",
    ),
    "type": (
        "ASOF and range joins",
        "Relationships are created as regular (equality) relationships.",
    ),
    "right_range": (
        "ASOF and range joins",
        "Relationships are created as regular (equality) relationships.",
    ),
    "variables": (
        "Variables",
        "Query-time variables have no semantic model equivalent.",
    ),
    "verified_queries": ("Verified queries", "Verified queries are not carried over."),
    "custom_instructions": (
        "Cortex Analyst instructions",
        "Custom instructions are not carried over.",
    ),
    "module_custom_instructions": (
        "Cortex Analyst instructions",
        "Custom instructions are not carried over.",
    ),
    "max_staleness": (
        "Materializations",
        "'max_staleness' has no semantic model equivalent.",
    ),
}

_SF_BARE_IDENTIFIER = re.compile(r'(?:"[^"]+"|`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)')


def _inspect_snowflake(values: Dict[str, str]) -> List[Dict[str, Any]]:
    """Lists the elements of a Snowflake semantic view which are not converted."""

    import yaml

    data = yaml.safe_load(values["yaml_file"]) or {}
    groups: Dict[str, Dict[str, Any]] = {}

    def add(element: str, detail: str, name: str, note: str = ""):
        group = groups.setdefault(
            element, {"element": element, "detail": detail, "items": []}
        )
        item = {"name": name, "note": note}
        if item not in group["items"]:
            group["items"].append(item)

    def check(obj: Any, kind: str, owner: str):
        if not isinstance(obj, dict):
            return
        for key, value in obj.items():
            if key in _SF_KNOWN_KEYS[kind] or value in (None, "", [], {}):
                continue
            if key == "access_modifier" and str(value).lower() != "private_access":
                continue
            element, detail = _SF_UNSUPPORTED.get(
                key,
                ("Other properties", "These properties are not recognized."),
            )
            add(element, detail, owner, "" if key in _SF_UNSUPPORTED else key)

    def synonyms(obj: dict, owner: str, kept: bool):
        if obj.get("synonyms"):
            add(
                "Synonyms",
                "Synonyms are not added to the linguistic schema used by Q&A and Copilot.",
                owner,
                "Kept as an annotation" if kept else "Dropped",
            )

    check(data, "view", "Semantic view")
    for t in data.get("tables") or []:
        table = t.get("name", "") or ""
        check(t, "table", table)
        synonyms(t, table, kept=False)
        base_table = t.get("base_table") or {}
        if base_table.get("definition"):
            element, detail = _SF_UNSUPPORTED["definition"]
            add(element, detail, table)
        check(
            {k: v for k, v in base_table.items() if k != "definition"},
            "base_table",
            table,
        )
        primary_key = t.get("primary_key") or {}
        check(primary_key, "primary_key", table)
        if len(primary_key.get("columns") or []) > 1:
            add(
                "Composite primary keys",
                "Only single-column keys are supported, so no key column is set on these tables.",
                table,
                ", ".join(str(c) for c in primary_key["columns"]),
            )
        for kind in ("dimensions", "time_dimensions", "facts"):
            for field in t.get(kind) or []:
                owner = f"{table}[{field.get('name', '')}]"
                check(field, "field", owner)
                synonyms(field, owner, kept=True)
                expr = str(field.get("expr") or "").strip()
                if expr and not _SF_BARE_IDENTIFIER.fullmatch(expr):
                    add(
                        "Calculated columns",
                        "Only plain column references are supported. These columns are excluded "
                        "from the model and kept as table annotations.",
                        owner,
                        expr,
                    )
        for metric in t.get("metrics") or []:
            owner = f"{table}[{metric.get('name', '')}]"
            check(metric, "metric", owner)
            synonyms(metric, owner, kept=True)
    for metric in data.get("metrics") or []:
        owner = f"[{metric.get('name', '')}]"
        check(metric, "metric", owner)
        synonyms(metric, owner, kept=True)
    for rel in data.get("relationships") or []:
        owner = rel.get("name") or (
            f"{rel.get('left_table', '')} \u2192 {rel.get('right_table', '')}"
        )
        check(rel, "relationship", owner)
        for column in rel.get("relationship_columns") or []:
            check(column, "relationship_column", owner)
    return list(groups.values())


# Vendors the converter can convert from. To support another vendor (e.g. a
# Databricks metric view), add an entry here: the UI is built from ``fields``
# and ``convert`` receives the entered values keyed by field ``key``. The optional
# ``inspect`` receives the same values and lists the elements it cannot convert.
# Field types: "definition" (multi-line text with file upload), "text", "secret".
# Optional: "syntax" ("yaml" highlights a definition), "pattern" (a regex, matched
# case-insensitively in both JS and Python) with its "pattern_error" message.
_VENDORS: Dict[str, Dict[str, Any]] = {
    "snowflake": {
        "label": "Snowflake",
        "object": "Semantic view",
        "description": "Converts a Snowflake semantic view YAML definition.",
        "docs_url": "https://docs.snowflake.com/en/user-guide/views-semantic/semantic-view-yaml-spec",
        "fields": [
            {
                "key": "yaml_file",
                "label": "Semantic view YAML",
                "type": "definition",
                "syntax": "yaml",
                "accept": ".yaml,.yml",
                "placeholder": "Paste the semantic view YAML definition, or upload a .yaml file\u2026",
            },
            {
                "key": "account",
                "label": "Account",
                "type": "text",
                "placeholder": "myorg-myaccount.snowflakecomputing.com",
                "help": "The host of the account URL. Used to read the columns of each base table.",
                # <orgname>-<account_name> or <locator>[.<region>[.<cloud>]], optionally privatelink.
                "pattern": r"^(?:https://)?[a-z0-9](?:[a-z0-9_-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9_-]*[a-z0-9])?)*\.snowflakecomputing\.(?:com|cn)/?$",
                "pattern_error": "Enter the account URL host, e.g. myorg-myaccount.snowflakecomputing.com.",
            },
            {
                "key": "token",
                "label": "Access token",
                "type": "secret",
                "placeholder": "Snowflake access token",
                "help": "Only used for this conversion.",
            },
        ],
        "convert": _convert_snowflake,
        "inspect": _inspect_snowflake,
    },
}

_SOURCE_TYPES: Dict[str, Callable[[str], List[Dict[str, str]]]] = {
    "Lakehouse": _list_picker_lakehouses,
    "Warehouse": _list_picker_warehouses,
}


def _vendor_payload() -> List[Dict[str, Any]]:
    return [
        {"key": key, **{k: v for k, v in vendor.items() if not callable(v)}}
        for key, vendor in _VENDORS.items()
    ]


def _list_semantic_models(workspace_id: str) -> List[Dict[str, str]]:
    """Lists the workspace's semantic models, raising if they cannot be listed."""

    return _picker_items_from_api(f"/v1/workspaces/{workspace_id}/semanticModels")


def _unconverted_reason(expression: Any) -> Optional[str]:
    """Returns the note of a measure which was emitted as a commented ``BLANK()``."""

    lines = [line.strip() for line in str(expression or "").splitlines()]
    lines = [line for line in lines if line]
    comments = [line[2:].strip() for line in lines if line.startswith("//")]
    code = [line for line in lines if not line.startswith("//")]
    if comments and code == ["BLANK()"]:
        return " ".join(comments)
    return None


def _summarize_bim(bim: dict) -> Dict[str, Any]:
    tables = (bim.get("model") or {}).get("tables") or []
    columns = hidden_columns = measures = 0
    unconverted = []
    for table in tables:
        for column in table.get("columns") or []:
            columns += 1
            hidden_columns += 1 if column.get("isHidden") else 0
        for measure in table.get("measures") or []:
            measures += 1
            reason = _unconverted_reason(measure.get("expression"))
            if reason is not None:
                unconverted.append(
                    {
                        "table": table.get("name", ""),
                        "measure": measure.get("name", ""),
                        "reason": reason,
                    }
                )
    return {
        "tables": len(tables),
        "columns": columns,
        "hidden_columns": hidden_columns,
        "measures": measures,
        "relationships": len((bim.get("model") or {}).get("relationships") or []),
        "unconverted": unconverted,
    }


def _model_view(bim: dict) -> Dict[str, Any]:
    """The tables, columns, measures and relationships of a .bim, for the model view."""

    model = bim.get("model") or {}
    tables = []
    for t in model.get("tables") or []:
        excluded = []
        for a in t.get("annotations") or []:
            name = str(a.get("name") or "")
            if not name.startswith("CalculatedColumn_"):
                continue
            try:
                info = json.loads(a.get("value") or "{}")
            except ValueError:
                info = {}
            excluded.append(
                {
                    "name": info.get("name")
                    or name.replace("CalculatedColumn_", "", 1),
                    "expression": info.get("expression") or "",
                }
            )
        measures = []
        for m in t.get("measures") or []:
            annotations = {
                a.get("name"): a.get("value") for a in m.get("annotations") or []
            }
            measures.append(
                {
                    "name": m.get("name", ""),
                    "sql": annotations.get("SourceExpression") or "",
                    "dax": m.get("expression") or "",
                    "converted": _unconverted_reason(m.get("expression")) is None,
                }
            )
        tables.append(
            {
                "name": t.get("name", ""),
                "description": t.get("description", ""),
                "columns": [
                    {
                        "name": c.get("name", ""),
                        "data_type": c.get("dataType", ""),
                        "source": c.get("sourceColumn", ""),
                        "hidden": bool(c.get("isHidden")),
                        "key": bool(c.get("isKey")),
                    }
                    for c in t.get("columns") or []
                ],
                "excluded_columns": excluded,
                "measures": measures,
            }
        )
    relationships = [
        {
            "from": f"'{r.get('fromTable', '')}'[{r.get('fromColumn', '')}]",
            "to": f"'{r.get('toTable', '')}'[{r.get('toColumn', '')}]",
            "cardinality": f"{r.get('fromCardinality', 'many')}-to-{r.get('toCardinality', 'one')}",
        }
        for r in model.get("relationships") or []
    ]
    return {"tables": tables, "relationships": relationships}


@log
def semantic_model_converter(
    workspace: Optional[str | UUID] = None,
    dark_mode: bool = False,
):
    """
    Opens an interactive tool which converts a semantic model defined by another vendor into a Power BI semantic model in Direct Lake mode.

    The source vendor is chosen in the tool. Currently supported:

    * Snowflake semantic views (converted by :func:`sempy_labs.semantic_model._snowflake.convert_from_snowflake`).

    The tables referenced by the definition must be available in a Fabric lakehouse or warehouse,
    which becomes the Direct Lake source of the new semantic model. The destination semantic model name
    must not already be used by a semantic model in the destination workspace. The conversion is
    previewed first, listing the elements which could not be converted and showing the resulting tables,
    columns and measures (with each measure's source SQL and converted DAX), before the semantic model is created.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID initially selected as the source and destination workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    dark_mode : bool, default=False
        If True, the tool opens in dark mode.
    """

    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "The 'semantic_model_converter' function requires the 'anywidget' "
            "package. Install it with: pip install anywidget"
        ) from e

    from IPython.display import display

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    workspace_id = str(workspace_id)

    class SemanticModelConverterWidget(anywidget.AnyWidget):
        _esm = _WIDGET_JS
        _css = _WIDGET_CSS

        vendors = traitlets.List().tag(sync=True)
        source_types = traitlets.List().tag(sync=True)
        workspace_id = traitlets.Unicode("").tag(sync=True)
        workspaces = traitlets.List().tag(sync=True)
        datasets = traitlets.Dict().tag(sync=True)
        source_items = traitlets.Dict().tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        result = traitlets.Dict().tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        action_done = traitlets.Int(0).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)

    widget = SemanticModelConverterWidget(
        vendors=_vendor_payload(),
        source_types=list(_SOURCE_TYPES),
        workspace_id=workspace_id,
        workspaces=[{"id": workspace_id, "name": str(workspace_name or "")}],
        dark_mode=bool(dark_mode),
    )

    def _convert(action: dict):
        vendor = _VENDORS.get(str(action.get("vendor") or ""))
        if vendor is None:
            raise ValueError("Select the vendor to convert from.")
        raw_values = action.get("values") or {}
        values = {
            f["key"]: str(raw_values.get(f["key"]) or "") for f in vendor["fields"]
        }
        missing = [
            f["label"]
            for f in vendor["fields"]
            if f.get("required", True) and not values[f["key"]].strip()
        ]
        if missing:
            raise ValueError(f"Provide the following: {', '.join(missing)}.")
        invalid = [
            f"{f['label']}: {f.get('pattern_error') or 'invalid value.'}"
            for f in vendor["fields"]
            if f.get("pattern")
            and values[f["key"]].strip()
            and not re.fullmatch(f["pattern"], values[f["key"]].strip(), re.I)
        ]
        if invalid:
            raise ValueError(" ".join(invalid))

        source_type = str(action.get("source_type") or "")
        source_workspace_id = str(action.get("source_workspace_id") or "")
        source_item_id = str(action.get("source_item_id") or "")
        if source_type not in _SOURCE_TYPES:
            raise ValueError(
                f"The source type must be one of: {', '.join(_SOURCE_TYPES)}."
            )
        if not source_workspace_id or not source_item_id:
            raise ValueError(f"Select the source workspace and {source_type.lower()}.")

        target_workspace_id = str(action.get("workspace_id") or "")
        target_workspace_name = str(action.get("workspace_name") or "")
        name = str(action.get("name") or "").strip()
        test_run = bool(action.get("test_run", True))
        if not target_workspace_id:
            raise ValueError("Select the destination workspace.")

        if not test_run:
            if not name:
                raise ValueError("Enter a name for the semantic model.")
            existing = _list_semantic_models(target_workspace_id)
            loaded = dict(widget.datasets)
            loaded[target_workspace_id] = existing
            widget.datasets = loaded
            if any(d["name"].strip().lower() == name.lower() for d in existing):
                raise ValueError(
                    f"A semantic model named '{name}' already exists in the "
                    f"'{target_workspace_name or target_workspace_id}' workspace."
                )

        bim = vendor["convert"](
            values,
            source_item=source_item_id,
            source_type=source_type,
            source_workspace=source_workspace_id,
            workspace=target_workspace_id,
            name=name or None,
            test_run=test_run,
        )

        if not test_run:
            loaded = dict(widget.datasets)
            loaded[target_workspace_id] = list(
                loaded.get(target_workspace_id) or []
            ) + [{"id": "", "name": name}]
            widget.datasets = loaded

        summary = _summarize_bim(bim)
        unconverted = summary.pop("unconverted")
        findings = []
        if unconverted:
            findings.append(
                {
                    "element": "Measures",
                    "detail": "These measures could not be converted and return BLANK().",
                    "items": [
                        {"name": f"'{u['table']}'[{u['measure']}]", "note": u["reason"]}
                        for u in unconverted
                    ],
                }
            )
        if vendor.get("inspect"):
            findings.extend(vendor["inspect"](values))

        widget.result = {
            "kind": "preview" if test_run else "created",
            "vendor": f"{vendor['label']} {vendor['object'].lower()}",
            "name": name or str(bim.get("name") or ""),
            "workspace_name": target_workspace_name,
            **summary,
            "findings": findings,
            "model": _model_view(bim),
        }

    def _handle_action(action: dict):
        act = action.get("action")
        if act == "list_workspaces":
            widget.workspaces = _list_picker_workspaces(workspace_id, workspace_name)
        elif act == "list_datasets":
            target = str(action.get("workspace_id") or "")
            if target:
                loaded = dict(widget.datasets)
                loaded[target] = _list_picker_datasets(target)
                widget.datasets = loaded
        elif act == "list_items":
            target = str(action.get("workspace_id") or "")
            item_type = str(action.get("item_type") or "")
            if target and item_type in _SOURCE_TYPES:
                loaded = dict(widget.source_items)
                loaded[f"{item_type}:{target}"] = _SOURCE_TYPES[item_type](target)
                widget.source_items = loaded
        elif act == "convert":
            widget.status = {}
            _convert(action)

    def _on_run(_change):
        action = dict(widget.pending_action or {})
        try:
            _handle_action(action)
        except Exception as e:
            widget.status = {"message": str(e), "kind": "error"}
        finally:
            if action.get("action") == "convert":
                # Drops the entered credentials from the synced widget state.
                widget.pending_action = {}
            # Releases the frontend's next queued action.
            widget.action_done = widget.action_done + 1

    widget.observe(_on_run, names=["run"])

    # The observer closure keeps the widget alive; returning it would render it twice.
    display(widget)


_WIDGET_CSS = """
.slls-cv {
    __LIGHT__
    __SYNTAX__
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "Helvetica Neue", Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    color: var(--ui-text);
    width: 100%;
    max-width: 860px;
    background: var(--ui-bg);
    border: 1px solid var(--ui-border);
    border-radius: 16px;
    box-shadow: var(--ui-shadow-lg);
    box-sizing: border-box;
}
.slls-cv.slls-cv-dark { __DARK__ }
.slls-cv * { box-sizing: border-box; }
.slls-cv [hidden] { display: none !important; }

.slls-cv-head { padding: 20px 24px 16px 24px; border-bottom: 1px solid var(--ui-border); }
.slls-cv-body { padding: 4px 24px 20px 24px; }
.slls-cv-section { padding-top: 18px; }
.slls-cv-section + .slls-cv-section { margin-top: 18px; border-top: 1px solid var(--ui-border); }
.slls-cv-label {
    font-size: 11.5px; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.05em; color: var(--ui-text-tertiary); margin-bottom: 10px;
}
.slls-cv-hint { font-size: 12.5px; line-height: 1.5; color: var(--ui-text-secondary); margin: -4px 0 12px 0; }
.slls-cv-hint a { color: var(--ui-accent); text-decoration: none; }
.slls-cv-hint a:hover { text-decoration: underline; }

/* ---- Vendor choice ---- */
.slls-cv-vendors { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 10px; }
.slls-cv-vendor {
    appearance: none; -webkit-appearance: none;
    display: flex; align-items: center; gap: 12px; padding: 12px 14px;
    border: 1px solid var(--ui-border-strong); border-radius: 12px;
    background: var(--ui-bg); color: var(--ui-text);
    font-family: inherit; text-align: left; cursor: pointer;
    transition: border-color 120ms ease, background 120ms ease, box-shadow 120ms ease;
}
.slls-cv-vendor:hover { border-color: var(--ui-text-tertiary); }
.slls-cv-vendor:focus-visible { outline: 2px solid var(--ui-accent); outline-offset: 2px; }
.slls-cv-vendor.slls-cv-active {
    border-color: var(--ui-accent); background: var(--ui-accent-soft);
    box-shadow: inset 0 0 0 1px var(--ui-accent);
}
.slls-cv-vendor-ic {
    width: 32px; height: 32px; border-radius: 9px; flex-shrink: 0;
    display: inline-flex; align-items: center; justify-content: center;
    background: var(--ui-bg-secondary); color: var(--ui-accent);
}
.slls-cv-vendor-ic svg { display: block; width: 17px; height: 17px; }
.slls-cv-vendor-text { display: flex; flex-direction: column; min-width: 0; }
.slls-cv-vendor-name { font-size: 14px; font-weight: 600; }
.slls-cv-vendor-obj { font-size: 12px; color: var(--ui-text-secondary); margin-top: 1px; }

/* ---- Fields ---- */
__SEARCH_SELECT_CSS__
.slls-cv-grid { display: flex; flex-wrap: wrap; gap: 14px 12px; }
.slls-cv-field { display: flex; flex-direction: column; gap: 6px; flex: 1 1 220px; min-width: 0; }
.slls-cv-field.slls-cv-full { flex-basis: 100%; }
.slls-cv-field.slls-cv-fit { flex: 0 0 auto; }
.slls-cv-field-label { font-size: 12.5px; font-weight: 500; color: var(--ui-text-secondary); }
.slls-cv-labelrow { display: flex; align-items: center; gap: 10px; min-width: 0; }
.slls-cv-input, .slls-cv-textarea {
    width: 100%; appearance: none; -webkit-appearance: none;
    background: var(--ui-bg); border: 1px solid var(--ui-border-strong);
    border-radius: 10px; padding: 10px 12px; font-size: 14px; font-family: inherit;
    color: var(--ui-text); outline: none; transition: border-color 120ms ease;
}
.slls-cv-input:focus, .slls-cv-textarea:focus { border-color: var(--ui-accent); }
.slls-cv-input::placeholder, .slls-cv-textarea::placeholder { color: var(--ui-text-tertiary); }
.slls-cv-input[aria-invalid="true"] { border-color: var(--ui-danger-border); }
.slls-cv-textarea {
    min-height: 190px; resize: vertical; white-space: pre; tab-size: 2;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 12.5px; line-height: 1.5;
}
.slls-cv-fieldhelp { font-size: 12px; color: var(--ui-text-tertiary); }

/* ---- Highlighted code editor (transparent textarea over a colored <pre>) ---- */
.slls-cv-code { position: relative; border-radius: 10px; background: var(--ui-bg); }
.slls-cv-code-hl, .slls-cv-textarea.slls-cv-code-input {
    margin: 0; padding: 10px 12px; border: 1px solid transparent; border-radius: 10px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 12.5px; line-height: 1.5; letter-spacing: normal;
    white-space: pre; word-wrap: normal; tab-size: 2;
}
.slls-cv-code-hl {
    position: absolute; inset: 0; overflow: hidden; pointer-events: none;
    color: var(--ui-text); background: transparent;
}
.slls-cv-textarea.slls-cv-code-input {
    position: relative; display: block; background: transparent;
    border-color: var(--ui-border-strong);
    color: transparent; -webkit-text-fill-color: transparent; caret-color: var(--ui-text);
}
.slls-cv-textarea.slls-cv-code-input:focus { border-color: var(--ui-accent); }
.slls-cv-textarea.slls-cv-code-input::placeholder {
    color: var(--ui-text-tertiary); -webkit-text-fill-color: var(--ui-text-tertiary);
}
.slls-cv-textarea.slls-cv-code-input::selection { background: var(--ui-accent-soft); }
.slls-cv-tk-key { color: var(--ui-syntax-keyword); }
.slls-cv-tk-string { color: var(--ui-syntax-string); }
.slls-cv-tk-number { color: var(--ui-syntax-number); }
.slls-cv-tk-bool { color: var(--ui-syntax-variable); }
.slls-cv-tk-comment { color: var(--ui-syntax-comment); font-style: italic; }
.slls-cv-tk-punct { color: var(--ui-syntax-punctuation); }
.slls-cv-tk-kw { color: var(--ui-syntax-keyword); }
.slls-cv-tk-fn { color: var(--ui-syntax-function); }
.slls-cv-tk-col { color: var(--ui-syntax-virtual-column); }
.slls-cv-upload {
    margin-left: auto; flex-shrink: 0;
    display: inline-flex; align-items: center; gap: 6px;
    border: 1px solid var(--ui-border-strong); background: var(--ui-surface);
    color: var(--ui-text); font-family: inherit; font-size: 12.5px; font-weight: 500;
    padding: 5px 10px; border-radius: 8px; cursor: pointer;
    transition: border-color 120ms ease, color 120ms ease;
}
.slls-cv-upload:hover { border-color: var(--ui-accent); color: var(--ui-accent); }
.slls-cv-upload svg { display: block; width: 14px; height: 14px; }
.slls-cv-filename {
    font-size: 12px; color: var(--ui-text-tertiary); min-width: 0;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.slls-cv-seg { display: inline-flex; background: var(--ui-bg-secondary); border: 1px solid var(--ui-border); border-radius: 10px; padding: 3px; }
.slls-cv-seg-btn {
    border: none; background: none; color: var(--ui-text-secondary);
    font-size: 13.5px; font-weight: 500; font-family: inherit;
    padding: 7px 14px; border-radius: 8px; cursor: pointer; white-space: nowrap;
    transition: background 120ms ease, color 120ms ease;
}
.slls-cv-seg-btn.slls-cv-active { background: var(--ui-accent-soft); color: var(--ui-accent); font-weight: 600; }
.slls-cv-namemsg {
    display: flex; align-items: center; gap: 6px; min-height: 18px;
    font-size: 12.5px; color: var(--ui-text-tertiary);
}
.slls-cv-namemsg svg { display: block; width: 14px; height: 14px; flex-shrink: 0; }
.slls-cv-namemsg.slls-cv-ok { color: var(--ui-text-secondary); }
.slls-cv-namemsg.slls-cv-ok svg { color: var(--ui-accent); }
.slls-cv-namemsg.slls-cv-error { color: var(--ui-danger-text); }

/* ---- Status / footer ---- */
.slls-cv-status {
    display: flex; align-items: flex-start; gap: 10px; margin-top: 18px;
    padding: 12px 14px; font-size: 13px; line-height: 1.5;
    border: 1px solid var(--ui-danger-border); border-radius: 10px;
    background: var(--ui-danger-bg); color: var(--ui-danger-text);
    white-space: pre-wrap; word-break: break-word;
}
.slls-cv-status svg { display: block; width: 15px; height: 15px; flex-shrink: 0; margin-top: 2px; }
.slls-cv-footer { display: flex; align-items: center; justify-content: flex-end; gap: 10px; margin-top: 22px; }
.slls-cv-footer .slls-cv-namemsg { margin-right: auto; }
.slls-cv-btn-primary, .slls-cv-btn-secondary {
    display: inline-flex; align-items: center; justify-content: center; gap: 8px;
    font-size: 14px; font-weight: 600; font-family: inherit;
    padding: 9px 20px; border-radius: 10px; cursor: pointer; min-width: 110px;
    transition: background 120ms ease, opacity 120ms ease, border-color 120ms ease;
}
.slls-cv-btn-primary { border: 1px solid transparent; background: var(--ui-accent); color: var(--ui-on-accent); }
.slls-cv-btn-primary:hover:not(:disabled) { background: var(--ui-accent-hover); }
.slls-cv-btn-secondary { border: 1px solid var(--ui-border-strong); background: var(--ui-bg); color: var(--ui-text); }
.slls-cv-btn-secondary:hover:not(:disabled) { background: var(--ui-bg-hover); }
.slls-cv-btn-primary:disabled, .slls-cv-btn-secondary:disabled { opacity: 0.5; cursor: not-allowed; }
.slls-cv-spin {
    display: inline-block; width: 15px; height: 15px; border-radius: 50%;
    border: 2px solid currentColor; border-right-color: transparent;
    animation: slls-cv-spin 0.7s linear infinite;
}
@keyframes slls-cv-spin { to { transform: rotate(360deg); } }

/* ---- Result ---- */
.slls-cv-banner {
    display: flex; align-items: flex-start; gap: 12px; margin-top: 18px;
    padding: 14px 16px; border: 1px solid var(--ui-border); border-radius: 12px;
    background: var(--ui-bg-secondary);
}
.slls-cv-banner-ic {
    width: 34px; height: 34px; border-radius: 9px; flex-shrink: 0;
    display: inline-flex; align-items: center; justify-content: center;
    background: var(--ui-accent-soft); color: var(--ui-accent);
}
.slls-cv-banner-ic svg { display: block; width: 18px; height: 18px; }
.slls-cv-banner-title { font-size: 15px; font-weight: 600; }
.slls-cv-banner-sub { font-size: 13px; line-height: 1.5; color: var(--ui-text-secondary); margin-top: 2px; }
.slls-cv-banner-sub b { color: var(--ui-text); font-weight: 600; }
.slls-cv-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; margin-top: 14px; }
.slls-cv-card { padding: 12px 14px; border: 1px solid var(--ui-border); border-radius: 12px; background: var(--ui-bg); }
.slls-cv-card-label {
    font-size: 11.5px; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.05em; color: var(--ui-text-tertiary);
}
.slls-cv-card-value { font-size: 24px; font-weight: 600; letter-spacing: -0.01em; margin-top: 4px; font-variant-numeric: tabular-nums; }
.slls-cv-card-sub { font-size: 12px; color: var(--ui-text-tertiary); margin-top: 2px; }
.slls-cv-sechead { display: flex; align-items: center; gap: 8px; margin: 24px 0 10px 0; }
.slls-cv-sechead .slls-cv-label { margin: 0; }
.slls-cv-sechead + .slls-cv-hint { margin-top: -4px; }
.slls-cv-tools { margin-left: auto; display: inline-flex; gap: 6px; }
.slls-cv-tool {
    width: 28px; height: 28px; padding: 0; border-radius: 8px; cursor: pointer;
    display: inline-flex; align-items: center; justify-content: center;
    border: 1px solid var(--ui-border-strong); background: var(--ui-surface); color: var(--ui-text-secondary);
    transition: border-color 120ms ease, color 120ms ease;
}
.slls-cv-tool:hover { border-color: var(--ui-accent); color: var(--ui-accent); }
.slls-cv-tool svg { display: block; width: 15px; height: 15px; }
.slls-cv-allgood {
    display: flex; align-items: center; gap: 8px; padding: 10px 12px; font-size: 13px;
    border: 1px solid var(--ui-border); border-radius: 12px; color: var(--ui-text-secondary);
}
.slls-cv-allgood svg { display: block; width: 15px; height: 15px; color: var(--ui-accent); }

/* ---- Collapsible tree (findings + model view) ---- */
.slls-cv-tree { border: 1px solid var(--ui-border); border-radius: 12px; overflow: hidden; }
.slls-cv-tree > .slls-cv-node + .slls-cv-node { border-top: 1px solid var(--ui-border); }
.slls-cv-node > summary {
    list-style: none; display: flex; align-items: center; gap: 8px; min-width: 0;
    padding: 9px 12px; font-size: 13px; cursor: pointer; user-select: none;
}
.slls-cv-node > summary::-webkit-details-marker { display: none; }
.slls-cv-node > summary:hover { background: var(--ui-surface-2); }
.slls-cv-node > summary:focus-visible { outline: 2px solid var(--ui-accent); outline-offset: -2px; }
.slls-cv-caret { display: inline-flex; width: 10px; flex-shrink: 0; color: var(--ui-text-tertiary); transition: transform 120ms ease; }
.slls-cv-node[open] > summary > .slls-cv-caret { transform: rotate(90deg); }
.slls-cv-node-ic { display: inline-flex; flex-shrink: 0; color: var(--ui-text-tertiary); }
.slls-cv-node-ic svg { display: block; width: 14px; height: 14px; }
.slls-cv-node-ic.slls-cv-warn-ic { color: var(--ui-warning-text); }
.slls-cv-node-name { font-weight: 600; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-cv-node-meta {
    margin-left: auto; flex-shrink: 0; font-size: 12px; color: var(--ui-text-tertiary);
    white-space: nowrap; font-variant-numeric: tabular-nums;
}
.slls-cv-node-body { padding: 2px 12px 12px 30px; }
.slls-cv-node.slls-cv-sub { border: 1px solid var(--ui-border); border-radius: 10px; margin-bottom: 6px; overflow: hidden; }
.slls-cv-node.slls-cv-sub > summary { padding: 7px 10px; }
.slls-cv-node.slls-cv-sub > summary .slls-cv-node-name { font-weight: 500; }
.slls-cv-node.slls-cv-sub > .slls-cv-node-body { padding: 0 10px 10px 10px; }
.slls-cv-badge {
    flex-shrink: 0; padding: 1px 8px; border-radius: 10px;
    font-size: 10.5px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em;
    background: var(--ui-bg-secondary); color: var(--ui-text-secondary);
}
.slls-cv-badge.slls-cv-badge-warn { background: var(--ui-warning-bg); color: var(--ui-warning-text); }
.slls-cv-find-detail { font-size: 12.5px; line-height: 1.5; color: var(--ui-text-secondary); margin-bottom: 6px; }
.slls-cv-find-item { display: flex; gap: 12px; padding: 5px 0; font-size: 12.5px; border-top: 1px solid var(--ui-border); }
.slls-cv-find-name { flex: 0 0 auto; max-width: 50%; font-weight: 500; overflow-wrap: anywhere; }
.slls-cv-find-note { min-width: 0; color: var(--ui-text-tertiary); overflow-wrap: anywhere; }
.slls-cv-subhead {
    font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em;
    color: var(--ui-text-tertiary); margin: 12px 0 6px 0;
}
.slls-cv-cols { display: grid; grid-template-columns: minmax(0, 1.4fr) minmax(0, 0.7fr) minmax(0, 1.3fr) auto; font-size: 12.5px; }
.slls-cv-cols > div { padding: 5px 8px 5px 0; border-top: 1px solid var(--ui-border); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-cv-cols > .slls-cv-colhead {
    border-top: none; font-size: 11px; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.04em; color: var(--ui-text-tertiary);
}
.slls-cv-muted { color: var(--ui-text-tertiary); }
.slls-cv-codelabel {
    font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em;
    color: var(--ui-text-tertiary); margin: 8px 0 4px 0;
}
.slls-cv-codeblock {
    margin: 0; padding: 8px 10px; border: 1px solid var(--ui-border); border-radius: 8px;
    background: var(--ui-bg-tertiary); color: var(--ui-text);
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 12px; line-height: 1.5; white-space: pre-wrap; overflow-wrap: anywhere;
}
.slls-cv-rel {
    display: flex; align-items: center; gap: 8px; padding: 6px 0; border-top: 1px solid var(--ui-border);
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px;
}
.slls-cv-rel:first-child { border-top: none; }
.slls-cv-rel .slls-cv-node-meta { font-family: inherit; }
.slls-cv-attr { padding: 0 24px 6px 24px; }
"""


_WIDGET_JS = r"""
__SEARCH_SELECT_JS__

__FULLSCREEN_JS__

function render({ model, el }) {
    const IC = {
        convert: `__IC_CONVERT__`, source: `__IC_SOURCE__`, upload: `__IC_UPLOAD__`,
        refresh: `__IC_REFRESH__`, check: `__IC_CHECK__`, checkCircle: `__IC_CHECK_CIRCLE__`,
        alert: `__IC_ALERT__`, eye: `__IC_EYE__`, caret: `__IC_CARET__`, table: `__IC_TABLE__`,
        measure: `__IC_MEASURE__`, calcColumn: `__IC_CALC_COLUMN__`, relationship: `__IC_RELATIONSHIP__`,
        expand: `__IC_EXPAND__`, collapse: `__IC_COLLAPSE__`,
    };
    const SUN = `__IC_SUN__`, MOON = `__IC_MOON__`, FS = `__IC_FS__`, FSX = `__IC_FSX__`;
    const uid = Math.random().toString(36).slice(2, 10);
    const vendors = model.get("vendors") || [];
    const sourceTypes = model.get("source_types") || [];

    function esc(s) {
        return String(s === null || s === undefined ? "" : s)
            .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
    }
    function make(tag, cls, html) {
        const node = document.createElement(tag);
        if (cls) node.className = cls;
        if (html !== undefined) node.innerHTML = html;
        return node;
    }

    const root = make("div", "slls-cv");
    el.appendChild(root);
    root.innerHTML = `
        <div class="slls-cv-head">
            <div class="sl-header">
                <span class="sl-title-icon">${IC.convert}</span>
                <div class="sl-titlewrap">
                    <div class="sl-title-row"><div class="sl-title">Semantic Model Converter</div></div>
                    <div class="sl-subtitle">Convert another vendor's semantic model into a Power BI semantic model</div>
                </div>
                <div class="sl-head-spacer"></div>
                <button class="sl-reload-btn" data-r="reload" type="button"
                    title="Reload workspaces, semantic models and items"
                    aria-label="Reload workspaces, semantic models and items">${IC.refresh}</button>
                <button class="sl-theme-btn" data-r="fs" type="button"></button>
                <button class="sl-theme-btn" data-r="theme" type="button"></button>
            </div>
        </div>
        <div class="slls-cv-body" data-r="form">
            <div class="slls-cv-section">
                <div class="slls-cv-label">Convert from</div>
                <div class="slls-cv-vendors" data-r="vendors" role="radiogroup" aria-label="Source vendor"></div>
            </div>
            <div class="slls-cv-section">
                <div class="slls-cv-label" data-r="deftitle"></div>
                <div class="slls-cv-hint" data-r="defhint"></div>
                <div class="slls-cv-grid" data-r="fields"></div>
            </div>
            <div class="slls-cv-section">
                <div class="slls-cv-label">Direct Lake source</div>
                <div class="slls-cv-hint">The Fabric item containing the tables referenced by the definition.</div>
                <div class="slls-cv-grid">
                    <div class="slls-cv-field"><span class="slls-cv-field-label">Workspace</span><div data-r="srcws"></div></div>
                    <div class="slls-cv-field slls-cv-fit">
                        <span class="slls-cv-field-label">Item type</span>
                        <div class="slls-cv-seg" data-r="srctype" role="radiogroup" aria-label="Item type"></div>
                    </div>
                    <div class="slls-cv-field"><span class="slls-cv-field-label" data-r="srcitemlabel"></span><div data-r="srcitem"></div></div>
                </div>
            </div>
            <div class="slls-cv-section">
                <div class="slls-cv-label">Destination</div>
                <div class="slls-cv-grid">
                    <div class="slls-cv-field"><span class="slls-cv-field-label">Workspace</span><div data-r="destws"></div></div>
                    <div class="slls-cv-field">
                        <label class="slls-cv-field-label" for="slls-cv-${uid}-name">Semantic model name</label>
                        <input class="slls-cv-input" id="slls-cv-${uid}-name" data-r="name" type="text" autocomplete="off" spellcheck="false" placeholder="Name of the new semantic model">
                        <div class="slls-cv-namemsg" data-r="namemsg" aria-live="polite"></div>
                    </div>
                </div>
            </div>
            <div data-r="status"></div>
            <div class="slls-cv-footer">
                <button class="slls-cv-btn-primary" data-r="preview" type="button">Preview</button>
            </div>
        </div>
        <div class="slls-cv-body" data-r="result" hidden></div>
        <div class="slls-cv-attr">__ATTRIBUTION__</div>`;

    const q = (r) => root.querySelector(`[data-r="${r}"]`);
    const formBox = q("form");
    const resultBox = q("result");
    const reloadBtn = q("reload");
    const themeBtn = q("theme");
    const nameInput = q("name");
    const previewBtn = q("preview");

    // ---- Theme / full screen ----
    function applyTheme() {
        const dark = model.get("dark_mode") === true;
        root.classList.toggle("slls-cv-dark", dark);
        themeBtn.innerHTML = dark ? SUN : MOON;
        themeBtn.title = dark ? "Switch to light mode" : "Switch to dark mode";
        themeBtn.setAttribute("aria-label", themeBtn.title);
    }
    applyTheme();
    model.on("change:dark_mode", applyTheme);
    themeBtn.addEventListener("click", () => {
        model.set("dark_mode", model.get("dark_mode") !== true);
        model.save_changes();
        applyTheme();
    });
    sllsSetupFullscreen(root, q("fs"), "slls-cv-fs", FS, FSX);

    // ---- Python actions (sent one at a time; see action_done) ----
    const queue = [];
    let inFlight = null;
    let reloadPending = 0;
    let workspacesLoaded = false;
    let converting = false;
    function dispatch(payload) {
        queue.push(payload);
        pump();
    }
    function pump() {
        if (inFlight || queue.length === 0) return;
        inFlight = queue.shift();
        model.set("pending_action", inFlight);
        model.set("run", (model.get("run") || 0) + 1);
        model.save_changes();
    }
    model.on("change:action_done", () => {
        const done = inFlight;
        inFlight = null;
        if (done && done.action === "list_workspaces") {
            workspacesLoaded = true;
            updatePickers();
        }
        if (done && done.reload) {
            reloadPending -= 1;
            if (reloadPending <= 0) {
                reloadPending = 0;
                reloadBtn.classList.remove("sl-spinning");
            }
        }
        if (done && done.action === "convert") {
            converting = false;
            clearBusy();
            updateValidity();
        }
        pump();
    });

    // ---- State ----
    let vendorKey = vendors.length ? vendors[0].key : "";
    const values = {};
    const fileNames = {};
    let srcWs = model.get("workspace_id") || "";
    let srcType = sourceTypes.length ? sourceTypes[0] : "Lakehouse";
    let srcItem = "";
    let destWs = model.get("workspace_id") || "";
    let modelName = "";
    let nameTouched = false;
    let workspacesRequested = false;
    const datasetsRequested = {};
    const itemsRequested = {};

    function vendor() { return vendors.find((v) => v.key === vendorKey) || null; }
    function vendorValues() { return values[vendorKey] || (values[vendorKey] = {}); }
    function itemKey() { return srcType + ":" + srcWs; }

    function ensureWorkspaces() {
        if (workspacesRequested) return;
        workspacesRequested = true;
        dispatch({ action: "list_workspaces" });
    }
    function ensureDatasets(wsId) {
        if (!wsId || (model.get("datasets") || {})[wsId] || datasetsRequested[wsId]) return;
        datasetsRequested[wsId] = true;
        dispatch({ action: "list_datasets", workspace_id: wsId });
    }
    function ensureItems() {
        const key = itemKey();
        if (!srcWs || (model.get("source_items") || {})[key] || itemsRequested[key]) return;
        itemsRequested[key] = true;
        dispatch({ action: "list_items", workspace_id: srcWs, item_type: srcType });
    }

    // ---- Vendor + vendor-specific fields ----
    function renderVendors() {
        const box = q("vendors");
        box.innerHTML = "";
        for (const v of vendors) {
            const active = v.key === vendorKey;
            const btn = make("button", "slls-cv-vendor" + (active ? " slls-cv-active" : ""),
                `<span class="slls-cv-vendor-ic">${IC.source}</span>`
                + `<span class="slls-cv-vendor-text"><span class="slls-cv-vendor-name">${esc(v.label)}</span>`
                + `<span class="slls-cv-vendor-obj">${esc(v.object)}</span></span>`);
            btn.type = "button";
            btn.setAttribute("role", "radio");
            btn.setAttribute("aria-checked", String(active));
            btn.addEventListener("click", () => {
                if (v.key === vendorKey) return;
                vendorKey = v.key;
                renderVendors();
                renderFields();
                updateValidity();
            });
            box.appendChild(btn);
        }
    }

    // Suggests the model name from a YAML definition's top-level ``name:``.
    function suggestName(text) {
        const match = /^name:[ \t]*(['"]?)([^\r\n#'"]*)\1[ \t]*(?:#.*)?$/m.exec(text || "");
        return match ? match[2].trim() : "";
    }

    // ---- YAML syntax highlighting ----
    function tok(kind, text) {
        return text ? `<span class="slls-cv-tk-${kind}">${esc(text)}</span>` : "";
    }
    function scalarKind(v) {
        if (/^([|>][-+0-9]*|-{3}|\.{3})$/.test(v)) return "punct";
        if (/^(true|false|yes|no|on|off|null|~)$/i.test(v) || /^[&*]\S+$/.test(v)) return "bool";
        if (/^[-+]?(\d[\d_]*(\.\d*)?|\.\d+)([eE][-+]?\d+)?$/.test(v)) return "number";
        return "string";
    }
    function scalarHtml(body) {
        if (!/^[\[{]/.test(body)) return tok(scalarKind(body), body);
        // Flow collections, e.g. synonyms: ["a", 'b', c]
        let html = "";
        const re = /("(?:[^"\\]|\\.)*"?|'(?:[^']|'')*'?)|([\[\]{},:])|([^\[\]{},:"']+)/g;
        let m;
        while ((m = re.exec(body)) !== null) {
            if (m[1]) html += tok("string", m[1]);
            else if (m[2]) html += tok("punct", m[2]);
            else {
                const word = m[3].trim();
                const lead = m[3].slice(0, m[3].indexOf(word));
                html += esc(lead) + tok(scalarKind(word), word) + esc(m[3].slice(lead.length + word.length));
            }
        }
        return html;
    }
    function valueHtml(text) {
        const lead = /^\s*/.exec(text)[0];
        let body = text.slice(lead.length);
        let comment = "";
        let inSingle = false, inDouble = false;
        for (let i = 0; i < body.length; i++) {
            const c = body[i];
            if (c === "'" && !inDouble) inSingle = !inSingle;
            else if (c === '"' && !inSingle) inDouble = !inDouble;
            else if (c === "#" && !inSingle && !inDouble && (i === 0 || /\s/.test(body[i - 1]))) {
                comment = body.slice(i);
                body = body.slice(0, i);
                break;
            }
        }
        const trail = /\s*$/.exec(body)[0];
        body = body.slice(0, body.length - trail.length);
        return esc(lead) + (body ? scalarHtml(body) : "") + esc(trail) + tok("comment", comment);
    }
    function highlightYaml(text) {
        const out = [];
        // Indentation of the line that opened a block scalar (``key: |``), or -1.
        let blockIndent = -1;
        for (const line of String(text || "").split("\n")) {
            const indent = /^ */.exec(line)[0].length;
            if (blockIndent >= 0) {
                if (line.trim() === "" || indent > blockIndent) {
                    out.push(tok("string", line));
                    continue;
                }
                blockIndent = -1;
            }
            const lead = /^(\s*)(-(?=\s|$)\s*)?/.exec(line);
            let html = esc(lead[1]) + tok("punct", lead[2] || "");
            let rest = line.slice(lead[0].length);
            if (/^#/.test(rest)) {
                out.push(html + tok("comment", rest));
                continue;
            }
            const key = /^("(?:[^"\\]|\\.)*"|'(?:[^']|'')*'|[^\s#'"{}\[\],][^:#]*?)(\s*:)(?=\s|$)/.exec(rest);
            if (key) {
                html += tok("key", key[1]) + tok("punct", key[2]);
                rest = rest.slice(key[0].length);
            }
            html += valueHtml(rest);
            if (/^\s*[|>][-+0-9]*\s*(#.*)?$/.test(rest)) blockIndent = indent;
            out.push(html);
        }
        return out.join("\n");
    }

    // ---- SQL / DAX syntax highlighting ----
    const SQL_KEYWORDS = new Set(("SELECT FROM WHERE AND OR NOT CASE WHEN THEN ELSE END AS IS NULL IN LIKE ILIKE "
        + "BETWEEN DISTINCT OVER PARTITION BY ORDER ASC DESC ROWS RANGE PRECEDING FOLLOWING CURRENT ROW "
        + "UNBOUNDED TRUE FALSE JOIN LEFT RIGHT INNER OUTER ON GROUP HAVING LIMIT INTERVAL EXCLUDING WITH QUALIFY").split(" "));
    const DAX_KEYWORDS = new Set("VAR RETURN IN NOT AND OR TRUE FALSE ASC DESC DEFINE EVALUATE MEASURE ORDER BY".split(" "));
    // Groups: 1 comment, 2 string, 3 quoted name, 4 [bracketed] reference, 5 number, 6 word, 7 operator.
    const CODE_RE = {
        sql: /(--[^\n]*|\/\*[\s\S]*?(?:\*\/|$))|('(?:[^']|'')*'?)|("(?:[^"]|"")*"?)|(\[[^\]\n]*\]?)|(\b\d+(?:\.\d+)?(?:[eE][-+]?\d+)?\b)|([A-Za-z_][A-Za-z0-9_$]*)|([^\sA-Za-z0-9_])/g,
        dax: /(\/\/[^\n]*|--[^\n]*|\/\*[\s\S]*?(?:\*\/|$))|("(?:[^"]|"")*"?)|('(?:[^']|'')*'?)|(\[[^\]\n]*\]?)|(\b\d+(?:\.\d+)?(?:[eE][-+]?\d+)?\b)|([A-Za-z_][A-Za-z0-9_.]*)|([^\sA-Za-z0-9_])/g,
    };
    function highlightCode(text, lang) {
        const src = String(text || "");
        const dax = lang === "dax";
        const re = CODE_RE[dax ? "dax" : "sql"];
        const keywords = dax ? DAX_KEYWORDS : SQL_KEYWORDS;
        re.lastIndex = 0;
        let out = "", last = 0, m;
        while ((m = re.exec(src)) !== null) {
            out += esc(src.slice(last, m.index));
            last = re.lastIndex;
            let kind = "";
            if (m[1]) kind = "comment";
            else if (m[2]) kind = "string";
            else if (m[4]) kind = dax ? "col" : "";
            else if (m[5]) kind = "number";
            else if (m[6]) {
                if (/^\s*\(/.test(src.slice(last, last + 64))) kind = "fn";
                else if (keywords.has(m[6].toUpperCase())) kind = "kw";
            } else if (m[7]) kind = "punct";
            out += kind ? tok(kind, m[0]) : esc(m[0]);
        }
        return out + esc(src.slice(last));
    }

    // ---- Field validation ----
    const touched = {};
    function fieldError(f, value) {
        const v = String(value || "").trim();
        if (!f.pattern || !v) return "";
        let re;
        try { re = new RegExp(f.pattern, "i"); } catch (e) { return ""; }
        return re.test(v) ? "" : (f.pattern_error || "Invalid value.");
    }

    function onValue(field, value) {
        vendorValues()[field.key] = value;
        if (field.type === "definition" && !nameTouched) {
            const suggested = suggestName(value);
            if (suggested) {
                modelName = suggested;
                nameInput.value = suggested;
                updateNameMsg();
            }
        }
        updateValidity();
    }

    function renderFields() {
        const v = vendor();
        const box = q("fields");
        box.innerHTML = "";
        q("deftitle").textContent = v ? `${v.label} ${v.object.toLowerCase()}` : "";
        const hint = q("defhint");
        hint.innerHTML = v ? esc(v.description || "")
            + (v.docs_url ? ` <a href="${esc(v.docs_url)}" target="_blank" rel="noopener noreferrer">Specification</a>` : "") : "";
        if (!v) return;
        const vals = vendorValues();
        for (const f of v.fields) {
            const id = `slls-cv-${uid}-${vendorKey}-${f.key}`;
            const wrap = make("div", "slls-cv-field" + (f.type === "definition" ? " slls-cv-full" : ""));
            const label = make("label", "slls-cv-field-label");
            label.htmlFor = id;
            label.textContent = f.label;
            let input;
            let hl = null;
            let errBox = null;
            const paint = () => {
                if (!hl) return;
                // A trailing space keeps a final empty line, so both layers scroll equally.
                hl.innerHTML = highlightYaml(input.value) + (input.value.endsWith("\n") ? " " : "");
                hl.scrollTop = input.scrollTop;
                hl.scrollLeft = input.scrollLeft;
            };
            if (f.type === "definition") {
                const row = make("div", "slls-cv-labelrow");
                row.appendChild(label);
                const fname = make("span", "slls-cv-filename");
                fname.textContent = fileNames[id] || "";
                row.appendChild(fname);
                const upload = make("button", "slls-cv-upload", `${IC.upload}<span>Upload file</span>`);
                upload.type = "button";
                const file = document.createElement("input");
                file.type = "file";
                file.hidden = true;
                if (f.accept) file.accept = f.accept;
                upload.addEventListener("click", () => file.click());
                file.addEventListener("change", () => {
                    const picked = file.files && file.files[0];
                    file.value = "";
                    if (!picked) return;
                    const reader = new FileReader();
                    reader.onload = () => {
                        input.value = String(reader.result || "");
                        fileNames[id] = picked.name;
                        fname.textContent = picked.name;
                        paint();
                        onValue(f, input.value);
                    };
                    reader.readAsText(picked);
                });
                row.appendChild(upload);
                row.appendChild(file);
                wrap.appendChild(row);
                input = make("textarea", "slls-cv-textarea");
                input.spellcheck = false;
                if (f.syntax === "yaml") {
                    hl = make("pre", "slls-cv-code-hl");
                    hl.setAttribute("aria-hidden", "true");
                    input.classList.add("slls-cv-code-input");
                    input.addEventListener("scroll", () => {
                        hl.scrollTop = input.scrollTop;
                        hl.scrollLeft = input.scrollLeft;
                    });
                }
                input.addEventListener("input", () => {
                    fileNames[id] = "";
                    fname.textContent = "";
                    paint();
                    onValue(f, input.value);
                });
            } else {
                wrap.appendChild(label);
                input = make("input", "slls-cv-input");
                input.type = f.type === "secret" ? "password" : "text";
                input.autocomplete = f.type === "secret" ? "new-password" : "off";
                input.spellcheck = false;
                input.addEventListener("input", () => onValue(f, input.value));
                if (f.pattern) {
                    errBox = make("div", "slls-cv-namemsg slls-cv-error");
                    errBox.setAttribute("aria-live", "polite");
                    // Errors appear once the field has been left, then update live.
                    const check = () => {
                        const err = touched[id] ? fieldError(f, input.value) : "";
                        errBox.innerHTML = err ? `${IC.alert}<span>${esc(err)}</span>` : "";
                        errBox.hidden = !err;
                        input.setAttribute("aria-invalid", String(!!err));
                    };
                    input.addEventListener("input", check);
                    input.addEventListener("blur", () => { touched[id] = true; check(); });
                    errBox.check = check;
                }
            }
            input.id = id;
            input.placeholder = f.placeholder || "";
            input.value = vals[f.key] || "";
            if (hl) {
                const code = make("div", "slls-cv-code");
                code.appendChild(hl);
                code.appendChild(input);
                wrap.appendChild(code);
                paint();
            } else {
                wrap.appendChild(input);
            }
            if (errBox) {
                wrap.appendChild(errBox);
                errBox.check();
            }
            if (f.help) {
                const help = make("div", "slls-cv-fieldhelp");
                help.textContent = f.help;
                wrap.appendChild(help);
            }
            box.appendChild(wrap);
        }
    }

    // ---- Direct Lake source + destination pickers ----
    const srcWsPicker = createSearchSelect({
        placeholder: "Select a workspace\u2026",
        searchPlaceholder: "Filter workspaces\u2026",
        ariaLabel: "Source workspace",
        emptyLabel: "Loading workspaces\u2026",
        onChange: (option) => {
            srcWs = option.value;
            srcItem = "";
            ensureItems();
            updatePickers();
            updateValidity();
        },
    });
    const srcItemPicker = createSearchSelect({
        searchPlaceholder: "Filter\u2026",
        ariaLabel: "Source item",
        onChange: (option) => {
            srcItem = option.value;
            updateValidity();
        },
    });
    const destWsPicker = createSearchSelect({
        placeholder: "Select a workspace\u2026",
        searchPlaceholder: "Filter workspaces\u2026",
        ariaLabel: "Destination workspace",
        emptyLabel: "Loading workspaces\u2026",
        onChange: (option) => {
            destWs = option.value;
            ensureDatasets(destWs);
            updateNameMsg();
            updateValidity();
        },
    });
    q("srcws").appendChild(srcWsPicker.el);
    q("srcitem").appendChild(srcItemPicker.el);
    q("destws").appendChild(destWsPicker.el);

    function renderSourceTypes() {
        const box = q("srctype");
        box.innerHTML = "";
        for (const t of sourceTypes) {
            const active = t === srcType;
            const btn = make("button", "slls-cv-seg-btn" + (active ? " slls-cv-active" : ""));
            btn.type = "button";
            btn.textContent = t;
            btn.setAttribute("role", "radio");
            btn.setAttribute("aria-checked", String(active));
            btn.addEventListener("click", () => {
                if (t === srcType) return;
                srcType = t;
                srcItem = "";
                renderSourceTypes();
                ensureItems();
                updatePickers();
                updateValidity();
            });
            box.appendChild(btn);
        }
        q("srcitemlabel").textContent = srcType;
    }

    function updatePickers() {
        const ws = (model.get("workspaces") || []).map((w) => ({ value: w.id, label: w.name }));
        const wsEmpty = workspacesLoaded ? "No workspaces" : "Loading workspaces\u2026";
        srcWsPicker.setEmptyLabel(wsEmpty);
        destWsPicker.setEmptyLabel(wsEmpty);
        srcWsPicker.setOptions(ws, srcWs);
        if (srcWsPicker.value !== srcWs) {
            srcWs = srcWsPicker.value;
            srcItem = "";
        }
        destWsPicker.setOptions(ws, destWs);
        destWs = destWsPicker.value;

        const items = srcWs ? (model.get("source_items") || {})[itemKey()] : undefined;
        const noun = srcType.toLowerCase();
        srcItemPicker.setPlaceholder(`Select a ${noun}\u2026`);
        srcItemPicker.setEmptyLabel(!srcWs ? "Select a workspace first\u2026"
            : (items === undefined ? `Loading ${noun}s\u2026` : `No ${noun}s in this workspace`));
        srcItemPicker.setOptions((items || []).map((i) => ({ value: i.id, label: i.name })), srcItem);
        srcItem = srcItemPicker.value;
        srcItemPicker.setDisabled(!srcWs || items === undefined);
    }

    // ---- Name validation ----
    function nameState() {
        const name = modelName.trim();
        if (!name) return { ok: false, kind: "", msg: "" };
        if (!destWs) return { ok: false, kind: "", msg: "Select the destination workspace." };
        const existing = (model.get("datasets") || {})[destWs];
        if (existing === undefined) {
            return { ok: false, kind: "pending", msg: "Checking existing semantic models\u2026" };
        }
        const lower = name.toLowerCase();
        if (existing.some((d) => String(d.name || "").trim().toLowerCase() === lower)) {
            return { ok: false, kind: "error", msg: `A semantic model named "${name}" already exists in this workspace.` };
        }
        return { ok: true, kind: "ok", msg: "This name is available." };
    }
    function nameMsgHtml(state) {
        if (!state.msg) return "";
        const icon = state.kind === "error" ? IC.alert : (state.kind === "ok" ? IC.check : "");
        return `${icon}<span>${esc(state.msg)}</span>`;
    }
    function updateNameMsg() {
        const state = nameState();
        const box = q("namemsg");
        box.className = "slls-cv-namemsg" + (state.kind ? " slls-cv-" + state.kind : "");
        box.innerHTML = nameMsgHtml(state);
        nameInput.setAttribute("aria-invalid", String(state.kind === "error"));
    }
    nameInput.addEventListener("input", () => {
        modelName = nameInput.value;
        nameTouched = modelName.trim() !== "";
        updateNameMsg();
        updateValidity();
    });

    // ---- Buttons ----
    function missingInputs() {
        const v = vendor();
        if (!v) return ["vendor"];
        const vals = vendorValues();
        const out = v.fields
            .filter((f) => f.required !== false && !String(vals[f.key] || "").trim())
            .map((f) => f.label);
        if (!srcItem) out.push(`Direct Lake source ${srcType.toLowerCase()}`);
        return out;
    }
    function invalidInputs() {
        const v = vendor();
        if (!v) return [];
        const vals = vendorValues();
        return v.fields.filter((f) => fieldError(f, vals[f.key])).map((f) => f.label);
    }
    function updateValidity() {
        const missing = missingInputs();
        const invalid = invalidInputs();
        const blocked = missing.length > 0 || invalid.length > 0;
        const name = nameState();
        const hint = [
            missing.length ? `Provide: ${missing.join(", ")}` : "",
            invalid.length ? `Fix: ${invalid.join(", ")}` : "",
        ].filter(Boolean).join(". ");
        previewBtn.disabled = converting || blocked;
        previewBtn.title = hint || "Preview the Power BI semantic model";
        const create = resultBox.querySelector('[data-r="create"]');
        if (create) {
            create.disabled = converting || !name.ok;
            const note = resultBox.querySelector('[data-r="rnote"]');
            const state = name.msg ? name : { kind: "", msg: "Enter a semantic model name to create it." };
            note.className = "slls-cv-namemsg" + (state.kind && state.kind !== "ok" ? " slls-cv-" + state.kind : "");
            note.innerHTML = state.kind === "ok" ? "" : nameMsgHtml(state);
        }
    }

    let busyBtn = null;
    function setBusy(btn) {
        busyBtn = btn;
        btn.dataset.label = btn.innerHTML;
        btn.innerHTML = '<span class="slls-cv-spin"></span>';
        btn.disabled = true;
    }
    function clearBusy() {
        if (busyBtn && busyBtn.isConnected && busyBtn.dataset.label !== undefined) {
            busyBtn.innerHTML = busyBtn.dataset.label;
        }
        busyBtn = null;
    }
    function runConversion(testRun, btn) {
        if (converting) return;
        converting = true;
        setBusy(btn);
        updateValidity();
        dispatch({
            action: "convert",
            vendor: vendorKey,
            values: Object.assign({}, vendorValues()),
            source_workspace_id: srcWs,
            source_type: srcType,
            source_item_id: srcItem,
            workspace_id: destWs,
            workspace_name: destWsPicker.label,
            name: modelName.trim(),
            test_run: testRun,
        });
    }
    previewBtn.addEventListener("click", () => runConversion(true, previewBtn));

    reloadBtn.addEventListener("click", () => {
        if (reloadPending > 0) return;
        const batch = [{ action: "list_workspaces", reload: true }];
        if (destWs) batch.push({ action: "list_datasets", workspace_id: destWs, reload: true });
        if (srcWs) batch.push({ action: "list_items", workspace_id: srcWs, item_type: srcType, reload: true });
        reloadPending = batch.length;
        reloadBtn.classList.add("sl-spinning");
        batch.forEach(dispatch);
    });

    // ---- Status + result ----
    function renderStatus() {
        const status = model.get("status") || {};
        const formStatus = q("status");
        const resultStatus = resultBox.querySelector('[data-r="rstatus"]');
        formStatus.innerHTML = "";
        if (resultStatus) resultStatus.innerHTML = "";
        const target = resultBox.hidden ? formStatus : resultStatus;
        if (target && status.message) {
            target.innerHTML = `<div class="slls-cv-status" role="alert">${IC.alert}<span>${esc(status.message)}</span></div>`;
        }
    }

    function backToForm() {
        model.set("result", {});
        model.set("status", {});
        model.save_changes();
        renderResult();
    }

    // ---- Findings + model view (collapsible trees) ----
    function plural(n, word) { return `${Number(n || 0).toLocaleString()} ${word}${n === 1 ? "" : "s"}`; }
    function node(summary, body, cls) {
        return `<details class="slls-cv-node${cls ? " " + cls : ""}"><summary><span class="slls-cv-caret">${IC.caret}</span>`
            + `${summary}</summary><div class="slls-cv-node-body">${body}</div></details>`;
    }
    function nodeTitle(icon, name, meta, iconCls) {
        return `<span class="slls-cv-node-ic${iconCls ? " " + iconCls : ""}">${icon}</span>`
            + `<span class="slls-cv-node-name" title="${esc(name)}">${esc(name)}</span>${meta || ""}`;
    }
    const notConverted = `<span class="slls-cv-badge slls-cv-badge-warn">Not converted</span>`;
    function codeBlock(label, text, lang) {
        return `<div class="slls-cv-codelabel">${esc(label)}</div><pre class="slls-cv-codeblock">`
            + (text ? highlightCode(text, lang) : `<span class="slls-cv-muted">\u2014</span>`) + `</pre>`;
    }
    function findingsHtml(findings) {
        if (findings.length === 0) {
            return `<div class="slls-cv-allgood">${IC.checkCircle}<span>Every element of the definition was converted.</span></div>`;
        }
        return `<div class="slls-cv-tree">` + findings.map((g) => {
            const items = Array.isArray(g.items) ? g.items : [];
            return node(
                nodeTitle(IC.alert, g.element, `<span class="slls-cv-node-meta">${plural(items.length, "item")}</span>`, "slls-cv-warn-ic"),
                `<div class="slls-cv-find-detail">${esc(g.detail)}</div>` + items.map((i) => `<div class="slls-cv-find-item">`
                    + `<span class="slls-cv-find-name">${esc(i.name)}</span>`
                    + (i.note ? `<span class="slls-cv-find-note">${esc(i.note)}</span>` : "") + `</div>`).join(""));
        }).join("") + `</div>`;
    }
    function modelHtml(m) {
        const tables = (m && m.tables) || [];
        const rels = (m && m.relationships) || [];
        const parts = tables.map((t) => {
            const cols = t.columns || [];
            const excluded = t.excluded_columns || [];
            const measures = t.measures || [];
            let body = t.description ? `<div class="slls-cv-find-detail">${esc(t.description)}</div>` : "";
            if (cols.length) {
                body += `<div class="slls-cv-subhead">Columns</div><div class="slls-cv-cols">`
                    + `<div class="slls-cv-colhead">Name</div><div class="slls-cv-colhead">Data type</div>`
                    + `<div class="slls-cv-colhead">Source column</div><div class="slls-cv-colhead"></div>`
                    + cols.map((c) => `<div title="${esc(c.name)}">${esc(c.name)}</div>`
                        + `<div class="slls-cv-muted">${esc(c.data_type)}</div>`
                        + `<div class="slls-cv-muted" title="${esc(c.source)}">${esc(c.source)}</div>`
                        + `<div>${c.key ? `<span class="slls-cv-badge">Key</span> ` : ""}`
                        + `${c.hidden ? `<span class="slls-cv-badge">Hidden</span>` : ""}</div>`).join("")
                    + `</div>`;
            }
            if (excluded.length) {
                body += `<div class="slls-cv-subhead">Excluded calculated columns</div>` + excluded.map((c) => node(
                    nodeTitle(IC.calcColumn, c.name, notConverted), codeBlock("SQL", c.expression, "sql"), "slls-cv-sub")).join("");
            }
            if (measures.length) {
                body += `<div class="slls-cv-subhead">Measures</div>` + measures.map((ms) => node(
                    nodeTitle(IC.measure, ms.name, ms.converted ? "" : notConverted),
                    codeBlock("SQL", ms.sql, "sql") + codeBlock("DAX", ms.dax, "dax"), "slls-cv-sub")).join("");
            }
            const meta = `<span class="slls-cv-node-meta">${plural(cols.length, "column")} \u00b7 ${plural(measures.length, "measure")}</span>`;
            return node(nodeTitle(IC.table, t.name, meta), body);
        });
        if (rels.length) {
            parts.push(node(
                nodeTitle(IC.relationship, "Relationships", `<span class="slls-cv-node-meta">${plural(rels.length, "relationship")}</span>`),
                rels.map((rel) => `<div class="slls-cv-rel"><span>${highlightCode(rel.from, "dax")}</span>`
                    + `<span class="slls-cv-muted">\u2192</span><span>${highlightCode(rel.to, "dax")}</span>`
                    + `<span class="slls-cv-node-meta">${esc(rel.cardinality)}</span></div>`).join("")));
        }
        return `<div class="slls-cv-tree" data-r="tree">${parts.join("")}</div>`;
    }

    function renderResult() {
        const r = model.get("result") || {};
        const show = !!r.kind;
        formBox.hidden = show;
        resultBox.hidden = !show;
        if (!show) {
            resultBox.innerHTML = "";
            renderStatus();
            updateValidity();
            return;
        }
        const created = r.kind === "created";
        const card = (label, value, sub) => `<div class="slls-cv-card"><div class="slls-cv-card-label">${esc(label)}</div>`
            + `<div class="slls-cv-card-value">${Number(value || 0).toLocaleString()}</div>`
            + (sub ? `<div class="slls-cv-card-sub">${esc(sub)}</div>` : "") + `</div>`;
        const sub = created
            ? `<b>${esc(r.name)}</b> was created in the <b>${esc(r.workspace_name)}</b> workspace.`
            : `This is a preview of the semantic model built from the ${esc(r.vendor)}. It has not been created yet: `
              + `review it below, then select <b>Create semantic model</b>.`;
        const hidden = Number(r.hidden_columns || 0);
        const findings = Array.isArray(r.findings) ? r.findings : [];
        const actions = created
            ? `<button class="slls-cv-btn-primary" data-r="again" type="button">Convert another</button>`
            : `<div class="slls-cv-namemsg" data-r="rnote"></div>`
              + `<button class="slls-cv-btn-secondary" data-r="back" type="button">Back</button>`
              + `<button class="slls-cv-btn-primary" data-r="create" type="button">Create semantic model</button>`;
        resultBox.innerHTML = `
            <div class="slls-cv-banner">
                <span class="slls-cv-banner-ic">${created ? IC.checkCircle : IC.eye}</span>
                <div>
                    <div class="slls-cv-banner-title">${created ? "Semantic model created" : "Semantic model preview"}</div>
                    <div class="slls-cv-banner-sub">${sub}</div>
                </div>
            </div>
            <div class="slls-cv-cards">
                ${card("Tables", r.tables)}
                ${card("Columns", r.columns, hidden ? `${hidden.toLocaleString()} hidden` : "")}
                ${card("Measures", r.measures)}
                ${card("Relationships", r.relationships)}
            </div>
            <div class="slls-cv-sechead"><span class="slls-cv-label">Not converted</span></div>
            <div class="slls-cv-hint">Elements of the ${esc(r.vendor)} which have no equivalent in the Power BI semantic model.</div>
            ${findingsHtml(findings)}
            <div class="slls-cv-sechead">
                <span class="slls-cv-label">Semantic model</span>
                <span class="slls-cv-tools">
                    <button class="slls-cv-tool" data-r="expand" type="button" title="Expand all" aria-label="Expand all">${IC.expand}</button>
                    <button class="slls-cv-tool" data-r="collapse" type="button" title="Collapse all" aria-label="Collapse all">${IC.collapse}</button>
                </span>
            </div>
            ${modelHtml(r.model)}
            <div data-r="rstatus"></div>
            <div class="slls-cv-footer">${actions}</div>`;

        const tree = resultBox.querySelector('[data-r="tree"]');
        resultBox.querySelector('[data-r="expand"]').addEventListener("click", () => {
            tree.querySelectorAll("details").forEach((d) => { d.open = true; });
        });
        resultBox.querySelector('[data-r="collapse"]').addEventListener("click", () => {
            tree.querySelectorAll("details").forEach((d) => { d.open = false; });
        });
        const again = resultBox.querySelector('[data-r="again"]');
        if (again) again.addEventListener("click", () => {
            const v = vendor();
            if (v) v.fields.filter((f) => f.type === "definition").forEach((f) => {
                vendorValues()[f.key] = "";
                fileNames[`slls-cv-${uid}-${vendorKey}-${f.key}`] = "";
            });
            modelName = "";
            nameTouched = false;
            nameInput.value = "";
            renderFields();
            updateNameMsg();
            backToForm();
        });
        const back = resultBox.querySelector('[data-r="back"]');
        if (back) back.addEventListener("click", backToForm);
        const create = resultBox.querySelector('[data-r="create"]');
        if (create) create.addEventListener("click", () => runConversion(false, create));
        renderStatus();
        updateValidity();
    }

    model.on("change:workspaces", () => {
        workspacesLoaded = true;
        updatePickers();
        ensureItems();
        ensureDatasets(destWs);
        updateNameMsg();
        updateValidity();
    });
    model.on("change:source_items", () => { updatePickers(); updateValidity(); });
    model.on("change:datasets", () => { updateNameMsg(); updateValidity(); });
    model.on("change:status", renderStatus);
    model.on("change:result", () => {
        const r = model.get("result") || {};
        // A preview without a name used the definition's own name; offer it.
        if (r.kind === "preview" && !modelName.trim() && r.name) {
            modelName = r.name;
            nameInput.value = r.name;
            updateNameMsg();
        }
        renderResult();
    });

    renderVendors();
    renderFields();
    renderSourceTypes();
    updatePickers();
    ensureWorkspaces();
    ensureDatasets(destWs);
    ensureItems();
    updateNameMsg();
    renderResult();
}

export default { render };
"""


_WIDGET_CSS = (
    _WIDGET_CSS.replace("__LIGHT__", _UI_LIGHT_VARS)
    .replace("__SYNTAX__", _UI_SYNTAX_VARS)
    .replace("__DARK__", _UI_DARK_VARS)
    .replace("__SEARCH_SELECT_CSS__", _UI_SEARCH_SELECT_CSS)
)
_WIDGET_CSS += _ui_scoped_header_css(".slls-cv")
_WIDGET_CSS += _ui_scoped_button_press_css(".slls-cv")
_WIDGET_CSS += _ui_scoped_attribution_css(".slls-cv")
_WIDGET_CSS += _ui_fullscreen_css(".slls-cv", "slls-cv-fs")

_WIDGET_JS = (
    _WIDGET_JS.replace("__SEARCH_SELECT_JS__", _UI_SEARCH_SELECT_JS)
    .replace("__FULLSCREEN_JS__", _ui_fullscreen_setup_js())
    .replace("__ATTRIBUTION__", _ui_render_attribution_html())
    .replace("__IC_CONVERT__", _UI_ICONS["sync"])
    .replace("__IC_SOURCE__", _UI_ICONS["source"])
    .replace("__IC_UPLOAD__", _UI_ICONS["upload"])
    .replace("__IC_REFRESH__", _UI_ICONS["refresh"])
    .replace("__IC_CHECK_CIRCLE__", _UI_ICONS["check_circle"])
    .replace("__IC_CHECK__", _UI_ICONS["check"])
    .replace("__IC_ALERT__", _UI_ICONS["alert"])
    .replace("__IC_EYE__", _UI_ICONS["eye"])
    .replace("__IC_CARET__", _UI_ICONS["caret_right"])
    .replace("__IC_TABLE__", _UI_ICONS["table"])
    .replace("__IC_MEASURE__", _UI_ICONS["measure"])
    .replace("__IC_CALC_COLUMN__", _UI_ICONS["calculated_column"])
    .replace("__IC_RELATIONSHIP__", _UI_ICONS["relationship"])
    .replace("__IC_EXPAND__", _UI_ICONS["expand_rows"])
    .replace("__IC_COLLAPSE__", _UI_ICONS["collapse_rows"])
    .replace("__IC_SUN__", _UI_ICONS["sun"])
    .replace("__IC_MOON__", _UI_ICONS["moon"])
    .replace("__IC_FSX__", _UI_ICONS["fullscreen_exit"])
    .replace("__IC_FS__", _UI_ICONS["fullscreen"])
)
