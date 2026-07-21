import re
from typing import Optional, Literal, Dict, List, Set, Tuple
from uuid import UUID

import pandas as pd
from sempy._utils._log import log

from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
)

# Regexes mirroring the reference implementation
# (m-kovalsky/Two - TomService.UsageAnalysis.cs). ``_FIELD_REF`` matches an
# object reference: 'Table'[Object], Table[Object] or [Object]. ``_QUOTED_REF``
# matches a bare quoted identifier that names a table (e.g. ALL('Sales')).
_FIELD_REF = re.compile(r"(?:'(?P<tq>[^']+)'|(?P<tu>\w+))?\[(?P<o>[^\]]+)\]")
_QUOTED_REF = re.compile(r"'(?P<t>[^']+)'")

# Context tuple layout (see ``_build_usage_context``).
_CTX_ALL = 0
_CTX_CANONICAL = 1
_CTX_TABLES = 2
_CTX_COLUMNS = 3
_CTX_MEASURES = 4
_CTX_ADJ = 5
_CTX_HIDDEN = 6
_CTX_TABLE_KIND = 7
_CTX_CALC_COLS = 8


def _ukey(object_type: str, table: str, name: str) -> str:
    """Builds a canonical, case-insensitive key for a model object."""
    return f"{object_type}\u0001{table.lower()}\u0001{name.lower()}"


def _normalize_usage_type(dmv_type: str) -> Optional[str]:
    """Maps a dependency object type to "Table", "Column" or "Measure".

    Returns None for types that are not tracked as usage objects (e.g.
    hierarchies, calculation items). Mirrors the reference implementation's
    ``NormalizeUsageType``.
    """
    t = (dmv_type or "").upper()
    if "MEASURE" in t:
        return "Measure"
    if "COLUMN" in t:
        return "Column"
    if t in (
        "TABLE",
        "CALC TABLE",
        "CALCULATED TABLE",
        "CALC_TABLE",
        "CALCULATED_TABLE",
    ):
        return "Table"
    return None


def _build_usage_context(dataset_id: str, workspace_id: str) -> Tuple[
    List[Tuple[str, str, str]],
    Set[str],
    Dict[str, str],
    Dict[str, Set[str]],
    Dict[str, str],
    Dict[str, Set[str]],
    Set[str],
]:
    """Builds the reusable analysis context for a semantic model.

    Enumerates every tracked object (tables, non-row-number columns and
    measures), builds case-insensitive lookups, records which objects are
    hidden, and derives the (transitive) dependency adjacency from the model's
    calculation dependencies.
    """
    from sempy_labs.tom import connect_semantic_model
    from sempy_labs._model_dependencies import get_model_calc_dependencies

    all_objects: List[Tuple[str, str, str]] = []
    canonical: Set[str] = set()
    tables_lower: Dict[str, str] = {}  # lower table name -> actual
    columns_by_table: Dict[str, Set[str]] = {}  # lower table -> {lower col}
    measure_home: Dict[str, str] = {}  # lower measure -> actual table
    hidden_keys: Set[str] = set()
    table_kinds: Dict[str, str] = {}  # actual table name -> kind
    calc_columns: Set[str] = set()  # ukeys of calculated columns

    def register(object_type: str, table: str, name: str) -> None:
        key = _ukey(object_type, table, name)
        if key in canonical:
            return
        canonical.add(key)
        all_objects.append((object_type, table, name))

    with connect_semantic_model(
        dataset=dataset_id, readonly=True, workspace=workspace_id
    ) as tom:
        import Microsoft.AnalysisServices.Tabular as TOM

        def _classify_table(t) -> str:
            # Field parameters expose a "ParameterMetadata" extended property
            # (or annotation) on one of their columns; checked first because
            # they are also calculated tables.
            for c in t.Columns:
                try:
                    if any(
                        ep.Name == "ParameterMetadata" for ep in c.ExtendedProperties
                    ):
                        return "field_parameter"
                except Exception:
                    pass
                try:
                    if any(a.Name == "ParameterMetadata" for a in c.Annotations):
                        return "field_parameter"
                except Exception:
                    pass
            if t.CalculationGroup is not None:
                return "calculation_group"
            # A date table is marked with DataCategory = "Time" and has a key
            # column of an integer or date/time data type.
            if str(t.DataCategory) == "Time":
                for c in t.Columns:
                    if c.IsKey and str(c.DataType) in ("Int64", "DateTime"):
                        return "date_table"
            if any(
                p.SourceType == TOM.PartitionSourceType.Calculated for p in t.Partitions
            ):
                return "calculated_table"
            return "table"

        for table in tom.model.Tables:
            table_name = table.Name
            tables_lower[table_name.lower()] = table_name
            register("Table", table_name, table_name)
            table_kinds[table_name] = _classify_table(table)
            if table.IsHidden:
                hidden_keys.add(_ukey("Table", table_name, table_name))

            cols: Set[str] = set()
            columns_by_table[table_name.lower()] = cols
            for col in table.Columns:
                if col.Type == TOM.ColumnType.RowNumber:
                    continue
                cols.add(col.Name.lower())
                register("Column", table_name, col.Name)
                if col.Type == TOM.ColumnType.Calculated:
                    calc_columns.add(_ukey("Column", table_name, col.Name))
                if col.IsHidden:
                    hidden_keys.add(_ukey("Column", table_name, col.Name))

            for measure in table.Measures:
                measure_home[measure.Name.lower()] = table_name
                register("Measure", table_name, measure.Name)
                if measure.IsHidden:
                    hidden_keys.add(_ukey("Measure", table_name, measure.Name))

    # Build the dependency adjacency (object -> referenced objects). The
    # ``get_model_calc_dependencies`` result is already transitively expanded,
    # so a single-level union reproduces the transitive closure used by the
    # reference implementation.
    adjacency: Dict[str, Set[str]] = {}
    dep = get_model_calc_dependencies(dataset=dataset_id, workspace=workspace_id)
    for _, row in dep.iterrows():
        from_type = _normalize_usage_type(row["Object Type"])
        to_type = _normalize_usage_type(row["Referenced Object Type"])
        if from_type is None or to_type is None:
            continue
        from_key = _ukey(from_type, row["Table Name"], row["Object Name"])
        to_key = _ukey(to_type, row["Referenced Table"], row["Referenced Object"])
        if to_key not in canonical:
            continue
        adjacency.setdefault(from_key, set()).add(to_key)

    return (
        all_objects,
        canonical,
        tables_lower,
        columns_by_table,
        measure_home,
        adjacency,
        hidden_keys,
        table_kinds,
        calc_columns,
    )


def _collect_direct_references(
    query: str,
    tables_lower: Dict[str, str],
    columns_by_table: Dict[str, Set[str]],
    measure_home: Dict[str, str],
) -> Set[str]:
    """Finds the model objects a single DAX query directly references."""
    direct: Set[str] = set()

    for m in _FIELD_REF.finditer(query):
        table_ref = m.group("tq") or m.group("tu")
        obj = m.group("o")
        if table_ref:
            actual = tables_lower.get(table_ref.lower())
            if actual is not None:
                direct.add(_ukey("Table", actual, actual))
                cols = columns_by_table.get(actual.lower())
                if cols is not None and obj.lower() in cols:
                    direct.add(_ukey("Column", actual, obj))
                    continue
            home = measure_home.get(obj.lower())
            if home is not None:
                direct.add(_ukey("Measure", home, obj))
        else:
            # [Object] alone is a measure (a bare column reference is ambiguous).
            home = measure_home.get(obj.lower())
            if home is not None:
                direct.add(_ukey("Measure", home, obj))

    for m in _QUOTED_REF.finditer(query):
        actual = tables_lower.get(m.group("t").lower())
        if actual is not None:
            direct.add(_ukey("Table", actual, actual))

    return direct


def _collect_report_references(
    references: List[Tuple[str, str, str]],
    tables_lower: Dict[str, str],
    columns_by_table: Dict[str, Set[str]],
    measure_home: Dict[str, str],
) -> Set[str]:
    """Finds the model objects a single report's references map to."""
    direct: Set[str] = set()
    for table, name, object_type in references:
        if object_type == "Column":
            actual = tables_lower.get(str(table).lower())
            cols = columns_by_table.get(actual.lower()) if actual else None
            if actual is not None and cols is not None and str(name).lower() in cols:
                direct.add(_ukey("Table", actual, actual))
                direct.add(_ukey("Column", actual, name))
        elif object_type == "Measure":
            home = measure_home.get(str(name).lower())
            if home is not None:
                direct.add(_ukey("Measure", home, name))
        elif object_type == "Hierarchy":
            # Hierarchies aren't tracked objects; mark the table as used.
            actual = tables_lower.get(str(table).lower())
            if actual is not None:
                direct.add(_ukey("Table", actual, actual))
    return direct


def _expand(direct: Set[str], adjacency: Dict[str, Set[str]]) -> Set[str]:
    """Expands a set of direct references through the dependency adjacency."""
    used = set(direct)
    for key in direct:
        used |= adjacency.get(key, set())
    return used


def _fetch_monitoring_queries(
    dataset_name: str, workspace_id: str, range_str: str
) -> List[str]:
    """Fetches the DAX query texts captured by workspace monitoring."""
    from sempy_labs._kusto import query_workspace_monitoring

    safe_name = dataset_name.replace("\\", "\\\\").replace('"', '\\"')
    kql_query = (
        "SemanticModelLogs\n"
        '| where OperationName == "QueryEnd" and '
        '(EventText startswith "EVALUATE" or EventText startswith "DEFINE")\n'
        f'| where ItemName == "{safe_name}"\n'
        f"| where Timestamp >= ago({range_str})\n"
        "| project EventText\n"
        "| take 50000"
    )
    df_queries = query_workspace_monitoring(
        query=kql_query, workspace=workspace_id, language="kql"
    )
    if not df_queries.empty and "EventText" in df_queries.columns:
        return [q for q in df_queries["EventText"].dropna().tolist() if str(q).strip()]
    return []


def _score_queries(context, queries: List[str]) -> Dict[str, int]:
    """Scores captured DAX queries, returning a usage count per object."""
    tables_lower = context[_CTX_TABLES]
    columns_by_table = context[_CTX_COLUMNS]
    measure_home = context[_CTX_MEASURES]
    adjacency = context[_CTX_ADJ]

    counts: Dict[str, int] = {}
    for query in queries:
        direct = _collect_direct_references(
            query, tables_lower, columns_by_table, measure_home
        )
        if not direct:
            continue
        for key in _expand(direct, adjacency):
            counts[key] = counts.get(key, 0) + 1
    return counts


def _score_reports(
    context, dataset_id: str, workspace_id: str
) -> Tuple[Dict[str, int], int]:
    """Scores the model's downstream reports, returning counts + report count."""
    from sempy_labs._list_functions import list_report_semantic_model_objects

    tables_lower = context[_CTX_TABLES]
    columns_by_table = context[_CTX_COLUMNS]
    measure_home = context[_CTX_MEASURES]
    adjacency = context[_CTX_ADJ]

    counts: Dict[str, int] = {}
    report_count = 0
    df_reports = list_report_semantic_model_objects(
        dataset=dataset_id, workspace=workspace_id
    )
    if not df_reports.empty:
        grouped = df_reports.groupby(["Report Name", "Report Workspace Name"])
        report_count = grouped.ngroups
        for _, grp in grouped:
            references = [
                (r["Table Name"], r["Object Name"], r["Object Type"])
                for _, r in grp.iterrows()
            ]
            direct = _collect_report_references(
                references, tables_lower, columns_by_table, measure_home
            )
            if not direct:
                continue
            for key in _expand(direct, adjacency):
                counts[key] = counts.get(key, 0) + 1
    return counts, report_count


def _counts_to_dataframe(
    all_objects: List[Tuple[str, str, str]], counts: Dict[str, int]
) -> pd.DataFrame:
    """Builds the public result dataframe from the accumulated usage counts."""
    rows = []
    for object_type, table, name in all_objects:
        usage_count = counts.get(_ukey(object_type, table, name), 0)
        rows.append(
            {
                "Table Name": table,
                "Object Type": object_type,
                "Object Name": name,
                "IsUsed": usage_count > 0,
                "UsageCount": usage_count,
            }
        )

    df = pd.DataFrame(
        rows,
        columns=["Table Name", "Object Type", "Object Name", "IsUsed", "UsageCount"],
    )
    if not df.empty:
        df = df.sort_values(
            by=["UsageCount", "Table Name", "Object Name"],
            ascending=[False, True, True],
        ).reset_index(drop=True)
    return df


def _make_widget_data(context, counts: Dict[str, int]) -> List[dict]:
    """Builds the object list sent to the interactive widget."""
    all_objects = context[_CTX_ALL]
    hidden_keys = context[_CTX_HIDDEN]
    table_kinds = context[_CTX_TABLE_KIND]
    calc_columns = context[_CTX_CALC_COLS]
    data = []
    for object_type, table, name in all_objects:
        key = _ukey(object_type, table, name)
        item = {
            "t": table,
            "ty": object_type,
            "n": name,
            "u": counts.get(key, 0),
            "h": key in hidden_keys,
        }
        if object_type == "Table":
            item["k"] = table_kinds.get(table, "table")
        elif object_type == "Column":
            item["c"] = key in calc_columns
        data.append(item)
    return data


@log
def find_unused_objects(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    method: Literal["Report", "WorkspaceMonitoring"] = "WorkspaceMonitoring",
    workspace_monitoring_days: int = 7,
    visualize: bool = True,
    dark_mode: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Identifies unused objects (tables, columns and measures) in a semantic model.

    Replicates the 'Find unused objects' subtool of the Semantic Model Explorer
    in the `Fabric Tools app <https://github.com/m-kovalsky/Two>`_. Object usage
    is determined by scoring the DAX queries that reference the model (either
    captured by workspace monitoring, or reconstructed from the model's
    downstream reports), expanding each direct reference through the model's
    calculation dependencies, and counting how many queries/reports used each
    object. An object with a usage count of zero is unused.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    method : Literal["Report", "WorkspaceMonitoring"], default="WorkspaceMonitoring"
        The source of usage information (used to compute the returned dataframe
        when `visualize` is False, and to preselect the analysis method in the
        interactive widget when `visualize` is True).

        * "WorkspaceMonitoring" scores the DAX queries captured by workspace
          monitoring. Workspace monitoring must be enabled on the workspace.
        * "Report" scores the objects referenced by the model's downstream
          reports (which must be in the PBIR format). Each report counts as one
          use.
    workspace_monitoring_days : int, default=7
        The number of days of workspace monitoring history to analyze (also the
        initial time frame preselected in the interactive widget). Only used with
        the "WorkspaceMonitoring" method.
    visualize : bool, default=True
        If True, displays an interactive widget that lets you choose the analysis
        method (workspace monitoring or downstream reports) and, for workspace
        monitoring, the time frame; it then shows the unused/used objects as a
        table -> column/measure tree. Returns None. If False, runs the analysis
        for the given `method`/`workspace_monitoring_days` and returns a pandas
        dataframe.
    dark_mode : bool, default=False
        If True, the interactive widget is rendered with a dark color theme.
        Only used when `visualize` is True.

    Returns
    -------
    pandas.DataFrame | None
        If `visualize` is False, a pandas dataframe with the columns
        'Table Name', 'Object Type', 'Object Name', 'IsUsed' and 'UsageCount'.
        If `visualize` is True, displays the widget and returns None.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    dataset_name, dataset_id = resolve_dataset_name_and_id(dataset, workspace_id)

    method_normalized = str(method).lower()
    if method_normalized not in ("report", "workspacemonitoring"):
        raise ValueError(
            f"{method} is not a valid method. Must be one of ['Report', 'WorkspaceMonitoring']."
        )

    context = _build_usage_context(dataset_id, workspace_id)

    if not visualize:
        if method_normalized == "workspacemonitoring":
            days = max(1, min(9999, int(workspace_monitoring_days)))
            queries = _fetch_monitoring_queries(dataset_name, workspace_id, f"{days}d")
            counts = _score_queries(context, queries)
        else:
            counts, _ = _score_reports(context, dataset_id, workspace_id)
        return _counts_to_dataframe(context[_CTX_ALL], counts)

    _render_find_unused_objects(
        context=context,
        dataset_name=dataset_name,
        dataset_id=dataset_id,
        workspace_name=workspace_name,
        workspace_id=workspace_id,
        method=method_normalized,
        days=int(workspace_monitoring_days),
        dark_mode=dark_mode,
    )
    return None


def _render_find_unused_objects(
    context,
    dataset_name: str,
    dataset_id: str,
    workspace_name: Optional[str],
    workspace_id: str,
    method: str,
    days: int,
    dark_mode: bool = False,
) -> None:
    """Renders the interactive 'Find unused objects' widget (anywidget)."""
    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "The interactive 'find_unused_objects' widget requires the 'anywidget' "
            "package. Install it with: pip install anywidget (or call with "
            "visualize=False to get a pandas dataframe instead)."
        ) from e

    from IPython.display import display

    # Captured workspace-monitoring queries (fetched on "Count queries", scored
    # on "Analyze") so the two-step flow does not re-query the monitoring db.
    captured = {"queries": None}

    class FindUnusedObjectsWidget(anywidget.AnyWidget):
        _esm = _WIDGET_JS
        _css = _WIDGET_CSS

        screen = traitlets.Unicode("config").tag(sync=True)
        method = traitlets.Unicode("workspacemonitoring").tag(sync=True)
        time_amount = traitlets.Int(7).tag(sync=True)
        time_unit = traitlets.Unicode("d").tag(sync=True)
        query_count = traitlets.Int(-1).tag(sync=True)
        data = traitlets.List().tag(sync=True)
        table_kinds = traitlets.Dict().tag(sync=True)
        subtitle_count = traitlets.Unicode("").tag(sync=True)
        has_results = traitlets.Bool(False).tag(sync=True)
        dataset_name = traitlets.Unicode("").tag(sync=True)
        workspace_name = traitlets.Unicode("").tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        running = traitlets.Bool(False).tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)

    widget = FindUnusedObjectsWidget(
        method=method,
        time_amount=max(1, min(9999, int(days))),
        time_unit="d",
        table_kinds=context[_CTX_TABLE_KIND],
        dataset_name=dataset_name or "",
        workspace_name=workspace_name or "",
        dark_mode=bool(dark_mode),
    )

    def _on_run(_change):
        action = dict(widget.pending_action or {})
        act = action.get("action")
        if act not in ("count", "analyze"):
            return
        m = str(action.get("method", "workspacemonitoring")).lower()
        if m not in ("report", "workspacemonitoring"):
            m = "workspacemonitoring"
        amount = max(1, min(9999, int(action.get("amount", 7) or 7)))
        unit = str(action.get("unit", "d")).lower()
        if unit not in ("m", "h", "d"):
            unit = "d"

        widget.running = True
        widget.status = {}
        try:
            if act == "count":
                queries = _fetch_monitoring_queries(
                    dataset_name, workspace_id, f"{amount}{unit}"
                )
                captured["queries"] = queries
                widget.query_count = len(queries)
            else:  # analyze
                if m == "workspacemonitoring":
                    queries = captured["queries"]
                    if queries is None:
                        queries = _fetch_monitoring_queries(
                            dataset_name, workspace_id, f"{amount}{unit}"
                        )
                        captured["queries"] = queries
                        widget.query_count = len(queries)
                    counts = _score_queries(context, queries)
                    widget.data = _make_widget_data(context, counts)
                    n = len(queries)
                    widget.subtitle_count = f"{n:,} quer" + ("y" if n == 1 else "ies")
                else:
                    counts, report_count = _score_reports(
                        context, dataset_id, workspace_id
                    )
                    widget.data = _make_widget_data(context, counts)
                    widget.subtitle_count = f"{report_count:,} report" + (
                        "" if report_count == 1 else "s"
                    )
                widget.has_results = True
                widget.screen = "results"
        except Exception as e:  # surface the error in the widget, don't raise
            widget.status = {"message": f"Error: {e}", "kind": "error"}
        finally:
            widget.running = False

    widget.observe(_on_run, names=["run"])

    # Keep the reference alive via the observer closure and display once. Do not
    # return the widget (that would render it a second time in Jupyter).
    display(widget)


_WIDGET_CSS = """
.fuo {
    __LIGHT__
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "Helvetica Neue", Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    color: var(--ui-text);
    width: 100%;
    max-width: 640px;
    background: var(--ui-bg);
    border: 1px solid var(--ui-border);
    border-radius: 16px;
    box-shadow: var(--ui-shadow-lg);
    box-sizing: border-box;
    overflow: hidden;
}
@media (prefers-color-scheme: dark) {
    .fuo.fuo-auto { __DARK__ }
}
.fuo.fuo-dark { __DARK__ }
.fuo * { box-sizing: border-box; }
.fuo.fuo-fs {
    position: fixed;
    inset: 0;
    z-index: 2147483000;
    width: 100vw;
    height: 100vh;
    max-width: none;
    margin: 0;
    border: none;
    border-radius: 0;
    box-shadow: none;
    overflow: auto;
}
.fuo.fuo-fs .fuo-tree { max-height: calc(100vh - 220px); }

/* ---- Icon buttons ---- */
.fuo-icon-btn {
    appearance: none; -webkit-appearance: none;
    border: 1px solid var(--ui-border-strong);
    background: var(--ui-surface);
    color: var(--ui-text);
    width: 32px; height: 32px; padding: 0;
    display: inline-flex; align-items: center; justify-content: center;
    border-radius: 50%; cursor: pointer;
    transition: background 120ms ease, border-color 120ms ease, transform 80ms ease;
    flex-shrink: 0;
}
.fuo-icon-btn.fuo-sm { width: 28px; height: 28px; }
.fuo-icon-btn:hover { background: var(--ui-surface-2); border-color: var(--ui-text-tertiary); }
.fuo-icon-btn:active { transform: scale(0.95); }
.fuo-icon-btn svg { display: block; width: 17px; height: 17px; }
.fuo-hdr-ctrls { display: inline-flex; gap: 8px; align-items: center; }

/* ================= CONFIG SCREEN ================= */
.fuo-config { padding: 22px 24px 20px 24px; }
.fuo-cfg-head { display: flex; align-items: flex-start; gap: 14px; margin-bottom: 20px; }
.fuo-badge {
    flex-shrink: 0; width: 40px; height: 40px; border-radius: 11px;
    display: inline-flex; align-items: center; justify-content: center;
    background: var(--ui-bg-secondary); color: var(--ui-text);
    border: 1px solid var(--ui-border);
}
.fuo-badge svg { width: 20px; height: 20px; }
.fuo-cfg-titlewrap { display: flex; flex-direction: column; margin-right: auto; min-width: 0; }
.fuo-title { font-size: 20px; font-weight: 600; letter-spacing: -0.01em; line-height: 1.2; color: var(--ui-text); }
.fuo-desc { font-size: 13px; line-height: 1.5; color: var(--ui-text-secondary); margin-top: 5px; }
.fuo-desc b { color: var(--ui-text); font-weight: 600; }
.fuo-section-label {
    font-size: 11.5px; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.05em; color: var(--ui-text-tertiary);
    margin: 18px 0 8px 0;
}
.fuo-timeframe { display: flex; gap: 10px; align-items: center; }
.fuo-amount {
    width: 92px; padding: 10px 12px; font-size: 15px; font-family: inherit;
    border: 1px solid var(--ui-border-strong); border-radius: 10px;
    background: var(--ui-bg); color: var(--ui-text); outline: none;
    font-variant-numeric: tabular-nums;
}
.fuo-amount:focus { border-color: var(--ui-accent); }
.fuo-unit {
    flex: 1 1 auto; padding: 10px 12px; font-size: 15px; font-family: inherit;
    border: 1px solid var(--ui-border-strong); border-radius: 10px;
    background: var(--ui-bg); color: var(--ui-text); outline: none; cursor: pointer;
}
.fuo-unit:focus { border-color: var(--ui-accent); }
.fuo-countmsg {
    margin-top: 16px; padding: 12px 14px; font-size: 13.5px;
    border: 1px solid var(--ui-border); border-radius: 10px;
    background: var(--ui-bg-secondary); color: var(--ui-text);
}
.fuo-countmsg b { font-weight: 700; }
.fuo-countmsg.fuo-err { border-color: rgba(255,59,48,0.4); color: #ff3b30; background: rgba(255,59,48,0.08); }
.fuo-footer { display: flex; justify-content: flex-end; gap: 10px; margin-top: 22px; }
.fuo-btn-secondary {
    border: 1px solid var(--ui-border-strong); background: var(--ui-bg);
    color: var(--ui-text); font-size: 14px; font-weight: 600; font-family: inherit;
    padding: 9px 20px; border-radius: 10px; cursor: pointer;
    transition: background 120ms ease;
}
.fuo-btn-secondary:hover { background: var(--ui-surface-2); }
.fuo-btn-primary {
    display: inline-flex; align-items: center; justify-content: center; gap: 8px;
    border: none; background: var(--ui-accent); color: #fff;
    font-size: 14px; font-weight: 600; font-family: inherit;
    padding: 9px 20px; border-radius: 10px; cursor: pointer; min-width: 120px;
    transition: background 120ms ease, opacity 120ms ease;
}
.fuo-btn-primary svg { width: 16px; height: 16px; }
.fuo-btn-primary:hover { background: var(--ui-accent-hover); }
.fuo-btn-primary:disabled { opacity: 0.7; cursor: default; }

/* ---- Segmented control ---- */
.fuo-seg { display: inline-flex; background: var(--ui-bg-secondary); border: 1px solid var(--ui-border); border-radius: 10px; padding: 3px; }
.fuo-seg-btn {
    border: none; background: none; color: var(--ui-text-secondary);
    font-size: 14px; font-weight: 500; font-family: inherit;
    padding: 8px 18px; border-radius: 8px; cursor: pointer; white-space: nowrap;
    transition: background 120ms ease, color 120ms ease;
}
.fuo-seg-btn.fuo-active { background: var(--ui-accent-soft); color: var(--ui-accent); font-weight: 600; }

/* ================= RESULTS SCREEN ================= */
.fuo-results { display: flex; flex-direction: column; }
.fuo-res-head {
    display: flex; align-items: center; gap: 12px;
    padding: 16px 18px; border-bottom: 1px solid var(--ui-border);
}
.fuo-res-titlewrap { display: flex; flex-direction: column; margin-right: auto; min-width: 0; }
.fuo-res-title { font-size: 16px; font-weight: 600; letter-spacing: -0.01em; color: var(--ui-text); }
.fuo-res-sub { font-size: 12px; color: var(--ui-text-secondary); margin-top: 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.fuo-res-sub b { color: var(--ui-text); font-weight: 600; }
.fuo-res-sub .fuo-sep { color: var(--ui-text-tertiary); margin: 0 6px; }
.fuo-rerun {
    display: inline-flex; align-items: center; gap: 6px;
    border: 1px solid var(--ui-border-strong); background: var(--ui-surface);
    color: var(--ui-text); font-size: 12.5px; font-weight: 500; font-family: inherit;
    padding: 6px 12px; border-radius: 8px; cursor: pointer; white-space: nowrap;
    transition: background 120ms ease, border-color 120ms ease;
}
.fuo-rerun:hover { background: var(--ui-surface-2); border-color: var(--ui-text-tertiary); }
.fuo-rerun svg { width: 14px; height: 14px; }
.fuo-res-toolbar {
    display: flex; align-items: center; gap: 16px;
    padding: 10px 18px; border-bottom: 1px solid var(--ui-border); flex-wrap: wrap;
}
.fuo-tools { display: inline-flex; gap: 6px; }
.fuo-toggle-wrap { display: inline-flex; align-items: center; gap: 10px; }
.fuo-toggle-label {
    border: none; background: none; font-family: inherit;
    font-size: 14px; font-weight: 600; color: var(--ui-text-secondary); cursor: pointer;
    padding: 0; transition: color 120ms ease;
}
.fuo-toggle-label.fuo-active { color: var(--ui-text); }
.fuo-switch {
    position: relative; width: 36px; height: 20px; border-radius: 10px;
    border: none; background: var(--ui-accent); cursor: pointer; padding: 0; flex-shrink: 0;
}
.fuo-knob {
    position: absolute; top: 2px; left: 2px; width: 16px; height: 16px;
    border-radius: 50%; background: #fff; transition: transform 140ms ease;
    box-shadow: 0 1px 2px rgba(0,0,0,0.25);
}
.fuo-switch.fuo-on .fuo-knob { transform: translateX(16px); }
.fuo-res-search { margin: 12px 18px 4px 18px; }

/* ---- Shared search ---- */
.fuo-search-wrap { position: relative; }
.fuo-search-wrap svg { position: absolute; left: 11px; top: 50%; transform: translateY(-50%); width: 15px; height: 15px; color: var(--ui-text-tertiary); pointer-events: none; }
.fuo-search {
    width: 100%; padding: 8px 10px 8px 34px;
    border: 1px solid var(--ui-border-strong); border-radius: 9px;
    background: var(--ui-bg); color: var(--ui-text);
    font-size: 13px; font-family: inherit; outline: none;
    transition: border-color 120ms ease;
}
.fuo-search:focus { border-color: var(--ui-accent); }

/* ---- Tree ---- */
.fuo-tree { max-height: 460px; overflow: auto; padding: 6px 12px 4px 12px; }
.fuo-table-row {
    display: flex; align-items: center; gap: 8px; padding: 7px 8px;
    border-radius: 8px; cursor: pointer; font-size: 13.5px; font-weight: 600;
    color: var(--ui-text); user-select: none;
}
.fuo-table-row:hover { background: var(--ui-surface-2); }
.fuo-caret { display: inline-flex; width: 10px; color: var(--ui-text-tertiary); transition: transform 120ms ease; flex-shrink: 0; }
.fuo-caret.fuo-open { transform: rotate(90deg); }
.fuo-obj-row {
    display: flex; align-items: center; gap: 8px; padding: 5px 8px 5px 32px;
    border-radius: 8px; font-size: 13px; color: var(--ui-text);
}
.fuo-obj-row:hover { background: var(--ui-surface-2); }
.fuo-ic { display: inline-flex; color: var(--ui-text-tertiary); flex-shrink: 0; }
.fuo-name { flex: 1 1 auto; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.fuo-name.fuo-hidden-obj { font-style: italic; color: var(--ui-text-tertiary); }
.fuo-count {
    flex-shrink: 0; font-size: 12px; font-weight: 600;
    font-variant-numeric: tabular-nums; color: var(--ui-text-secondary);
    padding-left: 8px;
}
.fuo-empty { padding: 40px 16px; text-align: center; color: var(--ui-text-tertiary); font-size: 13px; }
.fuo-attr { padding: 12px 18px 14px 18px; text-align: right; font-size: 11.5px; color: var(--ui-text-tertiary); }
.fuo-attr a { color: var(--ui-text-tertiary); text-decoration: none; transition: color 120ms ease; }
.fuo-attr a:hover { color: var(--ui-accent); }

/* ---- Spinner ---- */
.fuo-spin {
    display: inline-block; width: 15px; height: 15px;
    border: 2px solid rgba(255,255,255,0.5); border-top-color: #fff;
    border-radius: 50%; animation: fuospin 0.7s linear infinite;
}
@keyframes fuospin { to { transform: rotate(360deg); } }
"""


_WIDGET_JS = r"""
function render({ model, el }) {
    const root = document.createElement("div");
    root.className = "fuo";
    el.appendChild(root);

    const IC = {
        scan: `__IC_SCAN__`, table: `__IC_TABLE__`, calcTable: `__IC_CALCTABLE__`,
        calcGroup: `__IC_CALCGROUP__`, dateTable: `__IC_DATETABLE__`,
        fieldParam: `__IC_FIELDPARAM__`, column: `__IC_COLUMN__`,
        calcColumn: `__IC_CALCCOLUMN__`, measure: `__IC_MEASURE__`,
        caret: `__IC_CARET__`, search: `__IC_SEARCH__`,
        expand: `__IC_EXPAND__`, collapse: `__IC_COLLAPSE__`, rerun: `__IC_REFRESH__`,
    };
    const SUN = `__IC_SUN__`, MOON = `__IC_MOON__`, FS = `__IC_FS__`, FSX = `__IC_FSX__`;

    function tableIcon(kind) {
        if (kind === "calculation_group") return IC.calcGroup;
        if (kind === "date_table") return IC.dateTable;
        if (kind === "calculated_table") return IC.calcTable;
        if (kind === "field_parameter") return IC.fieldParam;
        return IC.table;
    }

    function kindLabel(kind) {
        return {
            calculation_group: "Calculation Group",
            date_table: "Date Table",
            calculated_table: "Calculated Table",
            field_parameter: "Field Parameter",
        }[kind] || "Table";
    }

    function objLabel(o) {
        if (o.ty === "Measure") return "Measure";
        return o.c ? "Calculated Column" : "Column";
    }

    let view = "unused";
    let search = "";
    let collapsed = {};
    let fsMode = false;

    function esc(s) {
        return (s + "").replace(/&/g, "&amp;").replace(/</g, "&lt;")
            .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
    }
    function coll(a, b) { return a.toLowerCase().localeCompare(b.toLowerCase()); }
    function unitName(u) { return u === "m" ? "minute" : (u === "h" ? "hour" : "day"); }

    function applyTheme() {
        root.classList.remove("fuo-dark", "fuo-auto");
        const dm = model.get("dark_mode");
        if (dm === true) root.classList.add("fuo-dark");
        else if (dm === null || dm === undefined) root.classList.add("fuo-auto");
    }
    applyTheme();
    model.on("change:dark_mode", applyTheme);

    function themeBtnHtml() {
        const d = model.get("dark_mode") === true;
        return `<button class="fuo-icon-btn" data-r="theme" type="button" title="${d ? 'Switch to light mode' : 'Switch to dark mode'}">${d ? SUN : MOON}</button>`;
    }
    function fsBtnHtml() {
        return `<button class="fuo-icon-btn" data-r="fs" type="button" title="${fsMode ? 'Exit full screen' : 'Toggle full screen'}">${fsMode ? FSX : FS}</button>`;
    }
    function rerunBtnHtml() {
        return `<button class="fuo-rerun" data-r="rerun" type="button" title="Run a new analysis">${IC.rerun}<span>New analysis</span></button>`;
    }
    function wireHeaderCtrls() {
        const tb = root.querySelector('[data-r="theme"]');
        if (tb) tb.addEventListener("click", () => { model.set("dark_mode", !(model.get("dark_mode") === true)); model.save_changes(); });
        const fb = root.querySelector('[data-r="fs"]');
        if (fb) fb.addEventListener("click", () => setFs(!fsMode));
    }
    function setFs(on) {
        fsMode = on;
        root.classList.toggle("fuo-fs", on);
        try {
            if (on) {
                const rq = root.requestFullscreen || root.webkitRequestFullscreen;
                if (rq) { const p = rq.call(root); if (p && p.catch) p.catch(() => {}); }
            } else {
                const ex = document.exitFullscreen || document.webkitExitFullscreen;
                if (ex && (document.fullscreenElement || document.webkitFullscreenElement)) {
                    const p = ex.call(document); if (p && p.catch) p.catch(() => {});
                }
            }
        } catch (e) { /* native fullscreen blocked; CSS overlay handles it */ }
        route();
    }
    document.addEventListener("fullscreenchange", () => {
        const on = !!(document.fullscreenElement || document.webkitFullscreenElement);
        if (!on && fsMode) { fsMode = false; root.classList.remove("fuo-fs"); route(); }
    });
    document.addEventListener("keydown", (e) => { if (e.key === "Escape" && fsMode) setFs(false); });

    // ================= CONFIG SCREEN =================
    function mountConfig() {
        const method = model.get("method") || "workspacemonitoring";
        const ds = model.get("dataset_name") || "";
        const amount = model.get("time_amount") || 7;
        const unit = model.get("time_unit") || "d";
        const qc = model.get("query_count");
        const hasResults = model.get("has_results") === true;
        const running = model.get("running") === true;
        const isMon = method === "workspacemonitoring";
        const desc = isMon
            ? `Reads the workspace-monitoring queries for <b>${esc(ds)}</b> in the chosen time frame and reports which objects were never used (plus how often each object was used).`
            : `Reads the downstream reports that consume <b>${esc(ds)}</b> and reports which objects are never referenced (plus how many reports use each object). Dependencies are included.`;
        const primaryLabel = isMon ? (qc >= 0 ? "Analyze" : "Count queries") : "Analyze reports";
        const primaryInner = running
            ? `<span class="fuo-spin"></span>`
            : `${IC.scan}<span>${primaryLabel}</span>`;
        const countMsg = (isMon && qc >= 0)
            ? `<div class="fuo-countmsg">Found <b>${qc.toLocaleString()}</b> quer${qc === 1 ? 'y' : 'ies'} in the last ${amount} ${unitName(unit)}${amount === 1 ? '' : 's'}.</div>`
            : "";
        const status = model.get("status") || {};
        const statusHtml = (status.kind === "error" && status.message)
            ? `<div class="fuo-countmsg fuo-err">${esc(status.message)}</div>` : "";

        root.innerHTML = `
            <div class="fuo-config">
                <div class="fuo-cfg-head">
                    <div class="fuo-badge">${IC.scan}</div>
                    <div class="fuo-cfg-titlewrap">
                        <div class="fuo-title">Find unused objects</div>
                        <div class="fuo-desc">${desc}</div>
                    </div>
                    <div class="fuo-hdr-ctrls">${fsBtnHtml()}${themeBtnHtml()}</div>
                </div>
                <div class="fuo-section-label">Analyze by</div>
                <div class="fuo-seg" data-r="method">
                    <button class="fuo-seg-btn ${isMon ? 'fuo-active' : ''}" data-method="workspacemonitoring" type="button">Workspace monitoring</button>
                    <button class="fuo-seg-btn ${isMon ? '' : 'fuo-active'}" data-method="report" type="button">Downstream reports</button>
                </div>
                ${isMon ? `
                <div class="fuo-section-label">Time frame</div>
                <div class="fuo-timeframe">
                    <input type="number" min="1" max="9999" class="fuo-amount" data-r="amount" value="${amount}" />
                    <select class="fuo-unit" data-r="unit">
                        <option value="m" ${unit === 'm' ? 'selected' : ''}>minutes</option>
                        <option value="h" ${unit === 'h' ? 'selected' : ''}>hours</option>
                        <option value="d" ${unit === 'd' ? 'selected' : ''}>days</option>
                    </select>
                </div>` : ``}
                ${countMsg}${statusHtml}
                <div class="fuo-footer">
                    ${hasResults ? `<button class="fuo-btn-secondary" data-r="cancel" type="button">Cancel</button>` : ``}
                    <button class="fuo-btn-primary" data-r="primary" type="button" ${running ? 'disabled' : ''}>${primaryInner}</button>
                </div>
            </div>`;

        wireHeaderCtrls();
        root.querySelectorAll('[data-r="method"] .fuo-seg-btn').forEach((b) => b.addEventListener("click", () => {
            const m = b.getAttribute("data-method");
            if (m === model.get("method")) return;
            model.set("method", m);
            model.set("query_count", -1);
            model.set("status", {});
            model.save_changes();
            mountConfig();
        }));
        const amt = root.querySelector('[data-r="amount"]');
        if (amt) amt.addEventListener("change", () => {
            let v = parseInt(amt.value, 10);
            if (isNaN(v) || v < 1) v = 1;
            if (v > 9999) v = 9999;
            amt.value = v;
            model.set("time_amount", v);
            model.set("query_count", -1);
            model.save_changes();
            mountConfig();
        });
        const un = root.querySelector('[data-r="unit"]');
        if (un) un.addEventListener("change", () => {
            model.set("time_unit", un.value);
            model.set("query_count", -1);
            model.save_changes();
            mountConfig();
        });
        const cancel = root.querySelector('[data-r="cancel"]');
        if (cancel) cancel.addEventListener("click", () => { model.set("screen", "results"); model.save_changes(); });
        const primary = root.querySelector('[data-r="primary"]');
        if (primary) primary.addEventListener("click", () => {
            if (model.get("running")) return;
            const m = model.get("method") || "workspacemonitoring";
            let action;
            if (m === "report") action = "analyze";
            else action = (model.get("query_count") >= 0) ? "analyze" : "count";
            model.set("pending_action", {
                action: action, method: m,
                amount: model.get("time_amount") || 7,
                unit: model.get("time_unit") || "d",
            });
            model.set("run", (model.get("run") || 0) + 1);
            model.save_changes();
        });
    }

    // ================= RESULTS SCREEN =================
    function mountResults() {
        root.innerHTML = `
            <div class="fuo-results">
                <div class="fuo-res-head">
                    <div class="fuo-res-titlewrap">
                        <div class="fuo-res-title">Object usage</div>
                        <div class="fuo-res-sub" data-r="subtitle"></div>
                    </div>
                    <div class="fuo-hdr-ctrls">${rerunBtnHtml()}${fsBtnHtml()}${themeBtnHtml()}</div>
                </div>
                <div class="fuo-res-toolbar">
                    <div class="fuo-tools">
                        <button class="fuo-icon-btn fuo-sm" data-r="expand" type="button" title="Expand all">${IC.expand}</button>
                        <button class="fuo-icon-btn fuo-sm" data-r="collapse" type="button" title="Collapse all">${IC.collapse}</button>
                    </div>
                    <div class="fuo-toggle-wrap" data-r="toggle"></div>
                </div>
                <div class="fuo-search-wrap fuo-res-search">${IC.search}<input type="text" class="fuo-search" data-r="search" placeholder="Filter objects…" /></div>
                <div class="fuo-tree" data-r="tree"></div>
                <div class="fuo-attr">Powered by <a href="https://github.com/microsoft/semantic-link-labs" target="_blank" rel="noopener noreferrer">Semantic Link Labs</a></div>
            </div>`;
        wireHeaderCtrls();
        root.querySelector('[data-r="rerun"]').addEventListener("click", () => { model.set("screen", "config"); model.save_changes(); });
        root.querySelector('[data-r="expand"]').addEventListener("click", () => { collapsed = {}; renderTree(); });
        root.querySelector('[data-r="collapse"]').addEventListener("click", () => { (model.get("data") || []).forEach((o) => { collapsed[o.t] = true; }); renderTree(); });
        root.querySelector('[data-r="search"]').addEventListener("input", (e) => { search = e.target.value; renderTree(); });
        renderSubtitle();
        renderToggle();
        renderTree();
    }

    function renderSubtitle() {
        const st = root.querySelector('[data-r="subtitle"]');
        if (!st) return;
        const ds = model.get("dataset_name") || "";
        const ws = model.get("workspace_name") || "";
        const cnt = model.get("subtitle_count") || "";
        const parts = [];
        if (ds) parts.push('<b>' + esc(ds) + '</b>');
        if (ws) parts.push(esc(ws));
        if (cnt) parts.push(esc(cnt));
        st.innerHTML = parts.join('<span class="fuo-sep">\u00b7</span>');
    }

    function renderToggle() {
        const wrap = root.querySelector('[data-r="toggle"]');
        if (!wrap) return;
        const data = model.get("data") || [];
        let unused = 0;
        data.forEach((o) => { if (o.u === 0) unused++; });
        const used = data.length - unused;
        wrap.innerHTML = `
            <button class="fuo-toggle-label ${view === 'unused' ? 'fuo-active' : ''}" data-view="unused" type="button">Unused (${unused.toLocaleString()})</button>
            <button class="fuo-switch ${view === 'used' ? 'fuo-on' : ''}" data-r="switch" type="button" aria-label="Toggle used / unused"><span class="fuo-knob"></span></button>
            <button class="fuo-toggle-label ${view === 'used' ? 'fuo-active' : ''}" data-view="used" type="button">Used (${used.toLocaleString()})</button>`;
        wrap.querySelectorAll("[data-view]").forEach((b) => b.addEventListener("click", () => { view = b.getAttribute("data-view"); renderToggle(); renderTree(); }));
        wrap.querySelector('[data-r="switch"]').addEventListener("click", () => { view = (view === "used" ? "unused" : "used"); renderToggle(); renderTree(); });
    }

    function buildTree(objs) {
        const byT = {}, order = [];
        objs.forEach((o) => {
            if (!byT[o.t]) { byT[o.t] = { table: o.t, tableObj: null, columns: [], measures: [] }; order.push(o.t); }
            const n = byT[o.t];
            if (o.ty === "Table") n.tableObj = o;
            else if (o.ty === "Measure") n.measures.push(o);
            else n.columns.push(o);
        });
        order.sort(coll);
        return order.map((t) => {
            const n = byT[t];
            n.columns.sort((a, b) => coll(a.n, b.n));
            n.measures.sort((a, b) => coll(a.n, b.n));
            return n;
        });
    }

    function renderTree() {
        const tree = root.querySelector('[data-r="tree"]');
        if (!tree) return;
        const data = model.get("data") || [];
        const showCount = view === "used";
        const s = search.trim().toLowerCase();
        let filtered = data.filter((o) => view === "used" ? o.u > 0 : o.u === 0);
        if (s) filtered = filtered.filter((o) => o.n.toLowerCase().indexOf(s) >= 0 || o.t.toLowerCase().indexOf(s) >= 0);
        const nodes = buildTree(filtered);
        if (nodes.length === 0) {
            tree.innerHTML = '<div class="fuo-empty">' + (view === "unused"
                ? "Every object was used at least once."
                : "No objects were used.") + "</div>";
            return;
        }
        const html = [];
        const tableKinds = model.get("table_kinds") || {};
        nodes.forEach((node) => {
            const children = node.columns.concat(node.measures);
            const open = !collapsed[node.table];
            const tableCount = (showCount && node.tableObj) ? node.tableObj.u : null;
            const tHidden = node.tableObj && node.tableObj.h;
            const kind = tableKinds[node.table] || "table";
            html.push('<div class="fuo-table-row" data-table="' + esc(node.table) + '">');
            html.push('<span class="fuo-caret' + (open ? ' fuo-open' : '') + '">' + IC.caret + '</span>');
            html.push('<span class="fuo-ic" title="' + esc(kindLabel(kind)) + '">' + tableIcon(kind) + '</span>');
            html.push('<span class="fuo-name' + (tHidden ? ' fuo-hidden-obj' : '') + '">' + esc(node.table) + '</span>');
            if (tableCount !== null) html.push('<span class="fuo-count">' + tableCount.toLocaleString() + '</span>');
            html.push('</div>');
            html.push('<div class="fuo-children" style="display:' + (open ? 'block' : 'none') + '">');
            children.forEach((o) => {
                const objIcon = o.ty === "Measure" ? IC.measure : (o.c ? IC.calcColumn : IC.column);
                html.push('<div class="fuo-obj-row">');
                html.push('<span class="fuo-ic" title="' + esc(objLabel(o)) + '">' + objIcon + '</span>');
                html.push('<span class="fuo-name' + (o.h ? ' fuo-hidden-obj' : '') + '">' + esc(o.n) + '</span>');
                if (showCount) html.push('<span class="fuo-count">' + o.u.toLocaleString() + '</span>');
                html.push('</div>');
            });
            html.push('</div>');
        });
        tree.innerHTML = html.join("");
        tree.querySelectorAll(".fuo-table-row").forEach((row) => row.addEventListener("click", () => {
            const t = row.getAttribute("data-table");
            collapsed[t] = !collapsed[t];
            const open = !collapsed[t];
            const c = row.querySelector(".fuo-caret");
            if (c) c.classList.toggle("fuo-open", open);
            const ch = row.nextElementSibling;
            if (ch && ch.classList.contains("fuo-children")) ch.style.display = open ? "block" : "none";
        }));
    }

    function currentScreen() { return model.get("screen") || "config"; }
    function route() { if (currentScreen() === "results") mountResults(); else mountConfig(); }

    model.on("change:screen", () => { view = "unused"; search = ""; collapsed = {}; route(); });
    model.on("change:running", route);
    model.on("change:query_count", () => { if (currentScreen() === "config") mountConfig(); });
    model.on("change:status", () => { if (currentScreen() === "config") mountConfig(); });
    model.on("change:dataset_name", () => { if (currentScreen() === "config") mountConfig(); });
    model.on("change:data", () => { if (currentScreen() === "results") { renderToggle(); renderTree(); } });
    model.on("change:subtitle_count", renderSubtitle);

    route();
}
export default { render };
"""


# Inject SVG icons from the shared UI components module so they stay in sync
# with the other interactive widgets (e.g. ``perspective_editor``).
from sempy_labs._ui_components import (  # noqa: E402
    ICONS as _UI_ICONS,
    LIGHT_THEME_VARS as _UI_LIGHT_VARS,
    DARK_THEME_VARS as _UI_DARK_VARS,
)

_WIDGET_CSS = _WIDGET_CSS.replace("__LIGHT__", _UI_LIGHT_VARS).replace(
    "__DARK__", _UI_DARK_VARS
)

_WIDGET_JS = (
    _WIDGET_JS.replace("__IC_SCAN__", _UI_ICONS["scan"])
    .replace("__IC_TABLE__", _UI_ICONS["table"])
    .replace("__IC_CALCTABLE__", _UI_ICONS["calculated_table"])
    .replace("__IC_CALCGROUP__", _UI_ICONS["calculation_group"])
    .replace("__IC_DATETABLE__", _UI_ICONS["date_table"])
    .replace("__IC_FIELDPARAM__", _UI_ICONS["field_parameter"])
    .replace("__IC_COLUMN__", _UI_ICONS["column"])
    .replace("__IC_CALCCOLUMN__", _UI_ICONS["calculated_column"])
    .replace("__IC_MEASURE__", _UI_ICONS["measure"])
    .replace("__IC_CARET__", _UI_ICONS["caret_right"])
    .replace("__IC_SEARCH__", _UI_ICONS["search"])
    .replace("__IC_EXPAND__", _UI_ICONS["expand_rows"])
    .replace("__IC_COLLAPSE__", _UI_ICONS["collapse_rows"])
    .replace("__IC_REFRESH__", _UI_ICONS["refresh"])
    .replace("__IC_SUN__", _UI_ICONS["sun"])
    .replace("__IC_MOON__", _UI_ICONS["moon"])
    .replace("__IC_FS__", _UI_ICONS["fullscreen"])
    .replace("__IC_FSX__", _UI_ICONS["fullscreen_exit"])
)
