import sempy.fabric as fabric
import pandas as pd
from sempy_labs._helper_functions import (
    resolve_item_name_and_id,
)
from typing import Optional, Tuple
from sempy._utils._log import log
from uuid import UUID
import time


@log
def test(
    dataset: Optional[str | UUID] = None,
    dax_string: str = "",
    workspace: Optional[str | UUID] = None,
    clear_cache: bool = True,
    visualize: bool = True,
    effective_user_name: Optional[str] = None,
    role: Optional[str] = None,
) -> pd.DataFrame:
    """
    Runs a DAX query against a semantic model while capturing a server-side
    trace, and computes high-level performance statistics (Total Duration,
    Formula Engine Duration, Storage Engine Duration, and CPU time) using
    the same conventions as `DAX Studio <https://github.com/DaxStudio/DaxStudio>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID, default=None
        Name or ID of the semantic model. Optional when ``visualize=True``:
        if not provided, the interactive widget lets you choose a workspace
        and a semantic model within it before running a query. Required when
        ``visualize=False``.
    dax_string : str, default=""
        The DAX query to execute. May be left empty when ``visualize=True``
        (you can type the query directly in the widget). Required when
        ``visualize=False``.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    clear_cache : bool, default=True
        If True, clears the dataset cache before running the query so the
        run reflects a cold-cache state.
    visualize : bool, default=True
        If True, displays an interactive widget showing the high-level
        timings (Duration, FE, SE, CPU), a per-event details table, and an
        editable DAX editor with a Run button to re-execute the query.
    effective_user_name : str, default=None
        If set, runs the query impersonating this user (passed as the
        ``effective_user_name`` parameter of ``fabric.evaluate_dax``). Use
        this for user impersonation. Cannot be used together with ``role``.
    role : str, default=None
        If set, runs the query impersonating this security role (passed as
        the ``role`` parameter of ``fabric.evaluate_dax``). Use this for
        role impersonation. Cannot be used together with
        ``effective_user_name``.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe of the captured trace events, including the
        ``Event Class``, ``Event Subclass``, ``Duration`` and ``Cpu Time``
        for each event.
    """

    from sempy_labs._helper_functions import resolve_workspace_name_and_id

    if effective_user_name and role:
        raise ValueError(
            "Cannot use both 'effective_user_name' (user impersonation) and "
            "'role' (role impersonation) at the same time. Specify at most "
            "one of them."
        )

    if not visualize:
        if dataset is None:
            raise ValueError(
                "The 'dataset' parameter is required when 'visualize=False'."
            )
        if not (dax_string and dax_string.strip()):
            raise ValueError(
                "The 'dax_string' parameter is required when 'visualize=False'."
            )

    df = pd.DataFrame()
    result_df = pd.DataFrame()
    total_duration = fe_duration = se_duration = cpu_time = 0
    dataset_name = None
    dataset_id = None
    workspace_name = None
    workspace_id = None

    if dataset is not None:
        (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
        (dataset_name, dataset_id) = resolve_item_name_and_id(
            item=dataset, type="SemanticModel", workspace=workspace_id
        )
        if dax_string and dax_string.strip():
            (
                df,
                total_duration,
                fe_duration,
                se_duration,
                cpu_time,
                result_df,
            ) = _run_dax_trace(
                dataset_id=dataset_id,
                workspace_id=workspace_id,
                dax_string=dax_string,
                clear_cache=clear_cache,
                effective_user_name=effective_user_name,
                role=role,
            )
    elif workspace is not None:
        # No dataset chosen yet, but a workspace was provided: resolve it so
        # the widget's model picker can pre-select that workspace.
        (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if visualize:
        _visualize_dax_test(
            df=df,
            total_duration=total_duration,
            fe_duration=fe_duration,
            se_duration=se_duration,
            cpu_time=cpu_time,
            dax_string=dax_string,
            dataset_id=dataset_id,
            workspace_id=workspace_id,
            dataset_name=(
                (str(dataset_name) if dataset_name else str(dataset))
                if dataset is not None
                else None
            ),
            workspace_name=workspace_name,
            clear_cache=clear_cache,
            result_df=result_df,
            effective_user_name=effective_user_name,
            role=role,
        )

    return df


# Trace event schema captured by :func:`test` / :func:`_run_dax_trace`.
_TEST_EVENT_SCHEMA: dict = {
    "QueryBegin": [
        "EventClass",
        "EventSubclass",
        "CurrentTime",
        "NTUserName",
        "TextData",
        "StartTime",
        "ApplicationName",
    ],
    "QueryEnd": [
        "EventClass",
        "EventSubclass",
        "CurrentTime",
        "NTUserName",
        "TextData",
        "StartTime",
        "EndTime",
        "Duration",
        "CpuTime",
        "Success",
        "ApplicationName",
    ],
    "VertiPaqSEQueryBegin": [
        "EventClass",
        "EventSubclass",
        "CurrentTime",
        "NTUserName",
        "TextData",
        "StartTime",
    ],
    "VertiPaqSEQueryEnd": [
        "EventClass",
        "EventSubclass",
        "CurrentTime",
        "NTUserName",
        "TextData",
        "StartTime",
        "EndTime",
        "Duration",
        "CpuTime",
        "Success",
    ],
    "VertiPaqSEQueryCacheMatch": [
        "EventClass",
        "EventSubclass",
        "CurrentTime",
        "NTUserName",
        "TextData",
    ],
}


def _run_dax_trace(
    dataset_id: str,
    workspace_id: str,
    dax_string: str,
    clear_cache: bool,
    effective_user_name: Optional[str] = None,
    role: Optional[str] = None,
) -> Tuple[pd.DataFrame, int, int, int, int, pd.DataFrame]:
    """Run a DAX query with a server-side trace and compute DAX Studio style
    aggregate stats.

    Returns
    -------
    tuple
        ``(df, total_duration, fe_duration, se_duration, cpu_time, result_df)``
    """
    from sempy_labs._clear_cache import clear_cache as _clear_cache_fn

    if clear_cache:
        _clear_cache_fn(dataset=dataset_id, workspace=workspace_id)

    result_df: pd.DataFrame = pd.DataFrame()
    with fabric.create_trace_connection(
        dataset=dataset_id, workspace=workspace_id
    ) as trace_connection:
        with trace_connection.create_trace(_TEST_EVENT_SCHEMA) as trace:
            trace.start()
            # Warm-up evaluation; filtered out of results below.
            fabric.evaluate_dax(
                dataset=dataset_id,
                workspace=workspace_id,
                dax_string="EVALUATE {1}",
            )
            # Run the actual DAX query.
            result_df = fabric.evaluate_dax(
                dataset=dataset_id,
                workspace=workspace_id,
                dax_string=dax_string,
                effective_user_name=effective_user_name,
                role=role,
            )
            # Allow the trace some time to flush events.
            time.sleep(2)
            df = trace.stop()

    # Drop events from other sessions / warm-up evaluation.
    if "Application Name" in df.columns:
        df = df[~df["Application Name"].isin(["PowerBI", "PowerBIEIM"])]
    if "Text Data" in df.columns:
        df = df[~df["Text Data"].astype(str).str.startswith("EVALUATE {1}")]
    df = df.reset_index(drop=True)

    # Compute aggregate stats using DAX Studio conventions:
    #   Total Duration = QueryEnd.Duration
    #   SE Duration    = sum of VertiPaqSEQueryEnd Duration, EXCLUDING
    #                    internal sub-queries (EventSubclass contains
    #                    "Internal") so we do not double-count time that
    #                    is already rolled up into the parent scan.
    #   FE Duration    = Total Duration - SE Duration
    #   CPU            = QueryEnd.CpuTime
    qe = df[df["Event Class"] == "QueryEnd"]
    total_duration = int(qe["Duration"].iloc[-1]) if not qe.empty else 0
    cpu_time = int(qe["Cpu Time"].iloc[-1]) if not qe.empty else 0

    se_events = df[df["Event Class"] == "VertiPaqSEQueryEnd"]
    if not se_events.empty:
        not_internal = (
            ~se_events["Event Subclass"]
            .astype(str)
            .str.contains("Internal", case=False, na=False)
        )
        se_duration = int(se_events.loc[not_internal, "Duration"].sum())
    else:
        se_duration = 0
    fe_duration = max(total_duration - se_duration, 0)

    return df, total_duration, fe_duration, se_duration, cpu_time, result_df


def _trace_rows_from_df(df: pd.DataFrame) -> list:
    """Convert the captured trace dataframe to a list of plain-dict rows
    suitable for serialization to the front-end."""

    detail_classes = {"QueryEnd", "VertiPaqSEQueryEnd", "VertiPaqSEQueryCacheMatch"}
    if df is None or df.empty:
        return []
    rows_df = df[df["Event Class"].isin(detail_classes)]
    out = []
    for _, row in rows_df.iterrows():
        ec = str(row.get("Event Class", "") or "")
        sc = str(row.get("Event Subclass", "") or "")
        dur = row.get("Duration", 0)
        cpu = row.get("Cpu Time", 0)
        try:
            dur_v = int(dur) if pd.notna(dur) else 0
        except (TypeError, ValueError):
            dur_v = 0
        try:
            cpu_v = int(cpu) if pd.notna(cpu) else 0
        except (TypeError, ValueError):
            cpu_v = 0
        out.append(
            {
                "event_class": ec,
                "event_subclass": sc if sc else ec,
                "duration": dur_v,
                "cpu": cpu_v,
            }
        )
    return out


def _result_payload_from_df(df: pd.DataFrame, max_rows: int = 5000) -> dict:
    """Convert a query result dataframe to a payload of ``{columns, rows,
    total_rows, truncated}`` for the front-end."""

    if df is None or not hasattr(df, "columns"):
        return {"columns": [], "rows": [], "total_rows": 0, "truncated": False}
    columns = [str(c) for c in df.columns]
    total_rows = int(len(df))
    truncated = total_rows > max_rows
    view = df.head(max_rows) if truncated else df
    rows: list = []
    for _, r in view.iterrows():
        row: list = []
        for v in r.tolist():
            if v is None:
                row.append(None)
            elif pd.isna(v):
                row.append(None)
            else:
                row.append(v if isinstance(v, (int, float, bool, str)) else str(v))
        rows.append(row)
    return {
        "columns": columns,
        "rows": rows,
        "total_rows": total_rows,
        "truncated": truncated,
    }


def _collect_model_tree(dataset_id: str, workspace_id: str) -> list:
    """Collect a lightweight metadata tree of the semantic model for the
    sidebar (tables → columns / measures / hierarchies)."""

    try:
        from sempy_labs.tom import connect_semantic_model
    except Exception:
        return []

    tree: list = []
    try:
        with connect_semantic_model(
            dataset=dataset_id, workspace=workspace_id, readonly=True
        ) as tom:
            for table in tom.model.Tables:
                tname = str(table.Name)
                columns = sorted(
                    (
                        {
                            "name": str(c.Name),
                            "hidden": bool(getattr(c, "IsHidden", False)),
                            "data_type": str(getattr(c, "DataType", "") or ""),
                            "description": str(getattr(c, "Description", "") or ""),
                            "display_folder": str(
                                getattr(c, "DisplayFolder", "") or ""
                            ),
                        }
                        for c in table.Columns
                        if str(getattr(c, "Type", "")) != "RowNumber"
                    ),
                    key=lambda x: x["name"].lower(),
                )
                measures = sorted(
                    (
                        {
                            "name": str(m.Name),
                            "hidden": bool(getattr(m, "IsHidden", False)),
                            "description": str(getattr(m, "Description", "") or ""),
                            "display_folder": str(
                                getattr(m, "DisplayFolder", "") or ""
                            ),
                        }
                        for m in table.Measures
                    ),
                    key=lambda x: x["name"].lower(),
                )
                hierarchies = sorted(
                    (
                        {
                            "name": str(h.Name),
                            "hidden": bool(getattr(h, "IsHidden", False)),
                            "description": str(getattr(h, "Description", "") or ""),
                            "display_folder": str(
                                getattr(h, "DisplayFolder", "") or ""
                            ),
                            "levels": [
                                {
                                    "name": str(lvl.Name),
                                    "description": str(
                                        getattr(lvl, "Description", "") or ""
                                    ),
                                }
                                for lvl in sorted(
                                    h.Levels,
                                    key=lambda lvl: getattr(lvl, "Ordinal", 0),
                                )
                            ],
                        }
                        for h in table.Hierarchies
                    ),
                    key=lambda x: x["name"].lower(),
                )
                is_calc_group = (
                    getattr(table, "CalculationGroup", None) is not None
                )
                calculation_items: list = []
                if is_calc_group:
                    calculation_items = sorted(
                        (
                            {
                                "name": str(ci.Name),
                                "description": str(
                                    getattr(ci, "Description", "") or ""
                                ),
                            }
                            for ci in table.CalculationGroup.CalculationItems
                        ),
                        key=lambda x: x["name"].lower(),
                    )
                tree.append(
                    {
                        "name": tname,
                        "hidden": bool(getattr(table, "IsHidden", False)),
                        "description": str(getattr(table, "Description", "") or ""),
                        "calculation_group": bool(is_calc_group),
                        "calculation_items": calculation_items,
                        "columns": columns,
                        "measures": measures,
                        "hierarchies": hierarchies,
                    }
                )
    except Exception:
        return []
    tree.sort(key=lambda t: t["name"].lower())
    return tree


def _collect_model_roles(dataset_id: str, workspace_id: str) -> list:
    """Collect the security role names defined in the semantic model, sorted
    alphabetically (case-insensitive). Returns an empty list if the model has
    no roles or the metadata cannot be read."""

    try:
        from sempy_labs.tom import connect_semantic_model
    except Exception:
        return []

    roles: list = []
    try:
        with connect_semantic_model(
            dataset=dataset_id, workspace=workspace_id, readonly=True
        ) as tom:
            roles = [str(r.Name) for r in tom.model.Roles]
    except Exception:
        return []
    roles.sort(key=lambda x: x.lower())
    return roles


def _list_workspaces_for_picker() -> list:
    """Return a list of ``{"id", "name"}`` dicts for the workspaces the user
    can access, sorted alphabetically. Used by the interactive widget's model
    picker when no ``dataset`` is supplied to :func:`test`."""

    out: list = []
    try:
        dfW = fabric.list_workspaces()
    except Exception:
        return []
    for _, r in dfW.iterrows():
        out.append({"id": str(r["Id"]), "name": str(r["Name"])})
    out.sort(key=lambda x: x["name"].lower())
    return out


def _list_datasets_for_picker(workspace_id: str) -> list:
    """Return a list of ``{"id", "name"}`` dicts for the semantic models in
    the given workspace, sorted alphabetically. Used by the interactive
    widget's model picker."""

    out: list = []
    try:
        dfD = fabric.list_datasets(workspace=workspace_id, mode="rest")
    except Exception:
        return []
    for _, r in dfD.iterrows():
        out.append(
            {"id": str(r["Dataset Id"]), "name": str(r["Dataset Name"])}
        )
    out.sort(key=lambda x: x["name"].lower())
    return out


def _pick_groupby_column(table) -> Optional[str]:
    """Pick a reasonable column to group by from a TOM table: prefer a
    visible string/text column, otherwise the first visible non-RowNumber
    column. Returns the column name or None."""

    candidate = None
    try:
        columns = list(table.Columns)
    except Exception:
        return None
    for c in columns:
        try:
            name = str(c.Name)
            col_type = str(getattr(c, "Type", ""))
            if col_type == "RowNumber" or name == "RowNumber":
                continue
            if getattr(c, "IsHidden", False):
                continue
        except Exception:
            continue
        if candidate is None:
            candidate = name
        if str(getattr(c, "DataType", "")) == "String":
            return name
    return candidate


def _generate_sample_dax(dataset_id: str, workspace_id: str) -> str:
    """Generate a simple ``EVALUATE SUMMARIZECOLUMNS(...)`` DAX query against
    the model: a measure grouped by a column from a table related to the
    measure's home table (falling back to a column from the measure's own
    table when no relationship exists). Returns an empty string if the model
    has no usable measure/column."""

    try:
        from sempy_labs.tom import connect_semantic_model
    except Exception:
        return ""

    try:
        with connect_semantic_model(
            dataset=dataset_id, workspace=workspace_id, readonly=True
        ) as tom:
            # Pick a measure (prefer a visible one).
            measure = None
            for m in tom.all_measures():
                if measure is None:
                    measure = m
                if not getattr(m, "IsHidden", False):
                    measure = m
                    break
            if measure is None:
                return ""
            measure_name = str(measure.Name)
            try:
                measure_table = str(measure.Parent.Name)
            except Exception:
                measure_table = None

            group_table = None
            group_column = None

            # Look for a table related to the measure's home table and pick a
            # column from it.
            if measure_table is not None:
                for rel in tom.model.Relationships:
                    try:
                        from_table = str(rel.FromTable.Name)
                        to_table = str(rel.ToTable.Name)
                    except Exception:
                        continue
                    other = None
                    if from_table == measure_table:
                        other = to_table
                    elif to_table == measure_table:
                        other = from_table
                    if other is None:
                        continue
                    try:
                        other_tbl = tom.model.Tables[other]
                    except Exception:
                        continue
                    col = _pick_groupby_column(other_tbl)
                    if col:
                        group_table = other
                        group_column = col
                        break

            # Fall back to a column from the measure's own table.
            if group_column is None and measure_table is not None:
                try:
                    own_tbl = tom.model.Tables[measure_table]
                    col = _pick_groupby_column(own_tbl)
                    if col:
                        group_table = measure_table
                        group_column = col
                except Exception:
                    pass
    except Exception:
        return ""

    def _tbl_ref(name: str) -> str:
        return "'" + name.replace("'", "''") + "'"

    def _mea_ref(name: str) -> str:
        return "[" + name.replace("]", "]]") + "]"

    if group_table and group_column:
        col_ref = _tbl_ref(group_table) + _mea_ref(group_column)
        return (
            "EVALUATE\n"
            "SUMMARIZECOLUMNS(\n"
            f"    {col_ref},\n"
            f'    "{measure_name}", {_mea_ref(measure_name)}\n'
            ")"
        )
    # No usable column: emit a measure-only query.
    return (
        "EVALUATE\n"
        "ROW(\n"
        f'    "{measure_name}", {_mea_ref(measure_name)}\n'
        ")"
    )


def _classify_filter_type(kind: str, data_type: str) -> str:
    """Classify a query-builder field into a filter family used to pick the
    available filter operators: ``measure``, ``numeric``, ``datetime``,
    ``boolean`` or ``text``."""

    if kind == "measure":
        return "measure"
    dt = (data_type or "").strip().lower()
    if dt in ("int64", "double", "decimal", "currency", "int", "integer"):
        return "numeric"
    if dt in ("datetime", "date", "time"):
        return "datetime"
    if dt in ("boolean", "bool"):
        return "boolean"
    return "text"


def _qb_quote_str(value: str) -> str:
    """Return a DAX string literal for ``value`` (double quotes escaped)."""

    return '"' + str(value).replace('"', '""') + '"'


def _qb_numeric_literal(value: str) -> str:
    """Return a numeric DAX literal for ``value`` or a quoted string if the
    value is not numeric."""

    raw = str(value).strip()
    try:
        float(raw)
        return raw
    except ValueError:
        return _qb_quote_str(raw)


def _qb_value_literal(filter_type: str, value: str) -> str:
    """Return a DAX literal for a filter value based on its filter family."""

    import re as _re

    raw = str(value).strip()
    if filter_type in ("numeric", "measure"):
        return _qb_numeric_literal(raw)
    if filter_type == "datetime":
        m = _re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", raw)
        if m:
            return f"DATE({int(m.group(1))}, {int(m.group(2))}, " f"{int(m.group(3))})"
        return _qb_quote_str(raw)
    return _qb_quote_str(raw)


def _qb_build_predicate(item: dict) -> Optional[str]:
    """Build a single DAX boolean predicate for a query-builder filter item.
    Returns None if the operator/value combination is unusable."""

    ref = item.get("ref") or ""
    if not ref:
        return None
    ftype = _classify_filter_type(
        item.get("kind", "column"), item.get("data_type", "")
    )
    op = (item.get("op") or "").strip()
    value = item.get("value", "")
    value2 = item.get("value2", "")

    if op == "blank":
        return f"ISBLANK({ref})"
    if op == "notblank":
        return f"NOT ISBLANK({ref})"
    if op == "istrue":
        return f"{ref} = TRUE()"
    if op == "isfalse":
        return f"{ref} = FALSE()"

    if op in ("in", "notin"):
        items = [v.strip() for v in str(value).split(",")]
        items = [v for v in items if v != ""]
        if not items:
            return None
        literals = ", ".join(_qb_value_literal(ftype, v) for v in items)
        predicate = f"{ref} IN {{{literals}}}"
        return f"NOT({predicate})" if op == "notin" else predicate

    if ftype == "text":
        if op == "contains":
            return f"CONTAINSSTRING({ref}, {_qb_quote_str(value)})"
        if op == "startswith":
            length = len(str(value))
            return f"LEFT({ref}, {length}) = {_qb_quote_str(value)}"
        if op == "eq":
            return f"{ref} = {_qb_quote_str(value)}"
        if op == "ne":
            return f"{ref} <> {_qb_quote_str(value)}"
        return None

    symbol = {
        "eq": "=",
        "ne": "<>",
        "gt": ">",
        "ge": ">=",
        "lt": "<",
        "le": "<=",
    }.get(op)
    if op == "between":
        lo = _qb_value_literal(ftype, value)
        hi = _qb_value_literal(ftype, value2)
        return f"{ref} >= {lo} && {ref} <= {hi}"
    if symbol is not None:
        return f"{ref} {symbol} {_qb_value_literal(ftype, value)}"
    return None


def _build_summarize_dax(state: dict, dataset_id: str, workspace_id: str) -> str:
    """Build an ``EVALUATE SUMMARIZECOLUMNS(...)`` DAX statement from a
    query-builder state (a dict with ``fields`` and ``filters`` lists).

    The generated query follows the canonical "DAX as a query language"
    pattern (see ``.claude/skills/sql_to_dax`` / the Query Builder SKILL):

    1. ``EVALUATE``
    2. ``SUMMARIZECOLUMNS(`` with elements in strict order:
       a. attributes (group-by columns),
       b. filters (column filters via ``FILTER(KEEPFILTERS(VALUES(col)), ...)``),
       c. measures (``"Name", [Measure]``),
    3. measure filters wrap the table via ``FILTER(SUMMARIZECOLUMNS(...), ...)``,
    4. ``ORDER BY`` the attribute columns.

    Returns an empty string if there is nothing usable to build.
    """

    fields = state.get("fields") or []
    filters = state.get("filters") or []

    group_cols = [f for f in fields if f.get("kind") == "column"]
    measures = [f for f in fields if f.get("kind") == "measure"]
    if not group_cols and not measures:
        return ""

    def _tbl_ref(name: str) -> str:
        return "'" + str(name).replace("'", "''") + "'"

    def _mea_ref(name: str) -> str:
        return "[" + str(name).replace("]", "]]") + "]"

    def _col_ref(item: dict) -> str:
        ref = item.get("ref")
        if ref:
            return str(ref)
        return _tbl_ref(item.get("table")) + _mea_ref(item.get("name"))

    # Split filters into column predicates (applied inline within
    # SUMMARIZECOLUMNS) and measure predicates (applied via an outer FILTER
    # over the summarized table, since measures are projected as columns).
    col_filters: list = []
    meas_preds: list = []
    for item in filters:
        pred = _qb_build_predicate(item)
        if not pred:
            continue
        if item.get("kind") == "measure":
            meas_preds.append(pred)
        else:
            col_filters.append(
                f"FILTER(KEEPFILTERS(VALUES({_col_ref(item)})), {pred})"
            )

    # SUMMARIZECOLUMNS elements: attributes, then column filters, then
    # measures (in that exact order, as required by the engine).
    parts: list = []
    for c in group_cols:
        parts.append(_col_ref(c))
    parts.extend(col_filters)
    for m in measures:
        parts.append(f'{_qb_quote_str(m.get("name"))}, {_mea_ref(m.get("name"))}')

    inner = "SUMMARIZECOLUMNS(" + ", ".join(parts) + ")"

    # Measure-based filters cannot live inside SUMMARIZECOLUMNS; wrap the
    # whole table in a FILTER referencing the measure columns.
    if meas_preds:
        inner = "FILTER(" + inner + ", " + " && ".join(meas_preds) + ")"

    dax = "EVALUATE\n" + inner

    # ORDER BY the attribute columns (ascending), per the canonical pattern.
    if group_cols:
        order_cols = ", ".join(_col_ref(c) for c in group_cols)
        dax += "\nORDER BY " + order_cols

    return dax


def _classify_dax_spans(dax_expression: str) -> list:
    """Classify a DAX expression into a flat list of ``{text, kind}`` spans
    using the project's DAX parser/tokenizer.

    Spans cover the full string including inter-token whitespace (which has
    ``kind = ""``) so the front-end can faithfully reproduce the input
    layout while applying syntax colors.
    """

    if not dax_expression:
        return []
    try:
        from sempy_labs.dax._format import _classify_tokens
        classified = _classify_tokens(dax_expression)
    except Exception:
        return [{"text": dax_expression, "kind": ""}]

    spans: list = []
    cursor = 0
    for token, kind in classified:
        if token.position > cursor:
            spans.append({"text": dax_expression[cursor:token.position], "kind": ""})
        spans.append({"text": token.text, "kind": kind or ""})
        cursor = token.position + len(token.text)
    if cursor < len(dax_expression):
        spans.append({"text": dax_expression[cursor:], "kind": ""})
    return spans


def _visualize_dax_test(
    df: pd.DataFrame,
    total_duration: int,
    fe_duration: int,
    se_duration: int,
    cpu_time: int,
    dax_string: str,
    dataset_id: Optional[str],
    workspace_id: Optional[str],
    dataset_name: Optional[str] = None,
    workspace_name: Optional[str] = None,
    dark_mode: bool = False,
    clear_cache: bool = True,
    result_df: Optional[pd.DataFrame] = None,
    effective_user_name: Optional[str] = None,
    role: Optional[str] = None,
) -> None:
    """Render an interactive editable DAX widget for :func:`test` results."""

    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "Visualizing 'test()' requires the 'anywidget' package. "
            "Install it with: pip install anywidget"
        ) from e

    from IPython.display import display
    from sempy_labs._daxformatter import _format_dax
    from sempy_labs._ui_components import (
        LIGHT_THEME_VARS as _UI_LIGHT_VARS,
        DARK_THEME_VARS as _UI_DARK_VARS,
        HEADER_CSS as _UI_HEADER_CSS,
        ATTRIBUTION_CSS as _UI_ATTRIBUTION_CSS,
        ICONS as _UI_ICONS,
    )

    try:
        _formatted = (
            _format_dax(dax_string) if dax_string and dax_string.strip() else []
        )
        formatted_initial = _formatted[0] if _formatted else (dax_string or "")
    except Exception:
        formatted_initial = dax_string or ""

    # Normalize line endings to "\n". The DAX Formatter API returns "\r\n"
    # line endings, but a <textarea> normalizes those to "\n" on assignment.
    # If left un-normalized, the classified token text (which still contains
    # "\r") no longer matches the textarea length, so the front-end falls
    # back to plain (uncolored) rendering of the DAX.
    formatted_initial = formatted_initial.replace("\r\n", "\n").replace("\r", "\n")
    initial_rows = _trace_rows_from_df(df)

    widget_css = (
        _UI_HEADER_CSS
        + "\n"
        + _UI_ATTRIBUTION_CSS
        + "\n"
        + f"""
.dtx {{
    {_UI_LIGHT_VARS}
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display",
        "SF Pro Text", "Helvetica Neue", Helvetica, Arial, sans-serif;
    color: var(--ui-text);
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    max-width: 100%;
    margin: 0;
    padding: 0;
}}
.dtx.dtx-dark {{
    {_UI_DARK_VARS}
}}
.dtx *, .dtx *::before, .dtx *::after {{ box-sizing: border-box; }}
.dtx .dtx-container {{
    background: var(--ui-bg);
    border: 1px solid var(--ui-border);
    border-radius: 12px;
    box-shadow: var(--ui-shadow-lg);
    overflow: hidden;
}}
.dtx .dtx-header {{
    padding: 22px 24px 18px 24px;
    background: var(--ui-bg);
}}
.dtx .dtx-cards {{
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 12px;
    flex: 0 0 auto;
    padding: 0 24px 18px 24px;
}}
.dtx .dtx-card {{
    background: var(--ui-bg-secondary);
    border: 1px solid var(--ui-border);
    border-radius: 8px;
    padding: 14px 16px;
    display: flex;
    flex-direction: column;
    gap: 4px;
}}
.dtx .dtx-card-label {{
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--ui-text-tertiary);
}}
.dtx .dtx-card-value {{
    font-size: 22px;
    font-weight: 700;
    letter-spacing: -0.02em;
    color: var(--ui-text);
    font-variant-numeric: tabular-nums;
    line-height: 1.1;
}}
.dtx .dtx-card-unit {{
    font-size: 12px;
    font-weight: 500;
    color: var(--ui-text-tertiary);
    margin-left: 4px;
}}
.dtx .dtx-card-sub {{
    font-size: 11px;
    color: var(--ui-text-secondary);
    font-variant-numeric: tabular-nums;
    margin-top: 2px;
}}
.dtx .dtx-query-block {{
    margin: 0 24px 16px 24px;
}}
.dtx .dtx-query-toolbar {{
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
}}
.dtx .dtx-query-titlegroup {{
    display: flex;
    align-items: center;
    gap: 8px;
    margin-right: auto;
}}
.dtx .dtx-query-title {{
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--ui-text-tertiary);
}}
.dtx .dtx-gen-btn {{
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-size: 11px;
    font-weight: 600;
    line-height: 1;
    padding: 4px 10px;
    border-radius: 6px;
    border: 1px solid var(--ui-border);
    background: var(--ui-surface);
    color: var(--ui-text-secondary);
    cursor: pointer;
}}
.dtx .dtx-gen-btn:hover:not(:disabled) {{
    border-color: var(--ui-accent);
    color: var(--ui-accent);
}}
.dtx .dtx-gen-btn:disabled {{
    opacity: 0.5;
    cursor: default;
}}
.dtx .dtx-change-btn {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 26px;
    height: 26px;
    padding: 0;
    margin-left: 4px;
    flex: 0 0 auto;
    border-radius: 6px;
    border: 1px solid var(--ui-border);
    background: transparent;
    color: var(--ui-text-secondary);
    cursor: pointer;
}}
.dtx .dtx-change-btn svg {{
    width: 15px;
    height: 15px;
}}
.dtx .dtx-change-btn:hover {{
    border-color: var(--ui-accent);
    color: var(--ui-accent);
}}
.dtx .dtx-builder-show-btn {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 30px;
    height: 30px;
    padding: 0;
    margin-right: 6px;
    flex: 0 0 auto;
    border-radius: 7px;
    border: 1px solid var(--ui-border);
    background: transparent;
    color: var(--ui-text-secondary);
    cursor: pointer;
}}
.dtx .dtx-builder-show-btn svg {{
    width: 17px;
    height: 17px;
}}
.dtx .dtx-builder-show-btn:hover {{
    border-color: var(--ui-accent);
    color: var(--ui-accent);
}}
.dtx .dtx-builder-show-btn.dtx-active {{
    border-color: var(--ui-accent);
    background: var(--ui-accent);
    color: #fff;
}}
.dtx .dtx-picker-cancel {{
    display: inline-flex;
    align-items: center;
    font-size: 12px;
    line-height: 1;
    padding: 5px 10px;
    border-radius: 6px;
    border: 1px solid var(--ui-border);
    background: transparent;
    color: var(--ui-text-secondary);
    cursor: pointer;
}}
.dtx .dtx-picker-cancel:hover {{
    border-color: var(--ui-accent);
    color: var(--ui-accent);
}}
.dtx .dtx-query-placeholder {{
    color: var(--ui-text-tertiary);
    font-style: italic;
}}
.dtx .dtx-cache-label {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    color: var(--ui-text-secondary);
    cursor: pointer;
    user-select: none;
}}
.dtx .dtx-cache-label input {{
    cursor: pointer;
    accent-color: var(--ui-accent);
}}
.dtx .dtx-imp-wrap {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
}}
.dtx .dtx-imp-select {{
    appearance: none;
    -webkit-appearance: none;
    background: var(--ui-bg-secondary);
    border: 1px solid var(--ui-border);
    border-radius: 6px;
    padding: 5px 26px 5px 8px;
    font-family: inherit;
    font-size: 12px;
    color: var(--ui-text);
    cursor: pointer;
    background-image: linear-gradient(45deg, transparent 50%, var(--ui-text-tertiary) 50%),
        linear-gradient(135deg, var(--ui-text-tertiary) 50%, transparent 50%);
    background-position: calc(100% - 14px) 50%, calc(100% - 9px) 50%;
    background-size: 5px 5px;
    background-repeat: no-repeat;
}}
.dtx .dtx-imp-select:focus {{
    outline: none;
    border-color: var(--ui-accent);
    box-shadow: 0 0 0 3px var(--ui-accent-soft);
}}
.dtx .dtx-imp-input {{
    background: var(--ui-bg-secondary);
    border: 1px solid var(--ui-border);
    border-radius: 6px;
    padding: 5px 8px;
    font-family: inherit;
    font-size: 12px;
    color: var(--ui-text);
    width: 150px;
}}
.dtx .dtx-imp-input:focus {{
    outline: none;
    border-color: var(--ui-accent);
    box-shadow: 0 0 0 3px var(--ui-accent-soft);
}}
.dtx .dtx-picker {{
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
    flex: 0 0 auto;
    padding: 14px 24px;
    margin-bottom: 18px;
    border-bottom: 1px solid var(--ui-border);
    background: var(--ui-bg-secondary);
}}
.dtx .dtx-picker-label {{
    font-size: 13px;
    font-weight: 600;
    color: var(--ui-text-secondary);
}}
.dtx .dtx-picker-select {{ min-width: 190px; }}
.dtx .dtx-picker-spin {{ font-size: 12px; color: var(--ui-text-tertiary); }}
.dtx .dtx-picker-btn {{
    appearance: none;
    -webkit-appearance: none;
    border: 1px solid var(--ui-accent);
    background: var(--ui-accent);
    color: #fff;
    padding: 6px 14px;
    border-radius: 6px;
    font-family: inherit;
    font-size: 12px;
    font-weight: 600;
    cursor: pointer;
    transition: background 120ms ease, opacity 120ms ease;
}}
.dtx .dtx-picker-btn:hover {{ background: var(--ui-accent-hover); }}
.dtx .dtx-picker-btn:disabled {{ opacity: 0.5; cursor: not-allowed; }}
.dtx .dtx-icon-btn {{
    appearance: none;
    -webkit-appearance: none;
    border: 1px solid var(--ui-border-strong);
    background: var(--ui-surface);
    color: var(--ui-text-secondary);
    width: 30px;
    height: 30px;
    padding: 0;
    border-radius: 6px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    cursor: pointer;
    font-family: inherit;
    transition: background 120ms ease, border-color 120ms ease,
        color 120ms ease, transform 80ms ease;
}}
.dtx .dtx-icon-btn:hover {{
    background: var(--ui-surface-2);
    border-color: var(--ui-text-tertiary);
    color: var(--ui-text);
}}
.dtx .dtx-icon-btn:active {{ transform: scale(0.95); }}
.dtx .dtx-icon-btn svg {{ width: 16px; height: 16px; display: block; }}
.dtx .dtx-icon-btn.dtx-icon-btn-on {{
    color: var(--ui-accent);
    border-color: var(--ui-accent);
    background: var(--ui-accent-soft);
}}
.dtx .dtx-icon-btn.dtx-icon-btn-on:hover {{
    background: var(--ui-accent-soft);
    border-color: var(--ui-accent);
    color: var(--ui-accent);
}}
.dtx .dtx-view-toolbar {{
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 4px 24px 10px 24px;
}}
.dtx .dtx-view-title {{
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--ui-text-tertiary);
    margin-right: auto;
}}
.dtx .dtx-seg {{
    display: inline-flex;
    background: var(--ui-bg-secondary);
    border: 1px solid var(--ui-border);
    border-radius: 8px;
    padding: 2px;
    gap: 2px;
}}
.dtx .dtx-seg-btn {{
    appearance: none;
    -webkit-appearance: none;
    border: none;
    background: transparent;
    color: var(--ui-text-secondary);
    padding: 5px 12px;
    border-radius: 6px;
    font-family: inherit;
    font-size: 12px;
    font-weight: 500;
    cursor: pointer;
    transition: background 120ms ease, color 120ms ease;
}}
.dtx .dtx-seg-btn:hover {{ color: var(--ui-text); }}
.dtx .dtx-seg-btn.dtx-seg-btn-on {{
    background: var(--ui-bg);
    color: var(--ui-text);
    box-shadow: var(--ui-shadow-sm);
}}
.dtx .dtx-seg-btn[disabled] {{
    opacity: 0.45;
    cursor: not-allowed;
}}
.dtx .dtx-seg-btn[disabled]:hover {{ color: var(--ui-text-secondary); }}
.dtx .dtx-chart-wrap {{
    padding: 12px 24px 20px 24px;
    overflow-x: auto;
    overflow-y: hidden;
}}
.dtx .dtx-chart-empty {{
    padding: 24px;
    text-align: center;
    color: var(--ui-text-tertiary);
    font-size: 12px;
}}
.dtx .dtx-chart-controls {{
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
    padding: 0 24px 8px 24px;
}}
.dtx .dtx-chart-controls label {{
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-size: 11px;
    color: var(--ui-text-tertiary);
    text-transform: uppercase;
    font-weight: 600;
    letter-spacing: 0.04em;
}}
.dtx .dtx-chart-controls select {{
    appearance: none;
    -webkit-appearance: none;
    background: var(--ui-bg-secondary);
    border: 1px solid var(--ui-border);
    border-radius: 6px;
    padding: 4px 26px 4px 8px;
    font-family: inherit;
    font-size: 12px;
    color: var(--ui-text);
    cursor: pointer;
    background-image: linear-gradient(45deg, transparent 50%, var(--ui-text-tertiary) 50%),
        linear-gradient(135deg, var(--ui-text-tertiary) 50%, transparent 50%);
    background-position: calc(100% - 14px) 50%, calc(100% - 9px) 50%;
    background-size: 5px 5px;
    background-repeat: no-repeat;
}}
.dtx .dtx-chart-controls select:focus {{
    outline: none;
    border-color: var(--ui-accent);
    box-shadow: 0 0 0 3px var(--ui-accent-soft);
}}
.dtx .dtx-chart-svg {{ display: block; max-width: 100%; }}
.dtx .dtx-chart-bar {{ fill: var(--ui-accent); transition: opacity 100ms ease; }}
.dtx .dtx-chart-bar:hover {{ opacity: 0.8; }}
.dtx .dtx-chart-axis line, .dtx .dtx-chart-axis path {{ stroke: var(--ui-border-strong); fill: none; }}
.dtx .dtx-chart-axis text {{ fill: var(--ui-text-secondary); font-size: 10px; font-family: inherit; }}
.dtx .dtx-chart-grid line {{ stroke: var(--ui-border); stroke-dasharray: 2 3; }}
.dtx .dtx-result-meta {{
    padding: 0 24px 8px 24px;
    font-size: 11px;
    color: var(--ui-text-tertiary);
    font-variant-numeric: tabular-nums;
}}
.dtx .dtx-btn {{
    appearance: none;
    -webkit-appearance: none;
    border: 1px solid var(--ui-accent);
    background: var(--ui-accent);
    color: #fff;
    width: 32px;
    height: 32px;
    padding: 0;
    border-radius: 50%;
    cursor: pointer;
    font-family: inherit;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    transition: background 120ms ease, border-color 120ms ease,
        opacity 120ms ease, transform 80ms ease;
}}
.dtx .dtx-btn:hover {{ background: var(--ui-accent-hover); border-color: var(--ui-accent-hover); }}
.dtx .dtx-btn:active {{ transform: scale(0.94); }}
.dtx .dtx-btn[disabled] {{
    opacity: 0.55;
    cursor: not-allowed;
}}
.dtx .dtx-btn svg {{ width: 14px; height: 14px; display: block; }}
.dtx .dtx-btn.dtx-btn-stop {{
    background: #dc2626;
    border-color: #dc2626;
}}
.dtx .dtx-btn.dtx-btn-stop:hover {{
    background: #b91c1c;
    border-color: #b91c1c;
}}
.dtx .dtx-query-wrap {{
    position: relative;
}}
.dtx .dtx-query-hl {{
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    margin: 0;
    padding: 12px 14px;
    border: 1px solid transparent;
    border-radius: 8px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 12px;
    line-height: 1.5;
    color: var(--ui-text);
    background: transparent;
    white-space: pre-wrap;
    word-wrap: break-word;
    overflow: hidden;
    pointer-events: none;
    z-index: 2;
}}
.dtx .dtx-query-hl span {{ background: transparent; }}
.dtx .dtx-query-hl .dtx-tk-function,
.dtx .dtx-query-hl .dtx-tk-keyword {{ color: #5E9EFF !important; }}
.dtx .dtx-query-hl .dtx-tk-variable {{ color: #5AC8B8 !important; }}
.dtx .dtx-query-hl .dtx-tk-number {{ color: #FF9F45 !important; }}
.dtx .dtx-query-hl .dtx-tk-virtual_column {{ color: #FF7A8A !important; }}
.dtx .dtx-query-hl .dtx-tk-string {{ color: #9BB87A !important; }}
.dtx .dtx-query-hl .dtx-tk-operator,
.dtx .dtx-query-hl .dtx-tk-punctuation {{ color: #A6A6A6 !important; }}
.dtx .dtx-query {{
    width: 100%;
    min-height: 120px;
    max-height: 320px;
    padding: 12px 14px;
    background: var(--ui-bg-tertiary);
    border: 1px solid var(--ui-border);
    border-radius: 8px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 12px;
    line-height: 1.5;
    color: transparent;
    caret-color: var(--ui-text);
    -webkit-text-fill-color: transparent;
    resize: vertical;
    outline: none;
    position: relative;
    z-index: 1;
    transition: border-color 120ms ease, box-shadow 120ms ease;
}}
.dtx .dtx-query::selection {{
    background: var(--ui-accent-soft);
    color: transparent;
    -webkit-text-fill-color: transparent;
}}
.dtx .dtx-query:focus {{
    border-color: var(--ui-accent);
    box-shadow: 0 0 0 3px var(--ui-accent-soft);
}}
.dtx .dtx-error {{
    margin: 0 24px 16px 24px;
    padding: 10px 14px;
    border-radius: 8px;
    font-size: 12px;
    background: rgba(220, 38, 38, 0.10);
    border: 1px solid rgba(220, 38, 38, 0.35);
    color: #b91c1c;
    white-space: pre-wrap;
}}
.dtx.dtx-dark .dtx-error {{
    background: rgba(248, 113, 113, 0.12);
    border-color: rgba(248, 113, 113, 0.35);
    color: #fca5a5;
}}
.dtx .dtx-section-title {{
    padding: 4px 24px 10px 24px;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--ui-text-tertiary);
}}
.dtx .dtx-table-wrap {{
    overflow-x: auto;
    overflow-y: auto;
    max-height: 480px;
    border-top: 1px solid var(--ui-border);
}}
.dtx table {{
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    font-size: 13px;
}}
.dtx thead th {{
    position: sticky;
    top: 0;
    padding: 10px 16px;
    text-align: left;
    font-weight: 600;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--ui-text-secondary);
    background: var(--ui-bg-secondary);
    border-bottom: 1px solid var(--ui-border-strong);
    white-space: nowrap;
}}
.dtx tbody tr {{ background: var(--ui-bg); }}
.dtx tbody td {{
    padding: 9px 16px;
    border-bottom: 1px solid var(--ui-border);
    color: var(--ui-text);
    background: var(--ui-bg);
    white-space: nowrap;
}}
.dtx tbody tr:nth-child(even) td {{ background: var(--ui-bg-tertiary); }}
.dtx tbody tr:hover td {{ background: var(--ui-surface-2); }}
.dtx td.dtx-num {{ text-align: right; font-variant-numeric: tabular-nums; }}
.dtx td.dtx-empty {{
    text-align: center;
    color: var(--ui-text-tertiary);
    padding: 24px 16px;
}}
.dtx .dtx-running .dtx-card {{ opacity: 0.55; }}
.dtx .dtx-body {{
    display: flex;
    align-items: stretch;
    min-height: 0;
}}
.dtx .dtx-sidebar {{
    flex: 0 0 260px;
    max-width: 260px;
    min-width: 260px;
    border-right: 1px solid var(--ui-border);
    background: var(--ui-bg-secondary);
    display: flex;
    flex-direction: column;
    transition: flex-basis 180ms ease, max-width 180ms ease,
        min-width 180ms ease;
    overflow: hidden;
}}
.dtx .dtx-sidebar.dtx-sidebar-collapsed {{
    flex: 0 0 36px;
    min-width: 36px;
    max-width: 36px;
}}
.dtx .dtx-sidebar-header {{
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 10px 10px 10px 14px;
    border-bottom: 1px solid var(--ui-border);
    background: var(--ui-bg-secondary);
    min-height: 44px;
}}
.dtx .dtx-sidebar-title {{
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--ui-text-tertiary);
    margin-right: auto;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}}
.dtx .dtx-sidebar.dtx-sidebar-collapsed .dtx-sidebar-title,
.dtx .dtx-sidebar.dtx-sidebar-collapsed .dtx-sidebar-refresh,
.dtx .dtx-sidebar.dtx-sidebar-collapsed .dtx-sidebar-body {{
    display: none;
}}
.dtx .dtx-sidebar.dtx-sidebar-collapsed .dtx-sidebar-header {{
    padding: 10px 4px;
    justify-content: center;
}}
.dtx .dtx-sidebar-toggle,
.dtx .dtx-sidebar-refresh {{
    appearance: none;
    -webkit-appearance: none;
    border: 1px solid transparent;
    background: transparent;
    color: var(--ui-text-secondary);
    width: 24px;
    height: 24px;
    padding: 0;
    border-radius: 4px;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    transition: background 120ms ease, color 120ms ease;
}}
.dtx .dtx-sidebar-toggle:hover,
.dtx .dtx-sidebar-refresh:hover {{
    background: var(--ui-surface-2);
    color: var(--ui-text);
}}
.dtx .dtx-sidebar-toggle svg,
.dtx .dtx-sidebar-refresh svg {{
    width: 14px;
    height: 14px;
}}
.dtx .dtx-sidebar-refresh.dtx-spinning svg {{
    animation: dtx-spin 0.9s linear infinite;
}}
@keyframes dtx-spin {{ to {{ transform: rotate(360deg); }} }}
.dtx .dtx-sidebar-search {{
    padding: 8px 10px;
    border-bottom: 1px solid var(--ui-border);
    background: var(--ui-bg-secondary);
}}
.dtx .dtx-sidebar.dtx-sidebar-collapsed .dtx-sidebar-search {{ display: none; }}
.dtx .dtx-sidebar-search input {{
    width: 100%;
    box-sizing: border-box;
    background: var(--ui-bg);
    border: 1px solid var(--ui-border);
    border-radius: 6px;
    padding: 5px 8px;
    font-family: inherit;
    font-size: 12px;
    color: var(--ui-text);
}}
.dtx .dtx-sidebar-search input:focus {{
    outline: none;
    border-color: var(--ui-accent);
    box-shadow: 0 0 0 3px var(--ui-accent-soft);
}}
.dtx .dtx-sidebar-search input::placeholder {{ color: var(--ui-text-tertiary); }}
.dtx .dtx-sidebar-body {{
    overflow-y: auto;
    overflow-x: hidden;
    padding: 6px 4px 10px 4px;
    flex: 1 1 auto;
    font-size: 12px;
}}
.dtx .dtx-sidebar-empty {{
    padding: 16px 14px;
    font-size: 12px;
    color: var(--ui-text-tertiary);
    text-align: center;
}}
.dtx .dtx-tree-node {{
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 3px 6px;
    border-radius: 4px;
    cursor: pointer;
    user-select: none;
    color: var(--ui-text);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}}
.dtx .dtx-tree-node:hover {{ background: var(--ui-surface-2); }}
.dtx .dtx-tree-leaf.dtx-draggable {{ cursor: grab; }}
.dtx .dtx-tree-leaf.dtx-draggable:active {{ cursor: grabbing; }}
.dtx .dtx-tree-node.dtx-draggable > .dtx-tree-label {{ cursor: grab; }}
.dtx .dtx-tree-node.dtx-draggable:active > .dtx-tree-label {{ cursor: grabbing; }}
.dtx .dtx-query.dtx-drop-target {{
    border-color: var(--ui-accent);
    box-shadow: 0 0 0 3px var(--ui-accent-soft);
}}
.dtx .dtx-tree-leaf {{
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 2px 6px 2px 6px;
    border-radius: 4px;
    color: var(--ui-text-secondary);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    cursor: default;
}}
.dtx .dtx-tree-leaf:hover {{ background: var(--ui-surface-2); color: var(--ui-text); }}
.dtx .dtx-tree-caret {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 12px;
    height: 12px;
    color: var(--ui-text-tertiary);
    flex: 0 0 12px;
    transition: transform 120ms ease;
}}
.dtx .dtx-tree-node.dtx-open > .dtx-tree-caret {{ transform: rotate(90deg); }}
.dtx .dtx-tree-folder-header.dtx-open > .dtx-tree-caret,
.dtx .dtx-tree-group-header.dtx-open > .dtx-tree-caret,
.dtx .dtx-tree-leaf.dtx-open > .dtx-tree-caret {{ transform: rotate(90deg); }}
.dtx .dtx-tree-caret-spacer {{
    display: inline-flex;
    width: 12px;
    height: 12px;
    flex: 0 0 12px;
}}
.dtx .dtx-tree-caret svg {{ width: 12px; height: 12px; }}
.dtx .dtx-tree-icon {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 18px;
    height: 18px;
    flex: 0 0 18px;
    color: var(--ui-text-tertiary);
}}
.dtx .dtx-tree-icon svg {{ width: 18px; height: 18px; }}
.dtx .dtx-tree-label {{
    flex: 1 1 auto;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}}
.dtx .dtx-tree-node > .dtx-tree-label {{
    font-size: 13px;
    font-weight: 600;
}}
.dtx .dtx-tree-label.dtx-hidden {{
    color: var(--ui-text-tertiary);
    font-style: italic;
}}
.dtx.dtx-dark .dtx-tree-label.dtx-hidden {{
    color: #9ca3af;
}}
.dtx:not(.dtx-dark) .dtx-tree-leaf .dtx-tree-label:not(.dtx-hidden),
.dtx:not(.dtx-dark) .dtx-tree-node > .dtx-tree-label:not(.dtx-hidden),
.dtx:not(.dtx-dark) .dtx-tree-folder-header .dtx-tree-label:not(.dtx-hidden) {{
    color: #2b2b30;
}}
.dtx.dtx-dark .dtx-tree-leaf .dtx-tree-label:not(.dtx-hidden),
.dtx.dtx-dark .dtx-tree-node > .dtx-tree-label:not(.dtx-hidden),
.dtx.dtx-dark .dtx-tree-folder-header .dtx-tree-label:not(.dtx-hidden) {{
    color: var(--ui-text);
}}
.dtx .dtx-tree-type {{
    margin-left: 6px;
    flex: 0 0 auto;
    font-size: 10px;
    font-weight: 500;
    color: var(--ui-text-tertiary);
    text-transform: lowercase;
    font-variant-numeric: tabular-nums;
    letter-spacing: 0.02em;
    padding: 1px 5px;
    border-radius: 3px;
    background: var(--ui-surface-2);
    border: 1px solid var(--ui-border);
    white-space: nowrap;
}}
.dtx .dtx-tree-children {{ display: none; padding-left: 14px; }}
.dtx .dtx-tree-node.dtx-open + .dtx-tree-children {{ display: block; }}
.dtx .dtx-tree-subtree {{ display: none; }}
.dtx .dtx-tree-folder-header {{
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 2px 6px;
    border-radius: 4px;
    cursor: pointer;
    user-select: none;
    color: var(--ui-text-secondary);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}}
.dtx .dtx-tree-folder-header:hover {{ background: var(--ui-surface-2); color: var(--ui-text); }}
.dtx .dtx-tree-folder-header .dtx-tree-icon {{ color: var(--ui-text-tertiary); }}
.dtx .dtx-tree-folder-header .dtx-tree-label {{ font-size: 12px; }}
.dtx .dtx-tree-level {{ color: var(--ui-text-tertiary); cursor: default; }}
.dtx .dtx-tree-level:hover {{ background: var(--ui-surface-2); color: var(--ui-text-secondary); }}
.dtx .dtx-tree-group {{
    margin-top: 2px;
}}
.dtx .dtx-tree-group-header {{
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 2px 6px;
    border-radius: 4px;
    cursor: pointer;
    user-select: none;
    color: var(--ui-text-tertiary);
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.03em;
}}
.dtx .dtx-tree-group-header:hover {{ background: var(--ui-surface-2); color: var(--ui-text-secondary); }}
.dtx .dtx-tree-group-count {{
    margin-left: auto;
    font-weight: 500;
    color: var(--ui-text-tertiary);
    font-variant-numeric: tabular-nums;
    font-size: 10px;
    text-transform: none;
    letter-spacing: 0;
}}
.dtx .dtx-main {{
    flex: 1 1 auto;
    min-width: 0;
    display: flex;
    flex-direction: column;
    padding-left: 12px;
}}
.dtx .dtx-builder {{
    flex: 0 0 260px;
    max-width: 260px;
    min-width: 260px;
    border-right: 1px solid var(--ui-border);
    background: var(--ui-bg-secondary);
    display: flex;
    flex-direction: column;
    overflow: hidden;
}}
.dtx .dtx-builder.dtx-builder-hidden {{
    display: none;
}}
.dtx .dtx-builder.dtx-builder-collapsed {{
    flex: 0 0 38px;
    max-width: 38px;
    min-width: 38px;
}}
.dtx .dtx-builder.dtx-builder-collapsed .dtx-builder-title,
.dtx .dtx-builder.dtx-builder-collapsed .dtx-builder-content {{
    display: none;
}}
.dtx .dtx-builder.dtx-builder-collapsed .dtx-builder-header {{
    justify-content: center;
    padding: 10px 6px;
}}
.dtx .dtx-builder.dtx-builder-collapsed .dtx-builder-toggle {{
    display: none;
}}
.dtx .dtx-builder.dtx-builder-collapsed .dtx-builder-toggle.dtx-builder-collapse {{
    display: inline-flex;
}}
.dtx .dtx-builder-header {{
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 10px 10px 10px 14px;
    border-bottom: 1px solid var(--ui-border);
    min-height: 44px;
}}
.dtx .dtx-builder-title {{
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--ui-text-tertiary);
    margin-right: auto;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}}
.dtx .dtx-builder-toggle {{
    flex: 0 0 auto;
    width: 26px;
    height: 26px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    padding: 0;
    border: 1px solid var(--ui-border);
    border-radius: 6px;
    background: var(--ui-bg);
    color: var(--ui-text-secondary);
    cursor: pointer;
}}
.dtx .dtx-builder-toggle:hover {{
    background: var(--ui-bg-hover);
    color: var(--ui-text);
}}
.dtx .dtx-builder-toggle svg {{ width: 15px; height: 15px; }}
.dtx .dtx-builder-content {{
    display: flex;
    flex-direction: column;
    min-height: 0;
    flex: 1 1 auto;
    padding: 10px;
    gap: 12px;
    overflow-y: auto;
}}
.dtx .dtx-builder-section {{
    display: flex;
    flex-direction: column;
    gap: 6px;
}}
.dtx .dtx-builder-section-label {{
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--ui-text-tertiary);
}}
.dtx .dtx-builder-zone {{
    min-height: 70px;
    border: 1px dashed var(--ui-border);
    border-radius: 8px;
    padding: 8px;
    display: flex;
    flex-direction: column;
    gap: 6px;
    background: var(--ui-bg);
    transition: border-color 120ms ease, background 120ms ease;
}}
.dtx .dtx-builder-zone.dtx-drop-over {{
    border-color: var(--ui-accent);
    background: var(--ui-bg-hover);
}}
.dtx .dtx-builder-placeholder {{
    font-size: 11px;
    color: var(--ui-text-tertiary);
    text-align: center;
    padding: 12px 4px;
    pointer-events: none;
}}
.dtx .dtx-builder-chip {{
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 5px 6px;
    border: 1px solid var(--ui-border);
    border-radius: 6px;
    background: var(--ui-bg-secondary);
    cursor: grab;
    flex-wrap: wrap;
    box-sizing: border-box;
    max-width: 100%;
}}
.dtx .dtx-builder-chip.dtx-chip-dragging {{ opacity: 0.5; }}
.dtx .dtx-chip-icon {{
    flex: 0 0 auto;
    display: inline-flex;
    align-items: center;
    color: var(--ui-text-secondary);
}}
.dtx .dtx-chip-icon svg {{ width: 14px; height: 14px; }}
.dtx .dtx-chip-label {{
    flex: 1 1 auto;
    min-width: 0;
    font-size: 11px;
    color: var(--ui-text);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}}
.dtx .dtx-chip-remove {{
    flex: 0 0 auto;
    width: 18px;
    height: 18px;
    border: none;
    background: transparent;
    color: var(--ui-text-tertiary);
    font-size: 15px;
    line-height: 1;
    cursor: pointer;
    border-radius: 4px;
}}
.dtx .dtx-chip-remove:hover {{
    background: var(--ui-bg-hover);
    color: var(--ui-text);
}}
.dtx .dtx-chip-op {{
    flex: 1 1 100%;
    font-size: 11px;
    padding: 3px 4px;
    border: 1px solid var(--ui-border);
    border-radius: 5px;
    background: var(--ui-bg);
    color: var(--ui-text);
}}
.dtx .dtx-chip-values {{
    flex: 1 1 100%;
    display: flex;
    align-items: center;
    gap: 4px;
    min-width: 0;
}}
.dtx .dtx-chip-value {{
    flex: 1 1 0;
    width: 0;
    min-width: 0;
    box-sizing: border-box;
    font-size: 11px;
    padding: 3px 5px;
    border: 1px solid var(--ui-border);
    border-radius: 5px;
    background: var(--ui-bg);
    color: var(--ui-text);
}}
.dtx .dtx-chip-sep {{
    flex: 0 0 auto;
    font-size: 10px;
    color: var(--ui-text-tertiary);
}}
.dtx .dtx-builder-footer {{
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 8px;
    margin-top: auto;
    padding-top: 6px;
}}
.dtx .dtx-builder-clear {{
    padding: 6px 12px;
    border: 1px solid var(--ui-border);
    border-radius: 6px;
    background: var(--ui-bg);
    color: var(--ui-text-secondary);
    font-size: 12px;
    cursor: pointer;
}}
.dtx .dtx-builder-clear:hover {{
    background: var(--ui-bg-hover);
    color: var(--ui-text);
}}
.dtx .dtx-build-btn {{
    padding: 6px 16px;
    border: 1px solid var(--ui-accent);
    border-radius: 6px;
    background: var(--ui-accent);
    color: #fff;
    font-size: 12px;
    font-weight: 600;
    cursor: pointer;
}}
.dtx .dtx-build-btn:hover {{ filter: brightness(1.05); }}
.dtx .dtx-build-btn:disabled {{
    opacity: 0.5;
    cursor: not-allowed;
    filter: none;
}}
"""
    )

    sun_icon = _UI_ICONS["sun"].replace("`", "\\`")
    moon_icon = _UI_ICONS["moon"].replace("`", "\\`")
    table_icon = _UI_ICONS["table"].replace("`", "\\`")
    calc_group_icon = _UI_ICONS["calculation_group"].replace("`", "\\`")
    calc_item_icon = _UI_ICONS["calculation_item"].replace("`", "\\`")
    column_icon = _UI_ICONS["column"].replace("`", "\\`")
    measure_icon = _UI_ICONS["measure"].replace("`", "\\`")
    hierarchy_icon = _UI_ICONS["hierarchy"].replace("`", "\\`")
    caret_icon = _UI_ICONS["caret_right"].replace("`", "\\`")
    folder_icon = _UI_ICONS["folder"].replace("`", "\\`")
    level_icon = _UI_ICONS["level"].replace("`", "\\`")

    widget_js = r"""
function escapeHtml(s) {
    return String(s == null ? "" : s)
        .replace(/&/g, "&amp;").replace(/</g, "&lt;")
        .replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

function render({ model, el }) {
    const SUN_SVG = `__DTX_SUN__`;
    const MOON_SVG = `__DTX_MOON__`;
    const PLAY_SVG = '<svg viewBox="0 0 16 16" fill="currentColor" aria-hidden="true"><path d="M4 2.5v11l9-5.5z"/></svg>';
    const STOP_SVG = '<svg viewBox="0 0 16 16" fill="currentColor" aria-hidden="true"><rect x="4" y="4" width="8" height="8" rx="1.2"/></svg>';
    const TABLE_SVG = `__DTX_TABLE__`;
    const CALC_GROUP_SVG = `__DTX_CALC_GROUP__`;
    const CALC_ITEM_SVG = `__DTX_CALC_ITEM__`;
    const COLUMN_SVG = `__DTX_COLUMN__`;
    const MEASURE_SVG = `__DTX_MEASURE__`;
    const HIERARCHY_SVG = `__DTX_HIERARCHY__`;
    const CARET_SVG = `__DTX_CARET__`;
    const FOLDER_SVG = `__DTX_FOLDER__`;
    const LEVEL_SVG = `__DTX_LEVEL__`;
    const REFRESH_SVG = '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor"'
        + ' stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
        + '<path d="M13.5 8a5.5 5.5 0 1 1-1.61-3.89"/><path d="M13.5 2.5v3h-3"/></svg>';
    const SWAP_SVG = '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor"'
        + ' stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
        + '<path d="M2.5 5.5h9"/><path d="M9 3l2.5 2.5L9 8"/>'
        + '<path d="M13.5 10.5h-9"/><path d="M7 8l-2.5 2.5L7 13"/></svg>';
    const PANEL_COLLAPSE_SVG = '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor"'
        + ' stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
        + '<rect x="2" y="3" width="12" height="10" rx="1.5"/><path d="M6.5 3v10"/>'
        + '<path d="M10.5 6.5L8.5 8l2 1.5"/></svg>';
    const PANEL_EXPAND_SVG = '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor"'
        + ' stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
        + '<rect x="2" y="3" width="12" height="10" rx="1.5"/><path d="M6.5 3v10"/>'
        + '<path d="M8.5 6.5L10.5 8l-2 1.5"/></svg>';
    const BUILDER_SVG = '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor"'
        + ' stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
        + '<rect x="2" y="2.5" width="12" height="11" rx="1.5"/>'
        + '<path d="M2 6h12"/><path d="M4.5 9h4"/><path d="M4.5 11h2"/>'
        + '<path d="M11.5 9.2v3.2M9.9 10.8h3.2"/></svg>';
    const CLOSE_SVG = '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor"'
        + ' stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
        + '<path d="M4 4l8 8M12 4l-8 8"/></svg>';

    const root = document.createElement("div");
    root.className = "dtx";
    function applyTheme() {
        root.classList.toggle("dtx-dark", model.get("dark_mode") === true);
        renderThemeBtn();
    }
    el.appendChild(root);

    const container = document.createElement("div");
    container.className = "dtx-container";
    root.appendChild(container);

    // ---------- Header (title + subtitle + theme toggle) ----------
    const headerWrap = document.createElement("div");
    headerWrap.className = "dtx-header";
    container.appendChild(headerWrap);

    const header = document.createElement("div");
    header.className = "sl-header";
    headerWrap.appendChild(header);

    const titleWrap = document.createElement("div");
    titleWrap.className = "sl-titlewrap";
    header.appendChild(titleWrap);

    const title = document.createElement("div");
    title.className = "sl-title";
    title.textContent = "DAX Query Performance";
    titleWrap.appendChild(title);

    const subtitle = document.createElement("div");
    subtitle.className = "sl-subtitle";
    titleWrap.appendChild(subtitle);

    const changeModelBtn = document.createElement("button");
    changeModelBtn.type = "button";
    changeModelBtn.className = "dtx-change-btn";
    changeModelBtn.innerHTML = SWAP_SVG;
    changeModelBtn.title = "Change model";
    changeModelBtn.setAttribute("aria-label", "Change model");
    titleWrap.appendChild(changeModelBtn);
    changeModelBtn.addEventListener("click", () => {
        pickerOpen = true;
        // (Re)load the workspace list so the picker is populated even when a
        // dataset was supplied directly to test().
        model.set("error_message", "");
        model.set("load_workspaces_trigger",
            (model.get("load_workspaces_trigger") || 0) + 1);
        model.save_changes();
        renderPicker();
    });
    function renderSubtitle() {
        const ds = model.get("dataset_name") || "";
        const ws = model.get("workspace_name") || "";
        changeModelBtn.style.display =
            model.get("dataset_chosen") === true ? "" : "none";
        if (model.get("dataset_chosen") !== true) {
            subtitle.textContent = "No semantic model selected";
            return;
        }
        if (!ds && !ws) { subtitle.textContent = ""; return; }
        subtitle.innerHTML =
            (ds ? `<b>${escapeHtml(ds)}</b>` : "") +
            (ds && ws ? `<span class="sl-sep">·</span>` : "") +
            (ws ? escapeHtml(ws) : "");
    }

    const themeBtn = document.createElement("button");
    themeBtn.type = "button";
    themeBtn.className = "sl-theme-btn";
    function renderThemeBtn() {
        const isDark = model.get("dark_mode") === true;
        themeBtn.innerHTML = isDark ? SUN_SVG : MOON_SVG;
        const label = isDark ? "Switch to light mode" : "Switch to dark mode";
        themeBtn.title = label;
        themeBtn.setAttribute("aria-label", label);
    }
    themeBtn.addEventListener("click", () => {
        model.set("dark_mode", !(model.get("dark_mode") === true));
        model.save_changes();
    });

    const builderShowBtn = document.createElement("button");
    builderShowBtn.type = "button";
    builderShowBtn.className = "dtx-builder-show-btn";
    builderShowBtn.innerHTML = BUILDER_SVG;
    builderShowBtn.title = "Show query builder";
    builderShowBtn.setAttribute("aria-label", "Show query builder");
    builderShowBtn.addEventListener("click", () => {
        builderVisible = !builderVisible;
        renderBuilderChrome();
    });
    header.appendChild(builderShowBtn);
    header.appendChild(themeBtn);

    // ---------- Body: sidebar + main ----------
    const body = document.createElement("div");
    body.className = "dtx-body";
    container.appendChild(body);

    const sidebar = document.createElement("div");
    sidebar.className = "dtx-sidebar";
    body.appendChild(sidebar);

    const sidebarHeader = document.createElement("div");
    sidebarHeader.className = "dtx-sidebar-header";
    sidebar.appendChild(sidebarHeader);

    const sidebarTitle = document.createElement("div");
    sidebarTitle.className = "dtx-sidebar-title";
    sidebarTitle.textContent = "Model";
    sidebarHeader.appendChild(sidebarTitle);

    const refreshBtn = document.createElement("button");
    refreshBtn.type = "button";
    refreshBtn.className = "dtx-sidebar-refresh";
    refreshBtn.innerHTML = REFRESH_SVG;
    refreshBtn.title = "Refresh metadata (re-read tables, columns, measures, hierarchies). Does not refresh the model.";
    refreshBtn.setAttribute("aria-label", "Refresh metadata");
    refreshBtn.addEventListener("click", () => {
        if (model.get("metadata_loading") === true) return;
        model.set("refresh_metadata_trigger", (model.get("refresh_metadata_trigger") || 0) + 1);
        model.save_changes();
    });
    sidebarHeader.appendChild(refreshBtn);

    const sidebarToggle = document.createElement("button");
    sidebarToggle.type = "button";
    sidebarToggle.className = "dtx-sidebar-toggle";
    sidebarToggle.addEventListener("click", () => {
        model.set("sidebar_collapsed", !(model.get("sidebar_collapsed") === true));
        model.save_changes();
    });
    sidebarHeader.appendChild(sidebarToggle);

    const sidebarSearch = document.createElement("div");
    sidebarSearch.className = "dtx-sidebar-search";
    const searchInput = document.createElement("input");
    searchInput.type = "text";
    searchInput.placeholder = "Search tables, columns, measures…";
    searchInput.setAttribute("aria-label", "Search the model");
    searchInput.spellcheck = false;
    sidebarSearch.appendChild(searchInput);
    sidebar.appendChild(sidebarSearch);

    let treeFilter = "";
    let treeExpand = false;
    searchInput.addEventListener("input", () => {
        treeFilter = searchInput.value || "";
        renderTree();
    });

    const sidebarBody = document.createElement("div");
    sidebarBody.className = "dtx-sidebar-body";
    sidebar.appendChild(sidebarBody);

    function renderSidebarChrome() {
        const collapsed = model.get("sidebar_collapsed") === true;
        sidebar.classList.toggle("dtx-sidebar-collapsed", collapsed);
        sidebarToggle.innerHTML = collapsed ? PANEL_EXPAND_SVG : PANEL_COLLAPSE_SVG;
        const label = collapsed ? "Show model panel" : "Hide model panel";
        sidebarToggle.title = label;
        sidebarToggle.setAttribute("aria-label", label);
        refreshBtn.classList.toggle("dtx-spinning", model.get("metadata_loading") === true);
    }

    // Shared drag payload for dropping model objects into the editor.
    let dragPayload = null;
    // Richer metadata for the dragged model object (used by the query
    // builder). Null when the drag is not a single column/measure.
    let dragFieldMeta = null;
    // Set when reordering a chip inside a query-builder pane.
    let builderReorder = null;

    function daxTableRef(name) {
        return "'" + String(name).replace(/'/g, "''") + "'";
    }
    function daxMeasureRef(name) {
        return "[" + String(name).replace(/\]/g, "]]") + "]";
    }
    function daxColumnRef(tableName, objName) {
        return daxTableRef(tableName)
            + "[" + String(objName).replace(/\]/g, "]]") + "]";
    }

    function makeDraggable(elem, dragText, meta) {
        elem.setAttribute("draggable", "true");
        elem.classList.add("dtx-draggable");
        elem.addEventListener("dragstart", (e) => {
            dragPayload = dragText;
            dragFieldMeta = meta || null;
            e.dataTransfer.setData("text/plain", dragText);
            if (meta) {
                e.dataTransfer.setData(
                    "application/x-dtx-field", JSON.stringify(meta));
            }
            e.dataTransfer.effectAllowed = "copy";
        });
        elem.addEventListener("dragend", () => {
            dragPayload = null;
            dragFieldMeta = null;
        });
    }

    function makeLeaf(iconSvg, name, hidden, dataType, dragText, description, pad, meta) {
        const leaf = document.createElement("div");
        leaf.className = "dtx-tree-leaf";
        leaf.style.paddingLeft = (pad == null ? 30 : pad) + "px";
        const tip = description ? description : name;
        const typeHtml = dataType
            ? `<span class="dtx-tree-type" title="${escapeHtml(dataType)}">${escapeHtml(dataType)}</span>`
            : "";
        leaf.innerHTML = `<span class="dtx-tree-icon">${iconSvg}</span>`
            + `<span class="dtx-tree-label${hidden ? " dtx-hidden" : ""}"`
            + ` title="${escapeHtml(tip)}">${escapeHtml(name)}</span>`
            + typeHtml;
        if (dragText) makeDraggable(leaf, dragText, meta);
        return leaf;
    }

    // Group items by their (possibly nested) display folder. Folder paths are
    // split on "\\"; items without a display folder live at the group root.
    function buildFolderTree(items) {
        const root = { folders: new Map(), items: [] };
        for (const it of items) {
            const df = String(it.display_folder || "").trim();
            if (!df) { root.items.push(it); continue; }
            const parts = df.split("\\").map(p => p.trim()).filter(p => p.length);
            let node = root;
            for (const part of parts) {
                if (!node.folders.has(part)) {
                    node.folders.set(part, { folders: new Map(), items: [] });
                }
                node = node.folders.get(part);
            }
            node.items.push(it);
        }
        return root;
    }

    function makeFolder(label, pad) {
        const wrap = document.createElement("div");
        wrap.className = "dtx-tree-folder";
        const header = document.createElement("div");
        header.className = "dtx-tree-folder-header";
        header.style.paddingLeft = pad + "px";
        header.innerHTML = `<span class="dtx-tree-caret">${CARET_SVG}</span>`
            + `<span class="dtx-tree-icon">${FOLDER_SVG}</span>`
            + `<span class="dtx-tree-label" title="${escapeHtml(label)}">${escapeHtml(label)}</span>`;
        const children = document.createElement("div");
        children.className = "dtx-tree-subtree";
        children.style.display = treeExpand ? "block" : "none";
        header.classList.toggle("dtx-open", treeExpand);
        header.addEventListener("click", () => {
            const open = !header.classList.contains("dtx-open");
            header.classList.toggle("dtx-open", open);
            children.style.display = open ? "block" : "none";
        });
        wrap.appendChild(header);
        wrap.appendChild(children);
        return { wrap, children };
    }

    function renderFolderTree(parentEl, node, build, depth) {
        const pad = 30 + depth * 14;
        const folderNames = Array.from(node.folders.keys()).sort(
            (a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));
        for (const fname of folderNames) {
            const folder = makeFolder(fname, pad);
            parentEl.appendChild(folder.wrap);
            renderFolderTree(folder.children, node.folders.get(fname), build, depth + 1);
        }
        for (const it of node.items) {
            parentEl.appendChild(build(it, pad));
        }
    }

    function makeGroup(label, items, iconSvg, dragFn, leafBuilder, metaFn) {
        if (!items || !items.length) return null;
        const wrap = document.createElement("div");
        wrap.className = "dtx-tree-group";
        const header = document.createElement("div");
        header.className = "dtx-tree-group-header";
        header.style.paddingLeft = "18px";
        header.innerHTML = `<span class="dtx-tree-caret">${CARET_SVG}</span><span>${escapeHtml(label)}</span><span class="dtx-tree-group-count">${items.length}</span>`;
        const children = document.createElement("div");
        children.className = "dtx-tree-children";
        const build = leafBuilder || ((it, pad) => makeLeaf(
            iconSvg, it.name, !!it.hidden, it.data_type,
            dragFn ? dragFn(it) : null, it.description, pad,
            metaFn ? metaFn(it) : null));
        renderFolderTree(children, buildFolderTree(items), build, 0);
        header.addEventListener("click", () => {
            const open = !header.classList.contains("dtx-open");
            header.classList.toggle("dtx-open", open);
            children.style.display = open ? "block" : "none";
        });
        children.style.display = treeExpand ? "block" : "none";
        header.classList.toggle("dtx-open", treeExpand);
        wrap.appendChild(header);
        wrap.appendChild(children);
        return wrap;
    }

    // Leaf builder for hierarchies: the hierarchy is draggable and (when it
    // has levels) expands to show its levels. Levels are NOT draggable.
    function makeHierarchyLeaf(tableName) {
        return (it, pad) => {
            const frag = document.createDocumentFragment();
            const hasLevels = it.levels && it.levels.length;
            const node = document.createElement("div");
            node.className = "dtx-tree-leaf";
            node.style.paddingLeft = pad + "px";
            const tip = it.description ? it.description : it.name;
            node.innerHTML = (hasLevels
                    ? `<span class="dtx-tree-caret">${CARET_SVG}</span>`
                    : `<span class="dtx-tree-caret-spacer"></span>`)
                + `<span class="dtx-tree-icon">${HIERARCHY_SVG}</span>`
                + `<span class="dtx-tree-label${it.hidden ? " dtx-hidden" : ""}"`
                + ` title="${escapeHtml(tip)}">${escapeHtml(it.name)}</span>`;
            makeDraggable(node, daxColumnRef(tableName, it.name));
            frag.appendChild(node);
            if (hasLevels) {
                const lchildren = document.createElement("div");
                lchildren.className = "dtx-tree-subtree";
                lchildren.style.display = treeExpand ? "block" : "none";
                node.classList.toggle("dtx-open", treeExpand);
                for (const lvl of it.levels) {
                    const lleaf = document.createElement("div");
                    lleaf.className = "dtx-tree-leaf dtx-tree-level";
                    lleaf.style.paddingLeft = (pad + 18) + "px";
                    const ltip = lvl.description ? lvl.description : lvl.name;
                    lleaf.innerHTML = `<span class="dtx-tree-icon">${LEVEL_SVG}</span>`
                        + `<span class="dtx-tree-label"`
                        + ` title="${escapeHtml(ltip)}">${escapeHtml(lvl.name)}</span>`;
                    lchildren.appendChild(lleaf);
                }
                node.addEventListener("click", () => {
                    const open = !node.classList.contains("dtx-open");
                    node.classList.toggle("dtx-open", open);
                    lchildren.style.display = open ? "block" : "none";
                });
                frag.appendChild(lchildren);
            }
            return frag;
        };
    }

    // Filter the model tree by a case-insensitive substring. A table is kept
    // in full when its own name matches; otherwise only its matching children
    // (columns, measures, hierarchies/levels, calculation items) are kept.
    function matchStr(s, q) {
        return String(s == null ? "" : s).toLowerCase().indexOf(q) !== -1;
    }

    function filterTree(tree, q) {
        if (!q) return tree;
        const out = [];
        for (const tbl of tree) {
            if (matchStr(tbl.name, q)) { out.push(tbl); continue; }
            const cols = (tbl.columns || []).filter(it => matchStr(it.name, q));
            const meas = (tbl.measures || []).filter(it => matchStr(it.name, q));
            const hiers = (tbl.hierarchies || []).map(h => {
                if (matchStr(h.name, q)) return h;
                const lv = (h.levels || []).filter(l => matchStr(l.name, q));
                return lv.length ? Object.assign({}, h, { levels: lv }) : null;
            }).filter(h => h);
            const cis = (tbl.calculation_items || []).filter(
                it => matchStr(it.name, q));
            if (cols.length || meas.length || hiers.length || cis.length) {
                out.push(Object.assign({}, tbl, {
                    columns: cols,
                    measures: meas,
                    hierarchies: hiers,
                    calculation_items: cis,
                }));
            }
        }
        return out;
    }

    function renderTree() {
        sidebarBody.innerHTML = "";
        const rawTree = model.get("model_tree") || [];
        const q = String(treeFilter || "").trim().toLowerCase();
        treeExpand = q.length > 0;
        const tree = filterTree(rawTree, q);
        if (!rawTree.length) {
            const empty = document.createElement("div");
            empty.className = "dtx-sidebar-empty";
            empty.textContent = model.get("metadata_loading") === true
                ? "Loading model metadata…"
                : "No metadata available.";
            sidebarBody.appendChild(empty);
            return;
        }
        if (!tree.length) {
            const empty = document.createElement("div");
            empty.className = "dtx-sidebar-empty";
            empty.textContent = 'No objects match "' + treeFilter + '".';
            sidebarBody.appendChild(empty);
            return;
        }
        for (const tbl of tree) {
            const node = document.createElement("div");
            node.className = "dtx-tree-node";
            const tblIcon = tbl.calculation_group ? CALC_GROUP_SVG : TABLE_SVG;
            node.innerHTML = `<span class="dtx-tree-caret">${CARET_SVG}</span>`
                + `<span class="dtx-tree-icon">${tblIcon}</span>`
                + `<span class="dtx-tree-label${tbl.hidden ? " dtx-hidden" : ""}"`
                + ` title="${escapeHtml(tbl.description ? tbl.description : tbl.name)}">${escapeHtml(tbl.name)}</span>`;
            makeDraggable(node, daxTableRef(tbl.name));
            const children = document.createElement("div");
            children.className = "dtx-tree-children";
            const colGrp = makeGroup("Columns", tbl.columns, COLUMN_SVG,
                (it) => daxColumnRef(tbl.name, it.name), null,
                (it) => ({kind: "column", table: tbl.name, name: it.name,
                    data_type: it.data_type,
                    ref: daxColumnRef(tbl.name, it.name)}));
            const meaGrp = makeGroup("Measures", tbl.measures, MEASURE_SVG,
                (it) => daxMeasureRef(it.name), null,
                (it) => ({kind: "measure", table: tbl.name, name: it.name,
                    data_type: it.data_type,
                    ref: daxMeasureRef(it.name)}));
            const hieGrp = makeGroup("Hierarchies", tbl.hierarchies, HIERARCHY_SVG,
                null, makeHierarchyLeaf(tbl.name));
            if (colGrp) children.appendChild(colGrp);
            if (meaGrp) children.appendChild(meaGrp);
            if (hieGrp) children.appendChild(hieGrp);
            const ciGrp = makeGroup("Calculation items", tbl.calculation_items, CALC_ITEM_SVG,
                (it) => daxColumnRef(tbl.name, it.name));
            if (ciGrp) children.appendChild(ciGrp);
            node.addEventListener("click", () => {
                const open = !node.classList.contains("dtx-open");
                node.classList.toggle("dtx-open", open);
                children.style.display = open ? "block" : "none";
            });
            children.style.display = treeExpand ? "block" : "none";
            node.classList.toggle("dtx-open", treeExpand);
            sidebarBody.appendChild(node);
            sidebarBody.appendChild(children);
        }
    }

    const main = document.createElement("div");
    main.className = "dtx-main";
    body.appendChild(main);

    // ---------- Query Builder pane (between sidebar and main) ----------
    // Hidden by default; revealed via the header "Query Builder" button.
    let builderVisible = false;
    let builderCollapsed = false;
    let builderFields = [];
    let builderFilters = [];
    let qbSeq = 0;

    const QB_OPS = {
        text: [["eq", "equals"], ["ne", "does not equal"],
            ["contains", "contains"], ["startswith", "starts with"],
            ["in", "in"], ["notin", "not in"],
            ["blank", "is blank"], ["notblank", "is not blank"]],
        numeric: [["eq", "="], ["ne", "\u2260"], ["gt", ">"], ["ge", "\u2265"],
            ["lt", "<"], ["le", "\u2264"], ["between", "between"],
            ["in", "in"], ["notin", "not in"],
            ["blank", "is blank"], ["notblank", "is not blank"]],
        datetime: [["eq", "on"], ["ne", "not on"], ["gt", "after"],
            ["ge", "on or after"], ["lt", "before"], ["le", "on or before"],
            ["between", "between"], ["in", "in"], ["notin", "not in"],
            ["blank", "is blank"], ["notblank", "is not blank"]],
        boolean: [["istrue", "is TRUE"], ["isfalse", "is FALSE"]],
        measure: [["eq", "="], ["ne", "\u2260"], ["gt", ">"], ["ge", "\u2265"],
            ["lt", "<"], ["le", "\u2264"], ["between", "between"],
            ["in", "in"], ["notin", "not in"]],
    };

    function qbClassify(field) {
        if (field.kind === "measure") return "measure";
        const dt = String(field.data_type || "").toLowerCase();
        if (dt === "int64" || dt === "double" || dt === "decimal"
            || dt === "currency" || dt === "int" || dt === "integer") {
            return "numeric";
        }
        if (dt === "datetime" || dt === "date" || dt === "time") {
            return "datetime";
        }
        if (dt === "boolean" || dt === "bool") return "boolean";
        return "text";
    }

    function qbIcon(kind) {
        return kind === "measure" ? MEASURE_SVG : COLUMN_SVG;
    }

    function qbDisplayName(f) {
        return (f.kind === "column" && f.table)
            ? f.table + "[" + f.name + "]"
            : "[" + f.name + "]";
    }

    const builderPane = document.createElement("div");
    builderPane.className = "dtx-builder";
    body.insertBefore(builderPane, main);

    const builderHeader = document.createElement("div");
    builderHeader.className = "dtx-builder-header";
    const builderTitle = document.createElement("div");
    builderTitle.className = "dtx-builder-title";
    builderTitle.textContent = "Query Builder";
    const builderCollapseBtn = document.createElement("button");
    builderCollapseBtn.type = "button";
    builderCollapseBtn.className = "dtx-builder-toggle dtx-builder-collapse";
    builderCollapseBtn.addEventListener("click", () => {
        builderCollapsed = !builderCollapsed;
        renderBuilderChrome();
    });
    const builderToggle = document.createElement("button");
    builderToggle.type = "button";
    builderToggle.className = "dtx-builder-toggle";
    builderToggle.innerHTML = CLOSE_SVG;
    builderToggle.title = "Hide query builder";
    builderToggle.setAttribute("aria-label", "Hide query builder");
    builderToggle.addEventListener("click", () => {
        builderVisible = false;
        builderCollapsed = false;
        renderBuilderChrome();
    });
    builderHeader.appendChild(builderTitle);
    builderHeader.appendChild(builderCollapseBtn);
    builderHeader.appendChild(builderToggle);
    builderPane.appendChild(builderHeader);

    const builderContent = document.createElement("div");
    builderContent.className = "dtx-builder-content";
    builderPane.appendChild(builderContent);

    const fieldsSection = document.createElement("div");
    fieldsSection.className = "dtx-builder-section";
    const fieldsLabel = document.createElement("div");
    fieldsLabel.className = "dtx-builder-section-label";
    fieldsLabel.textContent = "Columns & Measures";
    const fieldsZone = document.createElement("div");
    fieldsZone.className = "dtx-builder-zone dtx-builder-fields";
    fieldsSection.appendChild(fieldsLabel);
    fieldsSection.appendChild(fieldsZone);
    builderContent.appendChild(fieldsSection);

    const filtersSection = document.createElement("div");
    filtersSection.className = "dtx-builder-section";
    const filtersLabel = document.createElement("div");
    filtersLabel.className = "dtx-builder-section-label";
    filtersLabel.textContent = "Filters";
    const filtersZone = document.createElement("div");
    filtersZone.className = "dtx-builder-zone dtx-builder-filters";
    filtersSection.appendChild(filtersLabel);
    filtersSection.appendChild(filtersZone);
    builderContent.appendChild(filtersSection);

    const builderFooter = document.createElement("div");
    builderFooter.className = "dtx-builder-footer";
    const clearBtn = document.createElement("button");
    clearBtn.type = "button";
    clearBtn.className = "dtx-builder-clear";
    clearBtn.textContent = "Clear";
    clearBtn.addEventListener("click", () => {
        builderFields = [];
        builderFilters = [];
        renderBuilderZones();
    });
    const buildBtn = document.createElement("button");
    buildBtn.type = "button";
    buildBtn.className = "dtx-build-btn";
    buildBtn.textContent = "Build";
    buildBtn.addEventListener("click", onBuildClick);
    builderFooter.appendChild(clearBtn);
    builderFooter.appendChild(buildBtn);
    builderContent.appendChild(builderFooter);

    function zoneIndexFromEvent(zone, e) {
        const chips = Array.from(zone.querySelectorAll(".dtx-builder-chip"));
        for (let i = 0; i < chips.length; i++) {
            const r = chips[i].getBoundingClientRect();
            if (e.clientY < r.top + r.height / 2) return i;
        }
        return chips.length;
    }

    function setupZone(zone, isFilter) {
        zone.addEventListener("dragover", (e) => {
            e.preventDefault();
            const list = isFilter ? builderFilters : builderFields;
            const isDupe = !builderReorder && dragFieldMeta
                && list.some(x => x.kind === dragFieldMeta.kind
                    && x.ref === dragFieldMeta.ref);
            if (isDupe) {
                e.dataTransfer.dropEffect = "none";
                return;
            }
            e.dataTransfer.dropEffect = builderReorder ? "move" : "copy";
            zone.classList.add("dtx-drop-over");
        });
        zone.addEventListener("dragleave", (e) => {
            if (!zone.contains(e.relatedTarget)) {
                zone.classList.remove("dtx-drop-over");
            }
        });
        zone.addEventListener("drop", (e) => {
            e.preventDefault();
            zone.classList.remove("dtx-drop-over");
            const list = isFilter ? builderFilters : builderFields;
            const zoneName = isFilter ? "filters" : "fields";
            const idx = zoneIndexFromEvent(zone, e);
            if (builderReorder) {
                if (builderReorder.zone !== zoneName) return;
                const from = list.findIndex(x => x.id === builderReorder.id);
                if (from === -1) return;
                const moved = list.splice(from, 1)[0];
                let insert = idx;
                if (from < idx) insert = idx - 1;
                list.splice(insert, 0, moved);
                renderBuilderZones();
                return;
            }
            let meta = dragFieldMeta;
            if (!meta) {
                const raw = e.dataTransfer.getData("application/x-dtx-field");
                if (raw) {
                    try { meta = JSON.parse(raw); } catch (err) { meta = null; }
                }
            }
            if (!meta || !meta.name) return;
            const dupe = list.some(x =>
                x.kind === meta.kind && x.ref === meta.ref);
            if (dupe) return;
            const item = {
                id: "qb" + (++qbSeq),
                kind: meta.kind,
                table: meta.table || "",
                name: meta.name,
                data_type: meta.data_type || "",
                ref: meta.ref,
            };
            if (isFilter) {
                const tc = qbClassify(item);
                item.typeClass = tc;
                item.op = QB_OPS[tc][0][0];
                item.value = "";
                item.value2 = "";
            }
            list.splice(idx, 0, item);
            renderBuilderZones();
        });
    }
    setupZone(fieldsZone, false);
    setupZone(filtersZone, true);

    function attachChipReorder(chip, f, zoneName) {
        chip.setAttribute("draggable", "true");
        chip.addEventListener("dragstart", (e) => {
            builderReorder = {id: f.id, zone: zoneName};
            e.dataTransfer.effectAllowed = "move";
            e.dataTransfer.setData("text/plain", "");
            e.stopPropagation();
            chip.classList.add("dtx-chip-dragging");
        });
        chip.addEventListener("dragend", () => {
            builderReorder = null;
            chip.classList.remove("dtx-chip-dragging");
        });
    }

    function makeChipRemove(onClick) {
        const rm = document.createElement("button");
        rm.type = "button";
        rm.className = "dtx-chip-remove";
        rm.innerHTML = "&times;";
        rm.title = "Remove";
        rm.addEventListener("click", onClick);
        return rm;
    }

    function makeFieldChip(f) {
        const chip = document.createElement("div");
        chip.className = "dtx-builder-chip";
        const ic = document.createElement("span");
        ic.className = "dtx-chip-icon";
        ic.innerHTML = qbIcon(f.kind);
        const label = document.createElement("span");
        label.className = "dtx-chip-label";
        label.textContent = qbDisplayName(f);
        label.title = qbDisplayName(f);
        chip.appendChild(ic);
        chip.appendChild(label);
        chip.appendChild(makeChipRemove(() => {
            builderFields = builderFields.filter(x => x.id !== f.id);
            renderBuilderZones();
        }));
        attachChipReorder(chip, f, "fields");
        return chip;
    }

    function makeFilterChip(f) {
        const chip = document.createElement("div");
        chip.className = "dtx-builder-chip dtx-builder-chip-filter";
        const ic = document.createElement("span");
        ic.className = "dtx-chip-icon";
        ic.innerHTML = qbIcon(f.kind);
        const label = document.createElement("span");
        label.className = "dtx-chip-label";
        label.textContent = qbDisplayName(f);
        label.title = qbDisplayName(f);
        const opSel = document.createElement("select");
        opSel.className = "dtx-chip-op";
        const ops = QB_OPS[f.typeClass] || QB_OPS.text;
        for (const pair of ops) {
            const o = document.createElement("option");
            o.value = pair[0];
            o.textContent = pair[1];
            if (pair[0] === f.op) o.selected = true;
            opSel.appendChild(o);
        }
        const valWrap = document.createElement("span");
        valWrap.className = "dtx-chip-values";
        function renderVals() {
            valWrap.innerHTML = "";
            const op = f.op;
            if (op === "blank" || op === "notblank"
                || op === "istrue" || op === "isfalse") {
                return;
            }
            const ph = f.typeClass === "datetime" ? "YYYY-MM-DD" : "value";
            const v1 = document.createElement("input");
            v1.type = "text";
            v1.className = "dtx-chip-value";
            v1.value = f.value || "";
            v1.placeholder = (op === "in" || op === "notin")
                ? "value1, value2, …" : ph;
            v1.addEventListener("input", () => { f.value = v1.value; });
            valWrap.appendChild(v1);
            if (op === "between") {
                const sep = document.createElement("span");
                sep.className = "dtx-chip-sep";
                sep.textContent = "and";
                const v2 = document.createElement("input");
                v2.type = "text";
                v2.className = "dtx-chip-value";
                v2.value = f.value2 || "";
                v2.placeholder = ph;
                v2.addEventListener("input", () => { f.value2 = v2.value; });
                valWrap.appendChild(sep);
                valWrap.appendChild(v2);
            }
        }
        opSel.addEventListener("change", () => {
            f.op = opSel.value;
            renderVals();
        });
        renderVals();
        chip.appendChild(ic);
        chip.appendChild(label);
        chip.appendChild(opSel);
        chip.appendChild(valWrap);
        chip.appendChild(makeChipRemove(() => {
            builderFilters = builderFilters.filter(x => x.id !== f.id);
            renderBuilderZones();
        }));
        attachChipReorder(chip, f, "filters");
        return chip;
    }

    function renderBuilderZones() {
        fieldsZone.innerHTML = "";
        if (!builderFields.length) {
            const ph = document.createElement("div");
            ph.className = "dtx-builder-placeholder";
            ph.textContent = "Drag columns or measures here";
            fieldsZone.appendChild(ph);
        } else {
            for (const f of builderFields) {
                fieldsZone.appendChild(makeFieldChip(f));
            }
        }
        filtersZone.innerHTML = "";
        if (!builderFilters.length) {
            const ph = document.createElement("div");
            ph.className = "dtx-builder-placeholder";
            ph.textContent = "Drag fields here to filter";
            filtersZone.appendChild(ph);
        } else {
            for (const f of builderFilters) {
                filtersZone.appendChild(makeFilterChip(f));
            }
        }
    }

    function onBuildClick() {
        if (model.get("dataset_chosen") !== true) return;
        const state = {
            fields: builderFields.map(f => ({
                kind: f.kind, table: f.table, name: f.name,
                data_type: f.data_type, ref: f.ref,
            })),
            filters: builderFilters.map(f => ({
                kind: f.kind, table: f.table, name: f.name,
                data_type: f.data_type, ref: f.ref,
                op: f.op, value: f.value, value2: f.value2,
            })),
        };
        model.set("query_builder_state", JSON.stringify(state));
        model.set("error_message", "");
        model.set("build_query_trigger",
            (model.get("build_query_trigger") || 0) + 1);
        model.save_changes();
    }

    function renderBuilderChrome() {
        builderPane.classList.toggle("dtx-builder-hidden", !builderVisible);
        builderPane.classList.toggle("dtx-builder-collapsed", builderCollapsed);
        builderCollapseBtn.innerHTML = builderCollapsed
            ? PANEL_EXPAND_SVG : PANEL_COLLAPSE_SVG;
        const clabel = builderCollapsed
            ? "Expand query builder" : "Collapse query builder";
        builderCollapseBtn.title = clabel;
        builderCollapseBtn.setAttribute("aria-label", clabel);
        builderShowBtn.classList.toggle("dtx-active", builderVisible);
        const label = builderVisible
            ? "Hide query builder" : "Show query builder";
        builderShowBtn.title = label;
        builderShowBtn.setAttribute("aria-label", label);
    }

    function renderBuildBtn() {
        const chosen = model.get("dataset_chosen") === true;
        buildBtn.disabled = !chosen;
        buildBtn.title = chosen
            ? "Build a DAX query from the selected fields"
            : "Choose a semantic model first";
    }

    // ---------- Model picker (shown when no dataset is chosen) ----------
    let pickerOpen = model.get("dataset_chosen") !== true;
    const pickerBar = document.createElement("div");
    pickerBar.className = "dtx-picker";
    const pickerLabel = document.createElement("span");
    pickerLabel.className = "dtx-picker-label";
    pickerLabel.textContent = "Choose a semantic model:";
    const wsSel = document.createElement("select");
    wsSel.className = "dtx-imp-select dtx-picker-select";
    wsSel.title = "Workspace";
    const dsSel = document.createElement("select");
    dsSel.className = "dtx-imp-select dtx-picker-select";
    dsSel.title = "Semantic model";
    const pickerBtn = document.createElement("button");
    pickerBtn.type = "button";
    pickerBtn.className = "dtx-picker-btn";
    pickerBtn.textContent = "Use model";
    const pickerCancelBtn = document.createElement("button");
    pickerCancelBtn.type = "button";
    pickerCancelBtn.className = "dtx-picker-cancel";
    pickerCancelBtn.textContent = "Cancel";
    const pickerSpin = document.createElement("span");
    pickerSpin.className = "dtx-picker-spin";
    pickerBar.appendChild(pickerLabel);
    pickerBar.appendChild(wsSel);
    pickerBar.appendChild(dsSel);
    pickerBar.appendChild(pickerBtn);
    pickerBar.appendChild(pickerCancelBtn);
    pickerBar.appendChild(pickerSpin);
    main.appendChild(pickerBar);

    function renderPicker() {
        const chosen = model.get("dataset_chosen") === true;
        const show = pickerOpen || !chosen;
        pickerBar.style.display = show ? "" : "none";
        // Allow canceling only when a model is already in use.
        pickerCancelBtn.style.display = chosen ? "" : "none";
        const loading = model.get("picker_loading") === true;
        const curWs = model.get("selected_workspace_id") || "";
        const curDs = model.get("selected_dataset_id") || "";
        const wss = model.get("available_workspaces") || [];
        wsSel.innerHTML = "";
        const wph = document.createElement("option");
        wph.value = "";
        wph.textContent = wss.length ? "Select a workspace…" : "No workspaces";
        wsSel.appendChild(wph);
        wss.forEach(w => {
            const o = document.createElement("option");
            o.value = w.id;
            o.textContent = w.name;
            wsSel.appendChild(o);
        });
        wsSel.value = curWs;
        const dss = model.get("available_datasets") || [];
        dsSel.innerHTML = "";
        const dph = document.createElement("option");
        dph.value = "";
        dph.textContent = !curWs
            ? "Select a workspace first"
            : (loading
                ? "Loading…"
                : (dss.length ? "Select a semantic model…" : "No semantic models"));
        dsSel.appendChild(dph);
        dss.forEach(d => {
            const o = document.createElement("option");
            o.value = d.id;
            o.textContent = d.name;
            dsSel.appendChild(o);
        });
        dsSel.value = curDs;
        dsSel.disabled = !curWs || loading;
        // Disable "Use model" when the selection matches the model already
        // in use (same workspace and dataset).
        const sameAsActive = curWs === (model.get("active_workspace_id") || "")
            && curDs === (model.get("active_dataset_id") || "");
        pickerBtn.disabled = loading || !curDs || sameAsActive;
        pickerBtn.title = sameAsActive
            ? "This semantic model is already in use"
            : "";
        pickerSpin.textContent = loading ? "Loading…" : "";
    }
    wsSel.addEventListener("change", () => {
        model.set("selected_workspace_id", wsSel.value);
        model.set("selected_dataset_id", "");
        model.set("available_datasets", []);
        if (wsSel.value) {
            model.set("select_workspace_trigger",
                (model.get("select_workspace_trigger") || 0) + 1);
        }
        model.save_changes();
        renderPicker();
    });
    dsSel.addEventListener("change", () => {
        model.set("selected_dataset_id", dsSel.value);
        model.save_changes();
        renderPicker();
    });
    pickerBtn.addEventListener("click", () => {
        if (!model.get("selected_dataset_id")) return;
        model.set("error_message", "");
        model.set("select_dataset_trigger",
            (model.get("select_dataset_trigger") || 0) + 1);
        model.save_changes();
    });
    pickerCancelBtn.addEventListener("click", () => {
        pickerOpen = false;
        renderPicker();
    });

    // ---------- Cards ----------
    const cardsEl = document.createElement("div");
    cardsEl.className = "dtx-cards";
    main.appendChild(cardsEl);

    function renderCards() {
        const total = model.get("total_duration") || 0;
        const fe = model.get("fe_duration") || 0;
        const se = model.get("se_duration") || 0;
        const cpu = model.get("cpu_time") || 0;
        const fmt = (n) => Number(n).toLocaleString();
        const pct = (n) => total > 0 ? Math.round(n / total * 100) : 0;
        const cards = [
            { label: "Duration", value: fmt(total), sub: null },
            { label: "FE Duration", value: fmt(fe), sub: pct(fe) + "% of total" },
            { label: "SE Duration", value: fmt(se), sub: pct(se) + "% of total" },
            { label: "CPU", value: fmt(cpu), sub: null },
        ];
        cardsEl.innerHTML = cards.map(c => (
            `<div class="dtx-card">
                <div class="dtx-card-label">${escapeHtml(c.label)}</div>
                <div class="dtx-card-value">${escapeHtml(c.value)}<span class="dtx-card-unit">ms</span></div>
                ${c.sub ? `<div class="dtx-card-sub">${escapeHtml(c.sub)}</div>` : ""}
            </div>`
        )).join("");
    }

    // ---------- Query editor + Run button ----------
    const queryBlock = document.createElement("div");
    queryBlock.className = "dtx-query-block";
    main.appendChild(queryBlock);

    const toolbar = document.createElement("div");
    toolbar.className = "dtx-query-toolbar";
    queryBlock.appendChild(toolbar);

    const qTitleGroup = document.createElement("div");
    qTitleGroup.className = "dtx-query-titlegroup";
    toolbar.appendChild(qTitleGroup);

    const qTitle = document.createElement("div");
    qTitle.className = "dtx-query-title";
    qTitle.textContent = "DAX Query";
    qTitleGroup.appendChild(qTitle);

    const genBtn = document.createElement("button");
    genBtn.type = "button";
    genBtn.className = "dtx-gen-btn";
    genBtn.textContent = "Generate";
    genBtn.title = "Generate a sample EVALUATE SUMMARIZECOLUMNS query "
        + "from a measure and a related column in the model";
    qTitleGroup.appendChild(genBtn);
    genBtn.addEventListener("click", () => {
        if (model.get("dataset_chosen") !== true) return;
        model.set("error_message", "");
        model.set("generate_query_trigger",
            (model.get("generate_query_trigger") || 0) + 1);
        model.save_changes();
    });
    function renderGenBtn() {
        genBtn.disabled = model.get("dataset_chosen") !== true;
    }

    const cacheLabel = document.createElement("label");
    cacheLabel.className = "dtx-cache-label";
    cacheLabel.title = "Clear the dataset cache before running (cold-cache run)";
    const cacheCb = document.createElement("input");
    cacheCb.type = "checkbox";
    cacheCb.checked = model.get("clear_cache") === true;
    cacheCb.addEventListener("change", () => {
        model.set("clear_cache", cacheCb.checked);
        model.save_changes();
    });
    cacheLabel.appendChild(cacheCb);
    const cacheText = document.createElement("span");
    cacheText.textContent = "Clear cache";
    cacheLabel.appendChild(cacheText);
    function renderCacheBtn() {
        cacheCb.checked = model.get("clear_cache") === true;
    }
    toolbar.appendChild(cacheLabel);

    // Impersonation: none / user (effective_user_name) / role (role).
    const impWrap = document.createElement("div");
    impWrap.className = "dtx-imp-wrap";
    const impSel = document.createElement("select");
    impSel.className = "dtx-imp-select";
    impSel.title = "Run the query impersonating a user or a security role";
    [
        ["none", "No impersonation"],
        ["user", "User impersonation"],
        ["role", "Role impersonation"],
    ].forEach(([val, label]) => {
        const o = document.createElement("option");
        o.value = val;
        o.textContent = label;
        impSel.appendChild(o);
    });
    // Text input for user impersonation (effective_user_name).
    const impInput = document.createElement("input");
    impInput.type = "text";
    impInput.className = "dtx-imp-input";
    // Dropdown of model roles for role impersonation.
    const impRoleSel = document.createElement("select");
    impRoleSel.className = "dtx-imp-select dtx-imp-role-select";
    impWrap.appendChild(impSel);
    impWrap.appendChild(impInput);
    impWrap.appendChild(impRoleSel);
    toolbar.appendChild(impWrap);

    function hasRoles() {
        const roles = model.get("model_roles") || [];
        return roles.length > 0;
    }

    function renderRoleOption() {
        // Disable the "Role impersonation" choice when the model has no roles.
        const opt = Array.from(impSel.options).find(o => o.value === "role");
        if (opt) {
            opt.disabled = !hasRoles();
            opt.textContent = hasRoles()
                ? "Role impersonation"
                : "Role impersonation (no roles)";
        }
    }

    function renderRoleChoices() {
        const roles = model.get("model_roles") || [];
        const current = model.get("impersonation_value") || "";
        impRoleSel.innerHTML = "";
        roles.forEach(rn => {
            const o = document.createElement("option");
            o.value = rn;
            o.textContent = rn;
            impRoleSel.appendChild(o);
        });
        if (roles.length) {
            impRoleSel.value = roles.includes(current) ? current : roles[0];
        }
    }

    function renderImpersonation() {
        renderRoleOption();
        let mode = model.get("impersonation_mode") || "none";
        // If role impersonation is selected but the model has no roles, notify
        // the user and fall back to no impersonation.
        if (mode === "role" && !hasRoles()) {
            model.set("impersonation_mode", "none");
            model.set("impersonation_value", "");
            model.set("error_message",
                "Role impersonation is not available: this model does not "
                + "contain any roles.");
            model.save_changes();
            mode = "none";
        }
        if (impSel.value !== mode) impSel.value = mode;
        if (mode === "user") {
            impInput.style.display = "";
            impRoleSel.style.display = "none";
            impInput.placeholder = "user@domain.com";
            impInput.title = "User to impersonate (effective_user_name)";
            const val = model.get("impersonation_value") || "";
            if (impInput.value !== val) impInput.value = val;
        } else if (mode === "role") {
            impInput.style.display = "none";
            impRoleSel.style.display = "";
            impRoleSel.title = "Security role to impersonate (role)";
            renderRoleChoices();
            // Persist the resolved selection (e.g. defaulted to first role).
            if (impRoleSel.value !== (model.get("impersonation_value") || "")) {
                model.set("impersonation_value", impRoleSel.value);
                model.save_changes();
            }
        } else {
            impInput.style.display = "none";
            impRoleSel.style.display = "none";
        }
    }
    impSel.addEventListener("change", () => {
        const mode = impSel.value;
        if (mode === "role" && !hasRoles()) {
            model.set("error_message",
                "Role impersonation is not available: this model does not "
                + "contain any roles.");
            impSel.value = model.get("impersonation_mode") || "none";
            model.save_changes();
            return;
        }
        model.set("impersonation_mode", mode);
        // Reset the value so a stale user string isn't reused as a role, etc.
        model.set("impersonation_value", "");
        model.save_changes();
        renderImpersonation();
    });
    impInput.addEventListener("input", () => {
        model.set("impersonation_value", impInput.value);
        model.save_changes();
    });
    impRoleSel.addEventListener("change", () => {
        model.set("impersonation_value", impRoleSel.value);
        model.save_changes();
    });

    const runBtn = document.createElement("button");
    runBtn.type = "button";
    runBtn.className = "dtx-btn";
    function renderRunBtn() {
        const running = model.get("is_running") === true;
        const chosen = model.get("dataset_chosen") === true;
        runBtn.disabled = !running && !chosen;
        if (running) {
            runBtn.classList.add("dtx-btn-stop");
            runBtn.innerHTML = STOP_SVG;
            runBtn.title = "Cancel running query";
            runBtn.setAttribute("aria-label", "Cancel running query");
        } else {
            runBtn.classList.remove("dtx-btn-stop");
            runBtn.innerHTML = PLAY_SVG;
            runBtn.title = "Run DAX query (Ctrl/Cmd+Enter)";
            runBtn.setAttribute("aria-label", "Run DAX query");
        }
    }
    runBtn.addEventListener("click", () => {
        if (model.get("is_running") === true) {
            // Cancel
            model.set("cancel_trigger", (model.get("cancel_trigger") || 0) + 1);
            model.save_changes();
            return;
        }
        // Run
        model.set("dax_query", textarea.value);
        model.set("error_message", "");
        model.set("is_running", true);
        model.set("run_trigger", (model.get("run_trigger") || 0) + 1);
        model.save_changes();
    });
    toolbar.appendChild(runBtn);

    const textarea = document.createElement("textarea");
    textarea.className = "dtx-query";
    textarea.spellcheck = false;
    textarea.value = model.get("dax_query") || "";

    const queryWrap = document.createElement("div");
    queryWrap.className = "dtx-query-wrap";
    const hl = document.createElement("pre");
    hl.className = "dtx-query-hl";
    hl.setAttribute("aria-hidden", "true");
    queryWrap.appendChild(hl);
    queryWrap.appendChild(textarea);
    queryBlock.appendChild(queryWrap);

    function renderHighlight() {
        const tokens = model.get("dax_tokens") || [];
        const text = textarea.value;
        if (text.length === 0) {
            // The textarea's own text is transparent, so render the
            // placeholder helper text in the highlight overlay instead. It
            // disappears as soon as the user types anything.
            hl.innerHTML = '<span class="dtx-query-placeholder">'
                + 'EVALUATE — type a DAX query here, drag model objects in '
                + 'from the left, use the Query Builder, or click Generate '
                + 'to create a sample query.'
                + '</span>';
            hl.scrollTop = textarea.scrollTop;
            hl.scrollLeft = textarea.scrollLeft;
            return;
        }
        let total = 0;
        for (const t of tokens) total += (t.text || "").length;
        if (tokens.length && total === text.length) {
            hl.innerHTML = tokens.map(t => {
                const txt = escapeHtml(t.text);
                return t.kind
                    ? `<span class="dtx-tk-${t.kind}">${txt}</span>`
                    : txt;
            }).join("") + "\n";
        } else {
            // Token list out of sync with current text (user is typing) —
            // fall back to plain rendering until Python reclassifies.
            hl.textContent = text + "\n";
        }
        hl.scrollTop = textarea.scrollTop;
        hl.scrollLeft = textarea.scrollLeft;
    }

    textarea.addEventListener("input", () => {
        model.set("dax_query", textarea.value);
        model.save_changes();
        renderHighlight();
    });
    textarea.addEventListener("scroll", () => {
        hl.scrollTop = textarea.scrollTop;
        hl.scrollLeft = textarea.scrollLeft;
    });
    // Ctrl/Cmd+Enter to run
    textarea.addEventListener("keydown", (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
            e.preventDefault();
            runBtn.click();
        }
    });

    // Drag-and-drop model objects into the editor.
    function insertAtCursor(text) {
        const start = textarea.selectionStart != null
            ? textarea.selectionStart : textarea.value.length;
        const end = textarea.selectionEnd != null
            ? textarea.selectionEnd : textarea.value.length;
        const before = textarea.value.slice(0, start);
        const after = textarea.value.slice(end);
        textarea.value = before + text + after;
        const caret = start + text.length;
        textarea.selectionStart = textarea.selectionEnd = caret;
        textarea.focus();
        model.set("dax_query", textarea.value);
        model.save_changes();
        renderHighlight();
    }
    textarea.addEventListener("dragover", (e) => {
        if (dragPayload != null) {
            e.preventDefault();
            e.dataTransfer.dropEffect = "copy";
            textarea.classList.add("dtx-drop-target");
        }
    });
    textarea.addEventListener("dragleave", () => {
        textarea.classList.remove("dtx-drop-target");
    });
    textarea.addEventListener("drop", (e) => {
        const text = (e.dataTransfer && e.dataTransfer.getData("text/plain"))
            || dragPayload;
        if (text) {
            e.preventDefault();
            textarea.classList.remove("dtx-drop-target");
            // Position the caret where the drop happened, when supported.
            if (document.caretRangeFromPoint) {
                const range = document.caretRangeFromPoint(e.clientX, e.clientY);
                if (range && range.startContainer === textarea.firstChild) {
                    textarea.selectionStart = textarea.selectionEnd =
                        range.startOffset;
                }
            }
            insertAtCursor(text);
        }
    });

    // ---------- Error message ----------
    const errorEl = document.createElement("div");
    errorEl.className = "dtx-error";
    errorEl.style.display = "none";
    main.appendChild(errorEl);
    function renderError() {
        const msg = model.get("error_message") || "";
        if (msg) {
            errorEl.textContent = msg;
            errorEl.style.display = "";
        } else {
            errorEl.textContent = "";
            errorEl.style.display = "none";
        }
    }

    // ---------- View toggle (Trace details / Query result) ----------
    const viewToolbar = document.createElement("div");
    viewToolbar.className = "dtx-view-toolbar";
    main.appendChild(viewToolbar);

    const viewTitle = document.createElement("div");
    viewTitle.className = "dtx-view-title";
    viewTitle.textContent = "Results";
    viewToolbar.appendChild(viewTitle);

    const seg = document.createElement("div");
    seg.className = "dtx-seg";
    const segTrace = document.createElement("button");
    segTrace.type = "button";
    segTrace.className = "dtx-seg-btn";
    segTrace.textContent = "Trace details";
    const segResult = document.createElement("button");
    segResult.type = "button";
    segResult.className = "dtx-seg-btn";
    segResult.textContent = "Query result";
    const segChart = document.createElement("button");
    segChart.type = "button";
    segChart.className = "dtx-seg-btn";
    segChart.textContent = "Chart";
    seg.appendChild(segTrace);
    seg.appendChild(segResult);
    seg.appendChild(segChart);
    viewToolbar.appendChild(seg);
    segTrace.addEventListener("click", () => {
        model.set("view_mode", "trace");
        model.save_changes();
    });
    segResult.addEventListener("click", () => {
        model.set("view_mode", "result");
        model.save_changes();
    });
    segChart.addEventListener("click", () => {
        if (segChart.disabled) return;
        model.set("view_mode", "chart");
        model.save_changes();
    });

    // Maximum number of rows for which we render an interactive chart.
    // Beyond this, the Chart option is disabled to keep the widget responsive.
    const CHART_MAX_ROWS = 200;

    function chartEligibility() {
        const cols = model.get("result_columns") || [];
        const rows = model.get("result_rows") || [];
        const total = model.get("result_total_rows") || 0;
        const truncated = model.get("result_truncated") === true;
        if (!cols.length || !rows.length) {
            return { ok: false, reason: "No query result available." };
        }
        if (truncated || total > CHART_MAX_ROWS) {
            return {
                ok: false,
                reason: `Too many rows to chart (${total.toLocaleString()}; limit ${CHART_MAX_ROWS.toLocaleString()}).`,
            };
        }
        const numericCols = cols.map((_, i) =>
            rows.some(r => typeof r[i] === "number") &&
            rows.every(r => r[i] === null || typeof r[i] === "number")
        );
        if (!numericCols.some(Boolean)) {
            return { ok: false, reason: "No numeric column to chart." };
        }
        return { ok: true, numericCols };
    }

    function renderSeg() {
        const mode = model.get("view_mode") || "trace";
        segTrace.classList.toggle("dtx-seg-btn-on", mode === "trace");
        segResult.classList.toggle("dtx-seg-btn-on", mode === "result");
        segChart.classList.toggle("dtx-seg-btn-on", mode === "chart");
        const elig = chartEligibility();
        segChart.disabled = !elig.ok;
        segChart.title = elig.ok ? "Show simple chart of the result" : elig.reason;
    }

    const resultMeta = document.createElement("div");
    resultMeta.className = "dtx-result-meta";
    resultMeta.style.display = "none";
    main.appendChild(resultMeta);

    const tableWrap = document.createElement("div");
    tableWrap.className = "dtx-table-wrap";
    main.appendChild(tableWrap);

    const chartControls = document.createElement("div");
    chartControls.className = "dtx-chart-controls";
    chartControls.style.display = "none";
    main.appendChild(chartControls);

    const chartWrap = document.createElement("div");
    chartWrap.className = "dtx-chart-wrap";
    chartWrap.style.display = "none";
    main.appendChild(chartWrap);

    // Persisted per-render chart axis selections (not synced to Python).
    const chartState = { xIdx: null, yIdx: null };

    function renderTraceTable() {
        const rows = model.get("trace_rows") || [];
        const fmt = (n) => Number(n).toLocaleString();
        let body;
        if (!rows.length) {
            body = `<tr><td colspan="4" class="dtx-empty">No trace events captured.</td></tr>`;
        } else {
            body = rows.map(r => (
                `<tr>
                    <td>${escapeHtml(r.event_class)}</td>
                    <td>${escapeHtml(r.event_subclass)}</td>
                    <td class="dtx-num">${escapeHtml(fmt(r.duration))}</td>
                    <td class="dtx-num">${escapeHtml(fmt(r.cpu))}</td>
                </tr>`
            )).join("");
        }
        tableWrap.innerHTML = `
            <table>
                <thead><tr>
                    <th>Event Class</th>
                    <th>Event Subclass</th>
                    <th style="text-align:right">Duration (ms)</th>
                    <th style="text-align:right">CPU (ms)</th>
                </tr></thead>
                <tbody>${body}</tbody>
            </table>`;
    }

    function renderResultTable() {
        const cols = model.get("result_columns") || [];
        const rows = model.get("result_rows") || [];
        const total = model.get("result_total_rows") || 0;
        const truncated = model.get("result_truncated") === true;
        if (!cols.length) {
            tableWrap.innerHTML = `<table><tbody><tr><td class="dtx-empty">No query result available.</td></tr></tbody></table>`;
            return;
        }
        const isNum = cols.map((_, i) => rows.every(r => r[i] === null || typeof r[i] === "number"));
        const fmtCell = (v, i) => {
            if (v === null || v === undefined) return "";
            if (typeof v === "number") return escapeHtml(Number(v).toLocaleString(undefined, { maximumFractionDigits: 6 }));
            return escapeHtml(String(v));
        };
        const head = cols.map((c, i) => `<th${isNum[i] ? ' style="text-align:right"' : ""}>${escapeHtml(c)}</th>`).join("");
        const body = rows.length
            ? rows.map(r => `<tr>${r.map((v, i) => `<td${isNum[i] ? ' class="dtx-num"' : ""}>${fmtCell(v, i)}</td>`).join("")}</tr>`).join("")
            : `<tr><td colspan="${cols.length}" class="dtx-empty">Query returned no rows.</td></tr>`;
        tableWrap.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
        if (truncated) {
            resultMeta.textContent = `Showing first ${rows.length.toLocaleString()} of ${total.toLocaleString()} rows.`;
        } else {
            resultMeta.textContent = `${total.toLocaleString()} row${total === 1 ? "" : "s"}.`;
        }
    }

    function renderChart() {
        const cols = model.get("result_columns") || [];
        const rows = model.get("result_rows") || [];
        const elig = chartEligibility();
        chartControls.innerHTML = "";
        chartWrap.innerHTML = "";
        if (!elig.ok) {
            const msg = document.createElement("div");
            msg.className = "dtx-chart-empty";
            msg.textContent = elig.reason;
            chartWrap.appendChild(msg);
            chartControls.style.display = "none";
            return;
        }
        const numericCols = elig.numericCols;
        const numericIdxs = numericCols.map((b, i) => b ? i : -1).filter(i => i >= 0);
        // Default y = first numeric col; x = first non-numeric col or row index.
        if (chartState.yIdx == null || !numericIdxs.includes(chartState.yIdx)) {
            chartState.yIdx = numericIdxs[0];
        }
        const nonNumIdxs = cols.map((_, i) => numericCols[i] ? -1 : i).filter(i => i >= 0);
        if (chartState.xIdx == null || (chartState.xIdx !== -1 &&
            (chartState.xIdx >= cols.length || chartState.xIdx === chartState.yIdx))) {
            chartState.xIdx = nonNumIdxs.length ? nonNumIdxs[0] : -1;
        }

        // Axis selectors.
        const xLabel = document.createElement("label");
        xLabel.innerHTML = "<span>X</span>";
        const xSel = document.createElement("select");
        const idxOpt = document.createElement("option");
        idxOpt.value = "-1";
        idxOpt.textContent = "(row index)";
        xSel.appendChild(idxOpt);
        cols.forEach((c, i) => {
            if (i === chartState.yIdx) return;
            const o = document.createElement("option");
            o.value = String(i);
            o.textContent = c;
            xSel.appendChild(o);
        });
        xSel.value = String(chartState.xIdx);
        xSel.addEventListener("change", () => {
            chartState.xIdx = parseInt(xSel.value, 10);
            renderChart();
        });
        xLabel.appendChild(xSel);
        chartControls.appendChild(xLabel);

        const yLabel = document.createElement("label");
        yLabel.innerHTML = "<span>Y</span>";
        const ySel = document.createElement("select");
        numericIdxs.forEach(i => {
            const o = document.createElement("option");
            o.value = String(i);
            o.textContent = cols[i];
            ySel.appendChild(o);
        });
        ySel.value = String(chartState.yIdx);
        ySel.addEventListener("change", () => {
            chartState.yIdx = parseInt(ySel.value, 10);
            renderChart();
        });
        yLabel.appendChild(ySel);
        chartControls.appendChild(yLabel);
        chartControls.style.display = "";

        // Build data.
        const yIdx = chartState.yIdx;
        const xIdx = chartState.xIdx;
        const data = rows.map((r, i) => ({
            label: xIdx === -1 ? String(i + 1) : (r[xIdx] == null ? "" : String(r[xIdx])),
            value: typeof r[yIdx] === "number" ? r[yIdx] : 0,
        }));

        // SVG bar chart.
        const n = data.length;
        const barWidth = 28;
        const barGap = 8;
        const leftPad = 56;
        const rightPad = 16;
        const topPad = 12;
        const bottomPad = 56;
        const plotWidth = Math.max(n * (barWidth + barGap), 200);
        const width = leftPad + plotWidth + rightPad;
        const height = 280;
        const plotHeight = height - topPad - bottomPad;
        const values = data.map(d => d.value);
        const dataMin = Math.min(0, ...values);
        const dataMax = Math.max(0, ...values);

        // Compute "nice" integer-only axis bounds and step.
        function niceStep(raw) {
            if (raw <= 0) return 1;
            const mag = Math.pow(10, Math.floor(Math.log10(raw)));
            const norm = raw / mag;
            let nice;
            if (norm <= 1) nice = 1;
            else if (norm <= 2) nice = 2;
            else if (norm <= 5) nice = 5;
            else nice = 10;
            return Math.max(1, Math.round(nice * mag));
        }
        const ticks = 5;
        const rawSpan = (dataMax - dataMin) || 1;
        const step = niceStep(rawSpan / ticks);
        const axisMin = Math.floor(dataMin / step) * step;
        const axisMax = Math.ceil(dataMax / step) * step;
        const span = (axisMax - axisMin) || 1;
        const yScale = v => topPad + plotHeight - ((v - axisMin) / span) * plotHeight;
        const fmtNum = v => Number(v).toLocaleString();

        let gridLines = "";
        let yTicks = "";
        for (let v = axisMin; v <= axisMax + 0.5; v += step) {
            const iv = Math.round(v);
            const y = yScale(iv);
            gridLines += `<line x1="${leftPad}" x2="${leftPad + plotWidth}" y1="${y}" y2="${y}"/>`;
            yTicks += `<text x="${leftPad - 6}" y="${y + 3}" text-anchor="end">${escapeHtml(fmtNum(iv))}</text>`;
        }
        const baselineY = yScale(Math.max(axisMin, Math.min(0, axisMax)));

        let bars = "";
        let xLabels = "";
        data.forEach((d, i) => {
            const x = leftPad + i * (barWidth + barGap) + barGap / 2;
            const y = d.value >= 0 ? yScale(d.value) : baselineY;
            const h = Math.max(1, Math.abs(yScale(d.value) - baselineY));
            const tip = `${d.label}: ${fmtNum(d.value)}`;
            bars += `<rect class="dtx-chart-bar" x="${x}" y="${y}" width="${barWidth}" height="${h}" rx="2"><title>${escapeHtml(tip)}</title></rect>`;
            const cx = x + barWidth / 2;
            const labelTxt = d.label.length > 16 ? d.label.slice(0, 15) + "\u2026" : d.label;
            const ly = height - bottomPad + 14;
            xLabels += `<text x="${cx}" y="${ly}" text-anchor="end" transform="rotate(-35 ${cx} ${ly})">`
                + `<title>${escapeHtml(d.label)}</title>${escapeHtml(labelTxt)}</text>`;
        });

        const svg = `<svg class="dtx-chart-svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">`
            + `<g class="dtx-chart-grid">${gridLines}</g>`
            + `<g class="dtx-chart-axis">`
            + `<line x1="${leftPad}" x2="${leftPad}" y1="${topPad}" y2="${topPad + plotHeight}"/>`
            + `<line x1="${leftPad}" x2="${leftPad + plotWidth}" y1="${baselineY}" y2="${baselineY}"/>`
            + `${yTicks}${xLabels}`
            + `</g>`
            + `<g>${bars}</g>`
            + `</svg>`;
        chartWrap.innerHTML = svg;
    }

    function renderTable() {
        const mode = model.get("view_mode") || "trace";
        // Default visibility — chart/table swap below.
        tableWrap.style.display = "";
        chartWrap.style.display = "none";
        chartControls.style.display = "none";
        if (mode === "chart") {
            tableWrap.style.display = "none";
            chartWrap.style.display = "";
            resultMeta.style.display = "none";
            renderChart();
        } else if (mode === "result") {
            renderResultTable();
            resultMeta.style.display = (model.get("result_columns") || []).length ? "" : "none";
        } else {
            renderTraceTable();
            resultMeta.style.display = "none";
        }
        renderSeg();
    }

    // ---------- Attribution ----------
    const attribution = document.createElement("div");
    attribution.className = "sl-attribution";
    attribution.innerHTML = 'Powered by <a href="https://github.com/microsoft/semantic-link-labs" target="_blank" rel="noopener">Semantic Link Labs</a>';
    container.appendChild(attribution);

    // ---------- Wiring ----------
    model.on("change:dark_mode", applyTheme);
    model.on("change:dataset_name", renderSubtitle);
    model.on("change:workspace_name", renderSubtitle);
    model.on("change:total_duration", renderCards);
    model.on("change:fe_duration", renderCards);
    model.on("change:se_duration", renderCards);
    model.on("change:cpu_time", renderCards);
    model.on("change:trace_rows", renderTable);
    model.on("change:result_columns", renderTable);
    model.on("change:result_rows", renderTable);
    model.on("change:result_total_rows", renderTable);
    model.on("change:result_truncated", renderTable);
    model.on("change:view_mode", renderTable);
    model.on("change:is_running", () => {
        root.classList.toggle("dtx-running", model.get("is_running") === true);
        renderRunBtn();
    });
    model.on("change:error_message", renderError);
    model.on("change:dax_query", () => {
        if (textarea.value !== model.get("dax_query")) {
            textarea.value = model.get("dax_query") || "";
        }
        renderHighlight();
    });
    model.on("change:dax_tokens", renderHighlight);
    model.on("change:clear_cache", renderCacheBtn);
    model.on("change:impersonation_mode", renderImpersonation);
    model.on("change:impersonation_value", renderImpersonation);
    model.on("change:model_roles", renderImpersonation);
    model.on("change:sidebar_collapsed", renderSidebarChrome);
    model.on("change:metadata_loading", () => { renderSidebarChrome(); renderTree(); });
    model.on("change:model_tree", renderTree);
    model.on("change:dataset_chosen", () => {
        if (model.get("dataset_chosen") === true) { pickerOpen = false; }
        renderPicker(); renderRunBtn(); renderSubtitle(); renderGenBtn();
        renderBuildBtn();
    });
    model.on("change:available_workspaces", renderPicker);
    model.on("change:available_datasets", renderPicker);
    model.on("change:selected_workspace_id", renderPicker);
    model.on("change:selected_dataset_id", renderPicker);
    model.on("change:active_workspace_id", renderPicker);
    model.on("change:active_dataset_id", renderPicker);
    model.on("change:picker_loading", renderPicker);

    applyTheme();
    renderSubtitle();
    renderCards();
    renderRunBtn();
    renderCacheBtn();
    renderImpersonation();
    renderError();
    renderTable();
    renderSidebarChrome();
    renderPicker();
    renderGenBtn();
    renderTree();
    renderHighlight();
    renderBuilderChrome();
    renderBuilderZones();
    renderBuildBtn();
}
export default { render };
"""
    widget_js = (
        widget_js.replace("__DTX_SUN__", sun_icon)
        .replace("__DTX_MOON__", moon_icon)
        .replace("__DTX_TABLE__", table_icon)
        .replace("__DTX_CALC_GROUP__", calc_group_icon)
        .replace("__DTX_CALC_ITEM__", calc_item_icon)
        .replace("__DTX_COLUMN__", column_icon)
        .replace("__DTX_MEASURE__", measure_icon)
        .replace("__DTX_HIERARCHY__", hierarchy_icon)
        .replace("__DTX_CARET__", caret_icon)
        .replace("__DTX_FOLDER__", folder_icon)
        .replace("__DTX_LEVEL__", level_icon)
    )

    class DaxTestWidget(anywidget.AnyWidget):
        _esm = widget_js
        _css = widget_css

        dax_query = traitlets.Unicode("").tag(sync=True)
        dax_tokens = traitlets.List([]).tag(sync=True)
        dataset_name = traitlets.Unicode("").tag(sync=True)
        workspace_name = traitlets.Unicode("").tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)
        clear_cache = traitlets.Bool(True).tag(sync=True)
        total_duration = traitlets.Int(0).tag(sync=True)
        fe_duration = traitlets.Int(0).tag(sync=True)
        se_duration = traitlets.Int(0).tag(sync=True)
        cpu_time = traitlets.Int(0).tag(sync=True)
        trace_rows = traitlets.List([]).tag(sync=True)
        result_columns = traitlets.List([]).tag(sync=True)
        result_rows = traitlets.List([]).tag(sync=True)
        result_total_rows = traitlets.Int(0).tag(sync=True)
        result_truncated = traitlets.Bool(False).tag(sync=True)
        view_mode = traitlets.Unicode("trace").tag(sync=True)
        is_running = traitlets.Bool(False).tag(sync=True)
        error_message = traitlets.Unicode("").tag(sync=True)
        run_trigger = traitlets.Int(0).tag(sync=True)
        cancel_trigger = traitlets.Int(0).tag(sync=True)
        model_tree = traitlets.List([]).tag(sync=True)
        sidebar_collapsed = traitlets.Bool(False).tag(sync=True)
        refresh_metadata_trigger = traitlets.Int(0).tag(sync=True)
        metadata_loading = traitlets.Bool(False).tag(sync=True)
        impersonation_mode = traitlets.Unicode("none").tag(sync=True)
        impersonation_value = traitlets.Unicode("").tag(sync=True)
        model_roles = traitlets.List([]).tag(sync=True)
        dataset_chosen = traitlets.Bool(False).tag(sync=True)
        available_workspaces = traitlets.List([]).tag(sync=True)
        available_datasets = traitlets.List([]).tag(sync=True)
        selected_workspace_id = traitlets.Unicode("").tag(sync=True)
        selected_dataset_id = traitlets.Unicode("").tag(sync=True)
        active_workspace_id = traitlets.Unicode("").tag(sync=True)
        active_dataset_id = traitlets.Unicode("").tag(sync=True)
        picker_loading = traitlets.Bool(False).tag(sync=True)
        select_workspace_trigger = traitlets.Int(0).tag(sync=True)
        select_dataset_trigger = traitlets.Int(0).tag(sync=True)
        load_workspaces_trigger = traitlets.Int(0).tag(sync=True)
        generate_query_trigger = traitlets.Int(0).tag(sync=True)
        query_builder_state = traitlets.Unicode("").tag(sync=True)
        build_query_trigger = traitlets.Int(0).tag(sync=True)

    initial_result = _result_payload_from_df(result_df)

    # Mutable model context so the front-end model picker can switch the
    # active dataset/workspace at runtime (the run/metadata workers read the
    # current ids from this dict rather than closing over fixed values).
    model_ctx = {"dataset_id": dataset_id, "workspace_id": workspace_id}
    dataset_chosen = dataset_id is not None

    # Collect the model metadata tree synchronously before constructing the
    # widget. Loading it in a background thread that sets traits right after
    # display() races with the widget comm being opened: the finished-tree
    # update can be sent before the front-end is listening, leaving the
    # sidebar stuck on "Loading model metadata…". The tree collection is
    # fast, so building it up-front (and shipping it as initial state) is
    # both reliable and quick.
    if dataset_chosen:
        try:
            initial_tree = _collect_model_tree(dataset_id, workspace_id)
        except Exception:
            initial_tree = []
        try:
            initial_roles = _collect_model_roles(dataset_id, workspace_id)
        except Exception:
            initial_roles = []
    else:
        initial_tree = []
        initial_roles = []

    # When no dataset was supplied, populate the workspace picker up-front so
    # the user can immediately choose a workspace and then a semantic model.
    if dataset_chosen:
        initial_workspaces = []
        initial_datasets = []
    else:
        try:
            initial_workspaces = _list_workspaces_for_picker()
        except Exception:
            initial_workspaces = []
        # If a workspace was provided (but no dataset), pre-load its models.
        if workspace_id:
            try:
                initial_datasets = _list_datasets_for_picker(workspace_id)
            except Exception:
                initial_datasets = []
        else:
            initial_datasets = []

    widget = DaxTestWidget(
        dax_query=formatted_initial or "",
        dax_tokens=_classify_dax_spans(formatted_initial or ""),
        dataset_name=dataset_name or "",
        workspace_name=workspace_name or "",
        dark_mode=bool(dark_mode),
        clear_cache=bool(clear_cache),
        total_duration=int(total_duration),
        fe_duration=int(fe_duration),
        se_duration=int(se_duration),
        cpu_time=int(cpu_time),
        trace_rows=initial_rows,
        result_columns=initial_result["columns"],
        result_rows=initial_result["rows"],
        result_total_rows=int(initial_result["total_rows"]),
        result_truncated=bool(initial_result["truncated"]),
        view_mode="trace",
        is_running=False,
        error_message="",
        run_trigger=0,
        cancel_trigger=0,
        model_tree=initial_tree,
        sidebar_collapsed=False,
        refresh_metadata_trigger=0,
        metadata_loading=False,
        dataset_chosen=dataset_chosen,
        available_workspaces=initial_workspaces,
        available_datasets=initial_datasets,
        selected_workspace_id=str(workspace_id) if workspace_id else "",
        selected_dataset_id="",
        active_workspace_id=str(workspace_id) if workspace_id else "",
        active_dataset_id=str(dataset_id) if dataset_id else "",
        picker_loading=False,
        select_workspace_trigger=0,
        select_dataset_trigger=0,
        load_workspaces_trigger=0,
        generate_query_trigger=0,
        query_builder_state="",
        build_query_trigger=0,
        impersonation_mode=(
            "user" if effective_user_name else ("role" if role else "none")
        ),
        impersonation_value=(effective_user_name or role or ""),
        model_roles=initial_roles,
    )

    # Expose the most recent dataframes for programmatic access.
    widget.last_df = df  # type: ignore[attr-defined]
    widget.last_result_df = result_df  # type: ignore[attr-defined]

    # State shared between the run/cancel observers.
    import threading

    run_state = {
        "thread": None,
        "current_run_id": 0,
        "canceled_run_ids": set(),
    }
    state_lock = threading.Lock()

    def _worker(
        query: str,
        clear_cache_flag: bool,
        run_id: int,
        effective_user: Optional[str],
        role_name: Optional[str],
    ) -> None:
        try:
            (
                new_df,
                new_total,
                new_fe,
                new_se,
                new_cpu,
                new_result,
            ) = _run_dax_trace(
                dataset_id=model_ctx["dataset_id"],
                workspace_id=model_ctx["workspace_id"],
                dax_string=query,
                clear_cache=clear_cache_flag,
                effective_user_name=effective_user,
                role=role_name,
            )
        except Exception as exc:  # noqa: BLE001
            with state_lock:
                canceled = run_id in run_state["canceled_run_ids"]
            if canceled:
                return
            widget.error_message = f"{type(exc).__name__}: {exc}"
            widget.is_running = False
            return

        with state_lock:
            canceled = run_id in run_state["canceled_run_ids"]
        if canceled:
            # User canceled — discard results. The underlying engine may
            # still have completed the query in the background.
            return

        widget.last_df = new_df  # type: ignore[attr-defined]
        widget.last_result_df = new_result  # type: ignore[attr-defined]
        widget.total_duration = int(new_total)
        widget.fe_duration = int(new_fe)
        widget.se_duration = int(new_se)
        widget.cpu_time = int(new_cpu)
        widget.trace_rows = _trace_rows_from_df(new_df)
        payload = _result_payload_from_df(new_result)
        widget.result_columns = payload["columns"]
        widget.result_rows = payload["rows"]
        widget.result_total_rows = int(payload["total_rows"])
        widget.result_truncated = bool(payload["truncated"])
        widget.error_message = ""
        widget.is_running = False

    def _on_run(change):
        if change["new"] == change["old"]:
            return
        if model_ctx["dataset_id"] is None:
            widget.error_message = (
                "No semantic model selected. Choose a workspace and a "
                "semantic model first."
            )
            widget.is_running = False
            return
        query = widget.dax_query or ""
        if not query.strip():
            widget.error_message = "DAX query is empty."
            widget.is_running = False
            return
        mode = widget.impersonation_mode or "none"
        imp_value = (widget.impersonation_value or "").strip()
        effective_user = imp_value if mode == "user" else None
        role_name = imp_value if mode == "role" else None
        if mode in ("user", "role") and not imp_value:
            label = "user" if mode == "user" else "role"
            widget.error_message = (
                f"Impersonation is set to '{label}' but no {label} value "
                "was provided."
            )
            widget.is_running = False
            return
        with state_lock:
            run_state["current_run_id"] += 1
            run_id = run_state["current_run_id"]
        thread = threading.Thread(
            target=_worker,
            args=(query, bool(widget.clear_cache), run_id, effective_user, role_name),
            daemon=True,
        )
        with state_lock:
            run_state["thread"] = thread
        thread.start()

    def _on_cancel(change):
        if change["new"] == change["old"]:
            return
        with state_lock:
            run_id = run_state["current_run_id"]
            run_state["canceled_run_ids"].add(run_id)
        widget.is_running = False
        widget.error_message = (
            "Query canceled. Note: the DAX engine may still finish the "
            "query in the background; results have been discarded."
        )

    widget.observe(_on_run, names="run_trigger")
    widget.observe(_on_cancel, names="cancel_trigger")

    def _on_query_change(change):
        # Re-classify on every edit so the syntax-highlight overlay stays
        # in sync. The DAX tokenizer is cheap relative to comm latency.
        try:
            widget.dax_tokens = _classify_dax_spans(change["new"] or "")
        except Exception:
            pass

    widget.observe(_on_query_change, names="dax_query")

    def _load_metadata() -> None:
        if model_ctx["dataset_id"] is None:
            widget.metadata_loading = False
            return
        try:
            tree = _collect_model_tree(
                model_ctx["dataset_id"], model_ctx["workspace_id"]
            )
            roles = _collect_model_roles(
                model_ctx["dataset_id"], model_ctx["workspace_id"]
            )
        except Exception as exc:  # noqa: BLE001
            widget.metadata_loading = False
            widget.error_message = f"Failed to load model metadata: {exc}"
            return
        widget.model_tree = tree
        widget.model_roles = roles
        widget.metadata_loading = False

    def _on_refresh_metadata(change):
        if change["new"] == change["old"]:
            return
        if widget.metadata_loading:
            return
        widget.metadata_loading = True
        threading.Thread(target=_load_metadata, daemon=True).start()

    widget.observe(_on_refresh_metadata, names="refresh_metadata_trigger")

    def _load_datasets_for_selected_workspace() -> None:
        ws_id = (widget.selected_workspace_id or "").strip()
        if not ws_id:
            widget.available_datasets = []
            widget.picker_loading = False
            return
        try:
            datasets = _list_datasets_for_picker(ws_id)
        except Exception as exc:  # noqa: BLE001
            widget.available_datasets = []
            widget.picker_loading = False
            widget.error_message = f"Failed to list semantic models: {exc}"
            return
        widget.available_datasets = datasets
        widget.picker_loading = False

    def _on_select_workspace(change):
        if change["new"] == change["old"]:
            return
        if widget.picker_loading:
            return
        widget.picker_loading = True
        threading.Thread(
            target=_load_datasets_for_selected_workspace, daemon=True
        ).start()

    widget.observe(_on_select_workspace, names="select_workspace_trigger")

    def _activate_selected_dataset() -> None:
        ws_id = (widget.selected_workspace_id or "").strip()
        ds_id = (widget.selected_dataset_id or "").strip()
        if not ws_id or not ds_id:
            widget.picker_loading = False
            return
        try:
            from sempy_labs._helper_functions import (
                resolve_workspace_name_and_id as _rwni,
            )

            (ws_name, ws_id_resolved) = _rwni(ws_id)
            (ds_name, ds_id_resolved) = resolve_item_name_and_id(
                item=ds_id, type="SemanticModel", workspace=ws_id_resolved
            )
            model_ctx["workspace_id"] = ws_id_resolved
            model_ctx["dataset_id"] = ds_id_resolved
            tree = _collect_model_tree(ds_id_resolved, ws_id_resolved)
            roles = _collect_model_roles(ds_id_resolved, ws_id_resolved)
        except Exception as exc:  # noqa: BLE001
            widget.picker_loading = False
            widget.error_message = f"Failed to load semantic model: {exc}"
            return
        widget.dataset_name = str(ds_name) if ds_name else str(ds_id)
        widget.workspace_name = str(ws_name) if ws_name else ""
        widget.active_workspace_id = str(ws_id_resolved)
        widget.active_dataset_id = str(ds_id_resolved)
        widget.model_tree = tree
        widget.model_roles = roles
        # Reset impersonation so a stale role/user from a prior model isn't
        # reused against a model that may not define it.
        widget.impersonation_mode = "none"
        widget.impersonation_value = ""
        widget.dataset_chosen = True
        widget.error_message = ""
        widget.picker_loading = False

    def _on_select_dataset(change):
        if change["new"] == change["old"]:
            return
        if widget.picker_loading:
            return
        widget.picker_loading = True
        threading.Thread(target=_activate_selected_dataset, daemon=True).start()

    widget.observe(_on_select_dataset, names="select_dataset_trigger")

    def _load_workspaces() -> None:
        try:
            workspaces = _list_workspaces_for_picker()
        except Exception as exc:  # noqa: BLE001
            widget.picker_loading = False
            widget.error_message = f"Failed to list workspaces: {exc}"
            return
        widget.available_workspaces = workspaces
        # Refresh the dataset list for the currently selected workspace too.
        ws_id = (widget.selected_workspace_id or "").strip()
        if ws_id:
            try:
                widget.available_datasets = _list_datasets_for_picker(ws_id)
            except Exception:
                pass
        widget.picker_loading = False

    def _on_load_workspaces(change):
        if change["new"] == change["old"]:
            return
        if widget.picker_loading:
            return
        widget.picker_loading = True
        threading.Thread(target=_load_workspaces, daemon=True).start()

    widget.observe(_on_load_workspaces, names="load_workspaces_trigger")

    def _generate_query() -> None:
        if model_ctx["dataset_id"] is None:
            widget.error_message = (
                "No semantic model selected. Choose a workspace and a "
                "semantic model first."
            )
            return
        try:
            dax = _generate_sample_dax(
                model_ctx["dataset_id"], model_ctx["workspace_id"]
            )
        except Exception as exc:  # noqa: BLE001
            widget.error_message = f"Failed to generate a DAX query: {exc}"
            return
        if not dax:
            widget.error_message = (
                "Could not generate a DAX query: the model has no usable "
                "measure to summarize."
            )
            return
        try:
            formatted = _format_dax(dax)
            dax_out = formatted[0] if formatted else dax
        except Exception:
            dax_out = dax
        dax_out = dax_out.replace("\r\n", "\n").replace("\r", "\n")
        widget.dax_query = dax_out
        widget.dax_tokens = _classify_dax_spans(dax_out)
        widget.error_message = ""

    def _on_generate_query(change):
        if change["new"] == change["old"]:
            return
        threading.Thread(target=_generate_query, daemon=True).start()

    widget.observe(_on_generate_query, names="generate_query_trigger")

    def _build_query() -> None:
        if model_ctx["dataset_id"] is None:
            widget.error_message = (
                "No semantic model selected. Choose a workspace and a "
                "semantic model first."
            )
            return
        import json as _json

        try:
            state = _json.loads(widget.query_builder_state or "{}")
        except Exception:
            widget.error_message = "Could not read the query builder state."
            return
        fields = state.get("fields") or []
        if not fields:
            widget.error_message = (
                "Add at least one column or measure to the query builder "
                "before building a query."
            )
            return
        try:
            dax = _build_summarize_dax(
                state, model_ctx["dataset_id"], model_ctx["workspace_id"]
            )
        except Exception as exc:  # noqa: BLE001
            widget.error_message = f"Failed to build a DAX query: {exc}"
            return
        if not dax:
            widget.error_message = (
                "Could not build a DAX query from the current selection."
            )
            return
        try:
            formatted = _format_dax(dax)
            dax_out = formatted[0] if formatted else dax
        except Exception:
            dax_out = dax
        dax_out = dax_out.replace("\r\n", "\n").replace("\r", "\n")
        widget.dax_query = dax_out
        widget.dax_tokens = _classify_dax_spans(dax_out)
        widget.error_message = ""

    def _on_build_query(change):
        if change["new"] == change["old"]:
            return
        threading.Thread(target=_build_query, daemon=True).start()

    widget.observe(_on_build_query, names="build_query_trigger")

    display(widget)
