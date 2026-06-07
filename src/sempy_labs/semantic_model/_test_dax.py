import sempy.fabric as fabric
import pandas as pd
from sempy_labs._helper_functions import (
    resolve_item_name_and_id,
)
from typing import Optional, Tuple
from sempy._utils._log import log
from uuid import UUID
import time
from datetime import datetime, timezone


@log
def test(
    dataset: Optional[str | UUID] = None,
    dax_string: str = "",
    workspace: Optional[str | UUID] = None,
    clear_cache: bool = True,
    visualize: bool = True,
    effective_user_name: Optional[str] = None,
    role: Optional[str] = None,
    dark_mode: bool = False,
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
    dark_mode : bool, default=False
        If True, the interactive widget is initially displayed using its
        dark color theme. Only applies when ``visualize=True``.

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
            dark_mode=dark_mode,
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
    "DAXQueryPlan": [
        "EventClass",
        "EventSubclass",
        "CurrentTime",
        "TextData",
    ],
}


def _execute_and_capture(
    trace,
    dataset_id: str,
    workspace_id: str,
    dax_string: str,
    effective_user_name: Optional[str],
    role: Optional[str],
    baseline_count: int,
    run_warmup: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame, int]:
    """Run the real DAX query (optionally preceded by a one-time warm-up
    evaluation) against an already-started ``trace`` and capture the trace
    rows produced by this execution.

    The trace is *not* stopped: events are read live via
    :meth:`Trace.get_trace_logs`, which lets a single long-running trace serve
    many queries. ``baseline_count`` is the number of log rows already consumed
    by previous executions; only rows beyond it belong to this query.

    ``run_warmup`` controls whether the throwaway ``EVALUATE {1}`` warm-up
    query is executed first. This is only needed for the very first query run
    against a semantic model (to prime caches/connection); subsequent queries
    against the same model skip it.

    Returns
    -------
    tuple
        ``(result_df, new_logs, new_total_count)`` where ``new_logs`` are the
        trace rows for this execution (rows after ``baseline_count``) and
        ``new_total_count`` is the total log row count after this execution
        (the next baseline).
    """

    if run_warmup:
        # Warm-up evaluation; filtered out of results below. Only run for the
        # first query against the model.
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
    # Wait for the trace to flush the real query's QueryEnd event. Poll the
    # running trace rather than sleeping a fixed interval so the common case
    # returns quickly, while keeping a 2s cap so a missed event can never hang
    # the run (matching the prior behavior as an upper bound).
    logs: Optional[pd.DataFrame] = None
    _deadline = time.monotonic() + 2.0
    while time.monotonic() < _deadline:
        time.sleep(0.1)
        try:
            logs = trace.get_trace_logs()
        except Exception:
            continue
        if logs is None or logs.empty:
            continue
        _new = logs.iloc[baseline_count:] if len(logs) > baseline_count else logs.iloc[0:0]
        _ec = "Event Class" if "Event Class" in _new.columns else "EventClass"
        _td = "Text Data" if "Text Data" in _new.columns else "TextData"
        if _ec not in _new.columns:
            continue
        _qe = _new[_new[_ec] == "QueryEnd"]
        if _qe.empty:
            continue
        if _td in _new.columns:
            # Break once a QueryEnd for the real query (not the warm-up
            # EVALUATE {1}) has been captured.
            _real = _qe[~_qe[_td].astype(str).str.startswith("EVALUATE {1}")]
            if not _real.empty:
                break
        elif len(_qe) >= (2 if run_warmup else 1):
            # No text available: warm-up + real query == 2 QueryEnd, or just
            # the real query when the warm-up was skipped.
            break

    # DAX Query Plan events (Logical/Physical) are serialized by the engine
    # during query cleanup and are frequently flushed into the trace buffer
    # *after* the QueryEnd event that ended the loop above. A single query
    # normally emits two plan events (a logical and a physical plan), and they
    # can arrive in separate trace flushes, so breaking as soon as the first
    # plan row appears risks dropping the second one. Poll (always re-reading
    # the live trace, even if ``logs`` is currently empty) until the number of
    # captured DAXQueryPlan rows stops growing for a short grace period, or a
    # cap is reached.
    _plan_deadline = time.monotonic() + 5.0
    _plan_count = -1
    _stable_since: Optional[float] = None
    while time.monotonic() < _plan_deadline:
        try:
            _l = trace.get_trace_logs()
        except Exception:
            _l = None
        if _l is not None and not _l.empty:
            logs = _l
        if logs is not None and len(logs) > baseline_count:
            _new = logs.iloc[baseline_count:]
        elif logs is not None:
            _new = logs.iloc[0:0]
        else:
            _new = pd.DataFrame()
        _ec = "Event Class" if "Event Class" in _new.columns else "EventClass"
        _cur = (
            int((_new[_ec] == "DAXQueryPlan").sum()) if _ec in _new.columns else 0
        )
        if _cur > 0:
            if _cur != _plan_count:
                # New plan row(s) arrived; reset the stability timer so we keep
                # waiting for any further plan events still being flushed.
                _plan_count = _cur
                _stable_since = time.monotonic()
            elif _stable_since is not None and (
                time.monotonic() - _stable_since >= 0.5
            ):
                # Plan count held steady long enough: assume all DAX query plan
                # events for this query have been captured.
                break
        time.sleep(0.1)

    if logs is None:
        try:
            logs = trace.get_trace_logs()
        except Exception:
            logs = pd.DataFrame()
    if logs is None:
        logs = pd.DataFrame()

    if len(logs) > baseline_count:
        new_logs = logs.iloc[baseline_count:].reset_index(drop=True)
    else:
        new_logs = logs.iloc[0:0].reset_index(drop=True)
    return result_df, new_logs, len(logs)


def _compute_trace_stats(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, int, int, int, int]:
    """Filter trace rows and compute DAX Studio style aggregate stats.

    Returns
    -------
    tuple
        ``(df, total_duration, fe_duration, se_duration, cpu_time)``
    """

    if df is None:
        df = pd.DataFrame()
    # Drop events from other sessions / warm-up evaluation.
    if "Application Name" in df.columns:
        df = df[~df["Application Name"].isin(["PowerBI", "PowerBIEIM"])]
    if "Text Data" in df.columns:
        df = df[~df["Text Data"].astype(str).str.startswith("EVALUATE {1}")]
    df = df.reset_index(drop=True)

    if "Event Class" not in df.columns:
        return df, 0, 0, 0, 0

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

    return df, total_duration, fe_duration, se_duration, cpu_time


def _run_dax_trace(
    dataset_id: str,
    workspace_id: str,
    dax_string: str,
    clear_cache: bool,
    effective_user_name: Optional[str] = None,
    role: Optional[str] = None,
) -> Tuple[pd.DataFrame, int, int, int, int, pd.DataFrame]:
    """Run a DAX query with a one-shot server-side trace and compute DAX
    Studio style aggregate stats.

    A fresh trace is created, started, used for this single query, and then
    stopped. This is used for ``visualize=False`` and the initial query of the
    interactive widget. The widget itself uses a single long-running trace for
    subsequent queries (see ``_visualize_dax_test``).

    Returns
    -------
    tuple
        ``(df, total_duration, fe_duration, se_duration, cpu_time, result_df)``
    """
    from sempy_labs._clear_cache import clear_cache as _clear_cache_fn

    if clear_cache:
        _clear_cache_fn(dataset=dataset_id, workspace=workspace_id)

    result_df: pd.DataFrame = pd.DataFrame()
    df = pd.DataFrame()
    with fabric.create_trace_connection(
        dataset=dataset_id, workspace=workspace_id
    ) as trace_connection:
        with trace_connection.create_trace(_TEST_EVENT_SCHEMA) as trace:
            trace.start()
            result_df, df, _ = _execute_and_capture(
                trace,
                dataset_id,
                workspace_id,
                dax_string,
                effective_user_name,
                role,
                0,
            )
            # Stop the one-shot trace; prefer its authoritative logs.
            try:
                stopped = trace.stop()
                if stopped is not None and not stopped.empty:
                    df = stopped
            except Exception:
                pass

    df, total_duration, fe_duration, se_duration, cpu_time = _compute_trace_stats(
        df
    )

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


def _query_plan_rows_from_df(df: pd.DataFrame) -> list:
    """Convert the captured trace dataframe to a list of DAX Query Plan rows
    (``{plan_type, event_subclass, text}``) suitable for serialization to the
    front-end. ``plan_type`` is ``"Logical"`` or ``"Physical"`` based on the
    event subclass (e.g. ``DAXVertiPaqLogicalPlan`` /
    ``DAXVertiPaqPhysicalPlan``)."""

    if df is None or df.empty or "Event Class" not in df.columns:
        return []
    rows_df = df[df["Event Class"] == "DAXQueryPlan"]
    out = []
    for _, row in rows_df.iterrows():
        sc = str(row.get("Event Subclass", "") or "")
        text = row.get("Text Data", "")
        text = "" if not pd.notna(text) else str(text)
        low = sc.lower()
        if "physical" in low or sc.strip() == "2":
            plan_type = "Physical"
        elif "logical" in low or sc.strip() == "1":
            plan_type = "Logical"
        else:
            plan_type = sc or "Unknown"
        out.append(
            {
                "plan_type": plan_type,
                "event_subclass": sc,
                "text": text,
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

    try:
        with connect_semantic_model(
            dataset=dataset_id, workspace=workspace_id, readonly=True
        ) as tom:
            return _build_model_tree(tom)
    except Exception:
        return []


def _build_model_tree(tom) -> list:
    """Build the sidebar metadata tree from an already-open TOM connection."""

    tree: list = []
    try:
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
                        "expression": str(getattr(m, "Expression", "") or ""),
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
            is_calc_group = getattr(table, "CalculationGroup", None) is not None
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


def _build_relationship_lookup(tom) -> dict:
    """Build a mapping of relationship name -> a human-readable description of
    the columns it joins (``'FromTable'[FromColumn] → 'ToTable'[ToColumn]``),
    using an already-open TOM connection. Inactive relationships are flagged.
    """

    lookup: dict = {}
    try:
        for rel in tom.model.Relationships:
            try:
                detail = (
                    f"'{rel.FromTable.Name}'[{rel.FromColumn.Name}]"
                    f" → '{rel.ToTable.Name}'[{rel.ToColumn.Name}]"
                )
                if not bool(getattr(rel, "IsActive", True)):
                    detail += " (inactive)"
                lookup[str(rel.Name)] = detail
            except Exception:
                continue
    except Exception:
        return {}
    return lookup


def _build_relationship_columns(tom) -> dict:
    """Build a mapping of relationship name -> the list of ``(table, column)``
    tuples it joins (the From and To columns), using an already-open TOM
    connection. Used to include relationship columns in the unique list of
    referenced columns."""

    lookup: dict = {}
    try:
        for rel in tom.model.Relationships:
            try:
                lookup[str(rel.Name)] = [
                    (str(rel.FromTable.Name), str(rel.FromColumn.Name)),
                    (str(rel.ToTable.Name), str(rel.ToColumn.Name)),
                ]
            except Exception:
                continue
    except Exception:
        return {}
    return lookup


def _build_rownumber_columns(tom) -> set:
    """Build a set of ``(table_name, column_name)`` tuples for every column
    whose TOM ``ColumnType`` is ``RowNumber``, using an already-open TOM
    connection. These internal columns are excluded from the query
    dependencies output."""

    rownumber: set = set()
    try:
        for table in tom.model.Tables:
            tname = str(table.Name)
            for c in table.Columns:
                try:
                    if str(getattr(c, "Type", "")) == "RowNumber":
                        rownumber.add((tname, str(c.Name)))
                except Exception:
                    continue
    except Exception:
        return set()
    return rownumber


def _build_dependency_tree(
    rows: list, rel_lookup: dict, model_label: str, rownumber_cols: Optional[set] = None
) -> list:
    """Organize flat ``INFO.CALCDEPENDENCY`` rows into a hierarchical tree.

    The tree has a single ``Model`` root whose children are a ``Tables`` group
    (each referenced table, with its referenced columns / measures /
    hierarchies grouped beneath it) and a ``Relationships`` group (each
    referenced relationship, labeled with the columns it joins, looked up via
    TOM in ``rel_lookup``). Columns whose TOM ``ColumnType`` is ``RowNumber``
    (provided in ``rownumber_cols`` as ``(table, column)`` tuples) are omitted.
    """

    rownumber_cols = rownumber_cols or set()

    def _classify(obj_type: str) -> str:
        t = (obj_type or "").upper()
        if "RELATIONSHIP" in t:
            return "relationship"
        if "MEASURE" in t:
            return "measure"
        if "HIERARCHY" in t:
            return "hierarchy"
        if "COLUMN" in t:
            return "column"
        if "CALC_GROUP" in t or "CALCULATION_GROUP" in t:
            return "calc_group"
        if t == "TABLE":
            return "table"
        return "other"

    tables: dict = {}
    relationships: list = []
    seen_rel: set = set()

    def _ensure_table(name: str) -> dict:
        if name not in tables:
            tables[name] = {
                "columns": [],
                "measures": [],
                "hierarchies": [],
                "other": [],
            }
        return tables[name]

    def _add(bucket: list, value: str) -> None:
        if value and value not in bucket:
            bucket.append(value)

    for r in rows:
        kind = _classify(r.get("object_type"))
        table = (r.get("table") or "").strip()
        obj = (r.get("object") or "").strip()
        if kind == "relationship":
            if obj and obj not in seen_rel:
                seen_rel.add(obj)
                relationships.append(obj)
        elif kind == "measure":
            if table:
                _add(_ensure_table(table)["measures"], obj)
        elif kind == "column":
            if table and (table, obj) not in rownumber_cols:
                _add(_ensure_table(table)["columns"], obj)
        elif kind == "hierarchy":
            if table:
                _add(_ensure_table(table)["hierarchies"], obj)
        elif kind == "table":
            _ensure_table(obj or table)
        else:
            if table:
                _add(_ensure_table(table)["other"], obj)

    def _leaf_group(label: str, kind: str, names: list) -> dict:
        return {
            "label": label,
            "kind": "group",
            "children": [
                {"label": n, "kind": kind}
                for n in sorted(names, key=str.lower)
            ],
        }

    table_nodes = []
    for tname in sorted(tables, key=str.lower):
        tdata = tables[tname]
        tchildren = []
        if tdata["columns"]:
            tchildren.append(
                _leaf_group("Columns", "column", tdata["columns"])
            )
        if tdata["measures"]:
            tchildren.append(
                _leaf_group("Measures", "measure", tdata["measures"])
            )
        if tdata["hierarchies"]:
            tchildren.append(
                _leaf_group("Hierarchies", "hierarchy", tdata["hierarchies"])
            )
        if tdata["other"]:
            tchildren.append(_leaf_group("Other", "column", tdata["other"]))
        table_nodes.append(
            {"label": tname, "kind": "table", "children": tchildren}
        )

    children = []
    if table_nodes:
        children.append(
            {"label": "Tables", "kind": "group", "children": table_nodes}
        )
    if relationships:
        rel_nodes = []
        for rname in relationships:
            detail = rel_lookup.get(rname)
            rel_nodes.append(
                {"label": detail or rname, "kind": "relationship"}
            )
        rel_nodes.sort(key=lambda n: n["label"].lower())
        children.append(
            {"label": "Relationships", "kind": "group", "children": rel_nodes}
        )

    if not children:
        return []
    return [
        {
            "label": model_label or "Model",
            "kind": "model",
            "children": children,
        }
    ]


def _build_dependency_columns(
    rows: list,
    rel_columns: Optional[dict] = None,
    rownumber_cols: Optional[set] = None,
) -> list:
    """Build a unique, sorted list of the columns referenced by the query.

    Each entry is a ``{"table": ..., "column": ...}`` dict. Columns directly
    referenced by the query (``REFERENCED_OBJECT_TYPE`` containing ``COLUMN``)
    as well as the columns participating in any referenced relationship (looked
    up via TOM in ``rel_columns`` keyed by relationship name) are included.
    Columns whose TOM ``ColumnType`` is ``RowNumber`` (provided in
    ``rownumber_cols`` as ``(table, column)`` tuples) are omitted.
    """

    rel_columns = rel_columns or {}
    rownumber_cols = rownumber_cols or set()
    seen: set = set()
    out: list = []

    def _add(table: str, column: str) -> None:
        table = (table or "").strip()
        column = (column or "").strip()
        if not table or not column:
            return
        key = (table, column)
        if key in rownumber_cols or key in seen:
            return
        seen.add(key)
        out.append({"table": table, "column": column})

    for r in rows:
        otype = (r.get("object_type") or "").upper()
        if "RELATIONSHIP" in otype:
            obj = (r.get("object") or "").strip()
            for tbl, col in rel_columns.get(obj, []):
                _add(tbl, col)
        elif "COLUMN" in otype:
            _add(r.get("table"), r.get("object"))

    out.sort(key=lambda x: (x["table"].lower(), x["column"].lower()))
    return out


def _build_model_roles(tom) -> list:
    """Build the sorted list of security role names from an already-open TOM
    connection."""

    try:
        roles = [str(r.Name) for r in tom.model.Roles]
    except Exception:
        return []
    roles.sort(key=lambda x: x.lower())
    return roles


def _collect_model_metadata(dataset_id: str, workspace_id: str) -> tuple:
    """Collect both the sidebar metadata tree and the security role names in a
    single TOM connection (avoids opening the XMLA connection twice).

    Returns
    -------
    tuple
        ``(tree, roles)``. Either may be an empty list if metadata cannot be
        read.
    """

    try:
        from sempy_labs.tom import connect_semantic_model
    except Exception:
        return [], []

    try:
        with connect_semantic_model(
            dataset=dataset_id, workspace=workspace_id, readonly=True
        ) as tom:
            return _build_model_tree(tom), _build_model_roles(tom)
    except Exception:
        return [], []



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
        out.append({"id": str(r["Dataset Id"]), "name": str(r["Dataset Name"])})
    out.sort(key=lambda x: x["name"].lower())
    return out


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
    ftype = _classify_filter_type(item.get("kind", "column"), item.get("data_type", ""))
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
    4. ``ORDER BY`` the enabled Order By items (``ASC``/``DESC``), or all
       attribute columns ascending when no ``order_by`` is supplied.

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
            col_filters.append(f"FILTER(KEEPFILTERS(VALUES({_col_ref(item)})), {pred})")

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

    # ORDER BY clause. When the builder supplies an explicit "order_by" list
    # (from the Order By pane), only the enabled items contribute, each with
    # its chosen direction (ASC for A-Z, DESC for Z-A), in pane order. When
    # no "order_by" key is present (backward compatibility), fall back to
    # ordering by all attribute columns ascending.
    order_by = state.get("order_by")
    if order_by is None:
        if group_cols:
            order_cols = ", ".join(_col_ref(c) for c in group_cols)
            dax += "\nORDER BY " + order_cols
    else:
        order_parts: list = []
        for item in order_by:
            if not item.get("enabled"):
                continue
            ref = _col_ref(item)
            if not ref:
                continue
            direction = "DESC" if str(item.get("dir", "")).lower() == "desc" else "ASC"
            order_parts.append(f"{ref} {direction}")
        if order_parts:
            dax += "\nORDER BY " + ", ".join(order_parts)

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
            spans.append({"text": dax_expression[cursor : token.position], "kind": ""})
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
        SYNTAX_HIGHLIGHT_VARS as _UI_SYNTAX_VARS,
        HEADER_CSS as _UI_HEADER_CSS,
        ATTRIBUTION_CSS as _UI_ATTRIBUTION_CSS,
        ICONS as _UI_ICONS,
    )

    # The DAX is intentionally NOT auto-formatted on load (formatting calls
    # the external DAX Formatter service and would slow down ``test()``).
    # The user can format on demand via the "Format" button in the widget.
    # Still normalize line endings to "\n" so the classified token text
    # matches the <textarea> length (a <textarea> normalizes "\r\n" to "\n"
    # on assignment; a mismatch makes the front-end fall back to plain,
    # uncolored rendering of the DAX).
    formatted_initial = (dax_string or "").replace("\r\n", "\n").replace("\r", "\n")
    initial_rows = _trace_rows_from_df(df)
    initial_query_plan_rows = _query_plan_rows_from_df(df)

    widget_css = (
        _UI_HEADER_CSS
        + "\n"
        + _UI_ATTRIBUTION_CSS
        + "\n"
        + f"""
.dtx {{
    {_UI_LIGHT_VARS}
    {_UI_SYNTAX_VARS}
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
.dtx .dtx-fmt-btn {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 26px;
    height: 26px;
    padding: 0;
    border-radius: 6px;
    border: 1px solid var(--ui-border);
    background: var(--ui-surface);
    cursor: pointer;
}}
.dtx .dtx-fmt-btn svg {{
    width: 24px;
    height: auto;
    display: block;
}}
.dtx .dtx-fmt-btn:hover:not(:disabled) {{
    border-color: var(--ui-accent);
    background: var(--ui-surface-2);
}}
.dtx .dtx-fmt-btn:disabled {{
    opacity: 0.45;
    cursor: default;
}}
.dtx .dtx-fmt-btn.dtx-fmt-loading {{
    cursor: progress;
}}
.dtx .dtx-fmt-btn.dtx-fmt-loading svg {{
    animation: dtx-fmt-pulse 0.9s ease-in-out infinite;
}}
@keyframes dtx-fmt-pulse {{
    0%, 100% {{ opacity: 1; }}
    50% {{ opacity: 0.35; }}
}}
.dtx .dtx-hist-btn {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 26px;
    height: 26px;
    padding: 0;
    border-radius: 6px;
    border: 1px solid var(--ui-border);
    background: var(--ui-surface);
    color: var(--ui-text-secondary);
    cursor: pointer;
}}
.dtx .dtx-hist-btn svg {{
    width: 15px;
    height: 15px;
}}
.dtx .dtx-hist-btn:hover:not(:disabled) {{
    border-color: var(--ui-accent);
    color: var(--ui-accent);
}}
.dtx .dtx-hist-btn:disabled {{
    opacity: 0.4;
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
    color: var(--ui-on-accent);
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
    color: var(--ui-on-accent);
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
.dtx .dtx-chart-legend {{
    display: flex;
    flex-wrap: wrap;
    gap: 6px 16px;
    padding: 12px 16px 4px 56px;
}}
.dtx .dtx-legend-item {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 11px;
    color: var(--ui-text-secondary);
}}
.dtx .dtx-legend-swatch {{
    width: 11px;
    height: 11px;
    border-radius: 3px;
    flex: 0 0 auto;
}}
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
    color: var(--ui-on-accent);
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
    background: var(--ui-danger);
    border-color: var(--ui-danger);
}}
.dtx .dtx-btn.dtx-btn-stop:hover {{
    background: var(--ui-danger-hover);
    border-color: var(--ui-danger-hover);
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
.dtx .dtx-query-hl .dtx-tk-keyword {{ color: var(--ui-syntax-keyword) !important; }}
.dtx .dtx-query-hl .dtx-tk-variable {{ color: var(--ui-syntax-variable) !important; }}
.dtx .dtx-query-hl .dtx-tk-number {{ color: var(--ui-syntax-number) !important; }}
.dtx .dtx-query-hl .dtx-tk-virtual_column {{ color: var(--ui-syntax-virtual-column) !important; }}
.dtx .dtx-query-hl .dtx-tk-string {{ color: var(--ui-syntax-string) !important; }}
.dtx .dtx-query-hl .dtx-tk-operator,
.dtx .dtx-query-hl .dtx-tk-punctuation {{ color: var(--ui-syntax-operator) !important; }}
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
    background: var(--ui-danger-bg);
    border: 1px solid var(--ui-danger-border);
    color: var(--ui-danger-text);
    white-space: pre-wrap;
}}
.dtx.dtx-dark .dtx-error {{
    background: var(--ui-danger-bg);
    border-color: var(--ui-danger-border);
    color: var(--ui-danger-text);
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
.dtx td.dtx-hist-query {{
    max-width: 420px;
    color: var(--ui-text-secondary);
    vertical-align: top;
}}
.dtx td.dtx-hist-query pre {{
    margin: 0;
    max-height: 160px;
    overflow: auto;
    white-space: pre-wrap;
    word-break: break-word;
    font-family: var(--dtx-mono, ui-monospace, SFMono-Regular, Menlo, Consolas, monospace);
    font-size: 12px;
    line-height: 1.45;
    user-select: text;
    -webkit-user-select: text;
    cursor: text;
}}
.dtx .dtx-plan-seg {{ margin-right: 8px; }}
.dtx .dtx-dep-seg {{ margin-right: 8px; }}
.dtx table.dtx-plan-table td.dtx-plan-line {{
    color: var(--ui-text-secondary);
    vertical-align: top;
    white-space: nowrap;
}}
.dtx table.dtx-plan-table td.dtx-plan-line pre {{
    margin: 0;
    white-space: pre-wrap;
    word-break: break-word;
    font-family: var(--dtx-mono, ui-monospace, SFMono-Regular, Menlo, Consolas, monospace);
    font-size: 12px;
    line-height: 1.5;
    user-select: text;
    -webkit-user-select: text;
    cursor: text;
}}
.dtx .dtx-dep-tree {{
    padding: 6px 4px;
    user-select: none;
}}
.dtx .dtx-dep-row {{
    display: flex;
    align-items: center;
    gap: 6px;
    height: 26px;
    padding-right: 8px;
    border-radius: 6px;
    color: var(--ui-text-secondary);
    white-space: nowrap;
}}
.dtx .dtx-dep-row.dtx-dep-haschildren {{ cursor: pointer; }}
.dtx .dtx-dep-row:hover {{
    background: var(--ui-surface-2, rgba(127,127,127,0.10));
    color: var(--ui-text);
}}
.dtx .dtx-dep-caret {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 14px;
    height: 14px;
    flex: none;
    color: var(--ui-text-tertiary, var(--ui-text-secondary));
    transition: transform 120ms ease;
}}
.dtx .dtx-dep-caret.dtx-open {{ transform: rotate(90deg); }}
.dtx .dtx-dep-caret svg {{ width: 12px; height: 12px; }}
.dtx .dtx-dep-caret-spacer {{ width: 14px; height: 14px; flex: none; }}
.dtx .dtx-dep-icon {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 18px;
    height: 18px;
    flex: none;
    color: var(--ui-text-tertiary, var(--ui-text-secondary));
}}
.dtx .dtx-dep-icon svg {{ width: 16px; height: 16px; }}
.dtx .dtx-dep-label {{ font-size: 13px; }}
.dtx .dtx-dep-detail {{
    font-size: 12px;
    color: var(--ui-text-tertiary, var(--ui-text-secondary));
    margin-left: 6px;
    opacity: 0.85;
}}
.dtx .dtx-hist-download {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 30px;
    height: 30px;
    padding: 0;
    color: var(--ui-text-secondary);
    background: var(--ui-bg-secondary);
    border: 1px solid var(--ui-border-strong);
    border-radius: 6px;
    cursor: pointer;
    transition: background 120ms ease, border-color 120ms ease, color 120ms ease;
}}
.dtx .dtx-hist-download svg {{
    width: 17px;
    height: 17px;
}}
.dtx .dtx-hist-download:hover:not(:disabled) {{
    background: var(--ui-surface-2);
    border-color: var(--ui-accent);
    color: var(--ui-accent);
}}
.dtx .dtx-hist-download:disabled {{ opacity: 0.5; cursor: not-allowed; }}
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
    width: 260px;
    min-width: 180px;
    max-width: 70%;
    border-right: 1px solid var(--ui-border);
    background: var(--ui-bg-secondary);
    display: flex;
    flex-direction: column;
    transition: flex-basis 180ms ease, max-width 180ms ease,
        min-width 180ms ease;
    overflow: hidden;
}}
.dtx .dtx-sidebar.dtx-sidebar-resizing {{
    transition: none;
    user-select: none;
}}
.dtx .dtx-sidebar.dtx-sidebar-collapsed {{
    flex: 0 0 36px;
    min-width: 36px;
    max-width: 36px;
}}
.dtx .dtx-sidebar-resizer {{
    flex: 0 0 5px;
    cursor: col-resize;
    background: transparent;
    margin-left: -3px;
    position: relative;
    z-index: 2;
    transition: background 120ms ease;
}}
.dtx .dtx-sidebar-resizer:hover,
.dtx .dtx-sidebar-resizer.dtx-sidebar-resizing {{
    background: var(--ui-accent-soft);
}}
.dtx .dtx-sidebar.dtx-sidebar-collapsed + .dtx-sidebar-resizer {{
    display: none;
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
.dtx .dtx-sidebar-refresh,
.dtx .dtx-builder-toggle {{
    appearance: none;
    -webkit-appearance: none;
    flex: 0 0 auto;
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
.dtx .dtx-sidebar-refresh:hover,
.dtx .dtx-builder-toggle:hover {{
    background: var(--ui-surface-2);
    color: var(--ui-text);
}}
.dtx .dtx-sidebar-toggle svg,
.dtx .dtx-sidebar-refresh svg,
.dtx .dtx-builder-toggle svg {{
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
.dtx .dtx-context-menu {{
    position: fixed;
    z-index: 9999;
    min-width: 140px;
    padding: 4px;
    background: var(--ui-bg-solid);
    border: 1px solid var(--ui-border-strong);
    border-radius: 6px;
    box-shadow: 0 6px 20px rgba(0, 0, 0, 0.35);
    font-size: 13px;
    color: var(--ui-text);
}}
.dtx .dtx-context-menu-item {{
    padding: 6px 10px;
    border-radius: 4px;
    cursor: pointer;
    white-space: nowrap;
    color: var(--ui-text);
}}
.dtx .dtx-context-menu-item:hover {{
    background: var(--ui-bg-hover);
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
    color: var(--ui-text-tertiary);
}}
.dtx:not(.dtx-dark) .dtx-tree-leaf .dtx-tree-label:not(.dtx-hidden),
.dtx:not(.dtx-dark) .dtx-tree-node > .dtx-tree-label:not(.dtx-hidden),
.dtx:not(.dtx-dark) .dtx-tree-folder-header .dtx-tree-label:not(.dtx-hidden) {{
    color: var(--ui-text);
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
    width: 260px;
    min-width: 180px;
    max-width: 70%;
    border-right: 1px solid var(--ui-border);
    background: var(--ui-bg-secondary);
    display: flex;
    flex-direction: column;
    transition: flex-basis 180ms ease, max-width 180ms ease,
        min-width 180ms ease;
    overflow: hidden;
}}
.dtx .dtx-builder.dtx-builder-resizing {{
    transition: none;
    user-select: none;
}}
.dtx .dtx-builder.dtx-builder-hidden {{
    display: none;
}}
.dtx .dtx-builder.dtx-builder-collapsed {{
    flex: 0 0 36px;
    max-width: 36px;
    min-width: 36px;
}}
.dtx .dtx-builder-resizer {{
    flex: 0 0 5px;
    cursor: col-resize;
    background: transparent;
    margin-left: -3px;
    position: relative;
    z-index: 2;
    transition: background 120ms ease;
}}
.dtx .dtx-builder-resizer:hover,
.dtx .dtx-builder-resizer.dtx-builder-resizing {{
    background: var(--ui-accent-soft);
}}
.dtx .dtx-builder.dtx-builder-collapsed + .dtx-builder-resizer,
.dtx .dtx-builder.dtx-builder-hidden + .dtx-builder-resizer {{
    display: none;
}}
.dtx .dtx-builder.dtx-builder-collapsed .dtx-builder-title,
.dtx .dtx-builder.dtx-builder-collapsed .dtx-builder-content {{
    display: none;
}}
.dtx .dtx-builder.dtx-builder-collapsed .dtx-builder-header {{
    justify-content: center;
    padding: 10px 4px;
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
    background: var(--ui-bg-secondary);
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
}}
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
.dtx .dtx-builder-chip-order {{ flex-wrap: nowrap; }}
.dtx .dtx-builder-chip-order.dtx-chip-off .dtx-chip-label,
.dtx .dtx-builder-chip-order.dtx-chip-off .dtx-chip-icon {{
    opacity: 0.45;
}}
.dtx .dtx-order-toggle {{
    flex: 0 0 auto;
    width: 28px;
    height: 16px;
    padding: 0;
    border: 1px solid var(--ui-border);
    border-radius: 9px;
    background: var(--ui-bg);
    position: relative;
    cursor: pointer;
    transition: background 120ms ease, border-color 120ms ease;
}}
.dtx .dtx-order-toggle::after {{
    content: "";
    position: absolute;
    top: 1px;
    left: 1px;
    width: 12px;
    height: 12px;
    border-radius: 50%;
    background: var(--ui-text-tertiary);
    transition: transform 120ms ease, background 120ms ease;
}}
.dtx .dtx-order-toggle.dtx-on {{
    background: var(--ui-accent);
    border-color: var(--ui-accent);
}}
.dtx .dtx-order-toggle.dtx-on::after {{
    transform: translateX(12px);
    background: var(--ui-on-accent);
}}
.dtx .dtx-order-dir {{
    flex: 0 0 auto;
    display: inline-flex;
    align-items: center;
    gap: 3px;
    font-size: 10px;
    font-weight: 600;
    padding: 2px 6px;
    border: 1px solid var(--ui-border);
    border-radius: 5px;
    background: var(--ui-bg);
    color: var(--ui-text-secondary);
    cursor: pointer;
}}
.dtx .dtx-order-dir svg {{ width: 12px; height: 12px; }}
.dtx .dtx-order-dir:hover {{
    background: var(--ui-bg-hover);
    color: var(--ui-text);
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
    color: var(--ui-on-accent);
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
    play_icon = _UI_ICONS["play"].replace("`", "\\`")
    stop_icon = _UI_ICONS["stop"].replace("`", "\\`")
    refresh_icon = _UI_ICONS["refresh"].replace("`", "\\`")
    swap_icon = _UI_ICONS["swap"].replace("`", "\\`")
    sort_asc_icon = _UI_ICONS["sort_asc"].replace("`", "\\`")
    sort_desc_icon = _UI_ICONS["sort_desc"].replace("`", "\\`")
    panel_collapse_icon = _UI_ICONS["panel_collapse"].replace("`", "\\`")
    panel_expand_icon = _UI_ICONS["panel_expand"].replace("`", "\\`")
    builder_icon = _UI_ICONS["builder"].replace("`", "\\`")
    close_icon = _UI_ICONS["close"].replace("`", "\\`")
    # The DAX Formatter logo mark (the orange "formatted lines" glyph from
    # https://www.daxformatter.com/). Uses the SQLBI brand orange so it is
    # clearly visible in both light and dark themes.
    daxformat_icon = (
        '<svg viewBox="0 2.1 35 27.3" aria-hidden="true">'
        '<rect x="0" y="2.1" width="19.8" height="7.2" fill="#E14E37"/>'
        '<rect x="22.1" y="2.1" width="8.1" height="7.2" fill="#E14E37"/>'
        '<rect x="11.6" y="12.2" width="16.5" height="7.2" fill="#E14E37"/>'
        '<rect x="30.1" y="12.2" width="4.9" height="7.2" fill="#E14E37"/>'
        '<rect x="6.2" y="22.2" width="10.5" height="7.2" fill="#E14E37"/>'
        '</svg>'
    ).replace("`", "\\`")
    undo_icon = (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="M3 7 v5 h5"/>'
        '<path d="M3.5 12 a8 8 0 1 1 1.5 5"/>'
        '</svg>'
    ).replace("`", "\\`")
    redo_icon = (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="M21 7 v5 h-5"/>'
        '<path d="M20.5 12 a8 8 0 1 0 -1.5 5"/>'
        '</svg>'
    ).replace("`", "\\`")
    download_icon = (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="M12 3 v12"/>'
        '<path d="M7 10 l5 5 5-5"/>'
        '<path d="M4 20 h16"/>'
        '</svg>'
    ).replace("`", "\\`")
    cut_icon = (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<circle cx="6" cy="6" r="3"/>'
        '<circle cx="6" cy="18" r="3"/>'
        '<line x1="20" y1="4" x2="8.12" y2="15.88"/>'
        '<line x1="14.47" y1="14.48" x2="20" y2="20"/>'
        '<line x1="8.12" y1="8.12" x2="12" y2="12"/>'
        '</svg>'
    ).replace("`", "\\`")
    copy_icon = (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>'
        '<path d="M5 15 H4 a2 2 0 0 1 -2 -2 V4 a2 2 0 0 1 2 -2 h9 '
        'a2 2 0 0 1 2 2 v1"/>'
        '</svg>'
    ).replace("`", "\\`")
    paste_icon = (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="M16 4 h2 a2 2 0 0 1 2 2 v14 a2 2 0 0 1 -2 2 H6 '
        'a2 2 0 0 1 -2 -2 V6 a2 2 0 0 1 2 -2 h2"/>'
        '<rect x="8" y="2" width="8" height="4" rx="1" ry="1"/>'
        '</svg>'
    ).replace("`", "\\`")

    widget_js = r"""
function escapeHtml(s) {
    return String(s == null ? "" : s)
        .replace(/&/g, "&amp;").replace(/</g, "&lt;")
        .replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

function render({ model, el }) {
    const SUN_SVG = `__DTX_SUN__`;
    const MOON_SVG = `__DTX_MOON__`;
    const PLAY_SVG = `__DTX_PLAY__`;
    const STOP_SVG = `__DTX_STOP__`;
    const TABLE_SVG = `__DTX_TABLE__`;
    const CALC_GROUP_SVG = `__DTX_CALC_GROUP__`;
    const CALC_ITEM_SVG = `__DTX_CALC_ITEM__`;
    const COLUMN_SVG = `__DTX_COLUMN__`;
    const MEASURE_SVG = `__DTX_MEASURE__`;
    const HIERARCHY_SVG = `__DTX_HIERARCHY__`;
    const CARET_SVG = `__DTX_CARET__`;
    const FOLDER_SVG = `__DTX_FOLDER__`;
    const LEVEL_SVG = `__DTX_LEVEL__`;
    const REFRESH_SVG = `__DTX_REFRESH__`;
    const SWAP_SVG = `__DTX_SWAP__`;
    const SORT_ASC_SVG = `__DTX_SORT_ASC__`;
    const SORT_DESC_SVG = `__DTX_SORT_DESC__`;
    const PANEL_COLLAPSE_SVG = `__DTX_PANEL_COLLAPSE__`;
    const PANEL_EXPAND_SVG = `__DTX_PANEL_EXPAND__`;
    const BUILDER_SVG = `__DTX_BUILDER__`;
    const CLOSE_SVG = `__DTX_CLOSE__`;
    const DAXFORMAT_SVG = `__DTX_DAXFORMAT__`;
    const UNDO_SVG = `__DTX_UNDO__`;
    const REDO_SVG = `__DTX_REDO__`;
    const DOWNLOAD_SVG = `__DTX_DOWNLOAD__`;
    const CUT_SVG = `__DTX_CUT__`;
    const COPY_SVG = `__DTX_COPY__`;
    const PASTE_SVG = `__DTX_PASTE__`;

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

    // Drag handle that lets the user resize the model panel horizontally.
    // Placed immediately after the sidebar so it sits on its right edge and is
    // not clipped by the sidebar's own overflow:hidden.
    const sidebarResizer = document.createElement("div");
    sidebarResizer.className = "dtx-sidebar-resizer";
    sidebarResizer.title = "Drag to resize the model panel";
    sidebarResizer.setAttribute("aria-label", "Resize model panel");
    body.appendChild(sidebarResizer);

    // In-memory width of the model panel when expanded.
    let sidebarWidth = 260;
    const SIDEBAR_MIN_W = 180;

    function applySidebarWidth() {
        const collapsed = model.get("sidebar_collapsed") === true;
        if (collapsed) {
            // Let the collapsed CSS rule control the width.
            sidebar.style.flexBasis = "";
            sidebar.style.width = "";
            sidebar.style.maxWidth = "";
        } else {
            sidebar.style.flexBasis = sidebarWidth + "px";
            sidebar.style.width = sidebarWidth + "px";
            sidebar.style.maxWidth = sidebarWidth + "px";
        }
    }

    (function setupSidebarResize() {
        let startX = 0;
        let startW = 0;
        function onMove(e) {
            const maxW = Math.max(
                SIDEBAR_MIN_W, Math.floor(body.clientWidth * 0.7));
            let w = startW + (e.clientX - startX);
            w = Math.max(SIDEBAR_MIN_W, Math.min(maxW, w));
            sidebarWidth = w;
            applySidebarWidth();
        }
        function onUp() {
            sidebar.classList.remove("dtx-sidebar-resizing");
            sidebarResizer.classList.remove("dtx-sidebar-resizing");
            window.removeEventListener("mousemove", onMove);
            window.removeEventListener("mouseup", onUp);
        }
        sidebarResizer.addEventListener("mousedown", (e) => {
            if (model.get("sidebar_collapsed") === true) return;
            e.preventDefault();
            startX = e.clientX;
            startW = sidebar.getBoundingClientRect().width;
            sidebar.classList.add("dtx-sidebar-resizing");
            sidebarResizer.classList.add("dtx-sidebar-resizing");
            window.addEventListener("mousemove", onMove);
            window.addEventListener("mouseup", onUp);
        });
        // Double-click the handle to reset to the default width.
        sidebarResizer.addEventListener("dblclick", () => {
            sidebarWidth = 260;
            applySidebarWidth();
        });
    })();

    function renderSidebarChrome() {
        const collapsed = model.get("sidebar_collapsed") === true;
        sidebar.classList.toggle("dtx-sidebar-collapsed", collapsed);
        sidebarToggle.innerHTML = collapsed ? PANEL_EXPAND_SVG : PANEL_COLLAPSE_SVG;
        const label = collapsed ? "Show model panel" : "Hide model panel";
        sidebarToggle.title = label;
        sidebarToggle.setAttribute("aria-label", label);
        refreshBtn.classList.toggle("dtx-spinning", model.get("metadata_loading") === true);
        applySidebarWidth();
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

    // ---------- Lightweight right-click context menu ----------
    let ctxMenuEl = null;
    function hideContextMenu() {
        if (ctxMenuEl) { ctxMenuEl.remove(); ctxMenuEl = null; }
    }
    function showContextMenu(x, y, items) {
        hideContextMenu();
        const menu = document.createElement("div");
        menu.className = "dtx-context-menu";
        for (const it of items) {
            const mi = document.createElement("div");
            mi.className = "dtx-context-menu-item";
            mi.textContent = it.label;
            mi.addEventListener("click", (e) => {
                e.stopPropagation();
                hideContextMenu();
                try { it.action(); } catch (err) {}
            });
            menu.appendChild(mi);
        }
        menu.style.visibility = "hidden";
        root.appendChild(menu);
        // Reposition so the menu stays within the viewport.
        const rect = menu.getBoundingClientRect();
        let left = x, top = y;
        if (left + rect.width > window.innerWidth) {
            left = window.innerWidth - rect.width - 4;
        }
        if (top + rect.height > window.innerHeight) {
            top = window.innerHeight - rect.height - 4;
        }
        menu.style.left = Math.max(0, left) + "px";
        menu.style.top = Math.max(0, top) + "px";
        menu.style.visibility = "visible";
        ctxMenuEl = menu;
    }
    document.addEventListener("click", hideContextMenu);
    window.addEventListener("blur", hideContextMenu);
    window.addEventListener("scroll", hideContextMenu, true);

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
        if (meta && meta.kind === "measure") {
            leaf.addEventListener("contextmenu", (e) => {
                e.preventDefault();
                e.stopPropagation();
                showContextMenu(e.clientX, e.clientY, [
                    { label: "Define", action: () => defineMeasure(meta) },
                ]);
            });
        }
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
                    expression: it.expression,
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
    // Shown by default; can be hidden via the header "Query Builder" button.
    let builderVisible = true;
    let builderCollapsed = false;
    let builderFields = [];
    let builderFilters = [];
    let builderOrderBy = [];
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

    // Drag handle that lets the user resize the query builder pane
    // horizontally (to the right). Placed immediately after the builder pane
    // so it sits on its right edge and is not clipped by overflow:hidden.
    const builderResizer = document.createElement("div");
    builderResizer.className = "dtx-builder-resizer";
    builderResizer.title = "Drag to resize the query builder";
    builderResizer.setAttribute("aria-label", "Resize query builder");
    body.insertBefore(builderResizer, main);

    // In-memory width of the query builder pane when expanded.
    let builderWidth = 260;
    const BUILDER_MIN_W = 180;

    function applyBuilderWidth() {
        const inactive = builderCollapsed || !builderVisible
            || model.get("dataset_chosen") !== true;
        if (inactive) {
            // Let the collapsed/hidden CSS rules control the width.
            builderPane.style.flexBasis = "";
            builderPane.style.width = "";
            builderPane.style.maxWidth = "";
        } else {
            builderPane.style.flexBasis = builderWidth + "px";
            builderPane.style.width = builderWidth + "px";
            builderPane.style.maxWidth = builderWidth + "px";
        }
    }

    (function setupBuilderResize() {
        let startX = 0;
        let startW = 0;
        function onMove(e) {
            const maxW = Math.max(
                BUILDER_MIN_W, Math.floor(body.clientWidth * 0.7));
            let w = startW + (e.clientX - startX);
            w = Math.max(BUILDER_MIN_W, Math.min(maxW, w));
            builderWidth = w;
            applyBuilderWidth();
        }
        function onUp() {
            builderPane.classList.remove("dtx-builder-resizing");
            builderResizer.classList.remove("dtx-builder-resizing");
            window.removeEventListener("mousemove", onMove);
            window.removeEventListener("mouseup", onUp);
        }
        builderResizer.addEventListener("mousedown", (e) => {
            if (builderCollapsed || !builderVisible) return;
            e.preventDefault();
            startX = e.clientX;
            startW = builderPane.getBoundingClientRect().width;
            builderPane.classList.add("dtx-builder-resizing");
            builderResizer.classList.add("dtx-builder-resizing");
            window.addEventListener("mousemove", onMove);
            window.addEventListener("mouseup", onUp);
        });
        // Double-click the handle to reset to the default width.
        builderResizer.addEventListener("dblclick", () => {
            builderWidth = 260;
            applyBuilderWidth();
        });
    })();

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

    const orderBySection = document.createElement("div");
    orderBySection.className = "dtx-builder-section";
    const orderByLabel = document.createElement("div");
    orderByLabel.className = "dtx-builder-section-label";
    orderByLabel.textContent = "Order By";
    const orderByZone = document.createElement("div");
    orderByZone.className = "dtx-builder-zone dtx-builder-orderby";
    orderBySection.appendChild(orderByLabel);
    orderBySection.appendChild(orderByZone);
    builderContent.appendChild(orderBySection);

    const builderFooter = document.createElement("div");
    builderFooter.className = "dtx-builder-footer";
    const clearBtn = document.createElement("button");
    clearBtn.type = "button";
    clearBtn.className = "dtx-builder-clear";
    clearBtn.textContent = "Clear";
    clearBtn.addEventListener("click", () => {
        builderFields = [];
        builderFilters = [];
        builderOrderBy = [];
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

    // The Order By zone only allows reordering its existing chips (its items
    // are synced from the Columns & Measures pane, not dropped from outside).
    function setupReorderOnlyZone(zone, getList, zoneName) {
        zone.addEventListener("dragover", (e) => {
            if (!builderReorder || builderReorder.zone !== zoneName) return;
            e.preventDefault();
            e.dataTransfer.dropEffect = "move";
            zone.classList.add("dtx-drop-over");
        });
        zone.addEventListener("dragleave", (e) => {
            if (!zone.contains(e.relatedTarget)) {
                zone.classList.remove("dtx-drop-over");
            }
        });
        zone.addEventListener("drop", (e) => {
            zone.classList.remove("dtx-drop-over");
            if (!builderReorder || builderReorder.zone !== zoneName) return;
            e.preventDefault();
            const list = getList();
            const idx = zoneIndexFromEvent(zone, e);
            const from = list.findIndex(x => x.id === builderReorder.id);
            if (from === -1) return;
            const moved = list.splice(from, 1)[0];
            let insert = idx;
            if (from < idx) insert = idx - 1;
            list.splice(insert, 0, moved);
            renderBuilderZones();
        });
    }
    setupReorderOnlyZone(orderByZone, () => builderOrderBy, "orderby");

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

    // Keep the Order By pane in sync with the Columns & Measures pane:
    // drop entries whose field was removed, and append newly added fields
    // (defaulting to off/ascending) while preserving the user's order and
    // per-item toggle/direction state.
    function syncOrderBy() {
        builderOrderBy = builderOrderBy.filter(o =>
            builderFields.some(f => f.kind === o.kind && f.ref === o.ref));
        for (const f of builderFields) {
            const exists = builderOrderBy.some(
                o => o.kind === f.kind && o.ref === f.ref);
            if (!exists) {
                builderOrderBy.push({
                    id: "ob" + (++qbSeq),
                    kind: f.kind, table: f.table, name: f.name,
                    data_type: f.data_type, ref: f.ref,
                    enabled: false, dir: "asc",
                });
            }
        }
    }

    function makeOrderByChip(f) {
        const chip = document.createElement("div");
        chip.className = "dtx-builder-chip dtx-builder-chip-order";
        if (!f.enabled) chip.classList.add("dtx-chip-off");

        const tog = document.createElement("button");
        tog.type = "button";
        tog.className = "dtx-order-toggle";
        function renderTog() {
            tog.classList.toggle("dtx-on", !!f.enabled);
            chip.classList.toggle("dtx-chip-off", !f.enabled);
            tog.title = f.enabled
                ? "Included in ORDER BY (click to exclude)"
                : "Excluded from ORDER BY (click to include)";
            tog.setAttribute("aria-label", tog.title);
        }
        tog.addEventListener("click", () => {
            f.enabled = !f.enabled;
            renderTog();
        });
        renderTog();

        const ic = document.createElement("span");
        ic.className = "dtx-chip-icon";
        ic.innerHTML = qbIcon(f.kind);
        const label = document.createElement("span");
        label.className = "dtx-chip-label";
        label.textContent = qbDisplayName(f);
        label.title = qbDisplayName(f);

        const dir = document.createElement("button");
        dir.type = "button";
        dir.className = "dtx-order-dir";
        function renderDir() {
            const asc = f.dir !== "desc";
            dir.innerHTML = (asc ? SORT_ASC_SVG : SORT_DESC_SVG)
                + '<span>' + (asc ? "A-Z" : "Z-A") + '</span>';
            dir.title = asc
                ? "Ascending (A-Z) \u2014 click for descending"
                : "Descending (Z-A) \u2014 click for ascending";
            dir.setAttribute("aria-label", dir.title);
        }
        dir.addEventListener("click", () => {
            f.dir = (f.dir === "desc") ? "asc" : "desc";
            renderDir();
        });
        renderDir();

        chip.appendChild(tog);
        chip.appendChild(ic);
        chip.appendChild(label);
        chip.appendChild(dir);
        attachChipReorder(chip, f, "orderby");
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
        syncOrderBy();
        orderByZone.innerHTML = "";
        if (!builderOrderBy.length) {
            const ph = document.createElement("div");
            ph.className = "dtx-builder-placeholder";
            ph.textContent =
                "Add columns or measures above to set the sort order";
            orderByZone.appendChild(ph);
        } else {
            for (const f of builderOrderBy) {
                orderByZone.appendChild(makeOrderByChip(f));
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
            order_by: builderOrderBy.map(o => ({
                kind: o.kind, table: o.table, name: o.name,
                data_type: o.data_type, ref: o.ref,
                enabled: !!o.enabled, dir: o.dir || "asc",
            })),
        };
        model.set("query_builder_state", JSON.stringify(state));
        model.set("error_message", "");
        model.set("build_query_trigger",
            (model.get("build_query_trigger") || 0) + 1);
        model.save_changes();
    }

    function renderBuilderChrome() {
        // The query builder is only meaningful once a semantic model is
        // loaded; hide it (and its header toggle) entirely otherwise.
        const chosen = model.get("dataset_chosen") === true;
        builderShowBtn.style.display = chosen ? "" : "none";
        builderPane.classList.toggle("dtx-builder-hidden", !chosen || !builderVisible);
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
        applyBuilderWidth();
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
        // Only show the spinner next to "Use model" while the model metadata
        // is loading (i.e. after "Use model" is clicked, when a dataset is
        // selected). During workspace selection the dataset list is loading
        // and the spinner belongs in the dataset dropdown only.
        pickerSpin.textContent = (loading && curDs) ? "Loading…" : "";
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

    const fmtBtn = document.createElement("button");
    fmtBtn.type = "button";
    fmtBtn.className = "dtx-fmt-btn";
    fmtBtn.innerHTML = DAXFORMAT_SVG;
    fmtBtn.title = "Format the DAX query using DAX Formatter by SQLBI";
    fmtBtn.setAttribute("aria-label", "Format DAX with DAX Formatter");
    qTitleGroup.appendChild(fmtBtn);
    fmtBtn.addEventListener("click", () => {
        if (fmtBtn.disabled) return;
        model.set("dax_query", textarea.value);
        model.set("error_message", "");
        model.set("format_query_trigger",
            (model.get("format_query_trigger") || 0) + 1);
        model.save_changes();
    });
    function renderFmtBtn() {
        const loading = model.get("format_loading") === true;
        const hasText = String(textarea.value || "").trim().length > 0;
        fmtBtn.disabled = loading || !hasText;
        fmtBtn.classList.toggle("dtx-fmt-loading", loading);
    }

    const undoBtn = document.createElement("button");
    undoBtn.type = "button";
    undoBtn.className = "dtx-hist-btn";
    undoBtn.innerHTML = UNDO_SVG;
    undoBtn.title = "Undo (Ctrl/Cmd+Z)";
    undoBtn.setAttribute("aria-label", "Undo");
    qTitleGroup.appendChild(undoBtn);

    const redoBtn = document.createElement("button");
    redoBtn.type = "button";
    redoBtn.className = "dtx-hist-btn";
    redoBtn.innerHTML = REDO_SVG;
    redoBtn.title = "Redo (Ctrl/Cmd+Shift+Z or Ctrl/Cmd+Y)";
    redoBtn.setAttribute("aria-label", "Redo");
    qTitleGroup.appendChild(redoBtn);

    const cutBtn = document.createElement("button");
    cutBtn.type = "button";
    cutBtn.className = "dtx-hist-btn";
    cutBtn.innerHTML = CUT_SVG;
    cutBtn.title = "Cut selected DAX text (Ctrl/Cmd+X)";
    cutBtn.setAttribute("aria-label", "Cut");
    qTitleGroup.appendChild(cutBtn);

    const copyBtn = document.createElement("button");
    copyBtn.type = "button";
    copyBtn.className = "dtx-hist-btn";
    copyBtn.innerHTML = COPY_SVG;
    copyBtn.title = "Copy selected DAX text (Ctrl/Cmd+C)";
    copyBtn.setAttribute("aria-label", "Copy");
    qTitleGroup.appendChild(copyBtn);

    const pasteBtn = document.createElement("button");
    pasteBtn.type = "button";
    pasteBtn.className = "dtx-hist-btn";
    pasteBtn.innerHTML = PASTE_SVG;
    pasteBtn.title = "Paste text at the cursor (Ctrl/Cmd+V)";
    pasteBtn.setAttribute("aria-label", "Paste");
    qTitleGroup.appendChild(pasteBtn);

    // Cut/copy/paste operate on the DAX query textarea's current selection.
    function writeClipboard(text) {
        try {
            if (navigator.clipboard && navigator.clipboard.writeText) {
                return navigator.clipboard.writeText(text);
            }
        } catch (e) {}
        // Fallback: use execCommand via the textarea itself.
        try {
            textarea.focus();
            document.execCommand("copy");
        } catch (e) {}
        return Promise.resolve();
    }
    function doCopy() {
        const s = textarea.selectionStart || 0;
        const e = textarea.selectionEnd || 0;
        if (e <= s) return;
        writeClipboard(textarea.value.slice(s, e));
        textarea.focus();
    }
    function doCut() {
        const s = textarea.selectionStart || 0;
        const e = textarea.selectionEnd || 0;
        if (e <= s) return;
        const sel = textarea.value.slice(s, e);
        writeClipboard(sel);
        textarea.value = textarea.value.slice(0, s) + textarea.value.slice(e);
        textarea.selectionStart = textarea.selectionEnd = s;
        textarea.focus();
        commitHistory(textarea.value, false);
        model.set("dax_query", textarea.value);
        model.save_changes();
        renderHighlight();
        renderFmtBtn();
    }
    function doPaste() {
        const insert = (text) => {
            if (text == null || text === "") { textarea.focus(); return; }
            insertAtCursor(String(text));
            renderFmtBtn();
        };
        try {
            if (navigator.clipboard && navigator.clipboard.readText) {
                navigator.clipboard.readText().then(insert).catch(() => {
                    textarea.focus();
                });
                return;
            }
        } catch (e) {}
        // Fallback: rely on the textarea's native paste.
        textarea.focus();
        document.execCommand("paste");
    }
    cutBtn.addEventListener("click", () => doCut());
    copyBtn.addEventListener("click", () => doCopy());
    pasteBtn.addEventListener("click", () => doPaste());

    undoBtn.addEventListener("click", () => doUndo());
    redoBtn.addEventListener("click", () => doRedo());

    // Cut/Copy require a non-empty selection in the DAX query textarea.
    function renderClipBtns() {
        let hasSelection = false;
        try {
            hasSelection = (textarea.selectionEnd || 0)
                > (textarea.selectionStart || 0);
        } catch (e) {}
        cutBtn.disabled = !hasSelection;
        copyBtn.disabled = !hasSelection;
    }

    function renderHistBtns() {
        undoBtn.disabled = undoStack.length === 0;
        redoBtn.disabled = redoStack.length === 0;
        renderClipBtns();
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

    // ---------- Undo / redo history (DAX query pane only) ----------
    // A dedicated history stack for the editor text. A custom stack is used
    // (instead of the textarea's native undo) so programmatic edits such as
    // drag-and-drop, "Define" and "Format" are also undoable.
    const undoStack = [];
    const redoStack = [];
    const HISTORY_LIMIT = 200;
    let histPresent = { value: textarea.value, s: 0, e: 0 };
    let lastTypingTs = 0;
    let lastEditWasTyping = false;

    // Record a change to the editor text in the history. ``coalesce`` merges
    // rapid consecutive typing into a single undo entry.
    function commitHistory(newValue, coalesce) {
        if (newValue === histPresent.value) return;
        const now = Date.now();
        const canCoalesce = coalesce && lastEditWasTyping
            && (now - lastTypingTs < 500) && undoStack.length > 0;
        if (!canCoalesce) {
            undoStack.push({
                value: histPresent.value, s: histPresent.s, e: histPresent.e });
            if (undoStack.length > HISTORY_LIMIT) undoStack.shift();
        }
        histPresent = {
            value: newValue,
            s: textarea.selectionStart || 0,
            e: textarea.selectionEnd || 0,
        };
        redoStack.length = 0;
        lastTypingTs = now;
        lastEditWasTyping = !!coalesce;
        renderHistBtns();
    }

    function applyHistoryState(st) {
        textarea.value = st.value;
        try {
            textarea.selectionStart = st.s;
            textarea.selectionEnd = st.e;
        } catch (e) {}
        histPresent = { value: st.value, s: st.s, e: st.e };
        lastEditWasTyping = false;
        model.set("dax_query", textarea.value);
        model.save_changes();
        renderHighlight();
        renderFmtBtn();
        renderHistBtns();
        textarea.focus();
    }

    function doUndo() {
        if (!undoStack.length) return;
        redoStack.push({
            value: histPresent.value,
            s: textarea.selectionStart || 0,
            e: textarea.selectionEnd || 0,
        });
        applyHistoryState(undoStack.pop());
    }

    function doRedo() {
        if (!redoStack.length) return;
        undoStack.push({
            value: histPresent.value,
            s: textarea.selectionStart || 0,
            e: textarea.selectionEnd || 0,
        });
        applyHistoryState(redoStack.pop());
    }

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
                + 'from the left, or use the Query Builder.'
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
        commitHistory(textarea.value, true);
        model.set("dax_query", textarea.value);
        model.save_changes();
        renderHighlight();
        renderFmtBtn();
    });
    // Keep the Cut/Copy buttons in sync with the current text selection.
    ["select", "keyup", "mouseup", "focus", "blur", "input"].forEach((evt) => {
        textarea.addEventListener(evt, renderClipBtns);
    });
    document.addEventListener("selectionchange", () => {
        if (document.activeElement === textarea) renderClipBtns();
    });
    textarea.addEventListener("scroll", () => {
        hl.scrollTop = textarea.scrollTop;
        hl.scrollLeft = textarea.scrollLeft;
    });
    // Ctrl/Cmd+Enter to run; Ctrl/Cmd+Z / Ctrl/Cmd+Shift+Z / Ctrl/Cmd+Y for
    // undo/redo (handled by the editor's own history stack).
    textarea.addEventListener("keydown", (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
            e.preventDefault();
            runBtn.click();
            return;
        }
        if (e.ctrlKey || e.metaKey) {
            const k = e.key.toLowerCase();
            if (k === "z" && !e.shiftKey) {
                e.preventDefault();
                doUndo();
                return;
            }
            if ((k === "z" && e.shiftKey) || k === "y") {
                e.preventDefault();
                doRedo();
                return;
            }
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
        commitHistory(textarea.value, false);
        model.set("dax_query", textarea.value);
        model.save_changes();
        renderHighlight();
    }

    // Prepend a `DEFINE MEASURE` block for the given measure above the
    // existing query text. If a DEFINE block already exists, the new
    // measure line is added under it instead of duplicating the keyword.
    function defineMeasure(meta) {
        const table = meta.table || "";
        const name = meta.name || "";
        const expr = String(meta.expression == null ? "" : meta.expression).trim();
        const ref = (table ? daxTableRef(table) : "")
            + "[" + String(name).replace(/\]/g, "]]") + "]";
        const measureLine = "MEASURE " + ref + " = " + expr;
        const existing = textarea.value;
        const lead = /^(\s*)DEFINE\b[^\n]*/i.exec(existing);
        let newText;
        if (lead) {
            const idx = lead.index + lead[0].length;
            newText = existing.slice(0, idx) + "\n" + measureLine
                + existing.slice(idx);
        } else {
            newText = "DEFINE\n" + measureLine + "\n\n" + existing;
        }
        textarea.value = newText;
        commitHistory(textarea.value, false);
        model.set("dax_query", textarea.value);
        model.save_changes();
        renderHighlight();
        textarea.focus();
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
    const segHistory = document.createElement("button");
    segHistory.type = "button";
    segHistory.className = "dtx-seg-btn";
    segHistory.textContent = "Trace history";
    const segQueryPlan = document.createElement("button");
    segQueryPlan.type = "button";
    segQueryPlan.className = "dtx-seg-btn";
    segQueryPlan.textContent = "DAX query plan";
    const segDependencies = document.createElement("button");
    segDependencies.type = "button";
    segDependencies.className = "dtx-seg-btn";
    segDependencies.textContent = "Query dependencies";
    seg.appendChild(segTrace);
    seg.appendChild(segResult);
    seg.appendChild(segQueryPlan);
    seg.appendChild(segChart);
    seg.appendChild(segHistory);
    seg.appendChild(segDependencies);
    viewToolbar.appendChild(seg);

    // ---------- DAX Query Plan toggle (Logical / Physical) ----------
    const planSeg = document.createElement("div");
    planSeg.className = "dtx-seg dtx-plan-seg";
    planSeg.style.display = "none";
    const planLogicalBtn = document.createElement("button");
    planLogicalBtn.type = "button";
    planLogicalBtn.className = "dtx-seg-btn";
    planLogicalBtn.textContent = "Logical Query Plan";
    const planPhysicalBtn = document.createElement("button");
    planPhysicalBtn.type = "button";
    planPhysicalBtn.className = "dtx-seg-btn";
    planPhysicalBtn.textContent = "Physical Query Plan";
    planSeg.appendChild(planLogicalBtn);
    planSeg.appendChild(planPhysicalBtn);
    // Show the Logical/Physical toggle to the left of the tab group.
    viewToolbar.insertBefore(planSeg, seg);
    planLogicalBtn.addEventListener("click", () => {
        model.set("query_plan_type", "Logical");
        model.save_changes();
    });
    planPhysicalBtn.addEventListener("click", () => {
        model.set("query_plan_type", "Physical");
        model.save_changes();
    });

    // ---------- Query Dependencies toggle (Tree / Columns) ----------
    const depSeg = document.createElement("div");
    depSeg.className = "dtx-seg dtx-dep-seg";
    depSeg.style.display = "none";
    const depTreeBtn = document.createElement("button");
    depTreeBtn.type = "button";
    depTreeBtn.className = "dtx-seg-btn";
    depTreeBtn.textContent = "Tree";
    const depColumnsBtn = document.createElement("button");
    depColumnsBtn.type = "button";
    depColumnsBtn.className = "dtx-seg-btn";
    depColumnsBtn.textContent = "Columns";
    depSeg.appendChild(depTreeBtn);
    depSeg.appendChild(depColumnsBtn);
    // Show the Tree/Columns toggle to the left of the tab group.
    viewToolbar.insertBefore(depSeg, seg);
    depTreeBtn.addEventListener("click", () => {
        model.set("dependency_view", "tree");
        model.save_changes();
    });
    depColumnsBtn.addEventListener("click", () => {
        model.set("dependency_view", "columns");
        model.save_changes();
    });

    const histDownloadBtn = document.createElement("button");
    histDownloadBtn.type = "button";
    histDownloadBtn.className = "dtx-hist-download";
    histDownloadBtn.innerHTML = DOWNLOAD_SVG;
    histDownloadBtn.title = "Download the trace history as an Excel file";
    histDownloadBtn.setAttribute("aria-label", "Download trace history as Excel");
    histDownloadBtn.style.display = "none";
    viewToolbar.appendChild(histDownloadBtn);
    histDownloadBtn.addEventListener("click", () => {
        const hist = model.get("trace_history") || [];
        if (!hist.length) return;
        model.set("download_history_trigger", (model.get("download_history_trigger") || 0) + 1);
        model.save_changes();
    });
    // Tracks the DAX query text that the currently displayed dependency tree
    // was computed for, so dependencies are only recomputed when it changes.
    let lastDepQuery = null;
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
    segHistory.addEventListener("click", () => {
        model.set("view_mode", "history");
        model.save_changes();
    });
    segQueryPlan.addEventListener("click", () => {
        model.set("view_mode", "queryplan");
        model.save_changes();
    });
    segDependencies.addEventListener("click", () => {
        model.set("view_mode", "dependencies");
        // Only recompute dependencies when the DAX query text has changed
        // since the last computation; otherwise reuse the existing tree.
        const curQuery = model.get("dax_query") || "";
        if (curQuery !== lastDepQuery) {
            lastDepQuery = curQuery;
            model.set("dependencies_trigger", (model.get("dependencies_trigger") || 0) + 1);
        }
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
        segHistory.classList.toggle("dtx-seg-btn-on", mode === "history");
        segQueryPlan.classList.toggle("dtx-seg-btn-on", mode === "queryplan");
        segDependencies.classList.toggle("dtx-seg-btn-on", mode === "dependencies");
        const elig = chartEligibility();
        segChart.disabled = !elig.ok;
        segChart.title = elig.ok ? "Show simple chart of the result" : elig.reason;
        const hist = model.get("trace_history") || [];
        histDownloadBtn.style.display = (mode === "history") ? "" : "none";
        histDownloadBtn.disabled = !hist.length;
        // Logical/Physical toggle is only relevant on the DAX Query Plan tab.
        const planType = model.get("query_plan_type") || "Logical";
        planSeg.style.display = (mode === "queryplan") ? "" : "none";
        planLogicalBtn.classList.toggle("dtx-seg-btn-on", planType === "Logical");
        planPhysicalBtn.classList.toggle("dtx-seg-btn-on", planType === "Physical");
        // Tree/Columns toggle is only relevant on the Query Dependencies tab.
        const depView = model.get("dependency_view") || "tree";
        depSeg.style.display = (mode === "dependencies") ? "" : "none";
        depTreeBtn.classList.toggle("dtx-seg-btn-on", depView === "tree");
        depColumnsBtn.classList.toggle("dtx-seg-btn-on", depView === "columns");
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

    function renderHistoryTable() {
        const hist = model.get("trace_history") || [];
        const fmt = (n) => Number(n).toLocaleString();
        if (!hist.length) {
            tableWrap.innerHTML = `<table><tbody><tr><td class="dtx-empty">No queries have been executed in this session yet.</td></tr></tbody></table>`;
            return;
        }
        const body = hist.map(h => {
            const q = String(h.dax_query || "");
            return `<tr>
                <td class="dtx-hist-query"><pre>${escapeHtml(q)}</pre></td>
                <td>${escapeHtml(String(h.start_time || ""))}</td>
                <td>${escapeHtml(String(h.end_time || ""))}</td>
                <td class="dtx-num">${escapeHtml(fmt(h.rows))}</td>
                <td class="dtx-num">${escapeHtml(fmt(h.duration))}</td>
                <td class="dtx-num">${escapeHtml(fmt(h.cpu))}</td>
                <td class="dtx-num">${escapeHtml(fmt(h.fe_duration))}</td>
                <td class="dtx-num">${escapeHtml(fmt(h.se_duration))}</td>
                <td>${escapeHtml(String(h.dataset_name || ""))}</td>
                <td>${escapeHtml(String(h.workspace_name || ""))}</td>
                <td>${escapeHtml(String(h.impersonation_type || "None"))}</td>
                <td>${escapeHtml(String(h.impersonation || "None"))}</td>
            </tr>`;
        }).join("");
        tableWrap.innerHTML = `
            <table>
                <thead><tr>
                    <th>DAX Query</th>
                    <th>Start Time (UTC)</th>
                    <th>End Time (UTC)</th>
                    <th style="text-align:right">Rows</th>
                    <th style="text-align:right">Duration (ms)</th>
                    <th style="text-align:right">CPU (ms)</th>
                    <th style="text-align:right">FE (ms)</th>
                    <th style="text-align:right">SE (ms)</th>
                    <th>Semantic Model</th>
                    <th>Workspace</th>
                    <th>Impersonation Type</th>
                    <th>Impersonation</th>
                </tr></thead>
                <tbody>${body}</tbody>
            </table>`;
    }

    function renderQueryPlanTable() {
        const rows = model.get("query_plan_rows") || [];
        const planType = model.get("query_plan_type") || "Logical";
        const matching = rows.filter(r => String(r.plan_type || "") === planType);
        if (!rows.length) {
            tableWrap.innerHTML = `<table><tbody><tr><td class="dtx-empty">No DAX query plan captured. Run a query to capture its query plan.</td></tr></tbody></table>`;
            return;
        }
        if (!matching.length) {
            tableWrap.innerHTML = `<table><tbody><tr><td class="dtx-empty">No ${escapeHtml(planType)} Query Plan was captured for the last query.</td></tr></tbody></table>`;
            return;
        }
        const body = matching.map(r => {
            const lines = String(r.text || "").split(/\r?\n/);
            return lines.map(line => {
                // Preserve the plan's indentation while keeping it readable.
                const indentMatch = line.match(/^(\s*)/);
                const indent = indentMatch ? indentMatch[1].length : 0;
                return `<tr><td class="dtx-plan-line" style="padding-left:${8 + indent * 8}px"><pre>${escapeHtml(line.trimStart())}</pre></td></tr>`;
            }).join("");
        }).join("");
        tableWrap.innerHTML = `
            <table class="dtx-plan-table">
                <thead><tr><th>${escapeHtml(planType)} Query Plan</th></tr></thead>
                <tbody>${body}</tbody>
            </table>`;
    }

    // Collapsed-state set for the query-dependencies tree (keyed by node path).
    const depCollapsed = new Set();

    function depIcon(kind) {
        switch (kind) {
            case "model": return TABLE_SVG;
            case "group": return FOLDER_SVG;
            case "table": return TABLE_SVG;
            case "column": return COLUMN_SVG;
            case "measure": return MEASURE_SVG;
            case "hierarchy": return HIERARCHY_SVG;
            case "calc_group": return CALC_GROUP_SVG;
            case "relationship": return SWAP_SVG;
            default: return COLUMN_SVG;
        }
    }

    function renderDepNode(node, path, depth) {
        const hasChildren = !!(node.children && node.children.length);
        const collapsed = depCollapsed.has(path);
        const caret = hasChildren
            ? `<span class="dtx-dep-caret${collapsed ? "" : " dtx-open"}">${CARET_SVG}</span>`
            : `<span class="dtx-dep-caret-spacer"></span>`;
        const detail = node.detail
            ? `<span class="dtx-dep-detail">${escapeHtml(String(node.detail))}</span>`
            : "";
        const pad = 8 + depth * 16;
        let html = `<div class="dtx-dep-row${hasChildren ? " dtx-dep-haschildren" : ""}" `
            + `data-path="${escapeHtml(path)}" data-haschildren="${hasChildren ? "1" : "0"}" `
            + `style="padding-left:${pad}px">`
            + caret
            + `<span class="dtx-dep-icon">${depIcon(node.kind)}</span>`
            + `<span class="dtx-dep-label">${escapeHtml(String(node.label || ""))}</span>`
            + detail
            + `</div>`;
        if (hasChildren && !collapsed) {
            html += node.children
                .map((c, i) => renderDepNode(c, path + "/" + i, depth + 1))
                .join("");
        }
        return html;
    }

    function renderDependencyColumns() {
        const cols = model.get("dependency_columns") || [];
        if (!cols.length) {
            tableWrap.innerHTML = `<div class="dtx-dep-tree"><div class="dtx-empty">No columns referenced. Run this on a non-empty DAX query.</div></div>`;
            return;
        }
        const body = cols.map(c => {
            return `<tr>`
                + `<td>${escapeHtml(String(c.table || ""))}</td>`
                + `<td>${escapeHtml(String(c.column || ""))}</td>`
                + `</tr>`;
        }).join("");
        tableWrap.innerHTML = `
            <table class="dtx-dep-col-table">
                <thead><tr><th>Table Name</th><th>Column Name</th></tr></thead>
                <tbody>${body}</tbody>
            </table>`;
    }

    function renderDependenciesTable() {
        if (model.get("dependencies_loading") === true) {
            tableWrap.innerHTML = `<div class="dtx-dep-tree"><div class="dtx-empty">Computing query dependencies&hellip;</div></div>`;
            return;
        }
        const view = model.get("dependency_view") || "tree";
        if (view === "columns") {
            renderDependencyColumns();
            return;
        }
        const tree = model.get("dependency_tree") || [];
        if (!tree.length) {
            tableWrap.innerHTML = `<div class="dtx-dep-tree"><div class="dtx-empty">No dependencies found. Run this on a non-empty DAX query.</div></div>`;
            return;
        }
        const html = tree.map((n, i) => renderDepNode(n, String(i), 0)).join("");
        tableWrap.innerHTML = `<div class="dtx-dep-tree">${html}</div>`;
        tableWrap.querySelectorAll(".dtx-dep-row[data-haschildren='1']").forEach(row => {
            row.addEventListener("click", () => {
                const p = row.getAttribute("data-path");
                if (depCollapsed.has(p)) depCollapsed.delete(p);
                else depCollapsed.add(p);
                renderDependenciesTable();
            });
        });
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

        // Modern, clean qualitative color palette for multi-series / stacked bars.
        const PALETTE = [
            "#4f8cff", "#34d399", "#fbbf24", "#f472b6", "#a78bfa",
            "#22d3ee", "#fb7185", "#84cc16", "#f59e0b", "#38bdf8",
        ];

        // A "category" column is one whose values are strings (not numbers/bools).
        const isCategoryCol = (i) =>
            rows.some(r => typeof r[i] === "string") &&
            rows.every(r => r[i] === null || typeof r[i] === "string");

        // Single result row: numeric columns become the values to chart. If a
        // string column exists, its value is used as the X-axis category label;
        // multiple numeric columns are drawn as a stacked bar with a legend.
        const singleRow = rows.length === 1 && numericIdxs.length >= 1;
        const stackedMode = singleRow && numericIdxs.length >= 2;

        let data;
        let legendSegments = null;
        if (singleRow) {
            chartControls.style.display = "none";
            const catIdx = cols.findIndex((_, i) => isCategoryCol(i));
            const catLabel = catIdx >= 0 ? String(rows[0][catIdx] ?? "") : "";
            if (stackedMode) {
                const segments = numericIdxs.map((i, k) => ({
                    name: cols[i],
                    value: typeof rows[0][i] === "number" ? rows[0][i] : 0,
                    color: PALETTE[k % PALETTE.length],
                }));
                legendSegments = segments;
                data = [{ label: catLabel || "Total", segments }];
            } else {
                const i = numericIdxs[0];
                data = [{
                    label: catLabel || cols[i],
                    value: typeof rows[0][i] === "number" ? rows[0][i] : 0,
                }];
            }
        } else {
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
            data = rows.map((r, i) => ({
                label: xIdx === -1 ? String(i + 1) : (r[xIdx] == null ? "" : String(r[xIdx])),
                value: typeof r[yIdx] === "number" ? r[yIdx] : 0,
            }));
        }

        // SVG bar chart.
        const n = data.length;
        const barWidth = data.length === 1 ? 64 : 28;
        const barGap = 8;
        const leftPad = 56;
        const rightPad = 16;
        const topPad = 12;
        const bottomPad = 56;
        const plotWidth = Math.max(n * (barWidth + barGap), 200);
        const width = leftPad + plotWidth + rightPad;
        const height = 280;
        const plotHeight = height - topPad - bottomPad;
        const totalOf = d => d.segments ? d.segments.reduce((s, sg) => s + sg.value, 0) : d.value;
        const values = data.map(totalOf);
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
        // Compact axis labels: abbreviate large magnitudes (e.g. 1B, 10M, 100K).
        const fmtAxis = v => {
            const n = Number(v);
            const abs = Math.abs(n);
            const sign = n < 0 ? "-" : "";
            const compact = (val, suffix) => {
                let s = val.toFixed(1);
                if (s.endsWith(".0")) s = s.slice(0, -2);
                return sign + s + suffix;
            };
            if (abs >= 1e12) return compact(abs / 1e12, "T");
            if (abs >= 1e9) return compact(abs / 1e9, "B");
            if (abs >= 1e6) return compact(abs / 1e6, "M");
            if (abs >= 1e3) return compact(abs / 1e3, "K");
            return n.toLocaleString();
        };

        let gridLines = "";
        let yTicks = "";
        for (let v = axisMin; v <= axisMax + 0.5; v += step) {
            const iv = Math.round(v);
            const y = yScale(iv);
            gridLines += `<line x1="${leftPad}" x2="${leftPad + plotWidth}" y1="${y}" y2="${y}"/>`;
            yTicks += `<text x="${leftPad - 6}" y="${y + 3}" text-anchor="end">${escapeHtml(fmtAxis(iv))}</text>`;
        }
        const baselineY = yScale(Math.max(axisMin, Math.min(0, axisMax)));

        let bars = "";
        let xLabels = "";
        data.forEach((d, i) => {
            const x = leftPad + i * (barWidth + barGap) + barGap / 2;
            if (d.segments) {
                let cum = 0;
                d.segments.forEach((sg) => {
                    const y0 = yScale(cum);
                    const y1 = yScale(cum + sg.value);
                    const top = Math.min(y0, y1);
                    const h = Math.max(1, Math.abs(y1 - y0));
                    const tip = `${sg.name}: ${fmtNum(sg.value)}`;
                    bars += `<rect class="dtx-chart-bar" x="${x}" y="${top}" width="${barWidth}" height="${h}" rx="2" style="fill:${sg.color}"><title>${escapeHtml(tip)}</title></rect>`;
                    cum += sg.value;
                });
            } else {
                const y = d.value >= 0 ? yScale(d.value) : baselineY;
                const h = Math.max(1, Math.abs(yScale(d.value) - baselineY));
                const tip = `${d.label}: ${fmtNum(d.value)}`;
                bars += `<rect class="dtx-chart-bar" x="${x}" y="${y}" width="${barWidth}" height="${h}" rx="2"><title>${escapeHtml(tip)}</title></rect>`;
            }
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
        let legendHtml = "";
        if (legendSegments) {
            legendHtml = `<div class="dtx-chart-legend">`
                + legendSegments.map(sg =>
                    `<span class="dtx-legend-item">`
                    + `<span class="dtx-legend-swatch" style="background:${sg.color}"></span>`
                    + `${escapeHtml(sg.name)}</span>`
                ).join("")
                + `</div>`;
        }
        chartWrap.innerHTML = svg + legendHtml;
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
        } else if (mode === "history") {
            renderHistoryTable();
            resultMeta.style.display = "none";
        } else if (mode === "queryplan") {
            renderQueryPlanTable();
            resultMeta.style.display = "none";
        } else if (mode === "dependencies") {
            renderDependenciesTable();
            resultMeta.style.display = "none";
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
    model.on("change:trace_history", renderTable);
    model.on("change:query_plan_rows", renderTable);
    model.on("change:query_plan_type", renderTable);
    model.on("change:dependency_tree", renderTable);
    model.on("change:dependencies_loading", renderTable);
    model.on("change:dependency_columns", renderTable);
    model.on("change:dependency_view", renderTable);
    model.on("change:history_excel_b64", () => {
        const b64 = model.get("history_excel_b64") || "";
        if (!b64) return;
        try {
            const bin = atob(b64);
            const len = bin.length;
            const bytes = new Uint8Array(len);
            for (let i = 0; i < len; i++) bytes[i] = bin.charCodeAt(i);
            const blob = new Blob([bytes], {
                type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            });
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            a.download = model.get("history_excel_name") || "trace_history.xlsx";
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            setTimeout(() => URL.revokeObjectURL(url), 1000);
        } catch (e) {}
        // Clear so a subsequent identical download still fires a change.
        model.set("history_excel_b64", "");
        model.save_changes();
    });
    model.on("change:is_running", () => {
        root.classList.toggle("dtx-running", model.get("is_running") === true);
        renderRunBtn();
    });
    model.on("change:error_message", renderError);
    model.on("change:dax_query", () => {
        if (textarea.value !== model.get("dax_query")) {
            const newVal = model.get("dax_query") || "";
            textarea.value = newVal;
            // External update (e.g. Format from Python) — record
            // it as a discrete, undoable history entry.
            commitHistory(newVal, false);
        }
        renderHighlight();
        renderFmtBtn();
    });
    model.on("change:dax_tokens", renderHighlight);
    model.on("change:format_loading", renderFmtBtn);
    model.on("change:clear_cache", renderCacheBtn);
    model.on("change:impersonation_mode", renderImpersonation);
    model.on("change:impersonation_value", renderImpersonation);
    model.on("change:model_roles", renderImpersonation);
    model.on("change:sidebar_collapsed", renderSidebarChrome);
    model.on("change:metadata_loading", () => { renderSidebarChrome(); renderTree(); });
    model.on("change:model_tree", renderTree);
    model.on("change:dataset_chosen", () => {
        if (model.get("dataset_chosen") === true) { pickerOpen = false; }
        renderPicker(); renderRunBtn(); renderSubtitle();
        renderBuildBtn(); renderBuilderChrome();
    });
    model.on("change:available_workspaces", renderPicker);
    model.on("change:available_datasets", renderPicker);
    model.on("change:selected_workspace_id", renderPicker);
    model.on("change:selected_dataset_id", renderPicker);
    model.on("change:active_workspace_id", renderPicker);
    model.on("change:active_dataset_id", () => {
        // A new model finished activating — close the picker. This covers
        // switching between two already-chosen models, where dataset_chosen
        // does not change and so would not otherwise close the picker.
        pickerOpen = false;
        // Force a fresh dependency computation for the newly activated model.
        lastDepQuery = null;
        renderPicker();
    });
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
    renderFmtBtn();
    renderHistBtns();
    renderTree();
    renderHighlight();
    renderBuilderChrome();
    renderBuilderZones();
    renderBuildBtn();

    // Notify Python to tear down the long-running trace when this view is
    // disposed (cell re-run, widget removed, notebook closed).
    return () => {
        try {
            model.set("close_trigger", (model.get("close_trigger") || 0) + 1);
            model.save_changes();
        } catch (e) {}
    };
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
        .replace("__DTX_PLAY__", play_icon)
        .replace("__DTX_STOP__", stop_icon)
        .replace("__DTX_REFRESH__", refresh_icon)
        .replace("__DTX_SWAP__", swap_icon)
        .replace("__DTX_SORT_ASC__", sort_asc_icon)
        .replace("__DTX_SORT_DESC__", sort_desc_icon)
        .replace("__DTX_PANEL_COLLAPSE__", panel_collapse_icon)
        .replace("__DTX_PANEL_EXPAND__", panel_expand_icon)
        .replace("__DTX_BUILDER__", builder_icon)
        .replace("__DTX_CLOSE__", close_icon)
        .replace("__DTX_DAXFORMAT__", daxformat_icon)
        .replace("__DTX_UNDO__", undo_icon)
        .replace("__DTX_REDO__", redo_icon)
        .replace("__DTX_DOWNLOAD__", download_icon)
        .replace("__DTX_CUT__", cut_icon)
        .replace("__DTX_COPY__", copy_icon)
        .replace("__DTX_PASTE__", paste_icon)
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
        query_plan_rows = traitlets.List([]).tag(sync=True)
        query_plan_type = traitlets.Unicode("Logical").tag(sync=True)
        result_columns = traitlets.List([]).tag(sync=True)
        result_rows = traitlets.List([]).tag(sync=True)
        result_total_rows = traitlets.Int(0).tag(sync=True)
        result_truncated = traitlets.Bool(False).tag(sync=True)
        view_mode = traitlets.Unicode("trace").tag(sync=True)
        trace_history = traitlets.List([]).tag(sync=True)
        download_history_trigger = traitlets.Int(0).tag(sync=True)
        history_excel_b64 = traitlets.Unicode("").tag(sync=True)
        history_excel_name = traitlets.Unicode("").tag(sync=True)
        is_running = traitlets.Bool(False).tag(sync=True)
        error_message = traitlets.Unicode("").tag(sync=True)
        run_trigger = traitlets.Int(0).tag(sync=True)
        cancel_trigger = traitlets.Int(0).tag(sync=True)
        dependency_tree = traitlets.List([]).tag(sync=True)
        dependencies_loading = traitlets.Bool(False).tag(sync=True)
        dependencies_trigger = traitlets.Int(0).tag(sync=True)
        dependency_columns = traitlets.List([]).tag(sync=True)
        dependency_view = traitlets.Unicode("tree").tag(sync=True)
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
        format_query_trigger = traitlets.Int(0).tag(sync=True)
        format_loading = traitlets.Bool(False).tag(sync=True)
        query_builder_state = traitlets.Unicode("").tag(sync=True)
        build_query_trigger = traitlets.Int(0).tag(sync=True)
        close_trigger = traitlets.Int(0).tag(sync=True)

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
            initial_tree, initial_roles = _collect_model_metadata(
                dataset_id, workspace_id
            )
        except Exception:
            initial_tree = []
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
        query_plan_rows=initial_query_plan_rows,
        query_plan_type="Logical",
        result_columns=initial_result["columns"],
        result_rows=initial_result["rows"],
        result_total_rows=int(initial_result["total_rows"]),
        result_truncated=bool(initial_result["truncated"]),
        view_mode="trace",
        is_running=False,
        error_message="",
        run_trigger=0,
        cancel_trigger=0,
        dependency_tree=[],
        dependencies_loading=False,
        dependencies_trigger=0,
        dependency_columns=[],
        dependency_view="tree",
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
        format_query_trigger=0,
        format_loading=False,
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

    # ---- Persistent (long-running) trace shared by all queries in the UI ----
    # Instead of creating a fresh trace per query, the widget keeps a single
    # trace running for the active model. It is started when the model
    # metadata is loaded and torn down when the UI is closed. Each query reads
    # the rows it produced live via ``trace.get_trace_logs()`` (tracked with a
    # baseline row count) without stopping the trace.
    trace_ctx: dict = {
        "connection": None,
        "trace": None,
        "dataset_id": None,
        "workspace_id": None,
        "baseline": 0,
        "started": False,
        "warmed_up": False,
    }
    trace_lock = threading.Lock()

    def _teardown_trace_locked() -> None:
        """Stop/drop the running trace and dispose its connection. Caller must
        hold ``trace_lock``."""
        tr = trace_ctx.get("trace")
        conn = trace_ctx.get("connection")
        if tr is not None:
            try:
                tr.drop()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.disconnect_and_dispose()
            except Exception:
                pass
        trace_ctx["connection"] = None
        trace_ctx["trace"] = None
        trace_ctx["started"] = False
        trace_ctx["baseline"] = 0
        trace_ctx["dataset_id"] = None
        trace_ctx["workspace_id"] = None
        trace_ctx["warmed_up"] = False

    def _ensure_trace(ds_id: Optional[str], ws_id: Optional[str]) -> None:
        """Ensure a long-running trace is active for the given model. Starts a
        new trace (rebinding from any previous model) when needed. Safe to call
        repeatedly; a no-op when already running for the same model."""
        if not ds_id:
            return
        with trace_lock:
            if (
                trace_ctx["started"]
                and trace_ctx["dataset_id"] == ds_id
                and trace_ctx["workspace_id"] == ws_id
            ):
                return
            # Switching models (or first start): tear down any existing trace.
            _teardown_trace_locked()
            try:
                conn = fabric.create_trace_connection(
                    dataset=ds_id, workspace=ws_id
                )
                trace = conn.create_trace(_TEST_EVENT_SCHEMA)
                trace.start()
                # Prime the trace: a freshly started trace does not begin
                # capturing server-side events instantly, so the very first
                # real query's events can be missed. Run the throwaway warm-up
                # query now and wait until it actually shows up in the trace
                # logs, which confirms the trace is live. The baseline is then
                # advanced past these warm-up rows so the first real query is
                # captured from a known-good state.
                baseline = 0
                warmed = False
                try:
                    fabric.evaluate_dax(
                        dataset=ds_id,
                        workspace=ws_id,
                        dax_string="EVALUATE {1}",
                    )
                    _deadline = time.monotonic() + 5.0
                    while time.monotonic() < _deadline:
                        time.sleep(0.1)
                        try:
                            _logs = trace.get_trace_logs()
                        except Exception:
                            continue
                        if _logs is None or _logs.empty:
                            continue
                        _ec = (
                            "Event Class"
                            if "Event Class" in _logs.columns
                            else "EventClass"
                        )
                        if _ec not in _logs.columns:
                            continue
                        if not _logs[_logs[_ec] == "QueryEnd"].empty:
                            baseline = len(_logs)
                            warmed = True
                            break
                except Exception:
                    pass
                trace_ctx["connection"] = conn
                trace_ctx["trace"] = trace
                trace_ctx["dataset_id"] = ds_id
                trace_ctx["workspace_id"] = ws_id
                trace_ctx["baseline"] = baseline
                trace_ctx["started"] = True
                trace_ctx["warmed_up"] = warmed
            except Exception:
                # Tracing could not be started; queries fall back to a
                # one-shot trace via ``_run_dax_trace``.
                _teardown_trace_locked()

    def _stop_persistent_trace(*_args) -> None:
        """Tear down the long-running trace (called when the UI is closed)."""
        with trace_lock:
            _teardown_trace_locked()

    def _run_query_persistent(
        query: str,
        clear_cache_flag: bool,
        effective_user: Optional[str],
        role_name: Optional[str],
    ) -> Tuple[pd.DataFrame, int, int, int, int, pd.DataFrame]:
        """Run a query against the long-running trace, capturing only the rows
        it produced. Falls back to a one-shot trace if the persistent trace is
        not available."""
        from sempy_labs._clear_cache import clear_cache as _clear_cache_fn

        ds_id = model_ctx["dataset_id"]
        ws_id = model_ctx["workspace_id"]
        _ensure_trace(ds_id, ws_id)
        with trace_lock:
            trace = trace_ctx["trace"] if trace_ctx["started"] else None
            baseline = trace_ctx["baseline"]
            run_warmup = not trace_ctx["warmed_up"]
        if trace is None:
            # Persistent tracing unavailable: one-shot fallback.
            return _run_dax_trace(
                dataset_id=ds_id,
                workspace_id=ws_id,
                dax_string=query,
                clear_cache=clear_cache_flag,
                effective_user_name=effective_user,
                role=role_name,
            )
        if clear_cache_flag:
            _clear_cache_fn(dataset=ds_id, workspace=ws_id)
        result_df, new_logs, new_count = _execute_and_capture(
            trace,
            ds_id,
            ws_id,
            query,
            effective_user,
            role_name,
            baseline,
            run_warmup=run_warmup,
        )
        with trace_lock:
            # Only advance the baseline if the trace wasn't rebound meanwhile.
            if trace_ctx["trace"] is trace:
                trace_ctx["baseline"] = new_count
                trace_ctx["warmed_up"] = True
        df, total, fe, se, cpu = _compute_trace_stats(new_logs)
        return df, total, fe, se, cpu, result_df

    import atexit

    atexit.register(_stop_persistent_trace)

    def _worker(
        query: str,
        clear_cache_flag: bool,
        run_id: int,
        effective_user: Optional[str],
        role_name: Optional[str],
    ) -> None:
        _start_dt = datetime.now(timezone.utc)
        try:
            (
                new_df,
                new_total,
                new_fe,
                new_se,
                new_cpu,
                new_result,
            ) = _run_query_persistent(
                query=query,
                clear_cache_flag=clear_cache_flag,
                effective_user=effective_user,
                role_name=role_name,
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
        widget.query_plan_rows = _query_plan_rows_from_df(new_df)
        payload = _result_payload_from_df(new_result)
        widget.result_columns = payload["columns"]
        widget.result_rows = payload["rows"]
        widget.result_total_rows = int(payload["total_rows"])
        widget.result_truncated = bool(payload["truncated"])
        widget.error_message = ""
        widget.is_running = False

        # Append to the session trace history (newest first).
        _end_dt = datetime.now(timezone.utc)
        try:
            # Row count is the true number of rows in the query's result
            # dataframe (not the truncated display payload).
            try:
                _row_count = int(len(new_result)) if new_result is not None else 0
            except Exception:
                _row_count = int(payload["total_rows"])
            if role_name:
                _imp_type = "Role"
                _imp_value = str(role_name)
            elif effective_user:
                _imp_type = "User"
                _imp_value = str(effective_user)
            else:
                _imp_type = "None"
                _imp_value = "None"
            _entry = {
                "dax_query": query,
                "start_time": _start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": _end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "rows": _row_count,
                "duration": int(new_total),
                "cpu": int(new_cpu),
                "fe_duration": int(new_fe),
                "se_duration": int(new_se),
                "dataset_name": str(widget.dataset_name or ""),
                "workspace_name": str(widget.workspace_name or ""),
                "impersonation_type": _imp_type,
                "impersonation": _imp_value,
            }
            widget.trace_history = [_entry] + list(widget.trace_history)
        except Exception:
            pass

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

    def _compute_dependencies() -> None:
        """Compute the model objects (tables, columns, measures,
        relationships) referenced by the current DAX query via
        ``INFO.CALCDEPENDENCY`` and push the resulting hierarchical tree to the
        front-end. The trace data for this helper query is not captured."""
        try:
            if model_ctx["dataset_id"] is None:
                widget.dependency_tree = []
                widget.dependency_columns = []
                return
            dax_query = widget.dax_query or ""
            if not dax_query.strip():
                widget.dependency_tree = []
                widget.dependency_columns = []
                return
            # Escape double quotes for embedding as a DAX string literal.
            escaped_dax = dax_query.replace('"', '""')
            query = (
                "EVALUATE\n"
                "SELECTCOLUMNS(\n"
                "    INFO.CALCDEPENDENCY(\n"
                '        "Query",\n'
                f'        "{escaped_dax}"\n'
                "    ),\n"
                '    "Referenced Object Type", [REFERENCED_OBJECT_TYPE],\n'
                '    "Referenced Table", [REFERENCED_TABLE],\n'
                '    "Referenced Object", [REFERENCED_OBJECT]\n'
                ")"
            )
            dep_df = fabric.evaluate_dax(
                dataset=model_ctx["dataset_id"],
                dax_string=query,
                workspace=model_ctx["workspace_id"],
            )
            rows = []
            for _, r in dep_df.iterrows():
                rows.append(
                    {
                        "object_type": str(
                            r.get("[Referenced Object Type]", "") or ""
                        ),
                        "table": str(r.get("[Referenced Table]", "") or ""),
                        "object": str(r.get("[Referenced Object]", "") or ""),
                    }
                )
            # Enrich relationships with the columns they join (read via TOM)
            # only when the query actually depends on a relationship. Also read
            # the model's RowNumber columns (also via TOM) so they can be
            # excluded from the output whenever the query references columns.
            rel_lookup: dict = {}
            rel_columns: dict = {}
            rownumber_cols: set = set()
            needs_rel = any(
                "RELATIONSHIP" in (row["object_type"] or "").upper()
                for row in rows
            )
            needs_cols = any(
                "COLUMN" in (row["object_type"] or "").upper()
                for row in rows
            )
            if needs_rel or needs_cols:
                try:
                    from sempy_labs.tom import connect_semantic_model

                    with connect_semantic_model(
                        dataset=model_ctx["dataset_id"],
                        workspace=model_ctx["workspace_id"],
                        readonly=True,
                    ) as tom:
                        if needs_rel:
                            rel_lookup = _build_relationship_lookup(tom)
                            rel_columns = _build_relationship_columns(tom)
                        rownumber_cols = _build_rownumber_columns(tom)
                except Exception:
                    rel_lookup = {}
                    rel_columns = {}
                    rownumber_cols = set()
            widget.dependency_tree = _build_dependency_tree(
                rows, rel_lookup, widget.dataset_name or "Model", rownumber_cols
            )
            widget.dependency_columns = _build_dependency_columns(
                rows, rel_columns, rownumber_cols
            )
            widget.error_message = ""
        except Exception as exc:  # noqa: BLE001
            widget.dependency_tree = []
            widget.dependency_columns = []
            widget.error_message = (
                f"Failed to compute query dependencies: {exc}"
            )
        finally:
            widget.dependencies_loading = False

    def _on_dependencies(change):
        if change["new"] == change["old"]:
            return
        if widget.dependencies_loading:
            return
        widget.dependencies_loading = True
        threading.Thread(target=_compute_dependencies, daemon=True).start()

    widget.observe(_on_run, names="run_trigger")
    widget.observe(_on_cancel, names="cancel_trigger")
    widget.observe(_on_dependencies, names="dependencies_trigger")

    def _build_history_excel() -> None:
        import base64
        import io

        history = list(widget.trace_history)
        cols = [
            ("dax_query", "DAX Query"),
            ("start_time", "Start Time (UTC)"),
            ("end_time", "End Time (UTC)"),
            ("rows", "Rows"),
            ("duration", "Duration (ms)"),
            ("cpu", "CPU (ms)"),
            ("fe_duration", "FE Duration (ms)"),
            ("se_duration", "SE Duration (ms)"),
            ("dataset_name", "Semantic Model"),
            ("workspace_name", "Workspace"),
            ("impersonation_type", "Impersonation Type"),
            ("impersonation", "Impersonation"),
        ]
        rows = [
            {label: entry.get(key, "") for key, label in cols}
            for entry in history
        ]
        df_hist = pd.DataFrame(rows, columns=[label for _, label in cols])
        buf = io.BytesIO()
        # Use whichever Excel engine is available (openpyxl is standard in
        # Fabric notebooks; xlsxwriter is an accepted fallback).
        engine = None
        for _eng in ("openpyxl", "xlsxwriter"):
            try:
                __import__(_eng)
                engine = _eng
                break
            except Exception:
                continue
        if engine is None:
            widget.error_message = (
                "Could not export to Excel: no Excel engine is installed. "
                "Install 'openpyxl' (pip install openpyxl) and try again."
            )
            return
        try:
            with pd.ExcelWriter(buf, engine=engine) as writer:
                df_hist.to_excel(writer, index=False, sheet_name="Trace History")
        except Exception as exc:  # noqa: BLE001
            widget.error_message = (
                f"Failed to build the Excel file: {exc}"
            )
            return
        b64 = base64.b64encode(buf.getvalue()).decode("ascii")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        # Reset first so the front-end always observes a change event.
        widget.history_excel_name = f"trace_history_{stamp}.xlsx"
        widget.history_excel_b64 = ""
        widget.history_excel_b64 = b64

    def _on_download_history(change):
        if change["new"] == change["old"]:
            return
        threading.Thread(target=_build_history_excel, daemon=True).start()

    widget.observe(_on_download_history, names="download_history_trigger")

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
            tree, roles = _collect_model_metadata(
                model_ctx["dataset_id"], model_ctx["workspace_id"]
            )
        except Exception as exc:  # noqa: BLE001
            widget.metadata_loading = False
            widget.error_message = f"Failed to load model metadata: {exc}"
            return
        widget.model_tree = tree
        widget.model_roles = roles
        widget.metadata_loading = False
        # Start (or keep) the long-running trace for this model now that its
        # metadata has loaded.
        _ensure_trace(model_ctx["dataset_id"], model_ctx["workspace_id"])

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
            tree, roles = _collect_model_metadata(
                ds_id_resolved, ws_id_resolved
            )
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
        # Rebind the long-running trace to the newly selected model.
        _ensure_trace(ds_id_resolved, ws_id_resolved)

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

    def _format_query() -> None:
        dax = widget.dax_query or ""
        if not dax.strip():
            widget.format_loading = False
            return
        try:
            formatted = _format_dax(dax)
            dax_out = formatted[0] if formatted else dax
        except Exception as exc:  # noqa: BLE001
            widget.format_loading = False
            widget.error_message = f"Failed to format the DAX query: {exc}"
            return
        dax_out = dax_out.replace("\r\n", "\n").replace("\r", "\n")
        widget.dax_query = dax_out
        widget.dax_tokens = _classify_dax_spans(dax_out)
        widget.error_message = ""
        widget.format_loading = False

    def _on_format_query(change):
        if change["new"] == change["old"]:
            return
        if widget.format_loading:
            return
        widget.format_loading = True
        threading.Thread(target=_format_query, daemon=True).start()

    widget.observe(_on_format_query, names="format_query_trigger")

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

    def _on_close_trigger(change):
        if change["new"] == change["old"]:
            return
        _stop_persistent_trace()

    widget.observe(_on_close_trigger, names="close_trigger")

    # Start the long-running trace for the initially selected model (if any)
    # so it is ready by the time the first query runs.
    if model_ctx["dataset_id"] is not None:
        threading.Thread(
            target=lambda: _ensure_trace(
                model_ctx["dataset_id"], model_ctx["workspace_id"]
            ),
            daemon=True,
        ).start()

    display(widget)

    # Backstop for trace teardown if the comm is closed without the JS
    # cleanup hook firing (e.g. kernel/cell disposal).
    try:
        widget.comm.on_close(lambda *_a: _stop_persistent_trace())
    except Exception:
        pass
