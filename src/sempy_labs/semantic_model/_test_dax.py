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
    dataset: str | UUID,
    dax_string: str,
    workspace: Optional[str | UUID] = None,
    clear_cache: bool = True,
    visualize: bool = True,
) -> pd.DataFrame:
    """
    Runs a DAX query against a semantic model while capturing a server-side
    trace, and computes high-level performance statistics (Total Duration,
    Formula Engine Duration, Storage Engine Duration, and CPU time) using
    the same conventions as `DAX Studio <https://github.com/DaxStudio/DaxStudio>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    dax_string : str
        The DAX query to execute.
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

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe of the captured trace events, including the
        ``Event Class``, ``Event Subclass``, ``Duration`` and ``Cpu Time``
        for each event.
    """

    from sempy_labs._helper_functions import resolve_workspace_name_and_id

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_item_name_and_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )

    df, total_duration, fe_duration, se_duration, cpu_time, result_df = _run_dax_trace(
        dataset_id=dataset_id,
        workspace_id=workspace_id,
        dax_string=dax_string,
        clear_cache=clear_cache,
    )

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
            dataset_name=str(dataset_name) if dataset_name else str(dataset),
            workspace_name=workspace_name,
            clear_cache=clear_cache,
            result_df=result_df,
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
                dataset=dataset_id, workspace=workspace_id, dax_string=dax_string
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
                        }
                        for c in tom.all_columns()
                        if c.Parent == table
                    ),
                    key=lambda x: x["name"].lower(),
                )
                measures = sorted(
                    (
                        {
                            "name": str(m.Name),
                            "hidden": bool(getattr(m, "IsHidden", False)),
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
                        }
                        for h in table.Hierarchies
                    ),
                    key=lambda x: x["name"].lower(),
                )
                tree.append(
                    {
                        "name": tname,
                        "hidden": bool(getattr(table, "IsHidden", False)),
                        "columns": columns,
                        "measures": measures,
                        "hierarchies": hierarchies,
                    }
                )
    except Exception:
        return []
    tree.sort(key=lambda t: t["name"].lower())
    return tree


def _visualize_dax_test(
    df: pd.DataFrame,
    total_duration: int,
    fe_duration: int,
    se_duration: int,
    cpu_time: int,
    dax_string: str,
    dataset_id: str,
    workspace_id: str,
    dataset_name: Optional[str] = None,
    workspace_name: Optional[str] = None,
    dark_mode: bool = False,
    clear_cache: bool = True,
    result_df: Optional[pd.DataFrame] = None,
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
.dtx .dtx-query-title {{
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--ui-text-tertiary);
    margin-right: auto;
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
    color: var(--ui-text);
    resize: vertical;
    outline: none;
    transition: border-color 120ms ease, box-shadow 120ms ease;
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
.dtx .dtx-tree-caret svg {{ width: 10px; height: 10px; }}
.dtx .dtx-tree-icon {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 14px;
    height: 14px;
    flex: 0 0 14px;
    color: var(--ui-text-tertiary);
}}
.dtx .dtx-tree-icon svg {{ width: 14px; height: 14px; }}
.dtx .dtx-tree-label {{
    flex: 1 1 auto;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}}
.dtx .dtx-tree-label.dtx-hidden {{
    color: var(--ui-text-tertiary);
    font-style: italic;
}}
.dtx.dtx-dark .dtx-tree-label.dtx-hidden {{
    color: #9ca3af;
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
}}
"""
    )

    sun_icon = _UI_ICONS["sun"].replace("`", "\\`")
    moon_icon = _UI_ICONS["moon"].replace("`", "\\`")
    table_icon = _UI_ICONS["table"].replace("`", "\\`")
    column_icon = _UI_ICONS["column"].replace("`", "\\`")
    measure_icon = _UI_ICONS["measure"].replace("`", "\\`")
    hierarchy_icon = _UI_ICONS["hierarchy"].replace("`", "\\`")
    caret_icon = _UI_ICONS["caret_right"].replace("`", "\\`")

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
    const COLUMN_SVG = `__DTX_COLUMN__`;
    const MEASURE_SVG = `__DTX_MEASURE__`;
    const HIERARCHY_SVG = `__DTX_HIERARCHY__`;
    const CARET_SVG = `__DTX_CARET__`;
    const REFRESH_SVG = '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor"'
        + ' stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
        + '<path d="M13.5 8a5.5 5.5 0 1 1-1.61-3.89"/><path d="M13.5 2.5v3h-3"/></svg>';
    const PANEL_COLLAPSE_SVG = '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor"'
        + ' stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
        + '<rect x="2" y="3" width="12" height="10" rx="1.5"/><path d="M6.5 3v10"/>'
        + '<path d="M10.5 6.5L8.5 8l2 1.5"/></svg>';
    const PANEL_EXPAND_SVG = '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor"'
        + ' stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
        + '<rect x="2" y="3" width="12" height="10" rx="1.5"/><path d="M6.5 3v10"/>'
        + '<path d="M8.5 6.5L10.5 8l-2 1.5"/></svg>';

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
    function renderSubtitle() {
        const ds = model.get("dataset_name") || "";
        const ws = model.get("workspace_name") || "";
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

    function makeLeaf(iconSvg, name, hidden, dataType) {
        const leaf = document.createElement("div");
        leaf.className = "dtx-tree-leaf";
        leaf.style.paddingLeft = "30px";
        const typeHtml = dataType
            ? `<span class="dtx-tree-type" title="${escapeHtml(dataType)}">${escapeHtml(dataType)}</span>`
            : "";
        leaf.innerHTML = `<span class="dtx-tree-icon">${iconSvg}</span>`
            + `<span class="dtx-tree-label${hidden ? " dtx-hidden" : ""}"`
            + ` title="${escapeHtml(name)}">${escapeHtml(name)}</span>`
            + typeHtml;
        return leaf;
    }

    function makeGroup(label, items, iconSvg) {
        if (!items || !items.length) return null;
        const wrap = document.createElement("div");
        wrap.className = "dtx-tree-group";
        const header = document.createElement("div");
        header.className = "dtx-tree-group-header";
        header.style.paddingLeft = "18px";
        header.innerHTML = `<span class="dtx-tree-caret">${CARET_SVG}</span><span>${escapeHtml(label)}</span><span class="dtx-tree-group-count">${items.length}</span>`;
        const children = document.createElement("div");
        children.className = "dtx-tree-children";
        for (const it of items) children.appendChild(makeLeaf(iconSvg, it.name, !!it.hidden, it.data_type));
        header.addEventListener("click", () => {
            const open = !header.classList.contains("dtx-open");
            header.classList.toggle("dtx-open", open);
            children.style.display = open ? "block" : "none";
        });
        children.style.display = "none";
        wrap.appendChild(header);
        wrap.appendChild(children);
        return wrap;
    }

    function renderTree() {
        sidebarBody.innerHTML = "";
        const tree = model.get("model_tree") || [];
        if (!tree.length) {
            const empty = document.createElement("div");
            empty.className = "dtx-sidebar-empty";
            empty.textContent = model.get("metadata_loading") === true
                ? "Loading model metadata…"
                : "No metadata available.";
            sidebarBody.appendChild(empty);
            return;
        }
        for (const tbl of tree) {
            const node = document.createElement("div");
            node.className = "dtx-tree-node";
            node.innerHTML = `<span class="dtx-tree-caret">${CARET_SVG}</span>`
                + `<span class="dtx-tree-icon">${TABLE_SVG}</span>`
                + `<span class="dtx-tree-label${tbl.hidden ? " dtx-hidden" : ""}"`
                + ` title="${escapeHtml(tbl.name)}">${escapeHtml(tbl.name)}</span>`;
            const children = document.createElement("div");
            children.className = "dtx-tree-children";
            const colGrp = makeGroup("Columns", tbl.columns, COLUMN_SVG);
            const meaGrp = makeGroup("Measures", tbl.measures, MEASURE_SVG);
            const hieGrp = makeGroup("Hierarchies", tbl.hierarchies, HIERARCHY_SVG);
            if (colGrp) children.appendChild(colGrp);
            if (meaGrp) children.appendChild(meaGrp);
            if (hieGrp) children.appendChild(hieGrp);
            node.addEventListener("click", () => {
                const open = !node.classList.contains("dtx-open");
                node.classList.toggle("dtx-open", open);
                children.style.display = open ? "block" : "none";
            });
            children.style.display = "none";
            sidebarBody.appendChild(node);
            sidebarBody.appendChild(children);
        }
    }

    const main = document.createElement("div");
    main.className = "dtx-main";
    body.appendChild(main);

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

    const qTitle = document.createElement("div");
    qTitle.className = "dtx-query-title";
    qTitle.textContent = "DAX Query";
    toolbar.appendChild(qTitle);

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

    const runBtn = document.createElement("button");
    runBtn.type = "button";
    runBtn.className = "dtx-btn";
    function renderRunBtn() {
        const running = model.get("is_running") === true;
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
    textarea.addEventListener("input", () => {
        model.set("dax_query", textarea.value);
        model.save_changes();
    });
    // Ctrl/Cmd+Enter to run
    textarea.addEventListener("keydown", (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
            e.preventDefault();
            runBtn.click();
        }
    });
    queryBlock.appendChild(textarea);

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
    seg.appendChild(segTrace);
    seg.appendChild(segResult);
    viewToolbar.appendChild(seg);
    segTrace.addEventListener("click", () => {
        model.set("view_mode", "trace");
        model.save_changes();
    });
    segResult.addEventListener("click", () => {
        model.set("view_mode", "result");
        model.save_changes();
    });
    function renderSeg() {
        const mode = model.get("view_mode") || "trace";
        segTrace.classList.toggle("dtx-seg-btn-on", mode === "trace");
        segResult.classList.toggle("dtx-seg-btn-on", mode === "result");
    }

    const resultMeta = document.createElement("div");
    resultMeta.className = "dtx-result-meta";
    resultMeta.style.display = "none";
    main.appendChild(resultMeta);

    const tableWrap = document.createElement("div");
    tableWrap.className = "dtx-table-wrap";
    main.appendChild(tableWrap);

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

    function renderTable() {
        const mode = model.get("view_mode") || "trace";
        if (mode === "result") {
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
    });
    model.on("change:clear_cache", renderCacheBtn);
    model.on("change:sidebar_collapsed", renderSidebarChrome);
    model.on("change:metadata_loading", () => { renderSidebarChrome(); renderTree(); });
    model.on("change:model_tree", renderTree);

    applyTheme();
    renderSubtitle();
    renderCards();
    renderRunBtn();
    renderCacheBtn();
    renderError();
    renderTable();
    renderSidebarChrome();
    renderTree();
}
export default { render };
"""
    widget_js = (
        widget_js.replace("__DTX_SUN__", sun_icon)
        .replace("__DTX_MOON__", moon_icon)
        .replace("__DTX_TABLE__", table_icon)
        .replace("__DTX_COLUMN__", column_icon)
        .replace("__DTX_MEASURE__", measure_icon)
        .replace("__DTX_HIERARCHY__", hierarchy_icon)
        .replace("__DTX_CARET__", caret_icon)
    )

    class DaxTestWidget(anywidget.AnyWidget):
        _esm = widget_js
        _css = widget_css

        dax_query = traitlets.Unicode("").tag(sync=True)
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

    initial_result = _result_payload_from_df(result_df)

    widget = DaxTestWidget(
        dax_query=formatted_initial or "",
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
        model_tree=[],
        sidebar_collapsed=False,
        refresh_metadata_trigger=0,
        metadata_loading=True,
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

    def _worker(query: str, clear_cache_flag: bool, run_id: int) -> None:
        try:
            (
                new_df,
                new_total,
                new_fe,
                new_se,
                new_cpu,
                new_result,
            ) = _run_dax_trace(
                dataset_id=dataset_id,
                workspace_id=workspace_id,
                dax_string=query,
                clear_cache=clear_cache_flag,
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
        query = widget.dax_query or ""
        if not query.strip():
            widget.error_message = "DAX query is empty."
            widget.is_running = False
            return
        with state_lock:
            run_state["current_run_id"] += 1
            run_id = run_state["current_run_id"]
        thread = threading.Thread(
            target=_worker,
            args=(query, bool(widget.clear_cache), run_id),
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

    def _load_metadata() -> None:
        try:
            tree = _collect_model_tree(dataset_id, workspace_id)
        except Exception as exc:  # noqa: BLE001
            widget.metadata_loading = False
            widget.error_message = f"Failed to load model metadata: {exc}"
            return
        widget.model_tree = tree
        widget.metadata_loading = False

    def _on_refresh_metadata(change):
        if change["new"] == change["old"]:
            return
        if widget.metadata_loading:
            return
        widget.metadata_loading = True
        threading.Thread(target=_load_metadata, daemon=True).start()

    widget.observe(_on_refresh_metadata, names="refresh_metadata_trigger")

    # Initial metadata load in the background so the widget displays immediately.
    threading.Thread(target=_load_metadata, daemon=True).start()

    display(widget)
