import re
from typing import Optional
from uuid import UUID
from sempy._utils._log import log

# Source types offered by the migration wizard (mirrors the reference tool).
_MIGRATION_SOURCE_TYPES = ["Lakehouse", "Warehouse"]

_DIRECT_LAKE_DOCS_URL = (
    "https://learn.microsoft.com/power-bi/enterprise/directlake-overview"
    "#known-issues-and-limitations"
)

# A field-parameter field reference looks like 'Table'[Column], Table[Column]
# or [Measure] inside a NAMEOF() call.
_FP_FIELD_REGEX = re.compile(r"(?:'(?P<tq>[^']+)'|(?P<tu>\w+))?\[(?P<o>[^\]]+)\]")


def _mig_key(table_name: str, object_name: str) -> str:
    """Build the "table|object" key used to track removed columns/measures."""
    return f"{table_name}|{object_name}"


def _split_top_level(text: str):
    """Split a string on commas that sit at the top level (outside any
    parentheses, braces, brackets, or string literal)."""
    parts = []
    depth = 0
    in_string = False
    start = 0
    for i, ch in enumerate(text):
        if in_string:
            if ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch in "({[":
            depth += 1
        elif ch in ")}]":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(text[start:i])
            start = i + 1
    parts.append(text[start:])
    return parts


def _row_removed_reference(row, removed_tables, removed_columns, removed_measures):
    """Return the field reference (e.g. 'Sales'[Amount]) in a field-parameter
    row that points at a dropped object, or None when every reference in the
    row survives the migration."""
    for m in _FP_FIELD_REGEX.finditer(row):
        table = m.group("tq") or m.group("tu") or ""
        obj = m.group("o")
        if table:
            if (
                table in removed_tables
                or _mig_key(table, obj) in removed_columns
                or _mig_key(table, obj) in removed_measures
            ):
                return m.group(0).strip()
        else:
            # A measure reference with no table qualifier (e.g. [Sales]).
            suffix = f"|{obj}"
            if any(k.endswith(suffix) for k in removed_measures):
                return m.group(0).strip()
    return None


def _rewrite_field_parameter_dax(
    dax, removed_tables, removed_columns, removed_measures
):
    """Rewrite a field-parameter table-constructor DAX, dropping the individual
    rows whose field references a migrated-away object. Returns a tuple of the
    rewritten DAX (empty string when every row was dropped) and the list of
    removed field references."""
    removed = []
    if not dax:
        return dax, removed
    open_i = dax.find("{")
    close_i = dax.rfind("}")
    if open_i < 0 or close_i <= open_i:
        return dax, removed
    prefix = dax[: open_i + 1]
    suffix = dax[close_i:]
    body = dax[open_i + 1 : close_i]
    kept = []
    for row in _split_top_level(body):
        if not row.strip():
            continue
        dead = _row_removed_reference(
            row, removed_tables, removed_columns, removed_measures
        )
        if dead is not None:
            removed.append(dead)
        else:
            kept.append(row.strip())
    if not kept:
        return "", removed
    return prefix + "\n\t" + ",\n\t".join(kept) + "\n" + suffix, removed


def _expand_dependents(
    dep_df, removed_tables, removed_columns, removed_measures, unsupported
):
    """Transitively add measures / calculated columns that reference a removed
    object to the removal sets (mirrors the reference migration tool)."""
    if dep_df is None or getattr(dep_df, "empty", True):
        return

    def _clean(v):
        # Coerce None / NaN (v != v) to an empty string.
        return "" if v is None or (isinstance(v, float) and v != v) else str(v)

    rows = [
        {
            "table": _clean(r["Table Name"]),
            "object": _clean(r["Object Name"]),
            "type": _clean(r["Object Type"]).upper().replace(" ", "_"),
            "ref_table": _clean(r["Referenced Table"]),
            "ref_object": _clean(r["Referenced Object"]),
        }
        for _, r in dep_df.iterrows()
    ]

    def _is_removed_ref(ref_table, ref_object):
        if ref_table in removed_tables:
            return True
        key = _mig_key(ref_table, ref_object)
        return key in removed_columns or key in removed_measures

    changed = True
    while changed:
        changed = False
        for r in rows:
            if r["table"] in removed_tables:
                continue
            if not _is_removed_ref(r["ref_table"], r["ref_object"]):
                continue
            key = _mig_key(r["table"], r["object"])
            reason = (
                f"Depends on '{r['ref_table']}'"
                + (f"[{r['ref_object']}]" if r["ref_object"] else "")
                + ", which is not migrated."
            )
            if "MEASURE" in r["type"] and key not in removed_measures:
                removed_measures.add(key)
                changed = True
                unsupported.append(
                    {
                        "category": "Dependent objects",
                        "table": r["table"],
                        "name": r["object"],
                        "reason": reason,
                    }
                )
            elif "CALC_COLUMN" in r["type"] and key not in removed_columns:
                removed_columns.add(key)
                changed = True
                unsupported.append(
                    {
                        "category": "Dependent objects",
                        "table": r["table"],
                        "name": r["object"],
                        "reason": reason,
                    }
                )


_WIDGET_CSS = """
.slls-mdl {
    --slls-bg-solid: #ffffff;
    --slls-surface: rgba(255, 255, 255, 0.85);
    --slls-surface-2: rgba(0, 0, 0, 0.025);
    --slls-border: rgba(0, 0, 0, 0.08);
    --slls-border-strong: rgba(0, 0, 0, 0.14);
    --slls-text: #1d1d1f;
    --slls-text-secondary: #6e6e73;
    --slls-text-tertiary: #8a8a8e;
    --slls-accent: #007AFF;
    --slls-accent-hover: #0a6cdb;
    --slls-accent-soft: rgba(0, 122, 255, 0.12);
    --slls-orange: #FF9500;
    --slls-orange-soft: rgba(255, 149, 0, 0.14);
    --slls-success: #34c759;
    --slls-success-soft: rgba(52, 199, 89, 0.14);
    --slls-danger: #ff3b30;
    --slls-danger-soft: rgba(255, 59, 48, 0.12);
    --slls-radius: 14px;
    --slls-radius-sm: 8px;
    --slls-shadow: 0 1px 2px rgba(0,0,0,0.04), 0 8px 24px rgba(0,0,0,0.06);
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "SF Pro Display",
        "Helvetica Neue", Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    color: var(--slls-text);
    width: 100%;
    max-width: 960px;
    background: var(--slls-bg-solid);
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    box-shadow: var(--slls-shadow);
    padding: 24px;
    box-sizing: border-box;
    position: relative;
}
@media (prefers-color-scheme: dark) { .slls-mdl.slls-mdl-auto { --slls-bg-solid: #1c1c1e; --slls-surface: rgba(255,255,255,0.04); --slls-surface-2: rgba(255,255,255,0.03); --slls-border: rgba(255,255,255,0.08); --slls-border-strong: rgba(255,255,255,0.16); --slls-text: #f5f5f7; --slls-text-secondary: #a1a1a6; --slls-text-tertiary: #6e6e73; --slls-accent-soft: rgba(10,132,255,0.18); --slls-accent: #0A84FF; --slls-shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.5); } }
.slls-mdl.slls-mdl-dark { --slls-bg-solid: #1c1c1e; --slls-surface: rgba(255,255,255,0.04); --slls-surface-2: rgba(255,255,255,0.03); --slls-border: rgba(255,255,255,0.08); --slls-border-strong: rgba(255,255,255,0.16); --slls-text: #f5f5f7; --slls-text-secondary: #a1a1a6; --slls-text-tertiary: #6e6e73; --slls-accent-soft: rgba(10,132,255,0.18); --slls-accent: #0A84FF; --slls-shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.5); }
.slls-mdl * { box-sizing: border-box; }

/* Fullscreen: fill the viewport and drop the framing chrome. Notebook hosts
   often block the native Fullscreen API, so a CSS overlay (position: fixed
   covering the viewport) is the reliable primary mechanism; native fullscreen
   is attempted as a best-effort enhancement. */
.slls-mdl.slls-mdl-fs { position: fixed; inset: 0; z-index: 2147483000; width: 100vw; height: 100vh; max-width: none; margin: 0; border: none; border-radius: 0; box-shadow: none; overflow: auto; }
.slls-mdl:fullscreen, .slls-mdl:-webkit-full-screen { width: 100vw; height: 100vh; max-width: none; margin: 0; border: none; border-radius: 0; box-shadow: none; overflow: auto; }

.slls-mdl-header { display: flex; align-items: center; gap: 12px; margin-bottom: 18px; flex-wrap: wrap; }
.slls-mdl-badge { display: inline-flex; align-items: center; justify-content: center; width: 40px; height: 40px; border-radius: 12px; background: var(--slls-accent-soft); color: var(--slls-accent); flex-shrink: 0; }
.slls-mdl-badge svg { width: 22px; height: 22px; }
.slls-mdl-titlewrap { display: flex; flex-direction: column; margin-right: auto; min-width: 0; }
.slls-mdl-title { font-size: 20px; font-weight: 600; letter-spacing: -0.01em; line-height: 1.15; }
.slls-mdl-subtitle { font-size: 12px; color: var(--slls-text-secondary); margin-top: 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 620px; }
.slls-mdl-subtitle .slls-mdl-sep { color: var(--slls-text-tertiary); margin: 0 6px; }
.slls-mdl-subtitle b { color: var(--slls-text); font-weight: 500; }
.slls-mdl-hdr-ctrls { display: flex; align-items: center; gap: 8px; }

.slls-mdl-steps { display: flex; align-items: center; gap: 6px; margin-bottom: 18px; flex-wrap: wrap; }
.slls-mdl-step { display: flex; align-items: center; gap: 7px; font-size: 12.5px; color: var(--slls-text-tertiary); }
.slls-mdl-step-dot { width: 22px; height: 22px; border-radius: 50%; display: inline-flex; align-items: center; justify-content: center; font-size: 12px; font-weight: 600; border: 1px solid var(--slls-border-strong); background: var(--slls-surface); }
.slls-mdl-step.is-active { color: var(--slls-text); font-weight: 600; }
.slls-mdl-step.is-active .slls-mdl-step-dot { background: var(--slls-accent); border-color: var(--slls-accent); color: #fff; }
.slls-mdl-step.is-done .slls-mdl-step-dot { background: var(--slls-success); border-color: var(--slls-success); color: #fff; }
.slls-mdl-step-line { width: 22px; height: 1px; background: var(--slls-border-strong); }

.slls-mdl-select, .slls-mdl-input {
    appearance: none; -webkit-appearance: none;
    background: var(--slls-surface);
    border: 1px solid var(--slls-border-strong);
    border-radius: 999px;
    padding: 8px 14px;
    font-size: 13.5px;
    color: var(--slls-text);
    font-family: inherit;
    width: 100%;
    transition: border-color 120ms ease, box-shadow 120ms ease;
}
.slls-mdl-select {
    cursor: pointer;
    padding-right: 32px;
    background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6'><path fill='%236e6e73' d='M0 0l5 6 5-6z'/></svg>");
    background-repeat: no-repeat;
    background-position: right 12px center;
    text-overflow: ellipsis; overflow: hidden; white-space: nowrap;
}
.slls-mdl-select:hover, .slls-mdl-input:hover { border-color: var(--slls-text-tertiary); }
.slls-mdl-select:focus, .slls-mdl-input:focus { outline: none; border-color: var(--slls-accent); box-shadow: 0 0 0 3px var(--slls-accent-soft); }
.slls-mdl-select option, .slls-mdl-select optgroup { background: #ffffff; color: #1d1d1f; }
@media (prefers-color-scheme: dark) { .slls-mdl.slls-mdl-auto .slls-mdl-select option { background: #2c2c2e; color: #f5f5f7; } }
.slls-mdl.slls-mdl-dark .slls-mdl-select option { background: #2c2c2e; color: #f5f5f7; }
.slls-mdl-input::placeholder { color: var(--slls-text-tertiary); }
.slls-mdl-input:disabled { opacity: 0.5; cursor: not-allowed; }

.slls-mdl-btn {
    appearance: none;
    border: 1px solid var(--slls-border-strong);
    background: var(--slls-surface);
    color: var(--slls-text);
    font-family: inherit; font-size: 13.5px; font-weight: 500;
    padding: 8px 16px;
    border-radius: 999px;
    cursor: pointer;
    display: inline-flex; align-items: center; gap: 7px;
    transition: background 120ms ease, border-color 120ms ease, transform 80ms ease, box-shadow 120ms ease, opacity 120ms ease;
}
.slls-mdl-btn svg { width: 15px; height: 15px; }
.slls-mdl-btn:hover { background: var(--slls-surface-2); border-color: var(--slls-text-tertiary); }
.slls-mdl-btn:active { transform: scale(0.97); }
.slls-mdl-btn:disabled { opacity: 0.4; cursor: not-allowed; }
.slls-mdl-btn-primary { background: var(--slls-accent); border-color: var(--slls-accent); color: #fff; }
.slls-mdl-btn-primary:hover { background: var(--slls-accent-hover); border-color: var(--slls-accent-hover); }
.slls-mdl-btn-icon { width: 32px; height: 32px; padding: 0; justify-content: center; border-radius: 50%; }
.slls-mdl-btn-icon svg { width: 17px; height: 17px; }

.slls-mdl-body { min-height: 220px; }
.slls-mdl-footer { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-top: 22px; }
.slls-mdl-footer-end { justify-content: flex-end; }

.slls-mdl-field { display: flex; flex-direction: column; gap: 5px; margin-bottom: 14px; }
.slls-mdl-label { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; color: var(--slls-text-tertiary); }
.slls-mdl-grid { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 14px; }
.slls-mdl-grid > .slls-mdl-field { margin-bottom: 0; min-width: 0; }

.slls-mdl-stats { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin-bottom: 18px; }
.slls-mdl-stat { border: 1px solid var(--slls-border); border-radius: var(--slls-radius-sm); background: var(--slls-surface); padding: 14px 16px; display: flex; flex-direction: column; gap: 4px; }
.slls-mdl-stat-label { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; color: var(--slls-text-tertiary); }
.slls-mdl-stat-value { font-size: 26px; font-weight: 600; letter-spacing: -0.02em; }
.slls-mdl-stat.is-ok { border-color: rgba(52,199,89,0.3); background: var(--slls-success-soft); }
.slls-mdl-stat.is-ok .slls-mdl-stat-value { color: var(--slls-success); }
.slls-mdl-stat.is-warn { border-color: rgba(255,149,0,0.3); background: var(--slls-orange-soft); }
.slls-mdl-stat.is-warn .slls-mdl-stat-value { color: var(--slls-orange); }

.slls-mdl-link { display: inline-flex; align-items: center; gap: 6px; color: var(--slls-accent); font-size: 13px; font-weight: 500; text-decoration: none; margin-bottom: 14px; }
.slls-mdl-link:hover { text-decoration: underline; }
.slls-mdl-link svg { width: 14px; height: 14px; }

.slls-mdl-group { border: 1px solid var(--slls-border); border-radius: var(--slls-radius-sm); overflow: hidden; margin-bottom: 12px; }
.slls-mdl-group-head { background: var(--slls-surface-2); padding: 8px 14px; font-size: 12.5px; font-weight: 600; }
.slls-mdl-group-item { padding: 8px 14px; border-top: 1px solid var(--slls-border); display: flex; flex-direction: column; gap: 2px; }
.slls-mdl-group-item .slls-mdl-obj { font-size: 13px; }
.slls-mdl-group-item .slls-mdl-obj .slls-mdl-tblname { color: var(--slls-text-tertiary); }
.slls-mdl-group-item .slls-mdl-reason { font-size: 11.5px; color: var(--slls-text-secondary); }

.slls-mdl-note { display: flex; align-items: flex-start; gap: 10px; border-radius: var(--slls-radius-sm); padding: 12px 14px; margin-bottom: 14px; font-size: 12.5px; line-height: 1.45; }
.slls-mdl-note svg { width: 16px; height: 16px; flex-shrink: 0; margin-top: 1px; }
.slls-mdl-note-warn { background: var(--slls-orange-soft); color: var(--slls-text); }
.slls-mdl-note-warn svg { color: var(--slls-orange); }
.slls-mdl-note-err { background: var(--slls-danger-soft); color: var(--slls-text); }
.slls-mdl-note-err svg { color: var(--slls-danger); }
.slls-mdl-note-info { background: var(--slls-accent-soft); color: var(--slls-text); }
.slls-mdl-note-info svg { color: var(--slls-accent); }

.slls-mdl-radio { display: flex; align-items: flex-start; gap: 10px; border: 1px solid var(--slls-border-strong); border-radius: var(--slls-radius-sm); padding: 12px 14px; cursor: pointer; margin-bottom: 10px; transition: border-color 120ms ease, background 120ms ease; }
.slls-mdl-radio:hover { background: var(--slls-surface-2); }
.slls-mdl-radio.is-selected { border-color: var(--slls-accent); background: var(--slls-accent-soft); }
.slls-mdl-radio input { margin-top: 3px; accent-color: var(--slls-accent); }
.slls-mdl-radio-title { font-size: 13.5px; font-weight: 500; }
.slls-mdl-radio-desc { font-size: 12px; color: var(--slls-text-secondary); line-height: 1.4; margin-top: 2px; }

.slls-mdl-code { background: var(--slls-surface-2); border: 1px solid var(--slls-border); border-radius: var(--slls-radius-sm); padding: 12px 14px; font-family: "SF Mono", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; line-height: 1.5; white-space: pre-wrap; word-break: break-word; max-height: 260px; overflow: auto; margin-bottom: 14px; }
.slls-mdl-plan { border: 1px solid var(--slls-border); border-radius: var(--slls-radius-sm); overflow: hidden; margin-bottom: 14px; }
.slls-mdl-plan-row { padding: 8px 14px; border-top: 1px solid var(--slls-border); display: flex; align-items: center; gap: 8px; font-size: 13px; }
.slls-mdl-plan-row:first-child { border-top: none; }
.slls-mdl-plan-row svg { width: 15px; height: 15px; color: var(--slls-text-tertiary); flex-shrink: 0; }
.slls-mdl-plan-row .slls-mdl-cols { color: var(--slls-text-tertiary); font-size: 12px; margin-left: auto; }

.slls-mdl-table { width: 100%; border-collapse: collapse; font-size: 12.5px; margin-bottom: 14px; }
.slls-mdl-table th, .slls-mdl-table td { text-align: left; padding: 7px 10px; border-bottom: 1px solid var(--slls-border); }
.slls-mdl-table th { font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em; color: var(--slls-text-tertiary); font-weight: 600; }
.slls-mdl-pill { display: inline-block; padding: 1px 8px; border-radius: 999px; font-size: 11px; font-weight: 600; }
.slls-mdl-pill-ok { background: var(--slls-success-soft); color: var(--slls-success); }
.slls-mdl-pill-no { background: var(--slls-danger-soft); color: var(--slls-danger); }

.slls-mdl-center { display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 12px; padding: 40px 12px; text-align: center; color: var(--slls-text-secondary); }
.slls-mdl-center svg { width: 40px; height: 40px; }
.slls-mdl-center .slls-mdl-big { font-size: 17px; font-weight: 600; color: var(--slls-text); }
.slls-mdl-success-icon { color: var(--slls-success); }

.slls-mdl-spin { width: 16px; height: 16px; border: 2px solid var(--slls-border-strong); border-top-color: var(--slls-accent); border-radius: 50%; display: inline-block; animation: slls-mdl-spin 0.7s linear infinite; }
@keyframes slls-mdl-spin { to { transform: rotate(360deg); } }

.slls-mdl-steplist { margin: 6px 0 14px 0; padding-left: 18px; }
.slls-mdl-steplist li { font-size: 12.5px; line-height: 1.6; color: var(--slls-text-secondary); }
.slls-mdl-steplist li b { color: var(--slls-text); font-weight: 500; }
.slls-mdl-sec-title { font-size: 13.5px; font-weight: 600; margin: 4px 0 8px 0; }
"""


_WIDGET_JS = r"""
function render({ model, el }) {
    const root = document.createElement("div");
    root.className = "slls-mdl";
    el.appendChild(root);

    const IC = {
        database: `__IC_DATABASE__`,
        check: `__IC_CHECK__`,
        alert: `__IC_ALERT__`,
        ext: `__IC_EXT__`,
        close: `__IC_CLOSE__`,
        back: `__IC_BACK__`,
        sun: `__IC_SUN__`,
        moon: `__IC_MOON__`,
        table: `__IC_TABLE__`,
        chevron: `__IC_CHEVRON__`,
        fullscreen: `__IC_FULLSCREEN__`,
        fullscreen_exit: `__IC_FULLSCREEN_EXIT__`,
    };

    function esc(s) {
        return String(s ?? "").replace(/[&<>"']/g, (c) => ({
            "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
        }[c]));
    }

    function applyTheme() {
        root.classList.remove("slls-mdl-dark", "slls-mdl-auto");
        root.classList.add(model.get("dark_mode") ? "slls-mdl-dark" : "slls-mdl-auto");
    }
    applyTheme();
    model.on("change:dark_mode", applyTheme);

    // --- Fullscreen ---
    // Notebook hosts (VS Code, Jupyter, Fabric) frequently sandbox the widget
    // output. Toggle a CSS overlay on the root (reliable) and additionally
    // attempt the native Fullscreen API (best-effort).
    let fsMode = false;
    function setFullscreen(on) {
        fsMode = on;
        root.classList.toggle("slls-mdl-fs", on);
        try {
            if (on) {
                const req = root.requestFullscreen || root.webkitRequestFullscreen;
                if (req) { const p = req.call(root); if (p && p.catch) p.catch(() => {}); }
            } else {
                const ex = document.exitFullscreen || document.webkitExitFullscreen;
                if (ex && (document.fullscreenElement || document.webkitFullscreenElement)) {
                    const p = ex.call(document); if (p && p.catch) p.catch(() => {});
                }
            }
        } catch (e) { /* native fullscreen blocked; CSS overlay handles it */ }
        route();
    }
    function onFsChange() {
        // If the user left native fullscreen (Esc / F11), drop the overlay too.
        const nativeOn = !!(document.fullscreenElement || document.webkitFullscreenElement);
        if (!nativeOn && fsMode) { fsMode = false; root.classList.remove("slls-mdl-fs"); route(); }
    }
    document.addEventListener("fullscreenchange", onFsChange);
    document.addEventListener("webkitfullscreenchange", onFsChange);
    document.addEventListener("keydown", (e) => { if (e.key === "Escape" && fsMode) setFullscreen(false); });
        model.set("pending_action", Object.assign({ action: action }, extra || {}));
        model.set("run", (model.get("run") || 0) + 1);
        model.save_changes();
    }

    function busy() { return model.get("busy") === true; }

    function themeBtn() {
        const dark = model.get("dark_mode");
        return `<button class="slls-mdl-btn slls-mdl-btn-icon" data-r="theme" title="Toggle theme" aria-label="Toggle theme">${dark ? IC.sun : IC.moon}</button>`;
    }

    function fsBtn() {
        const label = fsMode ? "Exit full screen" : "Toggle full screen";
        return `<button class="slls-mdl-btn slls-mdl-btn-icon" data-r="fullscreen" title="${label}" aria-label="${label}">${fsMode ? IC.fullscreen_exit : IC.fullscreen}</button>`;
    }

    function headerHtml() {
        const ds = model.get("dataset_name") || "Semantic model";
        const ws = model.get("workspace_name") || "";
        const sub = ws ? `<b>${esc(ds)}</b><span class="slls-mdl-sep">·</span>${esc(ws)}` : `<b>${esc(ds)}</b>`;
        return `<div class="slls-mdl-header">
            <span class="slls-mdl-badge">${IC.database}</span>
            <div class="slls-mdl-titlewrap">
                <div class="slls-mdl-title">Migrate to Direct Lake</div>
                <div class="slls-mdl-subtitle">${sub}</div>
            </div>
            <div class="slls-mdl-hdr-ctrls">${fsBtn()}${themeBtn()}</div>
        </div>`;
    }

    const STEPS = [
        { key: "analyze", label: "Analyze" },
        { key: "configure", label: "Configure" },
        { key: "preview", label: "Preview" },
        { key: "done", label: "Done" },
    ];

    function stepsHtml() {
        const cur = model.get("screen") || "analyze";
        const curIdx = STEPS.findIndex((s) => s.key === cur);
        let out = '<div class="slls-mdl-steps">';
        STEPS.forEach((s, i) => {
            let cls = "slls-mdl-step";
            if (i === curIdx) cls += " is-active";
            else if (i < curIdx) cls += " is-done";
            const dot = i < curIdx ? IC.check : String(i + 1);
            out += `<div class="${cls}"><span class="slls-mdl-step-dot">${dot}</span>${esc(s.label)}</div>`;
            if (i < STEPS.length - 1) out += '<span class="slls-mdl-step-line"></span>';
        });
        out += "</div>";
        return out;
    }

    function statusHtml() {
        const st = model.get("status") || {};
        if (!st.message || st.kind !== "error") return "";
        return `<div class="slls-mdl-note slls-mdl-note-err">${IC.alert}<div>${esc(st.message)}</div></div>`;
    }

    function analyzeHtml() {
        const a = model.get("analysis") || {};
        if (busy() && !a.ready) {
            return `<div class="slls-mdl-center"><span class="slls-mdl-spin"></span><div>Analyzing the model…</div></div>`;
        }
        if (!a.ready) {
            return `<div class="slls-mdl-center"><div>No analysis yet.</div><button class="slls-mdl-btn slls-mdl-btn-primary" data-r="analyze">Analyze model</button></div>`;
        }
        const dropped = a.droppedTables || 0;
        let out = `<div class="slls-mdl-stats">
            <div class="slls-mdl-stat"><span class="slls-mdl-stat-label">Tables</span><span class="slls-mdl-stat-value">${a.totalTables || 0}</span></div>
            <div class="slls-mdl-stat is-ok"><span class="slls-mdl-stat-label">Will migrate</span><span class="slls-mdl-stat-value">${a.migratedTables || 0}</span></div>
            <div class="slls-mdl-stat ${dropped > 0 ? "is-warn" : ""}"><span class="slls-mdl-stat-label">Will be dropped</span><span class="slls-mdl-stat-value">${dropped}</span></div>
        </div>`;
        out += `<a class="slls-mdl-link" href="${esc(a.docsUrl || "")}" target="_blank" rel="noopener noreferrer">${IC.ext}Direct Lake considerations and limitations</a>`;

        if (a.isDirectLake) {
            out += `<div class="slls-mdl-note slls-mdl-note-err">${IC.alert}<div>This model already uses Direct Lake and cannot be migrated.</div></div>`;
        }
        const roles = a.removedRoles || [];
        if (roles.length > 0) {
            out += `<div class="slls-mdl-note slls-mdl-note-warn">${IC.alert}<div>The model has ${roles.length} security role${roles.length === 1 ? "" : "s"} (<b>${esc(roles.join(", "))}</b>). Re-create the equivalent security on the new model using OneLake security.</div></div>`;
        }

        const fpCount = a.fieldParameterCount || 0;
        const cgCount = a.calcGroupCount || 0;
        if (fpCount + cgCount > 0) {
            const bits = [];
            if (cgCount > 0) bits.push(`${cgCount} calculation group${cgCount === 1 ? "" : "s"}`);
            if (fpCount > 0) bits.push(`${fpCount} field parameter${fpCount === 1 ? "" : "s"}`);
            out += `<div class="slls-mdl-note slls-mdl-note-info">${IC.database}<div><b>${esc(bits.join(" and "))}</b> will be migrated (both are supported in Direct Lake).</div></div>`;
        }

        const fpChanges = (a.fieldParameterChanges || []).filter((f) => !f.dropped && (f.removed || []).length > 0);
        fpChanges.forEach((f) => {
            out += `<div class="slls-mdl-note slls-mdl-note-warn">${IC.alert}<div>Field parameter <b>${esc(f.name)}</b> will be migrated with ${f.removed.length} field reference${f.removed.length === 1 ? "" : "s"} removed (they point at objects that won't be in the migrated model): <b>${esc(f.removed.join(", "))}</b>.</div></div>`;
        });

        const groups = a.unsupportedGroups || [];
        if (groups.length > 0) {
            out += `<div class="slls-mdl-sec-title">Objects that won't be migrated</div>`;
            groups.forEach((g) => {
                out += `<div class="slls-mdl-group"><div class="slls-mdl-group-head">${esc(g.category)} (${g.items.length})</div>`;
                g.items.forEach((it) => {
                    const tbl = it.table ? `<span class="slls-mdl-tblname">${esc(it.table)} · </span>` : "";
                    out += `<div class="slls-mdl-group-item"><span class="slls-mdl-obj">${tbl}${esc(it.name)}</span>${it.reason ? `<span class="slls-mdl-reason">${esc(it.reason)}</span>` : ""}</div>`;
                });
                out += `</div>`;
            });
        }
        return out;
    }

    function optionsHtml(list, selected, placeholder) {
        let out = "";
        if (placeholder) out += `<option value="">${esc(placeholder)}</option>`;
        (list || []).forEach((it) => {
            out += `<option value="${esc(it.id)}"${it.id === selected ? " selected" : ""}>${esc(it.name)}</option>`;
        });
        return out;
    }

    function currentSourceItems() {
        const ws = model.get("source_workspace_id");
        const type = model.get("source_type") || "Lakehouse";
        const map = model.get("source_items") || {};
        return map[`${ws}::${type}`];
    }

    function configureHtml() {
        const workspaces = model.get("workspaces") || [];
        const type = model.get("source_type") || "Lakehouse";
        const srcItems = currentSourceItems();
        const schema = model.get("schema") || "";
        const schemaDisabled = type === "Lakehouse" && model.get("lakehouse_schema_enabled") === false;
        const movement = model.get("data_movement") || "manual";
        const typeLower = type.toLowerCase();

        let out = `<div class="slls-mdl-field">
            <span class="slls-mdl-label">New model name</span>
            <input class="slls-mdl-input" data-r="name" value="${esc(model.get("new_model_name") || "")}" placeholder="My model (Direct Lake)" />
        </div>`;

        out += `<div class="slls-mdl-field">
            <span class="slls-mdl-label">Target workspace</span>
            <select class="slls-mdl-select" data-r="target_ws">${optionsHtml(workspaces, model.get("target_workspace_id"), "Select a workspace…")}</select>
        </div>`;

        out += `<div class="slls-mdl-grid">
            <div class="slls-mdl-field">
                <span class="slls-mdl-label">Source type</span>
                <select class="slls-mdl-select" data-r="src_type">
                    ${(model.get("source_types") || []).map((t) => `<option value="${esc(t)}"${t === type ? " selected" : ""}>${esc(t)}</option>`).join("")}
                </select>
            </div>
            <div class="slls-mdl-field">
                <span class="slls-mdl-label">Source workspace</span>
                <select class="slls-mdl-select" data-r="src_ws">${optionsHtml(workspaces, model.get("source_workspace_id"), "Select a workspace…")}</select>
            </div>
        </div>`;

        const itemsPlaceholder = srcItems === undefined ? "Loading…" : `No ${typeLower}s`;
        out += `<div class="slls-mdl-grid">
            <div class="slls-mdl-field">
                <span class="slls-mdl-label">${esc(type)}</span>
                <select class="slls-mdl-select" data-r="src_item"${srcItems === undefined ? " disabled" : ""}>${optionsHtml(srcItems || [], model.get("source_item_id"), itemsPlaceholder)}</select>
            </div>
            <div class="slls-mdl-field">
                <span class="slls-mdl-label">Schema ${schemaDisabled ? "(not used)" : type === "Warehouse" ? "(required)" : "(optional)"}</span>
                <input class="slls-mdl-input" data-r="schema" value="${esc(schemaDisabled ? "" : schema)}"${schemaDisabled ? " disabled" : ""} placeholder="${schemaDisabled ? "Not a schema-enabled lakehouse" : type === "Warehouse" ? "dbo" : "(default)"}" />
            </div>
        </div>`;

        out += `<div class="slls-mdl-sec-title" style="margin-top:8px;">Load the data</div>`;
        out += `<label class="slls-mdl-radio ${movement === "manual" ? "is-selected" : ""}">
            <input type="radio" name="mv" data-r="mv-manual"${movement === "manual" ? " checked" : ""} />
            <span><span class="slls-mdl-radio-title">I'll move the data myself</span><span class="slls-mdl-radio-desc">Load each table into the ${esc(typeLower)} as a delta table whose name matches the model's table (spaces become underscores), then refresh the model.</span></span>
        </label>`;
        out += `<label class="slls-mdl-radio ${movement === "pqt" ? "is-selected" : ""}">
            <input type="radio" name="mv" data-r="mv-pqt"${movement === "pqt" ? " checked" : ""} />
            <span><span class="slls-mdl-radio-title">Generate a Power Query template (.pqt)</span><span class="slls-mdl-radio-desc">Save a .pqt to the attached lakehouse's Files, then import it as a Dataflow Gen2 to load the data.</span></span>
        </label>`;

        if (movement === "pqt") {
            out += `<div class="slls-mdl-field" style="margin-top:6px;">
                <span class="slls-mdl-label">Template name</span>
                <input class="slls-mdl-input" data-r="template" value="${esc(model.get("template_name") || "")}" placeholder="Template name" />
            </div>`;
        }
        return out;
    }

    function previewHtml() {
        const p = model.get("preview") || {};
        if (busy() && !p.ready) {
            return `<div class="slls-mdl-center"><span class="slls-mdl-spin"></span><div>Building the preview…</div></div>`;
        }
        if (!p.ready) return `<div class="slls-mdl-center"><div>No preview yet.</div></div>`;
        let out = `<div class="slls-mdl-note slls-mdl-note-info">${IC.database}<div>This is the plan for <b>${esc(model.get("new_model_name") || "")}</b>. ${p.pqt ? "A Power Query template (.pqt) will also be saved to the lakehouse to load the data." : "No data is moved — only the model structure is created."}</div></div>`;
        if (p.expression) {
            out += `<div class="slls-mdl-sec-title">Source connection (Power Query M)</div>`;
            out += `<div class="slls-mdl-code">${esc(p.expression)}</div>`;
        }
        const tables = p.tables || [];
        out += `<div class="slls-mdl-sec-title">Tables to create (${tables.length})</div>`;
        out += `<div class="slls-mdl-plan">`;
        tables.forEach((t) => {
            const kindLabel = t.kind === "fieldParameter" ? "field parameter" : t.kind === "calculationGroup" ? "calculation group" : "";
            const countLabel = t.kind === "calculationGroup"
                ? `${t.columnCount} calculation item${t.columnCount === 1 ? "" : "s"}`
                : `${t.columnCount} column${t.columnCount === 1 ? "" : "s"}`;
            const nameHtml = kindLabel
                ? `${esc(t.name)} <span class="slls-mdl-tblname">· ${kindLabel}</span>`
                : esc(t.name);
            out += `<div class="slls-mdl-plan-row">${IC.table}<span>${nameHtml}</span><span class="slls-mdl-cols">${countLabel}</span></div>`;
        });
        if (tables.length === 0) out += `<div class="slls-mdl-plan-row"><span>No eligible tables.</span></div>`;
        out += `</div>`;
        return out;
    }

    function doneHtml() {
        const r = model.get("result") || {};
        let out = `<div class="slls-mdl-center"><span class="slls-mdl-success-icon">${IC.check}</span><div class="slls-mdl-big">Model created</div><div><b>${esc(r.createdModel || "")}</b> was created in Direct Lake mode.</div></div>`;

        const warnings = r.warnings || [];
        warnings.forEach((w) => {
            out += `<div class="slls-mdl-note slls-mdl-note-warn">${IC.alert}<div>${esc(w)}</div></div>`;
        });

        if (r.pqt) {
            out += `<div class="slls-mdl-sec-title">Load the data</div>`;
            out += `<ol class="slls-mdl-steplist">
                <li>Open the target workspace in Fabric.</li>
                <li>Choose <b>Get data → New Dataflow Gen2</b>.</li>
                <li>Select <b>Import from a Power Query template</b> and pick <b>${esc(r.pqtFileName || "the .pqt")}</b>.</li>
                <li>Configure credentials, save the dataflow, then run it to load the data.</li>
                <li>Refresh (reframe) the new Direct Lake model.</li>
            </ol>`;
        } else {
            out += `<div class="slls-mdl-sec-title">Load the data</div>`;
            out += `<ol class="slls-mdl-steplist">
                <li>Load each table into the ${esc((r.sourceType || "lakehouse").toLowerCase())} as a delta table whose name matches the model's table (spaces become underscores).</li>
                <li>Refresh (reframe) the new Direct Lake model before using it.</li>
            </ol>`;
        }

        const rows = r.validation || [];
        if (rows.length > 0) {
            out += `<div class="slls-mdl-sec-title">Migration validation</div>`;
            out += `<table class="slls-mdl-table"><thead><tr><th>Object type</th><th>Migrated</th><th>Total</th><th>Status</th></tr></thead><tbody>`;
            rows.forEach((v) => {
                const ok = v.migrated >= v.total && v.total > 0;
                out += `<tr><td>${esc(v.objectType)}</td><td>${v.migrated}</td><td>${v.total}</td><td>${v.total === 0 ? "—" : `<span class="slls-mdl-pill ${ok ? "slls-mdl-pill-ok" : "slls-mdl-pill-no"}">${ok ? "Complete" : "Partial"}</span>`}</td></tr>`;
            });
            out += `</tbody></table>`;
        }
        return out;
    }

    function footerHtml() {
        const screen = model.get("screen") || "analyze";
        const a = model.get("analysis") || {};
        const p = model.get("preview") || {};
        const b = busy();
        const spin = `<span class="slls-mdl-spin"></span>`;
        if (screen === "analyze") {
            const canGo = a.ready && !a.isDirectLake && !b;
            return `<div class="slls-mdl-footer slls-mdl-footer-end">
                <button class="slls-mdl-btn slls-mdl-btn-primary" data-r="to-configure"${canGo ? "" : " disabled"}>Accept and continue ${IC.chevron}</button>
            </div>`;
        }
        if (screen === "configure") {
            return `<div class="slls-mdl-footer">
                <button class="slls-mdl-btn" data-r="to-analyze">${IC.back} Back</button>
                <button class="slls-mdl-btn slls-mdl-btn-primary" data-r="to-preview"${b ? " disabled" : ""}>${b ? spin : ""} Preview model</button>
            </div>`;
        }
        if (screen === "preview") {
            return `<div class="slls-mdl-footer">
                <button class="slls-mdl-btn" data-r="to-configure2">${IC.back} Back</button>
                <button class="slls-mdl-btn slls-mdl-btn-primary" data-r="create"${b || !p.ready ? " disabled" : ""}>${b ? spin : IC.database} Create Direct Lake model</button>
            </div>`;
        }
        return `<div class="slls-mdl-footer slls-mdl-footer-end">
            <button class="slls-mdl-btn slls-mdl-btn-primary" data-r="close">Done</button>
        </div>`;
    }

    function bodyHtml() {
        const screen = model.get("screen") || "analyze";
        if (screen === "configure") return configureHtml();
        if (screen === "preview") return previewHtml();
        if (screen === "done") return doneHtml();
        return analyzeHtml();
    }

    function route() {
        const screen = model.get("screen") || "analyze";
        const showSteps = screen !== "done";
        root.innerHTML = headerHtml()
            + (showSteps ? stepsHtml() : "")
            + statusHtml()
            + `<div class="slls-mdl-body">${bodyHtml()}</div>`
            + footerHtml();
        wire();
    }

    function on(sel, ev, fn) {
        const node = root.querySelector(sel);
        if (node) node.addEventListener(ev, fn);
    }

    function wire() {
        on('[data-r="theme"]', "click", () => {
            model.set("dark_mode", !model.get("dark_mode"));
            model.save_changes();
            route();
        });
        on('[data-r="fullscreen"]', "click", () => setFullscreen(!fsMode));
        on('[data-r="close"]', "click", () => { root.innerHTML = ""; });
        on('[data-r="analyze"]', "click", () => runAction("analyze"));

        on('[data-r="to-configure"]', "click", () => { model.set("screen", "configure"); model.save_changes(); requestSourceItems(); });
        on('[data-r="to-analyze"]', "click", () => { model.set("screen", "analyze"); model.save_changes(); });
        on('[data-r="to-configure2"]', "click", () => { model.set("screen", "configure"); model.save_changes(); });

        on('[data-r="name"]', "input", (e) => { model.set("new_model_name", e.target.value); model.save_changes(); });
        on('[data-r="target_ws"]', "change", (e) => { model.set("target_workspace_id", e.target.value); model.save_changes(); });
        on('[data-r="src_type"]', "change", (e) => {
            model.set("source_type", e.target.value);
            model.set("source_item_id", "");
            model.save_changes();
            requestSourceItems();
            route();
        });
        on('[data-r="src_ws"]', "change", (e) => {
            model.set("source_workspace_id", e.target.value);
            model.set("source_item_id", "");
            model.save_changes();
            requestSourceItems();
            route();
        });
        on('[data-r="src_item"]', "change", (e) => { model.set("source_item_id", e.target.value); model.save_changes(); });
        on('[data-r="schema"]', "input", (e) => { model.set("schema", e.target.value); model.save_changes(); });
        on('[data-r="mv-manual"]', "change", () => { model.set("data_movement", "manual"); model.save_changes(); route(); });
        on('[data-r="mv-pqt"]', "change", () => { model.set("data_movement", "pqt"); model.save_changes(); route(); });
        on('[data-r="template"]', "input", (e) => { model.set("template_name", e.target.value); model.save_changes(); });

        on('[data-r="to-preview"]', "click", () => {
            const name = (model.get("new_model_name") || "").trim();
            const ws = model.get("target_workspace_id");
            const item = model.get("source_item_id");
            if (!name || !ws || !item) {
                model.set("status", { message: "New model name, target workspace, and a source are required.", kind: "error" });
                model.save_changes();
                route();
                return;
            }
            runAction("preview", {
                new_model_name: name,
                target_workspace_id: ws,
                source_type: model.get("source_type"),
                source_workspace_id: model.get("source_workspace_id"),
                source_item_id: item,
                schema: model.get("schema") || "",
                data_movement: model.get("data_movement") || "manual",
            });
        });

        on('[data-r="create"]', "click", () => {
            runAction("create", {
                new_model_name: (model.get("new_model_name") || "").trim(),
                target_workspace_id: model.get("target_workspace_id"),
                source_type: model.get("source_type"),
                source_workspace_id: model.get("source_workspace_id"),
                source_item_id: model.get("source_item_id"),
                schema: model.get("schema") || "",
                data_movement: model.get("data_movement") || "manual",
                template_name: (model.get("template_name") || "").trim(),
            });
        });
    }

    function requestSourceItems() {
        const ws = model.get("source_workspace_id");
        const type = model.get("source_type") || "Lakehouse";
        if (!ws) return;
        const map = model.get("source_items") || {};
        if (map[`${ws}::${type}`] !== undefined) return;
        runAction("list_source_items", { workspace_id: ws, source_type: type });
    }

    [
        "screen", "busy", "status", "analysis", "preview", "result",
        "workspaces", "source_items", "source_type", "source_workspace_id",
        "source_item_id", "data_movement", "lakehouse_schema_enabled",
        "dataset_name", "workspace_name",
    ].forEach((name) => model.on("change:" + name, route));

    route();

    const a0 = model.get("analysis") || {};
    if (!a0.ready) runAction("analyze");
}
export default { render };
"""


@log
def migrate_to_direct_lake(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    dark_mode: bool = False,
):
    """
    Generates an interactive wizard to migrate an import/DirectQuery semantic model to Direct Lake.

    The wizard follows a guided, four-step flow: it analyzes the source model
    (showing which tables migrate and which are dropped), lets you configure the
    new model name, target workspace and Direct Lake source (Lakehouse or
    Warehouse), previews the plan, and then creates the new Direct Lake semantic
    model. It orchestrates the Semantic Link Labs migration functions
    (:func:`sempy_labs.migration.create_pqt_file`,
    :func:`sempy_labs.create_blank_semantic_model`,
    :func:`sempy_labs.migration.migrate_tables_columns_to_semantic_model` and the
    related ``migrate_*`` helpers) under the hood.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the import/DirectQuery semantic model to migrate.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the source semantic model
        exists. Defaults to None which resolves to the workspace of the attached
        lakehouse or, if no lakehouse is attached, the workspace of the notebook.
    dark_mode : bool, default=False
        If True, renders the wizard with a dark color theme. If False, renders
        with a light color theme.
    """

    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "The 'migrate_to_direct_lake' function requires the 'anywidget' package. "
            "Install it with: pip install anywidget"
        ) from e

    import sempy.fabric as fabric
    from IPython.display import display
    from sempy_labs._helper_functions import (
        resolve_workspace_name_and_id,
        resolve_dataset_name_and_id,
    )
    from sempy_labs.tom import connect_semantic_model
    from sempy_labs.directlake._generate_shared_expression import (
        generate_shared_expression,
    )

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    workspace_id = str(workspace_id)
    dataset_name, dataset_id = resolve_dataset_name_and_id(dataset, workspace_id)
    dataset_id = str(dataset_id)

    # ---------------- Catalog helpers ----------------
    def _pick_columns(df, preferred_id, preferred_name):
        cols = list(df.columns)
        if not cols:
            return None, None
        id_col = next((c for c in preferred_id if c in cols), cols[0])
        name_col = next((c for c in preferred_name if c in cols), cols[-1])
        return id_col, name_col

    def _list_workspaces_payload():
        try:
            dfW = fabric.list_workspaces()
        except Exception:
            return [{"id": workspace_id, "name": str(workspace_name or "")}]
        id_col, name_col = _pick_columns(dfW, ["Id"], ["Name"])
        if id_col is None or name_col is None:
            return [{"id": workspace_id, "name": str(workspace_name or "")}]
        rows = [
            {"id": str(r[id_col]), "name": str(r[name_col])} for _, r in dfW.iterrows()
        ]
        rows.sort(key=lambda x: x["name"].lower())
        return rows

    def _list_source_items_payload(ws_id, source_type):
        try:
            dfI = fabric.list_items(workspace=ws_id, item_type=source_type)
        except Exception:
            return []
        id_col, name_col = _pick_columns(dfI, ["Id"], ["Display Name", "Name"])
        if id_col is None or name_col is None:
            return []
        rows = [
            {"id": str(r[id_col]), "name": str(r[name_col])} for _, r in dfI.iterrows()
        ]
        rows.sort(key=lambda x: x["name"].lower())
        return rows

    # ---------------- Migration plan ----------------
    def _compute_migration_plan(tom):
        """Classifies every table/column for Direct Lake migration and resolves
        field-parameter / calculation-group dependencies. Mirrors the reference
        migration tool (m-kovalsky/Two)."""
        import Microsoft.AnalysisServices.Tabular as TOM

        # Calculation dependencies drive the transitive removal of measures /
        # calculated columns. Best-effort: skip the expansion if unavailable.
        try:
            from sempy_labs._model_dependencies import (
                get_model_calc_dependencies,
            )

            dep_df = get_model_calc_dependencies(
                dataset=dataset_id, workspace=workspace_id
            )
        except Exception:
            dep_df = None

        total = 0
        removed_tables = set()
        removed_columns = set()
        removed_measures = set()
        field_parameters = {}  # table name -> calculated-table DAX expression
        calc_groups = []
        unsupported = []

        # --- Table-level classification ---
        for t in tom.model.Tables:
            total += 1
            tname = t.Name
            is_calc = any(
                getattr(p, "SourceType", None) == TOM.PartitionSourceType.Calculated
                for p in t.Partitions
            )

            # Calculation groups are supported in Direct Lake — kept unchanged.
            if t.CalculationGroup is not None:
                calc_groups.append(tname)
                continue

            # Field parameters are supported in Direct Lake — kept as calculated
            # tables. Dead field references are pruned from their DAX below.
            if is_calc and tom.is_field_parameter(table_name=tname):
                expr = ""
                for p in t.Partitions:
                    if (
                        getattr(p, "SourceType", None)
                        == TOM.PartitionSourceType.Calculated
                    ):
                        e = p.Source.Expression
                        if e and e.strip():
                            expr = e
                            break
                field_parameters[tname] = expr
                continue

            # Other calculated (DAX) tables are not supported.
            if is_calc:
                removed_tables.add(tname)
                unsupported.append(
                    {
                        "category": "Calculated tables",
                        "table": tname,
                        "name": tname,
                        "reason": "Calculated (DAX) tables are not supported in Direct Lake.",
                    }
                )
                continue

            # Aggregation tables are not supported.
            if any(c.AlternateOf is not None for c in t.Columns):
                removed_tables.add(tname)
                unsupported.append(
                    {
                        "category": "Aggregation tables",
                        "table": tname,
                        "name": tname,
                        "reason": "Aggregation tables are not supported in Direct Lake.",
                    }
                )
                continue

        # --- Column-level unsupported features (within surviving tables) ---
        for t in tom.model.Tables:
            tname = t.Name
            if (
                tname in removed_tables
                or tname in field_parameters
                or t.CalculationGroup is not None
            ):
                continue
            for c in t.Columns:
                if c.Type == TOM.ColumnType.RowNumber:
                    continue
                if c.Type == TOM.ColumnType.Calculated:
                    removed_columns.add(_mig_key(tname, c.Name))
                    unsupported.append(
                        {
                            "category": "Calculated columns",
                            "table": tname,
                            "name": c.Name,
                            "reason": "Calculated (DAX) columns are not supported in Direct Lake.",
                        }
                    )
                elif c.DataType == TOM.DataType.Binary:
                    removed_columns.add(_mig_key(tname, c.Name))
                    unsupported.append(
                        {
                            "category": "Binary columns",
                            "table": tname,
                            "name": c.Name,
                            "reason": "Columns of data type Binary are not supported in Direct Lake.",
                        }
                    )

        # --- Transitive dependents (measures / calc columns) ---
        _expand_dependents(
            dep_df, removed_tables, removed_columns, removed_measures, unsupported
        )

        # --- Field-parameter DAX rewrite (drop only dead field references) ---
        fp_changes = []
        for tname in list(field_parameters.keys()):
            rewritten, removed_refs = _rewrite_field_parameter_dax(
                field_parameters[tname],
                removed_tables,
                removed_columns,
                removed_measures,
            )
            if rewritten is None or not rewritten.strip():
                # Every field referenced a dropped object — drop the table.
                del field_parameters[tname]
                removed_tables.add(tname)
                unsupported.append(
                    {
                        "category": "Field parameters",
                        "table": tname,
                        "name": tname,
                        "reason": "Every field references an object that is not migrated.",
                    }
                )
                fp_changes.append(
                    {"name": tname, "removed": removed_refs, "dropped": True}
                )
            else:
                field_parameters[tname] = rewritten
                if removed_refs:
                    fp_changes.append(
                        {"name": tname, "removed": removed_refs, "dropped": False}
                    )

        # A field parameter may have been emptied (and thus removed); re-expand
        # so measures depending on it are dropped too.
        _expand_dependents(
            dep_df, removed_tables, removed_columns, removed_measures, unsupported
        )

        return {
            "totalTables": total,
            "removedTables": removed_tables,
            "removedColumns": removed_columns,
            "removedMeasures": removed_measures,
            "fieldParameters": field_parameters,
            "fieldParameterChanges": fp_changes,
            "calcGroups": calc_groups,
            "unsupported": unsupported,
        }

    # ---------------- Analysis ----------------
    def _analyze_model():
        with connect_semantic_model(
            dataset=dataset_id, workspace=workspace_id, readonly=True
        ) as tom:
            is_dl = tom.is_direct_lake()
            plan = _compute_migration_plan(tom)
            roles = [r.Name for r in tom.model.Roles]

        # Group the unsupported objects by category (in a stable order).
        order = [
            "Calculated tables",
            "Aggregation tables",
            "Field parameters",
            "Calculated columns",
            "Binary columns",
            "Dependent objects",
        ]
        grouped = {}
        for u in plan["unsupported"]:
            grouped.setdefault(u["category"], []).append(
                {
                    "table": u.get("table", ""),
                    "name": u["name"],
                    "reason": u["reason"],
                }
            )
        groups = []
        for cat in order:
            if cat in grouped:
                groups.append({"category": cat, "items": grouped.pop(cat)})
        for cat, items in grouped.items():
            groups.append({"category": cat, "items": items})

        total = plan["totalTables"]
        dropped = len(plan["removedTables"])
        migrated = total - dropped

        return {
            "ready": True,
            "isDirectLake": bool(is_dl),
            "totalTables": total,
            "migratedTables": migrated,
            "droppedTables": dropped,
            "fieldParameterCount": len(plan["fieldParameters"]),
            "calcGroupCount": len(plan["calcGroups"]),
            "fieldParameterChanges": plan["fieldParameterChanges"],
            "removedRoles": roles,
            "unsupportedGroups": groups,
            "docsUrl": _DIRECT_LAKE_DOCS_URL,
        }

    # ---------------- Preview ----------------
    def _build_preview(data):
        import Microsoft.AnalysisServices.Tabular as TOM

        src_type = data.get("source_type") or "Lakehouse"
        src_ws = data.get("source_workspace_id") or workspace_id
        src_id = data.get("source_item_id")
        try:
            expression = generate_shared_expression(
                item=src_id,
                item_type=src_type,
                workspace=src_ws,
                use_sql_endpoint=False,
            )
        except Exception as e:
            expression = f"(Could not generate the source expression: {e})"

        tables = []
        with connect_semantic_model(
            dataset=dataset_id, workspace=workspace_id, readonly=True
        ) as tom:
            plan = _compute_migration_plan(tom)
            removed = plan["removedTables"]
            removed_cols = plan["removedColumns"]
            field_params = plan["fieldParameters"]
            calc_groups = set(plan["calcGroups"])
            for t in tom.model.Tables:
                tname = t.Name
                if tname in removed:
                    continue
                if tname in field_params:
                    col_count = sum(
                        1
                        for c in t.Columns
                        if c.Type != TOM.ColumnType.RowNumber
                    )
                    tables.append(
                        {
                            "name": tname,
                            "columnCount": col_count,
                            "kind": "fieldParameter",
                        }
                    )
                    continue
                if tname in calc_groups:
                    ci_count = (
                        t.CalculationGroup.CalculationItems.Count
                        if t.CalculationGroup is not None
                        else 0
                    )
                    tables.append(
                        {
                            "name": tname,
                            "columnCount": ci_count,
                            "kind": "calculationGroup",
                        }
                    )
                    continue
                col_count = sum(
                    1
                    for c in t.Columns
                    if c.Type != TOM.ColumnType.RowNumber
                    and c.Type != TOM.ColumnType.Calculated
                    and _mig_key(tname, c.Name) not in removed_cols
                )
                tables.append(
                    {"name": tname, "columnCount": col_count, "kind": "table"}
                )

        return {
            "ready": True,
            "expression": expression,
            "tables": tables,
            "pqt": data.get("data_movement") == "pqt",
        }

    # ---------------- Create ----------------
    def _validation_to_rows(df):
        if df is None or df.empty or "Object Type" not in df.columns:
            return []
        rows = []
        for obj_type, grp in df.groupby("Object Type"):
            total = len(grp)
            migrated = int(grp["Migrated"].sum()) if "Migrated" in grp.columns else 0
            rows.append(
                {
                    "objectType": str(obj_type),
                    "migrated": migrated,
                    "total": total,
                }
            )
        rows.sort(key=lambda x: x["objectType"].lower())
        return rows

    def _run_migration(data):
        from sempy_labs._generate_semantic_model import create_blank_semantic_model
        from sempy_labs.migration._create_pqt_file import create_pqt_file
        from sempy_labs.migration._migrate_tables_columns_to_semantic_model import (
            migrate_tables_columns_to_semantic_model,
        )
        from sempy_labs.migration._migrate_calctables_to_lakehouse import (
            migrate_calc_tables_to_lakehouse,
            migrate_field_parameters,
        )
        from sempy_labs.migration._migrate_model_objects_to_semantic_model import (
            migrate_model_objects_to_semantic_model,
        )
        from sempy_labs.migration._migrate_calctables_to_semantic_model import (
            migrate_calc_tables_to_semantic_model,
        )
        from sempy_labs.migration._refresh_calc_tables import refresh_calc_tables
        from sempy_labs.migration._migration_validation import migration_validation
        from sempy_labs._refresh_semantic_model import refresh_semantic_model

        new_name = (data.get("new_model_name") or "").strip()
        target_ws = data.get("target_workspace_id") or workspace_id
        src_type = data.get("source_type") or "Lakehouse"
        src_ws = data.get("source_workspace_id") or workspace_id
        src_id = data.get("source_item_id")
        movement = data.get("data_movement") or "manual"
        template_name = (
            data.get("template_name") or ""
        ).strip() or "PowerQueryTemplate"

        warnings = []
        pqt_file_name = ""

        # 1) Optionally generate the Power Query template into the lakehouse.
        if movement == "pqt":
            try:
                create_pqt_file(
                    dataset=dataset_id,
                    workspace=workspace_id,
                    file_name=template_name,
                )
                pqt_file_name = f"{template_name}.pqt"
            except Exception as e:
                warnings.append(f"Could not create the Power Query template: {e}")

        # 2) Create the blank Direct Lake model.
        create_blank_semantic_model(
            dataset=new_name, workspace=target_ws, overwrite=True
        )

        # 3) Migrate tables/columns and dependent objects. Each step is
        #    best-effort so a single failure doesn't abort the whole flow.
        def _step(fn, label, **kwargs):
            try:
                fn(**kwargs)
            except Exception as e:
                warnings.append(f"{label}: {e}")

        lakehouse = src_id if src_type == "Lakehouse" else None
        lakehouse_ws = src_ws if src_type == "Lakehouse" else None

        _step(
            migrate_tables_columns_to_semantic_model,
            "Migrate tables/columns",
            dataset=dataset_name,
            new_dataset=new_name,
            workspace=workspace_id,
            new_dataset_workspace=target_ws,
            lakehouse=lakehouse,
            lakehouse_workspace=lakehouse_ws,
        )
        _step(
            migrate_calc_tables_to_lakehouse,
            "Migrate calculated tables to lakehouse",
            dataset=dataset_name,
            new_dataset=new_name,
            workspace=workspace_id,
            new_dataset_workspace=target_ws,
            lakehouse=lakehouse,
            lakehouse_workspace=lakehouse_ws,
        )
        _step(
            migrate_field_parameters,
            "Migrate field parameters",
            dataset=dataset_name,
            new_dataset=new_name,
            workspace=workspace_id,
            new_dataset_workspace=target_ws,
        )
        _step(
            migrate_model_objects_to_semantic_model,
            "Migrate model objects",
            dataset=dataset_name,
            new_dataset=new_name,
            workspace=workspace_id,
            new_dataset_workspace=target_ws,
        )
        _step(
            migrate_calc_tables_to_semantic_model,
            "Migrate calculated tables to model",
            dataset=dataset_name,
            new_dataset=new_name,
            workspace=workspace_id,
            new_dataset_workspace=target_ws,
            lakehouse=lakehouse,
            lakehouse_workspace=lakehouse_ws,
        )
        _step(
            refresh_calc_tables,
            "Refresh calculated tables",
            dataset=new_name,
            workspace=target_ws,
        )
        _step(
            refresh_semantic_model,
            "Refresh model",
            dataset=new_name,
            workspace=target_ws,
        )

        # 4) Validate the migration.
        validation = []
        try:
            dfV = migration_validation(
                dataset=dataset_name,
                new_dataset=new_name,
                workspace=workspace_id,
                new_dataset_workspace=target_ws,
            )
            validation = _validation_to_rows(dfV)
        except Exception as e:
            warnings.append(f"Migration validation: {e}")

        return {
            "createdModel": new_name,
            "sourceType": src_type,
            "pqt": movement == "pqt",
            "pqtFileName": pqt_file_name,
            "warnings": warnings,
            "validation": validation,
        }

    # ---------------- Initial widget state ----------------
    workspaces_payload = _list_workspaces_payload()
    default_new_name = f"{dataset_name} (Direct Lake)"

    class MigrateDirectLakeWidget(anywidget.AnyWidget):
        _esm = _WIDGET_JS
        _css = _WIDGET_CSS

        screen = traitlets.Unicode("analyze").tag(sync=True)
        dataset_id = traitlets.Unicode("").tag(sync=True)
        dataset_name = traitlets.Unicode("").tag(sync=True)
        workspace_id = traitlets.Unicode("").tag(sync=True)
        workspace_name = traitlets.Unicode("").tag(sync=True)
        new_model_name = traitlets.Unicode("").tag(sync=True)
        target_workspace_id = traitlets.Unicode("").tag(sync=True)
        source_type = traitlets.Unicode("Lakehouse").tag(sync=True)
        source_types = traitlets.List().tag(sync=True)
        source_workspace_id = traitlets.Unicode("").tag(sync=True)
        source_item_id = traitlets.Unicode("").tag(sync=True)
        schema = traitlets.Unicode("").tag(sync=True)
        lakehouse_schema_enabled = traitlets.Bool(True).tag(sync=True)
        data_movement = traitlets.Unicode("manual").tag(sync=True)
        template_name = traitlets.Unicode("").tag(sync=True)
        workspaces = traitlets.List().tag(sync=True)
        source_items = traitlets.Dict().tag(sync=True)
        analysis = traitlets.Dict().tag(sync=True)
        preview = traitlets.Dict().tag(sync=True)
        result = traitlets.Dict().tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        busy = traitlets.Bool(False).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)

    widget = MigrateDirectLakeWidget(
        screen="analyze",
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        workspace_id=workspace_id,
        workspace_name=workspace_name or "",
        new_model_name=default_new_name,
        target_workspace_id=workspace_id,
        source_type="Lakehouse",
        source_types=list(_MIGRATION_SOURCE_TYPES),
        source_workspace_id=workspace_id,
        source_item_id="",
        schema="",
        lakehouse_schema_enabled=True,
        data_movement="manual",
        template_name=f"{dataset_name} data load",
        workspaces=workspaces_payload,
        source_items={},
        analysis={},
        preview={},
        result={},
        status={},
        busy=False,
        dark_mode=bool(dark_mode),
        pending_action={},
        run=0,
    )

    # ---------------- Action dispatcher ----------------
    def _on_run(_change):
        data = dict(widget.pending_action or {})
        action = data.get("action")
        if not action:
            return
        widget.busy = True
        try:
            if action == "analyze":
                widget.status = {}
                widget.analysis = _analyze_model()

            elif action == "list_source_items":
                ws_id = data.get("workspace_id")
                src_type = data.get("source_type")
                if not ws_id or not src_type:
                    return
                items = _list_source_items_payload(ws_id, src_type)
                key = f"{ws_id}::{src_type}"
                new_map = dict(widget.source_items)
                new_map[key] = items
                widget.source_items = new_map

            elif action == "preview":
                widget.status = {}
                widget.preview = _build_preview(data)
                widget.screen = "preview"

            elif action == "create":
                widget.status = {}
                widget.result = _run_migration(data)
                widget.screen = "done"

        except Exception as e:
            widget.status = {"message": f"Error: {e}", "kind": "error"}
        finally:
            widget.busy = False

    widget.observe(_on_run, names=["run"])

    display(widget)


from sempy_labs._ui_components import ICONS as _UI_ICONS  # noqa: E402

_WIDGET_JS = (
    _WIDGET_JS.replace("__IC_DATABASE__", _UI_ICONS["database"])
    .replace("__IC_CHECK__", _UI_ICONS["check_circle"])
    .replace("__IC_ALERT__", _UI_ICONS["alert"])
    .replace("__IC_EXT__", _UI_ICONS["external_link"])
    .replace("__IC_CLOSE__", _UI_ICONS["close"])
    .replace("__IC_BACK__", _UI_ICONS["back"])
    .replace("__IC_SUN__", _UI_ICONS["sun"])
    .replace("__IC_MOON__", _UI_ICONS["moon"])
    .replace("__IC_TABLE__", _UI_ICONS["table"])
    .replace("__IC_CHEVRON__", _UI_ICONS["chevron_right"])
    .replace("__IC_FULLSCREEN__", _UI_ICONS["fullscreen"])
    .replace("__IC_FULLSCREEN_EXIT__", _UI_ICONS["fullscreen_exit"])
)
