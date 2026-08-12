from typing import Optional
from uuid import UUID
from sempy._utils._log import log

# ---------------------------------------------------------------------------
# Widget CSS (scoped under .slls-mc). Tokens mirror sempy_labs._ui_components
# light/dark palettes so the tool matches every other interactive widget.
# ---------------------------------------------------------------------------
_WIDGET_CSS = """
.slls-mc {
    --slls-bg-solid: #ffffff;
    --slls-bg-secondary: #f5f5f7;
    --slls-surface: rgba(255, 255, 255, 0.85);
    --slls-surface-2: rgba(0, 0, 0, 0.025);
    --slls-border: rgba(0, 0, 0, 0.08);
    --slls-border-strong: rgba(0, 0, 0, 0.14);
    --slls-text: #1d1d1f;
    --slls-text-secondary: #6e6e73;
    --slls-text-tertiary: #86868b;
    --slls-accent: #0071e3;
    --slls-accent-hover: #0a6cdb;
    --slls-accent-soft: rgba(0, 113, 227, 0.12);
    --slls-danger: #ff3b30;
    --slls-danger-soft: rgba(255, 59, 48, 0.12);
    --slls-success: #34c759;
    --slls-success-soft: rgba(52, 199, 89, 0.14);
    --slls-warn: #ff9500;
    --slls-warn-soft: rgba(255, 149, 0, 0.14);
    --slls-radius: 14px;
    --slls-radius-sm: 8px;
    --slls-shadow: 0 1px 2px rgba(0,0,0,0.04), 0 8px 24px rgba(0,0,0,0.06);
    --ui-bg: var(--slls-bg-solid);
    --ui-bg-solid: var(--slls-bg-solid);
    --ui-bg-secondary: var(--slls-bg-secondary);
    --ui-surface: var(--slls-surface);
    --ui-surface-2: var(--slls-surface-2);
    --ui-border: var(--slls-border);
    --ui-border-strong: var(--slls-border-strong);
    --ui-text: var(--slls-text);
    --ui-text-secondary: var(--slls-text-secondary);
    --ui-text-tertiary: var(--slls-text-tertiary);
    --ui-accent: var(--slls-accent);
    --ui-accent-soft: var(--slls-accent-soft);
    --ui-shadow-lg: var(--slls-shadow);
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "SF Pro Display",
        "Helvetica Neue", Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    color: var(--slls-text);
    width: 100%;
    background: var(--slls-bg-solid);
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    box-shadow: var(--slls-shadow);
    box-sizing: border-box;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    min-height: 620px;
}
@media (prefers-color-scheme: dark) {
    .slls-mc.slls-mc-auto {
        --slls-bg-solid: #1e1e22; --slls-bg-secondary: #2a2a30;
        --slls-surface: rgba(255,255,255,0.04); --slls-surface-2: rgba(255,255,255,0.03);
        --slls-border: rgba(255,255,255,0.08); --slls-border-strong: rgba(255,255,255,0.16);
        --slls-text: #f5f5f7; --slls-text-secondary: #b8b8bf; --slls-text-tertiary: #8e8e94;
        --slls-accent: #0A84FF; --slls-accent-hover: #1a8cff; --slls-accent-soft: rgba(10,132,255,0.18);
        --slls-danger: #ff453a; --slls-danger-soft: rgba(255,69,58,0.18);
        --slls-success: #30d158; --slls-success-soft: rgba(48,209,88,0.18);
        --slls-warn: #ff9f0a; --slls-warn-soft: rgba(255,159,10,0.18);
        --slls-shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.5);
    }
}
.slls-mc.slls-mc-dark {
    --slls-bg-solid: #1e1e22; --slls-bg-secondary: #2a2a30;
    --slls-surface: rgba(255,255,255,0.04); --slls-surface-2: rgba(255,255,255,0.03);
    --slls-border: rgba(255,255,255,0.08); --slls-border-strong: rgba(255,255,255,0.16);
    --slls-text: #f5f5f7; --slls-text-secondary: #b8b8bf; --slls-text-tertiary: #8e8e94;
    --slls-accent: #0A84FF; --slls-accent-hover: #1a8cff; --slls-accent-soft: rgba(10,132,255,0.18);
    --slls-danger: #ff453a; --slls-danger-soft: rgba(255,69,58,0.18);
    --slls-success: #30d158; --slls-success-soft: rgba(48,209,88,0.18);
    --slls-warn: #ff9f0a; --slls-warn-soft: rgba(255,159,10,0.18);
    --slls-shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.5);
}
.slls-mc * { box-sizing: border-box; }

/* Fullscreen: notebook hosts often block the native Fullscreen API, so a CSS
   overlay covering the viewport is the reliable primary mechanism. */
.slls-mc:fullscreen, .slls-mc:-webkit-full-screen { width: 100vw; height: 100vh; max-height: none; border: none; border-radius: 0; box-shadow: none; }
.slls-mc.slls-mc-fs { position: fixed; inset: 0; z-index: 2147483000; width: 100vw; height: 100vh; max-height: none; margin: 0; border: none; border-radius: 0; box-shadow: none; }

/* Header */
.slls-mc-header { display: flex; align-items: center; gap: 12px; padding: 16px 20px; border-bottom: 1px solid var(--slls-border); flex-wrap: wrap; }
.slls-mc-headicon { display: inline-flex; align-items: center; justify-content: center; width: 34px; height: 34px; border-radius: 10px; background: var(--slls-accent-soft); color: var(--slls-accent); flex-shrink: 0; }
.slls-mc-titlewrap { display: flex; flex-direction: column; min-width: 0; }
.slls-mc-head-spacer { flex: 1 1 auto; }
.slls-mc-title { font-size: 20px; font-weight: 600; letter-spacing: -0.01em; line-height: 1.15; }
.slls-mc-subtitle { font-size: 12.5px; color: var(--slls-text-secondary); margin-top: 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 560px; }
.slls-mc-subtitle b { color: var(--slls-text); font-weight: 500; }

.slls-mc-btn { appearance: none; border: 1px solid var(--slls-border-strong); background: var(--slls-surface); color: var(--slls-text); font-family: inherit; font-size: 13px; font-weight: 500; padding: 7px 14px; border-radius: 999px; cursor: pointer; display: inline-flex; align-items: center; gap: 7px; transition: background 120ms ease, border-color 120ms ease, transform 80ms ease, opacity 120ms ease; }
.slls-mc-btn:hover { background: var(--slls-surface-2); border-color: var(--slls-text-tertiary); }
.slls-mc-btn:active { transform: scale(0.97); }
.slls-mc-btn:disabled { opacity: 0.45; cursor: not-allowed; }
.slls-mc-btn-primary { background: var(--slls-accent); border-color: var(--slls-accent); color: #fff; }
.slls-mc-btn-primary:hover { background: var(--slls-accent-hover); border-color: var(--slls-accent-hover); }
.slls-mc-btn svg { display: block; }

/* Compared-models bar */
.slls-mc-models { display: flex; flex-wrap: wrap; align-items: center; gap: 14px; padding: 12px 20px; border-bottom: 1px solid var(--slls-border); background: var(--slls-surface-2); }
.slls-mc-chip { display: flex; align-items: center; gap: 10px; min-width: 0; max-width: 340px; flex: 1 1 240px; padding: 9px 13px; border: 1px solid var(--slls-border); border-left-width: 4px; border-radius: 10px; background: var(--slls-bg-solid); }
.slls-mc-chip.current { border-left-color: var(--slls-danger); }
.slls-mc-chip.compared { border-left-color: var(--slls-success); }
.slls-mc-chip-dot { width: 9px; height: 9px; border-radius: 50%; flex-shrink: 0; }
.slls-mc-chip.current .slls-mc-chip-dot { background: var(--slls-danger); }
.slls-mc-chip.compared .slls-mc-chip-dot { background: var(--slls-success); }
.slls-mc-chip-body { display: flex; flex-direction: column; min-width: 0; line-height: 1.3; }
.slls-mc-chip-role { font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: var(--slls-text-tertiary); }
.slls-mc-chip-name { font-size: 13.5px; font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-mc-chip-ws { font-size: 11.5px; color: var(--slls-text-secondary); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-mc-vs { display: inline-flex; color: var(--slls-text-tertiary); flex-shrink: 0; }

/* Picker */
.slls-mc-picker-wrap { padding: 0 20px 18px; }
.slls-mc-picker { background: var(--slls-surface); border: 1px solid var(--slls-border); border-radius: 14px; padding: 16px; margin-top: 16px; }
.slls-mc-picker-top { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 14px; }
.slls-mc-picker-title { font-size: 14px; font-weight: 600; }
.slls-mc-picker-sub { font-size: 12.5px; color: var(--slls-text-secondary); margin-top: 3px; }
.slls-mc-picker-side { display: flex; align-items: flex-end; gap: 10px; flex-wrap: wrap; margin-bottom: 12px; }
.slls-mc-side-label { flex: 0 0 100%; font-size: 10.5px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; color: var(--slls-text-tertiary); margin-bottom: 2px; display: flex; align-items: center; gap: 7px; }
.slls-mc-side-label .slls-mc-chip-dot { width: 8px; height: 8px; }
.slls-mc-side-label.current .slls-mc-chip-dot { background: var(--slls-danger); }
.slls-mc-side-label.compared .slls-mc-chip-dot { background: var(--slls-success); }
.slls-mc-field { display: flex; flex-direction: column; gap: 4px; flex: 1 1 240px; min-width: 0; }
.slls-mc-field label { padding-left: 4px; color: var(--slls-text-tertiary); font-size: 11px; font-weight: 600; letter-spacing: 0.04em; text-transform: uppercase; }
.slls-mc-picker .slls-ss-btn { border-radius: 999px; padding: 7px 12px 7px 15px; background: var(--slls-surface); font-size: 13.5px; }
.slls-mc-picker-actions { display: flex; justify-content: flex-end; gap: 10px; margin-top: 6px; }

/* Summary + filter bar */
.slls-mc-filters { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; padding: 10px 20px; border-bottom: 1px solid var(--slls-border); }
.slls-mc-fchip { appearance: none; font-family: inherit; display: inline-flex; align-items: center; gap: 7px; padding: 5px 13px; border-radius: 999px; border: 1px solid var(--slls-border-strong); background: transparent; color: var(--slls-text-secondary); font-size: 12.5px; font-weight: 500; cursor: pointer; opacity: 0.6; transition: opacity 120ms ease, background 120ms ease, border-color 120ms ease; }
.slls-mc-fchip:hover { opacity: 1; }
.slls-mc-fchip.on { opacity: 1; }
.slls-mc-fchip .slls-mc-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
.slls-mc-fchip .slls-mc-count { font-variant-numeric: tabular-nums; font-weight: 600; }
.slls-mc-fchip.added .slls-mc-dot { background: var(--slls-success); }
.slls-mc-fchip.added.on { border-color: var(--slls-success); background: var(--slls-success-soft); color: var(--slls-success); }
.slls-mc-fchip.removed .slls-mc-dot { background: var(--slls-danger); }
.slls-mc-fchip.removed.on { border-color: var(--slls-danger); background: var(--slls-danger-soft); color: var(--slls-danger); }
.slls-mc-fchip.modified .slls-mc-dot { background: var(--slls-warn); }
.slls-mc-fchip.modified.on { border-color: var(--slls-warn); background: var(--slls-warn-soft); color: var(--slls-warn); }
.slls-mc-fchip.unchanged .slls-mc-dot { background: var(--slls-text-tertiary); }
.slls-mc-fchip.unchanged.on { background: var(--slls-surface-2); color: var(--slls-text); }
.slls-mc-searchwrap { position: relative; margin-left: auto; display: flex; align-items: center; }
.slls-mc-searchicon { position: absolute; left: 11px; display: inline-flex; color: var(--slls-text-tertiary); pointer-events: none; }
.slls-mc-searchicon svg { width: 14px; height: 14px; display: block; }
.slls-mc-search { width: 240px; background: var(--slls-surface); border: 1px solid var(--slls-border-strong); border-radius: 999px; padding: 7px 12px 7px 32px; font-size: 12.5px; color: var(--slls-text); font-family: inherit; }
.slls-mc-search:focus { outline: none; border-color: var(--slls-accent); box-shadow: 0 0 0 3px var(--slls-accent-soft); }
.slls-mc-search::placeholder { color: var(--slls-text-tertiary); }

/* Diff list */
.slls-mc-body { flex: 1; overflow-y: auto; padding: 14px 20px 20px; min-height: 0; }
.slls-mc-group { margin-bottom: 18px; }
.slls-mc-group-head { appearance: none; font-family: inherit; width: 100%; display: flex; align-items: center; gap: 8px; padding: 4px 2px; margin-bottom: 8px; border: none; background: transparent; color: var(--slls-text); cursor: pointer; text-align: left; }
.slls-mc-group-head .slls-mc-ic { display: inline-flex; color: var(--slls-text-tertiary); }
.slls-mc-group-kind { font-size: 13.5px; font-weight: 600; }
.slls-mc-group-count { font-size: 11px; font-weight: 600; padding: 1px 9px; border-radius: 999px; border: 1px solid var(--slls-border); background: var(--slls-surface-2); color: var(--slls-text-secondary); font-variant-numeric: tabular-nums; }

.slls-mc-row { border: 1px solid var(--slls-border); border-left-width: 3px; border-radius: 10px; background: var(--slls-bg-solid); margin-bottom: 6px; overflow: hidden; }
.slls-mc-row.added { border-left-color: var(--slls-success); }
.slls-mc-row.removed { border-left-color: var(--slls-danger); }
.slls-mc-row.modified { border-left-color: var(--slls-warn); }
.slls-mc-row.unchanged { border-left-color: var(--slls-border-strong); }
.slls-mc-rowhead { appearance: none; font-family: inherit; width: 100%; display: flex; align-items: center; gap: 10px; padding: 9px 12px; border: none; background: transparent; color: var(--slls-text); cursor: pointer; text-align: left; }
.slls-mc-rowhead:hover { background: var(--slls-surface-2); }
.slls-mc-rowhead.static { cursor: default; }
.slls-mc-badge { display: inline-flex; align-items: center; justify-content: center; width: 19px; height: 19px; border-radius: 50%; flex-shrink: 0; color: #fff; }
.slls-mc-badge svg { width: 11px; height: 11px; }
.slls-mc-row.added .slls-mc-badge { background: var(--slls-success); }
.slls-mc-row.removed .slls-mc-badge { background: var(--slls-danger); }
.slls-mc-row.modified .slls-mc-badge { background: var(--slls-warn); }
.slls-mc-row.unchanged .slls-mc-badge { background: var(--slls-text-tertiary); }
.slls-mc-rowname { font-size: 13px; font-weight: 500; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-mc-rowstatus { font-size: 11.5px; font-weight: 500; flex-shrink: 0; }
.slls-mc-row.added .slls-mc-rowstatus { color: var(--slls-success); }
.slls-mc-row.removed .slls-mc-rowstatus { color: var(--slls-danger); }
.slls-mc-row.modified .slls-mc-rowstatus { color: var(--slls-warn); }
.slls-mc-row.unchanged .slls-mc-rowstatus { color: var(--slls-text-tertiary); }
.slls-mc-rowmeta { font-size: 11.5px; color: var(--slls-text-tertiary); flex-shrink: 0; }
.slls-mc-rowchev { margin-left: auto; display: inline-flex; color: var(--slls-text-tertiary); flex-shrink: 0; }

.slls-mc-detail { border-top: 1px solid var(--slls-border); padding: 10px 13px 12px; }
.slls-mc-grid { display: grid; grid-template-columns: minmax(112px, auto) 1fr 1fr; gap: 4px 12px; align-items: start; }
.slls-mc-colhead { display: flex; align-items: center; gap: 6px; font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: var(--slls-text-tertiary); padding-bottom: 3px; overflow: hidden; }
.slls-mc-colhead span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-mc-colhead .slls-mc-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
.slls-mc-colhead.current .slls-mc-dot { background: var(--slls-danger); }
.slls-mc-colhead.compared .slls-mc-dot { background: var(--slls-success); }
.slls-mc-propname { font-size: 12px; font-weight: 500; color: var(--slls-text-secondary); padding-top: 7px; }
.slls-mc-cell { min-width: 0; border: 1px solid transparent; border-radius: 8px; padding: 5px 9px; font-size: 12px; overflow-wrap: anywhere; }
.slls-mc-cell.current { border-color: var(--slls-danger); background: var(--slls-danger-soft); }
.slls-mc-cell.compared { border-color: var(--slls-success); background: var(--slls-success-soft); }
.slls-mc-cell.absent { border-style: dashed; border-color: var(--slls-border-strong); color: var(--slls-text-tertiary); font-style: italic; }
.slls-mc-cell.none { font-style: italic; opacity: 0.7; }
.slls-mc-cell pre { margin: 0; white-space: pre-wrap; overflow-wrap: anywhere; font-family: "SF Mono", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 11.5px; line-height: 1.5; }

.slls-mc-more { font-size: 12px; color: var(--slls-text-tertiary); padding: 4px 2px 0; }

/* Center overlays */
.slls-mc-center { flex: 1; display: flex; align-items: center; justify-content: center; text-align: center; padding: 40px 24px; }
.slls-mc-center-inner { max-width: 400px; }
.slls-mc-center-inner .slls-mc-ic { color: var(--slls-text-tertiary); opacity: 0.6; }
.slls-mc-center-inner h4 { margin: 10px 0 4px; font-size: 15px; font-weight: 600; }
.slls-mc-center-inner p { margin: 0; font-size: 12.5px; color: var(--slls-text-secondary); }

/* Status toast */
.slls-mc-status { padding: 9px 20px; font-size: 12.5px; border-top: 1px solid var(--slls-border); }
.slls-mc-status.success { background: var(--slls-success-soft); color: var(--slls-success); }
.slls-mc-status.error { background: var(--slls-danger-soft); color: var(--slls-danger); }
.slls-mc-status.info { background: var(--slls-surface-2); color: var(--slls-text-secondary); }

/* Footer */
.slls-mc-footer { padding: 8px 20px; border-top: 1px solid var(--slls-border); text-align: right; font-size: 11.5px; color: var(--slls-text-tertiary); }
.slls-mc-footer a { color: var(--slls-text-tertiary); text-decoration: none; transition: color 120ms ease; }
.slls-mc-footer a:hover { color: var(--slls-accent); }

.slls-mc-spin { animation: slls-mc-spin 0.8s linear infinite; transform-origin: center; }
@keyframes slls-mc-spin { to { transform: rotate(360deg); } }
.slls-mc-body::-webkit-scrollbar { width: 10px; }
.slls-mc-body::-webkit-scrollbar-thumb { background: var(--slls-border-strong); border-radius: 999px; background-clip: padding-box; border: 2px solid transparent; }
@media (max-width: 720px) {
    .slls-mc-search { width: 160px; }
    .slls-mc-grid { grid-template-columns: 1fr; }
}
"""


# ---------------------------------------------------------------------------
# Widget JS (anywidget ESM). Icons injected via __ICON_*__ placeholders below.
# ---------------------------------------------------------------------------
_WIDGET_JS = r"""
function render({ model, el }) {
    const ICON = {
        compare: `__ICON_COMPARE__`, plus: `__ICON_PLUS__`, minus: `__ICON_MINUS__`,
        pencil: `__ICON_PENCIL__`, search: `__ICON_SEARCH__`, refresh: `__ICON_REFRESH__`,
        close: `__ICON_CLOSE__`, sun: `__ICON_SUN__`, moon: `__ICON_MOON__`,
        fullscreen: `__ICON_FULLSCREEN__`, fullscreen_exit: `__ICON_FULLSCREEN_EXIT__`,
        chevron_down: `__ICON_CHEVRON_DOWN__`, chevron_right: `__ICON_CHEVRON_RIGHT__`,
        swap: `__ICON_SWAP__`, check: `__ICON_CHECK_CIRCLE__`, alert: `__ICON_ALERT__`,
        table: `__ICON_TABLE__`, column: `__ICON_COLUMN__`, measure: `__ICON_MEASURE__`,
        hierarchy: `__ICON_HIERARCHY__`, calculation_item: `__ICON_CALC_ITEM__`,
        relationship: `__ICON_RELATIONSHIP__`, users: `__ICON_USERS__`,
        code: `__ICON_CODE__`, eye: `__ICON_EYE__`, text_type: `__ICON_TEXT_TYPE__`,
    };

    // Object kinds in display order; must match the Python DIFF_KINDS list.
    const KIND_ICON = {
        "Table": ICON.table, "Column": ICON.column, "Measure": ICON.measure,
        "Hierarchy": ICON.hierarchy, "Calculation item": ICON.calculation_item,
        "Relationship": ICON.relationship, "Role": ICON.users,
        "Expression": ICON.code, "Function": ICON.code,
        "Perspective": ICON.eye, "Perspective object": ICON.eye,
        "Culture": ICON.text_type, "Translation": ICON.text_type,
    };
    const STATUSES = ["added", "removed", "modified", "unchanged"];
    const STATUS_LABEL = { added: "Added", removed: "Removed", modified: "Modified", unchanged: "Unchanged" };
    const STATUS_ICON = { added: ICON.plus, removed: ICON.minus, modified: ICON.pencil, unchanged: ICON.minus };
    // Rendering every row of a large model would lock the page up; the rest
    // stay reachable through the search box.
    const MAX_ROWS_PER_KIND = 300;

    const root = document.createElement("div");
    root.className = "slls-mc";
    el.appendChild(root);

    function applyTheme() {
        root.classList.remove("slls-mc-dark", "slls-mc-auto");
        const dm = model.get("dark_mode");
        if (dm === true) root.classList.add("slls-mc-dark");
        else if (dm == null) root.classList.add("slls-mc-auto");
    }
    applyTheme();
    model.on("change:dark_mode", applyTheme);

    // --- Fullscreen ---
    let fsMode = false;
    function isFullscreen() { return fsMode; }
    function nativeExit() {
        const ex = document.exitFullscreen || document.webkitExitFullscreen;
        if (ex && (document.fullscreenElement || document.webkitFullscreenElement)) {
            const p = ex.call(document);
            if (p && p.catch) p.catch(() => {});
        }
    }
    function setFullscreen(on) {
        fsMode = on;
        root.classList.toggle("slls-mc-fs", on);
        try {
            if (on) {
                const req = root.requestFullscreen || root.webkitRequestFullscreen;
                if (req) { const p = req.call(root); if (p && p.catch) p.catch(() => {}); }
            } else {
                nativeExit();
            }
        } catch (e) { /* native fullscreen unavailable; CSS overlay covers it */ }
        renderAll();
    }
    function onFullscreenChange() {
        const nativeOn = !!(document.fullscreenElement || document.webkitFullscreenElement);
        if (!nativeOn && fsMode) { fsMode = false; root.classList.remove("slls-mc-fs"); renderAll(); }
    }
    function onEscKey(e) { if (e.key === "Escape" && fsMode) setFullscreen(false); }
    document.addEventListener("fullscreenchange", onFullscreenChange);
    document.addEventListener("webkitfullscreenchange", onFullscreenChange);
    document.addEventListener("keydown", onEscKey);

    // --- Local UI state ---
    let statusFilter = new Set(["added", "removed", "modified"]);
    let search = "";
    let collapsedKinds = new Set();
    let expanded = new Set();
    let pickerOpen = true;
    let baseWs = "", baseDs = "", cmpWs = "", cmpDs = "";
    let workspacesRequested = false;
    let busyLocal = false;
    // Preserved scroll offset so toggling a row does not jump the list.
    let bodyScroll = 0;

    const esc = (s) => String(s == null ? "" : s)
        .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;").replace(/'/g, "&#39;");

    const entities = () => model.get("entities") || [];
    const summary = () => model.get("summary") || {};
    const kinds = () => model.get("kinds") || [];
    const compared = () => !!model.get("compared");
    const busy = () => !!model.get("busy");
    const working = () => busyLocal || busy();

    function dispatch(payload) {
        busyLocal = true;
        model.set("pending_action", payload);
        // Coercing to 0 keeps the counter a valid integer when a host does not
        // seed every synced trait into the front-end model.
        model.set("run", (model.get("run") || 0) + 1);
        model.save_changes();
        renderAll();
    }

    function ensureWorkspaces() {
        if (workspacesRequested) return;
        workspacesRequested = true;
        dispatch({ action: "list_workspaces" });
    }

    function ensureDatasets(wsId) {
        const ds = model.get("datasets") || {};
        if (wsId && !ds[wsId]) dispatch({ action: "list_datasets", workspace_id: wsId });
    }

    function spinner() {
        return `<span class="slls-mc-ic"><svg class="slls-mc-spin" width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M8 1.6a6.4 6.4 0 1 1-6.4 6.4" opacity="0.85"/></svg></span>`;
    }

    // ---------- Rendering ----------
    function renderAll() {
        root.innerHTML = "";
        root.appendChild(buildHeader());
        if (compared()) root.appendChild(buildModelsBar());
        if (pickerOpen) root.appendChild(buildPicker());
        if (working() && !compared()) {
            root.appendChild(centerMsg("compare", "Reading and comparing models\u2026", ""));
        } else if (compared()) {
            root.appendChild(buildFilters());
            root.appendChild(buildBody());
        } else if (!pickerOpen) {
            root.appendChild(centerMsg("compare", "No comparison yet",
                "Choose two semantic models and select Compare."));
        }
        const st = model.get("status") || {};
        if (st.message) {
            const s = document.createElement("div");
            s.className = "slls-mc-status " + (st.kind || "info");
            s.textContent = st.message;
            root.appendChild(s);
        }
        root.appendChild(buildFooter());
    }

    function buildHeader() {
        const h = document.createElement("div");
        h.className = "slls-mc-header";
        const dm = model.get("dark_mode") === true;
        const baseName = model.get("base_name");
        const subtitle = baseName
            ? `Compare <b>${esc(baseName)}</b> against another semantic model`
            : "Compare two semantic models";
        h.innerHTML =
            `<span class="slls-mc-headicon">${ICON.compare}</span>` +
            `<div class="slls-mc-titlewrap">` +
                `<div class="slls-mc-title">Model comparison</div>` +
                `<div class="slls-mc-subtitle">${subtitle}</div>` +
            `</div>` +
            (compared() && !pickerOpen
                ? `<button class="sl-change-btn" data-act="change-model" title="Change semantic model / workspace" aria-label="Change semantic model / workspace">${ICON.swap}</button>`
                : "") +
            `<div class="slls-mc-head-spacer"></div>` +
            (compared()
                ? `<button class="sl-reload-btn${working() ? " sl-spinning" : ""}" data-act="reload" ` +
                    `title="Reload both models' metadata" aria-label="Reload both models' metadata" ` +
                    `${working() ? "disabled" : ""}>${ICON.refresh}</button>`
                : "") +
            `<button class="sl-theme-btn" data-act="fullscreen" title="Toggle full screen">${isFullscreen() ? ICON.fullscreen_exit : ICON.fullscreen}</button>` +
            `<button class="sl-theme-btn" data-act="theme" title="Toggle theme">${dm ? ICON.sun : ICON.moon}</button>`;

        const rl = h.querySelector('[data-act="reload"]');
        if (rl) rl.onclick = () => dispatch({ action: "reload" });
        const cm = h.querySelector('[data-act="change-model"]');
        if (cm) cm.onclick = () => {
            pickerOpen = true;
            ensureWorkspaces();
            ensureDatasets(baseWs);
            ensureDatasets(cmpWs);
            renderAll();
        };
        h.querySelector('[data-act="fullscreen"]').onclick = () => setFullscreen(!fsMode);
        h.querySelector('[data-act="theme"]').onclick = () => {
            model.set("dark_mode", !(model.get("dark_mode") === true));
            model.save_changes();
            renderAll();
        };
        return h;
    }

    function modelChip(role, name, workspace, side) {
        return `<div class="slls-mc-chip ${side}">` +
            `<span class="slls-mc-chip-dot"></span>` +
            `<div class="slls-mc-chip-body">` +
                `<span class="slls-mc-chip-role">${esc(role)}</span>` +
                `<span class="slls-mc-chip-name" title="${esc(name)}">${esc(name || "\u2014")}</span>` +
                `<span class="slls-mc-chip-ws" title="${esc(workspace)}">${esc(workspace || "\u2014")}</span>` +
            `</div></div>`;
    }

    function buildModelsBar() {
        const d = document.createElement("div");
        d.className = "slls-mc-models";
        d.innerHTML =
            modelChip("Current model", model.get("base_name"), model.get("base_workspace"), "current") +
            `<span class="slls-mc-vs">${ICON.compare}</span>` +
            modelChip("Compared model", model.get("compared_name"), model.get("compared_workspace"), "compared");
        return d;
    }

    function buildPicker() {
        const wrap = document.createElement("div");
        wrap.className = "slls-mc-picker-wrap";
        if (!baseWs) baseWs = model.get("base_workspace_id") || "";
        if (!baseDs) baseDs = model.get("base_id") || "";
        const workspaces = model.get("workspaces") || [];
        const dsMap = model.get("datasets") || {};

        const card = document.createElement("div");
        card.className = "slls-mc-picker";
        card.innerHTML =
            `<div class="slls-mc-picker-top">` +
                `<div>` +
                    `<div class="slls-mc-picker-title">Select the models to compare</div>` +
                    `<div class="slls-mc-picker-sub">Every object is matched by name and reported as added, removed or modified.</div>` +
                `</div>` +
                `<button class="sl-reload-btn${working() ? " sl-spinning" : ""}" data-p="reload" type="button" ` +
                    `title="Reload workspaces and semantic models" aria-label="Reload workspaces and semantic models" ` +
                    `${working() ? "disabled" : ""}>${ICON.refresh}</button>` +
            `</div>` +
            `<div class="slls-mc-picker-side">` +
                `<span class="slls-mc-side-label current"><span class="slls-mc-chip-dot"></span>Current model</span>` +
                `<div class="slls-mc-field"><label>Workspace</label><div data-p="bws"></div></div>` +
                `<div class="slls-mc-field"><label>Semantic model</label><div data-p="bds"></div></div>` +
            `</div>` +
            `<div class="slls-mc-picker-side">` +
                `<span class="slls-mc-side-label compared"><span class="slls-mc-chip-dot"></span>Compared model</span>` +
                `<div class="slls-mc-field"><label>Workspace</label><div data-p="cws"></div></div>` +
                `<div class="slls-mc-field"><label>Semantic model</label><div data-p="cds"></div></div>` +
            `</div>` +
            `<div class="slls-mc-picker-actions">` +
                (compared() ? `<button class="slls-mc-btn" data-p="cancel">Cancel</button>` : "") +
                `<button class="slls-mc-btn slls-mc-btn-primary" data-p="compare" ` +
                    `${(!baseDs || !cmpDs || working()) ? "disabled" : ""}>` +
                    `${working() ? spinner() : ICON.compare}Compare</button>` +
            `</div>`;

        const mount = (slot, opts) => {
            const picker = createSearchSelect(opts);
            card.querySelector(`[data-p="${slot}"]`).appendChild(picker.el);
            return picker;
        };
        const wsOptions = workspaces.map((w) => ({ value: w.id, label: w.name }));

        const bws = mount("bws", {
            placeholder: "Select a workspace\u2026", searchPlaceholder: "Filter workspaces\u2026",
            ariaLabel: "Current model workspace",
            emptyLabel: working() ? "Loading workspaces\u2026" : "No workspaces",
            onChange: (o) => { baseWs = o.value; baseDs = ""; ensureDatasets(baseWs); renderAll(); },
        });
        bws.setOptions(wsOptions, baseWs);
        bws.setDisabled(working());

        const baseList = dsMap[baseWs] || null;
        const bds = mount("bds", {
            placeholder: "Select a semantic model\u2026", searchPlaceholder: "Filter semantic models\u2026",
            ariaLabel: "Current semantic model",
            emptyLabel: !baseWs ? "Select a workspace first\u2026"
                : (baseList === null ? "Loading semantic models\u2026" : "No semantic models"),
            onChange: (o) => { baseDs = o.value; renderAll(); },
        });
        bds.setOptions((baseList || []).map((d) => ({ value: d.id, label: d.name })), baseDs);
        bds.setDisabled(!baseWs || baseList === null || working());

        const cws = mount("cws", {
            placeholder: "Select a workspace\u2026", searchPlaceholder: "Filter workspaces\u2026",
            ariaLabel: "Compared model workspace",
            emptyLabel: working() ? "Loading workspaces\u2026" : "No workspaces",
            onChange: (o) => { cmpWs = o.value; cmpDs = ""; ensureDatasets(cmpWs); renderAll(); },
        });
        cws.setOptions(wsOptions, cmpWs);
        cws.setDisabled(working());

        const cmpList = dsMap[cmpWs] || null;
        const cds = mount("cds", {
            placeholder: "Select a semantic model\u2026", searchPlaceholder: "Filter semantic models\u2026",
            ariaLabel: "Compared semantic model",
            emptyLabel: !cmpWs ? "Select a workspace first\u2026"
                : (cmpList === null ? "Loading semantic models\u2026" : "No semantic models"),
            onChange: (o) => { cmpDs = o.value; renderAll(); },
        });
        cds.setOptions((cmpList || []).map((d) => ({ value: d.id, label: d.name })), cmpDs);
        cds.setDisabled(!cmpWs || cmpList === null || working());

        card.querySelector('[data-p="reload"]').onclick = () => {
            workspacesRequested = false;
            ensureWorkspaces();
            if (baseWs) dispatch({ action: "list_datasets", workspace_id: baseWs });
            if (cmpWs && cmpWs !== baseWs) dispatch({ action: "list_datasets", workspace_id: cmpWs });
        };
        const cancel = card.querySelector('[data-p="cancel"]');
        if (cancel) cancel.onclick = () => { pickerOpen = false; renderAll(); };
        card.querySelector('[data-p="compare"]').onclick = () => {
            if (!baseDs || !cmpDs) return;
            const nameOf = (list, id) => ((list || []).find((x) => x.id === id) || {}).name || "";
            dispatch({
                action: "compare",
                base_workspace_id: baseWs, base_id: baseDs,
                base_workspace_name: nameOf(workspaces, baseWs),
                base_name: nameOf(dsMap[baseWs], baseDs),
                compared_workspace_id: cmpWs, compared_id: cmpDs,
                compared_workspace_name: nameOf(workspaces, cmpWs),
                compared_name: nameOf(dsMap[cmpWs], cmpDs),
            });
        };

        wrap.appendChild(card);
        return wrap;
    }

    function buildFilters() {
        const d = document.createElement("div");
        d.className = "slls-mc-filters";
        const s = summary();
        d.innerHTML = STATUSES.map((st) =>
            `<button class="slls-mc-fchip ${st}${statusFilter.has(st) ? " on" : ""}" data-s="${st}" ` +
            `aria-pressed="${statusFilter.has(st)}"><span class="slls-mc-dot"></span>` +
            `${STATUS_LABEL[st]}<span class="slls-mc-count">${s[st] || 0}</span></button>`).join("") +
            `<span class="slls-mc-searchwrap">` +
                `<span class="slls-mc-searchicon">${ICON.search}</span>` +
                `<input class="slls-mc-search" type="search" placeholder="Search objects\u2026" ` +
                `value="${esc(search)}" aria-label="Search objects">` +
            `</span>`;
        d.querySelectorAll("[data-s]").forEach((b) => {
            b.onclick = () => {
                const st = b.getAttribute("data-s");
                if (statusFilter.has(st)) statusFilter.delete(st); else statusFilter.add(st);
                renderAll();
            };
        });
        const input = d.querySelector(".slls-mc-search");
        input.addEventListener("input", () => {
            search = input.value;
            const start = input.selectionStart;
            renderAll();
            // Re-focus the freshly rendered input so typing is uninterrupted.
            const next = root.querySelector(".slls-mc-search");
            if (next) { next.focus(); next.setSelectionRange(start, start); }
        });
        return d;
    }

    function visibleGroups() {
        const q = search.trim().toLowerCase();
        const byKind = new Map();
        for (const e of entities()) {
            if (!statusFilter.has(e.status)) continue;
            if (q && !e.label.toLowerCase().includes(q)
                && !String(e.table || "").toLowerCase().includes(q)) continue;
            if (!byKind.has(e.kind)) byKind.set(e.kind, []);
            byKind.get(e.kind).push(e);
        }
        return kinds().filter((k) => byKind.has(k)).map((k) => ({ kind: k, items: byKind.get(k) }));
    }

    function buildBody() {
        const body = document.createElement("div");
        body.className = "slls-mc-body";
        const groups = visibleGroups();
        const total = groups.reduce((n, g) => n + g.items.length, 0);
        if (total === 0) {
            body.innerHTML = `<p class="slls-mc-more">No objects match the current filters.</p>`;
            return body;
        }
        for (const g of groups) body.appendChild(buildGroup(g));
        body.addEventListener("scroll", () => { bodyScroll = body.scrollTop; });
        requestAnimationFrame(() => { body.scrollTop = bodyScroll; });
        return body;
    }

    function buildGroup(g) {
        const open = !collapsedKinds.has(g.kind);
        const wrap = document.createElement("div");
        wrap.className = "slls-mc-group";
        const head = document.createElement("button");
        head.className = "slls-mc-group-head";
        head.innerHTML =
            `<span class="slls-mc-ic">${open ? ICON.chevron_down : ICON.chevron_right}</span>` +
            `<span class="slls-mc-ic">${KIND_ICON[g.kind] || ""}</span>` +
            `<span class="slls-mc-group-kind">${esc(g.kind)}</span>` +
            `<span class="slls-mc-group-count">${g.items.length}</span>`;
        head.onclick = () => {
            if (collapsedKinds.has(g.kind)) collapsedKinds.delete(g.kind);
            else collapsedKinds.add(g.kind);
            renderAll();
        };
        wrap.appendChild(head);
        if (!open) return wrap;
        g.items.slice(0, MAX_ROWS_PER_KIND).forEach((e) => wrap.appendChild(buildRow(e)));
        if (g.items.length > MAX_ROWS_PER_KIND) {
            const more = document.createElement("div");
            more.className = "slls-mc-more";
            more.textContent = `+${g.items.length - MAX_ROWS_PER_KIND} more \u2014 refine the search to see them.`;
            wrap.appendChild(more);
        }
        return wrap;
    }

    function buildRow(e) {
        const hasDetails = (e.props || []).length > 0;
        const open = expanded.has(e.key);
        const row = document.createElement("div");
        row.className = "slls-mc-row " + e.status;
        const head = document.createElement("button");
        head.className = "slls-mc-rowhead" + (hasDetails ? "" : " static");
        head.innerHTML =
            `<span class="slls-mc-badge">${STATUS_ICON[e.status]}</span>` +
            `<span class="slls-mc-rowname" title="${esc(e.label)}">${esc(e.label)}</span>` +
            `<span class="slls-mc-rowstatus">${STATUS_LABEL[e.status]}</span>` +
            (e.status === "modified"
                ? `<span class="slls-mc-rowmeta">${e.props.length} change${e.props.length === 1 ? "" : "s"}</span>`
                : "") +
            (hasDetails
                ? `<span class="slls-mc-rowchev">${open ? ICON.chevron_down : ICON.chevron_right}</span>`
                : "");
        if (hasDetails) {
            head.onclick = () => {
                if (expanded.has(e.key)) expanded.delete(e.key); else expanded.add(e.key);
                renderAll();
            };
        }
        row.appendChild(head);
        if (open && hasDetails) row.appendChild(buildDetail(e));
        return row;
    }

    function buildDetail(e) {
        const d = document.createElement("div");
        d.className = "slls-mc-detail";
        const baseName = model.get("base_name") || "Current";
        const cmpName = model.get("compared_name") || "Compared";
        let html = `<div class="slls-mc-grid"><span></span>` +
            `<span class="slls-mc-colhead current"><span class="slls-mc-dot"></span><span>${esc(baseName)}</span></span>` +
            `<span class="slls-mc-colhead compared"><span class="slls-mc-dot"></span><span>${esc(cmpName)}</span></span>`;
        for (const p of e.props) {
            html += `<span class="slls-mc-propname">${esc(p.name)}</span>` +
                sideCell(p.current, e.status === "added" ? "absent" : "current") +
                sideCell(p.compared, e.status === "removed" ? "absent" : "compared");
        }
        d.innerHTML = html + `</div>`;
        return d;
    }

    function sideCell(value, tone) {
        if (tone === "absent") {
            return `<div class="slls-mc-cell absent">Not present</div>`;
        }
        if (value == null || value === "") {
            return `<div class="slls-mc-cell ${tone} none">(none)</div>`;
        }
        const body = String(value).includes("\n")
            ? `<pre>${esc(value)}</pre>` : esc(value);
        return `<div class="slls-mc-cell ${tone}">${body}</div>`;
    }

    function centerMsg(ic, title, sub) {
        const d = document.createElement("div");
        d.className = "slls-mc-center";
        d.innerHTML = `<div class="slls-mc-center-inner">` +
            `<span class="slls-mc-ic">${ICON[ic] || ""}</span>` +
            `<h4>${esc(title)}</h4>${sub ? `<p>${esc(sub)}</p>` : ""}</div>`;
        return d;
    }

    function buildFooter() {
        const f = document.createElement("div");
        f.className = "slls-mc-footer";
        f.innerHTML = `Powered by <a href="https://github.com/microsoft/semantic-link-labs" ` +
            `target="_blank" rel="noopener noreferrer">Semantic Link Labs</a>`;
        return f;
    }

    // ---------- Model change wiring ----------
    function settle() { busyLocal = false; renderAll(); }
    model.on("change:busy", settle);
    model.on("change:status", settle);
    model.on("change:workspaces", () => { busyLocal = false; renderAll(); });
    model.on("change:datasets", () => { busyLocal = false; renderAll(); });
    model.on("change:entities", () => {
        busyLocal = false;
        expanded = new Set();
        collapsedKinds = new Set();
        bodyScroll = 0;
        renderAll();
    });
    model.on("change:compared", () => {
        busyLocal = false;
        if (model.get("compared")) pickerOpen = false;
        renderAll();
    });
    model.on("change:compare_done", () => {
        busyLocal = false;
        pickerOpen = false;
        renderAll();
    });

    renderAll();
}
export default { render };
"""

from sempy_labs._ui_components import (  # noqa: E402
    ICONS as _UI_ICONS,
    SEARCH_SELECT_CSS as _UI_SEARCH_SELECT_CSS,
    SEARCH_SELECT_JS as _UI_SEARCH_SELECT_JS,
    scoped_button_press_css as _ui_scoped_button_press_css,
    scoped_header_css as _ui_scoped_header_css,
)

_WIDGET_CSS += "\n" + _UI_SEARCH_SELECT_CSS
_WIDGET_CSS += _ui_scoped_header_css(".slls-mc")
_WIDGET_CSS += _ui_scoped_button_press_css(".slls-mc")

_WIDGET_JS = _UI_SEARCH_SELECT_JS + "\n" + _WIDGET_JS

_WIDGET_JS = (
    _WIDGET_JS.replace("__ICON_COMPARE__", _UI_ICONS["git_compare"])
    .replace("__ICON_PLUS__", _UI_ICONS["plus"])
    .replace("__ICON_MINUS__", _UI_ICONS["minus"])
    .replace("__ICON_PENCIL__", _UI_ICONS["pencil"])
    .replace("__ICON_SEARCH__", _UI_ICONS["search"])
    .replace("__ICON_REFRESH__", _UI_ICONS["refresh"])
    .replace("__ICON_CLOSE__", _UI_ICONS["close"])
    .replace("__ICON_SUN__", _UI_ICONS["sun"])
    .replace("__ICON_MOON__", _UI_ICONS["moon"])
    .replace("__ICON_FULLSCREEN__", _UI_ICONS["fullscreen"])
    .replace("__ICON_FULLSCREEN_EXIT__", _UI_ICONS["fullscreen_exit"])
    .replace("__ICON_CHEVRON_DOWN__", _UI_ICONS["chevron_down"])
    .replace("__ICON_CHEVRON_RIGHT__", _UI_ICONS["chevron_right"])
    .replace("__ICON_SWAP__", _UI_ICONS["swap"])
    .replace("__ICON_CHECK_CIRCLE__", _UI_ICONS["check_circle"])
    .replace("__ICON_ALERT__", _UI_ICONS["alert"])
    .replace("__ICON_TABLE__", _UI_ICONS["table"])
    .replace("__ICON_COLUMN__", _UI_ICONS["column"])
    .replace("__ICON_MEASURE__", _UI_ICONS["measure"])
    .replace("__ICON_HIERARCHY__", _UI_ICONS["hierarchy"])
    .replace("__ICON_CALC_ITEM__", _UI_ICONS["calculation_item"])
    .replace("__ICON_RELATIONSHIP__", _UI_ICONS["relationship"])
    .replace("__ICON_USERS__", _UI_ICONS["users"])
    .replace("__ICON_CODE__", _UI_ICONS["code"])
    .replace("__ICON_EYE__", _UI_ICONS["eye"])
    .replace("__ICON_TEXT_TYPE__", _UI_ICONS["text_type"])
)


# ---------------------------------------------------------------------------
# Diff engine
# ---------------------------------------------------------------------------
# The object kinds compared, in display order.
DIFF_KINDS = [
    "Table",
    "Column",
    "Measure",
    "Hierarchy",
    "Calculation item",
    "Relationship",
    "Role",
    "Expression",
    "Function",
    "Perspective",
    "Perspective object",
    "Culture",
    "Translation",
]

_SEP = "\u0001"


def _str(value) -> Optional[str]:
    """Normalize a string property: trimmed, with empty treated as absent."""
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _yes_no(value) -> str:
    """Boolean as Yes/No (None and False both read as "No" to avoid noise)."""
    return "Yes" if value else "No"


def _get(obj, *names):
    """First readable attribute from ``names``; .NET getters can raise."""
    for name in names:
        try:
            value = getattr(obj, name)
        except Exception:
            continue
        if value is not None:
            return value
    return None


def _entity(key, kind, label, props, table=None):
    return {"key": key, "kind": kind, "label": label, "table": table, "props": props}


def _build_entities(tom) -> list:
    """Flatten a connected model into comparable ``{key, kind, label, props}``."""
    import Microsoft.AnalysisServices.Tabular as TOM

    out = []

    for table in tom.model.Tables:
        t = table.Name
        out.append(
            _entity(
                f"table{_SEP}{t}",
                "Table",
                t,
                {
                    "Hidden": _yes_no(table.IsHidden),
                    "Description": _str(table.Description),
                    "Data category": _str(table.DataCategory),
                    "Calculation group": _yes_no(table.CalculationGroup is not None),
                },
                t,
            )
        )

        for col in table.Columns:
            if col.Type == TOM.ColumnType.RowNumber:
                continue
            sort_by = _get(col, "SortByColumn")
            out.append(
                _entity(
                    f"column{_SEP}{t}{_SEP}{col.Name}",
                    "Column",
                    f"'{t}'[{col.Name}]",
                    {
                        "Data type": _str(col.DataType),
                        "Column type": _str(col.Type),
                        "Expression": _str(_get(col, "Expression")),
                        "Hidden": _yes_no(col.IsHidden),
                        "Format string": _str(col.FormatString),
                        "Sort by column": _str(sort_by.Name if sort_by else None),
                        "Summarize by": _str(col.SummarizeBy),
                        "Data category": _str(col.DataCategory),
                        "Display folder": _str(col.DisplayFolder),
                        "Description": _str(col.Description),
                        "Is key": _yes_no(col.IsKey),
                    },
                    t,
                )
            )

        for measure in table.Measures:
            fse = _get(measure, "FormatStringDefinition")
            out.append(
                _entity(
                    f"measure{_SEP}{t}{_SEP}{measure.Name}",
                    "Measure",
                    f"'{t}'[{measure.Name}]",
                    {
                        "Expression": _str(measure.Expression),
                        "Format string": _str(measure.FormatString),
                        "Format string expression": _str(
                            fse.Expression if fse else None
                        ),
                        "Hidden": _yes_no(measure.IsHidden),
                        "Display folder": _str(measure.DisplayFolder),
                        "Data type": _str(_get(measure, "DataType")),
                        "Data category": _str(_get(measure, "DataCategory")),
                        "Description": _str(measure.Description),
                    },
                    t,
                )
            )

        for hierarchy in table.Hierarchies:
            levels = " \u203a ".join(
                f"{lvl.Name} ({lvl.Column.Name})" if _get(lvl, "Column") else lvl.Name
                for lvl in sorted(hierarchy.Levels, key=lambda x: x.Ordinal)
            )
            out.append(
                _entity(
                    f"hierarchy{_SEP}{t}{_SEP}{hierarchy.Name}",
                    "Hierarchy",
                    f"'{t}'[{hierarchy.Name}]",
                    {
                        "Levels": _str(levels),
                        "Hidden": _yes_no(hierarchy.IsHidden),
                        "Display folder": _str(hierarchy.DisplayFolder),
                        "Description": _str(hierarchy.Description),
                    },
                    t,
                )
            )

        if table.CalculationGroup is not None:
            for item in table.CalculationGroup.CalculationItems:
                fse = _get(item, "FormatStringDefinition")
                ordinal = _get(item, "Ordinal")
                out.append(
                    _entity(
                        f"calcitem{_SEP}{t}{_SEP}{item.Name}",
                        "Calculation item",
                        f"'{t}'[{item.Name}]",
                        {
                            "Expression": _str(item.Expression),
                            "Ordinal": (
                                str(ordinal)
                                if ordinal is not None and int(ordinal) >= 0
                                else None
                            ),
                            "Format string expression": _str(
                                fse.Expression if fse else None
                            ),
                        },
                        t,
                    )
                )

    for rel in tom.model.Relationships:
        try:
            ft, fc = rel.FromTable.Name, rel.FromColumn.Name
            tt, tc = rel.ToTable.Name, rel.ToColumn.Name
        except Exception:
            continue
        out.append(
            _entity(
                f"relationship{_SEP}{ft}{_SEP}{fc}{_SEP}{tt}{_SEP}{tc}",
                "Relationship",
                f"'{ft}'[{fc}] \u2192 '{tt}'[{tc}]",
                {
                    "Active": _yes_no(rel.IsActive),
                    "Cross filter": _str(_get(rel, "CrossFilteringBehavior")),
                    "From cardinality": _str(_get(rel, "FromCardinality")),
                    "To cardinality": _str(_get(rel, "ToCardinality")),
                    "Security filtering": _str(_get(rel, "SecurityFilteringBehavior")),
                    "Rely on referential integrity": _yes_no(
                        _get(rel, "RelyOnReferentialIntegrity")
                    ),
                },
            )
        )

    for role in tom.model.Roles:
        rls, ols, members = [], [], []
        try:
            for perm in role.TablePermissions:
                if _str(perm.FilterExpression):
                    rls.append(f"{perm.Table.Name}: {perm.FilterExpression.strip()}")
                if str(perm.MetadataPermission) not in ("Default", "None"):
                    ols.append(f"{perm.Table.Name} = {perm.MetadataPermission}")
                for cp in _get(perm, "ColumnPermissions") or []:
                    ols.append(
                        f"{perm.Table.Name}[{cp.Name}] = {cp.MetadataPermission}"
                    )
        except Exception:
            pass
        try:
            members = [str(_get(m, "MemberName", "MemberID")) for m in role.Members]
        except Exception:
            members = []
        out.append(
            _entity(
                f"role{_SEP}{role.Name}",
                "Role",
                role.Name,
                {
                    "Model permission": _str(role.ModelPermission),
                    "Members": _str(", ".join(sorted(m for m in members if m))),
                    "Row-level security": _str("\n".join(sorted(rls))),
                    "Object-level security": _str("\n".join(sorted(ols))),
                    "Description": _str(role.Description),
                },
            )
        )

    for prefix, kind, collection in (
        ("expression", "Expression", _get(tom.model, "Expressions") or []),
        ("function", "Function", _get(tom.model, "Functions") or []),
    ):
        for expr in collection:
            out.append(
                _entity(
                    f"{prefix}{_SEP}{expr.Name}",
                    kind,
                    expr.Name,
                    {
                        "Kind": _str(_get(expr, "Kind")),
                        "Expression": _str(expr.Expression),
                        "Hidden": _yes_no(_get(expr, "IsHidden")),
                        "Description": _str(_get(expr, "Description")),
                    },
                )
            )

    for perspective in tom.model.Perspectives:
        name = perspective.Name
        out.append(_entity(f"perspective{_SEP}{name}", "Perspective", name, {}))
        for pt in perspective.PerspectiveTables:
            t = pt.Table.Name
            members = [
                ("C", "Column", [c.Column.Name for c in pt.PerspectiveColumns]),
                ("M", "Measure", [m.Measure.Name for m in pt.PerspectiveMeasures]),
                (
                    "H",
                    "Hierarchy",
                    [h.Hierarchy.Name for h in pt.PerspectiveHierarchies],
                ),
            ]
            if not any(names for _, _, names in members):
                # No explicit members means the entire table is included.
                out.append(
                    _entity(
                        f"pobj{_SEP}{name}{_SEP}T{_SEP}{t}",
                        "Perspective object",
                        f"{name} \u203a Table \u203a '{t}' (entire table)",
                        {},
                        t,
                    )
                )
                continue
            for code, object_type, names in members:
                for obj in names:
                    out.append(
                        _entity(
                            f"pobj{_SEP}{name}{_SEP}{code}{_SEP}{t}{_SEP}{obj}",
                            "Perspective object",
                            f"{name} \u203a {object_type} \u203a '{t}'[{obj}]",
                            {},
                            t,
                        )
                    )

    for culture in tom.model.Cultures:
        out.append(
            _entity(
                f"culture{_SEP}{culture.Name}",
                "Culture",
                culture.Name,
                {
                    "Linguistic metadata": _yes_no(
                        _get(culture, "LinguisticMetadata") is not None
                    )
                },
            )
        )
        out.extend(_translation_entities(culture))

    return out


def _translation_entities(culture) -> list:
    """One entity per translated object, carrying its caption/description."""
    grouped = {}
    try:
        translations = list(culture.ObjectTranslations)
    except Exception:
        return []
    for tr in translations:
        obj = _get(tr, "Object")
        if obj is None:
            continue
        object_type = str(_get(obj, "ObjectType") or "")
        name = str(_get(obj, "Name") or "")
        try:
            parent = obj.Parent
        except Exception:
            parent = None
        parent_name = str(_get(parent, "Name") or "") if parent is not None else ""
        if object_type == "Table":
            key, label = name, f"'{name}'"
        elif object_type == "Level":
            hierarchy = parent_name
            table = str(_get(_get(parent, "Table"), "Name") or "")
            key = f"{table}|{hierarchy}|{name}"
            label = f"'{table}'[{hierarchy}] \u203a {name}"
        elif object_type in ("Column", "Measure", "Hierarchy"):
            key = f"{parent_name}|{name}"
            label = f"'{parent_name}'[{name}]"
        else:
            continue
        slot = grouped.setdefault(
            f"{object_type}{_SEP}{key}",
            {"label": label, "props": {}, "object_type": object_type, "key": key},
        )
        prop = str(_get(tr, "Property") or "")
        column = {
            "Caption": "Caption",
            "Description": "Description",
            "DisplayFolder": "Display folder",
        }.get(prop)
        if column:
            slot["props"][column] = _str(_get(tr, "Value"))

    out = []
    for slot in grouped.values():
        out.append(
            _entity(
                f"translation{_SEP}{culture.Name}{_SEP}{slot['object_type']}"
                f"{_SEP}{slot['key']}",
                "Translation",
                f"{culture.Name} \u203a {slot['label']}",
                {
                    "Caption": slot["props"].get("Caption"),
                    "Description": slot["props"].get("Description"),
                    "Display folder": slot["props"].get("Display folder"),
                },
            )
        )
    return out


def _full_props(props: dict, side: str) -> list:
    """Every non-empty property of an entity as a one-sided diff."""
    return [
        {"name": name, side: value}
        for name, value in props.items()
        if value is not None
    ]


def _changed_props(current: dict, compared: dict) -> list:
    """The properties that differ between two entities."""
    out = []
    for name in list(current.keys()) + [n for n in compared.keys() if n not in current]:
        cur, cmp_ = current.get(name), compared.get(name)
        if (cur or "") != (cmp_ or ""):
            out.append({"name": name, "current": cur, "compared": cmp_})
    return out


def _compute_diff(current: list, compared: list):
    """Object-level diff between the current and compared model snapshots."""
    a = {e["key"]: e for e in current}
    b = {e["key"]: e for e in compared}
    summary = {"added": 0, "removed": 0, "modified": 0, "unchanged": 0, "total": 0}
    entities = []

    for key in list(a.keys()) + [k for k in b.keys() if k not in a]:
        ea, eb = a.get(key), b.get(key)
        if ea is not None and eb is None:
            base, status, props = ea, "removed", _full_props(ea["props"], "current")
        elif ea is None and eb is not None:
            base, status, props = eb, "added", _full_props(eb["props"], "compared")
        else:
            props = _changed_props(ea["props"], eb["props"])
            base, status = ea, "modified" if props else "unchanged"
        summary[status] += 1
        entities.append(
            {
                "key": base["key"],
                "kind": base["kind"],
                "label": base["label"],
                "table": base["table"],
                "status": status,
                "props": props,
            }
        )

    summary["total"] = len(entities)
    kind_index = {k: i for i, k in enumerate(DIFF_KINDS)}
    entities.sort(
        key=lambda e: (
            kind_index.get(e["kind"], len(DIFF_KINDS)),
            e["table"] or "",
            e["label"],
        )
    )
    return entities, summary


@log
def model_comparison(
    dataset: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    compare_dataset: Optional[str | UUID] = None,
    compare_workspace: Optional[str | UUID] = None,
    dark_mode: bool = False,
):
    """
    Displays an interactive comparison of two semantic models.

    Every object in both models — tables, columns, measures, hierarchies,
    calculation items, relationships, roles, expressions, functions,
    perspectives, cultures and translations — is matched by name and reported as
    added (only in the compared model), removed (only in the current model),
    modified (present in both but some property differs) or unchanged. Selecting
    a modified object shows each differing property side by side.

    Results can be filtered by status, searched, and grouped by object kind.

    Parameters
    ----------
    dataset : str | uuid.UUID, default=None
        Name or ID of the semantic model on the "current" side of the comparison.
        Defaults to None which opens the view on a picker so both models can be
        chosen interactively.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID of the "current" semantic model.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    compare_dataset : str | uuid.UUID, default=None
        Name or ID of the semantic model to compare against.
        Defaults to None which requires the model to be chosen in the picker.
    compare_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID of the semantic model to compare against.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    dark_mode : bool, default=False
        If True, renders the view with a dark color theme. If False, renders with
        a light color theme.
    """

    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "The 'model_comparison' function requires the 'anywidget' package. "
            "Install it with: pip install anywidget"
        ) from e

    import sempy.fabric as fabric
    from IPython.display import display
    from sempy_labs._helper_functions import (
        resolve_workspace_name_and_id,
        resolve_dataset_name_and_id,
    )
    from sempy_labs.tom import connect_semantic_model

    ws_name, ws_id = resolve_workspace_name_and_id(workspace)
    ws_id = str(ws_id)
    if dataset is not None:
        ds_name, ds_id = resolve_dataset_name_and_id(dataset, ws_id)
        ds_id = str(ds_id)
    else:
        ds_name, ds_id = "", ""

    if compare_dataset is not None:
        cmp_ws_name, cmp_ws_id = resolve_workspace_name_and_id(
            compare_workspace if compare_workspace is not None else workspace
        )
        cmp_ws_id = str(cmp_ws_id)
        cmp_ds_name, cmp_ds_id = resolve_dataset_name_and_id(compare_dataset, cmp_ws_id)
        cmp_ds_id = str(cmp_ds_id)
    else:
        cmp_ws_name, cmp_ws_id, cmp_ds_name, cmp_ds_id = "", "", "", ""

    def _snapshot(dataset_id, workspace_id):
        with connect_semantic_model(
            dataset=dataset_id, readonly=True, workspace=workspace_id
        ) as tom:
            return _build_entities(tom)

    def _list_workspaces_payload():
        try:
            dfW = fabric.list_workspaces()
        except Exception:
            return [{"id": ws_id, "name": str(ws_name or "")}]
        cols = list(dfW.columns)
        id_col = "Id" if "Id" in cols else cols[0]
        name_col = "Name" if "Name" in cols else cols[-1]
        out = [
            {"id": str(r[id_col]), "name": str(r[name_col])} for _, r in dfW.iterrows()
        ]
        return sorted(out, key=lambda x: x["name"].lower())

    def _list_datasets_payload(target_ws_id):
        try:
            dfD = fabric.list_datasets(workspace=target_ws_id)
        except Exception:
            return []
        cols = list(dfD.columns)
        id_col = next(
            (c for c in ["Dataset Id", "Dataset ID", "Id"] if c in cols),
            cols[0] if cols else None,
        )
        name_col = next(
            (c for c in ["Dataset Name", "Name"] if c in cols),
            cols[-1] if cols else None,
        )
        if id_col is None or name_col is None:
            return []
        out = [
            {"id": str(r[id_col]), "name": str(r[name_col])} for _, r in dfD.iterrows()
        ]
        return sorted(out, key=lambda x: x["name"].lower())

    # Both models are read up-front only when the caller named both; otherwise
    # the picker is shown and nothing is read until the user presses Compare.
    initial_entities, initial_summary = [], {}
    initial_status = {}
    initial_compared = False
    if ds_id and cmp_ds_id:
        try:
            initial_entities, initial_summary = _compute_diff(
                _snapshot(ds_id, ws_id), _snapshot(cmp_ds_id, cmp_ws_id)
            )
            initial_compared = True
        except Exception as e:
            initial_status = {
                "message": f"Error comparing models: {e}",
                "kind": "error",
            }

    initial_workspaces = _list_workspaces_payload()
    initial_datasets = {ws_id: _list_datasets_payload(ws_id)}
    if cmp_ws_id and cmp_ws_id != ws_id:
        initial_datasets[cmp_ws_id] = _list_datasets_payload(cmp_ws_id)

    class ModelComparisonWidget(anywidget.AnyWidget):
        _esm = _WIDGET_JS
        _css = _WIDGET_CSS

        base_name = traitlets.Unicode("").tag(sync=True)
        base_id = traitlets.Unicode("").tag(sync=True)
        base_workspace = traitlets.Unicode("").tag(sync=True)
        base_workspace_id = traitlets.Unicode("").tag(sync=True)
        compared_name = traitlets.Unicode("").tag(sync=True)
        compared_workspace = traitlets.Unicode("").tag(sync=True)
        compared = traitlets.Bool(False).tag(sync=True)
        entities = traitlets.List().tag(sync=True)
        summary = traitlets.Dict().tag(sync=True)
        kinds = traitlets.List().tag(sync=True)
        workspaces = traitlets.List().tag(sync=True)
        datasets = traitlets.Dict().tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        compare_done = traitlets.Int(0).tag(sync=True)
        busy = traitlets.Bool(False).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)

    widget = ModelComparisonWidget(
        base_name=ds_name,
        base_id=ds_id,
        base_workspace=ws_name or "",
        base_workspace_id=ws_id,
        compared_name=cmp_ds_name,
        compared_workspace=cmp_ws_name or "",
        compared=initial_compared,
        entities=initial_entities,
        summary=initial_summary,
        kinds=DIFF_KINDS,
        workspaces=initial_workspaces,
        datasets=initial_datasets,
        status=initial_status,
        pending_action={},
        run=0,
        compare_done=0,
        busy=False,
        dark_mode=bool(dark_mode),
    )

    # Identifiers of the two models currently being compared.
    state = {
        "base_ws": ws_id,
        "base_ds": ds_id,
        "cmp_ws": cmp_ws_id,
        "cmp_ds": cmp_ds_id,
    }

    def _run_comparison():
        entities, summary = _compute_diff(
            _snapshot(state["base_ds"], state["base_ws"]),
            _snapshot(state["cmp_ds"], state["cmp_ws"]),
        )
        widget.entities = entities
        widget.summary = summary
        widget.compared = True
        widget.compare_done = widget.compare_done + 1

    def _on_run(_change):
        data = dict(widget.pending_action or {})
        action = data.get("action")
        if not action:
            return
        widget.busy = True
        try:
            if action == "list_workspaces":
                widget.workspaces = _list_workspaces_payload()

            elif action == "list_datasets":
                target_ws = data.get("workspace_id")
                if target_ws:
                    new_map = dict(widget.datasets)
                    new_map[str(target_ws)] = _list_datasets_payload(target_ws)
                    widget.datasets = new_map

            elif action == "compare":
                if not data.get("base_id") or not data.get("compared_id"):
                    widget.status = {
                        "message": "Select a semantic model on both sides.",
                        "kind": "error",
                    }
                    return
                state["base_ws"] = str(data.get("base_workspace_id") or "")
                state["base_ds"] = str(data.get("base_id"))
                state["cmp_ws"] = str(data.get("compared_workspace_id") or "")
                state["cmp_ds"] = str(data.get("compared_id"))
                widget.base_name = data.get("base_name") or ""
                widget.base_id = state["base_ds"]
                widget.base_workspace = data.get("base_workspace_name") or ""
                widget.base_workspace_id = state["base_ws"]
                widget.compared_name = data.get("compared_name") or ""
                widget.compared_workspace = data.get("compared_workspace_name") or ""
                _run_comparison()
                widget.status = {}

            elif action == "reload":
                if not state["base_ds"] or not state["cmp_ds"]:
                    return
                _run_comparison()
                widget.status = {
                    "message": "Both models reloaded and compared.",
                    "kind": "success",
                }
        except Exception as e:
            widget.status = {"message": f"Error: {e}", "kind": "error"}
        finally:
            widget.busy = False

    widget.observe(_on_run, names=["run"])

    display(widget)
