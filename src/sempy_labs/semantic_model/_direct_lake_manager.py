from typing import Optional
from uuid import UUID
from sempy._utils._log import log

_SOURCE_TYPES = [
    "Lakehouse",
    "Warehouse",
    "SQLDatabase",
    "MirroredAzureDatabricksCatalog",
    "MirroredDatabase",
]


_WIDGET_CSS = """
.slls-dle {
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
/* Dark-mode tokens: auto (system preference) + explicit class. */
@media (prefers-color-scheme: dark) { .slls-dle.slls-dle-auto { --slls-bg-solid: #1c1c1e; --slls-surface: rgba(255,255,255,0.04); --slls-surface-2: rgba(255,255,255,0.03); --slls-border: rgba(255,255,255,0.08); --slls-border-strong: rgba(255,255,255,0.16); --slls-text: #f5f5f7; --slls-text-secondary: #a1a1a6; --slls-text-tertiary: #6e6e73; --slls-accent-soft: rgba(10,132,255,0.18); --slls-accent: #0A84FF; --slls-shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.5); } }
.slls-dle.slls-dle-dark { --slls-bg-solid: #1c1c1e; --slls-surface: rgba(255,255,255,0.04); --slls-surface-2: rgba(255,255,255,0.03); --slls-border: rgba(255,255,255,0.08); --slls-border-strong: rgba(255,255,255,0.16); --slls-text: #f5f5f7; --slls-text-secondary: #a1a1a6; --slls-text-tertiary: #6e6e73; --slls-accent-soft: rgba(10,132,255,0.18); --slls-accent: #0A84FF; --slls-shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.5); }
.slls-dle * { box-sizing: border-box; }

.slls-dle-header { display: flex; align-items: center; gap: 12px; margin-bottom: 18px; flex-wrap: wrap; }
.slls-dle-titlewrap { display: flex; flex-direction: column; margin-right: auto; min-width: 0; }
.slls-dle-title { font-size: 22px; font-weight: 600; letter-spacing: -0.01em; line-height: 1.15; }
.slls-dle-subtitle { font-size: 12px; color: var(--slls-text-secondary); margin-top: 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 600px; }
.slls-dle-subtitle .slls-dle-sep { color: var(--slls-text-tertiary); margin: 0 6px; }
.slls-dle-subtitle b { color: var(--slls-text); font-weight: 500; }

.slls-dle-select, .slls-dle-input {
    appearance: none; -webkit-appearance: none;
    background: var(--slls-surface);
    border: 1px solid var(--slls-border-strong);
    border-radius: 999px;
    padding: 7px 14px;
    font-size: 13.5px;
    color: var(--slls-text);
    font-family: inherit;
    transition: border-color 120ms ease, box-shadow 120ms ease;
}
.slls-dle-select {
    cursor: pointer;
    padding-right: 32px;
    background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6'><path fill='%236e6e73' d='M0 0l5 6 5-6z'/></svg>");
    background-repeat: no-repeat;
    background-position: right 12px center;
}
.slls-dle-select:hover, .slls-dle-input:hover { border-color: var(--slls-text-tertiary); }
.slls-dle-select:focus, .slls-dle-input:focus { outline: none; border-color: var(--slls-accent); box-shadow: 0 0 0 3px var(--slls-accent-soft); }
/* The browser-native dropdown list renders against the system surface, so
   force explicit colors that remain legible in both light and dark modes. */
.slls-dle-select option, .slls-dle-select optgroup { background: #ffffff; color: #1d1d1f; }
@media (prefers-color-scheme: dark) { .slls-dle.slls-dle-auto .slls-dle-select option, .slls-dle.slls-dle-auto .slls-dle-select optgroup { background: #2c2c2e; color: #f5f5f7; } }
.slls-dle.slls-dle-dark .slls-dle-select option, .slls-dle.slls-dle-dark .slls-dle-select optgroup { background: #2c2c2e; color: #f5f5f7; }
.slls-dle-input::placeholder { color: var(--slls-text-tertiary); }

.slls-dle-btn {
    appearance: none;
    border: 1px solid var(--slls-border-strong);
    background: var(--slls-surface);
    color: var(--slls-text);
    font-family: inherit; font-size: 13.5px; font-weight: 500;
    padding: 7px 16px;
    border-radius: 999px;
    cursor: pointer;
    transition: background 120ms ease, border-color 120ms ease, transform 80ms ease, box-shadow 120ms ease, opacity 120ms ease;
}
.slls-dle-btn:hover { background: var(--slls-surface-2); border-color: var(--slls-text-tertiary); }
.slls-dle-btn:active { transform: scale(0.97); }
.slls-dle-btn:disabled { opacity: 0.4; cursor: not-allowed; }
.slls-dle-btn-primary { background: var(--slls-accent); border-color: var(--slls-accent); color: #fff; }
.slls-dle-btn-primary:hover { background: var(--slls-accent-hover); border-color: var(--slls-accent-hover); }
.slls-dle-btn-danger { background: transparent; border-color: var(--slls-danger); color: var(--slls-danger); }
.slls-dle-btn-danger:hover { background: var(--slls-danger-soft); }
.slls-dle-btn-icon { width: 32px; height: 32px; padding: 0; display: inline-flex; align-items: center; justify-content: center; border-radius: 50%; font-size: 18px; line-height: 1; }

.slls-dle-toolbar { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }

.slls-dle-section { border: 1px solid var(--slls-border); border-radius: var(--slls-radius); background: var(--slls-surface); padding: 16px; margin-top: 14px; }
.slls-dle-section h3 { margin: 0 0 12px 0; font-size: 14px; font-weight: 600; color: var(--slls-text); display: flex; align-items: center; gap: 8px; }
.slls-dle-section h3 .slls-dle-count { color: var(--slls-text-tertiary); font-weight: 400; font-size: 12.5px; }

/* minmax(0, 1fr) lets columns shrink so long option labels in
   child <select> elements don't blow out the modal width. */
.slls-dle-grid { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 12px; }
.slls-dle-grid > .slls-dle-field { min-width: 0; }
.slls-dle-grid > .slls-dle-field-wide { grid-column: 1 / -1; }
.slls-dle-grid .slls-dle-select, .slls-dle-grid .slls-dle-input { width: 100%; max-width: 100%; min-width: 0; text-overflow: ellipsis; overflow: hidden; white-space: nowrap; }
.slls-dle-field { display: flex; flex-direction: column; gap: 4px; }
.slls-dle-field label { font-size: 12px; color: var(--slls-text-secondary); padding-left: 10px; }
.slls-dle-checkbox { display: inline-flex; align-items: center; gap: 8px; font-size: 13.5px; color: var(--slls-text); cursor: pointer; user-select: none; }

.slls-dle-list { border: 1px solid var(--slls-border); border-radius: var(--slls-radius-sm); background: var(--slls-bg-solid); overflow: hidden; max-height: 360px; overflow-y: auto; }
.slls-dle-list::-webkit-scrollbar { width: 10px; height: 10px; }
.slls-dle-list::-webkit-scrollbar-thumb { background: var(--slls-border-strong); border-radius: 999px; background-clip: padding-box; border: 2px solid transparent; }
.slls-dle-list::-webkit-scrollbar-thumb:hover { background-color: var(--slls-text-tertiary); }
.slls-dle-item { display: flex; align-items: center; gap: 10px; padding: 10px 14px; border-bottom: 1px solid var(--slls-border); }
.slls-dle-item:last-child { border-bottom: none; }
.slls-dle-item-main { display: flex; flex-direction: column; gap: 2px; min-width: 0; flex: 1; }
.slls-dle-item-name { font-size: 14px; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-dle-item-meta { font-size: 12px; color: var(--slls-text-tertiary); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-dle-item-actions { display: inline-flex; gap: 6px; flex-shrink: 0; }

/* Apple-style icon-only button used as a menu trigger ("•••"). */
.slls-dle-icon-btn { width: 30px; height: 30px; padding: 0; border-radius: 8px; border: 1px solid transparent; background: transparent; color: var(--slls-text-secondary); cursor: pointer; display: inline-flex; align-items: center; justify-content: center; transition: background 120ms ease, color 120ms ease, border-color 120ms ease; }
.slls-dle-icon-btn:hover { background: var(--slls-surface-2); color: var(--slls-text); }
.slls-dle-icon-btn.active { background: var(--slls-surface-2); color: var(--slls-text); border-color: var(--slls-border); }

/* Floating popover menu. Tokens are re-declared because it is portaled to document.body. */
.slls-dle-menu {
    --slls-surface: #ffffff; --slls-surface-2: rgba(0,0,0,0.05); --slls-border: rgba(0,0,0,0.10);
    --slls-text: #1d1d1f; --slls-text-secondary: #6e6e73; --slls-red: #ff3b30;
    position: fixed; z-index: 10000;
    background: var(--slls-surface); border: 1px solid var(--slls-border); border-radius: 12px;
    box-shadow: 0 1px 1px rgba(0,0,0,0.04), 0 10px 30px rgba(0,0,0,0.18);
    padding: 6px; min-width: 200px; color: var(--slls-text);
    backdrop-filter: saturate(1.4) blur(20px); -webkit-backdrop-filter: saturate(1.4) blur(20px);
    opacity: 0; transform: translateY(-4px) scale(0.98); transform-origin: top right;
    transition: opacity 120ms ease, transform 120ms ease;
}
@media (prefers-color-scheme: dark) { .slls-dle-menu { --slls-surface: #2c2c2e; --slls-surface-2: rgba(255,255,255,0.08); --slls-border: rgba(255,255,255,0.12); --slls-text: #f5f5f7; --slls-text-secondary: #a1a1a6; box-shadow: 0 1px 1px rgba(0,0,0,0.3), 0 10px 30px rgba(0,0,0,0.55); } }
.slls-dle-menu.show { opacity: 1; transform: translateY(0) scale(1); }
.slls-dle-menu-item { display: flex; align-items: center; gap: 10px; width: 100%; padding: 7px 10px; border: none; background: transparent; color: var(--slls-text); font-size: 13px; text-align: left; border-radius: 7px; cursor: pointer; transition: background 100ms ease; }
.slls-dle-menu-item:hover, .slls-dle-menu-item:focus-visible { outline: none; background: var(--slls-surface-2); }
.slls-dle-menu-item-icon { width: 18px; display: inline-flex; align-items: center; justify-content: center; color: var(--slls-text-secondary); flex-shrink: 0; }
.slls-dle-menu-item-label { flex: 1; min-width: 0; }
.slls-dle-menu-divider { height: 1px; background: var(--slls-border); margin: 4px 2px; }

.slls-dle-pill { display: inline-block; padding: 2px 8px; border-radius: 999px; background: var(--slls-accent-soft); color: var(--slls-accent); font-size: 11.5px; font-weight: 500; }

.slls-dle-icon { display: inline-flex; align-items: center; justify-content: center; flex-shrink: 0; color: var(--slls-text-secondary); opacity: 0.9; width: 18px; height: 18px; }
.slls-dle-icon svg { display: block; }
.slls-dle-icon-inline { display: inline-flex; align-items: center; gap: 6px; min-width: 0; }

.slls-dle-empty { padding: 24px 16px; text-align: center; color: var(--slls-text-tertiary); font-size: 13.5px; }

.slls-dle-status { margin-top: 14px; margin-bottom: 6px; padding: 10px 14px; border-radius: var(--slls-radius-sm); font-size: 13.5px; display: none; }
.slls-dle-status.show { display: block; animation: slls-dle-fade-in 200ms ease; }
.slls-dle-status.success { background: var(--slls-success-soft); color: var(--slls-success); }
.slls-dle-status.error { background: var(--slls-danger-soft); color: var(--slls-danger); }
.slls-dle-status.info { background: var(--slls-accent-soft); color: var(--slls-accent); }

/* Orange "pending change" indicator for items modified since the last save. */
.slls-dle-pending-dot { width: 9px; height: 9px; border-radius: 50%; background: var(--slls-orange); display: inline-block; flex-shrink: 0; box-shadow: 0 0 0 2px rgba(255,149,0,0.18); }
.slls-dle-savebar { display: none; align-items: center; gap: 10px; padding: 10px 14px; margin-top: 10px; border-radius: var(--slls-radius-sm); background: rgba(255,149,0,0.10); border: 1px solid rgba(255,149,0,0.45); color: var(--slls-text); }
.slls-dle-savebar.show { display: flex; }
.slls-dle-savebar-label { font-size: 13.5px; margin-right: auto; display: flex; align-items: center; gap: 8px; }
.slls-dle-item-name .slls-dle-pending-dot { margin-right: 4px; }
.slls-dle-item.pending { background: rgba(255,149,0,0.06); }

/* Buffer above the manage toolbar so the Refresh button does not crowd
   any preceding cell output / status banner. */
.slls-dle-manage-top { margin-top: 8px; padding-top: 6px; border-top: 1px solid var(--slls-border); }
@keyframes slls-dle-fade-in { from { opacity: 0; transform: translateY(-4px); } to { opacity: 1; transform: translateY(0); } }

.slls-dle-busy { pointer-events: none; opacity: 0.55; transition: opacity 120ms ease; }

.slls-dle-overlay { display: none; position: absolute; inset: 0; background: rgba(0,0,0,0.45); z-index: 50; align-items: flex-start; justify-content: center; padding: 24px 16px; border-radius: var(--slls-radius); overflow-y: auto; }
.slls-dle-overlay.show { display: flex; }
.slls-dle-modal { background: var(--slls-bg-solid); color: var(--slls-text); border: 1px solid var(--slls-border); border-radius: var(--slls-radius); box-shadow: 0 30px 80px rgba(0,0,0,0.35); width: 100%; max-width: 560px; padding: 22px; margin: auto; }
.slls-dle-modal h2 { margin: 0 0 14px 0; font-size: 17px; font-weight: 600; }
.slls-dle-modal-footer { display: flex; justify-content: flex-end; gap: 8px; margin-top: 18px; }
.slls-dle-modal-wide { max-width: 820px; }
/* Extra-wide modal for the relationships editor, where each row contains
   two table+column pairs plus cardinality dropdowns and table/column
   names can be longer than what fits in the standard wide modal. */
.slls-dle-modal-xwide { max-width: 1100px; }
/* Pins the modal to the bottom of the overlay instead of the default vertical center. Useful for context popups (e.g. the relationships editor) that should sit close to the controls that opened them. */
.slls-dle-modal-bottom { margin-top: auto; margin-bottom: 16px; }
.slls-dle-collapse-btn { background: transparent; border: none; cursor: pointer; color: var(--slls-text-secondary); padding: 2px 6px; font-size: 12px; line-height: 1; border-radius: 4px; display: inline-flex; align-items: center; justify-content: center; transition: transform 120ms ease, background 120ms ease; }
.slls-dle-collapse-btn:hover { background: var(--slls-surface-2); color: var(--slls-text); }
.slls-dle-collapse-btn[aria-expanded="false"] { transform: rotate(-90deg); }
.slls-dle-columns-list { display: flex; flex-direction: column; gap: 8px; max-height: 60vh; overflow-y: auto; padding-right: 4px; margin-top: 4px; }
.slls-dle-column-row { border: 1px solid var(--slls-border); border-radius: var(--slls-radius-sm); background: var(--slls-surface-2); padding: 10px 12px; transition: border-color 120ms ease, background 120ms ease; }
.slls-dle-column-row.pending { border-color: var(--slls-orange); background: rgba(255,149,0,0.06); }
.slls-dle-column-head { display: flex; align-items: baseline; justify-content: space-between; gap: 8px; margin-bottom: 6px; }
.slls-dle-column-name { font-weight: 600; font-size: 13.5px; color: var(--slls-text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-dle-column-type { font-size: 11.5px; color: var(--slls-text-tertiary); text-transform: uppercase; letter-spacing: 0.4px; }
.slls-dle-column-fields { display: grid; grid-template-columns: minmax(0, 1.2fr) minmax(0, 1.4fr) minmax(0, 1fr); gap: 10px; }
.slls-dle-column-fields > .slls-dle-field { min-width: 0; }
.slls-dle-column-fields > .slls-dle-field-wide { grid-column: 1 / -1; }
.slls-dle-column-fields .slls-dle-select, .slls-dle-column-fields .slls-dle-input { width: 100%; max-width: 100%; min-width: 0; text-overflow: ellipsis; overflow: hidden; white-space: nowrap; }
.slls-dle-column-fields textarea.slls-dle-input { white-space: pre-wrap; text-overflow: clip; overflow: auto; resize: vertical; min-height: 38px; }

.slls-dle-tablerows { border: 1px solid var(--slls-border); border-radius: var(--slls-radius-sm); max-height: 240px; overflow-y: auto; margin-top: 6px; }
.slls-dle-tablerow { display: flex; align-items: center; gap: 10px; padding: 8px 12px; border-bottom: 1px solid var(--slls-border); font-size: 13px; }
.slls-dle-tablerow:last-child { border-bottom: none; }
.slls-dle-tablerow label { flex: 1; cursor: pointer; }

/* Per-row primary-key toggle used on the column-selection screen. */
.slls-dle-pk-toggle { display: inline-flex; align-items: center; justify-content: center; flex-shrink: 0; font-size: 10px; font-weight: 700; line-height: 1; letter-spacing: 0.4px; color: var(--slls-text-tertiary); border: 1px solid var(--slls-border); border-radius: 4px; padding: 2px 5px; cursor: pointer; user-select: none; background: transparent; transition: background 120ms ease, color 120ms ease, border-color 120ms ease; }
.slls-dle-pk-toggle:hover { background: var(--slls-surface-2); color: var(--slls-text-secondary); }
.slls-dle-pk-toggle.active { background: var(--slls-accent-soft); color: var(--slls-accent); border-color: var(--slls-accent); }
.slls-dle-pk-toggle.detected:not(.active) { border-style: dashed; color: var(--slls-accent); }
.slls-dle-pk-toggle.disabled { opacity: 0.4; cursor: not-allowed; }

.slls-dle-attribution { margin-top: 18px; text-align: right; font-size: 11.5px; color: var(--slls-text-tertiary); }
.slls-dle-attribution a { color: var(--slls-text-tertiary); text-decoration: none; transition: color 120ms ease; }
.slls-dle-attribution a:hover { color: var(--slls-accent); }

.slls-dle-screen { display: none; }
.slls-dle-screen.show { display: block; }
"""


_WIDGET_JS = r"""
function render({ model, el }) {
    const root = document.createElement("div");
    root.className = "slls-dle";

    function applyTheme() {
        root.classList.remove("slls-dle-dark", "slls-dle-auto");
        const dm = model.get("dark_mode");
        if (dm === true) root.classList.add("slls-dle-dark");
        else if (dm === null || dm === undefined) root.classList.add("slls-dle-auto");
    }
    applyTheme();
    model.on("change:dark_mode", applyTheme);
    el.appendChild(root);

    function escapeHtml(s) {
        return String(s ?? "").replace(/[&<>\"']/g, (c) => ({
            "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;"
        }[c]));
    }

    // Icons are injected from sempy_labs._ui_components.ICONS at module load
    // time so every widget shares one source of truth (see template
    // substitution at the bottom of this file).
    const PLUS_SVG = `__SLLS_ICON_PLUS__`;
    const BACK_SVG = `__SLLS_ICON_BACK__`;
    const REFRESH_SVG = `__SLLS_ICON_REFRESH__`;
    const SUN_SVG = `__SLLS_ICON_SUN__`;
    const MOON_SVG = `__SLLS_ICON_MOON__`;
    const ICON_SVG = {
        table: `__SLLS_ICON_TABLE__`,
        column: `__SLLS_ICON_COLUMN__`,
        source: `__SLLS_ICON_SOURCE__`,
        more: `__SLLS_ICON_MORE__`,
        sync: `__SLLS_ICON_SYNC__`,
        pencil: `__SLLS_ICON_PENCIL__`,
        link: `__SLLS_ICON_LINK__`,
    };
    function iconHtml(kind) {
        const svg = ICON_SVG[kind] || "";
        let cls = "slls-dle-icon";
        if (kind === "table") cls += " slls-dle-table-icon";
        else if (kind === "column") cls += " slls-dle-column-icon";
        else if (kind === "source") cls += " slls-dle-source-icon";
        return `<span class="${cls}">${svg}</span>`;
    }

    // Floating popover menu anchored to a trigger button. `items` is an
    // array of { label, icon?, onClick, danger?, separatorBefore? }.
    // Returns nothing; the menu auto-dismisses on outside click, Escape,
    // window scroll, or window resize.
    let _openMenuCleanup = null;
    function closeOpenMenu() {
        if (_openMenuCleanup) {
            _openMenuCleanup();
            _openMenuCleanup = null;
        }
    }
    function openRowMenu(anchor, items) {
        // Toggle if the same anchor reopens its menu.
        if (anchor.classList.contains("active")) {
            closeOpenMenu();
            return;
        }
        closeOpenMenu();

        const menu = document.createElement("div");
        menu.className = "slls-dle-menu";
        menu.setAttribute("role", "menu");
        for (const item of items) {
            if (!item) continue;
            if (item.separatorBefore) {
                const sep = document.createElement("div");
                sep.className = "slls-dle-menu-divider";
                menu.appendChild(sep);
            }
            const btn = document.createElement("button");
            btn.className = "slls-dle-menu-item";
            btn.setAttribute("role", "menuitem");
            if (item.danger) btn.style.color = "var(--slls-red)";
            const icon = document.createElement("span");
            icon.className = "slls-dle-menu-item-icon";
            icon.innerHTML = item.icon
                ? (ICON_SVG[item.icon] || item.icon)
                : "";
            const label = document.createElement("span");
            label.className = "slls-dle-menu-item-label";
            label.textContent = item.label || "";
            btn.appendChild(icon);
            btn.appendChild(label);
            btn.addEventListener("click", (ev) => {
                ev.stopPropagation();
                closeOpenMenu();
                try { item.onClick && item.onClick(); } catch (_) { /* noop */ }
            });
            menu.appendChild(btn);
        }
        document.body.appendChild(menu);

        // Position: right-align with the anchor's right edge, just below.
        const rect = anchor.getBoundingClientRect();
        // Temporarily make visible to measure.
        menu.style.visibility = "hidden";
        menu.style.display = "block";
        const mw = menu.offsetWidth;
        const mh = menu.offsetHeight;
        let left = rect.right - mw;
        let top = rect.bottom + 6;
        // Flip above if not enough room.
        if (top + mh > window.innerHeight - 8) {
            top = Math.max(8, rect.top - mh - 6);
        }
        left = Math.max(8, Math.min(left, window.innerWidth - mw - 8));
        menu.style.left = `${left}px`;
        menu.style.top = `${top}px`;
        menu.style.visibility = "visible";
        // Trigger transition.
        requestAnimationFrame(() => menu.classList.add("show"));

        anchor.classList.add("active");
        const onDocClick = (ev) => {
            if (menu.contains(ev.target) || anchor.contains(ev.target)) return;
            closeOpenMenu();
        };
        const onKey = (ev) => {
            if (ev.key === "Escape") closeOpenMenu();
        };
        const onScrollOrResize = () => closeOpenMenu();
        // Defer attaching the click listener so the current click doesn't
        // immediately dismiss the menu.
        setTimeout(() => {
            document.addEventListener("click", onDocClick, true);
        }, 0);
        document.addEventListener("keydown", onKey, true);
        window.addEventListener("scroll", onScrollOrResize, true);
        window.addEventListener("resize", onScrollOrResize, true);

        _openMenuCleanup = () => {
            document.removeEventListener("click", onDocClick, true);
            document.removeEventListener("keydown", onKey, true);
            window.removeEventListener("scroll", onScrollOrResize, true);
            window.removeEventListener("resize", onScrollOrResize, true);
            anchor.classList.remove("active");
            if (menu.parentNode) menu.parentNode.removeChild(menu);
        };
    }

    // ----------- Header -----------
    const header = document.createElement("div");
    header.className = "slls-dle-header";
    root.appendChild(header);

    const titleWrap = document.createElement("div");
    titleWrap.className = "slls-dle-titlewrap";
    header.appendChild(titleWrap);

    const title = document.createElement("div");
    title.className = "slls-dle-title";
    title.textContent = "Direct Lake Manager";
    titleWrap.appendChild(title);

    const subtitle = document.createElement("div");
    subtitle.className = "slls-dle-subtitle";
    titleWrap.appendChild(subtitle);

    const backBtn = document.createElement("button");
    backBtn.className = "slls-dle-btn slls-dle-btn-icon";
    backBtn.type = "button";
    backBtn.innerHTML = BACK_SVG;
    backBtn.title = "Back to model selection";
    backBtn.setAttribute("aria-label", "Back");
    backBtn.style.display = "none";
    backBtn.addEventListener("click", () => {
        model.set("screen", "select");
        model.save_changes();
    });
    header.appendChild(backBtn);

    const themeBtn = document.createElement("button");
    themeBtn.className = "slls-dle-btn slls-dle-btn-icon";
    themeBtn.type = "button";
    function renderThemeBtn() {
        const isDark = model.get("dark_mode") === true;
        themeBtn.innerHTML = isDark ? SUN_SVG : MOON_SVG;
        themeBtn.title = isDark ? "Switch to light mode" : "Switch to dark mode";
        themeBtn.setAttribute("aria-label", themeBtn.title);
    }
    themeBtn.addEventListener("click", () => {
        model.set("dark_mode", !(model.get("dark_mode") === true));
        model.save_changes();
    });
    model.on("change:dark_mode", renderThemeBtn);
    renderThemeBtn();
    header.appendChild(themeBtn);

    // ----------- Status banner (shared) -----------
    const status = document.createElement("div");
    status.className = "slls-dle-status";
    root.appendChild(status);
    function setStatus(message, kind) {
        if (!message) { status.classList.remove("show"); return; }
        status.className = `slls-dle-status show ${kind || "info"}`;
        status.textContent = message;
    }
    model.on("change:status", () => {
        const s = model.get("status") || {};
        if (s && s.message) setStatus(s.message, s.kind);
        else setStatus("", null);
    });

    function setBusy(b) {
        if (b) root.classList.add("slls-dle-busy");
        else root.classList.remove("slls-dle-busy");
    }
    model.on("change:busy", () => setBusy(model.get("busy") === true));

    function runAction(action, extra) {
        const payload = Object.assign({ action }, extra || {});
        model.set("pending_action", payload);
        model.set("run", (model.get("run") || 0) + 1);
        model.save_changes();
    }

    // ============================================================
    // SELECT SCREEN
    // ============================================================
    const selectScreen = document.createElement("div");
    selectScreen.className = "slls-dle-screen";
    root.appendChild(selectScreen);

    const selectSection = document.createElement("div");
    selectSection.className = "slls-dle-section";
    selectScreen.appendChild(selectSection);

    const selectHeading = document.createElement("h3");
    selectHeading.textContent = "Open a Direct Lake semantic model";
    selectSection.appendChild(selectHeading);

    const selectToolbar = document.createElement("div");
    selectToolbar.className = "slls-dle-toolbar";
    selectSection.appendChild(selectToolbar);

    const wsSelect = document.createElement("select");
    wsSelect.className = "slls-dle-select";
    wsSelect.style.minWidth = "220px";
    selectToolbar.appendChild(wsSelect);

    const dsSelect = document.createElement("select");
    dsSelect.className = "slls-dle-select";
    dsSelect.style.minWidth = "240px";
    selectToolbar.appendChild(dsSelect);

    const openBtn = document.createElement("button");
    openBtn.className = "slls-dle-btn slls-dle-btn-primary";
    openBtn.textContent = "Open";
    selectToolbar.appendChild(openBtn);

    const newModelBtn = document.createElement("button");
    newModelBtn.className = "slls-dle-btn slls-dle-btn-icon";
    newModelBtn.type = "button";
    newModelBtn.innerHTML = PLUS_SVG;
    newModelBtn.title = "Create new Direct Lake model";
    newModelBtn.setAttribute("aria-label", "Create new Direct Lake model");
    selectToolbar.appendChild(newModelBtn);

    function renderWorkspaces() {
        const items = model.get("workspaces") || [];
        const current = model.get("workspace_id") || "";
        wsSelect.innerHTML = "";
        if (items.length === 0) {
            const o = document.createElement("option");
            o.value = ""; o.textContent = "No workspaces"; o.disabled = true; o.selected = true;
            wsSelect.appendChild(o);
        }
        for (const ws of items) {
            const o = document.createElement("option");
            o.value = ws.id;
            o.textContent = ws.name;
            if (ws.id === current) o.selected = true;
            wsSelect.appendChild(o);
        }
    }
    function renderDatasets() {
        const items = model.get("datasets") || [];
        const current = model.get("dataset_id") || "";
        dsSelect.innerHTML = "";
        if (items.length === 0) {
            const o = document.createElement("option");
            o.value = ""; o.textContent = "No datasets in workspace"; o.disabled = true; o.selected = true;
            dsSelect.appendChild(o);
            openBtn.disabled = true;
            return;
        }
        openBtn.disabled = false;
        for (const ds of items) {
            const o = document.createElement("option");
            o.value = ds.id;
            o.textContent = ds.name;
            if (ds.id === current) o.selected = true;
            dsSelect.appendChild(o);
        }
    }
    wsSelect.addEventListener("change", () => {
        model.set("workspace_id", wsSelect.value);
        model.set("dataset_id", "");
        model.save_changes();
        runAction("list_datasets", { workspace_id: wsSelect.value });
    });
    dsSelect.addEventListener("change", () => {
        model.set("dataset_id", dsSelect.value);
        model.save_changes();
    });
    openBtn.addEventListener("click", () => {
        if (!dsSelect.value) return;
        runAction("open_model", {
            workspace_id: wsSelect.value,
            dataset_id: dsSelect.value,
        });
    });
    model.on("change:workspaces", renderWorkspaces);
    model.on("change:datasets", renderDatasets);

    // ---- Create new model form (in select screen) ----
    const createSection = document.createElement("div");
    createSection.className = "slls-dle-section";
    createSection.style.display = "none";
    selectScreen.appendChild(createSection);

    const createHeading = document.createElement("h3");
    createHeading.textContent = "Create new Direct Lake semantic model";
    createSection.appendChild(createHeading);

    const createGrid = document.createElement("div");
    createGrid.className = "slls-dle-grid";
    createSection.appendChild(createGrid);

    function makeField(labelText, inputEl, opts) {
        const wrap = document.createElement("div");
        wrap.className = "slls-dle-field";
        if (opts && opts.wide) {
            wrap.classList.add("slls-dle-field-wide");
        }
        const lab = document.createElement("label");
        lab.textContent = labelText;
        wrap.appendChild(lab);
        wrap.appendChild(inputEl);
        return wrap;
    }

    // Builds a reusable "tables picker" widget: a search input plus a
    // scrollable, grouped checkbox list of {schema, table} entries. Items
    // are populated asynchronously via setItems(); callers manage when to
    // request the underlying data from the backend (list_source_tables).
    function makeTablesPicker(opts) {
        const iconType = (opts && opts.iconType) || "table";
        const pkSupport = !!(opts && opts.pkSupport);
        const container = document.createElement("div");
        container.className = "slls-dle-tables-picker";

        const headerRow = document.createElement("div");
        headerRow.style.cssText = "display:flex;gap:6px;margin-bottom:6px;flex-wrap:wrap";
        container.appendChild(headerRow);

        const search = document.createElement("input");
        search.type = "text";
        search.className = "slls-dle-input";
        search.placeholder = "Filter tables…";
        search.style.cssText = "flex:1;min-width:160px";
        headerRow.appendChild(search);

        const selectAllBtn = document.createElement("button");
        selectAllBtn.type = "button";
        selectAllBtn.className = "slls-dle-btn";
        selectAllBtn.textContent = "Select all";
        headerRow.appendChild(selectAllBtn);

        const clearBtn = document.createElement("button");
        clearBtn.type = "button";
        clearBtn.className = "slls-dle-btn";
        clearBtn.textContent = "Clear";
        headerRow.appendChild(clearBtn);

        const listBox = document.createElement("div");
        listBox.className = "slls-dle-tablerows";
        container.appendChild(listBox);

        const countLabel = document.createElement("div");
        countLabel.className = "slls-dle-item-meta";
        countLabel.style.marginTop = "4px";
        container.appendChild(countLabel);

        let _items = [];
        let _selected = new Set();
        let _excluded = new Set();
        let _state = "empty";
        let _msg = "Pick a source to load available tables.";
        let _onChange = null;
        // Spec of the row currently marked as primary key (pkSupport only).
        // Empty string means "no primary key selected".
        let _primaryKey = "";
        // The pk auto-detected from the source (kept for the "detected"
        // visual hint even if the user clears or changes the selection).
        let _detectedPk = "";

        function specOf(it) {
            return it.schema ? `${it.schema}.${it.table}` : it.table;
        }
        function notifyChange() {
            if (typeof _onChange === "function") {
                try { _onChange(Array.from(_selected)); } catch (_) {}
            }
        }
        function updateCount() {
            if (_state !== "loaded") {
                countLabel.textContent = "";
                return;
            }
            const noun = iconType === "column" ? "columns" : "tables";
            countLabel.textContent =
                `${_selected.size}/${_items.length} ${noun} selected`;
        }
        function render() {
            listBox.innerHTML = "";
            if (_state !== "loaded") {
                const e = document.createElement("div");
                e.className = "slls-dle-empty";
                e.style.padding = "12px";
                e.textContent = _msg;
                listBox.appendChild(e);
                updateCount();
                return;
            }
            const q = (search.value || "").toLowerCase().trim();
            const groups = new Map();
            for (const it of _items) {
                const spec = specOf(it);
                if (q && !spec.toLowerCase().includes(q)) continue;
                const sch = it.schema || "";
                if (!groups.has(sch)) groups.set(sch, []);
                groups.get(sch).push(it);
            }
            if (groups.size === 0) {
                const e = document.createElement("div");
                e.className = "slls-dle-empty";
                e.style.padding = "12px";
                e.textContent = _items.length === 0
                    ? "No tables found in this source."
                    : "No tables match the filter.";
                listBox.appendChild(e);
                updateCount();
                return;
            }
            for (const [sch, items] of groups) {
                if (sch) {
                    const hdr = document.createElement("div");
                    hdr.style.cssText = "padding:6px 12px;font-weight:600;font-size:12px;letter-spacing:0.3px;text-transform:uppercase;color:var(--slls-text-secondary);background:var(--slls-surface-2);border-bottom:1px solid var(--slls-border)";
                    hdr.textContent = sch;
                    listBox.appendChild(hdr);
                }
                for (const it of items) {
                    const spec = specOf(it);
                    const row = document.createElement("div");
                    row.className = "slls-dle-tablerow";
                    const cb = document.createElement("input");
                    cb.type = "checkbox";
                    cb.checked = _selected.has(spec);
                    const isExcluded = _excluded.has(spec);
                    if (isExcluded) {
                        cb.checked = true;
                        cb.disabled = true;
                    }
                    cb.addEventListener("change", () => {
                        if (cb.checked) _selected.add(spec);
                        else {
                            _selected.delete(spec);
                            // Unchecking the PK column clears the PK.
                            if (pkSupport && _primaryKey === spec) {
                                _primaryKey = "";
                                render();
                            }
                        }
                        updateCount();
                        notifyChange();
                    });
                    const lab = document.createElement("label");
                    lab.style.cssText = "display:flex;align-items:center;gap:8px;flex:1;min-width:0";
                    const iconHolder = document.createElement("span");
                    iconHolder.innerHTML = iconHtml(iconType);
                    lab.appendChild(iconHolder.firstChild);
                    const nameSpan = document.createElement("span");
                    nameSpan.textContent = isExcluded
                        ? `${it.table} (already in model)`
                        : it.table;
                    nameSpan.style.cssText = "flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap";
                    if (isExcluded) nameSpan.style.color = "var(--slls-text-tertiary)";
                    lab.appendChild(nameSpan);
                    if (it.meta) {
                        const metaSpan = document.createElement("span");
                        metaSpan.textContent = it.meta;
                        metaSpan.style.cssText = "font-size:11px;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;color:var(--slls-text-secondary);background:var(--slls-surface-2);border:1px solid var(--slls-border);border-radius:4px;padding:1px 6px;flex-shrink:0";
                        lab.appendChild(metaSpan);
                    }
                    lab.addEventListener("click", (ev) => {
                        if (cb.disabled) return;
                        // Let clicks on the PK toggle propagate to its own handler.
                        if (ev.target && ev.target.closest(".slls-dle-pk-toggle")) return;
                        ev.preventDefault();
                        cb.checked = !cb.checked;
                        cb.dispatchEvent(new Event("change"));
                    });
                    row.appendChild(cb);
                    row.appendChild(lab);
                    if (pkSupport) {
                        const pkBtn = document.createElement("button");
                        pkBtn.type = "button";
                        pkBtn.className = "slls-dle-pk-toggle";
                        pkBtn.textContent = "PK";
                        if (_primaryKey === spec) pkBtn.classList.add("active");
                        if (_detectedPk === spec) pkBtn.classList.add("detected");
                        if (isExcluded) {
                            pkBtn.classList.add("disabled");
                            pkBtn.disabled = true;
                        }
                        pkBtn.setAttribute(
                            "aria-pressed",
                            _primaryKey === spec ? "true" : "false",
                        );
                        pkBtn.title = isExcluded
                            ? "This column is already in the model."
                            : (_detectedPk === spec
                                ? "Detected primary key from source. "
                                  + "Click to mark/unmark as the table's primary key."
                                : "Mark this column as the table's primary key.");
                        pkBtn.addEventListener("click", (ev) => {
                            ev.preventDefault();
                            ev.stopPropagation();
                            if (isExcluded) return;
                            if (_primaryKey === spec) {
                                _primaryKey = "";
                            } else {
                                _primaryKey = spec;
                                // Auto-select the column when made PK.
                                if (!_selected.has(spec)) {
                                    _selected.add(spec);
                                }
                            }
                            render();
                            updateCount();
                            notifyChange();
                        });
                        row.appendChild(pkBtn);
                    }
                    listBox.appendChild(row);
                }
            }
            updateCount();
        }

        search.addEventListener("input", render);
        selectAllBtn.addEventListener("click", () => {
            const q = (search.value || "").toLowerCase().trim();
            for (const it of _items) {
                const spec = specOf(it);
                if (_excluded.has(spec)) continue;
                if (!q || spec.toLowerCase().includes(q)) _selected.add(spec);
            }
            render();
            notifyChange();
        });
        clearBtn.addEventListener("click", () => {
            _selected.clear();
            if (pkSupport) _primaryKey = "";
            render();
            notifyChange();
        });

        return {
            container,
            getSelected: () => Array.from(_selected),
            getTotal: () => _items.length,
            getPrimaryKey: () => _primaryKey || "",
            setPrimaryKey(spec) {
                if (!pkSupport) return;
                _primaryKey = spec || "";
                if (_primaryKey && !_selected.has(_primaryKey)) {
                    _selected.add(_primaryKey);
                }
                render();
                notifyChange();
            },
            setItems(items, opts) {
                _items = items || [];
                _state = "loaded";
                if (opts && opts.preserveSelection !== true) _selected.clear();
                _excluded = new Set((opts && opts.excludeSpecs) || []);
                if (opts && opts.selectAll) {
                    for (const it of _items) {
                        const spec = specOf(it);
                        if (!_excluded.has(spec)) _selected.add(spec);
                    }
                }
                if (pkSupport) {
                    // Auto-pick a primary key from items flagged by the
                    // backend (e.g. via INFORMATION_SCHEMA / get_primary_key).
                    _detectedPk = "";
                    let pkSpec = "";
                    for (const it of _items) {
                        if (it && it.isPrimaryKey) {
                            const spec = specOf(it);
                            if (_excluded.has(spec)) continue;
                            pkSpec = spec;
                            _detectedPk = spec;
                            break;
                        }
                    }
                    if (!(opts && opts.preservePrimaryKey === true)) {
                        _primaryKey = pkSpec;
                        if (_primaryKey && !_selected.has(_primaryKey)) {
                            _selected.add(_primaryKey);
                        }
                    }
                }
                render();
                notifyChange();
            },
            setLoading(msg) {
                _state = "loading";
                _msg = msg || "Loading tables…";
                _selected.clear();
                if (pkSupport) { _primaryKey = ""; _detectedPk = ""; }
                render();
                notifyChange();
            },
            setEmpty(msg) {
                _state = "empty";
                _msg = msg || "Pick a source to load available tables.";
                _items = [];
                _selected.clear();
                if (pkSupport) { _primaryKey = ""; _detectedPk = ""; }
                render();
                notifyChange();
            },
            onChange(cb) { _onChange = cb; },
        };
    }
    // Toggles the orange pending dot inside a field's <label> based on
    // whether the current value differs from the baseline value.
    function setFieldDirty(fieldWrap, dirty) {
        if (!fieldWrap) return;
        const lab = fieldWrap.querySelector("label");
        if (!lab) return;
        let dot = lab.querySelector(".slls-dle-pending-dot");
        if (dirty && !dot) {
            dot = document.createElement("span");
            dot.className = "slls-dle-pending-dot";
            dot.setAttribute("aria-label", "Unsaved changes");
            dot.title = "Unsaved changes";
            dot.style.marginRight = "4px";
            lab.prepend(dot);
        } else if (!dirty && dot) {
            dot.remove();
        }
    }
    // Toggles the orange pending dot inside a makeToggle() label's text span.
    function setToggleDirty(toggleWrap, dirty) {
        if (!toggleWrap) return;
        const txt = toggleWrap.querySelector("span");
        if (!txt) return;
        let dot = txt.querySelector(".slls-dle-pending-dot");
        if (dirty && !dot) {
            dot = document.createElement("span");
            dot.className = "slls-dle-pending-dot";
            dot.setAttribute("aria-label", "Unsaved changes");
            dot.title = "Unsaved changes";
            dot.style.marginRight = "4px";
            txt.prepend(dot);
        } else if (!dirty && dot) {
            dot.remove();
        }
    }

    const newNameInput = document.createElement("input");
    newNameInput.type = "text";
    newNameInput.className = "slls-dle-input";
    newNameInput.placeholder = "My Direct Lake Model";
    createGrid.appendChild(makeField("Model name", newNameInput));

    const newWsSelect = document.createElement("select");
    newWsSelect.className = "slls-dle-select";
    createGrid.appendChild(makeField("Target workspace", newWsSelect));

    const newSrcTypeSelect = document.createElement("select");
    newSrcTypeSelect.className = "slls-dle-select";
    for (const t of (model.get("source_types") || [])) {
        const o = document.createElement("option"); o.value = t; o.textContent = t;
        newSrcTypeSelect.appendChild(o);
    }
    createGrid.appendChild(makeField("Source type", newSrcTypeSelect));

    const newSrcWsSelect = document.createElement("select");
    newSrcWsSelect.className = "slls-dle-select";
    createGrid.appendChild(makeField("Source workspace", newSrcWsSelect, { wide: true }));

    const newSrcItemSelect = document.createElement("select");
    newSrcItemSelect.className = "slls-dle-select";
    createGrid.appendChild(makeField("Source item", newSrcItemSelect));

    const newTablesPicker = makeTablesPicker();
    createGrid.appendChild(
        makeField("Tables", newTablesPicker.container, { wide: true })
    );

    const toggleRow = document.createElement("div");
    toggleRow.style.cssText = "margin-top:10px;display:flex;gap:18px;flex-wrap:wrap";
    createSection.appendChild(toggleRow);

    function makeToggle(labelText, defaultChecked) {
        const wrap = document.createElement("label");
        wrap.className = "slls-dle-checkbox";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        cb.checked = !!defaultChecked;
        wrap.appendChild(cb);
        const txt = document.createElement("span");
        txt.textContent = labelText;
        wrap.appendChild(txt);
        wrap._input = cb;
        return wrap;
    }

    const useSqlToggle = makeToggle("Use SQL endpoint", false);
    const refreshToggle = makeToggle("Refresh after create", true);
    toggleRow.appendChild(useSqlToggle);
    toggleRow.appendChild(refreshToggle);

    const createFooter = document.createElement("div");
    createFooter.style.cssText = "display:flex;gap:8px;margin-top:14px;justify-content:flex-end";
    createSection.appendChild(createFooter);

    const cancelCreateBtn = document.createElement("button");
    cancelCreateBtn.className = "slls-dle-btn";
    cancelCreateBtn.textContent = "Cancel";
    cancelCreateBtn.addEventListener("click", () => {
        createSection.style.display = "none";
        columnsSection.style.display = "none";
        selectSection.style.display = "";
    });
    createFooter.appendChild(cancelCreateBtn);

    const nextBtn = document.createElement("button");
    nextBtn.className = "slls-dle-btn slls-dle-btn-primary";
    nextBtn.textContent = "Next";
    nextBtn.disabled = true;
    nextBtn.addEventListener("click", () => {
        const name = (newNameInput.value || "").trim();
        const tables = newTablesPicker.getSelected();
        if (!name) { setStatus("Please provide a model name.", "error"); return; }
        if (!newSrcItemSelect.value) { setStatus("Please select a source item.", "error"); return; }
        if (tables.length === 0) { setStatus("Please select at least one table.", "error"); return; }
        openColumnsScreen(tables);
    });
    createFooter.appendChild(nextBtn);

    newTablesPicker.onChange((selected) => {
        nextBtn.disabled = (selected || []).length === 0;
    });

    // ---- Columns screen (shown after Next) ----
    const columnsSection = document.createElement("div");
    columnsSection.className = "slls-dle-section";
    columnsSection.style.display = "none";
    selectScreen.appendChild(columnsSection);

    const columnsHeading = document.createElement("h3");
    columnsHeading.textContent = "Choose columns to include";
    columnsSection.appendChild(columnsHeading);

    const columnsHint = document.createElement("div");
    columnsHint.className = "slls-dle-item-meta";
    columnsHint.style.marginBottom = "10px";
    columnsHint.textContent =
        "All columns are selected by default. Uncheck any columns you want to "
        + "exclude. Optionally pick one column per table as its primary key "
        + "(detected primary keys from the source are pre-selected).";
    columnsSection.appendChild(columnsHint);

    const columnsBody = document.createElement("div");
    columnsSection.appendChild(columnsBody);

    const columnsFooter = document.createElement("div");
    columnsFooter.style.cssText = "display:flex;gap:8px;margin-top:14px;align-items:center";
    columnsSection.appendChild(columnsFooter);

    const previewRelsBtn = document.createElement("button");
    previewRelsBtn.className = "slls-dle-btn slls-dle-btn-icon";
    previewRelsBtn.type = "button";
    previewRelsBtn.innerHTML = ICON_SVG.link;
    previewRelsBtn.title = "Preview relationships";
    previewRelsBtn.setAttribute("aria-label", "Preview relationships");
    previewRelsBtn.addEventListener("click", () => openRelationshipsModal());
    columnsFooter.appendChild(previewRelsBtn);

    // Pushes the Back/Create buttons to the right side of the footer.
    const columnsFooterSpacer = document.createElement("div");
    columnsFooterSpacer.style.cssText = "flex:1";
    columnsFooter.appendChild(columnsFooterSpacer);

    const backColumnsBtn = document.createElement("button");
    backColumnsBtn.className = "slls-dle-btn";
    backColumnsBtn.textContent = "Back";
    backColumnsBtn.addEventListener("click", () => {
        columnsSection.style.display = "none";
        createSection.style.display = "";
    });
    columnsFooter.appendChild(backColumnsBtn);

    const submitCreateBtn = document.createElement("button");
    submitCreateBtn.className = "slls-dle-btn slls-dle-btn-primary";
    submitCreateBtn.textContent = "Create";
    columnsFooter.appendChild(submitCreateBtn);

    // Map of tableSpec -> { picker, key }. Recreated each time we open
    // the columns screen so we always reflect the latest table picks.
    let columnsPickers = {};
    // Map of tableSpec -> input element for the user-facing table name.
    let tableNameInputs = {};
    // Relationships staged from the Preview relationships modal.
    // Each entry: { fromSpec, fromCol, toSpec, toCol }. Both sides are
    // identified by the source table spec so the backend can resolve them
    // against the user-facing display names at submit time.
    let relationshipsList = [];
    // Tracks whether the user has manually edited the list; if not we
    // re-run auto-detection every time the modal opens so PK / column
    // changes flow through.
    let relationshipsTouched = false;

    function columnKey(tableSpec) {
        const wsId = newSrcWsSelect.value;
        const srcType = newSrcTypeSelect.value;
        const srcId = newSrcItemSelect.value;
        if (!wsId || !srcType || !srcId) return "";
        // tableSpec is "schema.table" or just "table"
        let schema = "";
        let table = tableSpec;
        const idx = tableSpec.indexOf(".");
        if (idx >= 0) {
            schema = tableSpec.slice(0, idx);
            table = tableSpec.slice(idx + 1);
        }
        return `${wsId}::${srcType}::${srcId}::${schema}::${table}`;
    }

    function applyColumnsFor(tableSpec) {
        const entry = columnsPickers[tableSpec];
        if (!entry) return;
        const key = entry.key;
        const map = model.get("source_columns") || {};
        const v = map[key];
        if (!v) {
            entry.picker.setLoading("Loading columns…");
            return;
        }
        if (v.error) {
            entry.picker.setEmpty(`Could not list columns: ${v.error}`);
            return;
        }
        const items = (v.items || []).map(
            (c) => ({
                schema: "",
                table: c.name,
                // Show the Power BI / TOM data type (e.g. "Int64") in the
                // picker rather than the raw source type (e.g. "bigint").
                meta: convertColumnDataType(c.dataType) || "",
                isPrimaryKey: !!c.isPrimaryKey,
            })
        );
        entry.picker.setItems(items, { selectAll: true });
    }

    function requestColumnsFor(tableSpec) {
        const entry = columnsPickers[tableSpec];
        if (!entry) return;
        const key = entry.key;
        const map = model.get("source_columns") || {};
        if (map[key]) { applyColumnsFor(tableSpec); return; }
        entry.picker.setLoading("Loading columns…");
        let schema = "";
        let table = tableSpec;
        const idx = tableSpec.indexOf(".");
        if (idx >= 0) {
            schema = tableSpec.slice(0, idx);
            table = tableSpec.slice(idx + 1);
        }
        runAction("list_source_columns", {
            workspace_id: newSrcWsSelect.value,
            source_type: newSrcTypeSelect.value,
            source_id: newSrcItemSelect.value,
            schema: schema,
            table: table,
            use_sql_endpoint: useSqlToggle._input.checked,
        });
    }

    model.on("change:source_columns", () => {
        if (columnsSection.style.display === "none") return;
        for (const spec of Object.keys(columnsPickers)) {
            applyColumnsFor(spec);
        }
    });

    function openColumnsScreen(tables) {
        createSection.style.display = "none";
        columnsSection.style.display = "";
        columnsBody.innerHTML = "";
        columnsPickers = {};
        tableNameInputs = {};
        // Reset relationships state for a fresh create flow.
        relationshipsList = [];
        relationshipsTouched = false;
        const FIELD_LABEL_STYLE = "font-size:11px;text-transform:uppercase;letter-spacing:0.3px;color:var(--slls-text-secondary)";
        for (const spec of tables) {
            const wrap = document.createElement("div");
            wrap.className = "slls-dle-section";
            wrap.style.cssText = "margin-bottom:12px;padding:12px;border:1px solid var(--slls-border);border-radius:var(--slls-radius-sm)";

            // Header: collapse toggle + source label (read-only) +
            // editable display name. Clicking the chevron (or the source
            // label) collapses/expands just this table's column picker.
            const hdrRow = document.createElement("div");
            hdrRow.style.cssText = "display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:10px";
            wrap.appendChild(hdrRow);

            const collapseBtn = document.createElement("button");
            collapseBtn.type = "button";
            collapseBtn.className = "slls-dle-collapse-btn";
            collapseBtn.setAttribute("aria-expanded", "true");
            collapseBtn.title = "Collapse / expand columns";
            collapseBtn.setAttribute("aria-label", "Collapse / expand columns");
            collapseBtn.textContent = "\u25BE"; // ▾
            hdrRow.appendChild(collapseBtn);

            const srcWrap = document.createElement("div");
            srcWrap.style.cssText = "display:flex;flex-direction:column;min-width:0";
            const srcLab = document.createElement("div");
            srcLab.textContent = "Source table";
            srcLab.style.cssText = FIELD_LABEL_STYLE;
            const srcVal = document.createElement("div");
            srcVal.style.cssText = "font-weight:600;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;display:flex;align-items:center;gap:6px";
            srcVal.innerHTML = `${iconHtml("table")}<span>${escapeHtml(spec)}</span>`;
            srcWrap.appendChild(srcLab);
            srcWrap.appendChild(srcVal);
            hdrRow.appendChild(srcWrap);

            const arrow = document.createElement("div");
            arrow.textContent = "→";
            arrow.style.cssText = "color:var(--slls-text-tertiary);font-size:18px";
            hdrRow.appendChild(arrow);

            const nameWrap = document.createElement("div");
            nameWrap.style.cssText = "display:flex;flex-direction:column;flex:1;min-width:180px";
            const nameLab = document.createElement("div");
            nameLab.textContent = "Table name in model";
            nameLab.style.cssText = FIELD_LABEL_STYLE;
            const nameInput = document.createElement("input");
            nameInput.type = "text";
            nameInput.className = "slls-dle-input";
            // Default display name is the entity (part after the schema dot).
            const dot = spec.indexOf(".");
            nameInput.value = dot >= 0 ? spec.slice(dot + 1) : spec;
            nameInput.placeholder = "Table name";
            nameWrap.appendChild(nameLab);
            nameWrap.appendChild(nameInput);
            hdrRow.appendChild(nameWrap);
            tableNameInputs[spec] = nameInput;

            const picker = makeTablesPicker({ iconType: "column", pkSupport: true });

            // Summary shown next to the source label whenever the section
            // is collapsed so the user can see selection state without
            // expanding every table.
            const summary = document.createElement("div");
            summary.className = "slls-dle-item-meta";
            summary.style.cssText = "margin-left:8px;display:none;font-size:11.5px;line-height:1.4";

            const body = document.createElement("div");
            body.appendChild(picker.container);
            wrap.appendChild(body);

            const updateSummary = () => {
                const selCount = picker.getSelected().length;
                const total = picker.getTotal();
                const pkSpec = picker.getPrimaryKey();
                let pkName = "";
                if (pkSpec) {
                    const d = pkSpec.indexOf(".");
                    pkName = d >= 0 ? pkSpec.slice(d + 1) : pkSpec;
                }
                summary.innerHTML = "";
                const line1 = document.createElement("div");
                line1.textContent = `${selCount}/${total} columns selected`;
                summary.appendChild(line1);
                if (pkName) {
                    const line2 = document.createElement("div");
                    line2.textContent = `PK: ${pkName}`;
                    summary.appendChild(line2);
                }
            };
            picker.onChange(updateSummary);
            updateSummary();
            srcWrap.appendChild(summary);

            const setCollapsed = (collapsed) => {
                body.style.display = collapsed ? "none" : "";
                summary.style.display = collapsed ? "" : "none";
                if (collapsed) updateSummary();
                collapseBtn.setAttribute(
                    "aria-expanded",
                    collapsed ? "false" : "true",
                );
            };
            collapseBtn.addEventListener("click", (ev) => {
                ev.preventDefault();
                ev.stopPropagation();
                setCollapsed(body.style.display !== "none" ? true : false);
            });
            // Clicking the source label also toggles collapse — gives the
            // user a bigger hit target without interfering with the
            // editable display-name input.
            srcVal.style.cursor = "pointer";
            srcVal.addEventListener("click", () => {
                setCollapsed(body.style.display !== "none");
            });

            columnsBody.appendChild(wrap);
            columnsPickers[spec] = { picker, key: columnKey(spec), updateSummary };
        }
        // Dispatch a single batched request for all uncached tables.
        // Calling runAction per table in the same JS tick overwrites
        // pending_action and only the last request reaches Python — which
        // leaves the rest of the pickers stuck on "Loading columns…".
        const cacheMap = model.get("source_columns") || {};
        const missing = [];
        for (const spec of tables) {
            const entry = columnsPickers[spec];
            if (!entry) continue;
            if (cacheMap[entry.key]) {
                applyColumnsFor(spec);
            } else {
                entry.picker.setLoading("Loading columns…");
                let schema = "";
                let table = spec;
                const idx = spec.indexOf(".");
                if (idx >= 0) {
                    schema = spec.slice(0, idx);
                    table = spec.slice(idx + 1);
                }
                missing.push({ schema, table });
            }
        }
        if (missing.length > 0) {
            runAction("list_source_columns_batch", {
                workspace_id: newSrcWsSelect.value,
                source_type: newSrcTypeSelect.value,
                source_id: newSrcItemSelect.value,
                use_sql_endpoint: useSqlToggle._input.checked,
                tables: missing,
            });
        }
    }

    // ---- Relationships preview / editor ----
    // Builds a per-spec info bundle used by both auto-detection and the
    // modal's dropdowns. Reads the latest picker state so any in-flight
    // PK / column changes are reflected.
    function buildRelTableInfo() {
        const info = {};
        const cacheMap = model.get("source_columns") || {};
        for (const spec of Object.keys(columnsPickers)) {
            const entry = columnsPickers[spec];
            const dn = ((tableNameInputs[spec] && tableNameInputs[spec].value) || "")
                .trim() || spec;
            const cached = cacheMap[entry.key];
            const colsMap = {};
            for (const c of (cached && cached.items) || []) {
                // Compare relationships using the converted Power BI type
                // so e.g. "bigint" and "integer" both normalize to "Int64".
                colsMap[c.name] = (
                    convertColumnDataType(c.dataType) || ""
                ).toLowerCase().trim();
            }
            info[spec] = {
                displayName: dn,
                selectedCols: entry.picker.getSelected(),
                pk: entry.picker.getPrimaryKey() || "",
                colsMap: colsMap,
            };
        }
        return info;
    }

    // Auto-detects many-to-one relationships: every PK becomes a candidate
    // "to" column; every other selected column with the same name and
    // matching data type becomes a candidate "from" (Many) column.
    function autoDetectRelationships(info) {
        const rels = [];
        const specs = Object.keys(info);
        const seenKey = new Set();
        for (const toSpec of specs) {
            const pk = info[toSpec].pk;
            if (!pk) continue;
            const toDt = info[toSpec].colsMap[pk] || "";
            if (!toDt) continue;
            for (const fromSpec of specs) {
                if (fromSpec === toSpec) continue;
                const fromPk = info[fromSpec].pk;
                for (const c of info[fromSpec].selectedCols) {
                    if (c === fromPk) continue;
                    if (c.toLowerCase() !== pk.toLowerCase()) continue;
                    const fromDt = info[fromSpec].colsMap[c] || "";
                    if (!fromDt || fromDt !== toDt) continue;
                    const k = `${fromSpec}|${c}|${toSpec}|${pk}`;
                    if (seenKey.has(k)) continue;
                    seenKey.add(k);
                    rels.push({
                        fromSpec: fromSpec,
                        fromCol: c,
                        fromCardinality: "Many",
                        toSpec: toSpec,
                        toCol: pk,
                        toCardinality: "One",
                    });
                }
            }
        }
        return rels;
    }

    function openRelationshipsModal() {
        // Recompute auto-suggested rels every time unless the user has
        // already made manual edits in this create flow.
        const info = buildRelTableInfo();
        if (!relationshipsTouched) {
            relationshipsList = autoDetectRelationships(info);
        }
        const specs = Object.keys(info);

        modalHeader("Relationships");
        // Position this modal near the bottom of the widget so it appears
        // close to the "Preview relationships" button that opened it,
        // rather than centered vertically. Also widen it because each row
        // contains four table/column dropdowns that need room to breathe.
        modal.classList.add("slls-dle-modal-bottom");
        modal.classList.add("slls-dle-modal-xwide");
        registerModalCleanup(() => {
            modal.classList.remove("slls-dle-modal-bottom");
            modal.classList.remove("slls-dle-modal-xwide");
        });

        const hint = document.createElement("div");
        hint.className = "slls-dle-item-meta";
        hint.style.cssText = "margin-bottom:12px;line-height:1.4";
        hint.textContent =
            "Many-to-one relationships are auto-detected from primary keys "
            + "and matching column names with matching data types. Use the "
            + "controls below to add, modify, or remove relationships before "
            + "creating the model. The 'Many' side is the From table; the "
            + "'One' side is the To table.";
        modal.appendChild(hint);

        const toolbar = document.createElement("div");
        toolbar.style.cssText = "display:flex;gap:8px;margin-bottom:10px;align-items:center";
        modal.appendChild(toolbar);

        const detectBtn = makeBtn("Re-detect from primary keys", "slls-dle-btn");
        detectBtn.addEventListener("click", () => {
            relationshipsList = autoDetectRelationships(buildRelTableInfo());
            relationshipsTouched = false;
            renderList();
        });
        toolbar.appendChild(detectBtn);

        const addBtn = document.createElement("button");
        addBtn.className = "slls-dle-btn slls-dle-btn-icon";
        addBtn.type = "button";
        addBtn.innerHTML = PLUS_SVG;
        addBtn.title = "Add relationship";
        addBtn.setAttribute("aria-label", "Add relationship");
        addBtn.addEventListener("click", () => {
            // Add a blank row so the user makes all selections explicitly.
            relationshipsList.push({
                fromSpec: "",
                fromCol: "",
                fromCardinality: "Many",
                toSpec: "",
                toCol: "",
                toCardinality: "One",
            });
            relationshipsTouched = true;
            renderList();
        });
        toolbar.appendChild(addBtn);

        const listWrap = document.createElement("div");
        listWrap.style.cssText = "display:flex;flex-direction:column;gap:8px;max-height:380px;overflow-y:auto;padding-right:4px";
        modal.appendChild(listWrap);

        function makeSelect(value, options, onChange) {
            const s = document.createElement("select");
            s.className = "slls-dle-select";
            s.style.cssText = "min-width:0;flex:1 1 130px;max-width:200px";
            if (options.length === 0 || !options.find((o) => o.value === value)) {
                const placeholder = document.createElement("option");
                placeholder.value = "";
                placeholder.textContent = "(none)";
                placeholder.selected = !value;
                s.appendChild(placeholder);
            }
            for (const opt of options) {
                const o = document.createElement("option");
                o.value = opt.value;
                o.textContent = opt.label;
                if (opt.value === value) o.selected = true;
                s.appendChild(o);
            }
            s.addEventListener("change", () => onChange(s.value));
            return s;
        }

        function tableOptions() {
            return specs.map((s) => ({
                value: s,
                label: info[s].displayName,
            }));
        }

        function columnOptions(spec) {
            if (!spec || !info[spec]) return [];
            const pk = info[spec].pk;
            return info[spec].selectedCols.map((c) => ({
                value: c,
                label: c === pk ? `${c} (PK)` : c,
            }));
        }

        function dataTypeOf(spec, col) {
            if (!spec || !col || !info[spec]) return "";
            return info[spec].colsMap[col] || "";
        }

        function renderList() {
            listWrap.innerHTML = "";
            if (relationshipsList.length === 0) {
                const e = document.createElement("div");
                e.className = "slls-dle-empty";
                e.style.padding = "16px";
                e.textContent =
                    "No relationships defined. Click the '+' button to "
                    + "create one manually, or 'Re-detect from primary keys' "
                    + "to populate from auto-detection.";
                listWrap.appendChild(e);
                return;
            }
            const cardinalityOptions = [
                { value: "Many", label: "Many" },
                { value: "One", label: "One" },
            ];
            relationshipsList.forEach((rel, idx) => {
                const row = document.createElement("div");
                row.style.cssText = "display:flex;flex-wrap:wrap;align-items:center;gap:6px;padding:10px;border:1px solid var(--slls-border);border-radius:var(--slls-radius-sm);background:var(--slls-surface-2)";

                const fromLab = document.createElement("span");
                fromLab.textContent = "From:";
                fromLab.style.cssText = "font-size:11px;font-weight:600;color:var(--slls-text-secondary);text-transform:uppercase;letter-spacing:0.3px";
                row.appendChild(fromLab);

                const fromTblSel = makeSelect(rel.fromSpec, tableOptions(), (v) => {
                    rel.fromSpec = v;
                    // Reset column when the table changes.
                    rel.fromCol = "";
                    relationshipsTouched = true;
                    renderList();
                });
                row.appendChild(fromTblSel);

                const fromDot = document.createElement("span");
                fromDot.textContent = ".";
                fromDot.style.color = "var(--slls-text-tertiary)";
                row.appendChild(fromDot);

                const fromColSel = makeSelect(
                    rel.fromCol,
                    columnOptions(rel.fromSpec),
                    (v) => {
                        rel.fromCol = v;
                        relationshipsTouched = true;
                        renderList();
                    },
                );
                row.appendChild(fromColSel);

                const fromCardSel = makeSelect(
                    rel.fromCardinality || "Many",
                    cardinalityOptions,
                    (v) => {
                        rel.fromCardinality = v;
                        relationshipsTouched = true;
                        renderList();
                    },
                );
                fromCardSel.style.flex = "0 0 80px";
                fromCardSel.title = "From cardinality";
                row.appendChild(fromCardSel);

                const arrow = document.createElement("span");
                arrow.textContent = "→";
                arrow.style.cssText = "color:var(--slls-text-tertiary);font-size:16px;margin:0 4px";
                row.appendChild(arrow);

                const toLab = document.createElement("span");
                toLab.textContent = "To:";
                toLab.style.cssText = "font-size:11px;font-weight:600;color:var(--slls-text-secondary);text-transform:uppercase;letter-spacing:0.3px";
                row.appendChild(toLab);

                const toTblSel = makeSelect(rel.toSpec, tableOptions(), (v) => {
                    rel.toSpec = v;
                    rel.toCol = "";
                    relationshipsTouched = true;
                    renderList();
                });
                row.appendChild(toTblSel);

                const toDot = document.createElement("span");
                toDot.textContent = ".";
                toDot.style.color = "var(--slls-text-tertiary)";
                row.appendChild(toDot);

                const toColSel = makeSelect(
                    rel.toCol,
                    columnOptions(rel.toSpec),
                    (v) => {
                        rel.toCol = v;
                        relationshipsTouched = true;
                        renderList();
                    },
                );
                row.appendChild(toColSel);

                const toCardSel = makeSelect(
                    rel.toCardinality || "One",
                    cardinalityOptions,
                    (v) => {
                        rel.toCardinality = v;
                        relationshipsTouched = true;
                        renderList();
                    },
                );
                toCardSel.style.flex = "0 0 80px";
                toCardSel.title = "To cardinality";
                row.appendChild(toCardSel);

                const spacer = document.createElement("div");
                spacer.style.cssText = "flex:1";
                row.appendChild(spacer);

                const removeBtn = makeBtn("Remove", "slls-dle-btn slls-dle-btn-danger");
                removeBtn.addEventListener("click", () => {
                    relationshipsList.splice(idx, 1);
                    relationshipsTouched = true;
                    renderList();
                });
                row.appendChild(removeBtn);

                // Validation banner: same table or mismatched data types.
                const issues = [];
                if (!rel.fromSpec || !rel.fromCol
                    || !rel.toSpec || !rel.toCol) {
                    issues.push("Both sides must be set.");
                } else if (rel.fromSpec === rel.toSpec) {
                    issues.push("From and To tables must be different.");
                } else {
                    const fdt = dataTypeOf(rel.fromSpec, rel.fromCol);
                    const tdt = dataTypeOf(rel.toSpec, rel.toCol);
                    if (fdt && tdt && fdt !== tdt) {
                        issues.push(
                            `Data type mismatch: ${fdt} vs ${tdt}. `
                            + "The relationship may fail to apply.",
                        );
                    }
                }
                if (issues.length > 0) {
                    const warn = document.createElement("div");
                    warn.style.cssText = "flex-basis:100%;font-size:11.5px;color:var(--slls-danger,#ff3b30)";
                    warn.textContent = "⚠ " + issues.join(" ");
                    row.appendChild(warn);
                }

                listWrap.appendChild(row);
            });
        }

        renderList();

        const footer = modalFooter();
        addCloseBtn(footer, "Done");
        openModal();
    }

    submitCreateBtn.addEventListener("click", () => {
        const name = (newNameInput.value || "").trim();
        const specs = Object.keys(columnsPickers);
        if (!name || specs.length === 0) {
            setStatus("Missing model name or tables.", "error"); return;
        }
        // Build display-name -> source-spec map and validate names.
        const tablesDict = {};
        const seen = new Set();
        for (const spec of specs) {
            const input = tableNameInputs[spec];
            const dn = ((input && input.value) || "").trim();
            if (!dn) {
                setStatus(
                    `Please provide a table name for '${spec}'.`, "error",
                );
                return;
            }
            const lo = dn.toLowerCase();
            if (seen.has(lo)) {
                setStatus(
                    `Duplicate table name '${dn}'. Each table needs a unique name.`,
                    "error",
                );
                return;
            }
            seen.add(lo);
            tablesDict[dn] = spec;
        }
        // Keep selected_columns keyed by source spec so the backend can map
        // it back to source columns regardless of the renamed table name.
        const selectedColumns = {};
        const primaryKeys = {};
        for (const spec of specs) {
            const cols = columnsPickers[spec].picker.getSelected();
            if (cols.length === 0) {
                setStatus(
                    `Please select at least one column for '${spec}'.`,
                    "error",
                );
                return;
            }
            selectedColumns[spec] = cols;
            const pk = columnsPickers[spec].picker.getPrimaryKey();
            if (pk) primaryKeys[spec] = pk;
        }
        // If the user never opened the relationships modal, auto-detect now
        // so the model gets reasonable defaults out of the box.
        if (!relationshipsTouched && relationshipsList.length === 0) {
            relationshipsList = autoDetectRelationships(buildRelTableInfo());
        }
        // Filter out incomplete / self-referential relationships before
        // sending them to the backend.
        const cleanRels = relationshipsList.filter((r) =>
            r && r.fromSpec && r.fromCol && r.toSpec && r.toCol
            && r.fromSpec !== r.toSpec,
        ).map((r) => ({
            from_table_spec: r.fromSpec,
            from_column: r.fromCol,
            from_cardinality: r.fromCardinality || "Many",
            to_table_spec: r.toSpec,
            to_column: r.toCol,
            to_cardinality: r.toCardinality || "One",
        }));
        runAction("create_model", {
            dataset_name: name,
            workspace_id: newWsSelect.value,
            source_type: newSrcTypeSelect.value,
            source_workspace_id: newSrcWsSelect.value,
            source_id: newSrcItemSelect.value,
            tables: tablesDict,
            selected_columns: selectedColumns,
            primary_keys: primaryKeys,
            relationships: cleanRels,
            use_sql_endpoint: useSqlToggle._input.checked,
            refresh: refreshToggle._input.checked,
        });
    });

    function syncWorkspacesIntoCreate() {
        const items = model.get("workspaces") || [];
        const cur = model.get("workspace_id") || "";
        for (const sel of [newWsSelect, newSrcWsSelect]) {
            const prev = sel.value;
            sel.innerHTML = "";
            for (const ws of items) {
                const o = document.createElement("option");
                o.value = ws.id;
                o.textContent = ws.name;
                sel.appendChild(o);
            }
            if (prev) sel.value = prev;
            else if (cur) sel.value = cur;
        }
    }
    function refreshSourceItems() {
        const map = model.get("source_items") || {};
        const key = `${newSrcWsSelect.value}::${newSrcTypeSelect.value}`;
        const items = map[key] || [];
        newSrcItemSelect.innerHTML = "";
        if (items.length === 0) {
            const o = document.createElement("option");
            o.value = ""; o.textContent = "(none — pick workspace/type)"; o.disabled = true; o.selected = true;
            newSrcItemSelect.appendChild(o);
            return;
        }
        for (const it of items) {
            const o = document.createElement("option");
            o.value = it.id;
            o.textContent = it.name;
            newSrcItemSelect.appendChild(o);
        }
        // Auto-trigger tables load for the (default-selected) first item.
        if (typeof requestCreateTablesIfNeeded === "function") {
            requestCreateTablesIfNeeded();
        }
    }
    function requestSourceItemsIfNeeded() {
        const map = model.get("source_items") || {};
        const key = `${newSrcWsSelect.value}::${newSrcTypeSelect.value}`;
        if (!newSrcWsSelect.value || !newSrcTypeSelect.value) return;
        if (map[key]) { refreshSourceItems(); return; }
        runAction("list_source_items", {
            workspace_id: newSrcWsSelect.value,
            source_type: newSrcTypeSelect.value,
        });
    }
    newSrcTypeSelect.addEventListener("change", () => {
        newTablesPicker.setEmpty("Pick a source item to load available tables.");
        requestSourceItemsIfNeeded();
    });
    newSrcWsSelect.addEventListener("change", () => {
        newTablesPicker.setEmpty("Pick a source item to load available tables.");
        requestSourceItemsIfNeeded();
    });
    model.on("change:source_items", refreshSourceItems);

    function createKey() {
        const wsId = newSrcWsSelect.value;
        const srcType = newSrcTypeSelect.value;
        const srcId = newSrcItemSelect.value;
        if (!wsId || !srcType || !srcId) return "";
        return `${wsId}::${srcType}::${srcId}`;
    }
    function requestCreateTablesIfNeeded() {
        const key = createKey();
        if (!key) {
            newTablesPicker.setEmpty("Pick a source item to load available tables.");
            return;
        }
        const map = model.get("source_tables") || {};
        if (map[key]) {
            const v = map[key];
            if (v && v.error) {
                newTablesPicker.setEmpty(`Could not list tables: ${v.error}`);
            } else {
                newTablesPicker.setItems((v && v.items) || []);
            }
            return;
        }
        newTablesPicker.setLoading("Loading tables\u2026");
        runAction("list_source_tables", {
            workspace_id: newSrcWsSelect.value,
            source_type: newSrcTypeSelect.value,
            source_id: newSrcItemSelect.value,
            use_sql_endpoint: useSqlToggle._input.checked,
        });
    }
    newSrcItemSelect.addEventListener("change", requestCreateTablesIfNeeded);
    useSqlToggle._input.addEventListener("change", () => {
        // Lakehouse uses_sql_endpoint may change the underlying schema/tables
        // expectations; re-request to be safe.
        if (newSrcTypeSelect.value === "Lakehouse") {
            requestCreateTablesIfNeeded();
        }
    });
    model.on("change:source_tables", () => {
        // Only refresh if the create form is currently visible.
        if (createSection.style.display !== "none") requestCreateTablesIfNeeded();
    });

    newModelBtn.addEventListener("click", () => {
        selectSection.style.display = "none";
        columnsSection.style.display = "none";
        createSection.style.display = "";
        syncWorkspacesIntoCreate();
        setStatus("", null);
        requestSourceItemsIfNeeded();
        requestCreateTablesIfNeeded();
    });

    // ============================================================
    // MANAGE SCREEN
    // ============================================================
    const manageScreen = document.createElement("div");
    manageScreen.className = "slls-dle-screen";
    root.appendChild(manageScreen);

    // ----------- Pending changes / save bar -----------
    // Modal "Save" buttons enqueue changes here instead of writing to the
    // model immediately. Changes are only persisted when the user clicks
    // the global Save button below.
    const pendingState = { changes: [], counter: 0 };
    const saveBar = document.createElement("div");
    saveBar.className = "slls-dle-savebar";
    manageScreen.appendChild(saveBar);

    function pendingId() { return `__p_${++pendingState.counter}`; }
    function enqueuePendingChange(change) {
        pendingState.changes.push(change);
        renderSources();
        renderTables();
        renderSaveBar();
    }
    function discardPendingChanges() {
        if (pendingState.changes.length === 0) return;
        pendingState.changes = [];
        renderSources();
        renderTables();
        renderSaveBar();
        setStatus("Pending changes discarded.", "info");
    }
    function applyPendingChanges() {
        if (pendingState.changes.length === 0) return;
        const changes = pendingState.changes.slice();
        // Clear immediately so the UI reflects the in-flight save; if the
        // backend errors, the user will see an error status and can redo.
        pendingState.changes = [];
        renderSources();
        renderTables();
        renderSaveBar();
        runAction("apply_pending_changes", { changes });
    }
    function pendingForSource(expressionName) {
        return pendingState.changes.some(c =>
            (c.kind === "update_source" && c.key === expressionName) ||
            (c.kind === "add_tables" && c.payload && c.payload.expression_name === expressionName)
        );
    }
    function pendingForTable(tableName) {
        return pendingState.changes.some(
            c => (c.kind === "reassign_table" && c.key === tableName) ||
                 (c.kind === "edit_columns" && c.key === tableName) ||
                 (c.kind === "sync_columns" && c.key === tableName) ||
                 (c.kind === "sync_descriptions" && c.key === tableName) ||
                 (c.kind === "rename_table" && c.key === tableName)
        );
    }
    // Returns the most recently staged update_source payload for the given
    // expression name (or null). Used to re-hydrate the Edit Source modal
    // with the staged values instead of the saved values.
    function latestPendingUpdateForSource(expressionName) {
        let payload = null;
        for (const c of pendingState.changes) {
            if (c.kind === "update_source" && c.key === expressionName) {
                payload = c.payload || null;
            }
        }
        return payload;
    }
    // Returns the most recently staged reassign_table payload for the given
    // table name (or null).
    function latestPendingReassignForTable(tableName) {
        let payload = null;
        for (const c of pendingState.changes) {
            if (c.kind === "reassign_table" && c.key === tableName) {
                payload = c.payload || null;
            }
        }
        return payload;
    }
    // Merges all staged edit_columns payloads for the given table into a
    // single { columnName -> { source_column?, data_type?, description?,
    // new_name? } } map. Returns {} when there are no staged column edits.
    function mergedPendingColumnEditsForTable(tableName) {
        const merged = {};
        for (const c of pendingState.changes) {
            if (c.kind === "edit_columns" && c.key === tableName) {
                const cols = (c.payload && c.payload.columns) || [];
                for (const col of cols) {
                    if (!col || !col.name) continue;
                    const cur = merged[col.name] || {};
                    if ("source_column" in col) cur.source_column = col.source_column;
                    if ("data_type" in col) cur.data_type = col.data_type;
                    if ("description" in col) cur.description = col.description;
                    if ("new_name" in col) cur.new_name = col.new_name;
                    merged[col.name] = cur;
                }
            }
        }
        return merged;
    }
    // Returns the most recently staged table description for the given
    // table (or undefined if none was staged).
    function latestPendingTableDescription(tableName) {
        let desc;
        for (const c of pendingState.changes) {
            if (c.kind === "edit_columns"
                && c.key === tableName
                && c.payload
                && "table_description" in c.payload) {
                desc = c.payload.table_description;
            }
        }
        return desc;
    }
    // Applies any staged sync_columns changes for `tableName` to the given
    // baseline column list: removes columns flagged for removal and appends
    // columns flagged for addition (mirroring the backend behavior). The
    // returned list shares the shape of the input entries; added columns
    // have columnType "Data" and an empty data category.
    function applyPendingSyncToColumns(tableName, baseCols) {
        const cols = baseCols.slice();
        const known = new Set(cols.map(c => c.name));
        const removed = new Set();
        for (const c of pendingState.changes) {
            if (c.kind !== "sync_columns" || c.key !== tableName) continue;
            const p = c.payload || {};
            for (const r of (p.remove || [])) removed.add(r);
            for (const add of (p.add || [])) {
                if (!add || !add.name) continue;
                if (known.has(add.name) || removed.has(add.name)) continue;
                cols.push({
                    name: add.name,
                    sourceColumn: add.name,
                    dataType: convertColumnDataType(add.dataType),
                    dataCategory: "",
                    description: "",
                    columnType: "Data",
                    _stagedSync: true,
                });
                known.add(add.name);
            }
        }
        return cols.filter(c => !removed.has(c.name));
    }
    // Returns the most recently staged rename_table payload for the given
    // table name (or null).
    function latestPendingRenameForTable(tableName) {
        let payload = null;
        for (const c of pendingState.changes) {
            if (c.kind === "rename_table" && c.key === tableName) {
                payload = c.payload || null;
            }
        }
        return payload;
    }
    function pendingAddedSources() {
        return pendingState.changes.filter(c => c.kind === "add_source");
    }
    // Compute the expression name a staged add_source will receive on save.
    // Mirrors the backend logic: base = "DatabaseQuery" if SQL endpoint is
    // used, else `DL_<source_type>`; uniqueness suffix `_2`, `_3`, ...
    // against existing saved sources and any prior staged add_source.
    function previewExpressionName(payload, taken) {
        const base = payload && payload.use_sql_endpoint
            ? "DatabaseQuery"
            : `DL_${(payload && payload.source_type) || "Source"}`;
        if (!taken.has(base)) return base;
        let i = 2;
        while (taken.has(`${base}_${i}`)) i += 1;
        return `${base}_${i}`;
    }
    // Returns the staged add_source changes, each annotated with its
    // computed `expressionName` (the name it will receive on save).
    function pendingAddedSourcesWithNames() {
        const saved = model.get("sources") || [];
        const taken = new Set(saved.map(s => s.expressionName));
        const out = [];
        for (const c of pendingState.changes) {
            if (c.kind !== "add_source") continue;
            const p = c.payload || {};
            // Prefer the name stamped onto the payload at stage time; fall
            // back to recomputing if missing (older staged changes).
            let expressionName = p.expression_name;
            if (!expressionName || taken.has(expressionName)) {
                expressionName = previewExpressionName(p, taken);
            }
            taken.add(expressionName);
            out.push({ change: c, expressionName });
        }
        return out;
    }
    function pendingAddedTables() {
        const out = [];
        for (const c of pendingState.changes) {
            if (c.kind === "add_tables") {
                const tables = (c.payload && c.payload.tables) || [];
                for (const entry of tables) {
                    // Each entry may be a plain "schema.table" string (legacy)
                    // or a { spec, name } object when the user supplied an
                    // explicit display name.
                    const isObj = entry && typeof entry === "object";
                    const spec = isObj ? (entry.spec || "") : String(entry || "");
                    const name = isObj ? (entry.name || "") : "";
                    out.push({
                        spec,
                        name,
                        expressionName: c.payload.expression_name,
                        changeId: c.id,
                    });
                }
            }
        }
        return out;
    }
    // Removes all pending changes for which `predicate(change)` returns true.
    // Used to back per-row "Revert" buttons.
    function revertChangesMatching(predicate, message) {
        const before = pendingState.changes.length;
        pendingState.changes = pendingState.changes.filter(c => !predicate(c));
        if (pendingState.changes.length === before) return;
        renderSources();
        renderTables();
        renderSaveBar();
        setStatus(message || "Reverted staged change.", "info");
    }
    // Removes a single table spec from an add_tables change (by change id),
    // dropping the change entirely when its last spec is removed.
    function revertAddedTableSpec(changeId, spec) {
        const next = [];
        for (const c of pendingState.changes) {
            if (c.kind === "add_tables" && c.id === changeId) {
                const tables = ((c.payload && c.payload.tables) || []).filter(entry => {
                    const isObj = entry && typeof entry === "object";
                    const s = isObj ? (entry.spec || "") : String(entry || "");
                    return s !== spec;
                });
                if (tables.length > 0) {
                    next.push({
                        ...c,
                        payload: { ...c.payload, tables },
                    });
                }
            } else {
                next.push(c);
            }
        }
        pendingState.changes = next;
        renderSources();
        renderTables();
        renderSaveBar();
        setStatus("Reverted staged change.", "info");
    }
    function renderSaveBar() {
        saveBar.innerHTML = "";
        const n = pendingState.changes.length;
        if (n === 0) {
            saveBar.classList.remove("show");
            return;
        }
        saveBar.classList.add("show");
        const label = document.createElement("div");
        label.className = "slls-dle-savebar-label";
        const dot = document.createElement("span");
        dot.className = "slls-dle-pending-dot";
        dot.setAttribute("aria-hidden", "true");
        label.appendChild(dot);
        const txt = document.createElement("span");
        txt.textContent = `${n} pending change${n === 1 ? "" : "s"}`;
        label.appendChild(txt);
        saveBar.appendChild(label);
        const viewBtn = document.createElement("button");
        viewBtn.className = "slls-dle-btn";
        viewBtn.textContent = "View summary";
        viewBtn.addEventListener("click", openPendingSummaryModal);
        saveBar.appendChild(viewBtn);
        const discardBtn = document.createElement("button");
        discardBtn.className = "slls-dle-btn";
        discardBtn.textContent = "Discard";
        discardBtn.addEventListener("click", discardPendingChanges);
        saveBar.appendChild(discardBtn);
        const saveBtn = document.createElement("button");
        saveBtn.className = "slls-dle-btn slls-dle-btn-primary";
        saveBtn.textContent = "Save changes";
        saveBtn.addEventListener("click", applyPendingChanges);
        saveBar.appendChild(saveBtn);
    }

    function describePendingChange(c) {
        const p = c.payload || {};
        switch (c.kind) {
            case "add_source": {
                const wsBit = p.source_workspace_id
                    ? ` (workspace ${p.source_workspace_id})`
                    : "";
                const sqlBit = p.use_sql_endpoint ? " · SQL endpoint" : "";
                return {
                    title: `Add source '${p.source_name || "(new source)"}'`,
                    details: [
                        `Type: ${p.source_type || "(unknown)"}${sqlBit}`,
                        `Expression: ${p.expression_name || "(auto)"}${wsBit}`,
                    ],
                };
            }
            case "update_source": {
                const lines = [];
                if (p.source_name) lines.push(`New name: ${p.source_name}`);
                if (p.source_type) lines.push(`New type: ${p.source_type}`);
                if (p.source_workspace_id)
                    lines.push(`New workspace: ${p.source_workspace_id}`);
                if (p.use_sql_endpoint != null)
                    lines.push(
                        `Use SQL endpoint: ${p.use_sql_endpoint ? "yes" : "no"}`,
                    );
                return {
                    title: `Update source '${c.key}'`,
                    details: lines.length ? lines : ["(no field changes)"],
                };
            }
            case "reassign_table": {
                return {
                    title: `Reassign table '${c.key}'`,
                    details: [
                        `New source: ${p.expression_name || "(unchanged)"}`,
                        `New entity: ${p.schema_name ? p.schema_name + "." : ""}${p.entity_name || "(unchanged)"}`,
                    ],
                };
            }
            case "rename_table": {
                return {
                    title: `Rename table '${c.key}' → '${p.new_name || ""}'`,
                    details: [],
                };
            }
            case "add_tables": {
                const tables = p.tables || [];
                const lines = tables.map((entry) => {
                    if (entry && typeof entry === "object") {
                        const spec = entry.spec || "";
                        const name = entry.name || spec;
                        return name === spec ? spec : `${name}  ←  ${spec}`;
                    }
                    return String(entry || "");
                });
                return {
                    title: `Add ${tables.length} table${tables.length === 1 ? "" : "s"} from '${p.expression_name || ""}'`,
                    details: lines.length ? lines : ["(no tables)"],
                };
            }
            case "edit_columns": {
                const cols = p.columns || [];
                const lines = cols.map((col) => {
                    const parts = [];
                    if ("new_name" in col)
                        parts.push(`rename → ${col.new_name}`);
                    if ("source_column" in col)
                        parts.push(`source_column = ${col.source_column}`);
                    if ("data_type" in col)
                        parts.push(`data_type = ${col.data_type}`);
                    if ("description" in col)
                        parts.push(
                            `description = ${col.description ? '"' + col.description + '"' : "(none)"}`,
                        );
                    return `${col.name}: ${parts.join(", ") || "(no changes)"}`;
                });
                if ("table_description" in p) {
                    lines.unshift(
                        `table description = ${p.table_description ? '"' + p.table_description + '"' : "(none)"}`,
                    );
                }
                const colCount = cols.length;
                const titleParts = [];
                if (colCount > 0) {
                    titleParts.push(
                        `Edit ${colCount} column${colCount === 1 ? "" : "s"}`,
                    );
                }
                if ("table_description" in p) {
                    titleParts.push("table description");
                }
                const title = `${titleParts.join(" + ") || "Edit"} in '${c.key}'`;
                return {
                    title,
                    details: lines.length ? lines : ["(no changes)"],
                };
            }
            case "sync_descriptions": {
                return {
                    title: `Sync descriptions on '${c.key}' from source`,
                    details: [
                        p.overwrite
                            ? "Overwrite existing descriptions"
                            : "Fill only empty descriptions",
                    ],
                };
            }
            case "sync_columns": {
                const adds = (p.add || []).map((col) =>
                    col.dataType
                        ? `${col.name} (${col.dataType})`
                        : col.name,
                );
                const removes = p.remove || [];
                const lines = [];
                if (adds.length) lines.push(`Add: ${adds.join(", ")}`);
                if (removes.length)
                    lines.push(`Remove: ${removes.join(", ")}`);
                return {
                    title: `Sync columns on '${c.key}' (+${adds.length}, -${removes.length})`,
                    details: lines.length ? lines : ["(no changes)"],
                };
            }
            default:
                return {
                    title: `${c.kind} on '${c.key || ""}'`,
                    details: [JSON.stringify(p)],
                };
        }
    }

    function openPendingSummaryModal() {
        const n = pendingState.changes.length;
        modalHeader(`Pending changes (${n})`);

        if (n === 0) {
            const empty = document.createElement("div");
            empty.className = "slls-dle-empty";
            empty.textContent = "No pending changes.";
            modal.appendChild(empty);
        } else {
            const sub = document.createElement("div");
            sub.className = "slls-dle-item-meta";
            sub.style.marginBottom = "12px";
            sub.textContent =
                "These changes will be applied to the semantic model "
                + "when you click Save changes.";
            modal.appendChild(sub);

            const list = document.createElement("div");
            list.className = "slls-dle-list";
            list.style.maxHeight = "420px";
            list.style.overflowY = "auto";
            for (let i = 0; i < pendingState.changes.length; i++) {
                const c = pendingState.changes[i];
                const info = describePendingChange(c);
                const row = document.createElement("div");
                row.className = "slls-dle-item";
                row.style.alignItems = "flex-start";
                const main = document.createElement("div");
                main.className = "slls-dle-item-main";
                const nm = document.createElement("div");
                nm.className = "slls-dle-item-name";
                nm.textContent = `${i + 1}. ${info.title}`;
                main.appendChild(nm);
                for (const line of info.details) {
                    const meta = document.createElement("div");
                    meta.className = "slls-dle-item-meta";
                    meta.style.whiteSpace = "normal";
                    meta.textContent = line;
                    main.appendChild(meta);
                }
                row.appendChild(main);
                list.appendChild(row);
            }
            modal.appendChild(list);
        }

        const footer = modalFooter();
        addCloseBtn(footer, "Close");
        if (pendingState.changes.length > 0) {
            footer.appendChild(makeBtn("Save changes", "slls-dle-btn slls-dle-btn-primary", () => {
                closeModal();
                applyPendingChanges();
            }));
        }
        openModal();
    }

    // Sources section
    const sourcesSection = document.createElement("div");
    sourcesSection.className = "slls-dle-section";
    manageScreen.appendChild(sourcesSection);
    const sourcesHeading = document.createElement("h3");
    sourcesSection.appendChild(sourcesHeading);
    const sourcesList = document.createElement("div");
    sourcesList.className = "slls-dle-list";
    sourcesSection.appendChild(sourcesList);
    const addSourceBtn = document.createElement("button");
    addSourceBtn.className = "slls-dle-btn slls-dle-btn-primary";
    addSourceBtn.style.marginTop = "10px";
    addSourceBtn.innerHTML = `${PLUS_SVG} <span style="margin-left:6px;">Add source</span>`;
    addSourceBtn.addEventListener("click", () => openSourceModal(null));
    sourcesSection.appendChild(addSourceBtn);

    // Tables section
    const tablesSection = document.createElement("div");
    tablesSection.className = "slls-dle-section";
    manageScreen.appendChild(tablesSection);
    // Header row: heading on the left, "..." menu (with bulk actions that
    // apply across every Direct Lake table) on the right.
    const tablesHeaderRow = document.createElement("div");
    tablesHeaderRow.style.cssText =
        "display:flex; align-items:center; justify-content:space-between; gap:8px; margin-bottom:12px;";
    const tablesHeading = document.createElement("h3");
    tablesHeading.style.margin = "0";
    tablesHeaderRow.appendChild(tablesHeading);
    const tablesMoreBtn = document.createElement("button");
    tablesMoreBtn.className = "slls-dle-icon-btn";
    tablesMoreBtn.setAttribute("aria-label", "Actions for all tables");
    tablesMoreBtn.setAttribute("aria-haspopup", "menu");
    tablesMoreBtn.title = "More actions for all tables";
    tablesMoreBtn.innerHTML = ICON_SVG.more;
    tablesMoreBtn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        openRowMenu(tablesMoreBtn, [
            {
                label: "Sync descriptions from source…",
                icon: "sync",
                onClick: () => openSyncDescriptionsAllTablesModal(),
            },
        ]);
    });
    tablesHeaderRow.appendChild(tablesMoreBtn);
    tablesSection.appendChild(tablesHeaderRow);
    const tablesList = document.createElement("div");
    tablesList.className = "slls-dle-list";
    tablesSection.appendChild(tablesList);
    const addTablesBtn = document.createElement("button");
    addTablesBtn.className = "slls-dle-btn slls-dle-btn-primary";
    addTablesBtn.style.marginTop = "10px";
    addTablesBtn.innerHTML = `${PLUS_SVG} <span style="margin-left:6px;">Add table(s)</span>`;
    addTablesBtn.addEventListener("click", () => openAddTablesModal());
    tablesSection.appendChild(addTablesBtn);

    function renderSources() {
        const sources = model.get("sources") || [];
        const added = pendingAddedSourcesWithNames();
        const workspaces = model.get("workspaces") || [];
        const wsNameById = {};
        for (const w of workspaces) wsNameById[w.id] = w.name;
        const totalCount = sources.length + added.length;
        sourcesHeading.innerHTML = `Sources <span class="slls-dle-count">(${totalCount})</span>`;
        sourcesList.innerHTML = "";
        if (totalCount === 0) {
            const e = document.createElement("div");
            e.className = "slls-dle-empty";
            e.textContent = "No Direct Lake sources found.";
            sourcesList.appendChild(e);
            return;
        }
        for (const s of sources) {
            const row = document.createElement("div");
            row.className = "slls-dle-item";
            const dirty = pendingForSource(s.expressionName);
            if (dirty) row.classList.add("pending");
            // Merge the latest staged update_source payload (if any) over the
            // saved values so the main screen reflects pending edits.
            const staged = latestPendingUpdateForSource(s.expressionName) || {};
            const effItemName = staged.source_name || s.itemName;
            const effItemType = staged.source_type || s.itemType;
            const effWsId = staged.source_workspace_id || s.workspaceId;
            const effWsName = staged.source_workspace_id
                ? (wsNameById[effWsId] || s.workspaceName || "")
                : (s.workspaceName || "");
            const effUsesSql = staged.use_sql_endpoint != null
                ? !!staged.use_sql_endpoint
                : !!s.usesSqlEndpoint;
            const main = document.createElement("div");
            main.className = "slls-dle-item-main";
            const nm = document.createElement("div");
            nm.className = "slls-dle-item-name slls-dle-icon-inline";
            const dotHtml = dirty
                ? `<span class="slls-dle-pending-dot" aria-label="Unsaved changes" title="Unsaved changes"></span>`
                : "";
            nm.innerHTML = `${dotHtml}${iconHtml("source")}<span>${escapeHtml(effItemName || "(unknown)")}</span> <span class="slls-dle-pill">${escapeHtml(effItemType || "")}</span>`;
            main.appendChild(nm);
            const meta = document.createElement("div");
            meta.className = "slls-dle-item-meta";
            const sqlBit = effUsesSql ? " · SQL endpoint" : "";
            const tableCount = s.tableCount != null ? ` · ${s.tableCount} table${s.tableCount === 1 ? "" : "s"}` : "";
            meta.textContent = `${effWsName} · expression: ${s.expressionName || ""}${sqlBit}${tableCount}`;
            main.appendChild(meta);
            row.appendChild(main);
            const actions = document.createElement("div");
            actions.className = "slls-dle-item-actions";
            const editBtn = document.createElement("button");
            editBtn.className = "slls-dle-btn";
            editBtn.textContent = "Edit";
            editBtn.addEventListener("click", () => openSourceModal(s));
            actions.appendChild(editBtn);
            if (dirty) {
                const revertBtn = document.createElement("button");
                revertBtn.className = "slls-dle-btn slls-dle-btn-danger";
                revertBtn.textContent = "Revert";
                revertBtn.title = "Discard staged edits for this source";
                revertBtn.addEventListener("click", () => {
                    revertChangesMatching(
                        c => c.kind === "update_source" && c.key === s.expressionName,
                        `Reverted staged edits for '${s.itemName || s.expressionName}'.`,
                    );
                });
                actions.appendChild(revertBtn);
            }
            row.appendChild(actions);
            sourcesList.appendChild(row);
        }
        // Pending added sources (not yet persisted): show with orange dot
        // and a "pending" pill, plus a Revert action that removes the
        // staged add_source change entirely.
        for (const entry of added) {
            const c = entry.change;
            const p = c.payload || {};
            const wsName = wsNameById[p.source_workspace_id] || "";
            const sqlBit = p.use_sql_endpoint ? " · SQL endpoint" : "";
            const row = document.createElement("div");
            row.className = "slls-dle-item pending";
            const main = document.createElement("div");
            main.className = "slls-dle-item-main";
            const nm = document.createElement("div");
            nm.className = "slls-dle-item-name slls-dle-icon-inline";
            nm.innerHTML =
                `<span class="slls-dle-pending-dot" aria-label="Unsaved" title="Unsaved"></span>` +
                iconHtml("source") +
                `<span>${escapeHtml(p.source_name || "(new source)")}</span> <span class="slls-dle-pill">${escapeHtml(p.source_type || "")}</span>`;
            main.appendChild(nm);
            const meta = document.createElement("div");
            meta.className = "slls-dle-item-meta";
            meta.textContent =
                `${wsName} · expression: ${entry.expressionName}${sqlBit} · Pending — will be added on save`;
            main.appendChild(meta);
            row.appendChild(main);
            const actions = document.createElement("div");
            actions.className = "slls-dle-item-actions";
            const revertBtn = document.createElement("button");
            revertBtn.className = "slls-dle-btn slls-dle-btn-danger";
            revertBtn.textContent = "Revert";
            revertBtn.title = "Remove this staged source";
            revertBtn.addEventListener("click", () => {
                // Also drop any staged reassign_table / add_tables changes
                // that reference this pending source's expression name,
                // since that expression will no longer exist after revert.
                const exprName = entry.expressionName;
                revertChangesMatching(
                    ch => (
                        ch.id === c.id ||
                        (
                            ch.kind === "reassign_table" &&
                            ch.payload &&
                            ch.payload.expression_name === exprName
                        ) ||
                        (
                            ch.kind === "add_tables" &&
                            ch.payload &&
                            ch.payload.expression_name === exprName
                        )
                    ),
                    `Reverted staged source '${p.source_name || ""}'.`,
                );
            });
            actions.appendChild(revertBtn);
            row.appendChild(actions);
            sourcesList.appendChild(row);
        }
    }

    function renderTables() {
        closeOpenMenu();
        const tables = model.get("tables") || [];
        const addedTables = pendingAddedTables();
        const totalCount = tables.length + addedTables.length;
        tablesHeading.innerHTML = `Tables <span class="slls-dle-count">(${totalCount})</span>`;
        tablesList.innerHTML = "";
        if (totalCount === 0) {
            const e = document.createElement("div");
            e.className = "slls-dle-empty";
            e.textContent = "No tables in this model.";
            tablesList.appendChild(e);
            return;
        }
        for (const t of tables) {
            const row = document.createElement("div");
            row.className = "slls-dle-item";
            const dirty = pendingForTable(t.name);
            if (dirty) row.classList.add("pending");
            // Merge the latest staged reassign payload (if any) over the
            // saved values so the main screen reflects pending edits.
            const staged = latestPendingReassignForTable(t.name) || {};
            const stagedRename = latestPendingRenameForTable(t.name) || {};
            const effExpression = staged.expression_name || t.expressionName || "";
            const effSchema = staged.schema != null
                ? (staged.schema || "")
                : (t.schemaName || "");
            const effEntity = staged.entity_name || t.entityName || "";
            const effName = stagedRename.new_name || t.name;
            const main = document.createElement("div");
            main.className = "slls-dle-item-main";
            const nm = document.createElement("div");
            nm.className = "slls-dle-item-name slls-dle-icon-inline";
            const dotHtml = dirty
                ? `<span class="slls-dle-pending-dot" aria-label="Unsaved changes" title="Unsaved changes"></span>`
                : "";
            const renamedSuffix = stagedRename.new_name && stagedRename.new_name !== t.name
                ? ` <span class="slls-dle-item-meta" style="font-weight:400;">(was ${escapeHtml(t.name)})</span>`
                : "";
            nm.innerHTML = `${dotHtml}${iconHtml("table")}<span>${escapeHtml(effName)}</span>${renamedSuffix}`;
            main.appendChild(nm);
            const meta = document.createElement("div");
            meta.className = "slls-dle-item-meta";
            const sourceLabel = effExpression || "(no expression)";
            const entity = effSchema
                ? `${effSchema}.${effEntity}`
                : effEntity;
            meta.textContent = `Entity: ${entity || "(unknown)"} · Source: ${sourceLabel}`;
            main.appendChild(meta);
            // Render the table's description (if any) directly below the
            // entity/source meta line so it is visible at a glance.
            const stagedDesc = latestPendingTableDescription(t.name);
            const effDesc = stagedDesc !== undefined
                ? (stagedDesc || "")
                : (t.description || "");
            if (effDesc) {
                const descMeta = document.createElement("div");
                descMeta.className = "slls-dle-item-meta";
                descMeta.textContent = `Description: ${effDesc}`;
                main.appendChild(descMeta);
            }
            row.appendChild(main);
            const actions = document.createElement("div");
            actions.className = "slls-dle-item-actions";
            const moreBtn = document.createElement("button");
            moreBtn.className = "slls-dle-icon-btn";
            moreBtn.setAttribute("aria-label", `Actions for ${t.name}`);
            moreBtn.setAttribute("aria-haspopup", "menu");
            moreBtn.title = "More actions";
            moreBtn.innerHTML = ICON_SVG.more;
            moreBtn.addEventListener("click", (ev) => {
                ev.stopPropagation();
                openRowMenu(moreBtn, [
                    {
                        label: "Edit columns…",
                        icon: "column",
                        onClick: () => openColumnsModal(t),
                    },
                    {
                        label: "Edit table description…",
                        icon: "pencil",
                        onClick: () => openTableDescriptionModal(t),
                    },
                    {
                        label: "Sync columns with source…",
                        icon: "sync",
                        onClick: () => openSyncColumnsModal(t),
                    },
                    {
                        separatorBefore: true,
                        label: "Rename…",
                        icon: "pencil",
                        onClick: () => openRenameTableModal(t),
                    },
                    {
                        label: "Reassign source…",
                        icon: "link",
                        onClick: () => openReassignModal(t),
                    },
                ]);
            });
            actions.appendChild(moreBtn);
            row.appendChild(actions);
            tablesList.appendChild(row);
        }
        // Pending added tables (not yet persisted)
        for (const at of addedTables) {
            const row = document.createElement("div");
            row.className = "slls-dle-item pending";
            const main = document.createElement("div");
            main.className = "slls-dle-item-main";
            const nm = document.createElement("div");
            nm.className = "slls-dle-item-name";
            // Derive the display name: the user-supplied name takes priority,
            // otherwise fall back to the entity (the part after the schema in
            // "schema.entity") or the full spec.
            const specStr = at.spec || "";
            const fallbackEntity = specStr.includes(".")
                ? specStr.split(".").slice(1).join(".")
                : specStr;
            const displayName = (at.name || fallbackEntity || "(new table)");
            nm.classList.add("slls-dle-icon-inline");
            nm.innerHTML =
                `<span class="slls-dle-pending-dot" aria-label="Unsaved" title="Unsaved"></span>` +
                iconHtml("table") +
                `<span>${escapeHtml(displayName)}</span>`;
            main.appendChild(nm);
            const meta = document.createElement("div");
            meta.className = "slls-dle-item-meta";
            const entityBit = specStr ? `Entity: ${specStr} · ` : "";
            meta.textContent =
                `${entityBit}Pending — will be added from '${at.expressionName || ""}' on save`;
            main.appendChild(meta);
            row.appendChild(main);
            const actions = document.createElement("div");
            actions.className = "slls-dle-item-actions";
            const revertBtn = document.createElement("button");
            revertBtn.className = "slls-dle-btn slls-dle-btn-danger";
            revertBtn.textContent = "Revert";
            revertBtn.title = "Remove this staged table";
            revertBtn.addEventListener("click", () => {
                revertAddedTableSpec(at.changeId, at.spec);
            });
            actions.appendChild(revertBtn);
            row.appendChild(actions);
            tablesList.appendChild(row);
        }
    }

    model.on("change:sources", () => { renderSources(); renderTables(); renderSaveBar(); });
    model.on("change:tables", () => { renderTables(); renderSaveBar(); });

    // ----------- Modal infrastructure -----------
    const overlay = document.createElement("div");
    overlay.className = "slls-dle-overlay";
    root.appendChild(overlay);
    const modal = document.createElement("div");
    modal.className = "slls-dle-modal";
    overlay.appendChild(modal);
    // Track listeners registered while a modal is open so we can fully
    // detach them no matter how the modal is dismissed (cancel, save,
    // overlay click, or Escape key).
    let modalCleanups = [];
    function registerModalCleanup(fn) { modalCleanups.push(fn); }
    function runModalCleanups() {
        while (modalCleanups.length) {
            const fn = modalCleanups.pop();
            try { fn(); } catch (_) { /* ignore */ }
        }
    }
    overlay.addEventListener("click", (e) => {
        if (e.target === overlay) closeModal();
    });
    function openModal() {
        overlay.classList.add("show");
    }
    function closeModal() {
        runModalCleanups();
        overlay.classList.remove("show");
        modal.innerHTML = "";
    }
    document.addEventListener("keydown", (e) => {
        if (e.key === "Escape" && overlay.classList.contains("show")) {
            closeModal();
        }
    });

    // ----- Modal building helpers -----
    function modalHeader(title) {
        modal.innerHTML = "";
        const h = document.createElement("h2");
        h.textContent = title;
        modal.appendChild(h);
        return h;
    }
    function modalFooter() {
        const f = document.createElement("div");
        f.className = "slls-dle-modal-footer";
        modal.appendChild(f);
        return f;
    }
    function makeBtn(label, cls, onClick) {
        const b = document.createElement("button");
        b.className = cls || "slls-dle-btn";
        b.textContent = label;
        if (onClick) b.addEventListener("click", onClick);
        return b;
    }
    function addCloseBtn(footer, label) {
        const b = makeBtn(label || "Cancel", "slls-dle-btn", closeModal);
        footer.appendChild(b);
        return b;
    }

    function openSourceModal(existing) {
        modalHeader(existing ? `Edit source: ${existing.itemName}` : "Add a new source");

        // Baseline = saved server values; staged = latest pending update_source
        // payload (if any). Initial form values come from staged ?? baseline so
        // reopening the modal reflects any in-flight staged changes.
        const baseline = existing ? {
            itemType: existing.itemType || "",
            workspaceId: existing.workspaceId || "",
            itemId: existing.itemId || "",
            usesSqlEndpoint: !!existing.usesSqlEndpoint,
        } : null;
        const staged = existing ? latestPendingUpdateForSource(existing.expressionName) : null;
        const initial = baseline ? {
            itemType: staged ? (staged.source_type || baseline.itemType) : baseline.itemType,
            workspaceId: staged ? (staged.source_workspace_id || baseline.workspaceId) : baseline.workspaceId,
            itemId: staged ? (staged.source_id || baseline.itemId) : baseline.itemId,
            usesSqlEndpoint: staged ? !!staged.use_sql_endpoint : baseline.usesSqlEndpoint,
        } : null;

        const grid = document.createElement("div");
        grid.className = "slls-dle-grid";
        modal.appendChild(grid);

        const typeSel = document.createElement("select");
        typeSel.className = "slls-dle-select";
        for (const t of (model.get("source_types") || [])) {
            const o = document.createElement("option"); o.value = t; o.textContent = t;
            if (initial && initial.itemType === t) o.selected = true;
            typeSel.appendChild(o);
        }
        const typeField = makeField("Source type", typeSel);
        grid.appendChild(typeField);

        const wsSel = document.createElement("select");
        wsSel.className = "slls-dle-select";
        for (const ws of (model.get("workspaces") || [])) {
            const o = document.createElement("option"); o.value = ws.id; o.textContent = ws.name;
            if (initial && initial.workspaceId === ws.id) o.selected = true;
            wsSel.appendChild(o);
        }
        if (!existing) wsSel.value = model.get("workspace_id") || wsSel.value;
        const wsField = makeField("Source workspace", wsSel, { wide: true });
        grid.appendChild(wsField);

        const itemSel = document.createElement("select");
        itemSel.className = "slls-dle-select";
        const itemField = makeField("Source item", itemSel);
        grid.appendChild(itemField);

        const sqlToggle = makeToggle("Use SQL endpoint", initial ? initial.usesSqlEndpoint : false);
        modal.appendChild(sqlToggle);

        function refreshDots() {
            if (!baseline) return;
            setFieldDirty(typeField, typeSel.value !== baseline.itemType);
            setFieldDirty(wsField, wsSel.value !== baseline.workspaceId);
            setFieldDirty(itemField, !!itemSel.value && itemSel.value !== baseline.itemId);
            setToggleDirty(sqlToggle, !!sqlToggle._input.checked !== baseline.usesSqlEndpoint);
        }

        function refreshItems() {
            const map = model.get("source_items") || {};
            const key = `${wsSel.value}::${typeSel.value}`;
            const items = map[key] || [];
            itemSel.innerHTML = "";
            if (items.length === 0) {
                const o = document.createElement("option");
                o.value = ""; o.textContent = "(loading…)"; o.disabled = true; o.selected = true;
                itemSel.appendChild(o);
                if (wsSel.value && typeSel.value) {
                    runAction("list_source_items", {
                        workspace_id: wsSel.value,
                        source_type: typeSel.value,
                    });
                }
                refreshDots();
                return;
            }
            // Prefer the staged/initial itemId if it's valid for the current
            // (workspace, type) pair; otherwise fall back to the first option.
            const desiredItemId = initial ? initial.itemId : "";
            let matched = false;
            for (const it of items) {
                const o = document.createElement("option");
                o.value = it.id; o.textContent = it.name;
                if (desiredItemId && desiredItemId === it.id) {
                    o.selected = true;
                    matched = true;
                }
                itemSel.appendChild(o);
            }
            if (!matched && itemSel.options.length > 0) {
                itemSel.selectedIndex = 0;
            }
            refreshDots();
        }
        typeSel.addEventListener("change", refreshItems);
        wsSel.addEventListener("change", refreshItems);
        itemSel.addEventListener("change", refreshDots);
        sqlToggle._input.addEventListener("change", refreshDots);
        const itemsListener = () => refreshItems();
        model.on("change:source_items", itemsListener);
        registerModalCleanup(() => {
            try { model.off("change:source_items", itemsListener); } catch(_) {}
        });
        refreshItems();
        refreshDots();

        const footer = modalFooter();
        addCloseBtn(footer);
        const save = makeBtn(existing ? "Stage changes" : "Stage source", "slls-dle-btn slls-dle-btn-primary", () => {
            if (!itemSel.value) { setStatus("Please pick a source item.", "error"); return; }
            const opt = itemSel.options[itemSel.selectedIndex];
            const itemName = (opt && opt.textContent) || itemSel.value;
            const payload = {
                source_type: typeSel.value,
                source_workspace_id: wsSel.value,
                source_id: itemSel.value,
                source_name: itemName,
                use_sql_endpoint: sqlToggle._input.checked,
            };
            if (existing) {
                payload.expression_name = existing.expressionName;
                enqueuePendingChange({
                    id: pendingId(),
                    kind: "update_source",
                    key: existing.expressionName,
                    payload,
                });
            } else {
                // Precompute the expression name this source will receive on
                // save so it can be referenced immediately (e.g., for
                // reassigning a table to this not-yet-saved source).
                const savedExpr = (model.get("sources") || [])
                    .map(s => s.expressionName);
                const stagedExpr = pendingAddedSourcesWithNames()
                    .map(e => e.expressionName);
                const taken = new Set([...savedExpr, ...stagedExpr]);
                payload.expression_name = previewExpressionName(payload, taken);
                enqueuePendingChange({
                    id: pendingId(),
                    kind: "add_source",
                    key: pendingId(),
                    payload,
                });
            }
            closeModal();
        });
        footer.appendChild(save);
        openModal();
    }

    function openRenameTableModal(table) {
        modalHeader(`Rename table: ${table.name}`);

        const stagedRename = latestPendingRenameForTable(table.name) || {};
        const initialName = stagedRename.new_name || table.name;

        const grid = document.createElement("div");
        grid.className = "slls-dle-grid";
        modal.appendChild(grid);

        const nameInput = document.createElement("input");
        nameInput.type = "text";
        nameInput.className = "slls-dle-input";
        nameInput.value = initialName;
        nameInput.placeholder = table.name;
        const nameField = makeField("New table name", nameInput);
        grid.appendChild(nameField);

        function refreshDots() {
            setFieldDirty(nameField, (nameInput.value || "") !== table.name);
        }
        nameInput.addEventListener("input", refreshDots);
        refreshDots();

        const footer = modalFooter();
        addCloseBtn(footer);
        if (pendingState.changes.some(
            c => c.kind === "rename_table" && c.key === table.name,
        )) {
            const revert = makeBtn("Revert", "slls-dle-btn slls-dle-btn-danger", () => {
                revertChangesMatching(
                    c => c.kind === "rename_table" && c.key === table.name,
                    `Reverted staged rename for '${table.name}'.`,
                );
                closeModal();
            });
            revert.title = "Discard staged rename for this table";
            footer.appendChild(revert);
        }
        footer.appendChild(makeBtn("Stage changes", "slls-dle-btn slls-dle-btn-primary", () => {
            const newName = (nameInput.value || "").trim();
            if (!newName) {
                setStatus("Table name is required.", "error");
                return;
            }
            if (newName === table.name) {
                // No change; clear any prior staged rename so the row stops
                // showing as dirty.
                revertChangesMatching(
                    c => c.kind === "rename_table" && c.key === table.name,
                    "",
                );
                closeModal();
                return;
            }
            // Replace any previous rename staged for this table so the
            // modal behaves as a single-shot editor.
            pendingState.changes = pendingState.changes.filter(
                c => !(c.kind === "rename_table" && c.key === table.name),
            );
            enqueuePendingChange({
                id: pendingId(),
                kind: "rename_table",
                key: table.name,
                payload: { table_name: table.name, new_name: newName },
            });
            closeModal();
        }));
        openModal();
    }

    function openReassignModal(table) {
        modalHeader(`Reassign table: ${table.name}`);

        // Baseline = saved values; staged = latest pending reassign_table
        // payload. Initial form values come from staged ?? baseline.
        const baseline = {
            expressionName: table.expressionName || "",
            schema: table.schemaName || "",
            entityName: table.entityName || "",
        };
        const staged = latestPendingReassignForTable(table.name);
        const initial = {
            expressionName: staged ? (staged.expression_name || baseline.expressionName) : baseline.expressionName,
            schema: staged ? (staged.schema != null ? staged.schema : baseline.schema) : baseline.schema,
            entityName: staged ? (staged.entity_name || baseline.entityName) : baseline.entityName,
        };

        const sources = model.get("sources") || [];
        const stagedSources = pendingAddedSourcesWithNames();
        const grid = document.createElement("div");
        grid.className = "slls-dle-grid";
        modal.appendChild(grid);

        const exprSel = document.createElement("select");
        exprSel.className = "slls-dle-select";
        for (const s of sources) {
            const o = document.createElement("option");
            o.value = s.expressionName;
            o.textContent = s.expressionName;
            if (s.expressionName === initial.expressionName) o.selected = true;
            exprSel.appendChild(o);
        }
        if (stagedSources.length) {
            const group = document.createElement("optgroup");
            group.label = "Pending (unsaved) sources";
            for (const entry of stagedSources) {
                const p = entry.change.payload || {};
                const o = document.createElement("option");
                o.value = entry.expressionName;
                const label = p.source_name
                    ? `${entry.expressionName} — ${p.source_name}`
                    : entry.expressionName;
                o.textContent = label;
                if (entry.expressionName === initial.expressionName) o.selected = true;
                group.appendChild(o);
            }
            exprSel.appendChild(group);
        }
        const exprField = makeField("Source (expression)", exprSel);
        grid.appendChild(exprField);

        const schemaInput = document.createElement("input");
        schemaInput.type = "text";
        schemaInput.className = "slls-dle-input";
        schemaInput.value = initial.schema;
        schemaInput.placeholder = "dbo";
        const schemaField = makeField("Schema (optional)", schemaInput);
        grid.appendChild(schemaField);

        const entityInput = document.createElement("input");
        entityInput.type = "text";
        entityInput.className = "slls-dle-input";
        entityInput.value = initial.entityName;
        entityInput.placeholder = "source_table";
        const entityField = makeField("Entity (source table) name", entityInput);
        grid.appendChild(entityField);

        function refreshDots() {
            setFieldDirty(exprField, exprSel.value !== baseline.expressionName);
            setFieldDirty(schemaField, (schemaInput.value || "") !== baseline.schema);
            setFieldDirty(entityField, (entityInput.value || "") !== baseline.entityName);
        }
        exprSel.addEventListener("change", refreshDots);
        schemaInput.addEventListener("input", refreshDots);
        entityInput.addEventListener("input", refreshDots);
        refreshDots();

        const footer = modalFooter();
        addCloseBtn(footer);
        // Show Revert if there are staged reassign changes for this table.
        if (pendingState.changes.some(
            c => c.kind === "reassign_table" && c.key === table.name,
        )) {
            const revert = makeBtn("Revert", "slls-dle-btn slls-dle-btn-danger", () => {
                revertChangesMatching(
                    c => c.kind === "reassign_table" && c.key === table.name,
                    `Reverted staged reassign for '${table.name}'.`,
                );
                closeModal();
            });
            revert.title = "Discard staged reassign for this table";
            footer.appendChild(revert);
        }
        footer.appendChild(makeBtn("Stage changes", "slls-dle-btn slls-dle-btn-primary", () => {
            const entity = (entityInput.value || "").trim();
            if (!entity) { setStatus("Entity name is required.", "error"); return; }
            enqueuePendingChange({
                id: pendingId(),
                kind: "reassign_table",
                key: table.name,
                payload: {
                    table_name: table.name,
                    expression_name: exprSel.value,
                    entity_name: entity,
                    schema: (schemaInput.value || "").trim(),
                },
            });
            closeModal();
        }));
        openModal();
    }

    // Power BI data types supported by this editor. Binary and Variant are
    // intentionally excluded.
    const COLUMN_DATA_TYPES = [
        "Int64",
        "Double",
        "Decimal",
        "String",
        "Boolean",
        "DateTime",
    ];

    // JS mirror of sempy_labs.semantic_model._helper.convert_column_data_type
    // Maps a raw source data type (e.g. "integer", "varchar(50)") to the
    // matching Power BI data type. Returns "" if input is falsy.
    function convertColumnDataType(raw) {
        if (!raw) return "";
        const s = String(raw).toLowerCase();
        const MAP = {
            "boolean": "Boolean",
            "tinyint": "Int64", "smallint": "Int64", "int": "Int64",
            "integer": "Int64", "bigint": "Int64", "long": "Int64",
            "short": "Int64",
            "float": "Double", "double": "Double",
            "decimal": "Decimal",
            "string": "String", "char": "String", "varchar": "String",
            "binary": "Binary",
            "date": "DateTime", "timestamp": "DateTime",
            "timestamp_ntz": "DateTime",
        };
        if (MAP[s]) return MAP[s];
        if (s.includes("decimal")) return "Decimal";
        if (s.includes("char") || s.includes("string")) return "String";
        if (s.includes("int") || s.includes("long")) return "Int64";
        if (s.includes("float") || s.includes("double")) return "Double";
        return "String";
    }

    function openColumnsModal(table) {
        modal.classList.add("slls-dle-modal-wide");
        registerModalCleanup(() => modal.classList.remove("slls-dle-modal-wide"));
        modalHeader(`Edit columns: ${table.name}`);

        const baseCols = applyPendingSyncToColumns(
            table.name,
            (table.columns || []).map(c => ({
                name: c.name,
                sourceColumn: c.sourceColumn || "",
                dataType: c.dataType || "",
                dataCategory: c.dataCategory || "",
                description: c.description || "",
                columnType: c.columnType || "",
            })),
        );
        const stagedMap = mergedPendingColumnEditsForTable(table.name);

        if (baseCols.length === 0) {
            const p = document.createElement("div");
            p.className = "slls-dle-empty";
            p.textContent = "This table has no editable columns.";
            modal.appendChild(p);
            addCloseBtn(modalFooter(), "Close");
            openModal();
            return;
        }

        const list = document.createElement("div");
        list.className = "slls-dle-columns-list";
        modal.appendChild(list);

        // Track the latest values per column so we can compute diffs on save.
        const state = {};
        for (const bc of baseCols) {
            const st = stagedMap[bc.name] || {};
            state[bc.name] = {
                name: "new_name" in st ? (st.new_name || bc.name) : bc.name,
                sourceColumn: "source_column" in st ? (st.source_column || "") : bc.sourceColumn,
                dataType: "data_type" in st ? (st.data_type || "") : bc.dataType,
                description: "description" in st ? (st.description || "") : bc.description,
            };
        }

        // Updates a column row's "dirty" indicator after editing.
        for (const bc of baseCols) {
            const row = document.createElement("div");
            row.className = "slls-dle-column-row";
            const head = document.createElement("div");
            head.className = "slls-dle-column-head";
            const nm = document.createElement("div");
            nm.className = "slls-dle-column-name slls-dle-icon-inline";
            const renameSuffix = state[bc.name].name !== bc.name
                ? ` <span class="slls-dle-column-type" style="text-transform:none;">(was ${escapeHtml(bc.name)})</span>`
                : "";
            nm.innerHTML = `${iconHtml("column")}<span>${escapeHtml(state[bc.name].name)}</span>${renameSuffix}`;
            head.appendChild(nm);
            const ty = document.createElement("div");
            ty.className = "slls-dle-column-type";
            ty.textContent = bc._stagedSync
                ? `${bc.columnType || "Data"} · pending sync`
                : (bc.columnType || "");
            head.appendChild(ty);
            row.appendChild(head);

            const fields = document.createElement("div");
            fields.className = "slls-dle-column-fields";
            row.appendChild(fields);

            const nameInput = document.createElement("input");
            nameInput.type = "text";
            nameInput.className = "slls-dle-input";
            nameInput.value = state[bc.name].name;
            nameInput.placeholder = bc.name;
            // RowNumber columns cannot be renamed by the user.
            if (bc.columnType === "RowNumber") {
                nameInput.disabled = true;
            }
            const nameField = makeField("Column name", nameInput);
            fields.appendChild(nameField);

            const srcInput = document.createElement("input");
            srcInput.type = "text";
            srcInput.className = "slls-dle-input";
            srcInput.value = state[bc.name].sourceColumn;
            srcInput.placeholder = bc.name;
            // Source column is only meaningful for data columns.
            if (bc.columnType && bc.columnType !== "Data") {
                srcInput.disabled = true;
                srcInput.placeholder = "(not applicable)";
            }
            const srcField = makeField("Source column", srcInput);
            fields.appendChild(srcField);

            const typeSel = document.createElement("select");
            typeSel.className = "slls-dle-select";
            const options = COLUMN_DATA_TYPES.slice();
            // Keep any pre-existing data type (e.g. "Binary" on legacy data)
            // as an option so the user does not lose information.
            if (bc.dataType && !options.includes(bc.dataType)) {
                options.unshift(bc.dataType);
            }
            for (const dt of options) {
                const o = document.createElement("option");
                o.value = dt; o.textContent = dt;
                if (state[bc.name].dataType === dt) o.selected = true;
                typeSel.appendChild(o);
            }
            const typeField = makeField("Data type", typeSel);
            fields.appendChild(typeField);

            const descInput = document.createElement("textarea");
            descInput.className = "slls-dle-input";
            descInput.rows = 2;
            descInput.value = state[bc.name].description;
            descInput.placeholder = "(no description)";
            const descField = makeField(
                "Description", descInput, { wide: true },
            );
            fields.appendChild(descField);

            function refreshDots() {
                setFieldDirty(
                    nameField,
                    !nameInput.disabled && (nameInput.value || "") !== bc.name,
                );
                setFieldDirty(
                    srcField,
                    !srcInput.disabled && srcInput.value !== bc.sourceColumn,
                );
                setFieldDirty(typeField, typeSel.value !== bc.dataType);
                setFieldDirty(
                    descField,
                    (descInput.value || "") !== (bc.description || ""),
                );
                const rowDirty =
                    (!nameInput.disabled && (nameInput.value || "") !== bc.name) ||
                    (!srcInput.disabled && srcInput.value !== bc.sourceColumn) ||
                    typeSel.value !== bc.dataType ||
                    (descInput.value || "") !== (bc.description || "");
                row.classList.toggle("pending", rowDirty);
                // Update the head label live to reflect the staged rename.
                const curName = (nameInput.value || "").trim() || bc.name;
                const renamed = curName !== bc.name;
                const suffix = renamed
                    ? ` <span class="slls-dle-column-type" style="text-transform:none;">(was ${escapeHtml(bc.name)})</span>`
                    : "";
                nm.innerHTML = `${iconHtml("column")}<span>${escapeHtml(curName)}</span>${suffix}`;
            }
            nameInput.addEventListener("input", () => {
                state[bc.name].name = nameInput.value;
                refreshDots();
            });
            srcInput.addEventListener("input", () => {
                state[bc.name].sourceColumn = srcInput.value;
                refreshDots();
            });
            typeSel.addEventListener("change", () => {
                state[bc.name].dataType = typeSel.value;
                refreshDots();
            });
            descInput.addEventListener("input", () => {
                state[bc.name].description = descInput.value;
                refreshDots();
            });
            refreshDots();

            list.appendChild(row);
        }

        const footer = modalFooter();
        addCloseBtn(footer);
        // Show Revert if there are staged column edits for this table.
        if (pendingState.changes.some(
            c => c.kind === "edit_columns" && c.key === table.name,
        )) {
            const revert = makeBtn("Revert", "slls-dle-btn slls-dle-btn-danger", () => {
                revertChangesMatching(
                    c => c.kind === "edit_columns" && c.key === table.name,
                    `Reverted staged column edits for '${table.name}'.`,
                );
                closeModal();
            });
            revert.title = "Discard staged column edits for this table";
            footer.appendChild(revert);
        }
        footer.appendChild(makeBtn("Stage changes", "slls-dle-btn slls-dle-btn-primary", () => {
            // Collect only the columns that diverge from baseline; for each
            // such column include only the properties that actually changed.
            const changed = [];
            for (const bc of baseCols) {
                const cur = state[bc.name];
                const entry = { name: bc.name };
                let dirty = false;
                const newName = (cur.name || "").trim();
                if (bc.columnType !== "RowNumber"
                    && newName
                    && newName !== bc.name) {
                    entry.new_name = newName;
                    dirty = true;
                }
                if (bc.columnType === "Data" && cur.sourceColumn !== bc.sourceColumn) {
                    entry.source_column = cur.sourceColumn;
                    dirty = true;
                }
                if (cur.dataType !== bc.dataType) {
                    entry.data_type = cur.dataType;
                    dirty = true;
                }
                if ((cur.description || "") !== (bc.description || "")) {
                    entry.description = cur.description || "";
                    dirty = true;
                }
                if (dirty) changed.push(entry);
            }
            if (changed.length === 0) {
                setStatus("No column changes to stage.", "info");
                closeModal();
                return;
            }
            const payload = { table_name: table.name, columns: changed };
            enqueuePendingChange({
                id: pendingId(),
                kind: "edit_columns",
                key: table.name,
                payload,
            });
            closeModal();
        }));
        openModal();
    }

    function openTableDescriptionModal(table) {
        modalHeader(`Edit description: ${table.name}`);

        const baseTableDescription = table.description || "";
        const stagedTableDescription = latestPendingTableDescription(table.name);
        const initialTableDescription = stagedTableDescription !== undefined
            ? (stagedTableDescription || "")
            : baseTableDescription;

        const grid = document.createElement("div");
        grid.className = "slls-dle-grid";
        modal.appendChild(grid);

        const descInput = document.createElement("textarea");
        descInput.className = "slls-dle-input";
        descInput.rows = 4;
        descInput.value = initialTableDescription;
        descInput.placeholder = "(no description)";
        const descField = makeField(
            "Table description", descInput, { wide: true },
        );
        grid.appendChild(descField);

        function refreshDots() {
            setFieldDirty(
                descField,
                (descInput.value || "") !== baseTableDescription,
            );
        }
        descInput.addEventListener("input", refreshDots);
        refreshDots();

        const footer = modalFooter();
        addCloseBtn(footer);
        // Show Revert if a staged edit_columns change for this table carries
        // a table_description override.
        if (pendingState.changes.some(
            c => c.kind === "edit_columns"
                && c.key === table.name
                && c.payload
                && "table_description" in c.payload
                && !(c.payload.columns || []).length,
        )) {
            const revert = makeBtn(
                "Revert",
                "slls-dle-btn slls-dle-btn-danger",
                () => {
                    revertChangesMatching(
                        c => c.kind === "edit_columns"
                            && c.key === table.name
                            && c.payload
                            && "table_description" in c.payload
                            && !(c.payload.columns || []).length,
                        `Reverted staged description for '${table.name}'.`,
                    );
                    closeModal();
                },
            );
            revert.title = "Discard the staged table description for this table";
            footer.appendChild(revert);
        }
        footer.appendChild(makeBtn(
            "Stage changes",
            "slls-dle-btn slls-dle-btn-primary",
            () => {
                const newDesc = descInput.value || "";
                if (newDesc === baseTableDescription) {
                    setStatus("No description change to stage.", "info");
                    closeModal();
                    return;
                }
                // Replace any previously staged description-only change for
                // this table so only the latest value is applied on save.
                pendingState.changes = pendingState.changes.filter(
                    (c) => !(
                        c.kind === "edit_columns"
                        && c.key === table.name
                        && c.payload
                        && "table_description" in c.payload
                        && !(c.payload.columns || []).length
                    ),
                );
                enqueuePendingChange({
                    id: pendingId(),
                    kind: "edit_columns",
                    key: table.name,
                    payload: {
                        table_name: table.name,
                        columns: [],
                        table_description: newDesc,
                    },
                });
                closeModal();
            },
        ));
        openModal();
    }

    function openSyncDescriptionsAllTablesModal() {
        modalHeader("Sync descriptions from source");

        const tables = model.get("tables") || [];
        const sources = model.get("sources") || [];
        const sourceById = {};
        for (const s of sources) sourceById[s.expressionName] = s;
        const eligible = tables.filter((t) => {
            const s = sourceById[t.expressionName];
            return s && s.itemType === "Lakehouse";
        });

        const footer = modalFooter();
        const cancel = makeBtn("Cancel", "slls-dle-btn", closeModal);

        if (eligible.length === 0) {
            const p = document.createElement("div");
            p.className = "slls-dle-empty";
            p.textContent =
                "Description sync is only supported for tables sourced from "
                + "a Lakehouse. None of the tables in this model qualify.";
            modal.appendChild(p);
            footer.appendChild(cancel);
            openModal();
            return;
        }

        const intro = document.createElement("div");
        intro.className = "slls-dle-item-meta";
        intro.style.marginBottom = "12px";
        intro.textContent =
            `This will set descriptions on ${eligible.length} table(s) and `
            + `their columns from the comments/descriptions on the source `
            + `Lakehouse delta tables.`;
        modal.appendChild(intro);

        const optsRow = document.createElement("div");
        optsRow.className = "slls-dle-field";
        const overwriteWrap = document.createElement("label");
        overwriteWrap.style.cssText =
            "display:flex;gap:8px;align-items:center;font-size:13.5px;";
        const overwriteChk = document.createElement("input");
        overwriteChk.type = "checkbox";
        overwriteChk.checked = false;
        const overwriteLab = document.createElement("span");
        overwriteLab.textContent =
            "Overwrite existing descriptions (otherwise only empty "
            + "descriptions are populated)";
        overwriteWrap.appendChild(overwriteChk);
        overwriteWrap.appendChild(overwriteLab);
        optsRow.appendChild(overwriteWrap);
        modal.appendChild(optsRow);

        if (pendingState.changes.some(c => c.kind === "sync_descriptions")) {
            const revert = makeBtn(
                "Revert",
                "slls-dle-btn slls-dle-btn-danger",
                () => {
                    revertChangesMatching(
                        c => c.kind === "sync_descriptions",
                        "Reverted staged description sync for all tables.",
                    );
                    closeModal();
                },
            );
            revert.title =
                "Discard every staged description sync in this model";
            footer.appendChild(revert);
        }
        footer.appendChild(cancel);
        footer.appendChild(makeBtn(
            "Stage changes",
            "slls-dle-btn slls-dle-btn-primary",
            () => {
                // Replace any previously staged sync_descriptions changes so
                // a fresh bulk sync uses the latest overwrite setting.
                pendingState.changes = pendingState.changes.filter(
                    (c) => c.kind !== "sync_descriptions",
                );
                for (const t of eligible) {
                    enqueuePendingChange({
                        id: pendingId(),
                        kind: "sync_descriptions",
                        key: t.name,
                        payload: {
                            table_name: t.name,
                            overwrite: !!overwriteChk.checked,
                        },
                    });
                }
                closeModal();
            },
        ));
        openModal();
    }

    function openSyncColumnsModal(table) {
        modal.classList.add("slls-dle-modal-wide");
        registerModalCleanup(() => modal.classList.remove("slls-dle-modal-wide"));
        modalHeader(`Sync columns: ${table.name}`);

        const sources = model.get("sources") || [];
        const source = sources.find(
            (s) => s.expressionName === table.expressionName,
        );
        const footer = document.createElement("div");
        footer.className = "slls-dle-modal-footer";

        const cancel = makeBtn("Cancel", "slls-dle-btn", closeModal);

        if (!source) {
            const p = document.createElement("div");
            p.className = "slls-dle-empty";
            p.textContent =
                `Cannot find source for expression '${
                    table.expressionName || ""
                }'. Sync requires a resolvable Direct Lake source.`;
            modal.appendChild(p);
            modal.appendChild(footer);
            footer.appendChild(cancel);
            openModal();
            return;
        }
        if (!table.entityName) {
            const p = document.createElement("div");
            p.className = "slls-dle-empty";
            p.textContent =
                "This table has no source entity name and cannot be synced.";
            modal.appendChild(p);
            modal.appendChild(footer);
            footer.appendChild(cancel);
            openModal();
            return;
        }

        const banner = document.createElement("div");
        banner.className = "slls-dle-item-meta";
        banner.style.marginBottom = "12px";
        const srcLabel = table.schemaName
            ? `${table.schemaName}.${table.entityName}`
            : table.entityName;
        banner.textContent =
            `Comparing model columns against source '${srcLabel}' in `
            + `${source.itemType} '${source.itemName}'.`;
        modal.appendChild(banner);

        if (pendingState.changes.some(
            c => c.kind === "sync_columns" && c.key === table.name
        )) {
            const stagedNotice = document.createElement("div");
            stagedNotice.className = "slls-dle-item-meta";
            stagedNotice.style.cssText = "padding:8px 10px;margin-bottom:10px;border:1px solid var(--slls-orange);background:rgba(255,149,0,0.06);border-radius:var(--slls-radius-sm)";
            stagedNotice.textContent =
                "Showing the columns from your staged sync. Adjust the "
                + "selection and click \"Stage changes\" to replace it, or "
                + "use \"Revert\" to clear it.";
            modal.appendChild(stagedNotice);
        }

        const body = document.createElement("div");
        modal.appendChild(body);
        const loading = document.createElement("div");
        loading.className = "slls-dle-empty";
        loading.style.padding = "16px";
        loading.textContent = "Loading source columns…";
        body.appendChild(loading);

        modal.appendChild(footer);
        footer.appendChild(cancel);
        const hasStaged = pendingState.changes.some(
            c => c.kind === "sync_columns" && c.key === table.name
        );
        if (hasStaged) {
            const revertBtn = makeBtn("Revert", "slls-dle-btn slls-dle-btn-danger", () => {
                revertChangesMatching(
                    c => c.kind === "sync_columns" && c.key === table.name,
                    `Reverted staged column sync for '${table.name}'.`,
                );
                closeModal();
            });
            revertBtn.title = "Discard staged column sync for this table";
            footer.appendChild(revertBtn);
        }
        const applyBtn = makeBtn("Stage changes", "slls-dle-btn slls-dle-btn-primary");
        applyBtn.disabled = true;
        footer.appendChild(applyBtn);
        openModal();

        const schema = table.schemaName || "";
        const entity = table.entityName;
        const key = `${source.workspaceId}::${source.itemType}::${source.itemId}::${schema}::${entity}`;

        // Track checkbox state per category.
        const addChecks = {};
        const removeChecks = {};

        // Aggregate any previously staged sync_columns changes for this
        // table so the modal can pre-select the same columns. When at least
        // one staged change exists, only the staged columns start checked;
        // remaining missing/orphan columns are still shown (unchecked) so
        // the user can opt them in.
        let stagedAdds = null;
        let stagedRemoves = null;
        for (const c of pendingState.changes) {
            if (c.kind !== "sync_columns" || c.key !== table.name) continue;
            if (stagedAdds === null) stagedAdds = new Set();
            if (stagedRemoves === null) stagedRemoves = new Set();
            const p = c.payload || {};
            for (const a of p.add || []) {
                if (a && a.name) stagedAdds.add(a.name);
            }
            for (const r of p.remove || []) {
                if (r) stagedRemoves.add(r);
            }
        }

        function makeSection(title, hint) {
            const wrap = document.createElement("div");
            wrap.className = "slls-dle-section";
            wrap.style.cssText = "margin-top:12px;padding:12px;border:1px solid var(--slls-border);border-radius:var(--slls-radius-sm)";
            const heading = document.createElement("div");
            heading.style.cssText = "font-weight:600;margin-bottom:4px";
            heading.textContent = title;
            wrap.appendChild(heading);
            const sub = document.createElement("div");
            sub.className = "slls-dle-item-meta";
            sub.style.marginBottom = "8px";
            sub.textContent = hint;
            wrap.appendChild(sub);
            return wrap;
        }

        function renderRow(parent, label, meta, checked, onChange) {
            const row = document.createElement("div");
            row.className = "slls-dle-tablerow";
            const cb = document.createElement("input");
            cb.type = "checkbox";
            cb.checked = checked;
            cb.addEventListener("change", () => onChange(cb.checked));
            const lab = document.createElement("label");
            lab.style.cssText = "display:flex;align-items:center;gap:8px;flex:1;min-width:0";
            const iconHolder = document.createElement("span");
            iconHolder.innerHTML = iconHtml("column");
            lab.appendChild(iconHolder.firstChild);
            const nameSpan = document.createElement("span");
            nameSpan.textContent = label;
            nameSpan.style.cssText = "flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap";
            lab.appendChild(nameSpan);
            if (meta) {
                const metaSpan = document.createElement("span");
                metaSpan.textContent = meta;
                metaSpan.style.cssText = "font-size:11px;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;color:var(--slls-text-secondary);background:var(--slls-surface-2);border:1px solid var(--slls-border);border-radius:4px;padding:1px 6px;flex-shrink:0";
                lab.appendChild(metaSpan);
            }
            lab.addEventListener("click", (ev) => {
                ev.preventDefault();
                cb.checked = !cb.checked;
                cb.dispatchEvent(new Event("change"));
            });
            row.appendChild(cb);
            row.appendChild(lab);
            parent.appendChild(row);
        }

        function renderDiff(srcItems) {
            body.innerHTML = "";
            const modelSourceCols = new Set();
            for (const c of table.columns || []) {
                if (c.sourceColumn) modelSourceCols.add(c.sourceColumn);
            }
            const sourceColMap = new Map();
            for (const c of srcItems) sourceColMap.set(c.name, c.dataType || "");

            const missing = srcItems.filter(
                (c) => !modelSourceCols.has(c.name),
            );
            const naturalOrphans = (table.columns || []).filter(
                (c) =>
                    c.sourceColumn
                    && !sourceColMap.has(c.sourceColumn)
                    && (c.columnType === "" || c.columnType === "Data"),
            );
            // Include columns previously staged for removal even when their
            // source still exists, so the user sees the same selection.
            const orphans = naturalOrphans.slice();
            if (stagedRemoves) {
                const seen = new Set(naturalOrphans.map((c) => c.name));
                for (const c of table.columns || []) {
                    if (
                        stagedRemoves.has(c.name)
                        && !seen.has(c.name)
                        && (c.columnType === "" || c.columnType === "Data")
                    ) {
                        orphans.push(c);
                        seen.add(c.name);
                    }
                }
            }

            if (missing.length === 0 && orphans.length === 0) {
                const ok = document.createElement("div");
                ok.className = "slls-dle-empty";
                ok.style.padding = "16px";
                ok.textContent = "Model columns are already in sync with the source.";
                body.appendChild(ok);
                applyBtn.disabled = true;
                return;
            }

            if (missing.length > 0) {
                const sec = makeSection(
                    `Add to model (${missing.length})`,
                    "Source columns that are not in the semantic model. "
                    + "Checked columns will be added.",
                );
                const list = document.createElement("div");
                list.className = "slls-dle-tablerows";
                sec.appendChild(list);
                for (const c of missing) {
                    const checked = stagedAdds
                        ? stagedAdds.has(c.name)
                        : true;
                    addChecks[c.name] = checked;
                    renderRow(list, c.name, convertColumnDataType(c.dataType), checked, (v) => {
                        addChecks[c.name] = v;
                        updateApply();
                    });
                }
                body.appendChild(sec);
            }

            if (orphans.length > 0) {
                const sec = makeSection(
                    `Remove from model (${orphans.length})`,
                    "Model columns whose source column no longer exists in "
                    + "the source. Checked columns will be removed.",
                );
                const list = document.createElement("div");
                list.className = "slls-dle-tablerows";
                sec.appendChild(list);
                for (const c of orphans) {
                    const checked = stagedRemoves
                        ? stagedRemoves.has(c.name)
                        : true;
                    removeChecks[c.name] = checked;
                    const label = c.name === c.sourceColumn
                        ? c.name
                        : `${c.name} (source: ${c.sourceColumn})`;
                    renderRow(list, label, c.dataType || "", checked, (v) => {
                        removeChecks[c.name] = v;
                        updateApply();
                    });
                }
                body.appendChild(sec);
            }
            updateApply();
        }

        function updateApply() {
            const adds = Object.keys(addChecks).filter((k) => addChecks[k]);
            const removes = Object.keys(removeChecks).filter(
                (k) => removeChecks[k],
            );
            applyBtn.disabled = adds.length === 0 && removes.length === 0;
        }

        function handleResult() {
            const map = model.get("source_columns") || {};
            const v = map[key];
            if (!v) return;
            if (v.error) {
                body.innerHTML = "";
                const err = document.createElement("div");
                err.className = "slls-dle-empty";
                err.style.padding = "16px";
                err.textContent = `Could not list source columns: ${v.error}`;
                body.appendChild(err);
                return;
            }
            renderDiff(v.items || []);
        }

        const listener = () => handleResult();
        model.on("change:source_columns", listener);
        registerModalCleanup(() =>
            model.off("change:source_columns", listener),
        );

        // Either render from cache or request fresh.
        const cached = (model.get("source_columns") || {})[key];
        if (cached) {
            handleResult();
        } else {
            runAction("list_source_columns", {
                workspace_id: source.workspaceId,
                source_type: source.itemType,
                source_id: source.itemId,
                schema: schema,
                table: entity,
                use_sql_endpoint: !!source.usesSqlEndpoint,
            });
        }

        applyBtn.addEventListener("click", () => {
            const adds = [];
            const map = (model.get("source_columns") || {})[key];
            const srcMap = new Map();
            for (const c of (map && map.items) || []) srcMap.set(c.name, c.dataType || "");
            for (const colName of Object.keys(addChecks)) {
                if (!addChecks[colName]) continue;
                adds.push({
                    name: colName,
                    dataType: convertColumnDataType(srcMap.get(colName)),
                });
            }
            const removes = Object.keys(removeChecks).filter(
                (k) => removeChecks[k],
            );
            if (adds.length === 0 && removes.length === 0) return;
            // Replace any previously staged sync_columns changes for this
            // table so the modal acts like an editor for a single staged
            // sync rather than queuing duplicates.
            pendingState.changes = pendingState.changes.filter(
                (c) => !(c.kind === "sync_columns" && c.key === table.name),
            );
            enqueuePendingChange({
                id: pendingId(),
                kind: "sync_columns",
                key: table.name,
                payload: {
                    table_name: table.name,
                    add: adds,
                    remove: removes,
                },
            });
            setStatus(
                `Staged column sync for '${table.name}' `
                + `(+${adds.length}, -${removes.length}). `
                + `Click Save to apply.`,
                "info",
            );
            closeModal();
        });
    }

    function openAddTablesModal() {
        modalHeader("Add tables to model");

        const sources = model.get("sources") || [];
        if (sources.length === 0) {
            const p = document.createElement("div");
            p.className = "slls-dle-empty";
            p.textContent = "Add a source to the model before adding tables.";
            modal.appendChild(p);
            addCloseBtn(modalFooter(), "Close");
            openModal();
            return;
        }

        const grid = document.createElement("div");
        grid.className = "slls-dle-grid";
        modal.appendChild(grid);

        const exprSel = document.createElement("select");
        exprSel.className = "slls-dle-select";
        for (const s of sources) {
            const o = document.createElement("option");
            o.value = s.expressionName;
            o.textContent = s.expressionName;
            exprSel.appendChild(o);
        }
        grid.appendChild(makeField("Source (expression)", exprSel));

        const tablesPicker = makeTablesPicker();
        grid.appendChild(makeField("Tables to add", tablesPicker.container, { wide: true }));

        const nameInput = document.createElement("input");
        nameInput.type = "text";
        nameInput.className = "slls-dle-input";
        nameInput.placeholder = "(defaults to source table name)";
        grid.appendChild(
            makeField("Table name (optional)", nameInput, { wide: true })
        );

        const hint = document.createElement("div");
        hint.className = "slls-dle-item-meta";
        hint.style.marginTop = "6px";
        hint.textContent =
            "Pick one or more tables. All columns from each source table will be added. " +
            "The optional table name only applies when adding a single source table.";
        modal.appendChild(hint);

        function sourceFor(exprName) {
            for (const s of sources) {
                if (s.expressionName === exprName) return s;
            }
            return null;
        }
        function exprKey(s) {
            if (!s || !s.workspaceId || !s.itemType || !s.itemId) return "";
            return `${s.workspaceId}::${s.itemType}::${s.itemId}`;
        }
        function existingSpecsForExpression(exprName) {
            const out = new Set();
            const tables = model.get("tables") || [];
            for (const t of tables) {
                if (t.expressionName === exprName) {
                    const spec = t.schemaName
                        ? `${t.schemaName}.${t.entityName}`
                        : (t.entityName || "");
                    if (spec) out.add(spec);
                }
            }
            for (const at of pendingAddedTables()) {
                if (at.expressionName === exprName && at.spec) {
                    out.add(at.spec);
                }
            }
            return Array.from(out);
        }
        function loadTablesForExpression() {
            const s = sourceFor(exprSel.value);
            const key = exprKey(s);
            if (!key) {
                tablesPicker.setEmpty(
                    "Source details unavailable. Apply pending changes first.",
                );
                return;
            }
            const map = model.get("source_tables") || {};
            const excludeSpecs = existingSpecsForExpression(exprSel.value);
            if (map[key]) {
                const v = map[key];
                if (v && v.error) {
                    tablesPicker.setEmpty(`Could not list tables: ${v.error}`);
                } else {
                    tablesPicker.setItems((v && v.items) || [], { excludeSpecs });
                }
                return;
            }
            tablesPicker.setLoading("Loading tables\u2026");
            runAction("list_source_tables", {
                workspace_id: s.workspaceId,
                source_type: s.itemType,
                source_id: s.itemId,
                use_sql_endpoint: !!s.usesSqlEndpoint,
            });
        }
        exprSel.addEventListener("change", loadTablesForExpression);
        const stListener = () => {
            // Only refresh while modal is open.
            if (modal.parentNode) loadTablesForExpression();
        };
        model.on("change:source_tables", stListener);
        registerModalCleanup(() => {
            try { model.off("change:source_tables", stListener); } catch (_) {}
        });
        loadTablesForExpression();

        const footer = modalFooter();
        addCloseBtn(footer);
        footer.appendChild(makeBtn("Stage tables", "slls-dle-btn slls-dle-btn-primary", () => {
            const names = tablesPicker.getSelected();
            if (names.length === 0) { setStatus("Please select at least one table.", "error"); return; }
            const customName = (nameInput.value || "").trim();
            if (customName && names.length > 1) {
                setStatus(
                    "Table name only applies when adding a single source table.",
                    "error",
                );
                return;
            }
            // Encode entries as { spec, name } objects so the backend can
            // honor the optional display name. The Python side accepts both
            // strings (legacy) and { spec, name } dicts.
            const tablesPayload = names.map((spec, idx) => ({
                spec,
                name: idx === 0 ? customName : "",
            }));
            enqueuePendingChange({
                id: pendingId(),
                kind: "add_tables",
                key: pendingId(),
                payload: {
                    expression_name: exprSel.value,
                    tables: tablesPayload,
                },
            });
            closeModal();
        }));
        openModal();
    }

    // ----------- Screen switching -----------
    function renderScreen() {
        const screen = model.get("screen") || "select";
        const onManage = screen === "manage";
        selectScreen.classList.toggle("show", !onManage);
        manageScreen.classList.toggle("show", onManage);
        backBtn.style.display = onManage ? "" : "none";
        if (!onManage) {
            // Hide creation form by default
            createSection.style.display = "none";
            columnsSection.style.display = "none";
            selectSection.style.display = "";
        }
        renderSubtitle();
    }
    function renderSubtitle() {
        const ws = model.get("workspace_name") || "";
        const ds = model.get("dataset_name") || "";
        const screen = model.get("screen") || "select";
        if (screen === "manage" && ds) {
            subtitle.innerHTML =
                `<b>${escapeHtml(ds)}</b>` +
                (ws ? `<span class="slls-dle-sep">·</span>${escapeHtml(ws)}` : "");
        } else if (ws) {
            subtitle.innerHTML = `Workspace: <b>${escapeHtml(ws)}</b>`;
        } else {
            subtitle.textContent = "";
        }
    }
    model.on("change:screen", renderScreen);
    model.on("change:workspace_name", renderSubtitle);
    model.on("change:dataset_name", renderSubtitle);

    // ----------- Attribution -----------
    const attribution = document.createElement("div");
    attribution.className = "slls-dle-attribution";
    attribution.innerHTML =
        'Powered by <a href="https://github.com/microsoft/semantic-link-labs" target="_blank" rel="noopener noreferrer">Semantic Link Labs</a>';
    root.appendChild(attribution);

    // ----------- Initial render -----------
    renderWorkspaces();
    renderDatasets();
    renderSources();
    renderTables();
    renderSaveBar();
    renderScreen();
}
export default { render };
"""


# Inject SVG icons from the shared UI components module so every Semantic
# Link Labs widget renders the same visual primitives (see
# ``sempy_labs._ui_components.ICONS``).
from sempy_labs._ui_components import ICONS as _UI_ICONS  # noqa: E402

_WIDGET_JS = (
    _WIDGET_JS.replace("__SLLS_ICON_PLUS__", _UI_ICONS["plus"])
    .replace("__SLLS_ICON_BACK__", _UI_ICONS["back"])
    .replace("__SLLS_ICON_REFRESH__", _UI_ICONS["refresh"])
    .replace("__SLLS_ICON_SUN__", _UI_ICONS["sun"])
    .replace("__SLLS_ICON_MOON__", _UI_ICONS["moon"])
    .replace("__SLLS_ICON_TABLE__", _UI_ICONS["table"])
    .replace("__SLLS_ICON_COLUMN__", _UI_ICONS["column"])
    .replace("__SLLS_ICON_SOURCE__", _UI_ICONS["source"])
    .replace("__SLLS_ICON_MORE__", _UI_ICONS["more"])
    .replace("__SLLS_ICON_SYNC__", _UI_ICONS["sync"])
    .replace("__SLLS_ICON_PENCIL__", _UI_ICONS["pencil"])
    .replace("__SLLS_ICON_LINK__", _UI_ICONS["link"])
)


def _build_tables_payload(tom):
    """Return a list of dicts describing Direct Lake tables and their partitions."""
    import Microsoft.AnalysisServices.Tabular as TOM

    tables = []
    for t in tom.model.Tables:
        for p in t.Partitions:
            src = getattr(p, "Source", None)
            if src is None:
                continue
            # Only surface entity-partition (Direct Lake) tables.
            entity_name = getattr(src, "EntityName", None)
            schema_name = getattr(src, "SchemaName", None)
            expr_src = getattr(src, "ExpressionSource", None)
            expression_name = getattr(expr_src, "Name", None) if expr_src else None
            if entity_name is None and expression_name is None:
                continue
            columns = []
            for c in t.Columns:
                # Skip the auto-generated RowNumber column; it is not
                # user-editable.
                if c.Type == TOM.ColumnType.RowNumber:
                    continue
                columns.append(
                    {
                        "name": c.Name,
                        "sourceColumn": getattr(c, "SourceColumn", "") or "",
                        "dataType": str(c.DataType),
                        "dataCategory": getattr(c, "DataCategory", "") or "",
                        "description": getattr(c, "Description", "") or "",
                        "columnType": str(c.Type),
                    }
                )
            tables.append(
                {
                    "name": t.Name,
                    "expressionName": expression_name or "",
                    "entityName": entity_name or "",
                    "schemaName": schema_name or "",
                    "description": getattr(t, "Description", "") or "",
                    "columns": columns,
                }
            )
            break  # one partition per Direct Lake table
    return tables


def _build_sources_payload(tom, table_payload):
    """Run get_direct_lake_sources and annotate with table counts."""
    sources = tom.get_direct_lake_sources()
    counts = {}
    for t in table_payload:
        expr = t.get("expressionName") or ""
        counts[expr] = counts.get(expr, 0) + 1
    for s in sources:
        s["tableCount"] = counts.get(s.get("expressionName"), 0)
    return sources


def _unique_expression_name(tom, base):
    """Generate a unique expression name in the model."""
    existing = {e.Name for e in tom.model.Expressions}
    if base not in existing:
        return base
    i = 1
    while f"{base}{i}" in existing:
        i += 1
    return f"{base}{i}"


@log
def direct_lake_manager(
    dataset: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    dark_mode: bool = False,
):
    """
    Generates an interactive editor for managing Direct Lake semantic models.

    The editor lets you pick an existing Direct Lake semantic model within a
    given workspace or create a new Direct Lake model from a supported source
    (leveraging :func:`sempy_labs.directlake.generate_direct_lake_semantic_model`).
    Once a model is open, you can add or edit sources, reassign tables to a
    different source, add new tables to the model, and refresh the model.

    Parameters
    ----------
    dataset : str | uuid.UUID, default=None
        Name or ID of a Direct Lake semantic model to open immediately.
        If None, the manager opens on the model-selection screen.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where datasets are listed and where new
        models are created by default. Defaults to None which resolves to the
        workspace of the attached lakehouse or, if no lakehouse is attached,
        the workspace of the notebook.
    dark_mode : bool, default=False
        If True, renders the manager with a dark color theme. If False, renders
        with a light color theme.
    """

    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "The 'direct_lake_manager' function requires the 'anywidget' package. "
            "Install it with: pip install anywidget"
        ) from e

    import sempy.fabric as fabric
    from IPython.display import display
    from sempy_labs._helper_functions import (
        resolve_workspace_name_and_id,
        resolve_dataset_name_and_id,
        resolve_workspace_id,
        resolve_item_name_and_id,
        create_abfss_path,
        list_columns_from_path,
    )
    from sempy_labs.tom import connect_semantic_model
    from sempy_labs.directlake._generate_shared_expression import (
        generate_shared_expression,
    )
    from sempy_labs.semantic_model._generate import (
        generate_direct_lake_semantic_model,
    )
    from sempy_labs._refresh_semantic_model import refresh_semantic_model
    from sempy_labs.semantic_model._helper import convert_column_data_type

    # ---------------- Initial workspace / dataset resolution ----------------
    initial_ws_name, initial_ws_id = resolve_workspace_name_and_id(workspace)
    initial_ws_id = str(initial_ws_id)

    def _pick_columns(df, preferred_id, preferred_name):
        """Resolve id/name columns on a DataFrame, falling back safely."""
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
            return [{"id": initial_ws_id, "name": str(initial_ws_name or "")}]
        id_col, name_col = _pick_columns(dfW, ["Id"], ["Name"])
        if id_col is None or name_col is None:
            return [{"id": initial_ws_id, "name": str(initial_ws_name or "")}]
        rows = [
            {"id": str(r[id_col]), "name": str(r[name_col])} for _, r in dfW.iterrows()
        ]
        rows.sort(key=lambda x: x["name"].lower())
        return rows

    def _list_datasets_payload(workspace_id):
        try:
            dfD = fabric.list_datasets(workspace=workspace_id, mode="rest")
        except Exception:
            return []
        id_col, name_col = _pick_columns(
            dfD, ["Dataset Id", "Dataset ID"], ["Dataset Name"]
        )
        if id_col is None or name_col is None:
            return []
        rows = [
            {"id": str(r[id_col]), "name": str(r[name_col])} for _, r in dfD.iterrows()
        ]
        rows.sort(key=lambda x: x["name"].lower())
        return rows

    def _list_source_items_payload(workspace_id, source_type):
        # Map the Direct Lake source types to fabric.list_items types.
        type_map = {
            "Lakehouse": "Lakehouse",
            "Warehouse": "Warehouse",
            "SQLDatabase": "SQLDatabase",
            "MirroredAzureDatabricksCatalog": "MirroredAzureDatabricksCatalog",
            "MirroredDatabase": "MirroredDatabase",
        }
        item_type = type_map.get(source_type, source_type)
        try:
            dfI = fabric.list_items(workspace=workspace_id, item_type=item_type)
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

    def _list_source_tables_payload(
        workspace_id, source_type, source_id, use_sql_endpoint=False
    ):
        """Return ``{"items": [{"schema", "table"}, ...]}`` for the given source.

        For Lakehouse sources this uses
        :func:`sempy_labs.lakehouse.get_lakehouse_tables`; for other source
        types (Warehouse, SQLDatabase, MirroredDatabase, etc.) it queries
        ``INFORMATION_SCHEMA.TABLES`` through :class:`sempy_labs._sql.ConnectBase`.
        Returns ``{"error": "..."}`` if listing fails.
        """
        from sempy_labs._sql import ConnectBase

        try:
            if source_type == "Lakehouse":
                from sempy_labs.lakehouse import get_lakehouse_tables

                dfT = get_lakehouse_tables(lakehouse=source_id, workspace=workspace_id)
                items = []
                for _, r in dfT.iterrows():
                    schema = str(r.get("Schema Name") or "")
                    table = str(r.get("Table Name") or "")
                    if not table:
                        continue
                    items.append({"schema": schema, "table": table})
            else:
                with ConnectBase(
                    item=source_id, type=source_type, workspace=workspace_id
                ) as conn:
                    df = conn.query(
                        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                        "WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW') "
                        "ORDER BY TABLE_SCHEMA, TABLE_NAME"
                    )
                items = []
                if df is not None and not df.empty:
                    for _, r in df.iterrows():
                        schema = str(r.get("TABLE_SCHEMA") or "")
                        table = str(r.get("TABLE_NAME") or "")
                        if not table:
                            continue
                        items.append({"schema": schema, "table": table})
            items.sort(key=lambda x: ((x["schema"] or "").lower(), x["table"].lower()))
            return {"items": items}
        except Exception as e:
            return {"error": str(e)}

    def _list_source_columns_payload(
        workspace_id, source_type, source_id, schema, table, use_sql_endpoint=False
    ):
        """Return ``{"items": [{"name", "dataType", "isPrimaryKey"}, ...]}``
        for one source table.

        For Lakehouse sources this uses
        :func:`sempy_labs._helper_functions.list_columns_from_path` against the
        delta-table abfss path; for other source types it queries
        ``INFORMATION_SCHEMA.COLUMNS`` via :class:`sempy_labs._sql.ConnectBase`.
        The primary-key column (if any) is detected via
        :func:`sempy_labs._sql.get_primary_key`.
        """
        from sempy_labs._helper_functions import (
            list_columns_from_path,
            create_abfss_path,
        )
        from sempy_labs._sql import ConnectBase, get_primary_key

        # Best-effort primary-key lookup. Fails silently — e.g. when the
        # source type isn't supported by ConnectBase or the user lacks
        # SQL endpoint access — so column listing still works.
        pk_col = None
        try:
            pk_col = get_primary_key(
                table=table,
                schema=schema or "dbo",
                item=source_id,
                type=source_type,
                workspace=workspace_id,
            )
        except Exception:
            pk_col = None

        try:
            if source_type == "Lakehouse":
                path = create_abfss_path(
                    lakehouse_id=source_id,
                    lakehouse_workspace_id=workspace_id,
                    delta_table_name=table,
                    schema=schema or None,
                )
                dfC = list_columns_from_path(path=path)
                items = []
                if dfC is not None and not dfC.empty:
                    for _, r in dfC.iterrows():
                        col = str(r.get("Column Name") or "")
                        dtype = str(r.get("Data Type") or "")
                        if not col:
                            continue
                        items.append(
                            {
                                "name": col,
                                "dataType": dtype,
                                "isPrimaryKey": bool(pk_col) and col == pk_col,
                            }
                        )
            else:
                with ConnectBase(
                    item=source_id, type=source_type, workspace=workspace_id
                ) as conn:
                    sql = (
                        "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                        f"WHERE TABLE_NAME = '{table}'"
                    )
                    if schema:
                        sql += f" AND TABLE_SCHEMA = '{schema}'"
                    sql += " ORDER BY ORDINAL_POSITION"
                    df = conn.query(sql)
                items = []
                if df is not None and not df.empty:
                    for _, r in df.iterrows():
                        col = str(r.get("COLUMN_NAME") or "")
                        dtype = str(r.get("DATA_TYPE") or "")
                        if not col:
                            continue
                        items.append(
                            {
                                "name": col,
                                "dataType": dtype,
                                "isPrimaryKey": bool(pk_col) and col == pk_col,
                            }
                        )
            return {"items": items}
        except Exception as e:
            return {"error": str(e)}

    workspaces_payload = _list_workspaces_payload()
    datasets_payload = _list_datasets_payload(initial_ws_id)

    initial_screen = "select"
    initial_dataset_id = ""
    initial_dataset_name = ""
    initial_sources = []
    initial_tables = []
    initial_status = {}

    if dataset is not None:
        try:
            ds_name, ds_id = resolve_dataset_name_and_id(dataset, initial_ws_id)
            with connect_semantic_model(
                dataset=ds_id, workspace=initial_ws_id, readonly=True
            ) as tom:
                if not tom.is_direct_lake():
                    initial_status = {
                        "message": (f"Model '{ds_name}' is not in Direct Lake mode."),
                        "kind": "error",
                    }
                else:
                    initial_tables = _build_tables_payload(tom)
                    initial_sources = _build_sources_payload(tom, initial_tables)
                    initial_dataset_id = str(ds_id)
                    initial_dataset_name = ds_name
                    initial_screen = "manage"
        except Exception as e:
            initial_status = {"message": f"Error opening model: {e}", "kind": "error"}

    class DirectLakeEditorWidget(anywidget.AnyWidget):
        _esm = _WIDGET_JS
        _css = _WIDGET_CSS

        screen = traitlets.Unicode("select").tag(sync=True)
        workspaces = traitlets.List().tag(sync=True)
        datasets = traitlets.List().tag(sync=True)
        source_items = traitlets.Dict().tag(sync=True)
        source_types = traitlets.List().tag(sync=True)
        source_tables = traitlets.Dict().tag(sync=True)
        source_columns = traitlets.Dict().tag(sync=True)
        workspace_id = traitlets.Unicode("").tag(sync=True)
        workspace_name = traitlets.Unicode("").tag(sync=True)
        dataset_id = traitlets.Unicode("").tag(sync=True)
        dataset_name = traitlets.Unicode("").tag(sync=True)
        sources = traitlets.List().tag(sync=True)
        tables = traitlets.List().tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        busy = traitlets.Bool(False).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)

    widget = DirectLakeEditorWidget(
        screen=initial_screen,
        workspaces=workspaces_payload,
        datasets=datasets_payload,
        source_items={},
        source_types=list(_SOURCE_TYPES),
        source_tables={},
        source_columns={},
        workspace_id=initial_ws_id,
        workspace_name=initial_ws_name or "",
        dataset_id=initial_dataset_id,
        dataset_name=initial_dataset_name,
        sources=initial_sources,
        tables=initial_tables,
        status=initial_status,
        pending_action={},
        run=0,
        busy=False,
        dark_mode=bool(dark_mode),
    )

    # ------------------ Action helpers ------------------
    def _load_model_state(ds_id, ws_id):
        """Open the model read-only and populate sources/tables on the widget."""
        with connect_semantic_model(
            dataset=ds_id, workspace=ws_id, readonly=True
        ) as tom:
            if not tom.is_direct_lake():
                raise ValueError("Model is not in Direct Lake mode.")
            tables_payload = _build_tables_payload(tom)
            sources_payload = _build_sources_payload(tom, tables_payload)
        widget.tables = tables_payload
        widget.sources = sources_payload

    def _resolve_ws_name(ws_id):
        for ws in widget.workspaces:
            if ws.get("id") == ws_id:
                return ws.get("name", "")
        return ""

    # ------------------ Action dispatcher ------------------
    def _on_run(_change):
        data = dict(widget.pending_action or {})
        action = data.get("action")
        if not action:
            return
        widget.busy = True
        try:
            if action == "list_datasets":
                ws_id = data.get("workspace_id") or widget.workspace_id
                widget.datasets = _list_datasets_payload(ws_id)
                widget.workspace_id = str(ws_id)
                widget.workspace_name = _resolve_ws_name(ws_id)
                widget.status = {}

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

            elif action == "list_source_tables":
                ws_id = data.get("workspace_id")
                src_type = data.get("source_type")
                src_id = data.get("source_id")
                if not ws_id or not src_type or not src_id:
                    return
                key = f"{ws_id}::{src_type}::{src_id}"
                result = _list_source_tables_payload(
                    ws_id,
                    src_type,
                    src_id,
                    bool(data.get("use_sql_endpoint")),
                )
                new_map = dict(widget.source_tables)
                new_map[key] = result
                widget.source_tables = new_map

            elif action == "list_source_columns":
                ws_id = data.get("workspace_id")
                src_type = data.get("source_type")
                src_id = data.get("source_id")
                schema = data.get("schema") or ""
                table = data.get("table") or ""
                if not ws_id or not src_type or not src_id or not table:
                    return
                key = f"{ws_id}::{src_type}::{src_id}::{schema}::{table}"
                result = _list_source_columns_payload(
                    ws_id,
                    src_type,
                    src_id,
                    schema,
                    table,
                    bool(data.get("use_sql_endpoint")),
                )
                new_map = dict(widget.source_columns)
                new_map[key] = result
                widget.source_columns = new_map

            elif action == "list_source_columns_batch":
                ws_id = data.get("workspace_id")
                src_type = data.get("source_type")
                src_id = data.get("source_id")
                tables_list = data.get("tables") or []
                if not ws_id or not src_type or not src_id or not tables_list:
                    return
                use_sql = bool(data.get("use_sql_endpoint"))
                new_map = dict(widget.source_columns)
                for entry in tables_list:
                    schema = entry.get("schema") or ""
                    table = entry.get("table") or ""
                    if not table:
                        continue
                    key = f"{ws_id}::{src_type}::{src_id}::{schema}::{table}"
                    new_map[key] = _list_source_columns_payload(
                        ws_id,
                        src_type,
                        src_id,
                        schema,
                        table,
                        use_sql,
                    )
                # Single assignment triggers one sync to the frontend, which
                # then refreshes every visible column picker.
                widget.source_columns = new_map

            elif action == "open_model":
                ws_id = data.get("workspace_id") or widget.workspace_id
                ds_id = data.get("dataset_id")
                if not ds_id:
                    widget.status = {
                        "message": "Please select a dataset.",
                        "kind": "error",
                    }
                    return
                ds_name, ds_id_resolved = resolve_dataset_name_and_id(ds_id, ws_id)
                with connect_semantic_model(
                    dataset=ds_id_resolved, workspace=ws_id, readonly=True
                ) as tom:
                    if not tom.is_direct_lake():
                        widget.status = {
                            "message": f"Model '{ds_name}' is not in Direct Lake mode.",
                            "kind": "error",
                        }
                        return
                    tables_payload = _build_tables_payload(tom)
                    sources_payload = _build_sources_payload(tom, tables_payload)
                widget.workspace_id = str(ws_id)
                widget.workspace_name = _resolve_ws_name(ws_id)
                widget.dataset_id = str(ds_id_resolved)
                widget.dataset_name = ds_name
                widget.tables = tables_payload
                widget.sources = sources_payload
                widget.screen = "manage"
                widget.status = {"message": "", "kind": "info"}

            elif action == "create_model":
                name = (data.get("dataset_name") or "").strip()
                ws_id = data.get("workspace_id") or widget.workspace_id
                src_type = data.get("source_type") or "Lakehouse"
                src_ws_id = data.get("source_workspace_id") or ws_id
                src_id = data.get("source_id")
                tables = data.get("tables") or []
                selected_columns = data.get("selected_columns") or {}
                # Use `in data` so an empty dict is still treated as the
                # user's authoritative choice ("no PK on any table"), which
                # is distinct from older clients that omit the key entirely.
                primary_keys = (
                    data.get("primary_keys") if "primary_keys" in data else None
                )
                relationships = data.get("relationships") or []
                refresh_after = bool(data.get("refresh"))
                if not name or not src_id or not tables:
                    widget.status = {
                        "message": "Model name, source, and tables are required.",
                        "kind": "error",
                    }
                    return
                # If column selections, primary keys, or relationships were
                # provided, defer the refresh until after we post-process
                # the generated model.
                needs_post_process = (
                    bool(selected_columns)
                    or primary_keys is not None
                    or bool(relationships)
                )
                generate_direct_lake_semantic_model(
                    dataset=name,
                    tables=tables,
                    source=src_id,
                    source_type=src_type,
                    source_workspace=src_ws_id,
                    use_sql_endpoint=bool(data.get("use_sql_endpoint")),
                    workspace=ws_id,
                    refresh=refresh_after and not needs_post_process,
                )
                ds_name, ds_id_resolved = resolve_dataset_name_and_id(name, ws_id)
                if needs_post_process:
                    # Map each model table back to its source table spec by
                    # matching schema + entity names. generate_direct_lake_
                    # semantic_model derives table_name from the part after
                    # the schema dot, e.g. "dbo.sales" -> "sales".
                    import Microsoft.AnalysisServices.Tabular as TOM

                    # When `tables` is a dict it is keyed by display name with
                    # source spec values; otherwise iterate the list directly.
                    if isinstance(tables, dict):
                        source_specs = list(tables.values())
                    else:
                        source_specs = list(tables)

                    with connect_semantic_model(
                        dataset=ds_id_resolved,
                        workspace=ws_id,
                        readonly=False,
                    ) as tom:
                        # Built while iterating tables; used afterwards to
                        # resolve relationship endpoints from source specs
                        # back to TOM table names.
                        spec_to_tbl_name = {}
                        for t in list(tom.model.Tables):
                            # Find matching source spec for this table.
                            matching_spec = None
                            for spec in source_specs:
                                if "." in spec:
                                    sch, ent = spec.split(".", 1)
                                else:
                                    sch, ent = "", spec
                                if t.Name == ent or t.Name == spec:
                                    matching_spec = spec
                                    break
                                # Fall back: match by partition entity/schema.
                                for p in t.Partitions:
                                    src = getattr(p, "Source", None)
                                    if src is None:
                                        continue
                                    en = getattr(src, "EntityName", None) or ""
                                    sn = getattr(src, "SchemaName", None) or ""
                                    if en == ent and (sn or "") == (sch or ""):
                                        matching_spec = spec
                                        break
                                if matching_spec:
                                    break
                            if matching_spec is None:
                                continue
                            spec_to_tbl_name[matching_spec] = t.Name
                            wanted = set(selected_columns.get(matching_spec) or [])
                            if wanted:
                                for c in list(t.Columns):
                                    # Preserve the auto-generated RowNumber column.
                                    if c.Type == TOM.ColumnType.RowNumber:
                                        continue
                                    src_col = getattr(c, "SourceColumn", "") or ""
                                    if (
                                        src_col
                                        and src_col not in wanted
                                        and c.Name not in wanted
                                    ):
                                        t.Columns.Remove(c.Name)
                            # Apply the user's primary key choice. The
                            # frontend sends an empty/absent entry to mean
                            # "no primary key" — in that case clear any
                            # IsKey flag set by generate_direct_lake_
                            # semantic_model's auto-detection.
                            if primary_keys is not None:
                                pk_choice = (
                                    primary_keys.get(matching_spec) or ""
                                ).strip()
                                target = None
                                if pk_choice:
                                    for c in t.Columns:
                                        if c.Type == TOM.ColumnType.RowNumber:
                                            continue
                                        src_col = getattr(c, "SourceColumn", "") or ""
                                        if src_col == pk_choice or c.Name == pk_choice:
                                            target = c
                                            break
                                for c in t.Columns:
                                    if c.Type == TOM.ColumnType.RowNumber:
                                        continue
                                    desired = target is not None and c is target
                                    if bool(getattr(c, "IsKey", False)) != desired:
                                        try:
                                            c.IsKey = desired
                                        except Exception:
                                            pass
                        # Apply user-defined relationships now that all
                        # table-level edits are done. Endpoints come in
                        # as source specs + source column names; resolve
                        # to the matching TOM table and column names.
                        if relationships:

                            def _resolve_col(tbl_name, src_col_name):
                                tbl = tom.model.Tables[tbl_name]
                                for col in tbl.Columns:
                                    if col.Type == TOM.ColumnType.RowNumber:
                                        continue
                                    sc = getattr(col, "SourceColumn", "") or ""
                                    if sc == src_col_name or col.Name == src_col_name:
                                        return col.Name
                                return None

                            for rel in relationships:
                                from_spec = (rel.get("from_table_spec") or "").strip()
                                to_spec = (rel.get("to_table_spec") or "").strip()
                                from_col_src = (rel.get("from_column") or "").strip()
                                to_col_src = (rel.get("to_column") or "").strip()
                                from_card = (
                                    rel.get("from_cardinality") or "Many"
                                ).strip() or "Many"
                                to_card = (
                                    rel.get("to_cardinality") or "One"
                                ).strip() or "One"
                                if from_card not in ("Many", "One", "None"):
                                    from_card = "Many"
                                if to_card not in ("Many", "One", "None"):
                                    to_card = "One"
                                if not (
                                    from_spec
                                    and to_spec
                                    and from_col_src
                                    and to_col_src
                                ):
                                    continue
                                if from_spec == to_spec:
                                    continue
                                from_tbl = spec_to_tbl_name.get(from_spec)
                                to_tbl = spec_to_tbl_name.get(to_spec)
                                if not from_tbl or not to_tbl:
                                    continue
                                from_col = _resolve_col(from_tbl, from_col_src)
                                to_col = _resolve_col(to_tbl, to_col_src)
                                if not from_col or not to_col:
                                    continue
                                # Skip if a relationship with the same
                                # endpoints already exists (avoids dupes
                                # if the user re-runs create).
                                duplicate = False
                                for existing in tom.model.Relationships:
                                    try:
                                        if (
                                            existing.FromTable.Name == from_tbl
                                            and existing.FromColumn.Name == from_col
                                            and existing.ToTable.Name == to_tbl
                                            and existing.ToColumn.Name == to_col
                                        ):
                                            duplicate = True
                                            break
                                    except Exception:
                                        continue
                                if duplicate:
                                    continue
                                try:
                                    tom.add_relationship(
                                        from_table=from_tbl,
                                        from_column=from_col,
                                        to_table=to_tbl,
                                        to_column=to_col,
                                        from_cardinality=from_card,
                                        to_cardinality=to_card,
                                    )
                                except Exception as e:
                                    print(
                                        f"⚠ Could not create relationship "
                                        f"'{from_tbl}[{from_col}]' -> "
                                        f"'{to_tbl}[{to_col}]': {e}"
                                    )
                    if refresh_after:
                        refresh_semantic_model(dataset=ds_id_resolved, workspace=ws_id)
                widget.workspace_id = str(ws_id)
                widget.workspace_name = _resolve_ws_name(ws_id)
                widget.dataset_id = str(ds_id_resolved)
                widget.dataset_name = ds_name
                widget.datasets = _list_datasets_payload(ws_id)
                _load_model_state(ds_id_resolved, ws_id)
                widget.screen = "manage"
                widget.status = {
                    "message": f"Created '{ds_name}'.",
                    "kind": "success",
                }

            elif action == "refresh_model":
                ds_id = widget.dataset_id
                ws_id = widget.workspace_id
                if not ds_id:
                    widget.status = {"message": "No model selected.", "kind": "error"}
                    return
                refresh_semantic_model(dataset=ds_id, workspace=ws_id)
                widget.status = {
                    "message": f"Refreshed '{widget.dataset_name}'.",
                    "kind": "success",
                }

            elif action == "apply_pending_changes":
                ds_id = widget.dataset_id
                ws_id = widget.workspace_id
                if not ds_id:
                    widget.status = {
                        "message": "No model selected.",
                        "kind": "error",
                    }
                    return
                changes = data.get("changes") or []
                if not changes:
                    return

                # Defer table renames to the end so other staged changes
                # that reference the original table name resolve correctly.
                # Run add_source first so subsequent reassign_table /
                # add_tables changes can reference its new expression by name.
                def _change_order(ch):
                    k = ch.get("kind")
                    if k == "add_source":
                        return 0
                    if k == "sync_columns":
                        return 1
                    if k == "rename_table":
                        return 3
                    return 2

                changes = sorted(changes, key=_change_order)
                summary = []
                # Track whether any change adds or removes columns / tables
                # from the model. Direct Lake tables need a model refresh
                # after column-set changes so that the framing picks up the
                # new column data.
                needs_refresh = False
                with connect_semantic_model(
                    dataset=ds_id, workspace=ws_id, readonly=False
                ) as tom:
                    existing_names = {t.Name for t in tom.model.Tables}
                    for change in changes:
                        kind = change.get("kind")
                        p = change.get("payload") or {}
                        if kind == "add_source":
                            src_type = p.get("source_type")
                            src_ws_id = p.get("source_workspace_id")
                            src_id = p.get("source_id")
                            use_sql = bool(p.get("use_sql_endpoint"))
                            src_name, src_id_resolved = resolve_item_name_and_id(
                                item=src_id, type=src_type, workspace=src_ws_id
                            )
                            expr_text = generate_shared_expression(
                                item=src_id_resolved,
                                item_type=src_type,
                                workspace=src_ws_id,
                                use_sql_endpoint=use_sql,
                            )
                            base = "DatabaseQuery" if use_sql else f"DL_{src_type}"
                            preferred = p.get("expression_name")
                            if preferred and not tom.model.Expressions.Find(preferred):
                                expr_name = preferred
                            else:
                                expr_name = _unique_expression_name(tom, base)
                            tom.add_expression(name=expr_name, expression=expr_text)
                            summary.append(f"added source '{src_name}'")
                        elif kind == "update_source":
                            expr_name = p.get("expression_name")
                            src_type = p.get("source_type")
                            src_ws_id = p.get("source_workspace_id")
                            src_id = p.get("source_id")
                            use_sql = bool(p.get("use_sql_endpoint"))
                            src_name, src_id_resolved = resolve_item_name_and_id(
                                item=src_id, type=src_type, workspace=src_ws_id
                            )
                            expr_text = generate_shared_expression(
                                item=src_id_resolved,
                                item_type=src_type,
                                workspace=src_ws_id,
                                use_sql_endpoint=use_sql,
                            )
                            if not tom.model.Expressions.Find(expr_name):
                                raise ValueError(
                                    f"Expression '{expr_name}' not found in model."
                                )
                            tom.model.Expressions[expr_name].Expression = expr_text
                            summary.append(f"updated source '{expr_name}'")
                        elif kind == "reassign_table":
                            table_name = p.get("table_name")
                            expr_name = p.get("expression_name")
                            entity_name = p.get("entity_name")
                            schema = p.get("schema") or None
                            if not any(t.Name == table_name for t in tom.model.Tables):
                                raise ValueError(
                                    f"Table '{table_name}' not found in model."
                                )
                            if not tom.model.Expressions.Find(expr_name):
                                raise ValueError(
                                    f"Expression '{expr_name}' not found in model."
                                )
                            tbl = tom.model.Tables[table_name]
                            if tbl.Partitions.Count == 0:
                                raise ValueError(
                                    f"Table '{table_name}' has no partitions "
                                    f"to reassign."
                                )
                            part = next(iter(tbl.Partitions))
                            part.Source.EntityName = entity_name
                            part.Source.ExpressionSource = tom.model.Expressions[
                                expr_name
                            ]
                            if schema:
                                part.Source.SchemaName = schema
                                tbl.SourceLineageTag = f"[{schema}].[{entity_name}]"
                            else:
                                # Clear any existing schema so it doesn't
                                # persist while the lineage tag falls back
                                # to [dbo]; mirrors tom.add_entity_partition
                                # behavior when no schema is supplied.
                                part.Source.SchemaName = ""
                                tbl.SourceLineageTag = f"[dbo].[{entity_name}]"
                            summary.append(f"reassigned '{table_name}'")
                        elif kind == "add_tables":
                            expr_name = p.get("expression_name")
                            table_specs = p.get("tables") or []
                            if not expr_name or not table_specs:
                                raise ValueError(
                                    "Source and tables are required to add tables."
                                )
                            src_info = next(
                                (
                                    s
                                    for s in widget.sources
                                    if s.get("expressionName") == expr_name
                                ),
                                None,
                            )
                            if src_info is None:
                                raise ValueError(
                                    f"Source for '{expr_name}' could not be resolved."
                                )
                            src_id = src_info.get("itemId")
                            src_ws_id = src_info.get("workspaceId")
                            # Resolve the source workspace ID once outside the
                            # loop; resolve_workspace_id can perform
                            # network/validation calls.
                            src_ws_id_resolved = resolve_workspace_id(src_ws_id)
                            for spec in table_specs:
                                # Accept both legacy string specs ("schema.table")
                                # and dict specs ({"spec": ..., "name": ...})
                                # so the optional user-supplied display name
                                # can override the entity name.
                                if isinstance(spec, dict):
                                    raw_spec = (spec.get("spec") or "").strip()
                                    custom_name = (
                                        spec.get("name") or ""
                                    ).strip() or None
                                else:
                                    raw_spec = str(spec).strip()
                                    custom_name = None
                                if not raw_spec:
                                    continue
                                if "." in raw_spec:
                                    schema_name, entity_name = raw_spec.split(".", 1)
                                else:
                                    schema_name = None
                                    entity_name = raw_spec
                                table_display = custom_name or entity_name
                                if table_display in existing_names:
                                    raise ValueError(
                                        f"Table '{table_display}' already "
                                        f"exists in the model."
                                    )
                                path = create_abfss_path(
                                    lakehouse_id=src_id,
                                    lakehouse_workspace_id=src_ws_id_resolved,
                                    delta_table_name=entity_name,
                                    schema=schema_name,
                                )
                                dfC = list_columns_from_path(path=path)
                                if dfC.empty:
                                    raise ValueError(
                                        f"Source table '{raw_spec}' has no "
                                        f"columns or does not exist."
                                    )
                                tom.add_table(name=table_display)
                                tom.add_entity_partition(
                                    table_name=table_display,
                                    entity_name=entity_name,
                                    expression=expr_name,
                                    schema_name=schema_name,
                                )
                                for _, row in dfC.iterrows():
                                    col_name = row["Column Name"]
                                    dtype = convert_column_data_type(row["Data Type"])
                                    # Binary columns are not supported in
                                    # Direct Lake semantic models; mirror
                                    # generate_direct_lake_semantic_model
                                    # behavior and skip them silently.
                                    if dtype == "Binary":
                                        continue
                                    tom.add_data_column(
                                        table_name=table_display,
                                        column_name=col_name,
                                        data_type=dtype,
                                        source_column=col_name,
                                    )
                                existing_names.add(table_display)
                                summary.append(f"added table '{table_display}'")
                                needs_refresh = True
                        elif kind == "edit_columns":
                            table_name = p.get("table_name")
                            cols = p.get("columns") or []
                            if not table_name:
                                raise ValueError("Table is required to edit columns.")
                            # Apply table-level description first so it
                            # persists even if there are no column edits.
                            if "table_description" in p:
                                if table_name not in [t.Name for t in tom.model.Tables]:
                                    raise ValueError(
                                        f"Table '{table_name}' not found "
                                        "in the model."
                                    )
                                tom.model.Tables[table_name].Description = (
                                    p.get("table_description") or ""
                                )
                            if not cols and "table_description" not in p:
                                continue
                            renames = []
                            for col in cols:
                                col_name = col.get("name")
                                if not col_name:
                                    continue
                                kwargs = {}
                                # Only pass keys that were explicitly staged
                                # so update_column leaves untouched
                                # properties alone.
                                if "source_column" in col:
                                    kwargs["source_column"] = col["source_column"]
                                if "data_type" in col:
                                    kwargs["data_type"] = col["data_type"]
                                if "data_category" in col:
                                    kwargs["data_category"] = col["data_category"]
                                if "description" in col:
                                    kwargs["description"] = col["description"] or ""
                                if kwargs:
                                    tom.update_column(
                                        table_name=table_name,
                                        column_name=col_name,
                                        **kwargs,
                                    )
                                if "new_name" in col:
                                    new_name = (col.get("new_name") or "").strip()
                                    if new_name and new_name != col_name:
                                        renames.append((col_name, new_name))
                            # Apply renames after property updates so the
                            # lookups above still match the original names.
                            for old, new in renames:
                                try:
                                    tom.model.Tables[table_name].Columns[old].Name = new
                                except Exception as exc:
                                    raise ValueError(
                                        f"Could not rename column "
                                        f"'{old}' to '{new}' in "
                                        f"'{table_name}': {exc}"
                                    )
                            parts = []
                            if cols:
                                parts.append(f"updated {len(cols)} column(s)")
                            if "table_description" in p:
                                parts.append("updated table description")
                            summary.append(f"{' and '.join(parts)} in '{table_name}'")
                        elif kind == "rename_table":
                            old_name = change.get("key") or p.get("table_name")
                            new_name = (p.get("new_name") or "").strip()
                            if not old_name or not new_name:
                                raise ValueError(
                                    "Both the original and new table name "
                                    "are required to rename a table."
                                )
                            if old_name == new_name:
                                continue
                            if old_name not in [t.Name for t in tom.model.Tables]:
                                raise ValueError(
                                    f"Table '{old_name}' not found " "in the model."
                                )
                            try:
                                tom.model.Tables[old_name].Name = new_name
                            except Exception as exc:
                                raise ValueError(
                                    f"Could not rename table '{old_name}' "
                                    f"to '{new_name}': {exc}"
                                )
                            summary.append(
                                f"renamed table '{old_name}' to '{new_name}'"
                            )
                        elif kind == "sync_columns":
                            from sempy_labs.semantic_model._helper import (
                                convert_column_data_type,
                            )

                            table_name = p.get("table_name")
                            add_cols = p.get("add") or []
                            remove_cols = p.get("remove") or []
                            if not table_name:
                                raise ValueError("Table is required to sync columns.")
                            if table_name not in [t.Name for t in tom.model.Tables]:
                                raise ValueError(
                                    f"Table '{table_name}' not found " "in the model."
                                )
                            added = 0
                            removed = 0
                            for col in add_cols:
                                col_name = col.get("name")
                                if not col_name:
                                    continue
                                raw_dtype = col.get("dataType") or "string"
                                dtype = convert_column_data_type(raw_dtype)
                                # Binary columns are not supported in
                                # Direct Lake.
                                if dtype == "Binary":
                                    continue
                                existing = {
                                    c.Name for c in tom.model.Tables[table_name].Columns
                                }
                                target_name = col_name
                                suffix = 1
                                while target_name in existing:
                                    suffix += 1
                                    target_name = f"{col_name} {suffix}"
                                tom.add_data_column(
                                    table_name=table_name,
                                    column_name=target_name,
                                    source_column=col_name,
                                    data_type=dtype,
                                )
                                added += 1
                            for col_name in remove_cols:
                                try:
                                    tom.model.Tables[table_name].Columns.Remove(
                                        col_name
                                    )
                                    removed += 1
                                except Exception:
                                    continue
                            if added or removed:
                                needs_refresh = True
                            summary.append(
                                f"synced columns in '{table_name}' "
                                f"(+{added}, -{removed})"
                            )
                        elif kind == "sync_descriptions":
                            from sempy_labs._helper_functions import (
                                extract_descriptions_from_table_path,
                            )
                            import Microsoft.AnalysisServices.Tabular as TOM

                            table_name = p.get("table_name")
                            overwrite = bool(p.get("overwrite"))
                            if not table_name:
                                raise ValueError(
                                    "Table is required to sync descriptions."
                                )
                            if table_name not in [t.Name for t in tom.model.Tables]:
                                raise ValueError(
                                    f"Table '{table_name}' not found in the model."
                                )
                            t = tom.model.Tables[table_name]
                            part = next(iter(t.Partitions), None)
                            if part is None or part.Mode != TOM.ModeType.DirectLake:
                                raise ValueError(
                                    f"Table '{table_name}' is not a Direct Lake "
                                    "table."
                                )
                            expr_name = part.Source.ExpressionSource.Name
                            src_info = next(
                                (
                                    s
                                    for s in widget.sources
                                    if s.get("expressionName") == expr_name
                                ),
                                None,
                            )
                            if (
                                src_info is None
                                or src_info.get("itemType") != "Lakehouse"
                            ):
                                raise ValueError(
                                    f"Sync descriptions for '{table_name}' is "
                                    "only supported when the source is a Lakehouse."
                                )
                            path = create_abfss_path(
                                lakehouse_id=src_info.get("itemId"),
                                lakehouse_workspace_id=src_info.get("workspaceId"),
                                delta_table_name=part.Source.EntityName,
                                schema=part.Source.SchemaName or None,
                            )
                            descriptions = extract_descriptions_from_table_path(path)
                            table_description = (
                                descriptions.get("tableDescription") or ""
                            )
                            if overwrite or not (t.Description or ""):
                                t.Description = table_description
                            applied = 0
                            for c in t.Columns:
                                if c.Type == TOM.ColumnType.RowNumber:
                                    continue
                                if not overwrite and (c.Description or ""):
                                    continue
                                desc = next(
                                    (
                                        col.get("description")
                                        for col in descriptions.get("columns", [])
                                        if col.get("columnName") == c.SourceColumn
                                    ),
                                    None,
                                )
                                c.Description = desc or ""
                                applied += 1
                            summary.append(
                                f"synced descriptions on '{table_name}' "
                                f"({applied} column(s))"
                            )
                        else:
                            raise ValueError(f"Unknown pending change kind: {kind!r}")
                    tom.model.SaveChanges()
                # Refresh the model after the TOM context closes so the
                # SaveChanges commit is durable before the refresh picks
                # up the new column / table set.
                refresh_note = ""
                if needs_refresh:
                    try:
                        refresh_semantic_model(dataset=ds_id, workspace=ws_id)
                        refresh_note = " Model refreshed."
                    except Exception as refresh_exc:
                        refresh_note = f" Note: model refresh failed: {refresh_exc}"
                _load_model_state(ds_id, ws_id)
                widget.status = {
                    "message": (
                        f"Saved {len(changes)} change(s) to the model: "
                        f"{', '.join(summary)}.{refresh_note}"
                        if summary
                        else f"Saved {len(changes)} change(s) to the model."
                        f"{refresh_note}"
                    ),
                    "kind": "success",
                }

        except Exception as e:
            widget.status = {"message": f"Error: {e}", "kind": "error"}
        finally:
            widget.busy = False

    widget.observe(_on_run, names=["run"])

    # Keep a reference on the widget so the Python-side observer is not garbage
    # collected after this function returns.
    display(widget)
