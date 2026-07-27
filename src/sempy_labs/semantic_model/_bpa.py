from typing import Optional
from uuid import UUID

from sempy._utils._log import log

from sempy_labs._ui_components import (
    ICONS as _UI_ICONS,
    LIGHT_THEME_VARS as _UI_LIGHT_VARS,
    DARK_THEME_VARS as _UI_DARK_VARS,
)

# The maximum number of models a single bulk scan may target. Keeps the run time
# and the comparison report readable.
_MAX_BULK_MODELS = 10


_WIDGET_CSS = (
    """
.slls-bpa {
"""
    + _UI_LIGHT_VARS
    + """
    --slls-radius: 14px;
    --slls-radius-sm: 8px;
    --slls-error: #ff3b30;
    --slls-error-soft: rgba(255, 59, 48, 0.12);
    --slls-warning: #ff9500;
    --slls-warning-soft: rgba(255, 149, 0, 0.14);
    --slls-info: #0071e3;
    --slls-info-soft: rgba(0, 113, 227, 0.12);
    --slls-success: #34c759;
    --slls-success-soft: rgba(52, 199, 89, 0.14);
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "SF Pro Display",
        "Helvetica Neue", Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    color: var(--ui-text);
    width: 100%;
    max-width: 1100px;
    background: var(--ui-bg-solid);
    border: 1px solid var(--ui-border);
    border-radius: var(--slls-radius);
    box-shadow: var(--ui-shadow-md);
    padding: 24px;
    box-sizing: border-box;
    position: relative;
}
@media (prefers-color-scheme: dark) {
    .slls-bpa.slls-bpa-auto {
"""
    + _UI_DARK_VARS
    + """
        --slls-error: #ff453a;
        --slls-error-soft: rgba(255, 69, 58, 0.18);
        --slls-warning: #ff9f0a;
        --slls-warning-soft: rgba(255, 159, 10, 0.18);
        --slls-info: #0A84FF;
        --slls-info-soft: rgba(10, 132, 255, 0.18);
        --slls-success: #30d158;
        --slls-success-soft: rgba(48, 209, 88, 0.18);
    }
}
.slls-bpa.slls-bpa-dark {
"""
    + _UI_DARK_VARS
    + """
    --slls-error: #ff453a;
    --slls-error-soft: rgba(255, 69, 58, 0.18);
    --slls-warning: #ff9f0a;
    --slls-warning-soft: rgba(255, 159, 10, 0.18);
    --slls-info: #0A84FF;
    --slls-info-soft: rgba(10, 132, 255, 0.18);
    --slls-success: #30d158;
    --slls-success-soft: rgba(48, 209, 88, 0.18);
}
.slls-bpa * { box-sizing: border-box; }

/* Fullscreen: fill the whole screen and drop the framing chrome. Notebook
   hosts often block the native Fullscreen API, so a CSS overlay (position:
   fixed covering the viewport) is used as the reliable primary mechanism. */
.slls-bpa:fullscreen, .slls-bpa:-webkit-full-screen { width: 100vw; height: 100vh; max-width: none;
    max-height: none; border: none; border-radius: 0; box-shadow: none; overflow-y: auto; }
.slls-bpa.slls-bpa-fs { position: fixed; inset: 0; z-index: 2147483000; width: 100vw; height: 100vh;
    max-width: none; max-height: none; margin: 0; border: none; border-radius: 0; box-shadow: none; overflow-y: auto; }

/* ---------------- Header ---------------- */
.slls-bpa-header { display: flex; align-items: center; gap: 12px; margin-bottom: 18px; flex-wrap: wrap; }
.slls-bpa-titlewrap { display: flex; flex-direction: column; margin-right: auto; min-width: 0; }
.slls-bpa-title { font-size: 22px; font-weight: 600; letter-spacing: -0.01em; line-height: 1.15; display: flex; align-items: center; gap: 9px; }
.slls-bpa-title .slls-bpa-title-icon { color: var(--ui-accent); display: inline-flex; }
.slls-bpa-subtitle { font-size: 12.5px; color: var(--ui-text-secondary); margin-top: 3px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 640px; }
.slls-bpa-subtitle b { color: var(--ui-text); font-weight: 500; }
.slls-bpa-subtitle .slls-bpa-sep { color: var(--ui-text-tertiary); margin: 0 6px; }

/* ---------------- Controls ---------------- */
.slls-bpa-select, .slls-bpa-input {
    appearance: none; -webkit-appearance: none;
    background: var(--ui-surface);
    border: 1px solid var(--ui-border-strong);
    border-radius: 999px;
    padding: 7px 14px;
    font-size: 13.5px;
    color: var(--ui-text);
    font-family: inherit;
    transition: border-color 120ms ease, box-shadow 120ms ease;
}
.slls-bpa-select { cursor: pointer; padding-right: 32px;
    background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6'><path fill='%236e6e73' d='M0 0l5 6 5-6z'/></svg>");
    background-repeat: no-repeat; background-position: right 12px center; }
.slls-bpa-select:hover, .slls-bpa-input:hover { border-color: var(--ui-text-tertiary); }
.slls-bpa-select:focus, .slls-bpa-input:focus { outline: none; border-color: var(--ui-accent); box-shadow: 0 0 0 3px var(--ui-accent-soft); }
.slls-bpa-select option { background: #ffffff; color: #1d1d1f; }
@media (prefers-color-scheme: dark) { .slls-bpa.slls-bpa-auto .slls-bpa-select option { background: #2c2c2e; color: #f5f5f7; } }
.slls-bpa.slls-bpa-dark .slls-bpa-select option { background: #2c2c2e; color: #f5f5f7; }
.slls-bpa-input::placeholder { color: var(--ui-text-tertiary); }

.slls-bpa-btn {
    appearance: none;
    border: 1px solid var(--ui-border-strong);
    background: var(--ui-surface);
    color: var(--ui-text);
    font-family: inherit; font-size: 13.5px; font-weight: 500;
    padding: 7px 16px;
    border-radius: 999px;
    cursor: pointer;
    display: inline-flex; align-items: center; gap: 7px;
    transition: background 120ms ease, border-color 120ms ease, transform 80ms ease, opacity 120ms ease;
}
.slls-bpa-btn:hover { background: var(--ui-surface-2); border-color: var(--ui-text-tertiary); }
.slls-bpa-btn:active { transform: scale(0.97); }
.slls-bpa-btn:disabled { opacity: 0.4; cursor: not-allowed; }
.slls-bpa-btn-primary { background: var(--ui-accent); border-color: var(--ui-accent); color: #fff; }
.slls-bpa-btn-primary:hover { background: var(--ui-accent-hover); border-color: var(--ui-accent-hover); }
.slls-bpa-btn-icon { width: 32px; height: 32px; padding: 0; justify-content: center; border-radius: 50%; }
.slls-bpa-btn-sm { font-size: 12.5px; padding: 4px 11px; border-radius: 7px; }

.slls-bpa-toolbar { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.slls-bpa-section { border: 1px solid var(--ui-border); border-radius: var(--slls-radius); background: var(--ui-surface); padding: 16px; margin-top: 14px; }
.slls-bpa-section h3 { margin: 0 0 12px 0; font-size: 14px; font-weight: 600; display: flex; align-items: center; gap: 8px; }
.slls-bpa-section h3 .slls-bpa-count { color: var(--ui-text-tertiary); font-weight: 400; font-size: 12.5px; }
.slls-bpa-hint { font-size: 12.5px; color: var(--ui-text-tertiary); margin: 8px 2px 0 2px; }

.slls-bpa-segmented { display: inline-flex; background: var(--ui-bg-secondary); border-radius: 999px; padding: 3px; gap: 2px; }
.slls-bpa-segmented button { appearance: none; border: none; background: transparent; color: var(--ui-text-secondary); font-family: inherit; font-size: 13px; font-weight: 500;
    padding: 6px 14px; border-radius: 999px; cursor: pointer; transition: background 120ms ease, color 120ms ease; }
.slls-bpa-segmented button.active { background: var(--ui-bg-solid); color: var(--ui-text); box-shadow: var(--ui-shadow-sm); }

/* ---------------- Summary cards ---------------- */
.slls-bpa-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 10px; margin-top: 4px; }
.slls-bpa-card { border: 1px solid var(--ui-border); border-radius: var(--slls-radius-sm); background: var(--ui-bg-tertiary); padding: 12px 14px; display: flex; flex-direction: column; gap: 3px; }
.slls-bpa-card-label { font-size: 11.5px; text-transform: uppercase; letter-spacing: 0.5px; color: var(--ui-text-tertiary); display: flex; align-items: center; gap: 6px; }
.slls-bpa-card-value { font-size: 22px; font-weight: 600; font-variant-numeric: tabular-nums; letter-spacing: -0.01em; }
.slls-bpa-card.error .slls-bpa-card-value { color: var(--slls-error); }
.slls-bpa-card.warning .slls-bpa-card-value { color: var(--slls-warning); }
.slls-bpa-card.info .slls-bpa-card-value { color: var(--slls-info); }

.slls-bpa-catgrid { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 8px; margin-top: 10px; }
.slls-bpa-cat { display: flex; align-items: center; gap: 9px; border: 1px solid var(--ui-border); border-radius: var(--slls-radius-sm); background: var(--ui-bg-solid);
    padding: 9px 11px; cursor: pointer; text-align: left; font-family: inherit; transition: background 120ms ease, border-color 120ms ease; }
.slls-bpa-cat:hover { background: var(--ui-surface-2); }
.slls-bpa-cat.active { border-color: var(--ui-accent); box-shadow: 0 0 0 1px var(--ui-accent); }
.slls-bpa-cat-icon { color: var(--ui-text-tertiary); display: inline-flex; flex-shrink: 0; }
.slls-bpa-cat.active .slls-bpa-cat-icon { color: var(--ui-accent); }
.slls-bpa-cat-body { display: flex; flex-direction: column; gap: 2px; min-width: 0; flex: 1; }
.slls-bpa-cat-name { font-size: 13px; font-weight: 500; color: var(--ui-text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-bpa-cat-counts { display: flex; align-items: center; gap: 9px; font-size: 11.5px; color: var(--ui-text-tertiary); font-variant-numeric: tabular-nums; }
.slls-bpa-cat-counts span { display: inline-flex; align-items: center; gap: 3px; }

/* ---------------- Severity presentation ---------------- */
.slls-bpa-sev-error { color: var(--slls-error); }
.slls-bpa-sev-warning { color: var(--slls-warning); }
.slls-bpa-sev-info { color: var(--slls-info); }

/* ---------------- Multi-select dropdown filter ---------------- */
.slls-bpa-ms { position: relative; display: inline-flex; }
.slls-bpa-ms-btn { appearance: none; background: var(--ui-surface); border: 1px solid var(--ui-border-strong); border-radius: 999px;
    padding: 7px 12px 7px 15px; font-size: 13.5px; font-family: inherit; color: var(--ui-text); cursor: pointer;
    display: inline-flex; align-items: center; gap: 8px; transition: border-color 120ms ease, box-shadow 120ms ease; }
.slls-bpa-ms-btn:hover { border-color: var(--ui-text-tertiary); }
.slls-bpa-ms-btn:focus-visible { outline: none; border-color: var(--ui-accent); box-shadow: 0 0 0 3px var(--ui-accent-soft); }
.slls-bpa-ms-btn.filtered { border-color: var(--ui-accent); background: var(--ui-accent-soft); }
.slls-bpa-ms-label { white-space: nowrap; }
.slls-bpa-ms-caret { display: inline-flex; color: var(--ui-text-tertiary); transform: rotate(90deg); transition: transform 140ms ease; }
.slls-bpa-ms.open .slls-bpa-ms-caret { transform: rotate(-90deg); }
.slls-bpa-ms-panel { display: none; position: absolute; top: calc(100% + 6px); left: 0; z-index: 60; min-width: 210px;
    max-height: 300px; overflow-y: auto; padding: 5px; background: var(--ui-bg-solid); border: 1px solid var(--ui-border);
    border-radius: 10px; box-shadow: var(--ui-shadow-lg); }
.slls-bpa-ms.open .slls-bpa-ms-panel { display: block; }
.slls-bpa-ms-opt { display: flex; align-items: center; gap: 9px; width: 100%; padding: 6px 9px; border: none; background: transparent;
    color: var(--ui-text); font-family: inherit; font-size: 13px; text-align: left; border-radius: 6px; cursor: pointer; }
.slls-bpa-ms-opt:hover { background: var(--ui-surface-2); }
.slls-bpa-ms-check { width: 16px; height: 16px; border-radius: 4px; border: 1px solid var(--ui-border-strong); flex-shrink: 0;
    display: inline-flex; align-items: center; justify-content: center; color: transparent; transition: background 120ms ease, border-color 120ms ease; }
.slls-bpa-ms-opt.checked .slls-bpa-ms-check { background: var(--ui-accent); border-color: var(--ui-accent); color: #fff; }
.slls-bpa-ms-opt-label { display: inline-flex; align-items: center; gap: 7px; min-width: 0; flex: 1; }
.slls-bpa-ms-clear { width: 100%; padding: 6px 9px; margin-bottom: 3px; border: none; background: transparent; color: var(--ui-accent);
    font-family: inherit; font-size: 12.5px; font-weight: 500; text-align: left; border-radius: 6px; cursor: pointer; }
.slls-bpa-ms-clear:hover { background: var(--ui-surface-2); }
.slls-bpa-ms-empty { padding: 8px 10px; font-size: 12.5px; color: var(--ui-text-tertiary); }

/* ---------------- Rule groups ---------------- */
.slls-bpa-groups { display: flex; flex-direction: column; gap: 8px; margin-top: 12px; }
.slls-bpa-group { border: 1px solid var(--ui-border); border-radius: var(--slls-radius-sm); background: var(--ui-bg-solid); overflow: hidden; }
.slls-bpa-group-head { display: flex; align-items: center; gap: 9px; padding: 9px 12px; }
.slls-bpa-group-toggle { display: flex; align-items: center; gap: 9px; flex: 1; min-width: 0; background: transparent; border: none; padding: 0; cursor: pointer;
    font-family: inherit; text-align: left; color: inherit; }
.slls-bpa-caret { display: inline-flex; color: var(--ui-text-tertiary); transition: transform 140ms ease; flex-shrink: 0; }
.slls-bpa-caret.open { transform: rotate(90deg); }
.slls-bpa-group-name { flex: 1; min-width: 0; font-size: 13.5px; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-bpa-badge { flex-shrink: 0; background: var(--ui-bg-secondary); color: var(--ui-text-secondary); border-radius: 999px; padding: 2px 9px; font-size: 11.5px; font-variant-numeric: tabular-nums; }
.slls-bpa-icon { display: inline-flex; align-items: center; justify-content: center; flex-shrink: 0; }
.slls-bpa-info-btn { background: transparent; border: none; padding: 2px; cursor: help; color: var(--ui-text-tertiary); display: inline-flex; border-radius: 4px; transition: color 120ms ease; }
.slls-bpa-info-btn:hover { color: var(--ui-text); }

.slls-bpa-violations { border-top: 1px solid var(--ui-border); max-height: 340px; overflow-y: auto; }
.slls-bpa-violation { display: flex; align-items: center; gap: 10px; padding: 7px 12px 7px 34px; border-bottom: 1px solid var(--ui-border); font-size: 12.5px; }
.slls-bpa-violation:last-child { border-bottom: none; }
.slls-bpa-otype { flex-shrink: 0; background: var(--ui-bg-secondary); color: var(--ui-text-tertiary); border-radius: 5px; padding: 2px 7px; font-size: 11px; }
.slls-bpa-oname { min-width: 0; flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: 500; }

/* ---------------- Fix preview ---------------- */
.slls-bpa-fix { border-top: 1px solid var(--ui-border); background: var(--ui-surface-2); padding: 12px; }
.slls-bpa-fix-list { display: flex; flex-direction: column; gap: 4px; max-height: 280px; overflow-y: auto; margin-bottom: 10px; }
.slls-bpa-fix-item { display: flex; align-items: flex-start; gap: 9px; padding: 6px 8px; border-radius: 6px; font-size: 12.5px; cursor: pointer; }
.slls-bpa-fix-item:hover { background: var(--ui-surface-2); }
.slls-bpa-fix-item input { margin-top: 3px; flex-shrink: 0; }
.slls-bpa-fix-body { min-width: 0; flex: 1; }
.slls-bpa-fix-name { display: block; font-weight: 500; word-break: break-all; }
.slls-bpa-fix-diff { display: block; color: var(--ui-text-tertiary); word-break: break-all; margin-top: 2px; font-size: 11.5px;
    font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace; }
.slls-bpa-fix-before { color: var(--slls-error); text-decoration: line-through; }
.slls-bpa-fix-after { color: var(--slls-success); }
.slls-bpa-fix-actions { display: flex; justify-content: flex-end; gap: 8px; }

/* ---------------- Bulk report ---------------- */
.slls-bpa-bulk-list { display: flex; flex-direction: column; gap: 8px; margin-top: 12px; }
.slls-bpa-bulk-row { display: flex; align-items: center; gap: 12px; border: 1px solid var(--ui-border); border-radius: var(--slls-radius-sm);
    background: var(--ui-bg-solid); padding: 11px 14px; cursor: pointer; font-family: inherit; text-align: left; color: inherit;
    transition: background 120ms ease, border-color 120ms ease; }
.slls-bpa-bulk-row:hover { background: var(--ui-surface-2); border-color: var(--ui-border-strong); }
.slls-bpa-bulk-main { min-width: 0; flex: 1.4; display: flex; flex-direction: column; gap: 2px; }
.slls-bpa-bulk-name { font-size: 13.5px; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-bpa-bulk-ws { font-size: 11.5px; color: var(--ui-text-tertiary); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-bpa-bulk-counts { display: flex; align-items: center; gap: 12px; flex-shrink: 0; font-size: 12.5px; font-variant-numeric: tabular-nums; }
.slls-bpa-bulk-counts span { display: inline-flex; align-items: center; gap: 4px; }
.slls-bpa-bar { flex: 1; min-width: 90px; height: 6px; border-radius: 999px; background: var(--ui-bg-secondary); overflow: hidden; display: flex; }
.slls-bpa-bar i { display: block; height: 100%; }
.slls-bpa-bar .e { background: var(--slls-error); }
.slls-bpa-bar .w { background: var(--slls-warning); }
.slls-bpa-bar .i { background: var(--slls-info); }
.slls-bpa-bulk-total { flex-shrink: 0; width: 54px; text-align: right; font-size: 15px; font-weight: 600; font-variant-numeric: tabular-nums; }

/* ---------------- Selected models (bulk picker) ---------------- */
.slls-bpa-chips { display: flex; flex-wrap: wrap; gap: 7px; margin-top: 10px; }
.slls-bpa-modelchip { display: inline-flex; align-items: center; gap: 7px; background: var(--ui-accent-soft); color: var(--ui-accent);
    border-radius: 999px; padding: 5px 8px 5px 12px; font-size: 12.5px; max-width: 320px; }
.slls-bpa-modelchip span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-bpa-modelchip button { appearance: none; border: none; background: transparent; color: inherit; cursor: pointer; display: inline-flex; padding: 1px; border-radius: 50%; opacity: 0.7; }
.slls-bpa-modelchip button:hover { opacity: 1; }

/* ---------------- Rules panel (overlay) ---------------- */
.slls-bpa-overlay { display: none; position: absolute; inset: 0; background: rgba(0,0,0,0.45); z-index: 50; align-items: flex-start; justify-content: center;
    padding: 24px 16px; border-radius: var(--slls-radius); overflow-y: auto; }
.slls-bpa-overlay.show { display: flex; }
.slls-bpa-modal { background: var(--ui-bg-solid); color: var(--ui-text); border: 1px solid var(--ui-border); border-radius: var(--slls-radius);
    box-shadow: var(--ui-shadow-lg); width: 100%; max-width: 760px; padding: 20px; margin: auto; }
.slls-bpa-modal h2 { margin: 0 0 4px 0; font-size: 17px; font-weight: 600; }
.slls-bpa-modal-sub { font-size: 12.5px; color: var(--ui-text-secondary); margin-bottom: 14px; }
.slls-bpa-modal-footer { display: flex; justify-content: flex-end; gap: 8px; margin-top: 16px; }
.slls-bpa-rulelist { max-height: 46vh; overflow-y: auto; border: 1px solid var(--ui-border); border-radius: var(--slls-radius-sm); }
.slls-bpa-rule { display: flex; align-items: flex-start; gap: 10px; padding: 9px 12px; border-bottom: 1px solid var(--ui-border); }
.slls-bpa-rule:last-child { border-bottom: none; }
.slls-bpa-rule-body { min-width: 0; flex: 1; }
.slls-bpa-rule-name { font-size: 13px; font-weight: 500; display: flex; align-items: center; gap: 7px; }
.slls-bpa-rule-meta { font-size: 11.5px; color: var(--ui-text-tertiary); margin-top: 2px; }
.slls-bpa-rule-desc { font-size: 12px; color: var(--ui-text-secondary); margin-top: 4px; line-height: 1.45; }
.slls-bpa-switch { position: relative; width: 36px; height: 21px; flex-shrink: 0; margin-top: 1px; }
.slls-bpa-switch input { opacity: 0; width: 0; height: 0; position: absolute; }
.slls-bpa-switch i { position: absolute; inset: 0; background: var(--ui-border-strong); border-radius: 999px; cursor: pointer; transition: background 140ms ease; }
.slls-bpa-switch i::after { content: ""; position: absolute; width: 17px; height: 17px; left: 2px; top: 2px; background: #fff; border-radius: 50%;
    transition: transform 140ms ease; box-shadow: 0 1px 2px rgba(0,0,0,0.25); }
.slls-bpa-switch input:checked + i { background: var(--ui-accent); }
.slls-bpa-switch input:checked + i::after { transform: translateX(15px); }

/* ---------------- Rule info popover ---------------- */
.slls-bpa-popover {
    position: fixed; z-index: 2147483001; max-width: 320px;
    background: #ffffff; color: #1d1d1f;
    border: 1px solid rgba(0,0,0,0.10); border-radius: 12px;
    box-shadow: 0 1px 1px rgba(0,0,0,0.04), 0 10px 30px rgba(0,0,0,0.18);
    padding: 12px 14px; font-size: 12.5px; line-height: 1.5;
    opacity: 0; transition: opacity 120ms ease;
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Helvetica Neue", Helvetica, Arial, sans-serif;
}
@media (prefers-color-scheme: dark) { .slls-bpa-popover { background: #2c2c2e; color: #f5f5f7; border-color: rgba(255,255,255,0.12); } }
.slls-bpa-popover.dark { background: #2c2c2e; color: #f5f5f7; border-color: rgba(255,255,255,0.12); }
.slls-bpa-popover.show { opacity: 1; }
.slls-bpa-popover b { display: block; margin-bottom: 5px; font-size: 13px; }
.slls-bpa-popover a { color: #0071e3; text-decoration: none; display: inline-flex; align-items: center; gap: 5px; margin-top: 8px; }
.slls-bpa-popover a:hover { text-decoration: underline; }

/* ---------------- Status / empty / misc ---------------- */
.slls-bpa-status { margin-top: 14px; margin-bottom: 6px; padding: 10px 14px; border-radius: var(--slls-radius-sm); font-size: 13.5px; display: none; }
.slls-bpa-status.show { display: block; animation: slls-bpa-fade 200ms ease; }
.slls-bpa-status.success { background: var(--slls-success-soft); color: var(--slls-success); }
.slls-bpa-status.error { background: var(--slls-error-soft); color: var(--slls-error); }
.slls-bpa-status.info { background: var(--ui-accent-soft); color: var(--ui-accent); }
@keyframes slls-bpa-fade { from { opacity: 0; transform: translateY(-4px); } to { opacity: 1; transform: translateY(0); } }
.slls-bpa-empty { padding: 40px 16px; text-align: center; color: var(--ui-text-tertiary); font-size: 13.5px; display: flex; flex-direction: column; align-items: center; gap: 12px; }
.slls-bpa-empty .slls-bpa-empty-icon { color: var(--slls-success); transform: scale(2.2); margin-bottom: 8px; }
.slls-bpa-busy { pointer-events: none; opacity: 0.55; transition: opacity 120ms ease; }
.slls-bpa-screen { display: none; }
.slls-bpa-screen.show { display: block; }
.slls-bpa-attribution { margin-top: 18px; text-align: right; font-size: 11.5px; color: var(--ui-text-tertiary); }
.slls-bpa-attribution a { color: var(--ui-text-tertiary); text-decoration: none; transition: color 120ms ease; }
.slls-bpa-attribution a:hover { color: var(--ui-accent); }
.slls-bpa-searchwrap { position: relative; display: inline-flex; align-items: center; }
.slls-bpa-searchwrap .slls-bpa-searchicon { position: absolute; left: 12px; color: var(--ui-text-tertiary); display: inline-flex; pointer-events: none; }
.slls-bpa-searchwrap .slls-bpa-input { padding-left: 32px; min-width: 240px; }
"""
)


_WIDGET_JS = r"""
function render({ model, el }) {
    const root = document.createElement("div");
    root.className = "slls-bpa";

    function applyTheme() {
        root.classList.remove("slls-bpa-dark", "slls-bpa-auto");
        const dm = model.get("dark_mode");
        if (dm === true) root.classList.add("slls-bpa-dark");
        else if (dm === null || dm === undefined) root.classList.add("slls-bpa-auto");
    }
    applyTheme();
    model.on("change:dark_mode", applyTheme);
    el.appendChild(root);

    // Icons injected from sempy_labs._ui_components.ICONS (single source of truth).
    const ICON = {
        shield: `__SLLS_ICON_SHIELD_CHECK__`,
        sun: `__SLLS_ICON_SUN__`,
        moon: `__SLLS_ICON_MOON__`,
        back: `__SLLS_ICON_BACK__`,
        swap: `__SLLS_ICON_SWAP__`,
        refresh: `__SLLS_ICON_REFRESH__`,
        search: `__SLLS_ICON_SEARCH__`,
        wrench: `__SLLS_ICON_WRENCH__`,
        info: `__SLLS_ICON_INFO__`,
        alert: `__SLLS_ICON_ALERT__`,
        error: `__SLLS_ICON_ERROR_CIRCLE__`,
        check: `__SLLS_ICON_CHECK_CIRCLE__`,
        external: `__SLLS_ICON_EXTERNAL_LINK__`,
        close: `__SLLS_ICON_CLOSE__`,
        caret: `__SLLS_ICON_CARET_RIGHT__`,
        check: `__SLLS_ICON_CHECK__`,
        play: `__SLLS_ICON_PLAY__`,
        settings: `__SLLS_ICON_SETTINGS__`,
        activity: `__SLLS_ICON_ACTIVITY__`,
        code: `__SLLS_ICON_CODE__`,
        pencil: `__SLLS_ICON_PENCIL__`,
        text: `__SLLS_ICON_TEXT_TYPE__`,
        plus: `__SLLS_ICON_PLUS__`,
        expand: `__SLLS_ICON_EXPAND_ROWS__`,
        collapse: `__SLLS_ICON_COLLAPSE_ROWS__`,
        fullscreen: `__SLLS_ICON_FULLSCREEN__`,
        fullscreen_exit: `__SLLS_ICON_FULLSCREEN_EXIT__`,
    };

    const CATEGORY_ICON = {
        "Performance": ICON.activity,
        "Error Prevention": ICON.shield,
        "DAX Expressions": ICON.code,
        "Maintenance": ICON.settings,
        "Formatting": ICON.pencil,
        "Naming Conventions": ICON.text,
    };
    const SEVERITIES = ["Error", "Warning", "Info"];
    const SEVERITY_ORDER = { Error: 0, Warning: 1, Info: 2 };

    function severityIcon(severity) {
        if (severity === "Error") return ICON.error;
        if (severity === "Warning") return ICON.alert;
        return ICON.info;
    }
    function severityClass(severity) {
        if (severity === "Error") return "slls-bpa-sev-error";
        if (severity === "Warning") return "slls-bpa-sev-warning";
        return "slls-bpa-sev-info";
    }
    function severityTitle(severity) {
        if (severity === "Error") return "Error \u2014 a serious issue that should be fixed.";
        if (severity === "Warning") return "Warning \u2014 a best-practice issue worth reviewing.";
        return "Info \u2014 an informational suggestion.";
    }
    function plural(n, word) { return `${n} ${word}${n === 1 ? "" : "s"}`; }

    function iconSpan(svg, cls, title) {
        const span = document.createElement("span");
        span.className = "slls-bpa-icon" + (cls ? ` ${cls}` : "");
        span.innerHTML = svg || "";
        if (title) { span.title = title; span.setAttribute("aria-label", title); }
        return span;
    }
    function makeButton(label, cls, icon) {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = `slls-bpa-btn${cls ? " " + cls : ""}`;
        if (icon) btn.appendChild(iconSpan(icon));
        if (label) {
            const span = document.createElement("span");
            span.textContent = label;
            btn.appendChild(span);
        }
        return btn;
    }
    function clear(node) { while (node.firstChild) node.removeChild(node.firstChild); }

    // Multi-select dropdown filter. `options` are `{ value, label, icon, iconClass }`
    // descriptors; an empty selection means "all".
    const openDropdowns = new Set();
    function createMultiSelect(allLabel, ariaLabel, onChange) {
        const wrap = document.createElement("div");
        wrap.className = "slls-bpa-ms";

        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "slls-bpa-ms-btn";
        btn.setAttribute("aria-haspopup", "listbox");
        btn.setAttribute("aria-label", ariaLabel);
        const label = document.createElement("span");
        label.className = "slls-bpa-ms-label";
        btn.appendChild(label);
        btn.appendChild(iconSpan(ICON.caret, "slls-bpa-ms-caret"));
        wrap.appendChild(btn);

        const panel = document.createElement("div");
        panel.className = "slls-bpa-ms-panel";
        panel.setAttribute("role", "listbox");
        wrap.appendChild(panel);

        const selected = new Set();
        let options = [];
        let signature = null;

        function close() {
            wrap.classList.remove("open");
            btn.setAttribute("aria-expanded", "false");
        }
        function open() {
            for (const other of openDropdowns) if (other !== close) other();
            wrap.classList.add("open");
            btn.setAttribute("aria-expanded", "true");
        }
        openDropdowns.add(close);

        function renderLabel() {
            if (selected.size === 0) label.textContent = allLabel;
            else if (selected.size === 1) label.textContent = [...selected][0];
            else label.textContent = `${selected.size} selected`;
            btn.classList.toggle("filtered", selected.size > 0);
        }

        function renderPanel() {
            clear(panel);
            if (selected.size > 0) {
                const clearBtn = document.createElement("button");
                clearBtn.type = "button";
                clearBtn.className = "slls-bpa-ms-clear";
                clearBtn.textContent = "Clear selection";
                clearBtn.addEventListener("click", () => {
                    selected.clear();
                    renderLabel();
                    renderPanel();
                    onChange();
                });
                panel.appendChild(clearBtn);
            }
            if (options.length === 0) {
                const empty = document.createElement("div");
                empty.className = "slls-bpa-ms-empty";
                empty.textContent = "No options";
                panel.appendChild(empty);
                return;
            }
            for (const option of options) {
                const row = document.createElement("button");
                row.type = "button";
                row.className = "slls-bpa-ms-opt" + (selected.has(option.value) ? " checked" : "");
                row.setAttribute("role", "option");
                row.setAttribute("aria-selected", String(selected.has(option.value)));
                const box = document.createElement("span");
                box.className = "slls-bpa-ms-check";
                box.innerHTML = ICON.check;
                row.appendChild(box);
                const text = document.createElement("span");
                text.className = "slls-bpa-ms-opt-label";
                if (option.icon) text.appendChild(iconSpan(option.icon, option.iconClass));
                const name = document.createElement("span");
                name.textContent = option.label;
                text.appendChild(name);
                row.appendChild(text);
                if (option.title) row.title = option.title;
                row.addEventListener("click", () => {
                    if (selected.has(option.value)) selected.delete(option.value);
                    else selected.add(option.value);
                    renderLabel();
                    renderPanel();
                    onChange();
                });
                panel.appendChild(row);
            }
        }

        btn.addEventListener("click", (ev) => {
            ev.stopPropagation();
            if (wrap.classList.contains("open")) close();
            else open();
        });
        panel.addEventListener("click", (ev) => ev.stopPropagation());

        renderLabel();
        renderPanel();

        return {
            el: wrap,
            selected,
            close,
            setOptions(next) {
                const nextSignature = next.map((o) => o.value).join("\u0000");
                if (nextSignature === signature) return;
                signature = nextSignature;
                options = next;
                // Drop selections whose option no longer exists.
                const valid = new Set(next.map((o) => o.value));
                let changed = false;
                for (const value of [...selected]) {
                    if (!valid.has(value)) { selected.delete(value); changed = true; }
                }
                if (changed) renderLabel();
                renderPanel();
            },
            reset() {
                if (selected.size === 0) return;
                selected.clear();
                renderLabel();
                renderPanel();
            },
        };
    }
    document.addEventListener("click", () => {
        for (const close of openDropdowns) close();
    });

    function runAction(action, extra) {
        model.set("pending_action", Object.assign({ action }, extra || {}));
        model.set("run", (model.get("run") || 0) + 1);
        model.save_changes();
    }

    // ------------------------------------------------------------------
    // Header
    // ------------------------------------------------------------------
    const header = document.createElement("div");
    header.className = "slls-bpa-header";
    root.appendChild(header);

    const titleWrap = document.createElement("div");
    titleWrap.className = "slls-bpa-titlewrap";
    header.appendChild(titleWrap);

    const title = document.createElement("div");
    title.className = "slls-bpa-title";
    title.appendChild(iconSpan(ICON.shield, "slls-bpa-title-icon"));
    const titleText = document.createElement("span");
    titleText.textContent = "Best Practice Analyzer";
    title.appendChild(titleText);
    titleWrap.appendChild(title);

    const subtitle = document.createElement("div");
    subtitle.className = "slls-bpa-subtitle";
    titleWrap.appendChild(subtitle);

    const backBtn = makeButton("", "slls-bpa-btn-icon", ICON.back);
    backBtn.title = "Back to the comparison report";
    backBtn.setAttribute("aria-label", "Back to the comparison report");
    backBtn.style.display = "none";
    backBtn.addEventListener("click", () => {
        isBulkDrilldown = false;
        model.set("screen", "bulk");
        model.save_changes();
        renderScreen();
    });
    header.appendChild(backBtn);

    const changeModelBtn = makeButton("", "slls-bpa-btn-icon", ICON.swap);
    changeModelBtn.title = "Change semantic model / workspace";
    changeModelBtn.setAttribute("aria-label", "Change semantic model / workspace");
    changeModelBtn.style.display = "none";
    changeModelBtn.addEventListener("click", () => {
        isBulkDrilldown = false;
        model.set("screen", "select");
        model.save_changes();
        renderScreen();
    });
    header.appendChild(changeModelBtn);

    const rulesBtn = makeButton("", "slls-bpa-btn-icon", ICON.settings);
    rulesBtn.title = "Rules";
    rulesBtn.setAttribute("aria-label", "Rules");
    rulesBtn.addEventListener("click", () => openRulesPanel());
    header.appendChild(rulesBtn);

    const rerunBtn = makeButton("", "slls-bpa-btn-icon", ICON.refresh);
    rerunBtn.title = "Re-run the analysis";
    rerunBtn.setAttribute("aria-label", "Re-run the analysis");
    rerunBtn.style.display = "none";
    rerunBtn.addEventListener("click", () => rerun());
    header.appendChild(rerunBtn);

    const themeBtn = makeButton("", "slls-bpa-btn-icon", "");
    function renderThemeBtn() {
        const isDark = model.get("dark_mode") === true;
        themeBtn.innerHTML = isDark ? ICON.sun : ICON.moon;
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

    // ------------------------------------------------------------------
    // Fullscreen
    // Notebook hosts (VS Code, Jupyter, Fabric) frequently sandbox the widget
    // output, so the native Fullscreen API silently rejects. A CSS overlay that
    // covers the viewport is therefore the reliable primary mechanism, with
    // native fullscreen attempted as a best-effort enhancement.
    // ------------------------------------------------------------------
    let fsMode = false;
    const fullscreenBtn = makeButton("", "slls-bpa-btn-icon", "");
    function renderFullscreenBtn() {
        fullscreenBtn.innerHTML = fsMode ? ICON.fullscreen_exit : ICON.fullscreen;
        fullscreenBtn.title = fsMode ? "Exit full screen" : "Full screen";
        fullscreenBtn.setAttribute("aria-label", fullscreenBtn.title);
    }
    function nativeExitFullscreen() {
        const ex = document.exitFullscreen || document.webkitExitFullscreen;
        if (ex && (document.fullscreenElement || document.webkitFullscreenElement)) {
            const p = ex.call(document);
            if (p && p.catch) p.catch(() => {});
        }
    }
    function setFullscreen(on) {
        fsMode = on;
        root.classList.toggle("slls-bpa-fs", on);
        try {
            if (on) {
                const req = root.requestFullscreen || root.webkitRequestFullscreen;
                if (req) { const p = req.call(root); if (p && p.catch) p.catch(() => {}); }
            } else {
                nativeExitFullscreen();
            }
        } catch (e) { /* native fullscreen unavailable; the CSS overlay covers it */ }
        renderFullscreenBtn();
    }
    fullscreenBtn.addEventListener("click", () => setFullscreen(!fsMode));
    document.addEventListener("fullscreenchange", onFullscreenChange);
    document.addEventListener("webkitfullscreenchange", onFullscreenChange);
    document.addEventListener("keydown", onEscapeKey);
    function onFullscreenChange() {
        // Fires only when the native fullscreen state changes; if the user left
        // it (Esc / F11), drop the CSS overlay too.
        const nativeOn = !!(document.fullscreenElement || document.webkitFullscreenElement);
        if (!nativeOn && fsMode) {
            fsMode = false;
            root.classList.remove("slls-bpa-fs");
            renderFullscreenBtn();
        }
    }
    function onEscapeKey(e) {
        if (e.key !== "Escape") return;
        // Close the rules panel first, then leave full screen.
        if (overlay.classList.contains("show")) overlay.classList.remove("show");
        else if (fsMode) setFullscreen(false);
    }
    renderFullscreenBtn();
    header.appendChild(fullscreenBtn);

    // ------------------------------------------------------------------
    // Status banner
    // ------------------------------------------------------------------
    const status = document.createElement("div");
    status.className = "slls-bpa-status";
    root.appendChild(status);
    function setStatus(message, kind) {
        if (!message) { status.classList.remove("show"); return; }
        status.className = `slls-bpa-status show ${kind || "info"}`;
        status.textContent = message;
    }
    model.on("change:status", () => {
        const s = model.get("status") || {};
        setStatus(s.message || "", s.kind);
    });
    model.on("change:busy", () => {
        if (model.get("busy") === true) root.classList.add("slls-bpa-busy");
        else root.classList.remove("slls-bpa-busy");
    });

    // ==================================================================
    // SELECT SCREEN
    // ==================================================================
    const selectScreen = document.createElement("div");
    selectScreen.className = "slls-bpa-screen";
    root.appendChild(selectScreen);

    const selectSection = document.createElement("div");
    selectSection.className = "slls-bpa-section";
    selectScreen.appendChild(selectSection);

    const selectHeading = document.createElement("h3");
    selectHeading.textContent = "Analyze a semantic model";
    selectSection.appendChild(selectHeading);

    const modeToggle = document.createElement("div");
    modeToggle.className = "slls-bpa-segmented";
    modeToggle.style.marginBottom = "14px";
    const singleModeBtn = document.createElement("button");
    singleModeBtn.type = "button";
    singleModeBtn.textContent = "Single model";
    const bulkModeBtn = document.createElement("button");
    bulkModeBtn.type = "button";
    bulkModeBtn.textContent = "Multiple models";
    modeToggle.appendChild(singleModeBtn);
    modeToggle.appendChild(bulkModeBtn);
    selectSection.appendChild(modeToggle);

    let bulkMode = false;
    // key `${workspaceId}\u0000${datasetId}` -> target descriptor
    const bulkSelection = new Map();

    const pickerBar = document.createElement("div");
    pickerBar.className = "slls-bpa-toolbar";
    selectSection.appendChild(pickerBar);

    const wsSelect = document.createElement("select");
    wsSelect.className = "slls-bpa-select";
    wsSelect.style.minWidth = "230px";
    wsSelect.setAttribute("aria-label", "Workspace");
    pickerBar.appendChild(wsSelect);

    const dsSelect = document.createElement("select");
    dsSelect.className = "slls-bpa-select";
    dsSelect.style.minWidth = "250px";
    dsSelect.setAttribute("aria-label", "Semantic model");
    pickerBar.appendChild(dsSelect);

    const runBtn = makeButton("Run analysis", "slls-bpa-btn-primary", ICON.play);
    pickerBar.appendChild(runBtn);

    const addBtn = makeButton("Add", "", ICON.plus);
    addBtn.title = "Add this model to the scan";
    addBtn.style.display = "none";
    pickerBar.appendChild(addBtn);

    const chipsWrap = document.createElement("div");
    chipsWrap.className = "slls-bpa-chips";
    selectSection.appendChild(chipsWrap);

    const selectHint = document.createElement("div");
    selectHint.className = "slls-bpa-hint";
    selectSection.appendChild(selectHint);

    function renderWorkspaces() {
        const items = model.get("workspaces") || [];
        const current = model.get("workspace_id") || "";
        clear(wsSelect);
        if (items.length === 0) {
            const o = document.createElement("option");
            o.value = ""; o.textContent = "No workspaces"; o.disabled = true; o.selected = true;
            wsSelect.appendChild(o);
            return;
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
        clear(dsSelect);
        if (items.length === 0) {
            const o = document.createElement("option");
            o.value = ""; o.textContent = "No semantic models in workspace"; o.disabled = true; o.selected = true;
            dsSelect.appendChild(o);
        } else {
            for (const ds of items) {
                const o = document.createElement("option");
                o.value = ds.id;
                o.textContent = ds.name;
                if (ds.id === current) o.selected = true;
                dsSelect.appendChild(o);
            }
        }
        updateSelectState();
    }
    function renderBulkChips() {
        clear(chipsWrap);
        for (const [key, target] of bulkSelection.entries()) {
            const chip = document.createElement("div");
            chip.className = "slls-bpa-modelchip";
            const label = document.createElement("span");
            label.textContent = target.dataset_name;
            label.title = `${target.dataset_name} \u2022 ${target.workspace_name}`;
            chip.appendChild(label);
            const remove = document.createElement("button");
            remove.type = "button";
            remove.innerHTML = ICON.close;
            remove.title = "Remove";
            remove.setAttribute("aria-label", `Remove ${target.dataset_name}`);
            remove.addEventListener("click", () => {
                bulkSelection.delete(key);
                renderBulkChips();
                updateSelectState();
            });
            chip.appendChild(remove);
            chipsWrap.appendChild(chip);
        }
    }
    function updateSelectState() {
        singleModeBtn.classList.toggle("active", !bulkMode);
        bulkModeBtn.classList.toggle("active", bulkMode);
        addBtn.style.display = bulkMode ? "" : "none";
        chipsWrap.style.display = bulkMode ? "" : "none";
        const maxReached = bulkSelection.size >= MAX_BULK;
        addBtn.disabled = !dsSelect.value || maxReached;
        if (bulkMode) {
            runBtn.disabled = bulkSelection.size === 0;
            selectHint.textContent = maxReached
                ? `Maximum of ${MAX_BULK} models selected.`
                : `Add up to ${MAX_BULK} semantic models, from any workspace, then run the analysis.`;
        } else {
            runBtn.disabled = !dsSelect.value;
            selectHint.textContent = "";
        }
    }

    singleModeBtn.addEventListener("click", () => { bulkMode = false; updateSelectState(); });
    bulkModeBtn.addEventListener("click", () => { bulkMode = true; updateSelectState(); });

    wsSelect.addEventListener("change", () => {
        model.set("workspace_id", wsSelect.value);
        model.set("dataset_id", "");
        model.save_changes();
        runAction("list_datasets", { workspace_id: wsSelect.value });
    });
    dsSelect.addEventListener("change", () => {
        model.set("dataset_id", dsSelect.value);
        model.save_changes();
        updateSelectState();
    });
    addBtn.addEventListener("click", () => {
        if (!dsSelect.value || bulkSelection.size >= MAX_BULK) return;
        const key = `${wsSelect.value}\u0000${dsSelect.value}`;
        bulkSelection.set(key, {
            workspace_id: wsSelect.value,
            workspace_name: wsSelect.options[wsSelect.selectedIndex].textContent,
            dataset_id: dsSelect.value,
            dataset_name: dsSelect.options[dsSelect.selectedIndex].textContent,
        });
        renderBulkChips();
        updateSelectState();
    });
    runBtn.addEventListener("click", () => {
        resetFilters();
        if (bulkMode) {
            runAction("run_bulk", {
                targets: [...bulkSelection.values()],
                disabled_rules: [...disabledRules],
            });
        } else {
            runAction("run_scan", {
                workspace_id: wsSelect.value,
                workspace_name: wsSelect.options[wsSelect.selectedIndex].textContent,
                dataset_id: dsSelect.value,
                dataset_name: dsSelect.options[dsSelect.selectedIndex].textContent,
                disabled_rules: [...disabledRules],
            });
        }
    });

    model.on("change:workspaces", renderWorkspaces);
    model.on("change:datasets", renderDatasets);

    // ==================================================================
    // RESULTS SCREEN
    // ==================================================================
    const resultsScreen = document.createElement("div");
    resultsScreen.className = "slls-bpa-screen";
    root.appendChild(resultsScreen);

    const catGrid = document.createElement("div");
    catGrid.className = "slls-bpa-catgrid";
    catGrid.style.marginTop = "0";
    resultsScreen.appendChild(catGrid);

    const filterBar = document.createElement("div");
    filterBar.className = "slls-bpa-toolbar";
    filterBar.style.marginTop = "16px";
    resultsScreen.appendChild(filterBar);

    const searchWrap = document.createElement("div");
    searchWrap.className = "slls-bpa-searchwrap";
    searchWrap.appendChild(iconSpan(ICON.search, "slls-bpa-searchicon"));
    const searchInput = document.createElement("input");
    searchInput.className = "slls-bpa-input";
    searchInput.type = "search";
    searchInput.placeholder = "Filter rules or objects\u2026";
    searchInput.setAttribute("aria-label", "Filter rules or objects");
    searchWrap.appendChild(searchInput);
    filterBar.appendChild(searchWrap);

    const severityFilterSelect = createMultiSelect(
        "All severities", "Severity", () => renderResults());
    severityFilterSelect.setOptions(SEVERITIES.map((sev) => ({
        value: sev,
        label: sev,
        icon: severityIcon(sev),
        iconClass: severityClass(sev),
        title: severityTitle(sev),
    })));
    filterBar.appendChild(severityFilterSelect.el);

    const objectTypeFilterSelect = createMultiSelect(
        "All object types", "Object type", () => renderResults());
    filterBar.appendChild(objectTypeFilterSelect.el);

    const expandBtn = makeButton("", "slls-bpa-btn-icon", ICON.expand);
    expandBtn.title = "Expand all rules";
    expandBtn.setAttribute("aria-label", "Expand all rules");
    filterBar.appendChild(expandBtn);

    const collapseBtn = makeButton("", "slls-bpa-btn-icon", ICON.collapse);
    collapseBtn.title = "Collapse all rules";
    collapseBtn.setAttribute("aria-label", "Collapse all rules");
    filterBar.appendChild(collapseBtn);

    const groupsWrap = document.createElement("div");
    groupsWrap.className = "slls-bpa-groups";
    resultsScreen.appendChild(groupsWrap);

    // ==================================================================
    // BULK SCREEN
    // ==================================================================
    const bulkScreen = document.createElement("div");
    bulkScreen.className = "slls-bpa-screen";
    root.appendChild(bulkScreen);

    const bulkHeading = document.createElement("div");
    bulkHeading.className = "slls-bpa-hint";
    bulkHeading.style.margin = "0 2px 4px 2px";
    bulkHeading.textContent = "Select a model to review its violations.";
    bulkScreen.appendChild(bulkHeading);

    const bulkCards = document.createElement("div");
    bulkCards.className = "slls-bpa-cards";
    bulkScreen.appendChild(bulkCards);

    const bulkList = document.createElement("div");
    bulkList.className = "slls-bpa-bulk-list";
    bulkScreen.appendChild(bulkList);

    // ------------------------------------------------------------------
    // Attribution
    // ------------------------------------------------------------------
    const attribution = document.createElement("div");
    attribution.className = "slls-bpa-attribution";
    attribution.innerHTML =
        'Powered by <a href="https://github.com/microsoft/semantic-link-labs" target="_blank" rel="noopener noreferrer">Semantic Link Labs</a>' +
        ' &bull; <a href="https://github.com/microsoft/Analysis-Services/tree/master/BestPracticeRules" target="_blank" rel="noopener noreferrer">Best Practice Rules</a>';
    root.appendChild(attribution);

    // ==================================================================
    // Client-side view state
    // ==================================================================
    const MAX_BULK = model.get("max_bulk_models") || 10;
    const disabledRules = new Set(model.get("disabled_rules") || []);
    const expandedRules = new Set();
    let categoryFilter = null;
    let fixRule = null;
    const fixSelected = new Set();
    // Violations currently displayed (single scan, or one model drilled into from bulk).
    let activeViolations = [];

    function resetFilters() {
        expandedRules.clear();
        severityFilterSelect.reset();
        objectTypeFilterSelect.reset();
        categoryFilter = null;
        searchInput.value = "";
        fixRule = null;
        fixSelected.clear();
    }

    function rerun() {
        resetFilters();
        const screen = model.get("screen");
        if (screen === "bulk") {
            runAction("run_bulk", {
                targets: [...bulkSelection.values()],
                disabled_rules: [...disabledRules],
            });
        } else {
            runAction("run_scan", {
                workspace_id: model.get("workspace_id"),
                workspace_name: model.get("workspace_name"),
                dataset_id: model.get("dataset_id"),
                dataset_name: model.get("dataset_name"),
                disabled_rules: [...disabledRules],
            });
        }
    }

    // ------------------------------------------------------------------
    // Rule info popover
    // ------------------------------------------------------------------
    let popover = null;
    let popoverTimer = null;
    function hidePopover() {
        if (popoverTimer) { window.clearTimeout(popoverTimer); popoverTimer = null; }
        if (popover && popover.parentNode) popover.parentNode.removeChild(popover);
        popover = null;
    }
    function showPopover(anchor, group) {
        hidePopover();
        popover = document.createElement("div");
        popover.className = "slls-bpa-popover";
        if (model.get("dark_mode") === true) popover.classList.add("dark");
        const name = document.createElement("b");
        name.textContent = group.ruleName;
        popover.appendChild(name);
        if (group.description) {
            const desc = document.createElement("div");
            desc.textContent = group.description;
            popover.appendChild(desc);
        }
        if (group.url) {
            const link = document.createElement("a");
            link.href = group.url;
            link.target = "_blank";
            link.rel = "noopener noreferrer";
            link.innerHTML = `${ICON.external}<span>Learn more</span>`;
            popover.appendChild(link);
        }
        popover.addEventListener("mouseenter", () => {
            if (popoverTimer) { window.clearTimeout(popoverTimer); popoverTimer = null; }
        });
        popover.addEventListener("mouseleave", scheduleHidePopover);
        document.body.appendChild(popover);

        const rect = anchor.getBoundingClientRect();
        const height = popover.offsetHeight;
        const width = popover.offsetWidth;
        let top = rect.bottom + 8;
        if (top + height > window.innerHeight - 8) top = Math.max(8, rect.top - height - 8);
        let left = Math.min(rect.left, window.innerWidth - width - 12);
        popover.style.top = `${top}px`;
        popover.style.left = `${Math.max(8, left)}px`;
        requestAnimationFrame(() => popover && popover.classList.add("show"));
    }
    function scheduleHidePopover() {
        if (popoverTimer) window.clearTimeout(popoverTimer);
        popoverTimer = window.setTimeout(hidePopover, 140);
    }

    // ------------------------------------------------------------------
    // Results rendering
    // ------------------------------------------------------------------
    function visibleViolations() {
        const term = searchInput.value.trim().toLowerCase();
        const severities = severityFilterSelect.selected;
        const objectTypes = objectTypeFilterSelect.selected;
        return activeViolations.filter((v) => {
            if (categoryFilter && v.category !== categoryFilter) return false;
            if (severities.size > 0 && !severities.has(v.severity)) return false;
            if (objectTypes.size > 0 && !objectTypes.has(v.objectType)) return false;
            if (term
                && !String(v.ruleName).toLowerCase().includes(term)
                && !String(v.objectName).toLowerCase().includes(term)) return false;
            return true;
        });
    }

    function renderCards(container, violations) {
        clear(container);
        const counts = { Error: 0, Warning: 0, Info: 0 };
        for (const v of violations) {
            if (counts[v.severity] !== undefined) counts[v.severity] += 1;
        }
        const entries = [
            ["Violations", violations.length, "", ""],
            ["Errors", counts.Error, "error", ICON.error],
            ["Warnings", counts.Warning, "warning", ICON.alert],
            ["Info", counts.Info, "info", ICON.info],
        ];
        for (const [label, value, cls, icon] of entries) {
            const card = document.createElement("div");
            card.className = `slls-bpa-card${cls ? " " + cls : ""}`;
            const l = document.createElement("div");
            l.className = "slls-bpa-card-label";
            if (icon) l.appendChild(iconSpan(icon));
            const lt = document.createElement("span");
            lt.textContent = label;
            l.appendChild(lt);
            const v = document.createElement("div");
            v.className = "slls-bpa-card-value";
            v.textContent = String(value);
            card.appendChild(l);
            card.appendChild(v);
            container.appendChild(card);
        }
    }

    function renderCategoryCards() {
        clear(catGrid);
        const map = new Map();
        for (const v of activeViolations) {
            const entry = map.get(v.category) || { total: 0, Error: 0, Warning: 0, Info: 0 };
            entry.total += 1;
            if (entry[v.severity] !== undefined) entry[v.severity] += 1;
            map.set(v.category, entry);
        }
        const rows = [...map.entries()].sort((a, b) => a[0].localeCompare(b[0]));
        for (const [category, counts] of rows) {
            const btn = document.createElement("button");
            btn.type = "button";
            btn.className = "slls-bpa-cat" + (categoryFilter === category ? " active" : "");
            btn.title = `${category}: ${plural(counts.Error, "error")}, ${plural(counts.Warning, "warning")}, ${counts.Info} info`;
            btn.appendChild(iconSpan(CATEGORY_ICON[category] || ICON.info, "slls-bpa-cat-icon"));
            const body = document.createElement("div");
            body.className = "slls-bpa-cat-body";
            const name = document.createElement("div");
            name.className = "slls-bpa-cat-name";
            name.textContent = category;
            body.appendChild(name);
            const countsEl = document.createElement("div");
            countsEl.className = "slls-bpa-cat-counts";
            for (const sev of SEVERITIES) {
                if (!counts[sev]) continue;
                const span = document.createElement("span");
                span.appendChild(iconSpan(severityIcon(sev), severityClass(sev)));
                const n = document.createElement("span");
                n.textContent = String(counts[sev]);
                span.appendChild(n);
                countsEl.appendChild(span);
            }
            body.appendChild(countsEl);
            btn.appendChild(body);
            btn.addEventListener("click", () => {
                categoryFilter = categoryFilter === category ? null : category;
                renderResults();
            });
            catGrid.appendChild(btn);
        }
    }

    function renderObjectTypeOptions() {
        const types = [...new Set(activeViolations.map((v) => v.objectType))].sort();
        objectTypeFilterSelect.setOptions(
            types.map((t) => ({ value: t, label: t })));
    }

    function buildGroups(violations) {
        const byRule = new Map();
        for (const v of violations) {
            let g = byRule.get(v.ruleName);
            if (!g) {
                g = {
                    ruleName: v.ruleName,
                    severity: v.severity,
                    category: v.category,
                    description: v.description,
                    url: v.url,
                    fixable: v.fixable,
                    violations: [],
                };
                byRule.set(v.ruleName, g);
            }
            g.violations.push(v);
        }
        return [...byRule.values()].sort((a, b) =>
            (SEVERITY_ORDER[a.severity] ?? 9) - (SEVERITY_ORDER[b.severity] ?? 9)
            || a.ruleName.localeCompare(b.ruleName));
    }

    function renderFixPanel(group) {
        const panel = document.createElement("div");
        panel.className = "slls-bpa-fix";
        const preview = model.get("fix_preview") || {};
        const loading = preview.ruleName !== group.ruleName;
        const items = loading ? [] : (preview.items || []);

        if (loading) {
            const msg = document.createElement("div");
            msg.style.fontSize = "12.5px";
            msg.style.color = "var(--ui-text-tertiary)";
            msg.textContent = "Computing the fix\u2026";
            panel.appendChild(msg);
            return panel;
        }
        if (items.length === 0) {
            const msg = document.createElement("div");
            msg.style.fontSize = "12.5px";
            msg.style.color = "var(--ui-text-tertiary)";
            msg.textContent = "No changes can be applied for this rule.";
            panel.appendChild(msg);
        } else {
            const list = document.createElement("div");
            list.className = "slls-bpa-fix-list";
            for (const item of items) {
                const label = document.createElement("label");
                label.className = "slls-bpa-fix-item";
                const box = document.createElement("input");
                box.type = "checkbox";
                box.checked = fixSelected.has(item.objectName);
                box.addEventListener("change", () => {
                    if (box.checked) fixSelected.add(item.objectName);
                    else fixSelected.delete(item.objectName);
                    applyFixBtn.disabled = fixSelected.size === 0;
                    applyFixBtn.lastChild.textContent = fixSelected.size > 0
                        ? `Apply fix (${fixSelected.size})` : "Apply fix";
                });
                label.appendChild(box);
                const body = document.createElement("div");
                body.className = "slls-bpa-fix-body";
                const name = document.createElement("span");
                name.className = "slls-bpa-fix-name";
                name.textContent = item.objectName;
                body.appendChild(name);
                const diff = document.createElement("span");
                diff.className = "slls-bpa-fix-diff";
                const before = document.createElement("span");
                before.className = "slls-bpa-fix-before";
                before.textContent = item.before || "\u2014";
                const arrow = document.createElement("span");
                arrow.textContent = " \u2192 ";
                const after = document.createElement("span");
                after.className = "slls-bpa-fix-after";
                after.textContent = item.after || "\u2014";
                diff.appendChild(before);
                diff.appendChild(arrow);
                diff.appendChild(after);
                body.appendChild(diff);
                label.appendChild(body);
                list.appendChild(label);
            }
            panel.appendChild(list);
        }

        const actions = document.createElement("div");
        actions.className = "slls-bpa-fix-actions";
        const cancelBtn = makeButton("Cancel", "slls-bpa-btn-sm");
        cancelBtn.addEventListener("click", () => {
            fixRule = null;
            fixSelected.clear();
            renderResults();
        });
        actions.appendChild(cancelBtn);
        const applyFixBtn = makeButton(
            fixSelected.size > 0 ? `Apply fix (${fixSelected.size})` : "Apply fix",
            "slls-bpa-btn-sm slls-bpa-btn-primary",
            ICON.wrench,
        );
        applyFixBtn.disabled = fixSelected.size === 0;
        applyFixBtn.addEventListener("click", () => {
            runAction("apply_fix", {
                rule_name: group.ruleName,
                object_names: [...fixSelected],
                disabled_rules: [...disabledRules],
            });
            fixRule = null;
            fixSelected.clear();
        });
        actions.appendChild(applyFixBtn);
        panel.appendChild(actions);
        return panel;
    }

    function renderGroups(violations) {
        clear(groupsWrap);
        const groups = buildGroups(violations);
        if (groups.length === 0) {
            const empty = document.createElement("div");
            empty.className = "slls-bpa-empty";
            empty.appendChild(iconSpan(ICON.check, "slls-bpa-empty-icon"));
            const text = document.createElement("div");
            text.textContent = activeViolations.length > 0
                ? "No violations match the current filters."
                : "No best practice violations were found.";
            empty.appendChild(text);
            groupsWrap.appendChild(empty);
            return;
        }

        for (const group of groups) {
            const box = document.createElement("div");
            box.className = "slls-bpa-group";

            const head = document.createElement("div");
            head.className = "slls-bpa-group-head";

            const toggle = document.createElement("button");
            toggle.type = "button";
            toggle.className = "slls-bpa-group-toggle";
            const isOpen = expandedRules.has(group.ruleName);
            toggle.setAttribute("aria-expanded", String(isOpen));
            toggle.appendChild(iconSpan(ICON.caret, `slls-bpa-caret${isOpen ? " open" : ""}`));
            toggle.appendChild(iconSpan(
                severityIcon(group.severity), severityClass(group.severity), severityTitle(group.severity)));
            toggle.appendChild(iconSpan(
                CATEGORY_ICON[group.category] || ICON.info, "slls-bpa-cat-icon", `Category: ${group.category}`));
            const name = document.createElement("span");
            name.className = "slls-bpa-group-name";
            name.textContent = group.ruleName;
            toggle.appendChild(name);
            const badge = document.createElement("span");
            badge.className = "slls-bpa-badge";
            badge.textContent = String(group.violations.length);
            badge.title = plural(group.violations.length, "violation");
            toggle.appendChild(badge);
            toggle.addEventListener("click", () => {
                if (expandedRules.has(group.ruleName)) expandedRules.delete(group.ruleName);
                else expandedRules.add(group.ruleName);
                renderResults();
            });
            head.appendChild(toggle);

            const infoBtn = document.createElement("button");
            infoBtn.type = "button";
            infoBtn.className = "slls-bpa-info-btn";
            infoBtn.innerHTML = ICON.info;
            infoBtn.setAttribute("aria-label", `About "${group.ruleName}"`);
            infoBtn.addEventListener("mouseenter", () => showPopover(infoBtn, group));
            infoBtn.addEventListener("mouseleave", scheduleHidePopover);
            infoBtn.addEventListener("focus", () => showPopover(infoBtn, group));
            infoBtn.addEventListener("blur", hidePopover);
            head.appendChild(infoBtn);

            if (group.fixable && !isBulkDrilldown) {
                const fixBtn = makeButton("Apply fix", "slls-bpa-btn-sm", ICON.wrench);
                fixBtn.title = "Preview and apply the automatic fix for this rule";
                fixBtn.addEventListener("click", () => {
                    fixRule = group.ruleName;
                    fixSelected.clear();
                    model.set("fix_preview", {});
                    model.save_changes();
                    runAction("preview_fix", { rule_name: group.ruleName });
                    renderResults();
                });
                head.appendChild(fixBtn);
            }
            box.appendChild(head);

            if (fixRule === group.ruleName) box.appendChild(renderFixPanel(group));

            if (isOpen) {
                const list = document.createElement("div");
                list.className = "slls-bpa-violations";
                for (const v of group.violations) {
                    const row = document.createElement("div");
                    row.className = "slls-bpa-violation";
                    const type = document.createElement("span");
                    type.className = "slls-bpa-otype";
                    type.textContent = v.objectType;
                    row.appendChild(type);
                    const objectName = document.createElement("span");
                    objectName.className = "slls-bpa-oname";
                    objectName.textContent = v.objectName;
                    objectName.title = v.objectName;
                    row.appendChild(objectName);
                    list.appendChild(row);
                }
                box.appendChild(list);
            }

            groupsWrap.appendChild(box);
        }
    }

    function renderResults() {
        const visible = visibleViolations();
        renderCategoryCards();
        renderObjectTypeOptions();
        renderGroups(visible);
    }

    searchInput.addEventListener("input", () => renderResults());
    expandBtn.addEventListener("click", () => {
        for (const g of buildGroups(visibleViolations())) expandedRules.add(g.ruleName);
        renderResults();
    });
    collapseBtn.addEventListener("click", () => {
        expandedRules.clear();
        renderResults();
    });

    // ------------------------------------------------------------------
    // Bulk report rendering
    // ------------------------------------------------------------------
    let isBulkDrilldown = false;

    function renderBulk() {
        const results = model.get("bulk_results") || [];
        const all = [];
        for (const r of results) for (const v of (r.violations || [])) all.push(v);
        renderCards(bulkCards, all);

        clear(bulkList);
        const maxTotal = Math.max(1, ...results.map((r) => (r.violations || []).length));
        for (const result of results) {
            const violations = result.violations || [];
            const counts = { Error: 0, Warning: 0, Info: 0 };
            for (const v of violations) if (counts[v.severity] !== undefined) counts[v.severity] += 1;

            const row = document.createElement("button");
            row.type = "button";
            row.className = "slls-bpa-bulk-row";

            const main = document.createElement("div");
            main.className = "slls-bpa-bulk-main";
            const name = document.createElement("div");
            name.className = "slls-bpa-bulk-name";
            name.textContent = result.dataset_name;
            main.appendChild(name);
            const ws = document.createElement("div");
            ws.className = "slls-bpa-bulk-ws";
            ws.textContent = result.error ? result.error : result.workspace_name;
            main.appendChild(ws);
            row.appendChild(main);

            const countsEl = document.createElement("div");
            countsEl.className = "slls-bpa-bulk-counts";
            for (const sev of SEVERITIES) {
                const span = document.createElement("span");
                span.title = plural(counts[sev], sev.toLowerCase());
                span.appendChild(iconSpan(severityIcon(sev), severityClass(sev)));
                const n = document.createElement("span");
                n.textContent = String(counts[sev]);
                span.appendChild(n);
                countsEl.appendChild(span);
            }
            row.appendChild(countsEl);

            const bar = document.createElement("div");
            bar.className = "slls-bpa-bar";
            const scale = violations.length / maxTotal;
            for (const [sev, cls] of [["Error", "e"], ["Warning", "w"], ["Info", "i"]]) {
                if (!counts[sev]) continue;
                const fill = document.createElement("i");
                fill.className = cls;
                fill.style.width = `${(counts[sev] / Math.max(violations.length, 1)) * scale * 100}%`;
                bar.appendChild(fill);
            }
            row.appendChild(bar);

            const total = document.createElement("div");
            total.className = "slls-bpa-bulk-total";
            total.textContent = result.error ? "\u2014" : String(violations.length);
            row.appendChild(total);

            row.addEventListener("click", () => {
                activeViolations = violations;
                isBulkDrilldown = true;
                resetFilters();
                // Make the drilled-into model the active one so "re-run" targets it.
                model.set("workspace_id", result.workspace_id);
                model.set("workspace_name", result.workspace_name);
                model.set("dataset_id", result.dataset_id);
                model.set("dataset_name", result.dataset_name);
                model.set("screen", "results");
                model.save_changes();
                renderScreen();
            });
            bulkList.appendChild(row);
        }
    }

    // ------------------------------------------------------------------
    // Rules panel
    // ------------------------------------------------------------------
    const overlay = document.createElement("div");
    overlay.className = "slls-bpa-overlay";
    root.appendChild(overlay);
    overlay.addEventListener("click", (ev) => { if (ev.target === overlay) overlay.classList.remove("show"); });

    function openRulesPanel() {
        clear(overlay);
        const modal = document.createElement("div");
        modal.className = "slls-bpa-modal";

        const heading = document.createElement("h2");
        heading.textContent = "Rules";
        modal.appendChild(heading);
        const sub = document.createElement("div");
        sub.className = "slls-bpa-modal-sub";
        sub.textContent = "Turn rules on or off. Disabled rules are skipped on the next run.";
        modal.appendChild(sub);

        const bar = document.createElement("div");
        bar.className = "slls-bpa-toolbar";
        bar.style.marginBottom = "12px";
        const ruleSearchWrap = document.createElement("div");
        ruleSearchWrap.className = "slls-bpa-searchwrap";
        ruleSearchWrap.appendChild(iconSpan(ICON.search, "slls-bpa-searchicon"));
        const ruleSearch = document.createElement("input");
        ruleSearch.className = "slls-bpa-input";
        ruleSearch.type = "search";
        ruleSearch.placeholder = "Search rules\u2026";
        ruleSearch.setAttribute("aria-label", "Search rules");
        ruleSearchWrap.appendChild(ruleSearch);
        bar.appendChild(ruleSearchWrap);

        const enableAll = makeButton("Enable all", "slls-bpa-btn-sm");
        const disableAll = makeButton("Disable all", "slls-bpa-btn-sm");
        bar.appendChild(enableAll);
        bar.appendChild(disableAll);
        modal.appendChild(bar);

        const list = document.createElement("div");
        list.className = "slls-bpa-rulelist";
        modal.appendChild(list);

        function renderRuleList() {
            clear(list);
            const term = ruleSearch.value.trim().toLowerCase();
            const rules = model.get("rules") || [];
            const shown = rules.filter((r) =>
                !term
                || r.name.toLowerCase().includes(term)
                || r.category.toLowerCase().includes(term));
            if (shown.length === 0) {
                const empty = document.createElement("div");
                empty.className = "slls-bpa-empty";
                empty.textContent = "No rules match the search.";
                list.appendChild(empty);
                return;
            }
            for (const rule of shown) {
                const row = document.createElement("div");
                row.className = "slls-bpa-rule";

                const toggleLabel = document.createElement("label");
                toggleLabel.className = "slls-bpa-switch";
                toggleLabel.title = `Enable or disable "${rule.name}"`;
                const box = document.createElement("input");
                box.type = "checkbox";
                box.checked = !disabledRules.has(rule.id);
                box.setAttribute("aria-label", `Enable ${rule.name}`);
                box.addEventListener("change", () => {
                    if (box.checked) disabledRules.delete(rule.id);
                    else disabledRules.add(rule.id);
                });
                toggleLabel.appendChild(box);
                toggleLabel.appendChild(document.createElement("i"));
                row.appendChild(toggleLabel);

                const body = document.createElement("div");
                body.className = "slls-bpa-rule-body";
                const name = document.createElement("div");
                name.className = "slls-bpa-rule-name";
                name.appendChild(iconSpan(
                    severityIcon(rule.severity), severityClass(rule.severity), severityTitle(rule.severity)));
                const nameText = document.createElement("span");
                nameText.textContent = rule.name;
                name.appendChild(nameText);
                if (rule.fixable) {
                    name.appendChild(iconSpan(ICON.wrench, "", "An automatic fix is available"));
                }
                body.appendChild(name);
                const meta = document.createElement("div");
                meta.className = "slls-bpa-rule-meta";
                meta.textContent = `${rule.category} \u2022 ${rule.severity} \u2022 ${rule.scopes.join(", ")}`;
                body.appendChild(meta);
                if (rule.description) {
                    const desc = document.createElement("div");
                    desc.className = "slls-bpa-rule-desc";
                    desc.textContent = rule.description;
                    body.appendChild(desc);
                }
                row.appendChild(body);
                list.appendChild(row);
            }
        }

        ruleSearch.addEventListener("input", renderRuleList);
        enableAll.addEventListener("click", () => { disabledRules.clear(); renderRuleList(); });
        disableAll.addEventListener("click", () => {
            for (const r of (model.get("rules") || [])) disabledRules.add(r.id);
            renderRuleList();
        });

        const footer = document.createElement("div");
        footer.className = "slls-bpa-modal-footer";
        const closeBtn = makeButton("Close", "");
        closeBtn.addEventListener("click", () => overlay.classList.remove("show"));
        footer.appendChild(closeBtn);
        const applyBtn = makeButton("Save and re-run", "slls-bpa-btn-primary", ICON.play);
        applyBtn.addEventListener("click", () => {
            model.set("disabled_rules", [...disabledRules]);
            model.save_changes();
            overlay.classList.remove("show");
            if (model.get("screen") !== "select") rerun();
        });
        footer.appendChild(applyBtn);
        modal.appendChild(footer);

        overlay.appendChild(modal);
        overlay.classList.add("show");
        renderRuleList();
    }

    // ------------------------------------------------------------------
    // Screen switching
    // ------------------------------------------------------------------
    function renderScreen() {
        const screen = model.get("screen") || "select";
        selectScreen.classList.toggle("show", screen === "select");
        resultsScreen.classList.toggle("show", screen === "results");
        bulkScreen.classList.toggle("show", screen === "bulk");
        backBtn.style.display = screen === "results" && isBulkDrilldown ? "" : "none";
        changeModelBtn.style.display = screen === "select" ? "none" : "";
        rerunBtn.style.display = screen === "select" ? "none" : "";

        if (screen === "select") {
            subtitle.textContent = "Scan semantic models against the best practice rules.";
            isBulkDrilldown = false;
        } else if (screen === "bulk") {
            const n = (model.get("bulk_results") || []).length;
            subtitle.textContent = `${plural(n, "semantic model")} analyzed`;
            isBulkDrilldown = false;
            renderBulk();
        } else {
            subtitle.innerHTML =
                `<b>${escapeHtml(model.get("dataset_name") || "")}</b>` +
                `<span class="slls-bpa-sep">\u2022</span>${escapeHtml(model.get("workspace_name") || "")}`;
            renderResults();
        }
    }

    function escapeHtml(s) {
        return String(s ?? "").replace(/[&<>"']/g, (c) => ({
            "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
        }[c]));
    }

    model.on("change:violations", () => {
        activeViolations = model.get("violations") || [];
        isBulkDrilldown = false;
        if (model.get("screen") === "results") renderResults();
    });
    model.on("change:bulk_results", () => {
        if (model.get("screen") === "bulk") renderBulk();
    });
    model.on("change:fix_preview", () => {
        const preview = model.get("fix_preview") || {};
        if (preview.ruleName && preview.ruleName === fixRule) {
            fixSelected.clear();
            for (const item of (preview.items || [])) fixSelected.add(item.objectName);
        }
        if (model.get("screen") === "results") renderResults();
    });
    model.on("change:screen", renderScreen);
    model.on("change:render_token", () => {
        // A completed scan always refreshes the view, even when the resulting
        // violation list happens to be identical to the previous one.
        if (model.get("screen") !== "bulk") {
            activeViolations = model.get("violations") || [];
            isBulkDrilldown = false;
        }
        renderScreen();
    });
    model.on("change:disabled_rules", () => {
        disabledRules.clear();
        for (const id of (model.get("disabled_rules") || [])) disabledRules.add(id);
    });

    renderWorkspaces();
    renderDatasets();
    renderBulkChips();
    updateSelectState();
    activeViolations = model.get("violations") || [];
    renderScreen();
}
export default { render };
"""


_WIDGET_JS = (
    _WIDGET_JS.replace("__SLLS_ICON_SHIELD_CHECK__", _UI_ICONS["shield_check"])
    .replace("__SLLS_ICON_SUN__", _UI_ICONS["sun"])
    .replace("__SLLS_ICON_MOON__", _UI_ICONS["moon"])
    .replace("__SLLS_ICON_BACK__", _UI_ICONS["back"])
    .replace("__SLLS_ICON_SWAP__", _UI_ICONS["swap"])
    .replace("__SLLS_ICON_REFRESH__", _UI_ICONS["refresh"])
    .replace("__SLLS_ICON_SEARCH__", _UI_ICONS["search"])
    .replace("__SLLS_ICON_WRENCH__", _UI_ICONS["wrench"])
    .replace("__SLLS_ICON_INFO__", _UI_ICONS["info"])
    .replace("__SLLS_ICON_ALERT__", _UI_ICONS["alert"])
    .replace("__SLLS_ICON_ERROR_CIRCLE__", _UI_ICONS["error_circle"])
    .replace("__SLLS_ICON_CHECK_CIRCLE__", _UI_ICONS["check_circle"])
    .replace("__SLLS_ICON_EXTERNAL_LINK__", _UI_ICONS["external_link"])
    .replace("__SLLS_ICON_CLOSE__", _UI_ICONS["close"])
    .replace("__SLLS_ICON_CARET_RIGHT__", _UI_ICONS["caret_right"])
    .replace("__SLLS_ICON_CHECK__", _UI_ICONS["check"])
    .replace("__SLLS_ICON_PLAY__", _UI_ICONS["play"])
    .replace("__SLLS_ICON_SETTINGS__", _UI_ICONS["settings"])
    .replace("__SLLS_ICON_ACTIVITY__", _UI_ICONS["activity"])
    .replace("__SLLS_ICON_CODE__", _UI_ICONS["code"])
    .replace("__SLLS_ICON_PENCIL__", _UI_ICONS["pencil"])
    .replace("__SLLS_ICON_TEXT_TYPE__", _UI_ICONS["text_type"])
    .replace("__SLLS_ICON_PLUS__", _UI_ICONS["plus"])
    .replace("__SLLS_ICON_EXPAND_ROWS__", _UI_ICONS["expand_rows"])
    .replace("__SLLS_ICON_COLLAPSE_ROWS__", _UI_ICONS["collapse_rows"])
    .replace("__SLLS_ICON_FULLSCREEN__", _UI_ICONS["fullscreen"])
    .replace("__SLLS_ICON_FULLSCREEN_EXIT__", _UI_ICONS["fullscreen_exit"])
)


@log
def bpa(
    dataset: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    check_dependencies: bool = True,
    dark_mode: bool = False,
):
    """
    Generates an interactive Best Practice Analyzer for semantic models.

    The analyzer scans one semantic model - or up to 10 semantic models across any
    number of workspaces - against the semantic model best practice rules, groups the
    violations by rule, and lets you preview and apply the automatic fix for the rules
    which support one.

    The Best Practice Analyzer rules are based on the rules defined `here <https://github.com/microsoft/Analysis-Services/tree/master/BestPracticeRules>`_. The framework for the Best Practice Analyzer and rules are based on the foundation set by `Tabular Editor <https://github.com/TabularEditor/TabularEditor>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID, default=None
        Name or ID of a semantic model to analyze immediately.
        Defaults to None which opens the analyzer on the model-selection screen.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    check_dependencies : bool, default=True
        If True, leverages the model dependencies from
        :func:`sempy_labs.get_model_calc_dependencies` to evaluate the rules. Set this
        parameter to False if running the rules against a semantic model in a shared
        capacity.
    dark_mode : bool, default=False
        If True, renders the analyzer with a dark color theme. If False, renders with a
        light color theme.
    """

    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "The 'best_practice_analyzer' function requires the 'anywidget' package. "
            "Install it with: pip install anywidget"
        ) from e

    import pandas as pd
    import sempy.fabric as fabric
    from IPython.display import display

    from sempy_labs._helper_functions import (
        resolve_workspace_name_and_id,
        resolve_dataset_name_and_id,
    )
    from sempy_labs._model_bpa_rules import model_bpa_rules
    from sempy_labs._model_dependencies import get_model_calc_dependencies
    from sempy_labs.semantic_model._bpa_engine import (
        apply_fixes,
        preview_fixes,
        rules_payload,
        scan_model,
    )
    from sempy_labs.tom import connect_semantic_model

    _DEPENDENCY_COLUMNS = [
        "Table Name",
        "Object Name",
        "Object Type",
        "Expression",
        "Referenced Table",
        "Referenced Object",
        "Referenced Object Type",
        "Full Object Name",
        "Referenced Full Object Name",
        "Parent Node",
    ]

    initial_ws_name, initial_ws_id = resolve_workspace_name_and_id(workspace)
    initial_ws_id = str(initial_ws_id)

    initial_ds_name = ""
    initial_ds_id = ""
    if dataset is not None:
        resolved_name, resolved_id = resolve_dataset_name_and_id(
            dataset, workspace=initial_ws_id
        )
        initial_ds_name = str(resolved_name)
        initial_ds_id = str(resolved_id)

    def _pick_columns(df, preferred_id, preferred_name):
        cols = list(df.columns)
        if not cols:
            return None, None
        id_col = next((c for c in preferred_id if c in cols), cols[0])
        name_col = next((c for c in preferred_name if c in cols), cols[-1])
        return id_col, name_col

    def _list_workspaces_payload():
        try:
            df = fabric.list_workspaces()
        except Exception:
            return [{"id": initial_ws_id, "name": str(initial_ws_name or "")}]
        id_col, name_col = _pick_columns(df, ["Id"], ["Name"])
        if id_col is None or name_col is None:
            return [{"id": initial_ws_id, "name": str(initial_ws_name or "")}]
        rows = [
            {"id": str(r[id_col]), "name": str(r[name_col])} for _, r in df.iterrows()
        ]
        rows.sort(key=lambda x: x["name"].lower())
        return rows

    def _list_datasets_payload(workspace_id):
        try:
            df = fabric.list_datasets(workspace=workspace_id, mode="rest")
        except Exception:
            return []
        id_col, name_col = _pick_columns(
            df, ["Dataset Id", "Dataset ID"], ["Dataset Name"]
        )
        if id_col is None or name_col is None:
            return []
        rows = [
            {"id": str(r[id_col]), "name": str(r[name_col])} for _, r in df.iterrows()
        ]
        rows.sort(key=lambda x: x["name"].lower())
        return rows

    def _build_rules(workspace_id, dataset_id):
        """Builds the rules dataframe, optionally including the calc-dependency graph."""

        dependencies = pd.DataFrame(columns=_DEPENDENCY_COLUMNS)
        if check_dependencies and workspace_id and dataset_id:
            try:
                dependencies = get_model_calc_dependencies(
                    dataset=dataset_id, workspace=workspace_id
                )
            except Exception:
                # Dependency discovery is unavailable on shared capacities.
                pass

        return model_bpa_rules(dependencies=dependencies)

    # Rules currently in effect for the open model; reused by the fix actions.
    state = {"rules": None}

    def _scan(workspace_id, dataset_id, disabled_rules):
        rules = _build_rules(workspace_id, dataset_id)
        state["rules"] = rules
        with connect_semantic_model(
            dataset=dataset_id, workspace=workspace_id, readonly=True
        ) as tom:
            return scan_model(tom, rules, disabled_rules)

    class _BestPracticeAnalyzerWidget(anywidget.AnyWidget):
        _esm = _WIDGET_JS
        _css = _WIDGET_CSS

        screen = traitlets.Unicode("select").tag(sync=True)
        workspaces = traitlets.List().tag(sync=True)
        datasets = traitlets.List().tag(sync=True)
        workspace_id = traitlets.Unicode("").tag(sync=True)
        workspace_name = traitlets.Unicode("").tag(sync=True)
        dataset_id = traitlets.Unicode("").tag(sync=True)
        dataset_name = traitlets.Unicode("").tag(sync=True)
        rules = traitlets.List().tag(sync=True)
        disabled_rules = traitlets.List().tag(sync=True)
        violations = traitlets.List().tag(sync=True)
        bulk_results = traitlets.List().tag(sync=True)
        fix_preview = traitlets.Dict().tag(sync=True)
        render_token = traitlets.Int(0).tag(sync=True)
        max_bulk_models = traitlets.Int(_MAX_BULK_MODELS).tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        busy = traitlets.Bool(False).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)

    widget = _BestPracticeAnalyzerWidget(
        workspaces=_list_workspaces_payload(),
        datasets=_list_datasets_payload(initial_ws_id),
        workspace_id=initial_ws_id,
        workspace_name=str(initial_ws_name or ""),
        dataset_id=initial_ds_id,
        dataset_name=initial_ds_name,
        rules=rules_payload(
            model_bpa_rules(dependencies=pd.DataFrame(columns=_DEPENDENCY_COLUMNS))
        ),
        dark_mode=bool(dark_mode),
    )

    def _handle_list_datasets(payload):
        widget.datasets = _list_datasets_payload(payload.get("workspace_id"))

    def _handle_run_scan(payload):
        workspace_id = payload.get("workspace_id") or widget.workspace_id
        dataset_id = payload.get("dataset_id") or widget.dataset_id
        if not dataset_id:
            widget.status = {
                "message": "Select a semantic model to analyze.",
                "kind": "error",
            }
            return

        widget.workspace_id = str(workspace_id)
        widget.workspace_name = str(
            payload.get("workspace_name") or widget.workspace_name
        )
        widget.dataset_id = str(dataset_id)
        widget.dataset_name = str(payload.get("dataset_name") or widget.dataset_name)

        violations = _scan(workspace_id, dataset_id, payload.get("disabled_rules"))
        widget.violations = violations
        widget.fix_preview = {}
        widget.screen = "results"
        widget.render_token += 1

    def _handle_run_bulk(payload):
        targets = payload.get("targets") or []
        if not targets:
            widget.status = {
                "message": "Add at least one semantic model to analyze.",
                "kind": "error",
            }
            return

        targets = targets[:_MAX_BULK_MODELS]
        disabled_rules = payload.get("disabled_rules")
        results = []
        for target in targets:
            entry = {
                "workspace_id": str(target.get("workspace_id") or ""),
                "workspace_name": str(target.get("workspace_name") or ""),
                "dataset_id": str(target.get("dataset_id") or ""),
                "dataset_name": str(target.get("dataset_name") or ""),
                "violations": [],
                "error": "",
            }
            try:
                entry["violations"] = _scan(
                    entry["workspace_id"], entry["dataset_id"], disabled_rules
                )
            except Exception as e:
                entry["error"] = f"Could not analyze this model: {e}"
            results.append(entry)

        widget.bulk_results = results
        widget.screen = "bulk"
        widget.render_token += 1
        total = sum(len(r["violations"]) for r in results)
        widget.status = {
            "message": f"Analyzed {len(results)} semantic model(s); {total} violation(s) found.",
            "kind": "info",
        }

    def _handle_preview_fix(payload):
        rule_name = payload.get("rule_name")
        rules = state["rules"]
        if rules is None or not rule_name:
            return
        with connect_semantic_model(
            dataset=widget.dataset_id, workspace=widget.workspace_id, readonly=True
        ) as tom:
            items = preview_fixes(tom, rules, rule_name)
        widget.fix_preview = {"ruleName": rule_name, "items": items}

    def _handle_apply_fix(payload):
        rule_name = payload.get("rule_name")
        object_names = payload.get("object_names") or []
        rules = state["rules"]
        if rules is None or not rule_name:
            return

        with connect_semantic_model(
            dataset=widget.dataset_id, workspace=widget.workspace_id, readonly=False
        ) as tom:
            applied = apply_fixes(tom, rules, rule_name, object_names)

        widget.fix_preview = {}
        widget.violations = _scan(
            widget.workspace_id, widget.dataset_id, payload.get("disabled_rules")
        )
        widget.render_token += 1
        widget.status = {
            "message": (
                f"Applied the fix for '{rule_name}' to {applied} object(s)."
                if applied
                else f"No objects were changed for '{rule_name}'."
            ),
            "kind": "success" if applied else "info",
        }

    handlers = {
        "list_datasets": _handle_list_datasets,
        "run_scan": _handle_run_scan,
        "run_bulk": _handle_run_bulk,
        "preview_fix": _handle_preview_fix,
        "apply_fix": _handle_apply_fix,
    }

    def _on_run(_change):
        payload = dict(widget.pending_action or {})
        handler = handlers.get(payload.get("action"))
        if handler is None:
            return

        widget.busy = True
        widget.status = {}
        try:
            handler(payload)
        except Exception as e:
            widget.status = {"message": f"Error: {e}", "kind": "error"}
        finally:
            widget.busy = False

    widget.observe(_on_run, names=["run"])

    if initial_ds_id:
        try:
            widget.violations = _scan(initial_ws_id, initial_ds_id, [])
            widget.screen = "results"
        except Exception as e:
            widget.status = {"message": f"Error: {e}", "kind": "error"}

    display(widget)
