from typing import Optional
from uuid import UUID
import pandas as pd
from sempy._utils._log import log

from sempy_labs._ui_components import (
    ICONS as _UI_ICONS,
    LIGHT_THEME_VARS as _UI_LIGHT_VARS,
    DARK_THEME_VARS as _UI_DARK_VARS,
    scoped_button_press_css as _ui_scoped_button_press_css,
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
    --slls-syn-keyword: #ad3da4;
    --slls-syn-string: #d12f1b;
    --slls-syn-number: #272ad8;
    --slls-syn-comment: #707f8c;
    --slls-syn-builtin: #3900a0;
    --slls-syn-fn: #4b21b0;
    --slls-syn-type: #0b4f79;
    --slls-syn-prop: #326d74;
    --slls-syn-op: #6e6e73;
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
        --slls-syn-keyword: #ff7ab2;
        --slls-syn-string: #ff8170;
        --slls-syn-number: #d9c97c;
        --slls-syn-comment: #7f8c98;
        --slls-syn-builtin: #dabaff;
        --slls-syn-fn: #b281eb;
        --slls-syn-type: #4eb0cc;
        --slls-syn-prop: #6bdfff;
        --slls-syn-op: #a5a5ac;
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
    --slls-syn-keyword: #ff7ab2;
    --slls-syn-string: #ff8170;
    --slls-syn-number: #d9c97c;
    --slls-syn-comment: #7f8c98;
    --slls-syn-builtin: #dabaff;
    --slls-syn-fn: #b281eb;
    --slls-syn-type: #4eb0cc;
    --slls-syn-prop: #6bdfff;
    --slls-syn-op: #a5a5ac;
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
.slls-bpa-titlewrap { display: flex; flex-direction: column; min-width: 0; }
.slls-bpa-head-spacer { flex: 1 1 auto; }
.slls-bpa-title { font-size: 22px; font-weight: 600; letter-spacing: -0.01em; line-height: 1.15; display: flex; align-items: center; gap: 10px; }
.slls-bpa-title .slls-bpa-title-icon { color: var(--ui-accent); display: inline-flex; flex-shrink: 0; }
.slls-bpa-title .slls-bpa-title-icon svg { width: 27px; height: 27px; stroke-width: 1.5; }
.slls-bpa-subtitle { font-size: 12.5px; color: var(--ui-text-secondary); margin-top: 3px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 640px; }
.slls-bpa-subtitle b { color: var(--ui-text); font-weight: 500; }
.slls-bpa-subtitle .slls-bpa-sep { color: var(--ui-text-tertiary); margin: 0 6px; }

/* ---------------- Controls ---------------- */
.slls-bpa-input {
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
.slls-bpa-input:hover { border-color: var(--ui-text-tertiary); }
.slls-bpa-input:focus { outline: none; border-color: var(--ui-accent); box-shadow: 0 0 0 3px var(--ui-accent-soft); }
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
.slls-bpa-btn-sm.slls-bpa-btn-icon-sm { width: 30px; height: 30px; padding: 0; justify-content: center; border-radius: 7px; }
/* Anything that offers or advertises an automatic fix. */
.slls-bpa-btn-fix { color: var(--slls-success); border-color: transparent; background: var(--slls-success-soft); }
.slls-bpa-btn-fix:hover { color: var(--slls-success); border-color: var(--slls-success); background: var(--slls-success-soft); }
/* Destructive confirmations (e.g. discarding the staged fixes). */
.slls-bpa-btn-danger { color: var(--slls-error); border-color: transparent; background: var(--slls-error-soft); }
.slls-bpa-btn-danger:hover { color: var(--slls-error); border-color: var(--slls-error); background: var(--slls-error-soft); }
.slls-bpa-fix-badge { display: inline-flex; align-items: center; gap: 5px; flex-shrink: 0; cursor: default;
    padding: 3px 9px; border-radius: 7px; font-size: 11.5px; font-weight: 500;
    color: var(--slls-success); background: var(--slls-success-soft); }

.slls-bpa-toolbar { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.slls-bpa-section { border: 1px solid var(--ui-border); border-radius: var(--slls-radius); background: var(--ui-surface); padding: 16px; margin-top: 14px; }
.slls-bpa-section h3 { margin: 0 0 12px 0; font-size: 14px; font-weight: 600; display: flex; align-items: center; gap: 8px; }
.slls-bpa-section h3 .slls-bpa-count { color: var(--ui-text-tertiary); font-weight: 400; font-size: 12.5px; }
.slls-bpa-hint { font-size: 12.5px; color: var(--ui-text-tertiary); margin: 8px 2px 0 2px; }

.slls-bpa-segmented { display: inline-flex; background: var(--ui-bg-secondary); border-radius: 999px; padding: 3px; gap: 2px; }
.slls-bpa-segmented button { appearance: none; border: none; background: transparent; color: var(--ui-text-secondary); font-family: inherit; font-size: 13px; font-weight: 500;
    padding: 6px 14px; border-radius: 999px; cursor: pointer; transition: background 120ms ease, color 120ms ease; }
.slls-bpa-segmented button.active { background: var(--ui-bg-solid); color: var(--ui-text); box-shadow: var(--ui-shadow-sm); }

/* ---------------- Category summary cards (clickable filters) ---------------- */
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

/* ---------------- Searchable select (workspace / model pickers) ---------------- */
.slls-bpa-field { display: flex; flex-direction: column; gap: 5px; min-width: 0; }
.slls-bpa-field-label { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.6px;
    color: var(--ui-text-tertiary); padding-left: 4px; }
.slls-bpa-ss { position: relative; display: flex; }
.slls-bpa-ss-btn { appearance: none; width: 100%; background: var(--ui-surface); border: 1px solid var(--ui-border-strong);
    border-radius: 999px; padding: 7px 12px 7px 15px; font-size: 13.5px; font-family: inherit; color: var(--ui-text);
    cursor: pointer; display: inline-flex; align-items: center; gap: 8px; transition: border-color 120ms ease, box-shadow 120ms ease; }
.slls-bpa-ss-btn:hover:not(:disabled) { border-color: var(--ui-text-tertiary); }
.slls-bpa-ss-btn:focus-visible { outline: none; border-color: var(--ui-accent); box-shadow: 0 0 0 3px var(--ui-accent-soft); }
.slls-bpa-ss-btn:disabled { opacity: 0.5; cursor: not-allowed; }
.slls-bpa-ss-value { flex: 1; min-width: 0; text-align: left; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-bpa-ss-value.placeholder { color: var(--ui-text-tertiary); }
.slls-bpa-ss-caret { display: inline-flex; color: var(--ui-text-tertiary); transform: rotate(90deg); transition: transform 140ms ease; }
.slls-bpa-ss.open .slls-bpa-ss-caret { transform: rotate(-90deg); }
.slls-bpa-ss-panel { display: none; position: absolute; top: calc(100% + 6px); left: 0; right: 0; z-index: 70; min-width: 240px;
    padding: 6px; background: var(--ui-bg-solid); border: 1px solid var(--ui-border); border-radius: 12px; box-shadow: var(--ui-shadow-lg); }
.slls-bpa-ss.open .slls-bpa-ss-panel { display: block; }
.slls-bpa-ss-searchwrap { position: relative; display: flex; align-items: center; margin-bottom: 5px; }
.slls-bpa-ss-searchwrap .slls-bpa-ss-searchicon { position: absolute; left: 11px; color: var(--ui-text-tertiary); display: inline-flex; pointer-events: none; }
.slls-bpa-ss-search { width: 100%; appearance: none; background: var(--ui-bg-secondary); border: 1px solid transparent; border-radius: 8px;
    padding: 7px 10px 7px 31px; font-size: 13px; font-family: inherit; color: var(--ui-text); }
.slls-bpa-ss-search::placeholder { color: var(--ui-text-tertiary); }
.slls-bpa-ss-search:focus { outline: none; border-color: var(--ui-accent); }
.slls-bpa-ss-list { max-height: 240px; overflow-y: auto; }
.slls-bpa-ss-opt { display: block; width: 100%; padding: 7px 10px; border: none; background: transparent; color: var(--ui-text);
    font-family: inherit; font-size: 13px; text-align: left; border-radius: 7px; cursor: pointer;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-bpa-ss-opt:hover, .slls-bpa-ss-opt.active { background: var(--ui-surface-2); }
.slls-bpa-ss-opt.selected { color: var(--ui-accent); font-weight: 500; }
.slls-bpa-ss-empty { padding: 9px 10px; font-size: 12.5px; color: var(--ui-text-tertiary); }

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
.slls-bpa-fix-item { flex: 0 0 auto; display: flex; align-items: flex-start; gap: 9px; padding: 6px 8px; border-radius: 6px; font-size: 12.5px; cursor: pointer; }
.slls-bpa-fix-item:hover { background: var(--ui-surface-2); }
.slls-bpa-fix-item input { margin-top: 3px; flex-shrink: 0; }
.slls-bpa-fix-body { min-width: 0; flex: 1; }
.slls-bpa-fix-name { display: block; font-weight: 500; word-break: break-all; }
.slls-bpa-fix-diff { display: block; color: var(--ui-text-tertiary); word-break: break-all; margin-top: 2px; font-size: 11.5px;
    font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace; }
.slls-bpa-fix-before { color: var(--slls-error); text-decoration: line-through; }
.slls-bpa-fix-after { color: var(--slls-success); }
.slls-bpa-fix-actions { display: flex; justify-content: flex-end; gap: 8px; }

/* ---------------- Staged fixes ---------------- */
/* A compact pill directly under the header rather than a full-width bar. */
.slls-bpa-savebar { display: none; align-items: center; gap: 10px; margin-bottom: 14px; padding: 6px 7px 6px 14px;
    width: fit-content; max-width: 100%; border-radius: 999px;
    background: var(--slls-warning-soft); border: 1px solid var(--slls-warning); color: var(--ui-text); }
.slls-bpa-savebar.show { display: inline-flex; }
.slls-bpa-savebar-label { min-width: 0; display: flex; align-items: center; gap: 9px; font-size: 13px; white-space: nowrap; }
.slls-bpa-pending-dot { width: 9px; height: 9px; border-radius: 50%; background: var(--slls-warning); flex-shrink: 0;
    box-shadow: 0 0 0 3px var(--slls-warning-soft); }
.slls-bpa-savebar-review { appearance: none; border: none; background: transparent; color: var(--ui-text); font-family: inherit;
    font-size: 12.5px; font-weight: 500; text-decoration: underline; cursor: pointer; padding: 0; }
.slls-bpa-modal.slls-bpa-staged-modal { max-width: 640px; }
.slls-bpa-staged { border: 1px solid var(--ui-border); border-radius: var(--slls-radius-sm);
    background: var(--ui-bg-solid); max-height: 55vh; overflow-y: auto; }
.slls-bpa-staged-row { display: flex; align-items: center; gap: 10px; padding: 8px 12px; border-bottom: 1px solid var(--ui-border); font-size: 12.5px; }
.slls-bpa-staged-row:last-child { border-bottom: none; }
.slls-bpa-staged-main { flex: 1; min-width: 0; }
.slls-bpa-staged-rule { font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-bpa-staged-obj { color: var(--ui-text-tertiary); font-size: 11.5px; margin-top: 1px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-bpa-staged-row button { appearance: none; border: none; background: transparent; color: var(--ui-text-tertiary); cursor: pointer;
    display: inline-flex; padding: 3px; border-radius: 6px; flex-shrink: 0; transition: color 120ms ease, background 120ms ease; }
.slls-bpa-staged-row button:hover { color: var(--slls-error); background: var(--slls-error-soft); }
.slls-bpa-group.staged .slls-bpa-group-name { color: var(--ui-text-tertiary); }

/* ---------------- Rule change history ---------------- */
.slls-bpa-history-row { display: flex; align-items: center; gap: 10px; padding: 8px 12px;
    border-bottom: 1px solid var(--ui-border); font-size: 12.5px; }
.slls-bpa-history-row:last-child { border-bottom: none; }
.slls-bpa-history-index { flex-shrink: 0; min-width: 22px; text-align: right;
    color: var(--ui-text-tertiary); font-variant-numeric: tabular-nums; }
.slls-bpa-history-main { flex: 1; min-width: 0; }
.slls-bpa-history-label { font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-bpa-history-time { color: var(--ui-text-tertiary); font-size: 11.5px; margin-top: 1px;
    font-variant-numeric: tabular-nums; }
.slls-bpa-history-latest { flex-shrink: 0; padding: 2px 9px; border-radius: 7px; font-size: 11.5px;
    font-weight: 500; color: var(--ui-accent); background: var(--ui-accent-soft); }

/* ---------------- Bulk report ---------------- */
.slls-bpa-bulk-list { display: flex; flex-direction: column; gap: 8px; margin-top: 12px; }
.slls-bpa-bulk-row { display: flex; align-items: center; gap: 12px; border: 1px solid var(--ui-border); border-radius: var(--slls-radius-sm);
    background: var(--ui-bg-solid); padding: 11px 14px; cursor: pointer; font-family: inherit; text-align: left; color: inherit;
    transition: background 120ms ease, border-color 120ms ease; }
.slls-bpa-bulk-row:hover { background: var(--ui-surface-2); border-color: var(--ui-border-strong); }
.slls-bpa-bulk-row.active { border-color: var(--ui-accent); box-shadow: 0 0 0 1px var(--ui-accent); }
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

/* Inline detail: the violations of the model selected in the comparison report. */
.slls-bpa-detail { display: none; margin-top: 18px; padding-top: 16px; border-top: 1px solid var(--ui-border); }
.slls-bpa-detail.show { display: block; }
.slls-bpa-detail-head { display: flex; align-items: center; justify-content: flex-end; gap: 8px; margin-bottom: 12px; }

/* Progress while an analysis runs. */
.slls-bpa-progress { display: none; margin-top: 14px; margin-bottom: 6px; padding: 12px 14px;
    border: 1px solid var(--ui-border); border-radius: var(--slls-radius-sm); background: var(--ui-bg-tertiary); }
.slls-bpa-progress.show { display: block; }
.slls-bpa-progress-head { display: flex; align-items: center; gap: 10px; margin-bottom: 9px; font-size: 12.5px; }
.slls-bpa-progress-label { flex: 1; min-width: 0; color: var(--ui-text-secondary); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-bpa-progress-count { flex-shrink: 0; font-weight: 600; color: var(--ui-text); font-variant-numeric: tabular-nums; }
.slls-bpa-progress-cancel { flex-shrink: 0; }
.slls-bpa-progress-track { height: 6px; border-radius: 999px; background: var(--ui-bg-secondary); overflow: hidden; }
.slls-bpa-progress-fill { height: 100%; width: 0%; border-radius: 999px; background: var(--ui-accent); transition: width 220ms ease; }
/* Used for a single-model run, where there are no steps to count. */
.slls-bpa-progress-fill.slls-bpa-progress-pending { width: 35%; transition: none; animation: slls-bpa-sweep 1.2s ease-in-out infinite; }
@keyframes slls-bpa-sweep { 0% { margin-left: -35%; } 100% { margin-left: 100%; } }

/* ---------------- Multi-model picker (workspace / model tree) ----------------
   The list scrolls, so every row must opt out of flex shrinking - otherwise the
   cards are squashed to fit the container's max-height and become unreadable. */
.slls-bpa-tree { display: flex; flex-direction: column; gap: 8px; margin-top: 12px; max-height: 460px; overflow-y: auto; padding-right: 2px; }
.slls-bpa-tree-ws { flex: 0 0 auto; border: 1px solid var(--ui-border); border-radius: var(--slls-radius-sm); background: var(--ui-bg-solid); overflow: hidden; }
.slls-bpa-tree-head { display: flex; align-items: center; gap: 10px; width: 100%; min-height: 44px; padding: 11px 14px; background: transparent; border: none;
    color: var(--ui-text); font-family: inherit; font-size: 13.5px; font-weight: 600; line-height: 1.35; text-align: left; cursor: pointer; transition: background 120ms ease; }
.slls-bpa-tree-head:hover { background: var(--ui-surface-2); }
.slls-bpa-tree-caret { display: inline-flex; color: var(--ui-text-tertiary); flex-shrink: 0; transition: transform 140ms ease; }
.slls-bpa-tree-caret.open { transform: rotate(90deg); }
.slls-bpa-tree-name { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-bpa-tree-count { flex-shrink: 0; background: var(--ui-accent-soft); color: var(--ui-accent); border-radius: 999px;
    padding: 1px 9px; font-size: 11.5px; font-weight: 500; font-variant-numeric: tabular-nums; }
.slls-bpa-tree-models { border-top: 1px solid var(--ui-border); }
.slls-bpa-tree-model { display: flex; align-items: center; gap: 11px; min-height: 40px; padding: 8px 14px 8px 36px;
    border-bottom: 1px solid var(--ui-border); font-size: 13px; line-height: 1.35; }
.slls-bpa-tree-model:last-child { border-bottom: none; }
.slls-bpa-tree-model.selected { background: var(--ui-accent-soft); }
.slls-bpa-tree-modelname { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; cursor: pointer; }
.slls-bpa-tree-msg { padding: 11px 14px 11px 36px; font-size: 12.5px; color: var(--ui-text-tertiary); }
.slls-bpa-selcount { font-weight: 600; color: var(--ui-text); }

/* Summary of the models picked for a multi-model scan. */
.slls-bpa-selected { display: none; margin-top: 12px; padding: 11px 13px; border: 1px solid var(--ui-border);
    border-radius: var(--slls-radius-sm); background: var(--ui-bg-tertiary); }
.slls-bpa-selected.show { display: block; }
.slls-bpa-selected-head { display: flex; align-items: center; gap: 10px; margin-bottom: 9px; }
.slls-bpa-selected-title { flex: 1; min-width: 0; font-size: 11px; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.6px; color: var(--ui-text-tertiary); }
.slls-bpa-selected-clear { appearance: none; border: none; background: transparent; color: var(--ui-accent);
    font-family: inherit; font-size: 12px; font-weight: 500; padding: 2px 6px; border-radius: 6px; cursor: pointer; }
.slls-bpa-selected-clear:hover { background: var(--ui-surface-2); }
.slls-bpa-selected-list { display: flex; flex-wrap: wrap; gap: 7px; }
.slls-bpa-selchip { display: inline-flex; align-items: center; gap: 6px; max-width: 380px; padding: 4px 6px 4px 12px;
    border-radius: 999px; background: var(--ui-accent-soft); color: var(--ui-accent); font-size: 12.5px; line-height: 1.5; }
.slls-bpa-selchip-text { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-bpa-selchip-model { font-weight: 600; }
.slls-bpa-selchip-sep { opacity: 0.5; margin: 0 5px; }
.slls-bpa-selchip-ws { opacity: 0.85; }
.slls-bpa-selchip button { appearance: none; border: none; background: transparent; color: inherit; cursor: pointer;
    display: inline-flex; padding: 2px; border-radius: 50%; opacity: 0.6; flex-shrink: 0; transition: opacity 120ms ease; }
.slls-bpa-selchip button:hover { opacity: 1; }

/* ---------------- Rules panel (overlay) ----------------
   Fixed to the viewport (not the widget) so the panel is always visible at the
   top of the screen, however tall the results list is or how far it is scrolled.
   The z-index sits above the full-screen overlay and the rule-info popover. */
.slls-bpa-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.45); z-index: 2147483002;
    align-items: flex-start; justify-content: center; padding: 24px 16px; overflow-y: auto; }
.slls-bpa-overlay.show { display: flex; }
/* A modal opened from another modal (e.g. the rule change history, opened from
   the rule editor) has to sit above it. */
.slls-bpa-overlay-top { z-index: 2147483004; }
.slls-bpa-modal { background: var(--ui-bg-solid); color: var(--ui-text); border: 1px solid var(--ui-border); border-radius: var(--slls-radius);
    box-shadow: var(--ui-shadow-lg); width: 100%; max-width: 1040px; padding: 22px 24px; margin: 0 auto; }
.slls-bpa-modal h2 { margin: 0 0 4px 0; font-size: 17px; font-weight: 600; display: flex; align-items: center; gap: 9px; }
.slls-bpa-modal h2 .slls-bpa-icon { color: var(--ui-accent); }
.slls-bpa-modal h2 .slls-bpa-icon svg { width: 18px; height: 18px; }
.slls-bpa-modal-sub { font-size: 12.5px; color: var(--ui-text-secondary); margin-bottom: 14px; }
.slls-bpa-modal-footer { display: flex; justify-content: flex-end; gap: 8px; margin-top: 16px; }
.slls-bpa-rulelist { max-height: 68vh; min-height: 320px; overflow-y: auto; border: 1px solid var(--ui-border); border-radius: var(--slls-radius-sm); }
.slls-bpa-rule-count { font-size: 11.5px; color: var(--ui-text-tertiary); margin-bottom: 7px; }

/* Problems reported by an imported ruleset. */
.slls-bpa-issues { display: none; margin-bottom: 12px; padding: 10px 12px; border-radius: var(--slls-radius-sm); font-size: 12.5px; }
.slls-bpa-issues.show { display: block; }
.slls-bpa-issues.error { background: var(--slls-error-soft); color: var(--slls-error); }
.slls-bpa-issues.warning { background: var(--slls-warning-soft); color: var(--slls-warning); }
.slls-bpa-issues-head { display: flex; align-items: center; gap: 8px; font-weight: 600; }
.slls-bpa-issues-title { flex: 1; min-width: 0; }
.slls-bpa-issues-dismiss { appearance: none; border: none; background: transparent; color: inherit; cursor: pointer;
    display: inline-flex; padding: 2px; border-radius: 6px; opacity: 0.7; flex-shrink: 0; }
.slls-bpa-issues-dismiss:hover { opacity: 1; }
.slls-bpa-issues-list { margin: 8px 0 0 0; padding-left: 20px; max-height: 170px; overflow-y: auto; color: var(--ui-text); }
.slls-bpa-issues-list li { margin-bottom: 3px; line-height: 1.45; }
.slls-bpa-rule { display: flex; align-items: flex-start; gap: 10px; padding: 9px 12px; border-bottom: 1px solid var(--ui-border); }
.slls-bpa-rule:last-child { border-bottom: none; }
.slls-bpa-rule-body { min-width: 0; flex: 1; }
.slls-bpa-rule-name { font-size: 13px; font-weight: 500; display: flex; align-items: center; gap: 7px; }
.slls-bpa-rule-meta { font-size: 11.5px; color: var(--ui-text-tertiary); margin-top: 2px; }
.slls-bpa-rule-desc { font-size: 12px; color: var(--ui-text-secondary); margin-top: 4px; line-height: 1.45; }
.slls-bpa-rule-exprtoggle { display: inline-flex; align-items: center; gap: 4px; margin-top: 6px; padding: 2px 6px 2px 2px;
    background: none; border: none; border-radius: 5px; cursor: pointer; font: inherit; font-size: 11.5px;
    color: var(--ui-text-tertiary); transition: color 140ms ease, background 140ms ease; }
.slls-bpa-rule-exprtoggle:hover { color: var(--ui-text); background: var(--ui-bg-secondary); }
.slls-bpa-rule-expr { display: flex; align-items: flex-start; gap: 6px; margin-top: 5px; padding: 5px 8px; border-radius: 6px;
    background: var(--ui-bg-secondary); color: var(--ui-text-secondary); font-size: 11.5px; line-height: 1.5;
    white-space: pre-wrap; word-break: break-word; overflow-x: auto;
    font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace; }
.slls-bpa-rule-expr > .slls-bpa-icon { flex-shrink: 0; margin-top: 2px; }
.slls-bpa-rule-expr > code { flex: 1; min-width: 0; font: inherit; white-space: pre-wrap; word-break: break-word; }
.slls-bpa-rule-fixexpr { color: var(--slls-success); background: var(--slls-success-soft); }
.slls-bpa-syn-kw { color: var(--slls-syn-keyword); }
.slls-bpa-syn-string { color: var(--slls-syn-string); }
.slls-bpa-syn-number { color: var(--slls-syn-number); }
.slls-bpa-syn-comment { color: var(--slls-syn-comment); font-style: italic; }
.slls-bpa-syn-builtin { color: var(--slls-syn-builtin); }
.slls-bpa-syn-fn { color: var(--slls-syn-fn); }
.slls-bpa-syn-type { color: var(--slls-syn-type); }
.slls-bpa-syn-prop { color: var(--slls-syn-prop); }
.slls-bpa-syn-op { color: var(--slls-syn-op); }
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
/* While a scan runs the content dims, but the widget's own background must stay
   opaque - otherwise the page behind shows through in full-screen mode. The
   view-only controls (theme, full screen) stay clickable throughout. */
.slls-bpa-busy { pointer-events: none; }
.slls-bpa-busy > *:not(.slls-bpa-overlay):not(.slls-bpa-progress) { opacity: 0.55; transition: opacity 120ms ease; }
/* The progress panel keeps its cancel button live while everything else is inert. */
.slls-bpa-busy .slls-bpa-progress { pointer-events: auto; }
.slls-bpa-busy .slls-bpa-view-btn { pointer-events: auto; }
.slls-bpa-screen { display: none; }
.slls-bpa-screen.show { display: block; }
/* Reserve vertical room under the pickers so their dropdowns have somewhere to
   open without pushing past the bottom of the notebook output area. */
.slls-bpa-screen.slls-bpa-screen-select.show { min-height: 420px; }
.slls-bpa-attribution { margin-top: 18px; text-align: right; font-size: 11.5px; color: var(--ui-text-tertiary); }
.slls-bpa-attribution a { color: var(--ui-text-tertiary); text-decoration: none; transition: color 120ms ease; }
.slls-bpa-attribution a:hover { color: var(--ui-accent); }
.slls-bpa-searchwrap { position: relative; display: inline-flex; align-items: center; }
.slls-bpa-searchwrap .slls-bpa-searchicon { position: absolute; left: 12px; color: var(--ui-text-tertiary); display: inline-flex; pointer-events: none; }
.slls-bpa-searchwrap .slls-bpa-input { padding-left: 32px; min-width: 240px; }
"""
)
_WIDGET_CSS += _ui_scoped_button_press_css(".slls-bpa")


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
        swap: `__SLLS_ICON_SWAP__`,
        refresh: `__SLLS_ICON_REFRESH__`,
        search: `__SLLS_ICON_SEARCH__`,
        wand: `__SLLS_ICON_WAND__`,
        eye: `__SLLS_ICON_EYE__`,
        eyeOff: `__SLLS_ICON_EYE_OFF__`,
        save: `__SLLS_ICON_SAVE__`,
        undo: `__SLLS_ICON_UNDO__`,
        redo: `__SLLS_ICON_REDO__`,
        history: `__SLLS_ICON_HISTORY__`,
        reset: `__SLLS_ICON_RESET__`,
        upload: `__SLLS_ICON_UPLOAD__`,
        download: `__SLLS_ICON_DOWNLOAD__`,
        info: `__SLLS_ICON_INFO__`,
        alert: `__SLLS_ICON_ALERT__`,
        error: `__SLLS_ICON_ERROR_CIRCLE__`,
        checkCircle: `__SLLS_ICON_CHECK_CIRCLE__`,
        external: `__SLLS_ICON_EXTERNAL_LINK__`,
        close: `__SLLS_ICON_CLOSE__`,
        caret: `__SLLS_ICON_CARET_RIGHT__`,
        check: `__SLLS_ICON_CHECK__`,
        play: `__SLLS_ICON_PLAY__`,
        settings: `__SLLS_ICON_SETTINGS__`,
        sliders: `__SLLS_ICON_SLIDERS__`,
        activity: `__SLLS_ICON_ACTIVITY__`,
        code: `__SLLS_ICON_CODE__`,
        pencil: `__SLLS_ICON_PENCIL__`,
        text: `__SLLS_ICON_TEXT_TYPE__`,
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
    const CATEGORY_ORDER = {
        "Performance": 0,
        "Error Prevention": 1,
        "DAX Expressions": 2,
        "Maintenance": 3,
        "Formatting": 4,
        "Naming Conventions": 5,
    };
    function categoryRank(category) {
        const rank = CATEGORY_ORDER[category];
        return rank === undefined ? 99 : rank;
    }

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

    // ------------------------------------------------------------------
    // Minimal Python syntax highlighter, used to colour the rule
    // expressions shown in the rule editor.
    // ------------------------------------------------------------------
    const PY_KEYWORDS = new Set([
        "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class",
        "continue", "def", "del", "elif", "else", "except", "finally", "for", "from", "global",
        "if", "import", "in", "is", "lambda", "nonlocal", "not", "or", "pass", "raise",
        "return", "try", "while", "with", "yield"]);
    const PY_BUILTINS = new Set([
        "abs", "all", "any", "bool", "dict", "enumerate", "float", "getattr", "int", "isinstance",
        "len", "list", "max", "min", "range", "round", "set", "sorted", "str", "sum", "tuple", "zip"]);
    // Order matters: comments and strings are matched before identifiers so that a
    // string prefix such as the `r` of r"..." is not mistaken for a name.
    const PY_TOKEN_RE = new RegExp([
        "(#[^\\n]*)",
        "([rRbBfFuU]{0,2}(?:'''[\\s\\S]*?'''|\"\"\"[\\s\\S]*?\"\"\"|'(?:\\\\.|[^'\\\\\\n])*'|\"(?:\\\\.|[^\"\\\\\\n])*\"))",
        "(\\b\\d[\\d_]*(?:\\.\\d+)?\\b)",
        "([A-Za-z_][A-Za-z0-9_]*)",
        "([+\\-*/%=<>!&|^~]+)",
    ].join("|"), "g");

    function highlightPython(code) {
        const frag = document.createDocumentFragment();
        const push = (text, cls) => {
            if (!text) return;
            if (!cls) { frag.appendChild(document.createTextNode(text)); return; }
            const span = document.createElement("span");
            span.className = cls;
            span.textContent = text;
            frag.appendChild(span);
        };
        let last = 0;
        let m;
        PY_TOKEN_RE.lastIndex = 0;
        while ((m = PY_TOKEN_RE.exec(code)) !== null) {
            push(code.slice(last, m.index), "");
            last = PY_TOKEN_RE.lastIndex;
            const text = m[0];
            if (m[1]) { push(text, "slls-bpa-syn-comment"); }
            else if (m[2]) { push(text, "slls-bpa-syn-string"); }
            else if (m[3]) { push(text, "slls-bpa-syn-number"); }
            else if (m[4]) {
                const before = code.slice(0, m.index).replace(/\s+$/, "");
                const after = code.slice(last).replace(/^\s+/, "");
                const isCall = after.startsWith("(");
                if (PY_KEYWORDS.has(text)) push(text, "slls-bpa-syn-kw");
                else if (before.endsWith(".")) push(text, isCall ? "slls-bpa-syn-fn" : "slls-bpa-syn-prop");
                else if (PY_BUILTINS.has(text)) push(text, "slls-bpa-syn-builtin");
                else if (isCall) push(text, "slls-bpa-syn-fn");
                else if (/^[A-Z]/.test(text)) push(text, "slls-bpa-syn-type");
                else push(text, "");
            } else {
                push(text, "slls-bpa-syn-op");
            }
        }
        push(code.slice(last), "");
        return frag;
    }

    function codeBlock(code, cls, title, icon) {
        const wrap = document.createElement("div");
        wrap.className = cls;
        if (icon) wrap.appendChild(iconSpan(icon));
        const el = document.createElement("code");
        el.appendChild(highlightPython(code));
        wrap.appendChild(el);
        if (title) wrap.title = title;
        return wrap;
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

    // Searchable single-select. `options` are `{ value, label }` descriptors.
    // Returns a controller exposing the current value plus option management.
    // The option list never grows past this, and shrinks further when the space
    // below the picker is tight.
    const MAX_LIST_HEIGHT = 240;
    const MIN_LIST_HEIGHT = 120;
    function createSearchSelect(placeholder, searchPlaceholder, ariaLabel, emptyLabel, onChange) {
        const wrap = document.createElement("div");
        wrap.className = "slls-bpa-ss";

        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "slls-bpa-ss-btn";
        btn.setAttribute("aria-haspopup", "listbox");
        btn.setAttribute("aria-label", ariaLabel);
        const valueLabel = document.createElement("span");
        valueLabel.className = "slls-bpa-ss-value";
        btn.appendChild(valueLabel);
        btn.appendChild(iconSpan(ICON.caret, "slls-bpa-ss-caret"));
        wrap.appendChild(btn);

        const panel = document.createElement("div");
        panel.className = "slls-bpa-ss-panel";
        const searchWrap = document.createElement("div");
        searchWrap.className = "slls-bpa-ss-searchwrap";
        searchWrap.appendChild(iconSpan(ICON.search, "slls-bpa-ss-searchicon"));
        const search = document.createElement("input");
        search.className = "slls-bpa-ss-search";
        search.type = "search";
        search.placeholder = searchPlaceholder;
        search.setAttribute("aria-label", searchPlaceholder);
        searchWrap.appendChild(search);
        panel.appendChild(searchWrap);
        const list = document.createElement("div");
        list.className = "slls-bpa-ss-list";
        list.setAttribute("role", "listbox");
        panel.appendChild(list);
        wrap.appendChild(panel);

        let options = [];
        let value = "";
        // Index into `shown` (the currently filtered options) for keyboard navigation.
        let activeIndex = -1;
        let shown = [];

        function close() {
            wrap.classList.remove("open");
            btn.setAttribute("aria-expanded", "false");
            activeIndex = -1;
        }
        function open() {
            for (const other of openDropdowns) if (other !== close) other();
            wrap.classList.add("open");
            btn.setAttribute("aria-expanded", "true");
            search.value = "";
            // Always drops downward; the list is capped to whatever room is left
            // below the picker so it scrolls instead of overflowing the widget.
            const rect = btn.getBoundingClientRect();
            const room = window.innerHeight - rect.bottom - 70;
            list.style.maxHeight =
                `${Math.max(MIN_LIST_HEIGHT, Math.min(MAX_LIST_HEIGHT, room))}px`;
            activeIndex = -1;
            renderList();
            setActive(shown.findIndex((o) => o.value === value));
            search.focus();
        }
        openDropdowns.add(close);

        function selectedOption() {
            return options.find((o) => o.value === value) || null;
        }
        function renderValue() {
            const option = selectedOption();
            const text = option ? option.label : (options.length === 0 ? emptyLabel : placeholder);
            valueLabel.textContent = text;
            valueLabel.classList.toggle("placeholder", !option);
            valueLabel.title = option ? option.label : "";
            btn.disabled = options.length === 0;
        }
        function setActive(index) {
            const rows = list.querySelectorAll(".slls-bpa-ss-opt");
            if (rows.length === 0) { activeIndex = -1; return; }
            activeIndex = Math.max(0, Math.min(index, rows.length - 1));
            rows.forEach((row, i) => row.classList.toggle("active", i === activeIndex));
            rows[activeIndex].scrollIntoView({ block: "nearest" });
        }
        function commit(option) {
            const changed = value !== option.value;
            value = option.value;
            renderValue();
            close();
            btn.focus();
            if (changed) onChange(option);
        }
        function renderList() {
            clear(list);
            const term = search.value.trim().toLowerCase();
            shown = term
                ? options.filter((o) => o.label.toLowerCase().includes(term))
                : options;
            if (shown.length === 0) {
                const empty = document.createElement("div");
                empty.className = "slls-bpa-ss-empty";
                empty.textContent = options.length === 0 ? "No items" : "No matches";
                list.appendChild(empty);
                activeIndex = -1;
                return;
            }
            for (const option of shown) {
                const row = document.createElement("button");
                row.type = "button";
                row.tabIndex = -1;
                row.className = "slls-bpa-ss-opt" + (option.value === value ? " selected" : "");
                row.setAttribute("role", "option");
                row.setAttribute("aria-selected", String(option.value === value));
                row.textContent = option.label;
                row.title = option.label;
                row.addEventListener("click", () => commit(option));
                list.appendChild(row);
            }
            if (activeIndex >= 0) setActive(activeIndex);
        }

        btn.addEventListener("click", (ev) => {
            ev.stopPropagation();
            if (wrap.classList.contains("open")) close();
            else open();
        });
        btn.addEventListener("keydown", (ev) => {
            if (ev.key === "ArrowDown" || ev.key === "ArrowUp") {
                ev.preventDefault();
                if (!wrap.classList.contains("open")) open();
            }
        });
        panel.addEventListener("click", (ev) => ev.stopPropagation());
        search.addEventListener("input", () => { activeIndex = -1; renderList(); setActive(0); });
        search.addEventListener("keydown", (ev) => {
            if (ev.key === "ArrowDown") {
                ev.preventDefault();
                setActive(activeIndex + 1);
            } else if (ev.key === "ArrowUp") {
                ev.preventDefault();
                setActive(activeIndex <= 0 ? 0 : activeIndex - 1);
            } else if (ev.key === "Home") {
                ev.preventDefault();
                setActive(0);
            } else if (ev.key === "End") {
                ev.preventDefault();
                setActive(shown.length - 1);
            } else if (ev.key === "Enter") {
                ev.preventDefault();
                if (activeIndex >= 0 && shown[activeIndex]) commit(shown[activeIndex]);
            } else if (ev.key === "Escape") {
                ev.stopPropagation();
                ev.preventDefault();
                close();
                btn.focus();
            } else if (ev.key === "Tab") {
                // Collapse first so Tab continues on to the next picker / button
                // rather than stepping through the option list.
                ev.preventDefault();
                close();
                btn.focus();
            }
        });

        renderValue();

        return {
            el: wrap,
            get value() { return value; },
            get label() { const o = selectedOption(); return o ? o.label : ""; },
            setOptions(next, nextValue) {
                options = next;
                if (nextValue !== undefined) value = nextValue;
                if (!options.some((o) => o.value === value)) value = "";
                renderValue();
                renderList();
            },
            setEmptyLabel(text) {
                emptyLabel = text;
                renderValue();
                renderList();
            },
        };
    }

    // Set while a scan is the action in flight, so that quick background calls
    // (loading models, loading rules) do not raise the progress bar.
    let scanRunning = false;
    function runAction(action, extra) {
        scanRunning = action === "run_scan" || action === "run_bulk";
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

    const changeModelBtn = makeButton("", "slls-bpa-btn-icon", ICON.swap);
    changeModelBtn.title = "Change semantic model / workspace";
    changeModelBtn.setAttribute("aria-label", "Change semantic model / workspace");
    changeModelBtn.style.display = "none";
    changeModelBtn.addEventListener("click", () => {
        // Staged fixes belong to the model they were staged against, so warn
        // before switching (which throws them away).
        if (stagedFixes.size > 0) openDiscardConfirm();
        else goToSelectScreen();
    });
    header.appendChild(changeModelBtn);

    // Pushes the remaining header actions to the right edge.
    const headSpacer = document.createElement("div");
    headSpacer.className = "slls-bpa-head-spacer";
    header.appendChild(headSpacer);

    const rulesBtn = makeButton("", "slls-bpa-btn-icon", ICON.sliders);
    rulesBtn.title = "Edit rules";
    rulesBtn.setAttribute("aria-label", "Edit rules");
    rulesBtn.addEventListener("click", () => openRulesPanel());
    header.appendChild(rulesBtn);

    const rerunBtn = makeButton("", "slls-bpa-btn-icon", ICON.refresh);
    rerunBtn.title = "Re-run the analysis";
    rerunBtn.setAttribute("aria-label", "Re-run the analysis");
    rerunBtn.style.display = "none";
    rerunBtn.addEventListener("click", () => rerun());
    header.appendChild(rerunBtn);

    const themeBtn = makeButton("", "slls-bpa-btn-icon slls-bpa-view-btn", "");
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
    const fullscreenBtn = makeButton("", "slls-bpa-btn-icon slls-bpa-view-btn", "");
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
        // Close whichever modal is open first, then leave full screen.
        if (historyOverlay.classList.contains("show")) closeHistory();
        else if (stagedOverlay.classList.contains("show")) closeStaged();
        else if (discardOverlay.classList.contains("show")) closeDiscardConfirm();
        else if (overlay.classList.contains("show")) overlay.classList.remove("show");
        else if (fsMode) setFullscreen(false);
    }
    renderFullscreenBtn();
    header.appendChild(fullscreenBtn);

    // Sits directly under the header and holds the staged-fixes pill.
    const topSlot = document.createElement("div");
    root.appendChild(topSlot);

    // ------------------------------------------------------------------
    // Status banner
    // ------------------------------------------------------------------
    const status = document.createElement("div");
    status.className = "slls-bpa-status";
    root.appendChild(status);
    let statusTimer = null;
    function setStatus(message, kind) {
        if (statusTimer) { window.clearTimeout(statusTimer); statusTimer = null; }
        if (!message) { status.classList.remove("show"); return; }
        status.className = `slls-bpa-status show ${kind || "info"}`;
        status.textContent = message;
        // Confirmations are transient; errors stay until the next action.
        if (kind !== "error") {
            statusTimer = window.setTimeout(() => {
                status.classList.remove("show");
                statusTimer = null;
            }, 6000);
        }
    }
    model.on("change:status", () => {
        const s = model.get("status") || {};
        setStatus(s.message || "", s.kind);
    });
    model.on("change:busy", () => {
        if (model.get("busy") === true) root.classList.add("slls-bpa-busy");
        else root.classList.remove("slls-bpa-busy");
    });

    // ------------------------------------------------------------------
    // Scan progress (multi-model runs)
    // ------------------------------------------------------------------
    const progress = document.createElement("div");
    progress.className = "slls-bpa-progress";
    const progressHead = document.createElement("div");
    progressHead.className = "slls-bpa-progress-head";
    const progressLabel = document.createElement("span");
    progressLabel.className = "slls-bpa-progress-label";
    const progressCount = document.createElement("span");
    progressCount.className = "slls-bpa-progress-count";
    progressHead.appendChild(progressLabel);
    progressHead.appendChild(progressCount);
    const cancelRunBtn = makeButton("Cancel", "slls-bpa-btn-sm slls-bpa-progress-cancel", ICON.close);
    cancelRunBtn.title = "Stop the analysis";
    cancelRunBtn.addEventListener("click", () => {
        // The scan stops at its next checkpoint and keeps whatever it has found.
        cancelRunBtn.disabled = true;
        cancelRunBtn.lastChild.textContent = "Cancelling\u2026";
        model.set("cancel_requested", true);
        model.save_changes();
    });
    progressHead.appendChild(cancelRunBtn);
    progress.appendChild(progressHead);
    const progressTrack = document.createElement("div");
    progressTrack.className = "slls-bpa-progress-track";
    const progressFill = document.createElement("div");
    progressFill.className = "slls-bpa-progress-fill";
    progressFill.setAttribute("role", "progressbar");
    progressTrack.appendChild(progressFill);
    progress.appendChild(progressTrack);
    root.appendChild(progress);

    function renderProgress() {
        const p = model.get("progress") || {};
        const total = Number(p.total) || 0;
        const running = scanRunning && model.get("busy") === true;
        if (!total && !running) {
            progress.classList.remove("show");
            return;
        }
        progress.classList.add("show");
        if (!total) {
            // A single model, or a run that has not reported a step count yet:
            // there is nothing to count, so the bar just sweeps.
            progressLabel.textContent = p.current
                ? `Analyzing ${p.current}\u2026`
                : "Analyzing\u2026";
            progressCount.textContent = "";
            progressFill.classList.add("slls-bpa-progress-pending");
            progressFill.style.width = "";
            return;
        }
        const done = Math.min(Number(p.done) || 0, total);
        progressFill.classList.remove("slls-bpa-progress-pending");
        progressLabel.textContent = p.current
            ? `Analyzing ${p.current}\u2026`
            : "Analyzing semantic models\u2026";
        progressCount.textContent = `${done} of ${total}`;
        progressFill.style.width = `${Math.round((done / total) * 100)}%`;
        progressFill.setAttribute("aria-valuenow", String(done));
        progressFill.setAttribute("aria-valuemin", "0");
        progressFill.setAttribute("aria-valuemax", String(total));
    }
    model.on("change:progress", renderProgress);
    model.on("change:busy", () => {
        if (model.get("busy") === true) {
            cancelRunBtn.disabled = false;
            cancelRunBtn.lastChild.textContent = "Cancel";
        }
        renderProgress();
    });

    // ==================================================================
    // SELECT SCREEN
    // ==================================================================
    const selectScreen = document.createElement("div");
    selectScreen.className = "slls-bpa-screen slls-bpa-screen-select";
    root.appendChild(selectScreen);

    const selectSection = document.createElement("div");
    selectSection.className = "slls-bpa-section";
    selectScreen.appendChild(selectSection);

    const selectHeading = document.createElement("h3");
    selectHeading.textContent = "Analyze a semantic model";
    selectSection.appendChild(selectHeading);

    const modeRow = document.createElement("div");
    modeRow.className = "slls-bpa-toolbar";
    modeRow.style.marginBottom = "14px";
    selectSection.appendChild(modeRow);

    const modeToggle = document.createElement("div");
    modeToggle.className = "slls-bpa-segmented";
    const singleModeBtn = document.createElement("button");
    singleModeBtn.type = "button";
    singleModeBtn.textContent = "Single model";
    const bulkModeBtn = document.createElement("button");
    bulkModeBtn.type = "button";
    bulkModeBtn.textContent = "Multiple models";
    modeToggle.appendChild(singleModeBtn);
    modeToggle.appendChild(bulkModeBtn);
    modeRow.appendChild(modeToggle);

    // Refetches the workspaces, the models of the selected workspace and any
    // workspace already expanded in the multi-model picker.
    const reloadBtn = makeButton("", "slls-bpa-btn-icon", ICON.refresh);
    reloadBtn.title = "Reload workspaces and semantic models";
    reloadBtn.setAttribute("aria-label", reloadBtn.title);
    reloadBtn.addEventListener("click", () => {
        runAction("reload_lists", { workspace_id: model.get("workspace_id") || "" });
    });
    modeRow.appendChild(reloadBtn);

    let bulkMode = false;
    // key `${workspaceId}\u0000${datasetId}` -> target descriptor
    const bulkSelection = new Map();
    // Workspace ids whose model list has been expanded (and therefore loaded).
    const bulkExpanded = new Set();

    const pickerBar = document.createElement("div");
    pickerBar.className = "slls-bpa-toolbar";
    pickerBar.style.alignItems = "flex-end";
    selectSection.appendChild(pickerBar);

    function pickerField(labelText, control, minWidth) {
        const field = document.createElement("div");
        field.className = "slls-bpa-field";
        field.style.minWidth = minWidth;
        const label = document.createElement("span");
        label.className = "slls-bpa-field-label";
        label.textContent = labelText;
        field.appendChild(label);
        field.appendChild(control);
        return field;
    }

    const wsSelect = createSearchSelect(
        "Select a workspace\u2026", "Filter workspaces\u2026", "Workspace",
        "Loading workspaces\u2026",
        (option) => {
            model.set("workspace_id", option.value);
            model.set("dataset_id", "");
            model.save_changes();
            dsSelect.setOptions([], "");
            dsSelect.setEmptyLabel("Loading semantic models\u2026");
            runAction("list_datasets", { workspace_id: option.value });
        });
    pickerBar.appendChild(pickerField("Workspace", wsSelect.el, "240px"));

    const dsSelect = createSearchSelect(
        "Select a semantic model\u2026", "Filter models\u2026", "Semantic model",
        "Select a workspace first\u2026",
        (option) => {
            model.set("dataset_id", option.value);
            model.save_changes();
            updateSelectState();
        });
    pickerBar.appendChild(pickerField("Semantic model", dsSelect.el, "260px"));

    const runBtn = makeButton("Run analysis", "slls-bpa-btn-primary", ICON.play);
    pickerBar.appendChild(runBtn);

    // ---- Multi-model picker: expandable workspaces with their models ----
    const bulkPane = document.createElement("div");
    bulkPane.style.display = "none";
    selectSection.appendChild(bulkPane);

    const bulkBar = document.createElement("div");
    bulkBar.className = "slls-bpa-toolbar";
    bulkPane.appendChild(bulkBar);

    const bulkSearchWrap = document.createElement("div");
    bulkSearchWrap.className = "slls-bpa-searchwrap";
    bulkSearchWrap.style.flex = "1";
    bulkSearchWrap.appendChild(iconSpan(ICON.search, "slls-bpa-searchicon"));
    const bulkSearch = document.createElement("input");
    bulkSearch.className = "slls-bpa-input";
    bulkSearch.type = "search";
    bulkSearch.style.width = "100%";
    bulkSearch.placeholder = "Search workspaces and models\u2026";
    bulkSearch.setAttribute("aria-label", "Search workspaces and models");
    bulkSearchWrap.appendChild(bulkSearch);
    bulkBar.appendChild(bulkSearchWrap);

    const bulkRunBtn = makeButton("Run analysis", "slls-bpa-btn-primary", ICON.play);
    bulkBar.appendChild(bulkRunBtn);

    // Summary of everything picked so far, so the chosen models (and the
    // workspace each one lives in) stay visible while browsing other workspaces.
    const bulkSelected = document.createElement("div");
    bulkSelected.className = "slls-bpa-selected";
    bulkPane.appendChild(bulkSelected);

    const bulkSelectedHead = document.createElement("div");
    bulkSelectedHead.className = "slls-bpa-selected-head";
    const bulkSelectedTitle = document.createElement("span");
    bulkSelectedTitle.className = "slls-bpa-selected-title";
    bulkSelectedHead.appendChild(bulkSelectedTitle);
    const bulkClearBtn = document.createElement("button");
    bulkClearBtn.type = "button";
    bulkClearBtn.className = "slls-bpa-selected-clear";
    bulkClearBtn.textContent = "Clear all";
    bulkSelectedHead.appendChild(bulkClearBtn);
    bulkSelected.appendChild(bulkSelectedHead);

    const bulkSelectedList = document.createElement("div");
    bulkSelectedList.className = "slls-bpa-selected-list";
    bulkSelected.appendChild(bulkSelectedList);

    const bulkTree = document.createElement("div");
    bulkTree.className = "slls-bpa-tree";
    bulkPane.appendChild(bulkTree);

    const selectHint = document.createElement("div");
    selectHint.className = "slls-bpa-hint";
    selectSection.appendChild(selectHint);

    function renderWorkspaces() {
        const items = model.get("workspaces") || [];
        if (items.length > 0) wsSelect.setEmptyLabel("No workspaces");
        wsSelect.setOptions(
            items.map((ws) => ({ value: ws.id, label: ws.name })),
            model.get("workspace_id") || "",
        );
        renderBulkTree();
    }
    function renderDatasets() {
        const items = model.get("datasets") || [];
        dsSelect.setEmptyLabel(model.get("workspace_id")
            ? "No semantic models in workspace"
            : "Select a workspace first\u2026");
        dsSelect.setOptions(
            items.map((ds) => ({ value: ds.id, label: ds.name })),
            model.get("dataset_id") || "",
        );
        updateSelectState();
    }

    function bulkKey(workspaceId, datasetId) {
        return `${workspaceId}\u0000${datasetId}`;
    }

    function toggleBulkModel(workspace, dataset) {
        const key = bulkKey(workspace.id, dataset.id);
        if (bulkSelection.has(key)) {
            bulkSelection.delete(key);
        } else {
            if (bulkSelection.size >= MAX_BULK) return;
            bulkSelection.set(key, {
                workspace_id: workspace.id,
                workspace_name: workspace.name,
                dataset_id: dataset.id,
                dataset_name: dataset.name,
            });
        }
        renderBulkTree();
        updateSelectState();
    }

    function renderBulkTree() {
        clear(bulkTree);
        const term = bulkSearch.value.trim().toLowerCase();
        const workspaces = model.get("workspaces") || [];
        const loaded = model.get("workspace_datasets") || {};
        const maxReached = bulkSelection.size >= MAX_BULK;

        const shown = workspaces.filter((ws) => {
            if (!term) return true;
            if (ws.name.toLowerCase().includes(term)) return true;
            const models = loaded[ws.id];
            return Array.isArray(models)
                && models.some((ds) => ds.name.toLowerCase().includes(term));
        });

        if (shown.length === 0) {
            const empty = document.createElement("div");
            empty.className = "slls-bpa-tree-msg";
            empty.style.paddingLeft = "14px";
            empty.textContent = workspaces.length === 0
                ? "No workspaces available."
                : "No workspaces or models match the search.";
            bulkTree.appendChild(empty);
            return;
        }

        for (const ws of shown) {
            const box = document.createElement("div");
            box.className = "slls-bpa-tree-ws";

            const isOpen = bulkExpanded.has(ws.id);
            const head = document.createElement("button");
            head.type = "button";
            head.className = "slls-bpa-tree-head";
            head.setAttribute("aria-expanded", String(isOpen));
            head.appendChild(iconSpan(ICON.caret, `slls-bpa-tree-caret${isOpen ? " open" : ""}`));
            const name = document.createElement("span");
            name.className = "slls-bpa-tree-name";
            name.textContent = ws.name;
            name.title = ws.name;
            head.appendChild(name);
            const selectedHere = [...bulkSelection.keys()]
                .filter((k) => k.startsWith(`${ws.id}\u0000`)).length;
            if (selectedHere > 0) {
                const count = document.createElement("span");
                count.className = "slls-bpa-tree-count";
                count.textContent = `${selectedHere} selected`;
                head.appendChild(count);
            }
            head.addEventListener("click", () => {
                if (bulkExpanded.has(ws.id)) {
                    bulkExpanded.delete(ws.id);
                } else {
                    bulkExpanded.add(ws.id);
                    // Models are only listed the first time a workspace is expanded.
                    if (!Array.isArray((model.get("workspace_datasets") || {})[ws.id])) {
                        runAction("load_workspace_datasets", { workspace_id: ws.id });
                    }
                }
                renderBulkTree();
            });
            box.appendChild(head);

            if (isOpen) {
                const models = loaded[ws.id];
                const list = document.createElement("div");
                list.className = "slls-bpa-tree-models";
                if (!Array.isArray(models)) {
                    const msg = document.createElement("div");
                    msg.className = "slls-bpa-tree-msg";
                    msg.textContent = "Loading semantic models\u2026";
                    list.appendChild(msg);
                } else {
                    const visible = term
                        ? models.filter((ds) => ds.name.toLowerCase().includes(term)
                            || ws.name.toLowerCase().includes(term))
                        : models;
                    if (visible.length === 0) {
                        const msg = document.createElement("div");
                        msg.className = "slls-bpa-tree-msg";
                        msg.textContent = models.length === 0
                            ? "No semantic models in this workspace."
                            : "No models match the search.";
                        list.appendChild(msg);
                    }
                    for (const ds of visible) {
                        const key = bulkKey(ws.id, ds.id);
                        const selected = bulkSelection.has(key);
                        const row = document.createElement("div");
                        row.className = "slls-bpa-tree-model" + (selected ? " selected" : "");

                        const toggle = document.createElement("label");
                        toggle.className = "slls-bpa-switch";
                        const box2 = document.createElement("input");
                        box2.type = "checkbox";
                        box2.checked = selected;
                        box2.disabled = !selected && maxReached;
                        box2.setAttribute("aria-label", `Select ${ds.name}`);
                        box2.addEventListener("change", () => toggleBulkModel(ws, ds));
                        toggle.appendChild(box2);
                        toggle.appendChild(document.createElement("i"));
                        toggle.title = !selected && maxReached
                            ? `Maximum of ${MAX_BULK} models selected`
                            : `Include "${ds.name}" in the analysis`;
                        row.appendChild(toggle);

                        const modelName = document.createElement("span");
                        modelName.className = "slls-bpa-tree-modelname";
                        modelName.textContent = ds.name;
                        modelName.title = ds.name;
                        modelName.addEventListener("click", () => {
                            if (!selected && maxReached) return;
                            toggleBulkModel(ws, ds);
                        });
                        row.appendChild(modelName);
                        list.appendChild(row);
                    }
                }
                box.appendChild(list);
            }

            bulkTree.appendChild(box);
        }
    }

    function renderBulkSelection() {
        bulkSelected.classList.toggle("show", bulkSelection.size > 0);
        bulkSelectedTitle.textContent =
            `${plural(bulkSelection.size, "model")} selected`;
        clear(bulkSelectedList);
        // Group by workspace so the same workspace is not repeated needlessly,
        // then list one chip per model showing both names.
        const targets = [...bulkSelection.entries()].sort((a, b) =>
            a[1].workspace_name.localeCompare(b[1].workspace_name)
            || a[1].dataset_name.localeCompare(b[1].dataset_name));
        for (const [key, target] of targets) {
            const chip = document.createElement("div");
            chip.className = "slls-bpa-selchip";
            const text = document.createElement("span");
            text.className = "slls-bpa-selchip-text";
            text.title = `${target.dataset_name} \u2022 ${target.workspace_name}`;
            const modelName = document.createElement("span");
            modelName.className = "slls-bpa-selchip-model";
            modelName.textContent = target.dataset_name;
            const sep = document.createElement("span");
            sep.className = "slls-bpa-selchip-sep";
            sep.textContent = "\u2022";
            const wsName = document.createElement("span");
            wsName.className = "slls-bpa-selchip-ws";
            wsName.textContent = target.workspace_name;
            text.appendChild(modelName);
            text.appendChild(sep);
            text.appendChild(wsName);
            chip.appendChild(text);

            const remove = document.createElement("button");
            remove.type = "button";
            remove.innerHTML = ICON.close;
            remove.title = `Remove ${target.dataset_name}`;
            remove.setAttribute("aria-label", `Remove ${target.dataset_name}`);
            remove.addEventListener("click", () => {
                bulkSelection.delete(key);
                renderBulkTree();
                updateSelectState();
            });
            chip.appendChild(remove);
            bulkSelectedList.appendChild(chip);
        }
    }

    function updateSelectState() {
        singleModeBtn.classList.toggle("active", !bulkMode);
        bulkModeBtn.classList.toggle("active", bulkMode);
        pickerBar.style.display = bulkMode ? "none" : "";
        bulkPane.style.display = bulkMode ? "" : "none";
        if (bulkMode) {
            bulkRunBtn.disabled = bulkSelection.size === 0;
            selectHint.innerHTML =
                `Select up to ${MAX_BULK} semantic models across workspaces. ` +
                `<span class="slls-bpa-selcount">${bulkSelection.size}/${MAX_BULK} selected</span>`;
            renderBulkSelection();
        } else {
            runBtn.disabled = !dsSelect.value;
            selectHint.textContent = "";
        }
    }

    function startRun() {
        resetFilters();
        if (bulkMode) {
            runAction("run_bulk", {
                targets: [...bulkSelection.values()],
                disabled_rules: [...disabledRules],
            });
        } else {
            runAction("run_scan", {
                workspace_id: wsSelect.value,
                workspace_name: wsSelect.label,
                dataset_id: dsSelect.value,
                dataset_name: dsSelect.label,
                disabled_rules: [...disabledRules],
            });
        }
    }

    singleModeBtn.addEventListener("click", () => { bulkMode = false; updateSelectState(); });
    bulkModeBtn.addEventListener("click", () => {
        bulkMode = true;
        updateSelectState();
        renderBulkTree();
    });
    bulkSearch.addEventListener("input", renderBulkTree);
    bulkClearBtn.addEventListener("click", () => {
        bulkSelection.clear();
        renderBulkTree();
        updateSelectState();
    });
    runBtn.addEventListener("click", startRun);
    bulkRunBtn.addEventListener("click", startRun);

    model.on("change:workspaces", renderWorkspaces);
    model.on("change:datasets", renderDatasets);
    model.on("change:workspace_datasets", renderBulkTree);

    // ==================================================================
    // RESULTS SCREEN
    // ==================================================================
    const resultsScreen = document.createElement("div");
    resultsScreen.className = "slls-bpa-screen";
    root.appendChild(resultsScreen);

    // The category cards, filter bar and rule groups are built once and
    // re-parented, so the comparison report can show a model's violations
    // inline using exactly the same controls as the single-model view.
    const resultsContent = document.createElement("div");
    resultsScreen.appendChild(resultsContent);

    const catGrid = document.createElement("div");
    catGrid.className = "slls-bpa-catgrid";
    catGrid.style.marginTop = "0";
    resultsContent.appendChild(catGrid);

    const filterBar = document.createElement("div");
    filterBar.className = "slls-bpa-toolbar";
    filterBar.style.marginTop = "16px";
    resultsContent.appendChild(filterBar);

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
    resultsContent.appendChild(groupsWrap);

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

    const bulkCatGrid = document.createElement("div");
    bulkCatGrid.className = "slls-bpa-catgrid";
    bulkCatGrid.style.marginTop = "0";
    bulkScreen.appendChild(bulkCatGrid);

    const bulkList = document.createElement("div");
    bulkList.className = "slls-bpa-bulk-list";
    bulkScreen.appendChild(bulkList);

    // Holds the shared search / severity controls while a model is expanded, so
    // they sit directly under the category panels rather than inside the detail.
    const bulkFilterSlot = document.createElement("div");
    bulkScreen.insertBefore(bulkFilterSlot, bulkList);

    const bulkDetail = document.createElement("div");
    bulkDetail.className = "slls-bpa-detail";
    bulkScreen.appendChild(bulkDetail);

    const bulkDetailHead = document.createElement("div");
    bulkDetailHead.className = "slls-bpa-detail-head";
    const bulkDetailClose = makeButton("", "slls-bpa-btn-icon", ICON.close);
    bulkDetailClose.title = "Close these violations";
    bulkDetailClose.setAttribute("aria-label", "Close these violations");
    bulkDetailHead.appendChild(bulkDetailClose);
    bulkDetail.appendChild(bulkDetailHead);

    // ------------------------------------------------------------------
    // Staged fixes pill (rendered into the slot under the header)
    // ------------------------------------------------------------------
    const saveBar = document.createElement("div");
    saveBar.className = "slls-bpa-savebar";
    const saveBarLabel = document.createElement("div");
    saveBarLabel.className = "slls-bpa-savebar-label";
    const pendingDot = document.createElement("span");
    pendingDot.className = "slls-bpa-pending-dot";
    saveBarLabel.appendChild(pendingDot);
    const saveBarText = document.createElement("span");
    saveBarLabel.appendChild(saveBarText);
    const saveBarReview = document.createElement("button");
    saveBarReview.type = "button";
    saveBarReview.className = "slls-bpa-savebar-review";
    saveBarReview.textContent = "Review";
    saveBarReview.title = "See the staged changes";
    saveBarLabel.appendChild(saveBarReview);
    saveBar.appendChild(saveBarLabel);
    const discardBtn = makeButton("Discard", "slls-bpa-btn-sm", ICON.undo);
    discardBtn.title = "Discard every staged fix";
    saveBar.appendChild(discardBtn);
    const saveBtn = makeButton("Save", "slls-bpa-btn-sm slls-bpa-btn-primary", ICON.save);
    saveBtn.title = "Apply every staged fix to the semantic model(s)";
    saveBar.appendChild(saveBtn);
    topSlot.appendChild(saveBar);

    // The staged changes are reviewed in their own modal.
    const stagedOverlay = document.createElement("div");
    stagedOverlay.className = "slls-bpa-overlay";
    root.appendChild(stagedOverlay);
    stagedOverlay.addEventListener("click", (ev) => {
        if (ev.target === stagedOverlay) closeStaged();
    });

    const stagedList = document.createElement("div");
    stagedList.className = "slls-bpa-staged";

    // Warns that the staged fixes are lost before changing the model/workspace.
    const discardOverlay = document.createElement("div");
    discardOverlay.className = "slls-bpa-overlay";
    root.appendChild(discardOverlay);
    discardOverlay.addEventListener("click", (ev) => {
        if (ev.target === discardOverlay) closeDiscardConfirm();
    });

    // The rule change history is reviewed in its own modal, opened from the rule
    // editor, so it is stacked above it.
    const historyOverlay = document.createElement("div");
    historyOverlay.className = "slls-bpa-overlay slls-bpa-overlay-top";
    root.appendChild(historyOverlay);
    historyOverlay.addEventListener("click", (ev) => {
        if (ev.target === historyOverlay) closeHistory();
    });

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
    // Rule editor change history. Every entry snapshots the state *before* a
    // change so it can be undone, and is listed in the change-history popup.
    const ruleHistory = [];
    // Changes which were undone, so they can be redone. Making a new change
    // clears it.
    const ruleRedoStack = [];
    // Which ruleset the backend currently holds: "initial" (whatever was passed
    // to the function), "default" (the built-in rules) or an imported ruleset.
    let rulesetRef = { source: "initial", rules: null };
    // A ruleset change awaiting the backend's confirmation.
    let pendingRulesetChange = null;
    const expandedRules = new Set();
    // Rule ids whose Expression / FixExpression code is revealed in the rule editor.
    const expandedRuleExprs = new Set();
    let categoryFilter = null;
    let fixRule = null;
    const fixSelected = new Set();
    // Fixes staged but not yet written to the model(s), keyed by
    // `${workspaceId}\u0000${datasetId}\u0000${ruleName}\u0000${objectName}`.
    const stagedFixes = new Map();
    // Whether the staged changes modal spells out the before/after values.
    let showStagedDiff = true;
    // Violations currently displayed (single scan, or one model drilled into from bulk).
    let activeViolations = [];

    function resetFilters() {
        expandedRules.clear();
        severityFilterSelect.reset();
        categoryFilter = null;
        searchInput.value = "";
        fixRule = null;
        fixSelected.clear();
    }

    // ------------------------------------------------------------------
    // Staged fixes
    // ------------------------------------------------------------------
    // The model whose violations are currently on screen (single scan or the
    // model expanded in the comparison report).
    function currentTarget() {
        return {
            workspace_id: model.get("workspace_id") || "",
            dataset_id: model.get("dataset_id") || "",
            dataset_name: model.get("dataset_name") || "",
        };
    }
    function stagedKey(datasetId, ruleName, objectName) {
        return `${datasetId}\u0000${ruleName}\u0000${objectName}`;
    }
    function isStaged(ruleName, objectName) {
        return stagedFixes.has(
            stagedKey(model.get("dataset_id") || "", ruleName, objectName));
    }

    function renderStaged() {
        const count = stagedFixes.size;
        saveBar.classList.toggle("show", count > 0);
        if (count === 0) {
            closeStaged();
            return;
        }
        const models = new Set([...stagedFixes.values()].map((f) => f.dataset_id));
        saveBarText.textContent = models.size > 1
            ? `${plural(count, "fix")} staged across ${plural(models.size, "semantic model")}`
            : `${plural(count, "fix")} staged`;
        saveBtn.lastChild.textContent = `Save ${count}`;
        if (stagedOverlay.classList.contains("show")) renderStagedList();
    }

    function renderStagedList() {
        clear(stagedList);
        for (const [key, fix] of stagedFixes.entries()) {
            const row = document.createElement("div");
            row.className = "slls-bpa-staged-row";
            const main = document.createElement("div");
            main.className = "slls-bpa-staged-main";
            const rule = document.createElement("div");
            rule.className = "slls-bpa-staged-rule";
            rule.textContent = fix.rule_name;
            rule.title = fix.rule_name;
            main.appendChild(rule);
            const object = document.createElement("div");
            object.className = "slls-bpa-staged-obj";
            object.textContent = `${fix.object_name} \u2022 ${fix.dataset_name}`;
            object.title = `${fix.object_name} \u2022 ${fix.dataset_name}`;
            main.appendChild(object);
            if (showStagedDiff) {
                // Same before/after treatment as the "Apply fix" preview.
                const diff = document.createElement("span");
                diff.className = "slls-bpa-fix-diff";
                const before = document.createElement("span");
                before.className = "slls-bpa-fix-before";
                before.textContent = fix.before || "\u2014";
                const arrow = document.createElement("span");
                arrow.textContent = " \u2192 ";
                const after = document.createElement("span");
                after.className = "slls-bpa-fix-after";
                after.textContent = fix.after || "\u2014";
                diff.appendChild(before);
                diff.appendChild(arrow);
                diff.appendChild(after);
                main.appendChild(diff);
            }
            row.appendChild(main);

            const unstage = document.createElement("button");
            unstage.type = "button";
            unstage.innerHTML = ICON.close;
            unstage.title = "Unstage this fix";
            unstage.setAttribute("aria-label", `Unstage the fix for ${fix.object_name}`);
            unstage.addEventListener("click", () => {
                stagedFixes.delete(key);
                renderStaged();
                refreshViolations();
            });
            row.appendChild(unstage);
            stagedList.appendChild(row);
        }
    }

    function closeStaged() {
        stagedOverlay.classList.remove("show");
    }

    function goToSelectScreen() {
        closeBulkDetail();
        model.set("screen", "select");
        model.save_changes();
        renderScreen();
    }

    function closeDiscardConfirm() {
        discardOverlay.classList.remove("show");
    }

    // The staged fixes are tied to the model they were staged against, so
    // changing the semantic model / workspace throws them away. Ask first,
    // offering a look at what would be lost.
    function openDiscardConfirm() {
        if (stagedFixes.size === 0) {
            goToSelectScreen();
            return;
        }
        clear(discardOverlay);
        const modal = document.createElement("div");
        modal.className = "slls-bpa-modal";

        const heading = document.createElement("h2");
        heading.textContent = "Discard staged changes?";
        modal.appendChild(heading);
        const sub = document.createElement("div");
        sub.className = "slls-bpa-modal-sub";
        const count = stagedFixes.size;
        sub.textContent = count === 1
            ? "1 staged fix has not been saved yet. Changing the semantic model / workspace discards it."
            : `${count} staged fixes have not been saved yet. Changing the semantic model / workspace discards them.`;
        modal.appendChild(sub);

        const footer = document.createElement("div");
        footer.className = "slls-bpa-modal-footer";
        const cancelBtn = makeButton("Cancel", "slls-bpa-btn-sm");
        cancelBtn.addEventListener("click", closeDiscardConfirm);
        footer.appendChild(cancelBtn);
        const reviewBtn = makeButton("Review", "slls-bpa-btn-sm");
        reviewBtn.title = "See the staged changes";
        reviewBtn.addEventListener("click", () => {
            closeDiscardConfirm();
            openStaged();
        });
        footer.appendChild(reviewBtn);
        const discardChangeBtn = makeButton(
            "Discard and change", "slls-bpa-btn-sm slls-bpa-btn-danger", ICON.undo);
        discardChangeBtn.addEventListener("click", () => {
            stagedFixes.clear();
            closeDiscardConfirm();
            // Hides the save bar (and closes the staged changes modal).
            renderStaged();
            refreshViolations();
            goToSelectScreen();
        });
        footer.appendChild(discardChangeBtn);
        modal.appendChild(footer);

        discardOverlay.appendChild(modal);
        discardOverlay.classList.add("show");
    }

    function openStaged() {
        if (stagedFixes.size === 0) return;
        clear(stagedOverlay);
        const modal = document.createElement("div");
        modal.className = "slls-bpa-modal slls-bpa-staged-modal";

        const heading = document.createElement("h2");
        heading.textContent = "Staged changes";
        modal.appendChild(heading);
        const sub = document.createElement("div");
        sub.className = "slls-bpa-modal-sub";
        sub.textContent = "Nothing is written to the semantic model until you save.";
        modal.appendChild(sub);

        const bar = document.createElement("div");
        bar.className = "slls-bpa-toolbar";
        bar.style.marginBottom = "12px";
        const diffToggle = makeButton("Changes", "slls-bpa-btn-sm");
        function renderDiffToggle() {
            clear(diffToggle);
            diffToggle.appendChild(iconSpan(showStagedDiff ? ICON.eye : ICON.eyeOff));
            const text = document.createElement("span");
            text.textContent = "Changes";
            diffToggle.appendChild(text);
            diffToggle.title = showStagedDiff
                ? "Hide the before and after values"
                : "Show the before and after values";
            diffToggle.setAttribute("aria-pressed", String(showStagedDiff));
        }
        renderDiffToggle();
        diffToggle.addEventListener("click", () => {
            showStagedDiff = !showStagedDiff;
            renderDiffToggle();
            renderStagedList();
        });
        bar.appendChild(diffToggle);
        modal.appendChild(bar);

        renderStagedList();
        modal.appendChild(stagedList);

        const footer = document.createElement("div");
        footer.className = "slls-bpa-modal-footer";
        const closeBtn = makeButton("Close", "slls-bpa-btn-sm");
        closeBtn.addEventListener("click", closeStaged);
        footer.appendChild(closeBtn);
        const modalSave = makeButton(
            `Save ${stagedFixes.size}`, "slls-bpa-btn-sm slls-bpa-btn-primary", ICON.save);
        modalSave.addEventListener("click", () => { closeStaged(); saveStaged(); });
        footer.appendChild(modalSave);
        modal.appendChild(footer);

        stagedOverlay.appendChild(modal);
        stagedOverlay.classList.add("show");
    }

    // Re-renders whichever violation view is currently visible.
    function refreshViolations() {
        if (model.get("screen") === "bulk") renderBulk();
        else renderResults();
    }

    saveBarReview.addEventListener("click", openStaged);
    discardBtn.addEventListener("click", () => {
        stagedFixes.clear();
        closeStaged();
        renderStaged();
        refreshViolations();
    });
    function saveStaged() {
        if (stagedFixes.size === 0) return;
        const fixes = [...stagedFixes.values()].map((f) => ({
            workspace_id: f.workspace_id,
            dataset_id: f.dataset_id,
            rule_name: f.rule_name,
            object_name: f.object_name,
        }));
        stagedFixes.clear();
        renderStaged();
        runAction("apply_staged_fixes", {
            fixes,
            disabled_rules: [...disabledRules],
        });
    }
    saveBtn.addEventListener("click", saveStaged);

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
        return activeViolations.filter((v) => {
            // A staged fix removes its violation optimistically; it comes back
            // if the fix is unstaged.
            if (isStaged(v.ruleName, v.objectName)) return false;
            if (categoryFilter && v.category !== categoryFilter) return false;
            if (severities.size > 0 && !severities.has(v.severity)) return false;
            if (term
                && !String(v.ruleName).toLowerCase().includes(term)
                && !String(v.objectName).toLowerCase().includes(term)) return false;
            return true;
        });
    }

    // Clickable per-category summary cards. Used both for a single model's
    // results and for the aggregate across a multi-model comparison report.
    function renderCategoryCards(container, violations, onSelect) {
        clear(container);
        const map = new Map();
        for (const v of violations) {
            const entry = map.get(v.category) || { total: 0, Error: 0, Warning: 0, Info: 0 };
            entry.total += 1;
            if (entry[v.severity] !== undefined) entry[v.severity] += 1;
            map.set(v.category, entry);
        }
        const rows = [...map.entries()].sort(
            (a, b) => categoryRank(a[0]) - categoryRank(b[0]) || a[0].localeCompare(b[0]));
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
                onSelect();
            });
            container.appendChild(btn);
        }
    }

    // A category picked while a comparison-report model is expanded also
    // re-filters the per-model totals, so both views stay in step.
    function onCategoryChange() {
        if (bulkDetailKey) renderBulk();
        else renderResults();
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
            || categoryRank(a.category) - categoryRank(b.category)
            || a.ruleName.localeCompare(b.ruleName));
    }

    function renderFixPanel(group) {
        const panel = document.createElement("div");
        panel.className = "slls-bpa-fix";
        const preview = model.get("fix_preview") || {};
        const target = currentTarget();
        const loading = preview.ruleName !== group.ruleName
            || preview.datasetId !== target.dataset_id;
        // Items already staged are hidden so the panel only offers new changes.
        const items = loading
            ? []
            : (preview.items || []).filter(
                (i) => !isStaged(group.ruleName, i.objectName));

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
            msg.textContent = "No further changes can be staged for this rule.";
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
                    stageFixBtn.disabled = fixSelected.size === 0;
                    stageFixBtn.lastChild.textContent = fixSelected.size > 0
                        ? `Stage fix (${fixSelected.size})` : "Stage fix";
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
            refreshViolations();
        });
        actions.appendChild(cancelBtn);
        const stageFixBtn = makeButton(
            fixSelected.size > 0 ? `Stage fix (${fixSelected.size})` : "Stage fix",
            "slls-bpa-btn-sm slls-bpa-btn-primary",
            ICON.wand,
        );
        stageFixBtn.title = "Stage these changes; nothing is written until you save";
        stageFixBtn.disabled = fixSelected.size === 0;
        stageFixBtn.addEventListener("click", () => {
            for (const item of items) {
                if (!fixSelected.has(item.objectName)) continue;
                stagedFixes.set(
                    stagedKey(target.dataset_id, group.ruleName, item.objectName), {
                        workspace_id: target.workspace_id,
                        dataset_id: target.dataset_id,
                        dataset_name: target.dataset_name,
                        rule_name: group.ruleName,
                        object_name: item.objectName,
                        before: item.before,
                        after: item.after,
                    });
            }
            fixRule = null;
            fixSelected.clear();
            renderStaged();
            refreshViolations();
        });
        actions.appendChild(stageFixBtn);
        panel.appendChild(actions);
        return panel;
    }

    function renderGroups(violations) {
        clear(groupsWrap);
        const groups = buildGroups(violations);
        if (groups.length === 0) {
            const empty = document.createElement("div");
            empty.className = "slls-bpa-empty";
            empty.appendChild(iconSpan(ICON.checkCircle, "slls-bpa-empty-icon"));
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
            toggle.addEventListener("click", () => {
                if (expandedRules.has(group.ruleName)) expandedRules.delete(group.ruleName);
                else expandedRules.add(group.ruleName);
                renderResults();
            });
            head.appendChild(toggle);

            // "Apply fix" sits to the left of the violation count so the rule
            // name, its action and its count read left-to-right.
            if (group.fixable) {
                const fixBtn = makeButton("Apply fix", "slls-bpa-btn-sm slls-bpa-btn-fix", ICON.wand);
                fixBtn.title = "Preview and stage the automatic fix for this rule";
                fixBtn.addEventListener("click", () => {
                    fixRule = group.ruleName;
                    fixSelected.clear();
                    model.set("fix_preview", {});
                    model.save_changes();
                    const target = currentTarget();
                    runAction("preview_fix", {
                        rule_name: group.ruleName,
                        workspace_id: target.workspace_id,
                        dataset_id: target.dataset_id,
                    });
                    refreshViolations();
                });
                head.appendChild(fixBtn);
            }

            const badge = document.createElement("span");
            badge.className = "slls-bpa-badge";
            badge.textContent = String(group.violations.length);
            badge.title = plural(group.violations.length, "violation");
            head.appendChild(badge);

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
        // In the comparison report the category panels live above the model list,
        // so the copy inside the inline detail is redundant.
        if (bulkDetailKey) {
            catGrid.style.display = "none";
        } else {
            catGrid.style.display = "";
            renderCategoryCards(catGrid, activeViolations, onCategoryChange);
        }
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
    // `${workspaceId}\u0000${datasetId}` of the model expanded inline, or null.
    let bulkDetailKey = null;

    function closeBulkDetail() {
        bulkDetailKey = null;
        bulkDetail.classList.remove("show");
        // Restore the shared filter controls to the single-model layout.
        bulkDetailHead.appendChild(bulkDetailClose);
        filterBar.appendChild(expandBtn);
        filterBar.appendChild(collapseBtn);
        resultsContent.insertBefore(filterBar, groupsWrap);
        resultsScreen.appendChild(resultsContent);
    }
    bulkDetailClose.addEventListener("click", () => {
        closeBulkDetail();
        renderBulk();
    });

    function renderBulk() {
        const results = model.get("bulk_results") || [];
        const detail = results.find(
            (r) => `${r.workspace_id}\u0000${r.dataset_id}` === bulkDetailKey) || null;

        // The category panels summarize the selected model when one is expanded,
        // and every analyzed model otherwise.
        const summarized = [];
        if (detail) {
            for (const v of (detail.violations || [])) summarized.push(v);
        } else {
            for (const r of results) for (const v of (r.violations || [])) summarized.push(v);
        }
        renderCategoryCards(bulkCatGrid, summarized, renderBulk);

        clear(bulkList);
        // Selecting a category narrows the per-model totals as well.
        const forModel = (result) => (result.violations || []).filter(
            (v) => !categoryFilter || v.category === categoryFilter);
        const maxTotal = Math.max(1, ...results.map((r) => forModel(r).length));
        for (const result of results) {
            const violations = forModel(result);
            const key = `${result.workspace_id}\u0000${result.dataset_id}`;
            const counts = { Error: 0, Warning: 0, Info: 0 };
            for (const v of violations) if (counts[v.severity] !== undefined) counts[v.severity] += 1;

            const row = document.createElement("button");
            row.type = "button";
            row.className = "slls-bpa-bulk-row" + (bulkDetailKey === key ? " active" : "");

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
                if (bulkDetailKey === key) {
                    closeBulkDetail();
                } else {
                    bulkDetailKey = key;
                    // The category picked in the report carries into the detail.
                    const keepCategory = categoryFilter;
                    resetFilters();
                    categoryFilter = keepCategory;
                    // Make the inspected model the active one so "re-run" targets it.
                    model.set("workspace_id", result.workspace_id);
                    model.set("workspace_name", result.workspace_name);
                    model.set("dataset_id", result.dataset_id);
                    model.set("dataset_name", result.dataset_name);
                    model.save_changes();
                }
                renderBulk();
            });
            bulkList.appendChild(row);
        }

        if (!detail) {
            closeBulkDetail();
            return;
        }

        bulkDetail.classList.add("show");
        // Search, severity, expand / collapse and close all share one row,
        // directly under the category panels.
        bulkFilterSlot.appendChild(filterBar);
        filterBar.appendChild(expandBtn);
        filterBar.appendChild(collapseBtn);
        filterBar.appendChild(bulkDetailClose);
        bulkDetail.appendChild(resultsContent);
        activeViolations = detail.violations || [];
        renderResults();
    }

    // ------------------------------------------------------------------
    // Rules panel
    // ------------------------------------------------------------------
    const overlay = document.createElement("div");
    overlay.className = "slls-bpa-overlay";
    root.appendChild(overlay);
    overlay.addEventListener("click", (ev) => { if (ev.target === overlay) overlay.classList.remove("show"); });

    // Set while the rules panel is open, so the list can be re-rendered when the
    // catalog finishes loading or a ruleset is imported.
    let activeRuleListRender = null;
    // Set while the rules panel is open, so the problems reported by an imported
    // ruleset can be re-rendered when they arrive from the backend.
    let activeIssuesRender = null;
    // Set while the rules panel is open, so the undo / history buttons can follow
    // the history stack.
    let activeRuleCtrlsRender = null;
    function refreshRuleList() {
        if (activeRuleListRender && overlay.classList.contains("show")) {
            activeRuleListRender();
        }
        if (activeRuleCtrlsRender && overlay.classList.contains("show")) {
            activeRuleCtrlsRender();
        }
    }
    model.on("change:rules", () => {
        // A ruleset change is only recorded once the backend has adopted it.
        if (pendingRulesetChange) {
            rulesetRef = pendingRulesetChange.ruleset;
            if (pendingRulesetChange.history) {
                ruleHistory.push({
                    time: new Date(),
                    label: pendingRulesetChange.history.label,
                    state: pendingRulesetChange.history.state,
                });
                ruleRedoStack.length = 0;
            }
            pendingRulesetChange = null;
            renderHistoryList();
        }
        refreshRuleList();
    });
    model.on("change:import_issues", () => {
        // A rejected ruleset changed nothing, so it never enters the history.
        if ((model.get("import_issues") || {}).kind === "error") {
            pendingRulesetChange = null;
        }
        if (activeIssuesRender && overlay.classList.contains("show")) {
            activeIssuesRender();
        }
    });

    // ------------------------------------------------------------------
    // Rule change history
    // ------------------------------------------------------------------
    function ruleStateSnapshot() {
        return { disabled: [...disabledRules], ruleset: rulesetRef };
    }
    // Records a change which is applied entirely in the browser (a rule toggled
    // on or off); ruleset changes are recorded when the backend confirms them.
    function recordRuleChange(label) {
        ruleHistory.push({ time: new Date(), label, state: ruleStateSnapshot() });
        // A new change invalidates anything that was undone.
        ruleRedoStack.length = 0;
        renderHistoryList();
    }
    function applyRuleState(state) {
        disabledRules.clear();
        for (const id of state.disabled) disabledRules.add(id);
    }
    // Restores a snapshot, asking the backend to reinstate the ruleset when the
    // ruleset itself (not just the enabled/disabled state) changed.
    function applyRuleSnapshot(state, message) {
        applyRuleState(state);
        renderHistoryList();
        if (state.ruleset !== rulesetRef) {
            pendingRulesetChange = { ruleset: state.ruleset, history: null };
            runAction("set_ruleset", {
                source: state.ruleset.source,
                rules: state.ruleset.rules,
                disabled_rules: [...disabledRules],
                message,
            });
        } else {
            // A local toggle: nothing has to be written back, the editor just
            // returns to the other enabled/disabled state.
            model.set("status", { message, kind: "info" });
            model.save_changes();
            refreshRuleList();
        }
    }
    function undoRuleChange() {
        const entry = ruleHistory.pop();
        if (!entry) return;
        // The state being left behind is what a redo restores.
        ruleRedoStack.push({
            time: new Date(), label: entry.label, state: ruleStateSnapshot(),
        });
        applyRuleSnapshot(entry.state, `Undid: ${entry.label}`);
    }
    function redoRuleChange() {
        const entry = ruleRedoStack.pop();
        if (!entry) return;
        ruleHistory.push({
            time: new Date(), label: entry.label, state: ruleStateSnapshot(),
        });
        applyRuleSnapshot(entry.state, `Redid: ${entry.label}`);
    }
    function resetRulesToDefault() {
        const target = { source: "default", rules: null };
        pendingRulesetChange = {
            ruleset: target,
            history: { label: "Reverted to the default rules", state: ruleStateSnapshot() },
        };
        disabledRules.clear();
        runAction("set_ruleset", {
            source: "default",
            rules: null,
            disabled_rules: [],
            message: "Reverted to the default rules.",
        });
    }

    function closeHistory() { historyOverlay.classList.remove("show"); }

    function renderHistoryList() {
        if (!historyOverlay.classList.contains("show")) return;
        const body = historyOverlay.querySelector("[data-history-list]");
        if (!body) return;
        clear(body);
        if (ruleHistory.length === 0) {
            const empty = document.createElement("div");
            empty.className = "slls-bpa-empty";
            empty.textContent = "No rule changes have been made yet.";
            body.appendChild(empty);
            return;
        }
        // Newest first.
        for (let i = ruleHistory.length - 1; i >= 0; i--) {
            const entry = ruleHistory[i];
            const row = document.createElement("div");
            row.className = "slls-bpa-history-row";
            const index = document.createElement("span");
            index.className = "slls-bpa-history-index";
            index.textContent = String(i + 1);
            row.appendChild(index);
            const main = document.createElement("div");
            main.className = "slls-bpa-history-main";
            const label = document.createElement("div");
            label.className = "slls-bpa-history-label";
            label.textContent = entry.label;
            main.appendChild(label);
            const time = document.createElement("div");
            time.className = "slls-bpa-history-time";
            time.textContent = entry.time.toLocaleTimeString();
            main.appendChild(time);
            row.appendChild(main);
            if (i === ruleHistory.length - 1) {
                const latest = document.createElement("span");
                latest.className = "slls-bpa-history-latest";
                latest.textContent = "Most recent";
                row.appendChild(latest);
            }
            body.appendChild(row);
        }
    }

    function openHistory() {
        clear(historyOverlay);
        const modal = document.createElement("div");
        modal.className = "slls-bpa-modal slls-bpa-staged-modal";

        const heading = document.createElement("h2");
        heading.textContent = "Rule change history";
        modal.appendChild(heading);
        const sub = document.createElement("div");
        sub.className = "slls-bpa-modal-sub";
        sub.textContent = "The changes made to the rules in this session, newest first. "
            + "Use Undo to step back through them.";
        modal.appendChild(sub);

        const body = document.createElement("div");
        body.className = "slls-bpa-staged";
        body.setAttribute("data-history-list", "");
        modal.appendChild(body);

        const footer = document.createElement("div");
        footer.className = "slls-bpa-modal-footer";
        const undoLast = makeButton("", "slls-bpa-btn-sm slls-bpa-btn-icon-sm", ICON.undo);
        const redoLast = makeButton("", "slls-bpa-btn-sm slls-bpa-btn-icon-sm", ICON.redo);
        function syncHistoryFooter() {
            undoLast.disabled = ruleHistory.length === 0;
            undoLast.title = ruleHistory.length === 0
                ? "No rule changes to undo"
                : `Undo: ${ruleHistory[ruleHistory.length - 1].label}`;
            undoLast.setAttribute("aria-label", undoLast.title);
            redoLast.disabled = ruleRedoStack.length === 0;
            redoLast.title = ruleRedoStack.length === 0
                ? "No undone rule changes to redo"
                : `Redo: ${ruleRedoStack[ruleRedoStack.length - 1].label}`;
            redoLast.setAttribute("aria-label", redoLast.title);
        }
        syncHistoryFooter();
        undoLast.addEventListener("click", () => {
            undoRuleChange();
            syncHistoryFooter();
        });
        redoLast.addEventListener("click", () => {
            redoRuleChange();
            syncHistoryFooter();
        });
        footer.appendChild(undoLast);
        footer.appendChild(redoLast);
        const closeBtn = makeButton("Close", "slls-bpa-btn-sm");
        closeBtn.addEventListener("click", closeHistory);
        footer.appendChild(closeBtn);
        modal.appendChild(footer);

        historyOverlay.appendChild(modal);
        historyOverlay.classList.add("show");
        renderHistoryList();
    }

    // Downloads the effective ruleset in the Best Practice Rules JSON format.
    const SEVERITY_CODE = { Error: 3, Warning: 2, Info: 1 };
    function exportRuleset() {
        const entries = (model.get("rules") || []).map((r) => {
            const entry = {
                ID: String(r.id || "").toUpperCase(),
                Name: r.name,
                Category: r.category,
                Description: r.description || "",
                Severity: SEVERITY_CODE[r.severity] || 2,
                Scope: (r.scopes || []).join(", "),
                Expression: r.expression || "",
                Url: r.url || null,
                Enabled: !disabledRules.has(r.id),
            };
            if (r.fixExpression) entry.FixExpression = r.fixExpression;
            return entry;
        });
        const blob = new Blob([JSON.stringify(entries, null, 2)],
            { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const anchor = document.createElement("a");
        anchor.href = url;
        anchor.download = "BPARules.json";
        anchor.rel = "noopener";
        anchor.style.display = "none";
        document.body.appendChild(anchor);
        anchor.click();
        // Revoking synchronously can cancel the download in embedded hosts.
        window.setTimeout(() => {
            anchor.remove();
            URL.revokeObjectURL(url);
        }, 1000);
    }

    function openRulesPanel() {
        clear(overlay);
        const modal = document.createElement("div");
        modal.className = "slls-bpa-modal";

        const heading = document.createElement("h2");
        heading.appendChild(iconSpan(ICON.sliders));
        const headingText = document.createElement("span");
        headingText.textContent = "Rule Editor";
        heading.appendChild(headingText);
        modal.appendChild(heading);
        const sub = document.createElement("div");
        sub.className = "slls-bpa-modal-sub";
        sub.textContent = "Turn rules on or off. Disabled rules are skipped on the next run. "
            + "You can also import a ruleset from a .json file, or export the current one.";
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

        const resetBtn = makeButton("", "slls-bpa-btn-sm slls-bpa-btn-icon-sm", ICON.reset);
        resetBtn.title = "Restore the default rules (discards any imported ruleset)";
        resetBtn.setAttribute("aria-label", resetBtn.title);
        resetBtn.addEventListener("click", () => resetRulesToDefault());
        bar.appendChild(resetBtn);

        const undoBtn = makeButton("", "slls-bpa-btn-sm slls-bpa-btn-icon-sm", ICON.undo);
        undoBtn.title = "Undo the last rule change";
        undoBtn.setAttribute("aria-label", undoBtn.title);
        undoBtn.addEventListener("click", () => undoRuleChange());
        bar.appendChild(undoBtn);

        const redoBtn = makeButton("", "slls-bpa-btn-sm slls-bpa-btn-icon-sm", ICON.redo);
        redoBtn.title = "Redo the last undone rule change";
        redoBtn.setAttribute("aria-label", redoBtn.title);
        redoBtn.addEventListener("click", () => redoRuleChange());
        bar.appendChild(redoBtn);

        const historyBtn = makeButton("", "slls-bpa-btn-sm slls-bpa-btn-icon-sm", ICON.history);
        historyBtn.title = "Show the rule change history";
        historyBtn.setAttribute("aria-label", historyBtn.title);
        historyBtn.addEventListener("click", () => openHistory());
        bar.appendChild(historyBtn);

        function renderRuleCtrls() {
            undoBtn.disabled = ruleHistory.length === 0;
            undoBtn.title = ruleHistory.length === 0
                ? "No rule changes to undo"
                : `Undo: ${ruleHistory[ruleHistory.length - 1].label}`;
            redoBtn.disabled = ruleRedoStack.length === 0;
            redoBtn.title = ruleRedoStack.length === 0
                ? "No undone rule changes to redo"
                : `Redo: ${ruleRedoStack[ruleRedoStack.length - 1].label}`;
            historyBtn.title = ruleHistory.length === 0
                ? "Show the rule change history (no changes yet)"
                : `Show the rule change history (${plural(ruleHistory.length, "change")})`;
        }

        // ---- Import / export the ruleset as .json ----
        const fileInput = document.createElement("input");
        fileInput.type = "file";
        fileInput.accept = "application/json,.json";
        fileInput.style.display = "none";
        fileInput.addEventListener("change", () => {
            const file = fileInput.files && fileInput.files[0];
            if (!file) return;
            file.text().then((text) => {
                let parsed;
                try {
                    parsed = JSON.parse(text);
                } catch (e) {
                    setIssues("error", "The file is not valid JSON.",
                        [String((e && e.message) || e)]);
                    model.set("status", {
                        message: "The file is not valid JSON.", kind: "error" });
                    model.save_changes();
                    return;
                }
                // The panel stays open: the list re-renders once the imported
                // ruleset arrives, and any problems are reported above it.
                setIssues("", "", []);
                pendingRulesetChange = {
                    ruleset: { source: "custom", rules: parsed },
                    history: {
                        label: `Imported the ruleset "${file.name}"`,
                        state: ruleStateSnapshot(),
                    },
                };
                runAction("import_rules", { rules: parsed });
            }).catch(() => {
                setIssues("error", "The file could not be read.", []);
                model.set("status", { message: "The file could not be read.", kind: "error" });
                model.save_changes();
            });
            fileInput.value = "";
        });
        bar.appendChild(fileInput);

        const importBtn = makeButton("", "slls-bpa-btn-sm slls-bpa-btn-icon-sm", ICON.upload);
        importBtn.title = "Import a ruleset from a .json file";
        importBtn.setAttribute("aria-label", importBtn.title);
        importBtn.addEventListener("click", () => fileInput.click());
        bar.appendChild(importBtn);

        const exportBtn = makeButton("", "slls-bpa-btn-sm slls-bpa-btn-icon-sm", ICON.download);
        exportBtn.title = "Export the current ruleset as a .json file";
        exportBtn.setAttribute("aria-label", exportBtn.title);
        exportBtn.addEventListener("click", () => exportRuleset());
        bar.appendChild(exportBtn);

        modal.appendChild(bar);

        // Problems found in an imported ruleset, so they can be corrected in the file.
        const issuesBox = document.createElement("div");
        issuesBox.className = "slls-bpa-issues";
        modal.appendChild(issuesBox);

        function renderIssues() {
            clear(issuesBox);
            const issues = model.get("import_issues") || {};
            const items = issues.items || [];
            if (items.length === 0 && !issues.title) {
                issuesBox.className = "slls-bpa-issues";
                return;
            }
            const kind = issues.kind === "error" ? "error" : "warning";
            issuesBox.className = `slls-bpa-issues show ${kind}`;

            const head = document.createElement("div");
            head.className = "slls-bpa-issues-head";
            head.appendChild(iconSpan(kind === "error" ? ICON.error : ICON.alert));
            const title = document.createElement("span");
            title.className = "slls-bpa-issues-title";
            title.textContent = issues.title
                || "The ruleset reported some problems.";
            head.appendChild(title);
            const dismiss = document.createElement("button");
            dismiss.type = "button";
            dismiss.className = "slls-bpa-issues-dismiss";
            dismiss.innerHTML = ICON.close;
            dismiss.title = "Dismiss";
            dismiss.setAttribute("aria-label", "Dismiss the reported problems");
            dismiss.addEventListener("click", () => setIssues("", "", []));
            head.appendChild(dismiss);
            issuesBox.appendChild(head);

            if (items.length > 0) {
                const list = document.createElement("ul");
                list.className = "slls-bpa-issues-list";
                for (const item of items) {
                    const li = document.createElement("li");
                    li.textContent = item;
                    list.appendChild(li);
                }
                issuesBox.appendChild(list);
            }
        }

        function setIssues(kind, title, items) {
            model.set("import_issues",
                items.length === 0 && !title ? {} : { kind, title, items });
            model.save_changes();
            renderIssues();
        }

        const countLine = document.createElement("div");
        countLine.className = "slls-bpa-rule-count";
        modal.appendChild(countLine);

        const list = document.createElement("div");
        list.className = "slls-bpa-rulelist";
        modal.appendChild(list);

        // The number of rules currently passing the search filter.
        let shownCount = 0;

        function renderRuleCount() {
            const rules = model.get("rules") || [];
            if (rules.length === 0) {
                countLine.textContent = "";
                return;
            }
            const enabled = rules.filter((r) => !disabledRules.has(r.id)).length;
            const scope = shownCount === rules.length
                ? plural(rules.length, "rule")
                : `${shownCount} of ${plural(rules.length, "rule")} shown`;
            countLine.textContent = `${scope} \u2022 ${enabled} enabled`;
        }

        function renderRuleList() {
            clear(list);
            const term = ruleSearch.value.trim().toLowerCase();
            const rules = model.get("rules") || [];
            if (rules.length === 0) {
                // The catalog is fetched the first time the editor is opened.
                shownCount = 0;
                renderRuleCount();
                const empty = document.createElement("div");
                empty.className = "slls-bpa-empty";
                empty.textContent = "Loading rules\u2026";
                list.appendChild(empty);
                return;
            }
            const shown = rules.filter((r) =>
                !term
                || r.name.toLowerCase().includes(term)
                || r.category.toLowerCase().includes(term));
            shownCount = shown.length;
            renderRuleCount();
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
                    recordRuleChange(
                        `${box.checked ? "Enabled" : "Disabled"} "${rule.name}"`);
                    if (box.checked) disabledRules.delete(rule.id);
                    else disabledRules.add(rule.id);
                    renderRuleCount();
                    renderRuleCtrls();
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
                    // Mirrors the "Apply fix" button shown on a violation group.
                    const badge = document.createElement("span");
                    badge.className = "slls-bpa-fix-badge";
                    badge.appendChild(iconSpan(ICON.wand));
                    const badgeText = document.createElement("span");
                    badgeText.textContent = "Apply fix";
                    badge.appendChild(badgeText);
                    badge.title = "This rule can be fixed automatically";
                    name.appendChild(badge);
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
                if (rule.expression || rule.fixExpression) {
                    // The code behind a rule is long, so it stays collapsed until asked for.
                    const isOpen = expandedRuleExprs.has(rule.id);
                    const toggle = document.createElement("button");
                    toggle.type = "button";
                    toggle.className = "slls-bpa-rule-exprtoggle";
                    toggle.setAttribute("aria-expanded", String(isOpen));
                    const caret = iconSpan(ICON.caret, `slls-bpa-caret${isOpen ? " open" : ""}`);
                    toggle.appendChild(caret);
                    const toggleText = document.createElement("span");
                    toggleText.textContent = isOpen ? "Hide expression" : "Show expression";
                    toggle.appendChild(toggleText);
                    body.appendChild(toggle);

                    const exprWrap = document.createElement("div");
                    exprWrap.hidden = !isOpen;
                    if (rule.expression) {
                        exprWrap.appendChild(codeBlock(
                            rule.expression, "slls-bpa-rule-expr", "What this rule checks", ICON.code));
                    }
                    if (rule.fixExpression) {
                        exprWrap.appendChild(codeBlock(
                            rule.fixExpression,
                            "slls-bpa-rule-expr slls-bpa-rule-fixexpr",
                            "What the automatic fix does",
                            ICON.wand));
                    }
                    body.appendChild(exprWrap);

                    toggle.addEventListener("click", () => {
                        const open = !expandedRuleExprs.has(rule.id);
                        if (open) expandedRuleExprs.add(rule.id);
                        else expandedRuleExprs.delete(rule.id);
                        exprWrap.hidden = !open;
                        caret.classList.toggle("open", open);
                        toggle.setAttribute("aria-expanded", String(open));
                        toggleText.textContent = open ? "Hide expression" : "Show expression";
                    });
                }
                row.appendChild(body);
                list.appendChild(row);
            }
        }

        ruleSearch.addEventListener("input", renderRuleList);
        enableAll.addEventListener("click", () => {
            if (disabledRules.size === 0) return;
            recordRuleChange("Enabled all rules");
            disabledRules.clear();
            renderRuleList();
        });
        disableAll.addEventListener("click", () => {
            const rules = model.get("rules") || [];
            if (rules.length > 0 && rules.every((r) => disabledRules.has(r.id))) return;
            recordRuleChange("Disabled all rules");
            for (const r of rules) disabledRules.add(r.id);
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
        activeRuleListRender = renderRuleList;
        activeIssuesRender = renderIssues;
        activeRuleCtrlsRender = renderRuleCtrls;
        renderIssues();
        renderRuleCtrls();
        renderRuleList();
        // The catalog is loaded on demand, so ask for it the first time.
        if ((model.get("rules") || []).length === 0) runAction("load_rules", {});
    }

    // ------------------------------------------------------------------
    // Screen switching
    // ------------------------------------------------------------------
    function renderScreen() {
        const screen = model.get("screen") || "select";
        selectScreen.classList.toggle("show", screen === "select");
        resultsScreen.classList.toggle("show", screen === "results");
        bulkScreen.classList.toggle("show", screen === "bulk");
        changeModelBtn.style.display = screen === "select" ? "none" : "";
        rerunBtn.style.display = screen === "select" ? "none" : "";

        if (screen === "select") {
            subtitle.textContent = "Scan semantic models against the best practice rules.";
            closeBulkDetail();
        } else if (screen === "bulk") {
            const n = (model.get("bulk_results") || []).length;
            subtitle.textContent = `${plural(n, "semantic model")} analyzed`;
            renderBulk();
        } else {
            closeBulkDetail();
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
        if (model.get("screen") !== "bulk") {
            activeViolations = model.get("violations") || [];
            if (model.get("screen") === "results") renderResults();
        }
    });
    model.on("change:bulk_results", () => {
        if (model.get("screen") === "bulk") renderBulk();
    });
    model.on("change:fix_preview", () => {
        const preview = model.get("fix_preview") || {};
        if (preview.ruleName && preview.ruleName === fixRule) {
            fixSelected.clear();
            for (const item of (preview.items || [])) {
                if (!isStaged(preview.ruleName, item.objectName)) {
                    fixSelected.add(item.objectName);
                }
            }
        }
        refreshViolations();
    });
    model.on("change:screen", renderScreen);
    model.on("change:render_token", () => {
        // A completed scan always refreshes the view, even when the resulting
        // violation list happens to be identical to the previous one.
        if (model.get("screen") === "bulk") {
            closeBulkDetail();
        } else {
            activeViolations = model.get("violations") || [];
        }
        renderScreen();
    });
    model.on("change:disabled_rules", () => {
        // An imported ruleset brings its own enabled/disabled state, and the scans
        // are driven from the local set, so it has to follow.
        disabledRules.clear();
        for (const id of (model.get("disabled_rules") || [])) disabledRules.add(id);
        refreshRuleList();
    });

    renderWorkspaces();
    renderDatasets();
    updateSelectState();
    renderProgress();
    renderStaged();
    activeViolations = model.get("violations") || [];
    renderScreen();

    // The workspace / semantic model lists are fetched after this first render
    // (through the run/observe channel) so that the widget appears immediately
    // instead of waiting for the tenant workspace list.
    if ((model.get("workspaces") || []).length === 0) {
        runAction("load_lists", { workspace_id: model.get("workspace_id") || "" });
    }
}
export default { render };
"""


_WIDGET_JS = (
    _WIDGET_JS.replace("__SLLS_ICON_SHIELD_CHECK__", _UI_ICONS["shield_check"])
    .replace("__SLLS_ICON_SUN__", _UI_ICONS["sun"])
    .replace("__SLLS_ICON_MOON__", _UI_ICONS["moon"])
    .replace("__SLLS_ICON_SWAP__", _UI_ICONS["swap"])
    .replace("__SLLS_ICON_REFRESH__", _UI_ICONS["refresh"])
    .replace("__SLLS_ICON_SEARCH__", _UI_ICONS["search"])
    .replace("__SLLS_ICON_WAND__", _UI_ICONS["wand"])
    .replace("__SLLS_ICON_EYE_OFF__", _UI_ICONS["eye_off"])
    .replace("__SLLS_ICON_EYE__", _UI_ICONS["eye"])
    .replace("__SLLS_ICON_SAVE__", _UI_ICONS["save"])
    .replace("__SLLS_ICON_UNDO__", _UI_ICONS["undo"])
    .replace("__SLLS_ICON_REDO__", _UI_ICONS["redo"])
    .replace("__SLLS_ICON_HISTORY__", _UI_ICONS["history"])
    .replace("__SLLS_ICON_RESET__", _UI_ICONS["reset"])
    .replace("__SLLS_ICON_UPLOAD__", _UI_ICONS["upload"])
    .replace("__SLLS_ICON_DOWNLOAD__", _UI_ICONS["download"])
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
    .replace("__SLLS_ICON_SLIDERS__", _UI_ICONS["sliders"])
    .replace("__SLLS_ICON_ACTIVITY__", _UI_ICONS["activity"])
    .replace("__SLLS_ICON_CODE__", _UI_ICONS["code"])
    .replace("__SLLS_ICON_PENCIL__", _UI_ICONS["pencil"])
    .replace("__SLLS_ICON_TEXT_TYPE__", _UI_ICONS["text_type"])
    .replace("__SLLS_ICON_EXPAND_ROWS__", _UI_ICONS["expand_rows"])
    .replace("__SLLS_ICON_COLLAPSE_ROWS__", _UI_ICONS["collapse_rows"])
    .replace("__SLLS_ICON_FULLSCREEN__", _UI_ICONS["fullscreen"])
    .replace("__SLLS_ICON_FULLSCREEN_EXIT__", _UI_ICONS["fullscreen_exit"])
)


@log
def bpa(
    dataset: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    rules: Optional[pd.DataFrame | list[dict] | dict] = None,
    check_dependencies: bool = True,
    dark_mode: bool = False,
    return_dataframe: bool = False,
    export: bool = False,
    export_table: str = "modelbparesults",
    export_lakehouse: Optional[str | UUID] = None,
    export_workspace: Optional[str | UUID] = None,
):
    """
    Generates an interactive Best Practice Analyzer for semantic models.

    The analyzer scans one semantic model - or up to 10 semantic models across any
    number of workspaces - against the semantic model best practice rules, groups the
    violations by rule, and lets you preview and apply the automatic fix for the rules
    which support one. The rule editor can also import a custom ruleset from a .json
    file and export the current ruleset back to .json.

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
    rules : pandas.DataFrame | List[dict] | dict, default=None
        The rules to evaluate. Accepts either a pandas dataframe in the shape produced
        by :func:`sempy_labs.model_bpa_rules`, or a list of dictionaries in the
        `Best Practice Rules <https://github.com/microsoft/Analysis-Services/blob/master/BestPracticeRules/BPARules.json>`_
        JSON format, for example::

            [
                {
                    "ID": "AVOID_FLOATING_POINT_DATA_TYPES",
                    "Name": "Do not use floating point data types",
                    "Category": "Performance",
                    "Description": "...",
                    "Severity": 2,
                    "Scope": "Column",
                    "Expression": "lambda obj, tom: obj.DataType == TOM.DataType.Double",
                    "FixExpression": "column.DataType = TOM.DataType.Decimal",
                    "Enabled": true
                }
            ]

        Because the rule logic is compiled in Python, each entry is matched to a
        built-in rule by its ``ID`` or ``Name`` (a leading ``[Category]`` prefix is
        ignored). The ruleset is validated first and is rejected as a whole if any
        entry does not match a built-in rule or has a malformed property; the
        problems found are reported so they can be corrected. ``Category``,
        ``Severity`` (1 = Info, 2 = Warning, 3 = Error, or the name), ``Description``,
        ``Url``, ``Scope`` and ``Enabled`` may be overridden. ``Expression`` holds the
        source of the rule's predicate and ``FixExpression`` the code its automatic
        fix runs; both are informational and are always taken from the current
        built-in definition.
        Defaults to None which uses the built-in rules.
    check_dependencies : bool, default=True
        If True, leverages the model dependencies from
        :func:`sempy_labs.get_model_calc_dependencies` to evaluate the rules. Set this
        parameter to False if running the rules against a semantic model in a shared
        capacity.
    dark_mode : bool, default=False
        If True, renders the analyzer with a dark color theme. If False, renders with a
        light color theme.
    return_dataframe : bool, default=False
        If True, no user interface is shown; the rule violations of the semantic model
        given by the 'dataset' parameter are returned as a pandas dataframe instead.
    export : bool, default=False
        If True, no user interface is shown; the rule violations of the semantic model
        given by the 'dataset' parameter are saved to a delta table in a lakehouse
        instead. The exported rows also carry the Capacity Name, Capacity Id, Workspace
        Name, Workspace Id, Dataset Name, Dataset Id, Configured By, Timestamp and
        RunId columns.
    export_table : str, default="modelbparesults"
        The name of the delta table the results are appended to.
    export_lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID to export to.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    export_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID of the lakehouse to export to.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame | None
        A pandas dataframe of the rule violations if 'return_dataframe' is True,
        otherwise None.
    """

    import pandas as pd
    import sempy.fabric as fabric

    from sempy_labs._helper_functions import (
        resolve_workspace_name_and_id,
        resolve_dataset_name_and_id,
    )
    from sempy_labs.semantic_model._bpa_engine import (
        apply_fixes,
        normalize_rules,
        parse_rules_json,
        preview_fixes,
        rules_payload,
        scan_model,
        validate_rules_json,
    )

    # `sempy_labs.tom`, the rules module and the dependency graph all pull in the
    # Analysis Services client, which is slow to initialize. They are imported at
    # the point of use so that displaying the analyzer stays fast.
    def _connect(dataset_id, workspace_id, readonly):
        from sempy_labs.tom import connect_semantic_model

        return connect_semantic_model(
            dataset=dataset_id, workspace=workspace_id, readonly=readonly
        )

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

    def _api_items(request):
        """Collects ``{id, name}`` entries from a paginated Fabric list endpoint.

        These endpoints only return the item identity, which makes them
        considerably faster than the equivalent semantic-link dataframes.
        """

        from sempy_labs._helper_functions import _base_api

        responses = _base_api(request=request, uses_pagination=True, client="fabric_sp")
        return [
            {"id": str(v.get("id")), "name": str(v.get("displayName"))}
            for r in responses
            for v in r.get("value", [])
            if v.get("id")
        ]

    def _df_items(df, id_names, name_names):
        id_col, name_col = _pick_columns(df, id_names, name_names)
        if id_col is None or name_col is None:
            return []
        return [
            {"id": str(r[id_col]), "name": str(r[name_col])} for _, r in df.iterrows()
        ]

    def _list_workspaces_payload():
        try:
            rows = _api_items("/v1/workspaces")
        except Exception:
            rows = []
        if not rows:
            try:
                rows = _df_items(fabric.list_workspaces(), ["Id", "ID"], ["Name"])
            except Exception:
                rows = []
        if not rows:
            return [{"id": initial_ws_id, "name": str(initial_ws_name or "")}]
        return sorted(rows, key=lambda x: x["name"].lower())

    def _list_datasets_payload(workspace_id):
        if not workspace_id:
            return []
        try:
            rows = _api_items(f"/v1/workspaces/{workspace_id}/semanticModels")
        except Exception:
            rows = []
        if not rows:
            try:
                rows = _df_items(
                    fabric.list_datasets(workspace=workspace_id, mode="rest"),
                    ["Dataset Id", "Dataset ID", "Id"],
                    ["Dataset Name", "Name"],
                )
            except Exception:
                rows = []
        return sorted(rows, key=lambda x: x["name"].lower())

    # The active ruleset. A dataframe is used as-is; JSON entries are matched to the
    # built-in rules (which supply the logic) each time the defaults are rebuilt.
    # "initial" keeps whatever was supplied to the function so that an undo in the
    # rule editor can restore it.
    ruleset = {"custom": rules, "initial": rules}

    def _default_rules(workspace_id, dataset_id):
        """Builds the built-in rules, optionally including the calc-dependency graph."""

        from sempy_labs._model_bpa_rules import model_bpa_rules

        dependencies = pd.DataFrame(columns=_DEPENDENCY_COLUMNS)
        if check_dependencies and workspace_id and dataset_id:
            try:
                from sempy_labs._model_dependencies import get_model_calc_dependencies

                dependencies = get_model_calc_dependencies(
                    dataset=dataset_id, workspace=workspace_id
                )
            except Exception:
                # Dependency discovery is unavailable on shared capacities.
                pass

        return model_bpa_rules(dependencies=dependencies)

    def _build_rules(workspace_id, dataset_id):
        defaults = _default_rules(workspace_id, dataset_id)
        return normalize_rules(ruleset["custom"], defaults)

    # Building the catalog initializes the Analysis Services client, which is slow.
    # It is therefore created on first use rather than when the widget is displayed.
    _catalog_cache = {}

    def _catalog():
        if "value" not in _catalog_cache:
            _catalog_cache["value"] = _default_rules(None, None)
        return _catalog_cache["value"]

    # Per-model rules, cached so that scanning, previewing and applying a fix for
    # the same model do not rebuild the (expensive) calc-dependency graph.
    rules_cache = {}

    def _rules_for(workspace_id, dataset_id):
        key = (str(workspace_id or ""), str(dataset_id or ""))
        rules = rules_cache.get(key)
        if rules is None:
            rules = _build_rules(workspace_id, dataset_id)
            rules_cache[key] = rules
        return rules

    def _cancelled():
        """
        Reports whether the user asked to stop the run that is in progress.

        This only sees a cancel request on hosts which deliver comm messages while
        a cell is executing. Deliberately no attempt is made to pump the kernel's
        event loop from here: doing so blocks waiting for the next message, which
        hangs the run and can re-enter the action handler.
        """

        widget = _widget_ref.get("value")
        return bool(widget is not None and widget.cancel_requested)

    def _scan(workspace_id, dataset_id, disabled_rules):
        rules = _rules_for(workspace_id, dataset_id)
        with _connect(dataset_id, workspace_id, True) as tom:
            return scan_model(tom, rules, disabled_rules, should_cancel=_cancelled)

    # Set once the widget exists; the dataframe path below runs without one.
    _widget_ref = {}

    _VIOLATION_COLUMNS = [
        "Category",
        "Rule Name",
        "Severity",
        "Object Type",
        "Object Name",
        "Description",
        "URL",
    ]

    def _violations_dataframe(violations):
        return pd.DataFrame(
            [
                {
                    "Category": v["category"],
                    "Rule Name": v["ruleName"],
                    "Severity": v["severity"],
                    "Object Type": v["objectType"],
                    "Object Name": v["objectName"],
                    "Description": v["description"],
                    "URL": v["url"],
                }
                for v in violations
            ],
            columns=_VIOLATION_COLUMNS,
        )

    def _require_dataset(parameter):
        if not initial_ds_id:
            raise ValueError(
                f"The 'dataset' parameter is required when '{parameter}' is True."
            )

    def _export_violations(df):
        """Appends the violations to a delta table, stamped with the run's context."""

        import datetime

        import sempy_labs._icons as icons
        from sempy_labs._helper_functions import (
            _get_column_aggregate,
            resolve_workspace_capacity,
            save_as_delta_table,
        )
        from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables

        # A new run gets the next id, so successive exports stay comparable.
        tables = get_lakehouse_tables(
            lakehouse=export_lakehouse, workspace=export_workspace
        )
        if tables[tables["Table Name"] == export_table].empty:
            run_id = 1
        else:
            run_id = (
                _get_column_aggregate(
                    table_name=export_table,
                    lakehouse=export_lakehouse,
                    workspace=export_workspace,
                )
                + 1
            )

        datasets = fabric.list_datasets(workspace=initial_ws_id, mode="rest")
        matching = datasets[datasets["Dataset Id"] == initial_ds_id]
        configured_by = "" if matching.empty else matching["Configured By"].iloc[0]
        capacity_id, capacity_name = resolve_workspace_capacity(workspace=initial_ws_id)

        df = df.copy()
        df["Capacity Name"] = capacity_name
        df["Capacity Id"] = capacity_id
        df["Workspace Name"] = str(initial_ws_name or "")
        df["Workspace Id"] = initial_ws_id
        df["Dataset Name"] = initial_ds_name
        df["Dataset Id"] = initial_ds_id
        df["Configured By"] = configured_by
        df["Timestamp"] = datetime.datetime.now()
        df["RunId"] = run_id
        df["RunId"] = df["RunId"].astype("int")

        df = df[list(icons.bpa_schema.keys())]
        save_as_delta_table(
            dataframe=df,
            delta_table_name=export_table,
            write_mode="append",
            schema={k.replace(" ", "_"): v for k, v in icons.bpa_schema.items()},
            merge_schema=True,
            lakehouse=export_lakehouse,
            workspace=export_workspace,
        )

    if return_dataframe:
        _require_dataset("return_dataframe")
        return _violations_dataframe(_scan(initial_ws_id, initial_ds_id, None))

    if export:
        _require_dataset("export")
        _export_violations(
            _violations_dataframe(_scan(initial_ws_id, initial_ds_id, None))
        )
        return

    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "The 'bpa' function requires the 'anywidget' package. "
            "Install it with: pip install anywidget"
        ) from e

    from IPython.display import display

    class _BestPracticeAnalyzerWidget(anywidget.AnyWidget):
        _esm = _WIDGET_JS
        _css = _WIDGET_CSS

        screen = traitlets.Unicode("select").tag(sync=True)
        workspaces = traitlets.List().tag(sync=True)
        datasets = traitlets.List().tag(sync=True)
        workspace_datasets = traitlets.Dict().tag(sync=True)
        workspace_id = traitlets.Unicode("").tag(sync=True)
        workspace_name = traitlets.Unicode("").tag(sync=True)
        dataset_id = traitlets.Unicode("").tag(sync=True)
        dataset_name = traitlets.Unicode("").tag(sync=True)
        rules = traitlets.List().tag(sync=True)
        disabled_rules = traitlets.List().tag(sync=True)
        import_issues = traitlets.Dict().tag(sync=True)
        violations = traitlets.List().tag(sync=True)
        bulk_results = traitlets.List().tag(sync=True)
        fix_preview = traitlets.Dict().tag(sync=True)
        progress = traitlets.Dict().tag(sync=True)
        render_token = traitlets.Int(0).tag(sync=True)
        max_bulk_models = traitlets.Int(_MAX_BULK_MODELS).tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        busy = traitlets.Bool(False).tag(sync=True)
        cancel_requested = traitlets.Bool(False).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)

    # Rule metadata for the editor. When the defaults are used it is loaded on
    # demand (see the "load_rules" action) so that nothing expensive happens before
    # the workspace picker is on screen.
    _initial_disabled = []
    if rules is None:
        _initial_rules = []
    elif isinstance(rules, pd.DataFrame):
        _initial_rules = rules_payload(rules)
    else:
        # A custom ruleset has to be matched against the built-in rules up front,
        # otherwise the rules it disables would not be known to the first scan.
        import sempy_labs._icons as icons

        _entries = rules if isinstance(rules, list) else (rules.get("rules") or [])
        _errors, _warnings = validate_rules_json(_entries, _catalog())
        _problems = _errors + _warnings
        if _problems:
            raise ValueError(
                f"{icons.red_dot} The 'rules' parameter is not a valid ruleset and "
                "was not used:\n- " + "\n- ".join(_problems)
            )
        _, _initial_disabled = parse_rules_json(_entries, _catalog())
        _initial_rules = rules_payload(normalize_rules(ruleset["custom"], _catalog()))

    # Nothing is fetched before the widget is displayed: the workspace / semantic
    # model lists are requested by the frontend right after the first render, so
    # the analyzer appears immediately.
    widget = _BestPracticeAnalyzerWidget(
        workspace_id=initial_ws_id,
        workspace_name=str(initial_ws_name or ""),
        dataset_id=initial_ds_id,
        dataset_name=initial_ds_name,
        rules=_initial_rules,
        disabled_rules=_initial_disabled,
        dark_mode=bool(dark_mode),
    )
    _widget_ref["value"] = widget

    def _handle_load_rules(payload):
        if widget.rules:
            return
        widget.rules = rules_payload(normalize_rules(ruleset["custom"], _catalog()))

    def _handle_list_datasets(payload):
        widget.datasets = _list_datasets_payload(payload.get("workspace_id"))

    def _handle_load_lists(payload):
        """Initial (deferred) load of the workspace and semantic model lists."""

        workspace_id = str(payload.get("workspace_id") or "")
        if workspace_id:
            # The current workspace and its models are published first, so a
            # model can be picked while the tenant workspace list is still
            # loading.
            widget.workspaces = [
                {"id": workspace_id, "name": str(initial_ws_name or "")}
            ]
            widget.datasets = _list_datasets_payload(workspace_id)
        widget.workspaces = _list_workspaces_payload()

    def _handle_load_workspace_datasets(payload):
        workspace_id = str(payload.get("workspace_id") or "")
        if not workspace_id:
            return
        # A new dict is assigned so the traitlet change event fires.
        loaded = dict(widget.workspace_datasets)
        loaded[workspace_id] = _list_datasets_payload(workspace_id)
        widget.workspace_datasets = loaded

    def _handle_reload_lists(payload):
        """Refetches the workspaces and the model lists currently on screen."""

        workspace_id = str(payload.get("workspace_id") or "")
        widget.workspaces = _list_workspaces_payload()
        widget.datasets = _list_datasets_payload(workspace_id) if workspace_id else []
        # Only the workspaces already expanded in the multi-model picker are
        # refetched; the rest are still loaded on demand.
        widget.workspace_datasets = {
            ws_id: _list_datasets_payload(ws_id) for ws_id in widget.workspace_datasets
        }
        widget.status = {
            "message": "Workspaces and semantic models reloaded.",
            "kind": "success",
        }

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
        widget.progress = {"done": 0, "total": 0, "current": widget.dataset_name}

        violations = _scan(workspace_id, dataset_id, payload.get("disabled_rules"))
        widget.progress = {}
        widget.violations = violations
        widget.fix_preview = {}
        widget.screen = "results"
        widget.render_token += 1
        if widget.cancel_requested:
            widget.status = {
                "message": (
                    "Analysis cancelled; showing the violations found before it "
                    "was stopped."
                ),
                "kind": "info",
            }

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
        total = len(targets)
        results = []
        for index, target in enumerate(targets):
            if _cancelled():
                break
            entry = {
                "workspace_id": str(target.get("workspace_id") or ""),
                "workspace_name": str(target.get("workspace_name") or ""),
                "dataset_id": str(target.get("dataset_id") or ""),
                "dataset_name": str(target.get("dataset_name") or ""),
                "violations": [],
                "error": "",
            }
            # Reported before the scan so the progress bar names the model
            # currently being analyzed.
            widget.progress = {
                "done": index,
                "total": total,
                "current": entry["dataset_name"],
            }
            try:
                entry["violations"] = _scan(
                    entry["workspace_id"], entry["dataset_id"], disabled_rules
                )
            except Exception as e:
                entry["error"] = f"Could not analyze this model: {e}"
            results.append(entry)

        widget.progress = {"done": total, "total": total, "current": ""}
        widget.bulk_results = results
        widget.screen = "bulk"
        widget.render_token += 1
        widget.progress = {}
        total_violations = sum(len(r["violations"]) for r in results)
        cancelled = widget.cancel_requested
        widget.status = {
            "message": (
                ("Analysis cancelled after " if cancelled else "Analyzed ")
                + f"{len(results)} of {total} semantic model(s); "
                f"{total_violations} violation(s) found."
            ),
            "kind": "info",
        }

    def _handle_preview_fix(payload):
        rule_name = payload.get("rule_name")
        if not rule_name:
            return
        workspace_id = str(payload.get("workspace_id") or widget.workspace_id)
        dataset_id = str(payload.get("dataset_id") or widget.dataset_id)
        items = []
        try:
            rules = _rules_for(workspace_id, dataset_id)
            with _connect(dataset_id, workspace_id, True) as tom:
                items = preview_fixes(tom, rules, rule_name)
        except Exception as e:
            widget.status = {
                "message": f"Could not compute the fix: {e}",
                "kind": "error",
            }
        # Always published, otherwise the panel would wait for a result forever.
        widget.fix_preview = {
            "ruleName": rule_name,
            "datasetId": dataset_id,
            "items": items,
        }

    def _handle_apply_staged_fixes(payload):
        """Applies every staged fix, grouped so each model is opened only once."""

        staged = payload.get("fixes") or []
        disabled_rules = payload.get("disabled_rules")
        if not staged:
            return

        grouped = {}
        for fix in staged:
            key = (
                str(fix.get("workspace_id") or ""),
                str(fix.get("dataset_id") or ""),
            )
            grouped.setdefault(key, {}).setdefault(
                str(fix.get("rule_name") or ""), []
            ).append(str(fix.get("object_name") or ""))

        applied = 0
        failures = []
        for (workspace_id, dataset_id), by_rule in grouped.items():
            try:
                rules = _rules_for(workspace_id, dataset_id)
                with _connect(dataset_id, workspace_id, False) as tom:
                    for rule_name, object_names in by_rule.items():
                        applied += apply_fixes(tom, rules, rule_name, object_names)
            except Exception as e:
                failures.append(str(e))
            finally:
                # The model changed, so its dependency graph must be rebuilt.
                rules_cache.pop((workspace_id, dataset_id), None)

        # Re-scan whatever is on screen so the violations reflect the saved model.
        if widget.screen == "bulk":
            results = [dict(r) for r in widget.bulk_results]
            for result in results:
                key = (result.get("workspace_id"), result.get("dataset_id"))
                if key not in grouped:
                    continue
                try:
                    result["violations"] = _scan(key[0], key[1], disabled_rules)
                    result["error"] = ""
                except Exception as e:
                    result["error"] = f"Could not analyze this model: {e}"
            widget.bulk_results = results
        elif widget.dataset_id:
            widget.violations = _scan(
                widget.workspace_id, widget.dataset_id, disabled_rules
            )

        widget.fix_preview = {}
        widget.render_token += 1
        if failures:
            widget.status = {
                "message": (
                    f"Applied {applied} fix(es); "
                    f"{len(failures)} model(s) failed: {'; '.join(failures)}"
                ),
                "kind": "error",
            }
        else:
            widget.status = {
                "message": (
                    f"Saved {applied} fix(es) across {len(grouped)} semantic model(s)."
                    if applied
                    else "No objects were changed."
                ),
                "kind": "success" if applied else "info",
            }

    def _handle_import_rules(payload):
        """Adopts a ruleset loaded from a .json file in the rule editor."""

        entries = payload.get("rules")
        if isinstance(entries, dict):
            entries = entries.get("rules") or []

        catalog = _catalog()
        errors, warnings = validate_rules_json(entries, catalog)
        problems = errors + warnings
        if problems:
            # An invalid ruleset is never adopted, even partially: the file has
            # to be corrected and imported again.
            count = len(problems)
            widget.import_issues = {
                "kind": "error",
                "title": (
                    f"The ruleset was not imported ({count} problem"
                    f"{'' if count == 1 else 's'} found)."
                ),
                "items": problems,
            }
            widget.status = {
                "message": (
                    "The ruleset was not imported. See the problems listed in the "
                    "rule editor."
                ),
                "kind": "error",
            }
            return

        parsed, disabled = parse_rules_json(entries, catalog)
        ruleset["custom"] = entries
        rules_cache.clear()
        widget.rules = rules_payload(parsed)
        widget.disabled_rules = disabled
        widget.import_issues = {}
        widget.status = {
            "message": f"Loaded {len(parsed)} rule(s).",
            "kind": "success",
        }

    def _handle_set_ruleset(payload):
        """
        Replaces the active ruleset without validating it again.

        Used by the rule editor's undo and "Reset to defaults" actions, which
        restore a ruleset that was already accepted once (or the built-in rules).
        """

        source = str(payload.get("source") or "default")
        if source == "default":
            entries = None
        elif source == "initial":
            entries = ruleset["initial"]
        else:
            entries = payload.get("rules") or []

        ruleset["custom"] = entries
        rules_cache.clear()
        widget.rules = rules_payload(normalize_rules(entries, _catalog()))
        widget.disabled_rules = [str(r) for r in (payload.get("disabled_rules") or [])]
        widget.import_issues = {}
        widget.status = {
            "message": str(payload.get("message") or "The ruleset was restored."),
            "kind": "success",
        }

    handlers = {
        "list_datasets": _handle_list_datasets,
        "load_lists": _handle_load_lists,
        "load_workspace_datasets": _handle_load_workspace_datasets,
        "reload_lists": _handle_reload_lists,
        "load_rules": _handle_load_rules,
        "import_rules": _handle_import_rules,
        "set_ruleset": _handle_set_ruleset,
        "run_scan": _handle_run_scan,
        "run_bulk": _handle_run_bulk,
        "preview_fix": _handle_preview_fix,
        "apply_staged_fixes": _handle_apply_staged_fixes,
    }

    _running = [False]

    # Lightweight lookups which fill a picker in the background: they must not
    # dim / block the user interface.
    _background_actions = {
        "load_lists",
        "list_datasets",
        "load_workspace_datasets",
        "load_rules",
    }

    def _on_run(_change):
        payload = dict(widget.pending_action or {})
        action = payload.get("action")
        handler = handlers.get(action)
        if handler is None:
            return
        # A handler must never be entered from inside another one, otherwise the
        # busy state and the traitlet updates interleave.
        if _running[0]:
            return

        background = action in _background_actions
        _running[0] = True
        if not background:
            widget.busy = True
            widget.status = {}
        widget.cancel_requested = False
        try:
            handler(payload)
        except Exception as e:
            widget.status = {"message": f"Error: {e}", "kind": "error"}
        finally:
            widget.busy = False
            widget.cancel_requested = False
            _running[0] = False

    widget.observe(_on_run, names=["run"])

    if initial_ds_id:
        try:
            widget.violations = _scan(initial_ws_id, initial_ds_id, [])
            widget.screen = "results"
        except Exception as e:
            widget.status = {"message": f"Error: {e}", "kind": "error"}

    display(widget)
