from typing import Optional
from uuid import UUID
from sempy._utils._log import log
import base64
import json
import re

# ---------------------------------------------------------------------------
# Widget CSS (scoped under .slls-lv). Tokens mirror sempy_labs._ui_components
# light/dark palettes so the tool matches every other interactive widget.
# ---------------------------------------------------------------------------
_WIDGET_CSS = """
.slls-lv {
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
    --slls-radius: 14px;
    --slls-radius-sm: 8px;
    --slls-shadow: 0 1px 2px rgba(0,0,0,0.04), 0 8px 24px rgba(0,0,0,0.06);
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
    .slls-lv.slls-lv-auto {
        --slls-bg-solid: #1e1e22; --slls-bg-secondary: #2a2a30;
        --slls-surface: rgba(255,255,255,0.04); --slls-surface-2: rgba(255,255,255,0.03);
        --slls-border: rgba(255,255,255,0.08); --slls-border-strong: rgba(255,255,255,0.16);
        --slls-text: #f5f5f7; --slls-text-secondary: #b8b8bf; --slls-text-tertiary: #8e8e94;
        --slls-accent: #0A84FF; --slls-accent-hover: #1a8cff; --slls-accent-soft: rgba(10,132,255,0.18);
        --slls-danger: #ff453a; --slls-danger-soft: rgba(255,69,58,0.18);
        --slls-success: #30d158; --slls-success-soft: rgba(48,209,88,0.18);
        --slls-shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.5);
    }
}
.slls-lv.slls-lv-dark {
    --slls-bg-solid: #1e1e22; --slls-bg-secondary: #2a2a30;
    --slls-surface: rgba(255,255,255,0.04); --slls-surface-2: rgba(255,255,255,0.03);
    --slls-border: rgba(255,255,255,0.08); --slls-border-strong: rgba(255,255,255,0.16);
    --slls-text: #f5f5f7; --slls-text-secondary: #b8b8bf; --slls-text-tertiary: #8e8e94;
    --slls-accent: #0A84FF; --slls-accent-hover: #1a8cff; --slls-accent-soft: rgba(10,132,255,0.18);
    --slls-danger: #ff453a; --slls-danger-soft: rgba(255,69,58,0.18);
    --slls-success: #30d158; --slls-success-soft: rgba(48,209,88,0.18);
    --slls-shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.5);
}
.slls-lv * { box-sizing: border-box; }

/* Fullscreen: fill the whole screen and drop the framing chrome. Notebook
   hosts often block the native Fullscreen API, so a CSS overlay (position:
   fixed covering the viewport) is used as the reliable primary mechanism. */
.slls-lv:fullscreen, .slls-lv:-webkit-full-screen { width: 100vw; height: 100vh; max-height: none; border: none; border-radius: 0; box-shadow: none; }
.slls-lv.slls-lv-fs { position: fixed; inset: 0; z-index: 2147483000; width: 100vw; height: 100vh; max-height: none; margin: 0; border: none; border-radius: 0; box-shadow: none; }

/* Header */
.slls-lv-header { display: flex; align-items: center; gap: 12px; padding: 16px 20px; border-bottom: 1px solid var(--slls-border); flex-wrap: wrap; }
.slls-lv-headicon { display: inline-flex; align-items: center; justify-content: center; width: 34px; height: 34px; border-radius: 10px; background: var(--slls-accent-soft); color: var(--slls-accent); flex-shrink: 0; }
.slls-lv-titlewrap { display: flex; flex-direction: column; margin-right: auto; min-width: 0; }
.slls-lv-title { font-size: 20px; font-weight: 600; letter-spacing: -0.01em; line-height: 1.15; }
.slls-lv-subtitle { font-size: 12.5px; color: var(--slls-text-secondary); margin-top: 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 540px; }
.slls-lv-subtitle .slls-lv-sep { color: var(--slls-text-tertiary); margin: 0 6px; }
.slls-lv-subtitle b { color: var(--slls-text); font-weight: 500; }

.slls-lv-btn { appearance: none; border: 1px solid var(--slls-border-strong); background: var(--slls-surface); color: var(--slls-text); font-family: inherit; font-size: 13px; font-weight: 500; padding: 7px 14px; border-radius: 999px; cursor: pointer; display: inline-flex; align-items: center; gap: 7px; transition: background 120ms ease, border-color 120ms ease, transform 80ms ease, opacity 120ms ease; }
.slls-lv-btn:hover { background: var(--slls-surface-2); border-color: var(--slls-text-tertiary); }
.slls-lv-btn:active { transform: scale(0.97); }
.slls-lv-btn:disabled { opacity: 0.45; cursor: not-allowed; }
.slls-lv-btn-primary { background: var(--slls-accent); border-color: var(--slls-accent); color: #fff; }
.slls-lv-btn-primary:hover { background: var(--slls-accent-hover); border-color: var(--slls-accent-hover); }
.slls-lv-btn-icon { width: 34px; height: 34px; padding: 0; justify-content: center; border-radius: 50%; }
.slls-lv-btn svg { display: block; }

/* Summary bar */
.slls-lv-summary { display: flex; flex-wrap: wrap; align-items: center; gap: 26px; padding: 10px 20px; border-bottom: 1px solid var(--slls-border); background: var(--slls-surface-2); }
.slls-lv-stat { display: flex; flex-direction: column; line-height: 1.15; }
.slls-lv-stat-label { font-size: 11px; color: var(--slls-text-secondary); }
.slls-lv-stat-value { font-size: 18px; font-weight: 600; font-variant-numeric: tabular-nums; }
.slls-lv-stat.bad .slls-lv-stat-value { color: var(--slls-danger); }
.slls-lv-stat.good .slls-lv-stat-value { color: var(--slls-success); }
.slls-lv-stat.warn .slls-lv-stat-value { color: var(--slls-warn); }
.slls-lv-summary-note { font-size: 12.5px; color: var(--slls-text-secondary); }

/* Body layout */
.slls-lv-body { display: flex; flex: 1; min-height: 0; }
.slls-lv-graphwrap { position: relative; flex: 1; min-width: 0; background: var(--slls-bg-secondary); }
.slls-lv-scroll { position: absolute; inset: 0; overflow: auto; cursor: grab; touch-action: none; }
.slls-lv-scroll.slls-lv-panning { cursor: grabbing; }
.slls-lv-canvas { position: relative; transform-origin: top left; }
.slls-lv-edges { position: absolute; inset: 0; pointer-events: none; }

/* Graph nodes */
.slls-lv-node { position: absolute; border-radius: 12px; border: 1.5px solid var(--slls-border-strong); background: var(--slls-bg-solid); box-shadow: var(--slls-shadow); padding: 10px 14px; cursor: move; user-select: none; display: flex; flex-direction: column; justify-content: center; gap: 3px; transition: box-shadow 120ms ease; }
.slls-lv-node.broken { border-color: var(--slls-danger); background: var(--slls-danger-soft); }
.slls-lv-node.clean { border-color: var(--slls-success); background: var(--slls-success-soft); }
.slls-lv-node.error { border-color: var(--slls-border-strong); background: var(--slls-surface); }
.slls-lv-node.selected { box-shadow: 0 0 0 2.5px var(--slls-accent); }
.slls-lv-node.picked { box-shadow: 0 0 0 2px var(--slls-accent-soft); }
.slls-lv-node-check { position: absolute; top: 8px; right: 8px; width: 15px; height: 15px; accent-color: var(--slls-accent); cursor: pointer; }
.slls-lv-node-name { font-size: 13px; font-weight: 600; line-height: 1.4; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; padding-right: 16px; }
.slls-lv-node-ws { display: flex; align-items: center; gap: 5px; font-size: 11px; color: var(--slls-text-secondary); overflow: hidden; }
.slls-lv-node-ws span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-lv-node-status { display: flex; align-items: center; gap: 6px; font-size: 11.5px; font-weight: 500; }
.slls-lv-dot { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; }
.slls-lv-node.broken .slls-lv-dot { background: var(--slls-danger); }
.slls-lv-node.clean .slls-lv-dot { background: var(--slls-success); }
.slls-lv-node.error .slls-lv-dot { background: var(--slls-text-tertiary); }
.slls-lv-node.broken .slls-lv-node-status { color: var(--slls-danger); }
.slls-lv-node.clean .slls-lv-node-status { color: var(--slls-success); }
.slls-lv-node.error .slls-lv-node-status { color: var(--slls-text-tertiary); }

.slls-lv-model { align-items: center; text-align: center; border-width: 2px; border-color: var(--slls-accent); background: var(--slls-accent-soft); }
.slls-lv-model-top { display: flex; align-items: center; gap: 7px; color: var(--slls-accent); font-size: 14px; font-weight: 700; line-height: 1.35; overflow: hidden; }
.slls-lv-model-top span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-lv-model-ws { display: flex; align-items: center; gap: 5px; font-size: 11.5px; color: var(--slls-text-secondary); overflow: hidden; max-width: 100%; }
.slls-lv-model-ws span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-lv-model-sub { font-size: 10.5px; text-transform: uppercase; letter-spacing: 0.04em; color: var(--slls-text-tertiary); }

/* Zoom controls */
.slls-lv-zoom { position: absolute; bottom: 16px; right: 16px; display: flex; flex-direction: column; align-items: center; gap: 4px; background: var(--slls-bg-solid); border: 1px solid var(--slls-border); border-radius: 10px; padding: 5px; box-shadow: var(--slls-shadow); }
.slls-lv-zoom button { width: 28px; height: 28px; border: none; background: transparent; color: var(--slls-text-secondary); border-radius: 6px; cursor: pointer; display: inline-flex; align-items: center; justify-content: center; }
.slls-lv-zoom button:hover { background: var(--slls-surface-2); color: var(--slls-text); }
.slls-lv-zoom-label { font-size: 11px; font-weight: 600; color: var(--slls-text-secondary); font-variant-numeric: tabular-nums; cursor: pointer; border-radius: 5px; padding: 1px 5px; transition: background 120ms ease, color 120ms ease; }
.slls-lv-zoom-label:hover { background: var(--slls-surface-2); color: var(--slls-text); }
.slls-lv-zoom-input { width: 46px; text-align: center; font-size: 11px; font-weight: 600; font-variant-numeric: tabular-nums; color: var(--slls-text); background: var(--slls-bg-solid); border: 1px solid var(--slls-accent); border-radius: 5px; padding: 1px 2px; font-family: inherit; outline: none; box-shadow: 0 0 0 3px var(--slls-accent-soft); }

/* Center overlays (loading / empty / error) */
.slls-lv-center { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; text-align: center; padding: 24px; }
.slls-lv-center-inner { max-width: 380px; }
.slls-lv-center-inner .slls-lv-ic { color: var(--slls-text-tertiary); opacity: 0.6; }
.slls-lv-center-inner h4 { margin: 10px 0 4px; font-size: 15px; font-weight: 600; }
.slls-lv-center-inner p { margin: 0; font-size: 12.5px; color: var(--slls-text-secondary); }

/* Side panel */
.slls-lv-panel { position: relative; width: 360px; flex-shrink: 0; border-left: 1px solid var(--slls-border); display: flex; flex-direction: column; min-height: 0; background: var(--slls-bg-solid); }
.slls-lv-panel.slls-lv-panel-collapsed { width: 46px; min-width: 46px; }
/* Drag handle straddling the panel's left border to resize its width. */
.slls-lv-panel-resize { position: absolute; left: -4px; top: 0; bottom: 0; width: 8px; cursor: col-resize; z-index: 6; touch-action: none; }
.slls-lv-panel-resize::after { content: ""; position: absolute; left: 3px; top: 0; bottom: 0; width: 2px; background: transparent; transition: background 120ms ease; }
.slls-lv-panel-resize:hover::after, .slls-lv-panel-resize.slls-lv-resizing::after { background: var(--slls-accent); }
/* Collapsed rail */
.slls-lv-rail { display: flex; flex-direction: column; align-items: center; gap: 12px; padding: 12px 0; height: 100%; }
.slls-lv-rail-badge { font-size: 11px; font-weight: 600; padding: 2px 7px; border-radius: 999px; background: var(--slls-danger-soft); color: var(--slls-danger); font-variant-numeric: tabular-nums; }
.slls-lv-rail-label { writing-mode: vertical-rl; transform: rotate(180deg); font-size: 11.5px; font-weight: 600; letter-spacing: 0.04em; color: var(--slls-text-secondary); white-space: nowrap; }
.slls-lv-panel-head { display: flex; align-items: flex-start; gap: 10px; padding: 14px 16px; border-bottom: 1px solid var(--slls-border); }
.slls-lv-panel-head .slls-lv-ic { color: var(--slls-accent); margin-top: 1px; flex-shrink: 0; }
.slls-lv-panel-head .slls-lv-pt { min-width: 0; flex: 1; }
.slls-lv-panel-title { font-size: 14px; font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-lv-panel-sub { font-size: 11.5px; color: var(--slls-text-secondary); margin-top: 2px; display: flex; align-items: center; gap: 5px; }
.slls-lv-panel-sub span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-lv-panel-close { width: 28px; height: 28px; border: none; background: transparent; color: var(--slls-text-secondary); border-radius: 6px; cursor: pointer; display: inline-flex; align-items: center; justify-content: center; flex-shrink: 0; }
.slls-lv-panel-close:hover { background: var(--slls-surface-2); color: var(--slls-text); }
.slls-lv-weblink { display: flex; align-items: center; gap: 6px; padding: 9px 16px; border-bottom: 1px solid var(--slls-border); font-size: 12px; color: var(--slls-accent); text-decoration: none; }
.slls-lv-weblink:hover { background: var(--slls-surface-2); }
.slls-lv-panel-btn { flex-shrink: 0; font-size: 12px; padding: 6px 12px; }
.slls-lv-panel-body { flex: 1; overflow-y: auto; padding: 10px 12px; min-height: 0; }
.slls-lv-empty { display: flex; flex-direction: column; align-items: center; justify-content: center; text-align: center; gap: 8px; padding: 48px 16px; }
.slls-lv-empty .slls-lv-ic { opacity: 0.65; }
.slls-lv-empty.good .slls-lv-ic { color: var(--slls-success); opacity: 1; }
.slls-lv-empty h4 { margin: 0; font-size: 13.5px; font-weight: 600; }
.slls-lv-empty p { margin: 0; font-size: 12px; color: var(--slls-text-secondary); max-width: 260px; }

.slls-lv-repbtn { width: 100%; text-align: left; border: 1px solid var(--slls-border); background: var(--slls-surface); color: var(--slls-text); border-radius: 10px; padding: 10px 12px; margin-bottom: 8px; cursor: pointer; display: flex; align-items: center; gap: 10px; transition: background 120ms ease, border-color 120ms ease; }
.slls-lv-repbtn .slls-lv-ic { color: var(--slls-text-secondary); flex-shrink: 0; }
.slls-lv-repbtn:hover { background: var(--slls-surface-2); border-color: var(--slls-text-tertiary); }
.slls-lv-repbtn .slls-lv-rp { min-width: 0; flex: 1; }
.slls-lv-repbtn-name { font-size: 13px; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-lv-repbtn-meta { font-size: 11.5px; color: var(--slls-danger); margin-top: 1px; }
.slls-lv-badge { flex-shrink: 0; font-size: 11px; font-weight: 600; padding: 2px 9px; border-radius: 999px; background: var(--slls-danger-soft); color: var(--slls-danger); font-variant-numeric: tabular-nums; }

.slls-lv-obj { border: 1px solid var(--slls-border); border-radius: 10px; padding: 9px 11px; margin-bottom: 7px; background: var(--slls-surface); }
.slls-lv-obj-name { font-family: "SF Mono", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; color: var(--slls-text); overflow-wrap: anywhere; }
.slls-lv-obj-meta { display: flex; align-items: center; gap: 6px; margin-top: 5px; }
.slls-lv-tag { font-size: 10.5px; font-weight: 500; padding: 1px 8px; border-radius: 999px; border: 1px solid var(--slls-border-strong); color: var(--slls-text-secondary); }
.slls-lv-tag.type { background: var(--slls-accent-soft); border-color: transparent; color: var(--slls-accent); }
.slls-lv-obj-top { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
.slls-lv-fix { display: flex; align-items: center; gap: 8px; margin-top: 8px; }
.slls-lv-fix-select { flex: 1; padding-top: 6px; padding-bottom: 6px; font-size: 12.5px; }
.slls-lv-fix-arrow { color: var(--slls-success); display: inline-flex; flex-shrink: 0; }
.slls-lv-staged { flex: 1; font-size: 12.5px; color: var(--slls-text-secondary); overflow-wrap: anywhere; }
.slls-lv-unstage { width: 24px; height: 24px; border: none; background: transparent; color: var(--slls-text-tertiary); border-radius: 6px; cursor: pointer; display: inline-flex; align-items: center; justify-content: center; flex-shrink: 0; }
.slls-lv-unstage:hover { background: var(--slls-surface-2); color: var(--slls-text); }
.slls-lv-fix-note { font-size: 11.5px; color: var(--slls-text-tertiary); font-style: italic; }
.slls-lv-panel-meta { font-size: 11.5px; color: var(--slls-text-secondary); margin-top: 3px; }
.slls-lv-section-label { font-size: 10.5px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; color: var(--slls-text-tertiary); margin: 2px 2px 8px; }
.slls-lv-obj-main { display: flex; align-items: center; gap: 8px; min-width: 0; }
.slls-lv-obj-ic { display: inline-flex; color: var(--slls-text-secondary); flex-shrink: 0; }
.slls-lv-fix-ic { display: inline-flex; color: var(--slls-text-secondary); flex-shrink: 0; }
.slls-lv-fix-staged { border: 1px solid var(--slls-border-strong); background: var(--slls-surface-2); border-radius: 9px; padding: 7px 9px; }
.slls-lv-combo { position: relative; flex: 1; min-width: 0; }
.slls-lv-combo-input { width: 100%; background: var(--slls-surface); border: 1px solid var(--slls-border-strong); border-radius: 10px; padding: 7px 12px; font-size: 12.5px; color: var(--slls-text); font-family: inherit; }
.slls-lv-combo-input:focus { outline: none; border-color: var(--slls-accent); box-shadow: 0 0 0 3px var(--slls-accent-soft); }
.slls-lv-combo-input::placeholder { color: var(--slls-text-tertiary); }
.slls-lv-combo-list { position: absolute; top: calc(100% + 4px); left: 0; right: 0; z-index: 30; max-height: 220px; overflow-y: auto; background: var(--slls-bg-solid); border: 1px solid var(--slls-border-strong); border-radius: 10px; box-shadow: var(--slls-shadow); padding: 4px; }
.slls-lv-combo-item { padding: 6px 10px; border-radius: 7px; font-size: 12px; color: var(--slls-text); cursor: pointer; font-family: "SF Mono", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.slls-lv-combo-item:hover { background: var(--slls-accent-soft); color: var(--slls-accent); }
.slls-lv-combo-empty { padding: 8px 10px; font-size: 12px; color: var(--slls-text-tertiary); }

/* Status toast */
.slls-lv-status { padding: 9px 20px; font-size: 12.5px; border-top: 1px solid var(--slls-border); }
.slls-lv-status.success { background: var(--slls-success-soft); color: var(--slls-success); }
.slls-lv-status.error { background: var(--slls-danger-soft); color: var(--slls-danger); }
.slls-lv-status.info { background: var(--slls-surface-2); color: var(--slls-text-secondary); }

/* Footer */
.slls-lv-footer { padding: 8px 20px; border-top: 1px solid var(--slls-border); text-align: right; font-size: 11.5px; color: var(--slls-text-tertiary); }
.slls-lv-footer a { color: var(--slls-text-tertiary); text-decoration: none; transition: color 120ms ease; }
.slls-lv-footer a:hover { color: var(--slls-accent); }

/* Rebind modal */
.slls-lv-overlay { position: absolute; inset: 0; background: rgba(0,0,0,0.35); display: flex; align-items: center; justify-content: center; z-index: 40; }
.slls-lv-modal { width: 420px; max-width: calc(100% - 40px); background: var(--slls-bg-solid); border: 1px solid var(--slls-border); border-radius: var(--slls-radius); box-shadow: 0 20px 60px rgba(0,0,0,0.35); padding: 20px; }
.slls-lv-modal h3 { margin: 0 0 4px; font-size: 16px; font-weight: 600; }
.slls-lv-modal p { margin: 0 0 16px; font-size: 12.5px; color: var(--slls-text-secondary); }
.slls-lv-field { display: flex; flex-direction: column; gap: 4px; margin-bottom: 12px; }
.slls-lv-field label { font-size: 12px; color: var(--slls-text-secondary); padding-left: 8px; }
.slls-lv-select { appearance: none; -webkit-appearance: none; width: 100%; background: var(--slls-surface); border: 1px solid var(--slls-border-strong); border-radius: 10px; padding: 8px 32px 8px 12px; font-size: 13px; color: var(--slls-text); font-family: inherit; cursor: pointer; background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6'><path fill='%236e6e73' d='M0 0l5 6 5-6z'/></svg>"); background-repeat: no-repeat; background-position: right 12px center; text-overflow: ellipsis; }
.slls-lv-select:focus { outline: none; border-color: var(--slls-accent); box-shadow: 0 0 0 3px var(--slls-accent-soft); }
.slls-lv-select option { background: #ffffff; color: #1d1d1f; }
.slls-lv.slls-lv-dark .slls-lv-select option { background: #2c2c2e; color: #f5f5f7; }
@media (prefers-color-scheme: dark) { .slls-lv.slls-lv-auto .slls-lv-select option { background: #2c2c2e; color: #f5f5f7; } }
.slls-lv-modal-actions { display: flex; justify-content: flex-end; gap: 10px; margin-top: 18px; }

.slls-lv-spin { animation: slls-lv-spin 0.8s linear infinite; transform-origin: center; }
@keyframes slls-lv-spin { to { transform: rotate(360deg); } }
.slls-lv-scroll::-webkit-scrollbar, .slls-lv-panel-body::-webkit-scrollbar { width: 10px; height: 10px; }
.slls-lv-scroll::-webkit-scrollbar-thumb, .slls-lv-panel-body::-webkit-scrollbar-thumb { background: var(--slls-border-strong); border-radius: 999px; background-clip: padding-box; border: 2px solid transparent; }
"""


# ---------------------------------------------------------------------------
# Widget JS (anywidget ESM). Icons injected via __ICON_*__ placeholders below.
# ---------------------------------------------------------------------------
_WIDGET_JS = r"""
function render({ model, el }) {
    const ICON = {
        database: `__ICON_DATABASE__`, report: `__ICON_REPORT__`,
        refresh: `__ICON_REFRESH__`, scan: `__ICON_SCAN__`, link: `__ICON_LINK__`,
        check: `__ICON_CHECK__`, alert: `__ICON_ALERT__`, external: `__ICON_EXTERNAL__`,
        zoomin: `__ICON_ZOOMIN__`, zoomout: `__ICON_ZOOMOUT__`, close: `__ICON_CLOSE__`,
        workflow: `__ICON_WORKFLOW__`, sun: `__ICON_SUN__`, moon: `__ICON_MOON__`,
        fullscreen: `__ICON_FULLSCREEN__`, fullscreen_exit: `__ICON_FULLSCREEN_EXIT__`,
        measure: `__ICON_MEASURE__`, column: `__ICON_COLUMN__`, hierarchy: `__ICON_HIERARCHY__`,
        save: `__ICON_SAVE__`, wrench: `__ICON_WRENCH__`, undo: `__ICON_UNDO__`,
        chevron_left: `__ICON_CHEVRON_LEFT__`, chevron_right: `__ICON_CHEVRON_RIGHT__`,
    };

    const MODEL_KEY = "__model__";
    const NODE_W = 184, NODE_H = 86, CENTER_W = 204, CENTER_H = 92, MARGIN = 150;
    const MIN_ZOOM = 0.3, MAX_ZOOM = 2.5;

    const root = document.createElement("div");
    root.className = "slls-lv";
    el.appendChild(root);

    function applyTheme() {
        root.classList.remove("slls-lv-dark", "slls-lv-auto");
        const dm = model.get("dark_mode");
        if (dm === true) root.classList.add("slls-lv-dark");
        else if (dm == null) root.classList.add("slls-lv-auto");
    }
    applyTheme();
    model.on("change:dark_mode", applyTheme);

    // --- Fullscreen ---
    // Notebook hosts (VS Code, Jupyter, Fabric) frequently sandbox the widget
    // output, so the native Fullscreen API silently rejects. We therefore drive
    // a CSS overlay that covers the viewport as the reliable primary mechanism,
    // and additionally attempt native fullscreen as a best-effort enhancement.
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
        root.classList.toggle("slls-lv-fs", on);
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
    function toggleFullscreen() { setFullscreen(!fsMode); }
    function onFullscreenChange() {
        // Fires only when native fullscreen state changes; if the user left it
        // (Esc / F11), drop the CSS overlay too.
        const nativeOn = !!(document.fullscreenElement || document.webkitFullscreenElement);
        if (!nativeOn && fsMode) { fsMode = false; root.classList.remove("slls-lv-fs"); renderAll(); }
    }
    function onEscKey(e) {
        if (e.key === "Escape" && fsMode) setFullscreen(false);
    }
    document.addEventListener("fullscreenchange", onFullscreenChange);
    document.addEventListener("webkitfullscreenchange", onFullscreenChange);
    document.addEventListener("keydown", onEscKey);

    // --- Local UI state ---
    let selectedId = null;
    let picked = new Set();          // report ids checked for rebind
    let stagedFixes = new Map();     // fixKey -> staged fix payload
    let zoom = 1;
    let positions = {};              // key -> {x, y}
    let positionsKey = "";
    let rebindOpen = false;
    let rebindWs = "";
    let rebindDs = "";
    let panelWidth = 360;             // side panel width (px)
    let panelCollapsed = false;       // side panel collapsed to a thin rail
    // Preserved graph scroll offset. viewKey tracks the layout+zoom the view
    // was last centred for, so re-renders that don't change either (e.g.
    // toggling a report checkbox) keep the user's current position.
    let viewKey = "";
    let viewport = null;
    const PANEL_MIN = 280, PANEL_MAX = 720;

    const esc = (s) => String(s == null ? "" : s)
        .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;").replace(/'/g, "&#39;");

    const reports = () => model.get("reports") || [];
    const analyzed = () => !!model.get("analyzed");
    const busy = () => !!model.get("busy");
    // Front-end-local "working" flag: flipped on the instant a button is
    // clicked so the UI reacts immediately, without waiting for the back-end
    // round-trip to set the synced "busy" trait. Cleared once the back-end
    // reports a result (see the model change wiring below).
    let busyLocal = false;
    const working = () => busyLocal || busy();
    const modelObjects = () => model.get("model_objects") || [];
    const health = (r) => (!r.analyzed ? "error" : (r.invalidCount > 0 ? "broken" : "clean"));
    const fixKey = (rid, type, table, name) => `${rid}\u0000${type}\u0000${table}\u0000${name}`;
    // Measures are model-global, so they are shown without a table prefix or
    // square brackets; columns/hierarchies keep the 'Table'[Object] form.
    const objLabel = (type, table, name) =>
        type === "Measure" ? `${name}` : `'${table}'[${name}]`;
    const typeIcon = (type) =>
        type === "Measure" ? ICON.measure
        : type === "Column" ? ICON.column
        : type === "Hierarchy" ? ICON.hierarchy : ICON.report;

    function dispatch(payload) {
        // Reflect activity immediately for snappy feedback, then re-render so
        // the clicked button shows its working state before the round-trip
        // completes.
        busyLocal = true;
        model.set("pending_action", payload);
        // Guard against an undefined "run" (some notebook hosts do not seed
        // every synced trait into the front-end model): "undefined + 1" is NaN,
        // which serializes to JSON null and makes the back-end set_state raise a
        // TraitError before the observer fires, so the click silently does
        // nothing. Coercing to 0 keeps the counter a valid integer.
        model.set("run", (model.get("run") || 0) + 1);
        model.save_changes();
        renderAll();
    }

    function saveFixes() {
        if (stagedFixes.size === 0) return;
        dispatch({ action: "save_fixes", fixes: [...stagedFixes.values()] });
    }

    function computePositions() {
        const rs = reports();
        const n = rs.length;
        const radius = Math.max(240, (n * (NODE_W + 48)) / (2 * Math.PI));
        const cx = radius + CENTER_W + MARGIN;
        const cy = radius + CENTER_H + MARGIN;
        const pos = {};
        pos[MODEL_KEY] = { x: cx, y: cy };
        rs.forEach((r, i) => {
            const a = -Math.PI / 2 + (i / Math.max(1, n)) * Math.PI * 2;
            pos[r.id] = { x: cx + radius * Math.cos(a), y: cy + radius * Math.sin(a) };
        });
        return pos;
    }

    function syncPositions() {
        const key = reports().map((r) => r.id).join("\u0000");
        if (key !== positionsKey) {
            positions = computePositions();
            positionsKey = key;
        }
    }

    function canvasSize() {
        let maxX = 900, maxY = 640;
        for (const k of Object.keys(positions)) {
            const p = positions[k];
            maxX = Math.max(maxX, p.x + NODE_W);
            maxY = Math.max(maxY, p.y + NODE_H);
        }
        return { w: maxX + MARGIN, h: maxY + MARGIN };
    }

    // Border point of a rect (centre cx,cy; half hw,hh) toward (tx,ty).
    function edgePoint(cx, cy, tx, ty, hw, hh) {
        const dx = tx - cx, dy = ty - cy;
        if (dx === 0 && dy === 0) return { x: cx, y: cy };
        const scale = 1 / Math.max(Math.abs(dx) / hw, Math.abs(dy) / hh);
        return { x: cx + dx * scale, y: cy + dy * scale };
    }

    function icon(name, cls) {
        return `<span class="slls-lv-ic ${cls || ""}">${ICON[name] || ""}</span>`;
    }

    // ---------- Rendering ----------
    function renderAll() {
        syncPositions();
        root.innerHTML = "";
        root.appendChild(buildHeader());
        root.appendChild(buildSummary());
        root.appendChild(buildBody());
        const st = model.get("status") || {};
        if (st.message) {
            const s = document.createElement("div");
            s.className = "slls-lv-status " + (st.kind || "info");
            s.textContent = st.message;
            root.appendChild(s);
        }
        root.appendChild(buildFooter());
        if (rebindOpen) root.appendChild(buildRebindModal());
        attachGraphInteractions();
    }

    function buildHeader() {
        const h = document.createElement("div");
        h.className = "slls-lv-header";
        const dm = model.get("dark_mode") === true;
        const nPick = picked.size;
        h.innerHTML =
            `<span class="slls-lv-headicon">${ICON.database}</span>` +
            `<div class="slls-lv-titlewrap">` +
                `<div class="slls-lv-title">Lineage view</div>` +
                `<div class="slls-lv-subtitle"><b>${esc(model.get("dataset_name"))}</b>` +
                `<span class="slls-lv-sep">&middot;</span>${esc(model.get("workspace_name"))}</div>` +
            `</div>` +
            (stagedFixes.size > 0
                ? `<button class="slls-lv-btn slls-lv-btn-primary slls-lv-btn-icon" data-act="save" title="Save ${stagedFixes.size} staged fix${stagedFixes.size === 1 ? "" : "es"}" ${working() ? "disabled" : ""}>` +
                    `${working() ? spinner() : ICON.save}</button>`
                : "") +
            (nPick > 0
                ? `<button class="slls-lv-btn" data-act="open-rebind">${ICON.link}Rebind ${nPick}</button>`
                : "") +
            `<button class="slls-lv-btn slls-lv-btn-icon" data-act="refresh" title="Reload downstream reports" ${working() ? "disabled" : ""}>` +
                `${working() ? spinner() : ICON.refresh}</button>` +
            `<button class="slls-lv-btn slls-lv-btn-icon" data-act="fullscreen" title="Toggle full screen">${isFullscreen() ? ICON.fullscreen_exit : ICON.fullscreen}</button>` +
            `<button class="slls-lv-btn slls-lv-btn-icon" data-act="theme" title="Toggle theme">${dm ? ICON.sun : ICON.moon}</button>`;

        h.querySelector('[data-act="refresh"]').onclick = () => dispatch({ action: "refresh" });
        h.querySelector('[data-act="fullscreen"]').onclick = () => toggleFullscreen();
        h.querySelector('[data-act="theme"]').onclick = () => {
            model.set("dark_mode", !(model.get("dark_mode") === true));
            model.save_changes();
            renderAll();
        };
        const rb = h.querySelector('[data-act="open-rebind"]');
        if (rb) rb.onclick = () => openRebind();
        const sv = h.querySelector('[data-act="save"]');
        if (sv) sv.onclick = () => saveFixes();
        return h;
    }

    function spinner() {
        return `<span class="slls-lv-ic"><svg class="slls-lv-spin" width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M8 1.6a6.4 6.4 0 1 1-6.4 6.4" opacity="0.85"/></svg></span>`;
    }

    function buildSummary() {
        const s = document.createElement("div");
        s.className = "slls-lv-summary";
        const rs = reports();
        const broken = rs.filter((r) => health(r) === "broken").length;
        const errored = rs.filter((r) => health(r) === "error").length;
        const brokenObjs = rs.reduce((n, r) => n + (r.invalidCount || 0), 0);
        let html = stat("Downstream reports", rs.length, "");
        if (analyzed()) {
            html += stat("Reports with broken elements", broken, broken > 0 ? "bad" : "good");
            html += stat("Broken objects", brokenObjs, brokenObjs > 0 ? "bad" : "good");
            if (errored > 0) html += stat("Not analyzed", errored, "warn");
        } else {
            html += `<span class="slls-lv-summary-note">Broken elements not analyzed yet &mdash; use &ldquo;Analyze broken elements&rdquo;.</span>`;
        }
        s.innerHTML = html;
        return s;
    }

    function stat(label, value, tone) {
        return `<div class="slls-lv-stat ${tone}"><span class="slls-lv-stat-label">${esc(label)}</span>` +
            `<span class="slls-lv-stat-value">${value}</span></div>`;
    }

    function buildBody() {
        const body = document.createElement("div");
        body.className = "slls-lv-body";
        body.appendChild(buildGraph());
        const rs = reports();
        if (rs.length > 0) body.appendChild(buildPanel());
        return body;
    }

    function buildGraph() {
        const wrap = document.createElement("div");
        wrap.className = "slls-lv-graphwrap";
        const rs = reports();

        if (busy() && rs.length === 0) {
            wrap.innerHTML = centerMsg("scan", "Loading downstream reports\u2026", "");
            return wrap;
        }
        if (rs.length === 0) {
            wrap.innerHTML = centerMsg("report", "No downstream reports",
                "No reports in this workspace consume this semantic model.");
            return wrap;
        }

        const size = canvasSize();
        const scroll = document.createElement("div");
        scroll.className = "slls-lv-scroll";
        const canvas = document.createElement("div");
        canvas.className = "slls-lv-canvas";
        canvas.style.width = size.w + "px";
        canvas.style.height = size.h + "px";
        canvas.style.transform = `scale(${zoom})`;

        // Edges
        const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
        svg.setAttribute("class", "slls-lv-edges");
        svg.setAttribute("width", size.w);
        svg.setAttribute("height", size.h);
        const m = positions[MODEL_KEY];
        rs.forEach((r) => {
            const p = positions[r.id];
            if (!p) return;
            const a = edgePoint(m.x, m.y, p.x, p.y, CENTER_W / 2, CENTER_H / 2);
            const b = edgePoint(p.x, p.y, m.x, m.y, NODE_W / 2, NODE_H / 2);
            const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
            line.setAttribute("id", "slls-lv-edge-" + r.id);
            line.setAttribute("x1", a.x); line.setAttribute("y1", a.y);
            line.setAttribute("x2", b.x); line.setAttribute("y2", b.y);
            const hs = health(r);
            const col = selectedId === r.id ? "var(--slls-accent)"
                : hs === "broken" ? "var(--slls-danger)"
                : hs === "clean" ? "var(--slls-success)" : "var(--slls-border-strong)";
            line.setAttribute("stroke", col);
            line.setAttribute("stroke-width", selectedId === r.id ? "2.4" : "1.5");
            svg.appendChild(line);
        });
        canvas.appendChild(svg);

        // Model node
        canvas.appendChild(buildModelNode());
        // Report nodes
        rs.forEach((r) => canvas.appendChild(buildReportNode(r)));

        scroll.appendChild(canvas);

        // Click-and-drag panning on empty canvas space. Nodes call
        // stopPropagation on pointerdown, so dragging a node never pans.
        const startPan = (e) => {
            if (e.button != null && e.button !== 0) return;
            e.preventDefault();
            const sx = e.clientX, sy = e.clientY;
            const sl = scroll.scrollLeft, stp = scroll.scrollTop;
            scroll.classList.add("slls-lv-panning");
            const move = (ev) => {
                scroll.scrollLeft = sl - (ev.clientX - sx);
                scroll.scrollTop = stp - (ev.clientY - sy);
            };
            const up = () => {
                window.removeEventListener("pointermove", move);
                window.removeEventListener("pointerup", up);
                scroll.classList.remove("slls-lv-panning");
                document.body.style.userSelect = "";
            };
            window.addEventListener("pointermove", move);
            window.addEventListener("pointerup", up);
            document.body.style.userSelect = "none";
        };
        scroll.addEventListener("pointerdown", startPan);
        canvas.addEventListener("pointerdown", startPan);

        // Remember the scroll offset so re-renders (e.g. toggling a report
        // checkbox or selecting a node) can restore the user's current view.
        scroll.addEventListener("scroll", () => {
            viewport = { left: scroll.scrollLeft, top: scroll.scrollTop };
        });

        wrap.appendChild(scroll);
        wrap.appendChild(buildZoom());

        // Only recentre on the model node when the layout or zoom actually
        // changed; otherwise restore the previous offset so the diagram stays
        // put across incidental re-renders.
        const centerKey = positionsKey + "|" + zoom;
        requestAnimationFrame(() => {
            if (viewKey !== centerKey || viewport === null) {
                scroll.scrollLeft = m.x * zoom - scroll.clientWidth / 2;
                scroll.scrollTop = m.y * zoom - scroll.clientHeight / 2;
                viewKey = centerKey;
            } else {
                scroll.scrollLeft = viewport.left;
                scroll.scrollTop = viewport.top;
            }
            viewport = { left: scroll.scrollLeft, top: scroll.scrollTop };
        });
        return wrap;
    }

    function centerMsg(ic, title, sub) {
        return `<div class="slls-lv-center"><div class="slls-lv-center-inner">` +
            `<span class="slls-lv-ic">${ICON[ic] || ""}</span>` +
            `<h4>${esc(title)}</h4>${sub ? `<p>${esc(sub)}</p>` : ""}</div></div>`;
    }

    function buildModelNode() {
        const p = positions[MODEL_KEY];
        const node = document.createElement("div");
        node.className = "slls-lv-node slls-lv-model";
        node.id = "slls-lv-node-" + MODEL_KEY;
        node.style.width = CENTER_W + "px";
        node.style.height = CENTER_H + "px";
        node.style.left = (p.x - CENTER_W / 2) + "px";
        node.style.top = (p.y - CENTER_H / 2) + "px";
        node.innerHTML =
            `<div class="slls-lv-model-top">${ICON.database}<span>${esc(model.get("dataset_name"))}</span></div>` +
            `<div class="slls-lv-model-ws">${ICON.workflow}<span>${esc(model.get("workspace_name"))}</span></div>` +
            `<div class="slls-lv-model-sub">Semantic model</div>`;
        node.addEventListener("pointerdown", (e) => startDrag(e, MODEL_KEY));
        return node;
    }

    function buildReportNode(r) {
        const p = positions[r.id];
        const hs = health(r);
        const node = document.createElement("div");
        node.className = "slls-lv-node " + hs +
            (selectedId === r.id ? " selected" : "") +
            (picked.has(r.id) && selectedId !== r.id ? " picked" : "");
        node.id = "slls-lv-node-" + r.id;
        node.style.width = NODE_W + "px";
        node.style.height = NODE_H + "px";
        node.style.left = (p.x - NODE_W / 2) + "px";
        node.style.top = (p.y - NODE_H / 2) + "px";
        const label = hs === "broken" ? `${r.invalidCount} broken`
            : hs === "clean" ? "No issues" : "Not analyzed";
        node.innerHTML =
            `<input type="checkbox" class="slls-lv-node-check" ${picked.has(r.id) ? "checked" : ""}>` +
            `<div class="slls-lv-node-name">${esc(r.name)}</div>` +
            `<div class="slls-lv-node-ws">${ICON.report}<span>${esc(r.workspaceName)}</span></div>` +
            `<div class="slls-lv-node-status"><span class="slls-lv-dot"></span>${esc(label)}</div>`;

        const chk = node.querySelector(".slls-lv-node-check");
        chk.addEventListener("pointerdown", (e) => e.stopPropagation());
        chk.addEventListener("click", (e) => {
            e.stopPropagation();
            if (chk.checked) picked.add(r.id); else picked.delete(r.id);
            renderAll();
        });
        node.addEventListener("pointerdown", (e) => startDrag(e, r.id));
        node.addEventListener("click", () => {
            if (dragMoved) { dragMoved = false; return; }
            selectedId = selectedId === r.id ? null : r.id;
            renderAll();
        });
        return node;
    }

    function buildZoom() {
        const z = document.createElement("div");
        z.className = "slls-lv-zoom";
        z.innerHTML =
            `<button data-z="in" title="Zoom in">${ICON.zoomin}</button>` +
            `<span class="slls-lv-zoom-label" data-z="label" role="button" tabindex="0" title="Click to set zoom level">${Math.round(zoom * 100)}%</span>` +
            `<button data-z="out" title="Zoom out">${ICON.zoomout}</button>`;
        z.querySelector('[data-z="in"]').onclick = () => setZoom(zoom * 1.2);
        z.querySelector('[data-z="out"]').onclick = () => setZoom(zoom / 1.2);
        const label = z.querySelector('[data-z="label"]');
        // The label doubles as a button: click (or keyboard-activate) to type
        // an exact zoom percentage.
        label.onclick = () => startEditZoom(label);
        label.onkeydown = (e) => {
            if (e.key === "Enter" || e.key === " ") { e.preventDefault(); startEditZoom(label); }
        };
        label.addEventListener("pointerdown", (e) => e.stopPropagation());
        return z;
    }

    // Replace the zoom label with an inline input so the user can type an
    // exact percentage. setZoom() clamps the value and re-renders, which
    // restores the label; an invalid entry simply re-renders unchanged.
    function startEditZoom(label) {
        const input = document.createElement("input");
        input.type = "text";
        input.className = "slls-lv-zoom-input";
        input.value = String(Math.round(zoom * 100));
        input.setAttribute("aria-label", "Zoom percentage");
        label.replaceWith(input);
        input.focus();
        input.select();
        input.addEventListener("pointerdown", (e) => e.stopPropagation());
        let done = false;
        const commit = (apply) => {
            if (done) return;
            done = true;
            const v = parseFloat(String(input.value).replace("%", "").trim());
            if (apply && !isNaN(v) && v > 0) setZoom(v / 100);
            else renderAll();
        };
        input.addEventListener("keydown", (e) => {
            if (e.key === "Enter") { e.preventDefault(); commit(true); }
            else if (e.key === "Escape") { e.preventDefault(); commit(false); }
        });
        input.addEventListener("blur", () => commit(true));
    }

    function setZoom(v) {
        zoom = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, Math.round(v * 100) / 100));
        renderAll();
    }

    // ---------- Side panel ----------
    function buildPanel() {
        const panel = document.createElement("div");
        panel.className = "slls-lv-panel";
        if (panelCollapsed) {
            panel.classList.add("slls-lv-panel-collapsed");
            panel.appendChild(buildPanelRail());
            return panel;
        }
        panel.style.width = panelWidth + "px";
        const handle = document.createElement("div");
        handle.className = "slls-lv-panel-resize";
        handle.title = "Drag to resize";
        handle.addEventListener("pointerdown", startPanelResize);
        panel.appendChild(handle);
        const rep = reports().find((r) => r.id === selectedId) || null;
        if (rep) buildReportDetail(panel, rep);
        else buildBrokenSummary(panel);
        return panel;
    }

    // Thin rail shown when the panel is collapsed; click to expand.
    function buildPanelRail() {
        const rail = document.createElement("div");
        rail.className = "slls-lv-rail";
        const broken = reports().reduce((n, r) => n + (r.invalidCount || 0), 0);
        rail.innerHTML =
            `<button class="slls-lv-panel-close" data-act="expand" title="Expand panel">${ICON.chevron_left}</button>` +
            (analyzed() && broken > 0 ? `<span class="slls-lv-rail-badge">${broken}</span>` : "") +
            `<span class="slls-lv-rail-label">Broken elements</span>`;
        rail.querySelector('[data-act="expand"]').onclick = () => {
            panelCollapsed = false; renderAll();
        };
        return rail;
    }

    // Collapse toggle placed in each panel head.
    function collapseBtn() {
        return `<button class="slls-lv-panel-close" data-act="collapse" title="Collapse panel">${ICON.chevron_right}</button>`;
    }

    function startPanelResize(e) {
        if (e.button != null && e.button !== 0) return;
        e.preventDefault();
        e.stopPropagation();
        const startX = e.clientX;
        const startW = panelWidth;
        const handle = e.currentTarget;
        const panel = handle.parentElement;
        handle.classList.add("slls-lv-resizing");
        const move = (ev) => {
            // Panel sits on the right, so dragging left widens it.
            const w = startW - (ev.clientX - startX);
            panelWidth = Math.max(PANEL_MIN, Math.min(PANEL_MAX, w));
            if (panel) panel.style.width = panelWidth + "px";
        };
        const up = () => {
            window.removeEventListener("pointermove", move);
            window.removeEventListener("pointerup", up);
            handle.classList.remove("slls-lv-resizing");
            document.body.style.userSelect = "";
        };
        window.addEventListener("pointermove", move);
        window.addEventListener("pointerup", up);
        document.body.style.userSelect = "none";
    }

    function buildBrokenSummary(panel) {
        const head = document.createElement("div");
        head.className = "slls-lv-panel-head";
        head.innerHTML =
            `<span class="slls-lv-ic">${ICON.scan}</span>` +
            `<div class="slls-lv-pt"><div class="slls-lv-panel-title">Broken elements summary</div>` +
            `<div class="slls-lv-panel-sub"><span>${analyzed()
                ? "Select a report to see its broken elements."
                : "Analyze the reports to check for broken elements."}</span></div></div>` +
            `<button class="slls-lv-btn slls-lv-btn-primary slls-lv-panel-btn" data-act="panel-analyze" ${working() ? "disabled" : ""}>` +
            `${ICON.scan}${analyzed() ? "Re-analyze" : "Analyze"}${working() ? spinner() : ""}</button>` +
            collapseBtn();
        head.querySelector('[data-act="panel-analyze"]').onclick = () =>
            dispatch({ action: "analyze" });
        head.querySelector('[data-act="collapse"]').onclick = () => {
            panelCollapsed = true; renderAll();
        };
        panel.appendChild(head);

        const bodyEl = document.createElement("div");
        bodyEl.className = "slls-lv-panel-body";
        const rs = reports();
        const brokenReports = rs.filter((r) => health(r) === "broken");
        if (!analyzed()) {
            bodyEl.innerHTML = emptyState("scan", "Not analyzed yet",
                "Broken elements are not calculated automatically. Use the \u201CAnalyze broken elements\u201D button to check each report.", false);
        } else if (brokenReports.length === 0) {
            bodyEl.innerHTML = emptyState("check", "All reports are healthy",
                "Every downstream report only references objects that exist in the model.", true);
        } else {
            brokenReports.forEach((r) => {
                const b = document.createElement("button");
                b.className = "slls-lv-repbtn";
                b.innerHTML =
                    `<span class="slls-lv-ic">${ICON.report}</span>` +
                    `<span class="slls-lv-rp"><span class="slls-lv-repbtn-name">${esc(r.name)}</span></span>` +
                    `<span class="slls-lv-badge">${r.invalidCount}</span>`;
                b.onclick = () => { selectedId = r.id; renderAll(); };
                bodyEl.appendChild(b);
            });
        }
        panel.appendChild(bodyEl);
    }

    function emptyState(ic, title, sub, good) {
        return `<div class="slls-lv-empty ${good ? "good" : ""}">` +
            `<span class="slls-lv-ic">${ICON[ic] || ""}</span>` +
            `<h4>${esc(title)}</h4><p>${esc(sub)}</p></div>`;
    }

    function buildReportDetail(panel, r) {
        const hs = health(r);
        const brokenCount = (r.invalidObjects || []).length;
        const refMeta = `${r.objectCount} object reference${r.objectCount === 1 ? "" : "s"} \u00b7 ${brokenCount} broken`;
        const head = document.createElement("div");
        head.className = "slls-lv-panel-head";
        head.innerHTML =
            `<span class="slls-lv-ic">${ICON.report}</span>` +
            `<div class="slls-lv-pt"><div class="slls-lv-panel-title">${esc(r.name)}</div>` +
            `<div class="slls-lv-panel-sub">${ICON.workflow}<span>${esc(r.workspaceName)}</span></div>` +
            (hs === "broken"
                ? `<div class="slls-lv-panel-meta">${esc(refMeta)}</div>`
                : "") +
            `</div>` +
            `<button class="slls-lv-panel-close" title="Close">${ICON.close}</button>` +
            collapseBtn();
        head.querySelector(".slls-lv-panel-close").onclick = () => { selectedId = null; renderAll(); };
        head.querySelector('[data-act="collapse"]').onclick = () => {
            panelCollapsed = true; renderAll();
        };
        panel.appendChild(head);

        if (r.webUrl) {
            const a = document.createElement("a");
            a.className = "slls-lv-weblink";
            a.href = r.webUrl; a.target = "_blank"; a.rel = "noopener noreferrer";
            a.innerHTML = `${ICON.external}Open in Power BI`;
            panel.appendChild(a);
        }

        const bodyEl = document.createElement("div");
        bodyEl.className = "slls-lv-panel-body";
        if (hs === "error") {
            bodyEl.innerHTML = emptyState("alert", "Not analyzed",
                r.error || "This report could not be analyzed for broken elements.", false);
        } else if (hs === "clean") {
            bodyEl.innerHTML = emptyState("check", "No broken elements",
                "Every object this report references exists in the model.", true);
        } else {
            const secLabel = document.createElement("div");
            secLabel.className = "slls-lv-section-label";
            secLabel.textContent = "Broken elements";
            bodyEl.appendChild(secLabel);
            // invalidObjects are already grouped per model object (deduped in
            // Python), so each row is a single broken object with a fix picker.
            (r.invalidObjects || []).forEach((o) => {
                bodyEl.appendChild(buildBrokenRow(r, o));
            });
        }
        panel.appendChild(bodyEl);
    }

    function buildBrokenRow(r, o) {
        const d = document.createElement("div");
        d.className = "slls-lv-obj";
        const key = fixKey(r.id, o.objectType, o.table, o.name);
        const staged = stagedFixes.get(key);
        d.innerHTML =
            `<div class="slls-lv-obj-top">` +
            `<div class="slls-lv-obj-main">` +
            `<span class="slls-lv-obj-ic">${typeIcon(o.objectType)}</span>` +
            `<span class="slls-lv-obj-name">${esc(objLabel(o.objectType, o.table, o.name))}</span>` +
            `</div><span class="slls-lv-tag type">${esc(o.objectType)}</span></div>`;

        if (staged) {
            const fix = document.createElement("div");
            fix.className = "slls-lv-fix slls-lv-fix-staged";
            fix.innerHTML =
                `<span class="slls-lv-fix-ic">${ICON.wrench}</span>` +
                `<span class="slls-lv-staged">Point to ${esc(objLabel(o.objectType, staged.targetTable, staged.targetName))}</span>` +
                `<button class="slls-lv-unstage" title="Undo fix">${ICON.undo}</button>`;
            fix.querySelector(".slls-lv-unstage").onclick = () => {
                stagedFixes.delete(key);
                renderAll();
            };
            d.appendChild(fix);
        } else {
            const cands = modelObjects()
                .filter((mo) => mo.type === o.objectType)
                .sort((a, b) => (o.objectType === "Measure"
                    ? a.name.toLowerCase().localeCompare(b.name.toLowerCase())
                    : (a.table + "\u0000" + a.name).toLowerCase()
                        .localeCompare((b.table + "\u0000" + b.name).toLowerCase())));
            const fix = document.createElement("div");
            fix.className = "slls-lv-fix";
            fix.appendChild(buildFixCombo(r, o, key, cands));
            d.appendChild(fix);
        }
        return d;
    }

    // Searchable combobox used to pick a replacement model object. A native
    // <select> cannot carry a table\u0000name value (the HTML parser strips NUL
    // from attribute values, which previously produced "[undefined]"), so we
    // reference the candidate objects directly from this closure instead.
    function buildFixCombo(r, o, key, cands) {
        const combo = document.createElement("div");
        combo.className = "slls-lv-combo";
        const input = document.createElement("input");
        input.type = "text";
        input.className = "slls-lv-combo-input";
        input.placeholder = "Apply fix\u2026";
        input.autocomplete = "off";
        input.spellcheck = false;
        const list = document.createElement("div");
        list.className = "slls-lv-combo-list";
        list.style.display = "none";

        function choose(mo) {
            stagedFixes.set(key, {
                reportId: r.id,
                reportName: r.name,
                reportWorkspace: r.workspaceName,
                objectType: o.objectType,
                brokenTable: o.table,
                brokenName: o.name,
                targetTable: mo.table,
                targetName: mo.name,
            });
            renderAll();
        }

        function renderList() {
            const q = input.value.trim().toLowerCase();
            const matches = cands.filter((mo) =>
                objLabel(mo.type, mo.table, mo.name).toLowerCase().includes(q));
            list.innerHTML = "";
            if (matches.length === 0) {
                const em = document.createElement("div");
                em.className = "slls-lv-combo-empty";
                em.textContent = "No matches";
                list.appendChild(em);
                return;
            }
            matches.slice(0, 200).forEach((mo) => {
                const item = document.createElement("div");
                item.className = "slls-lv-combo-item";
                item.textContent = objLabel(mo.type, mo.table, mo.name);
                // mousedown fires before the input blur, so the click registers.
                item.addEventListener("mousedown", (e) => { e.preventDefault(); choose(mo); });
                list.appendChild(item);
            });
        }

        input.addEventListener("focus", () => { renderList(); list.style.display = "block"; });
        input.addEventListener("input", () => { renderList(); list.style.display = "block"; });
        input.addEventListener("blur", () => { setTimeout(() => { list.style.display = "none"; }, 150); });
        input.addEventListener("keydown", (e) => {
            if (e.key === "Escape") { input.blur(); }
            else if (e.key === "Enter") {
                e.preventDefault();
                const first = list.querySelector(".slls-lv-combo-item");
                if (first) first.dispatchEvent(new MouseEvent("mousedown"));
            }
        });

        combo.appendChild(input);
        combo.appendChild(list);
        return combo;
    }

    function buildFooter() {
        const f = document.createElement("div");
        f.className = "slls-lv-footer";
        f.innerHTML = `Powered by <a href="https://github.com/microsoft/semantic-link-labs" ` +
            `target="_blank" rel="noopener noreferrer">Semantic Link Labs</a>`;
        return f;
    }

    // ---------- Rebind modal ----------
    function openRebind() {
        rebindOpen = true;
        rebindWs = model.get("workspace_id") || "";
        rebindDs = "";
        ensureDatasets(rebindWs);
        renderAll();
    }

    function ensureDatasets(wsId) {
        const ds = model.get("datasets") || {};
        if (wsId && !ds[wsId]) dispatch({ action: "list_datasets", workspace_id: wsId });
    }

    function buildRebindModal() {
        const overlay = document.createElement("div");
        overlay.className = "slls-lv-overlay";
        const workspaces = model.get("workspaces") || [];
        const ds = (model.get("datasets") || {})[rebindWs] || null;

        const wsOptions = workspaces.map((w) =>
            `<option value="${esc(w.id)}" ${w.id === rebindWs ? "selected" : ""}>${esc(w.name)}</option>`).join("");
        let dsInner;
        if (ds === null) dsInner = `<option value="">Loading\u2026</option>`;
        else if (ds.length === 0) dsInner = `<option value="">No semantic models</option>`;
        else dsInner = `<option value="">Select a semantic model\u2026</option>` + ds.map((d) =>
            `<option value="${esc(d.id)}" ${d.id === rebindDs ? "selected" : ""}>${esc(d.name)}</option>`).join("");

        const modal = document.createElement("div");
        modal.className = "slls-lv-modal";
        modal.innerHTML =
            `<h3>Rebind ${picked.size} report${picked.size === 1 ? "" : "s"}</h3>` +
            `<p>Point the selected report${picked.size === 1 ? "" : "s"} at a different semantic model.</p>` +
            `<div class="slls-lv-field"><label>Target workspace</label>` +
            `<select class="slls-lv-select" data-r="ws">${wsOptions}</select></div>` +
            `<div class="slls-lv-field"><label>Target semantic model</label>` +
            `<select class="slls-lv-select" data-r="ds" ${ds === null ? "disabled" : ""}>${dsInner}</select></div>` +
            `<div class="slls-lv-modal-actions">` +
            `<button class="slls-lv-btn" data-r="cancel">Cancel</button>` +
            `<button class="slls-lv-btn slls-lv-btn-primary" data-r="confirm" ${(!rebindDs || working()) ? "disabled" : ""}>` +
            `${ICON.link}Rebind${working() ? spinner() : ""}</button></div>`;

        modal.querySelector('[data-r="ws"]').onchange = (e) => {
            rebindWs = e.target.value; rebindDs = ""; ensureDatasets(rebindWs); renderAll();
        };
        modal.querySelector('[data-r="ds"]').onchange = (e) => { rebindDs = e.target.value; renderAll(); };
        modal.querySelector('[data-r="cancel"]').onclick = () => { rebindOpen = false; renderAll(); };
        modal.querySelector('[data-r="confirm"]').onclick = () => {
            if (!rebindDs) return;
            dispatch({
                action: "rebind",
                report_ids: [...picked],
                dataset_id: rebindDs,
                dataset_workspace_id: rebindWs,
            });
        };
        overlay.appendChild(modal);
        overlay.addEventListener("pointerdown", (e) => {
            if (e.target === overlay) { rebindOpen = false; renderAll(); }
        });
        return overlay;
    }

    // ---------- Dragging (nodes) ----------
    let dragMoved = false;
    function startDrag(e, key) {
        if (e.button != null && e.button !== 0) return;
        e.preventDefault();
        e.stopPropagation();
        dragMoved = false;
        const start = positions[key] || { x: 0, y: 0 };
        const ox = e.clientX, oy = e.clientY;
        const node = document.getElementById("slls-lv-node-" + key);
        const half = key === MODEL_KEY ? { w: CENTER_W / 2, h: CENTER_H / 2 } : { w: NODE_W / 2, h: NODE_H / 2 };

        function move(ev) {
            const dx = (ev.clientX - ox) / zoom;
            const dy = (ev.clientY - oy) / zoom;
            if (Math.abs(ev.clientX - ox) + Math.abs(ev.clientY - oy) > 4) dragMoved = true;
            const nx = Math.max(half.w, start.x + dx);
            const ny = Math.max(half.h, start.y + dy);
            positions[key] = { x: nx, y: ny };
            if (node) {
                node.style.left = (nx - half.w) + "px";
                node.style.top = (ny - half.h) + "px";
            }
            updateEdges(key);
        }
        function up() {
            window.removeEventListener("pointermove", move);
            window.removeEventListener("pointerup", up);
            document.body.style.userSelect = "";
        }
        window.addEventListener("pointermove", move);
        window.addEventListener("pointerup", up);
        document.body.style.userSelect = "none";
    }

    function updateEdges(key) {
        const m = positions[MODEL_KEY];
        const rs = key === MODEL_KEY ? reports() : reports().filter((r) => r.id === key);
        rs.forEach((r) => {
            const p = positions[r.id];
            const line = document.getElementById("slls-lv-edge-" + r.id);
            if (!p || !line) return;
            const a = edgePoint(m.x, m.y, p.x, p.y, CENTER_W / 2, CENTER_H / 2);
            const b = edgePoint(p.x, p.y, m.x, m.y, NODE_W / 2, NODE_H / 2);
            line.setAttribute("x1", a.x); line.setAttribute("y1", a.y);
            line.setAttribute("x2", b.x); line.setAttribute("y2", b.y);
        });
    }

    function attachGraphInteractions() { /* listeners attached during build */ }

    // ---------- Model change wiring ----------
    // Any result arriving from the back-end also clears the local working flag.
    function settle() { busyLocal = false; renderAll(); }
    model.on("change:reports", settle);
    model.on("change:analyzed", settle);
    model.on("change:busy", settle);
    model.on("change:status", settle);
    model.on("change:workspaces", () => { if (rebindOpen) renderAll(); });
    model.on("change:datasets", () => { busyLocal = false; if (rebindOpen) renderAll(); });
    model.on("change:model_objects", () => { if (selectedId) renderAll(); });
    model.on("change:rebind_done", () => {
        busyLocal = false; rebindOpen = false; picked = new Set(); selectedId = null; renderAll();
    });
    model.on("change:fixes_saved", () => {
        busyLocal = false; stagedFixes = new Map(); renderAll();
    });

    renderAll();
}
export default { render };
"""

from sempy_labs._ui_components import ICONS as _UI_ICONS  # noqa: E402

_WIDGET_JS = (
    _WIDGET_JS.replace("__ICON_DATABASE__", _UI_ICONS["database"])
    .replace("__ICON_REPORT__", _UI_ICONS["report"])
    .replace("__ICON_REFRESH__", _UI_ICONS["refresh"])
    .replace("__ICON_SCAN__", _UI_ICONS["scan"])
    .replace("__ICON_LINK__", _UI_ICONS["link"])
    .replace("__ICON_CHECK__", _UI_ICONS["check_circle"])
    .replace("__ICON_ALERT__", _UI_ICONS["alert"])
    .replace("__ICON_EXTERNAL__", _UI_ICONS["external_link"])
    .replace("__ICON_ZOOMIN__", _UI_ICONS["zoom_in"])
    .replace("__ICON_ZOOMOUT__", _UI_ICONS["zoom_out"])
    .replace("__ICON_CLOSE__", _UI_ICONS["close"])
    .replace("__ICON_WORKFLOW__", _UI_ICONS["workflow"])
    .replace("__ICON_SUN__", _UI_ICONS["sun"])
    .replace("__ICON_MOON__", _UI_ICONS["moon"])
    .replace("__ICON_FULLSCREEN__", _UI_ICONS["fullscreen"])
    .replace("__ICON_FULLSCREEN_EXIT__", _UI_ICONS["fullscreen_exit"])
    .replace("__ICON_MEASURE__", _UI_ICONS["measure"])
    .replace("__ICON_COLUMN__", _UI_ICONS["column"])
    .replace("__ICON_HIERARCHY__", _UI_ICONS["hierarchy"])
    .replace("__ICON_SAVE__", _UI_ICONS["save"])
    .replace("__ICON_WRENCH__", _UI_ICONS["wrench"])
    .replace("__ICON_UNDO__", _UI_ICONS["undo"])
    .replace("__ICON_CHEVRON_LEFT__", _UI_ICONS["chevron_left"])
    .replace("__ICON_CHEVRON_RIGHT__", _UI_ICONS["chevron_right"])
)


@log
def lineage_view(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    dark_mode: bool = False,
):
    """
    Displays an interactive lineage view for a semantic model.

    Shows a node graph placing the semantic model at the center with each downstream report
    (i.e. the reports in the model's workspace that consume it) arranged around
    it. Each report can be analyzed for broken elements — references to columns,
    measures or hierarchies that no longer exist in the semantic model — with a
    side panel summarizing the results per report. Selecting a broken report
    lists its broken objects (grouped by object); a valid replacement object can
    be chosen from the model to stage a fix, and the staged fixes are written
    back to the report(s) with the Save button. Reports can also be selected and
    rebound to a different semantic model.


    Both the 'PBIR' and 'PBIRLegacy' report formats are supported for
    broken-element analysis and fixes.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
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
            "The 'lineage_view' function requires the 'anywidget' package. "
            "Install it with: pip install anywidget"
        ) from e

    import sempy.fabric as fabric
    from IPython.display import display
    from sempy_labs._helper_functions import (
        resolve_workspace_name_and_id,
        resolve_dataset_name_and_id,
        format_dax_object_name,
        _base_api,
    )
    from sempy_labs._list_functions import list_reports_using_semantic_model
    from sempy_labs.report._report_rebind import report_rebind

    ws_name, ws_id = resolve_workspace_name_and_id(workspace)
    ds_name, ds_id = resolve_dataset_name_and_id(dataset, ws_id)
    ws_id = str(ws_id)
    ds_id = str(ds_id)

    def _web_url_map():
        """Report Id -> Web Url, best effort."""
        try:
            dfAll = fabric.list_reports(workspace=ws_id)
        except Exception:
            return {}
        cols = list(dfAll.columns)
        id_col = "Id" if "Id" in cols else (cols[0] if cols else None)
        url_col = next((c for c in cols if "url" in c.lower()), None)
        if id_col is None or url_col is None:
            return {}
        return {
            str(r[id_col]): (str(r[url_col]) if r[url_col] else None)
            for _, r in dfAll.iterrows()
        }

    def _model_objects_full():
        """Valid model objects.

        Returns the sets of valid measure names, fully-qualified column names and
        fully-qualified hierarchy names (used to detect broken references), plus
        an ordered list of ``{table, name, type}`` objects offered as fix targets.
        """
        from sempy_labs.tom import connect_semantic_model

        measures = set()
        columns = set()
        hierarchies = set()
        objects = []
        with connect_semantic_model(
            dataset=ds_id, readonly=True, workspace=ws_id
        ) as tom:
            for m in tom.all_measures():
                measures.add(m.Name)
                objects.append(
                    {"table": m.Parent.Name, "name": m.Name, "type": "Measure"}
                )
            for c in tom.all_columns():
                columns.add(format_dax_object_name(c.Parent.Name, c.Name))
                objects.append(
                    {"table": c.Parent.Name, "name": c.Name, "type": "Column"}
                )
            for h in tom.all_hierarchies():
                hierarchies.add(format_dax_object_name(h.Parent.Name, h.Name))
                objects.append(
                    {"table": h.Parent.Name, "name": h.Name, "type": "Hierarchy"}
                )
        return measures, columns, hierarchies, objects

    # ------------------------------------------------------------------
    # Report definition extractor / mutator.
    #
    # Works for both the 'PBIR' (multiple JSON parts) and 'PBIRLegacy' (single
    # report.json with stringified config/query blobs) formats by walking every
    # part's JSON and descending into any stringified JSON blobs. Ports the
    # 'Tools' app's ReportObjectExtractor so this tool does not depend on the
    # PBIR-only ReportWrapper. Resolves both direct SourceRef.Entity references
    # and aliased SourceRef.Source references via each query's From clause.
    # ------------------------------------------------------------------
    def _get_report_definition(report_id):
        result = _base_api(
            request=(f"/v1/workspaces/{ws_id}/items/{report_id}/getDefinition"),
            method="post",
            status_codes=None,
            lro_return_json=True,
            client="fabric_sp",
        )
        return result.get("definition", {}).get("parts", [])

    def _looks_like_json(s):
        t = s.lstrip()
        return t.startswith("{") or t.startswith("[")

    def _walk_refs(node, parent_key, aliases, refs, report_measures):
        if isinstance(node, dict):
            # A 'From' clause maps query aliases (Source) to entities (tables).
            local_aliases = aliases
            frm = node.get("From")
            if isinstance(frm, list):
                local_aliases = dict(aliases)
                for f in frm:
                    if isinstance(f, dict):
                        alias = f.get("Name")
                        ent = f.get("Entity")
                        if isinstance(alias, str) and alias and isinstance(ent, str):
                            local_aliases[alias] = ent

            # Report-level measures live in entities[].measures[].name; they are
            # defined in the report (not the model) so must not be flagged broken.
            entities = node.get("entities")
            if isinstance(entities, list):
                for ent in entities:
                    if isinstance(ent, dict) and isinstance(ent.get("measures"), list):
                        for m in ent["measures"]:
                            if isinstance(m, dict) and isinstance(m.get("name"), str):
                                if m["name"]:
                                    report_measures.add(m["name"])

            expr = node.get("Expression")
            if isinstance(expr, dict) and isinstance(expr.get("SourceRef"), dict):
                src = expr["SourceRef"]
                entity = None
                if isinstance(src.get("Entity"), str) and src["Entity"]:
                    entity = src["Entity"]
                else:
                    alias = src.get("Source")
                    if isinstance(alias, str) and alias in local_aliases:
                        entity = local_aliases[alias]
                if entity:
                    prop = node.get("Property")
                    hier = node.get("Hierarchy")
                    if isinstance(prop, str) and prop:
                        otype = "Measure" if parent_key == "Measure" else "Column"
                        refs[(otype, entity, prop)] = {
                            "table": entity,
                            "name": prop,
                            "objectType": otype,
                        }
                    elif isinstance(hier, str) and hier:
                        refs[("Hierarchy", entity, hier)] = {
                            "table": entity,
                            "name": hier,
                            "objectType": "Hierarchy",
                        }

            for k, v in node.items():
                if isinstance(v, str) and _looks_like_json(v):
                    # PBIRLegacy stores visual config / query as stringified JSON.
                    try:
                        inner = json.loads(v)
                    except Exception:
                        inner = None
                    if inner is not None:
                        _walk_refs(inner, k, local_aliases, refs, report_measures)
                else:
                    _walk_refs(v, k, local_aliases, refs, report_measures)
        elif isinstance(node, list):
            for item in node:
                _walk_refs(item, parent_key, aliases, refs, report_measures)

    def _extract_report_objects(parts):
        refs = {}
        report_measures = set()
        for part in parts:
            payload = part.get("payload")
            if not payload:
                continue
            try:
                node = json.loads(base64.b64decode(payload).decode("utf-8"))
            except Exception:
                continue
            _walk_refs(node, None, {}, refs, report_measures)
        return list(refs.values()), report_measures

    def _rewrite_ref_string(s, fixes, native=False):
        """Rewrite a query-alias string in place.

        Handles the qualified alias strings (``queryRef``, ``Name``,
        ``metadata``) and, when ``native`` is True, the bare display strings
        (``nativeQueryRef``, ``NativeReferenceName``).

        Matching is anchored to the whole token so renaming an object never
        corrupts a longer friendly name (e.g. renaming ``Table.Fiscal`` must not
        touch ``Table.Fiscal Year``). Both the plain ``Table.Column`` form and
        the aggregation / bracket wrapped forms (``Sum(Table.Column)``,
        ``[Table.Column]``) are supported.
        """
        result = s
        for fx in fixes:
            old_qual = f"{fx['oldTable']}.{fx['oldName']}"
            new_qual = f"{fx['newTable']}.{fx['newName']}"
            if native:
                # Display names carry only the bare object name.
                if result.lower() == str(fx["oldName"]).lower():
                    result = fx["newName"]
                continue
            low = result.lower()
            if low == old_qual.lower():
                result = new_qual
            elif low == f"[{old_qual}]".lower():
                result = f"[{new_qual}]"
            else:
                # Aggregation wrapper, e.g. "Sum(Table.Column)".
                m = re.match(r"^([A-Za-z]\w*)\((.+)\)$", result)
                if m and m.group(2).lower() == old_qual.lower():
                    result = f"{m.group(1)}({new_qual})"
        return result

    def _new_from_alias(from_list, table):
        """Return an unused query alias for ``table`` within ``from_list``."""
        base = "".join(ch for ch in table.lower() if ch.isalnum()) or "t"
        used = {str(f.get("Name", "")) for f in from_list if isinstance(f, dict)}
        if base not in used:
            return base
        i = 1
        while f"{base}{i}" in used:
            i += 1
        return f"{base}{i}"

    def _repoint_source_ref(src, entity_based, from_list, new_table):
        """Point a ``SourceRef`` at ``new_table`` for a cross-table fix.

        Direct references update ``Entity`` in place. Aliased references reuse an
        existing ``From`` alias for the target table (or append a new one) so
        that sibling references to the original table are left untouched.
        """
        if entity_based:
            src["Entity"] = new_table
            return
        if not isinstance(from_list, list):
            return
        existing = next(
            (
                f.get("Name")
                for f in from_list
                if isinstance(f, dict)
                and str(f.get("Entity", "")).lower() == new_table.lower()
            ),
            None,
        )
        if existing:
            src["Source"] = existing
        else:
            alias = _new_from_alias(from_list, new_table)
            from_list.append({"Name": alias, "Entity": new_table, "Type": 0})
            src["Source"] = alias

    def _mutate_reference(obj, parent_key, aliases, from_list, fixes):
        expr = obj.get("Expression")
        if not isinstance(expr, dict) or not isinstance(expr.get("SourceRef"), dict):
            return
        src = expr["SourceRef"]
        entity_based = False
        entity = None
        if isinstance(src.get("Entity"), str) and src["Entity"]:
            entity = src["Entity"]
            entity_based = True
        else:
            alias = src.get("Source")
            if isinstance(alias, str) and alias in aliases:
                entity = aliases[alias]
        if not entity:
            return
        prop = obj.get("Property")
        hier = obj.get("Hierarchy")
        if isinstance(prop, str) and prop:
            ref_type = "Measure" if parent_key == "Measure" else "Column"
            for fx in fixes:
                if (
                    fx["objectType"].lower() == ref_type.lower()
                    and entity.lower() == fx["oldTable"].lower()
                    and prop.lower() == fx["oldName"].lower()
                ):
                    obj["Property"] = fx["newName"]
                    if fx["newTable"].lower() != fx["oldTable"].lower():
                        _repoint_source_ref(
                            src, entity_based, from_list, fx["newTable"]
                        )
                    break
        elif isinstance(hier, str) and hier:
            for fx in fixes:
                if (
                    fx["objectType"].lower() == "hierarchy"
                    and entity.lower() == fx["oldTable"].lower()
                    and hier.lower() == fx["oldName"].lower()
                ):
                    obj["Hierarchy"] = fx["newName"]
                    if fx["newTable"].lower() != fx["oldTable"].lower():
                        _repoint_source_ref(
                            src, entity_based, from_list, fx["newTable"]
                        )
                    break

    def _mutate_walk(node, parent_key, aliases, from_list, fixes):
        if isinstance(node, dict):
            local_aliases = aliases
            local_from = from_list
            frm = node.get("From")
            if isinstance(frm, list):
                local_aliases = dict(aliases)
                local_from = frm
                for f in frm:
                    if isinstance(f, dict):
                        alias = f.get("Name")
                        ent = f.get("Entity")
                        if isinstance(alias, str) and alias and isinstance(ent, str):
                            local_aliases[alias] = ent
            _mutate_reference(node, parent_key, local_aliases, local_from, fixes)
            for k in list(node.keys()):
                v = node[k]
                if isinstance(v, str):
                    # Rewrite query aliases so they stay in sync with the
                    # structured reference mutated above. These are checked
                    # before the stringified-JSON branch because a bracketed
                    # alias such as "[Table.Column]" also starts with "[".
                    if k in ("queryRef", "metadata", "Name"):
                        node[k] = _rewrite_ref_string(v, fixes)
                    elif k in ("nativeQueryRef", "NativeReferenceName"):
                        node[k] = _rewrite_ref_string(v, fixes, native=True)
                    elif _looks_like_json(v):
                        try:
                            inner = json.loads(v)
                        except Exception:
                            inner = None
                        if inner is not None:
                            _mutate_walk(inner, k, local_aliases, local_from, fixes)
                            node[k] = json.dumps(inner)
                else:
                    _mutate_walk(v, k, local_aliases, local_from, fixes)
        elif isinstance(node, list):
            for item in node:
                _mutate_walk(item, parent_key, aliases, from_list, fixes)

    def _apply_report_fixes(report_id, fixes):
        parts = _get_report_definition(report_id)
        out_parts = []
        for part in parts:
            path = part.get("path", "")
            payload = part.get("payload")
            out_payload = payload or ""
            if payload:
                try:
                    node = json.loads(base64.b64decode(payload).decode("utf-8"))
                except Exception:
                    node = None
                if node is not None:
                    _mutate_walk(node, None, {}, None, fixes)
                    out_payload = base64.b64encode(
                        json.dumps(node).encode("utf-8")
                    ).decode("utf-8")
            out_parts.append(
                {"path": path, "payload": out_payload, "payloadType": "InlineBase64"}
            )
        _base_api(
            request=(f"/v1/workspaces/{ws_id}/reports/{report_id}/updateDefinition"),
            method="post",
            payload={"definition": {"parts": out_parts}},
            status_codes=None,
            lro_return_status_code=True,
            client="fabric_sp",
        )

    def _build_reports(analyze):
        """Return the list of downstream-report payload dicts."""
        dfR = list_reports_using_semantic_model(dataset=ds_id, workspace=ws_id)
        url_map = _web_url_map()
        reports = []
        for _, r in dfR.iterrows():
            reports.append(
                {
                    "id": str(r["Report Id"]),
                    "name": str(r["Report Name"]),
                    "workspaceName": str(r["Report Workspace Name"]),
                    "webUrl": url_map.get(str(r["Report Id"])),
                    "objectCount": 0,
                    "invalidCount": 0,
                    "analyzed": False,
                    "error": None,
                    "invalidObjects": [],
                }
            )

        if not analyze or not reports:
            return reports

        # Re-capture the model metadata on every analysis (including
        # Re-analyze) so the valid fix targets and broken-element checks
        # reflect any changes to the model since the view was opened.
        measures, columns, hierarchies, objects = _model_objects_full()
        widget.model_objects = objects

        for rep in reports:
            try:
                parts = _get_report_definition(rep["id"])
                references, report_measures = _extract_report_objects(parts)
                invalid = []
                for ref in references:
                    otype = ref["objectType"]
                    tname = ref["table"]
                    oname = ref["name"]
                    if otype == "Measure":
                        valid = oname in measures or oname in report_measures
                    elif otype == "Column":
                        valid = format_dax_object_name(tname, oname) in columns
                    elif otype == "Hierarchy":
                        valid = format_dax_object_name(tname, oname) in hierarchies
                    else:
                        valid = True
                    if not valid:
                        invalid.append(
                            {"table": tname, "name": oname, "objectType": otype}
                        )
                rep["objectCount"] = len(references)
                rep["invalidCount"] = len(invalid)
                rep["invalidObjects"] = invalid
                rep["analyzed"] = True
            except Exception as e:
                rep["analyzed"] = False
                rep["error"] = str(e)

        return reports

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

    # Initial load (downstream reports only, no analysis).
    try:
        initial_reports = _build_reports(analyze=False)
        initial_status = {}
    except Exception as e:
        initial_reports = []
        initial_status = {"message": f"Error loading lineage: {e}", "kind": "error"}

    # Valid model objects offered as fix targets in the report detail panel.
    try:
        initial_model_objects = _model_objects_full()[3]
    except Exception:
        initial_model_objects = []

    class LineageViewWidget(anywidget.AnyWidget):
        _esm = _WIDGET_JS
        _css = _WIDGET_CSS

        dataset_name = traitlets.Unicode("").tag(sync=True)
        workspace_name = traitlets.Unicode("").tag(sync=True)
        workspace_id = traitlets.Unicode("").tag(sync=True)
        reports = traitlets.List().tag(sync=True)
        analyzed = traitlets.Bool(False).tag(sync=True)
        model_objects = traitlets.List().tag(sync=True)
        workspaces = traitlets.List().tag(sync=True)
        datasets = traitlets.Dict().tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        rebind_done = traitlets.Int(0).tag(sync=True)
        fixes_saved = traitlets.Int(0).tag(sync=True)
        busy = traitlets.Bool(False).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)

    widget = LineageViewWidget(
        dataset_name=ds_name,
        workspace_name=ws_name or "",
        workspace_id=ws_id,
        reports=initial_reports,
        analyzed=False,
        model_objects=initial_model_objects,
        workspaces=_list_workspaces_payload(),
        datasets={},
        status=initial_status,
        pending_action={},
        run=0,
        rebind_done=0,
        fixes_saved=0,
        busy=False,
        dark_mode=bool(dark_mode),
    )

    def _on_run(_change):
        data = dict(widget.pending_action or {})
        action = data.get("action")
        if not action:
            return
        widget.busy = True
        try:
            if action == "analyze":
                widget.reports = _build_reports(analyze=True)
                widget.analyzed = True
                widget.status = {}

            elif action == "refresh":
                keep = widget.analyzed
                widget.reports = _build_reports(analyze=keep)
                widget.status = {"message": "Lineage refreshed.", "kind": "success"}

            elif action == "list_datasets":
                target_ws = data.get("workspace_id")
                if target_ws:
                    new_map = dict(widget.datasets)
                    new_map[str(target_ws)] = _list_datasets_payload(target_ws)
                    widget.datasets = new_map

            elif action == "rebind":
                report_ids = data.get("report_ids") or []
                target_ds = data.get("dataset_id")
                target_ds_ws = data.get("dataset_workspace_id") or ws_id
                if not report_ids or not target_ds:
                    widget.status = {
                        "message": "Select a target semantic model to rebind.",
                        "kind": "error",
                    }
                    return
                report_rebind(
                    report=[str(x) for x in report_ids],
                    dataset=str(target_ds),
                    report_workspace=ws_id,
                    dataset_workspace=str(target_ds_ws),
                )
                # Rebound reports drop out of this model's lineage; reload.
                keep = widget.analyzed
                widget.reports = _build_reports(analyze=keep)
                widget.rebind_done = widget.rebind_done + 1
                widget.status = {
                    "message": (
                        f"Rebound {len(report_ids)} report"
                        f"{'' if len(report_ids) == 1 else 's'} to the selected "
                        "semantic model."
                    ),
                    "kind": "success",
                }

            elif action == "save_fixes":
                fixes = data.get("fixes") or []
                if not fixes:
                    return

                # Group staged fixes by the report they apply to.
                by_report = {}
                for f in fixes:
                    by_report.setdefault(f.get("reportId"), []).append(f)

                saved = 0
                for report_id, flist in by_report.items():
                    if not report_id:
                        continue
                    report_fixes = []
                    for f in flist:
                        target_name = f.get("targetName") or ""
                        if not target_name:
                            continue
                        report_fixes.append(
                            {
                                "objectType": f.get("objectType") or "",
                                "oldTable": f.get("brokenTable") or "",
                                "oldName": f.get("brokenName") or "",
                                "newTable": f.get("targetTable") or "",
                                "newName": target_name,
                            }
                        )
                    if not report_fixes:
                        continue
                    _apply_report_fixes(report_id, report_fixes)
                    saved += len(report_fixes)

                # Re-analyze so the fixed objects drop off the broken list.
                keep = widget.analyzed
                widget.reports = _build_reports(analyze=keep)
                widget.fixes_saved = widget.fixes_saved + 1
                widget.status = {
                    "message": f"Saved {saved} fix{'' if saved == 1 else 'es'}.",
                    "kind": "success",
                }
        except Exception as e:
            widget.status = {"message": f"Error: {e}", "kind": "error"}
        finally:
            widget.busy = False

    widget.observe(_on_run, names=["run"])

    display(widget)
