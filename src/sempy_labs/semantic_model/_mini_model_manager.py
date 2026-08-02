from typing import Optional
from uuid import UUID
from sempy._utils._log import log

_WIDGET_CSS = """
.slls-mmm {
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
    --slls-orange-soft: rgba(255, 149, 0, 0.12);
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
    -moz-osx-font-smoothing: grayscale;
    color: var(--slls-text);
    width: 100%;
    max-width: 960px;
    background: var(--slls-bg-solid);
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    box-shadow: var(--slls-shadow);
    padding: 24px;
    box-sizing: border-box;
}
@media (prefers-color-scheme: dark) {
    .slls-mmm.slls-mmm-auto {
        --slls-bg-solid: #1c1c1e;
        --slls-surface: rgba(255, 255, 255, 0.04);
        --slls-surface-2: rgba(255, 255, 255, 0.03);
        --slls-border: rgba(255, 255, 255, 0.08);
        --slls-border-strong: rgba(255, 255, 255, 0.16);
        --slls-text: #f5f5f7;
        --slls-text-secondary: #a1a1a6;
        --slls-text-tertiary: #6e6e73;
        --slls-accent: #0A84FF;
        --slls-accent-soft: rgba(10, 132, 255, 0.18);
        --slls-shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.5);
    }
}
.slls-mmm.slls-mmm-dark {
    --slls-bg-solid: #1c1c1e;
    --slls-surface: rgba(255, 255, 255, 0.04);
    --slls-surface-2: rgba(255, 255, 255, 0.03);
    --slls-border: rgba(255, 255, 255, 0.08);
    --slls-border-strong: rgba(255, 255, 255, 0.16);
    --slls-text: #f5f5f7;
    --slls-text-secondary: #a1a1a6;
    --slls-text-tertiary: #6e6e73;
    --slls-accent: #0A84FF;
    --slls-accent-soft: rgba(10, 132, 255, 0.18);
    --slls-shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.5);
}
.slls-mmm * { box-sizing: border-box; }

.slls-mmm.slls-mmm-fs {
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
    overflow: hidden;
    display: flex;
    flex-direction: column;
    background: var(--slls-bg-solid);
}
.slls-mmm:fullscreen,
.slls-mmm:-webkit-full-screen { background: var(--slls-bg-solid); }
.slls-mmm::backdrop { background: var(--slls-bg-solid); }
.slls-mmm.slls-mmm-fs .slls-mmm-objects-page {
    display: flex;
    flex: 1 1 auto;
    flex-direction: column;
    min-height: 0;
}
.slls-mmm.slls-mmm-fs .slls-mmm-tree {
    flex: 1 1 auto;
    max-height: none;
    min-height: 180px;
}
.slls-mmm.slls-mmm-fs .slls-mmm-filters-page {
    flex: 1 1 auto;
    min-height: 0;
    overflow-y: auto;
    padding-right: 4px;
}

/* ---------------- Header ---------------- */
.slls-mmm-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 16px;
    flex-wrap: wrap;
}
.slls-mmm-title-icon {
    display: inline-flex;
    width: 40px;
    height: 40px;
    color: var(--slls-accent);
    flex-shrink: 0;
}
.slls-mmm-title-icon svg { display: block; width: 40px; height: 40px; }
.slls-mmm-titlewrap {
    display: flex;
    flex-direction: column;
    margin-right: auto;
    min-width: 0;
}
.slls-mmm-title-row { display: flex; align-items: center; gap: 8px; }
.slls-mmm-title {
    font-size: 22px;
    font-weight: 600;
    letter-spacing: -0.01em;
    line-height: 1.15;
}
.slls-mmm-subtitle {
    font-size: 12px;
    color: var(--slls-text-secondary);
    margin-top: 2px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 560px;
}
.slls-mmm-subtitle .slls-mmm-sep { color: var(--slls-text-tertiary); margin: 0 6px; }
.slls-mmm-subtitle b { color: var(--slls-text); font-weight: 500; }

/* ---------------- Controls ---------------- */
.slls-mmm-btn {
    appearance: none;
    border: 1px solid var(--slls-border-strong);
    background: var(--slls-surface);
    color: var(--slls-text);
    font-family: inherit;
    font-size: 13.5px;
    font-weight: 500;
    padding: 7px 16px;
    border-radius: 999px;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 7px;
    transition: background 120ms ease, border-color 120ms ease,
        transform 80ms ease, box-shadow 120ms ease, opacity 120ms ease;
}
.slls-mmm-btn:hover { background: var(--slls-surface-2); border-color: var(--slls-text-tertiary); }
.slls-mmm-btn:active { transform: scale(0.97); }
.slls-mmm-btn:disabled { opacity: 0.4; cursor: not-allowed; }
.slls-mmm-btn svg { display: block; width: 15px; height: 15px; }
.slls-mmm-btn-primary {
    background: var(--slls-accent);
    border-color: var(--slls-accent);
    color: #fff;
}
.slls-mmm-btn-primary:hover { background: var(--slls-accent-hover); border-color: var(--slls-accent-hover); }
.slls-mmm-btn-icon {
    width: 32px; height: 32px;
    padding: 0;
    justify-content: center;
    border-radius: 50%;
}

.slls-mmm-input, .slls-mmm-select {
    appearance: none;
    -webkit-appearance: none;
    width: 100%;
    background: var(--slls-surface);
    border: 1px solid var(--slls-border-strong);
    border-radius: 999px;
    padding: 7px 14px;
    font-size: 13.5px;
    color: var(--slls-text);
    font-family: inherit;
    transition: border-color 120ms ease, box-shadow 120ms ease;
}
.slls-mmm-select {
    padding-right: 32px;
    cursor: pointer;
    background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6'><path fill='%236e6e73' d='M0 0l5 6 5-6z'/></svg>");
    background-repeat: no-repeat;
    background-position: right 12px center;
}
.slls-mmm-select option { background-color: var(--slls-bg-solid); color: var(--slls-text); }
.slls-mmm-input::placeholder { color: var(--slls-text-tertiary); }
.slls-mmm-input:focus, .slls-mmm-select:focus {
    outline: none;
    border-color: var(--slls-accent);
    box-shadow: 0 0 0 3px var(--slls-accent-soft);
}
.slls-mmm-input.slls-mmm-mono {
    font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
    font-size: 12.5px;
    border-radius: var(--slls-radius-sm);
}

/* ---------------- Segmented mode control ---------------- */
.slls-mmm-segment {
    display: inline-flex;
    padding: 3px;
    gap: 3px;
    background: var(--slls-surface-2);
    border: 1px solid var(--slls-border);
    border-radius: 999px;
    margin-bottom: 16px;
}
.slls-mmm-segment button {
    appearance: none;
    border: none;
    background: transparent;
    color: var(--slls-text-secondary);
    font-family: inherit;
    font-size: 13px;
    font-weight: 600;
    padding: 6px 16px;
    border-radius: 999px;
    cursor: pointer;
    transition: background 120ms ease, color 120ms ease;
}
.slls-mmm-segment button:hover { color: var(--slls-text); }
.slls-mmm-segment button.active { background: var(--slls-accent); color: #fff; }
.slls-mmm-segment button:disabled { opacity: 0.5; cursor: not-allowed; }

/* ---------------- Fields ---------------- */
.slls-mmm-fields {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 14px;
    margin-bottom: 16px;
}
@media (max-width: 620px) {
    .slls-mmm-fields { grid-template-columns: minmax(0, 1fr); }
}
.slls-mmm-field { display: flex; flex-direction: column; gap: 6px; min-width: 0; }
.slls-mmm-label {
    font-size: 10.5px;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--slls-text-secondary);
}

/* ---------------- Model picker ---------------- */
.slls-mmm-picker-wrap {
    display: flex;
    flex: 1 1 auto;
    align-items: center;
    justify-content: center;
    min-height: 280px;
    padding: 24px 0;
}
.slls-mmm-picker {
    width: 100%;
    max-width: 760px;
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    background: var(--slls-surface-2);
    padding: 22px 24px;
}
.slls-mmm-picker-title { font-size: 17px; font-weight: 600; }
.slls-mmm-picker-sub {
    margin-top: 3px;
    color: var(--slls-text-secondary);
    font-size: 12.5px;
}
.slls-mmm-picker-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 16px;
    margin-top: 20px;
}
.slls-mmm-picker-actions { display: flex; justify-content: flex-end; gap: 10px; margin-top: 20px; }
.slls-mmm-picker-combo { position: relative; }
.slls-mmm-picker-combo-input { padding-right: 14px; }
.slls-mmm-picker-combo-list {
    display: none;
    position: absolute;
    top: calc(100% + 5px);
    left: 0;
    right: 0;
    z-index: 50;
    max-height: 220px;
    overflow-y: auto;
    padding: 4px;
    border: 1px solid var(--slls-border-strong);
    border-radius: var(--slls-radius-sm);
    background: var(--slls-bg-solid);
    box-shadow: var(--slls-shadow);
}
.slls-mmm-picker-combo.open .slls-mmm-picker-combo-list { display: block; }
.slls-mmm-picker-combo-option {
    display: block;
    width: 100%;
    padding: 7px 10px;
    border: none;
    border-radius: 6px;
    background: transparent;
    color: var(--slls-text);
    font: inherit;
    font-size: 13px;
    text-align: left;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    cursor: pointer;
}
.slls-mmm-picker-combo-option:hover,
.slls-mmm-picker-combo-option.active { background: var(--slls-accent-soft); }
.slls-mmm-picker-combo-option.selected { color: var(--slls-accent); font-weight: 600; }
.slls-mmm-picker-combo-empty {
    padding: 8px 10px;
    color: var(--slls-text-tertiary);
    font-size: 12.5px;
}
@media (max-width: 620px) {
    .slls-mmm-picker-grid { grid-template-columns: minmax(0, 1fr); }
}

/* ---------------- Panels / cards ---------------- */
.slls-mmm-panel {
    border: 1px solid var(--slls-border);
    background: var(--slls-surface-2);
    border-radius: var(--slls-radius);
    padding: 14px 16px;
    margin-bottom: 16px;
}
.slls-mmm-hide { display: none !important; }
.slls-mmm-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 8px 28px;
    align-items: center;
}
.slls-mmm-meta-item { display: flex; align-items: center; gap: 8px; min-width: 0; }
.slls-mmm-meta-value { font-size: 13px; color: var(--slls-text); }
.slls-mmm-link {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    color: var(--slls-accent);
    text-decoration: none;
    font-size: 13px;
    font-weight: 500;
}
.slls-mmm-link:hover { text-decoration: underline; }
.slls-mmm-link svg { width: 13px; height: 13px; }

.slls-mmm-note {
    display: flex;
    gap: 10px;
    align-items: flex-start;
    border-radius: var(--slls-radius-sm);
    padding: 12px 14px;
    font-size: 13px;
    line-height: 1.45;
    margin-top: 12px;
}
.slls-mmm-note svg { width: 16px; height: 16px; flex-shrink: 0; margin-top: 1px; }
.slls-mmm-note.info { background: var(--slls-accent-soft); color: var(--slls-text); }
.slls-mmm-note.warn { background: var(--slls-orange-soft); color: var(--slls-text); }
.slls-mmm-note.error { background: var(--slls-danger-soft); color: var(--slls-text); }
.slls-mmm-note.success { background: var(--slls-success-soft); color: var(--slls-text); }
.slls-mmm-note b { font-weight: 600; }
.slls-mmm-broken-list {
    list-style: none;
    margin: 6px 0 0 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 3px;
    max-height: 160px;
    overflow-y: auto;
}
.slls-mmm-broken-list li {
    font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
    font-size: 12px;
    color: var(--slls-danger);
}

/* ---------------- Toolbar / tree ---------------- */
.slls-mmm-toolbar {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 12px;
    flex-wrap: wrap;
}
.slls-mmm-search { flex: 1; min-width: 180px; max-width: 360px; }
.slls-mmm-summary {
    margin-left: auto;
    font-size: 12.5px;
    color: var(--slls-text-secondary);
}

.slls-mmm-tree {
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    overflow: hidden;
    background: var(--slls-surface);
    max-height: 460px;
    overflow-y: auto;
}
.slls-mmm-objects-page.slls-mmm-objects-fs {
    position: fixed;
    inset: 0;
    z-index: 2147483001;
    display: flex;
    flex-direction: column;
    width: 100vw;
    height: 100vh;
    min-height: 0;
    padding: 24px;
    background: var(--slls-bg-solid);
}
.slls-mmm-objects-page.slls-mmm-objects-fs .slls-mmm-tree {
    flex: 1 1 auto;
    max-height: none;
    min-height: 0;
}
.slls-mmm-tree::-webkit-scrollbar { width: 10px; height: 10px; }
.slls-mmm-tree::-webkit-scrollbar-thumb {
    background: var(--slls-border-strong);
    border-radius: 999px;
    background-clip: padding-box;
    border: 2px solid transparent;
}
.slls-mmm-table { border-bottom: 1px solid var(--slls-border); }
.slls-mmm-table:last-child { border-bottom: none; }
.slls-mmm-table.hidden-match { display: none; }
.slls-mmm-row {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 9px 14px;
    cursor: pointer;
    user-select: none;
    transition: background 100ms ease;
}
.slls-mmm-row:hover { background: var(--slls-surface-2); }
.slls-mmm-caret {
    width: 16px; height: 16px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    color: var(--slls-text-tertiary);
    transition: transform 160ms ease;
    flex-shrink: 0;
}
.slls-mmm-table.expanded .slls-mmm-caret { transform: rotate(90deg); }
.slls-mmm-check {
    width: 18px;
    height: 18px;
    border: 1.5px solid var(--slls-border-strong);
    border-radius: 5px;
    background: var(--slls-bg-solid);
    display: inline-flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    transition: background 100ms ease, border-color 100ms ease;
    cursor: pointer;
}
.slls-mmm-check[data-state="all"], .slls-mmm-check[data-state="some"] {
    background: var(--slls-accent);
    border-color: var(--slls-accent);
}
.slls-mmm-check[data-state="all"]::after {
    content: "";
    width: 10px; height: 6px;
    border-left: 2px solid #fff;
    border-bottom: 2px solid #fff;
    transform: rotate(-45deg) translate(1px, -1px);
}
.slls-mmm-check[data-state="some"]::after {
    content: "";
    width: 10px; height: 2px;
    background: #fff;
    border-radius: 1px;
}
.slls-mmm-icon {
    width: 22px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    color: var(--slls-text);
    flex-shrink: 0;
    opacity: 0.85;
}
.slls-mmm-icon svg { display: block; }
.slls-mmm-row.is-hidden .slls-mmm-icon { color: var(--slls-text-tertiary); opacity: 0.7; }
.slls-mmm-name { font-size: 14px; font-weight: 500; }
.slls-mmm-name.hidden-obj { color: var(--slls-text-tertiary); font-style: italic; }
.slls-mmm-table-summary {
    font-size: 12px;
    color: var(--slls-text-tertiary);
    margin-left: 4px;
}
.slls-mmm-badge {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--slls-accent);
    background: var(--slls-accent-soft);
    border-radius: 999px;
    padding: 2px 7px;
    flex-shrink: 0;
}
.slls-mmm-children {
    display: none;
    padding: 2px 0 8px 0;
    background: var(--slls-surface-2);
    border-top: 1px solid var(--slls-border);
}
.slls-mmm-table.expanded .slls-mmm-children { display: block; }
.slls-mmm-child { padding-left: 56px; padding-top: 5px; padding-bottom: 5px; }
.slls-mmm-child.filtered-out { display: none; }
.slls-mmm-child.slls-mmm-disabled { cursor: default; opacity: 0.65; }
.slls-mmm-child.slls-mmm-disabled:hover { background: transparent; }
.slls-mmm-dirty {
    display: inline-block;
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--slls-orange);
    margin-left: 6px;
    flex-shrink: 0;
}
.slls-mmm-empty {
    padding: 32px 16px;
    text-align: center;
    color: var(--slls-text-tertiary);
    font-size: 13.5px;
}

/* ---------------- Filters page ---------------- */
.slls-mmm-desc {
    font-size: 12.5px;
    line-height: 1.5;
    color: var(--slls-text-secondary);
    margin-bottom: 12px;
}
.slls-mmm-desc code, .slls-mmm-hint code {
    font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
    font-size: 11.5px;
    background: var(--slls-surface-2);
    border-radius: 4px;
    padding: 1px 5px;
    color: var(--slls-text);
}
.slls-mmm-verifybar {
    display: flex;
    align-items: center;
    gap: 14px;
    flex-wrap: wrap;
    margin-bottom: 12px;
}
.slls-mmm-verifysummary {
    display: flex;
    align-items: center;
    gap: 14px;
    flex-wrap: wrap;
    font-size: 12.5px;
    font-weight: 500;
}
.slls-mmm-verifysummary span { display: inline-flex; align-items: center; gap: 5px; }
.slls-mmm-verifysummary svg { width: 14px; height: 14px; }
.slls-mmm-verifyprogress {
    display: none;
    align-items: center;
    gap: 10px;
    margin-bottom: 12px;
    color: var(--slls-text-secondary);
    font-size: 12.5px;
}
.slls-mmm-verifyprogress.show { display: flex; }
.slls-mmm-updateprogress {
    display: none;
    align-items: center;
    gap: 10px;
    margin-bottom: 16px;
    color: var(--slls-text-secondary);
    font-size: 12.5px;
}
.slls-mmm-updateprogress.show { display: flex; }
.slls-mmm-verifytrack {
    position: relative;
    flex: 1 1 auto;
    height: 4px;
    overflow: hidden;
    border-radius: 999px;
    background: var(--slls-accent-soft);
}
.slls-mmm-verifytrack::after {
    content: "";
    position: absolute;
    inset: 0 auto 0 0;
    width: 35%;
    border-radius: inherit;
    background: var(--slls-accent);
    animation: slls-mmm-verify-progress 1.1s ease-in-out infinite;
}
@keyframes slls-mmm-verify-progress {
    from { transform: translateX(-110%); }
    to { transform: translateX(320%); }
}
.slls-mmm-ok { color: var(--slls-success); }
.slls-mmm-bad { color: var(--slls-danger); }
.slls-mmm-warnc { color: var(--slls-orange); }

.slls-mmm-switch {
    display: flex;
    align-items: flex-start;
    gap: 12px;
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius-sm);
    padding: 10px 14px;
    margin-bottom: 14px;
    cursor: pointer;
}
.slls-mmm-switch-track {
    position: relative;
    width: 36px;
    height: 20px;
    border-radius: 999px;
    background: var(--slls-border-strong);
    flex-shrink: 0;
    margin-top: 1px;
    transition: background 140ms ease;
}
.slls-mmm-switch.on .slls-mmm-switch-track { background: var(--slls-accent); }
.slls-mmm-switch-knob {
    position: absolute;
    top: 2px; left: 2px;
    width: 16px; height: 16px;
    border-radius: 50%;
    background: #fff;
    box-shadow: 0 1px 2px rgba(0,0,0,0.25);
    transition: transform 140ms ease;
}
.slls-mmm-switch.on .slls-mmm-switch-knob { transform: translateX(16px); }
.slls-mmm-switch-text { display: flex; flex-direction: column; gap: 2px; }
.slls-mmm-switch-title { font-size: 13.5px; font-weight: 500; }
.slls-mmm-switch-desc { font-size: 12px; color: var(--slls-text-secondary); line-height: 1.45; }

.slls-mmm-filterlist { display: flex; flex-direction: column; gap: 10px; }
.slls-mmm-filtercard {
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius-sm);
    padding: 12px 14px;
}
.slls-mmm-filterhead {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
}
.slls-mmm-filterhead .slls-mmm-icon { width: 16px; }
.slls-mmm-filterhead svg { width: 15px; height: 15px; }
.slls-mmm-filtername { font-size: 13.5px; font-weight: 500; }
.slls-mmm-sql {
    margin: 8px 0 0 0;
    padding: 10px 12px;
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius-sm);
    background: var(--slls-surface-2);
    font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
    font-size: 11.5px;
    line-height: 1.5;
    color: var(--slls-text);
    overflow-x: auto;
    white-space: pre-wrap;
    word-break: break-word;
}
.slls-mmm-filterstate {
    display: flex;
    align-items: flex-start;
    gap: 6px;
    font-size: 12.5px;
    margin-top: 8px;
}
.slls-mmm-filterstate svg { width: 14px; height: 14px; flex-shrink: 0; margin-top: 1px; }

/* ---------------- Footer / status ---------------- */
.slls-mmm-footer {
    display: flex;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
    margin-top: 18px;
    padding-top: 16px;
    border-top: 1px solid var(--slls-border);
}
.slls-mmm-hint {
    font-size: 12.5px;
    color: var(--slls-text-secondary);
    flex: 1;
    min-width: 180px;
    line-height: 1.45;
}
.slls-mmm-status {
    margin-top: 14px;
    padding: 10px 14px;
    border-radius: var(--slls-radius-sm);
    font-size: 13.5px;
    display: none;
}
.slls-mmm-status.show { display: block; animation: slls-mmm-fade 200ms ease; }
.slls-mmm-status.success { background: var(--slls-success-soft); color: var(--slls-success); }
.slls-mmm-status.error { background: var(--slls-danger-soft); color: var(--slls-danger); }
.slls-mmm-status.warning { background: var(--slls-orange-soft); color: var(--slls-orange); }
.slls-mmm-status.info { background: var(--slls-accent-soft); color: var(--slls-accent); }
@keyframes slls-mmm-fade {
    from { opacity: 0; transform: translateY(-4px); }
    to { opacity: 1; transform: translateY(0); }
}
.slls-mmm > * { transition: opacity 120ms ease; }
.slls-mmm-busy > :not(.slls-mmm-header):not(.slls-mmm-updateprogress) {
    pointer-events: none;
    opacity: 0.55;
}

.slls-mmm-attribution {
    margin-top: 18px;
    text-align: right;
    font-size: 11.5px;
    color: var(--slls-text-tertiary);
}
.slls-mmm-attribution a {
    color: var(--slls-text-tertiary);
    text-decoration: none;
    transition: color 120ms ease;
}
.slls-mmm-attribution a:hover { color: var(--slls-accent); }
"""


_WIDGET_JS = r"""
function render({ model, el }) {
    const TYPES = ["columns", "measures", "hierarchies"];

    const ICON_SVG = {
        columns: `__SLLS_ICON_COLUMN__`,
        measures: `__SLLS_ICON_MEASURE__`,
        hierarchies: `__SLLS_ICON_HIERARCHY__`,
        table: `__SLLS_ICON_TABLE__`,
        calc_group: `__SLLS_ICON_CALC_GROUP__`,
    };
    const CARET = `__SLLS_ICON_CARET__`;
    const EXPAND_ROWS_SVG = `__SLLS_ICON_EXPAND_ROWS__`;
    const COLLAPSE_ROWS_SVG = `__SLLS_ICON_COLLAPSE_ROWS__`;
    const SUN_SVG = `__SLLS_ICON_SUN__`;
    const MOON_SVG = `__SLLS_ICON_MOON__`;
    const FS_SVG = `__SLLS_ICON_FULLSCREEN__`;
    const FSX_SVG = `__SLLS_ICON_FULLSCREEN_EXIT__`;
    const CHECK_SVG = `__SLLS_ICON_CHECK_CIRCLE__`;
    const ERROR_SVG = `__SLLS_ICON_ERROR_CIRCLE__`;
    const ALERT_SVG = `__SLLS_ICON_ALERT__`;
    const INFO_SVG = `__SLLS_ICON_INFO__`;
    const LINK_SVG = `__SLLS_ICON_EXTERNAL_LINK__`;
    const DEPLOY_SVG = `__SLLS_ICON_UPLOAD__`;
    const SYNC_SVG = `__SLLS_ICON_SYNC__`;
    const PREV_SVG = `__SLLS_ICON_CHEVRON_LEFT__`;
    const NEXT_SVG = `__SLLS_ICON_CHEVRON_RIGHT__`;
    const APP_SVG = `__SLLS_ICON_MINI_MODEL_MANAGER__`;
    const SWAP_SVG = `__SLLS_ICON_SWAP__`;
    const OBJECT_EXPAND_SVG = `__SLLS_ICON_EXPAND_DIAGONAL__`;
    const OBJECT_COLLAPSE_SVG = `__SLLS_ICON_COLLAPSE_DIAGONAL__`;

    // ---------------- Local state ----------------
    let selection = {};
    let originalSelection = {};
    let filters = {};
    let verifyResults = {};
    let expanded = {};
    let filterText = "";
    let step = "objects";
    let metadataOnly = false;
    let verifyingFilters = false;
    let updatingFromMaster = false;
    let fsMode = false;
    let objectsFsMode = false;
    let pickWs = model.get("workspace_id") || "";
    let pickDs = "";
    let pickerReopen = false;

    function getMode() { return model.get("mode") || "create"; }
    function getMetadata() { return model.get("metadata") || {}; }
    function getMasterInfo() { return model.get("master_info") || {}; }
    function getVerify() { return verifyResults; }
    function getDeploy() { return model.get("deploy_result") || {}; }
    function isBusy() { return model.get("busy") === true; }
    function isConnected() { return model.get("connected") === true; }
    function showPicker() { return !isConnected() || pickerReopen; }

    function escapeHtml(s) {
        return String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({
            "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
        }[c]));
    }

    // ---------------- Root ----------------
    const root = document.createElement("div");
    root.className = "slls-mmm";
    function applyTheme() {
        root.classList.remove("slls-mmm-dark", "slls-mmm-auto");
        const dm = model.get("dark_mode");
        if (dm === true) root.classList.add("slls-mmm-dark");
        else if (dm === null || dm === undefined) root.classList.add("slls-mmm-auto");
    }
    applyTheme();
    model.on("change:dark_mode", applyTheme);
    el.appendChild(root);

    // ---------------- Header ----------------
    const header = document.createElement("div");
    header.className = "slls-mmm-header";
    root.appendChild(header);

    const titleIcon = document.createElement("span");
    titleIcon.className = "slls-mmm-title-icon";
    titleIcon.innerHTML = APP_SVG;
    header.appendChild(titleIcon);

    const titleWrap = document.createElement("div");
    titleWrap.className = "slls-mmm-titlewrap";
    header.appendChild(titleWrap);

    const titleRow = document.createElement("div");
    titleRow.className = "slls-mmm-title-row";
    titleWrap.appendChild(titleRow);

    const title = document.createElement("div");
    title.className = "slls-mmm-title";
    title.textContent = "Mini Model Manager";
    titleRow.appendChild(title);

    const subtitle = document.createElement("div");
    subtitle.className = "slls-mmm-subtitle";
    titleWrap.appendChild(subtitle);

    function renderSubtitle() {
        const ds = model.get("dataset_name") || "";
        const ws = model.get("workspace_name") || "";
        subtitle.innerHTML = isConnected() && !showPicker()
            ? (ds ? `<b>${escapeHtml(ds)}</b>` : "") +
                (ds && ws ? `<span class="slls-mmm-sep">·</span>` : "") +
                (ws ? escapeHtml(ws) : "")
            : "Select a semantic model to manage";
    }

    const changeModelBtn = document.createElement("button");
    changeModelBtn.className = "slls-mmm-btn slls-mmm-btn-icon";
    changeModelBtn.type = "button";
    changeModelBtn.innerHTML = SWAP_SVG;
    changeModelBtn.title = "Change semantic model / workspace";
    changeModelBtn.setAttribute("aria-label", changeModelBtn.title);
    changeModelBtn.addEventListener("click", () => {
        pickerReopen = true;
        pickWs = model.get("workspace_id") || "";
        pickDs = "";
        if (pickWs && !(model.get("datasets") || {})[pickWs]) {
            send({ action: "list_datasets", workspace_id: pickWs });
        }
        renderConnectionState();
        renderSubtitle();
    });
    titleRow.appendChild(changeModelBtn);

    const fsBtn = document.createElement("button");
    fsBtn.className = "slls-mmm-btn slls-mmm-btn-icon";
    fsBtn.type = "button";
    function renderFsBtn() {
        fsBtn.innerHTML = fsMode ? FSX_SVG : FS_SVG;
        fsBtn.title = fsMode ? "Exit full screen" : "Toggle full screen";
        fsBtn.setAttribute("aria-label", fsBtn.title);
    }
    function setFullscreen(on) {
        fsMode = on;
        root.classList.toggle("slls-mmm-fs", on);
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
        } catch (e) { /* native fullscreen blocked; the CSS overlay handles it */ }
        renderFsBtn();
    }
    fsBtn.addEventListener("click", () => setFullscreen(!fsMode));
    document.addEventListener("fullscreenchange", () => {
        const nativeOn = !!(document.fullscreenElement || document.webkitFullscreenElement);
        if (!nativeOn && fsMode) { fsMode = false; root.classList.remove("slls-mmm-fs"); renderFsBtn(); }
    });
    document.addEventListener("keydown", (e) => {
        if (e.key === "Escape" && objectsFsMode) setObjectsFullscreen(false);
        else if (e.key === "Escape" && fsMode) setFullscreen(false);
    });
    renderFsBtn();
    header.appendChild(fsBtn);

    const themeBtn = document.createElement("button");
    themeBtn.className = "slls-mmm-btn slls-mmm-btn-icon";
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

    // ---------------- Model picker ----------------
    const pickerPage = document.createElement("div");
    pickerPage.className = "slls-mmm-picker-wrap";
    root.appendChild(pickerPage);

    function createPickerCombo(host, options, value, placeholder, disabled, onChoose, onClear) {
        const combo = document.createElement("div");
        combo.className = "slls-mmm-picker-combo";

        const input = document.createElement("input");
        input.type = "text";
        input.className = "slls-mmm-input slls-mmm-picker-combo-input";
        input.placeholder = placeholder;
        input.autocomplete = "off";
        input.spellcheck = false;
        input.disabled = disabled;
        input.setAttribute("role", "combobox");
        input.setAttribute("aria-autocomplete", "list");
        input.setAttribute("aria-expanded", "false");

        const selected = options.find((option) => option.id === value);
        input.value = selected ? selected.name : "";

        const list = document.createElement("div");
        list.className = "slls-mmm-picker-combo-list";
        list.setAttribute("role", "listbox");
        combo.appendChild(input);
        combo.appendChild(list);
        host.appendChild(combo);

        let shown = [];
        let activeIndex = -1;

        function close() {
            combo.classList.remove("open");
            input.setAttribute("aria-expanded", "false");
            activeIndex = -1;
        }

        function setActive(index) {
            const rows = list.querySelectorAll(".slls-mmm-picker-combo-option");
            if (rows.length === 0) { activeIndex = -1; return; }
            activeIndex = Math.max(0, Math.min(index, rows.length - 1));
            rows.forEach((row, rowIndex) => row.classList.toggle("active", rowIndex === activeIndex));
            rows[activeIndex].scrollIntoView({ block: "nearest" });
        }

        function choose(option, focusNext = false) {
            input.value = option.name;
            close();
            onChoose(option, focusNext);
        }

        function renderList() {
            const query = input.value.trim().toLowerCase();
            shown = query
                ? options.filter((option) => option.name.toLowerCase().includes(query))
                : options;
            list.innerHTML = "";
            if (shown.length === 0) {
                const empty = document.createElement("div");
                empty.className = "slls-mmm-picker-combo-empty";
                empty.textContent = options.length === 0 ? "No items" : "No matches";
                list.appendChild(empty);
                activeIndex = -1;
                return;
            }
            for (const option of shown) {
                const row = document.createElement("button");
                row.type = "button";
                row.tabIndex = -1;
                row.className = "slls-mmm-picker-combo-option" + (option.id === value ? " selected" : "");
                row.setAttribute("role", "option");
                row.setAttribute("aria-selected", String(option.id === value));
                row.textContent = option.name;
                row.title = option.name;
                row.addEventListener("mousedown", (event) => {
                    event.preventDefault();
                    choose(option);
                });
                list.appendChild(row);
            }
        }

        input.addEventListener("focus", () => {
            input.select();
            renderList();
            combo.classList.add("open");
            input.setAttribute("aria-expanded", "true");
        });
        input.addEventListener("input", () => {
            onClear();
            activeIndex = -1;
            renderList();
            setActive(0);
            combo.classList.add("open");
            input.setAttribute("aria-expanded", "true");
        });
        input.addEventListener("blur", () => { setTimeout(close, 100); });
        input.addEventListener("keydown", (event) => {
            if (event.key === "ArrowDown") {
                event.preventDefault();
                setActive(activeIndex + 1);
            } else if (event.key === "ArrowUp") {
                event.preventDefault();
                setActive(activeIndex <= 0 ? 0 : activeIndex - 1);
            } else if (event.key === "Enter") {
                event.preventDefault();
                if (activeIndex >= 0 && shown[activeIndex]) choose(shown[activeIndex]);
                else if (shown.length === 1) choose(shown[0]);
            } else if (event.key === "Escape") {
                event.preventDefault();
                close();
            } else if (event.key === "Tab") {
                if (activeIndex >= 0 && shown[activeIndex]) {
                    event.preventDefault();
                    choose(shown[activeIndex], true);
                } else {
                    close();
                }
            }
        });

        return input;
    }

    function renderPicker() {
        if (!showPicker()) {
            pickerPage.classList.add("slls-mmm-hide");
            return;
        }
        pickerPage.classList.remove("slls-mmm-hide");
        if (!pickWs) pickWs = model.get("workspace_id") || "";
        const workspaces = model.get("workspaces") || [];
        const datasets = (model.get("datasets") || {})[pickWs] || null;

        pickerPage.innerHTML =
            `<div class="slls-mmm-picker">` +
                `<div class="slls-mmm-picker-title">Choose a semantic model</div>` +
                `<div class="slls-mmm-picker-sub">Pick the master or mini model you want to work with.</div>` +
                `<div class="slls-mmm-picker-grid">` +
                    `<div class="slls-mmm-field"><label class="slls-mmm-label">Workspace</label>` +
                        `<div data-picker="workspace"></div></div>` +
                    `<div class="slls-mmm-field"><label class="slls-mmm-label">Semantic model</label>` +
                        `<div data-picker="dataset"></div></div>` +
                `</div>` +
                `<div class="slls-mmm-picker-actions">` +
                    `<button class="slls-mmm-btn slls-mmm-btn-primary" data-picker="connect" ${(!pickDs || isBusy()) ? "disabled" : ""}>Connect</button>` +
                    (isConnected()
                        ? `<button class="slls-mmm-btn" data-picker="cancel">Cancel</button>`
                        : "") +
                `</div>` +
            `</div>`;

        const connectBtn = pickerPage.querySelector('[data-picker="connect"]');
        createPickerCombo(
            pickerPage.querySelector('[data-picker="workspace"]'),
            workspaces,
            pickWs,
            "Search workspaces…",
            isBusy(),
            (workspace, focusNext) => {
                pickWs = workspace.id;
                pickDs = "";
                if (!(model.get("datasets") || {})[pickWs]) {
                    send({ action: "list_datasets", workspace_id: pickWs });
                }
                renderPicker();
                if (focusNext) {
                    const modelInput = pickerPage.querySelector('[data-picker="dataset"] input');
                    if (modelInput) modelInput.focus();
                }
            },
            () => {
                pickWs = "";
                pickDs = "";
                connectBtn.disabled = true;
            }
        );
        createPickerCombo(
            pickerPage.querySelector('[data-picker="dataset"]'),
            datasets || [],
            pickDs,
            !pickWs ? "Select a workspace first…" : datasets === null ? "Loading…" : "Search semantic models…",
            !pickWs || datasets === null || isBusy(),
            (dataset, focusNext) => {
                pickDs = dataset.id;
                connectBtn.disabled = false;
                if (focusNext) connectBtn.focus();
            },
            () => {
                pickDs = "";
                connectBtn.disabled = true;
            }
        );
        const cancelBtn = pickerPage.querySelector('[data-picker="cancel"]');
        if (cancelBtn) cancelBtn.onclick = () => {
            pickerReopen = false;
            reloadFromModel();
        };
        connectBtn.onclick = () => {
            if (!pickWs || !pickDs) return;
            const workspace = workspaces.find((w) => w.id === pickWs) || {};
            const dataset = (datasets || []).find((d) => d.id === pickDs) || {};
            send({
                action: "connect",
                workspace_id: pickWs,
                dataset_id: pickDs,
                workspace_name: workspace.name || "",
                dataset_name: dataset.name || "",
            });
        };
    }

    // ---------------- Mode toggle ----------------
    const segment = document.createElement("div");
    segment.className = "slls-mmm-segment";
    root.appendChild(segment);

    const createModeBtn = document.createElement("button");
    createModeBtn.type = "button";
    createModeBtn.textContent = "Create new";
    const manageModeBtn = document.createElement("button");
    manageModeBtn.type = "button";
    manageModeBtn.textContent = "Manage existing";
    segment.appendChild(createModeBtn);
    segment.appendChild(manageModeBtn);

    createModeBtn.addEventListener("click", () => switchMode("create"));
    manageModeBtn.addEventListener("click", () => switchMode("manage"));

    function switchMode(next) {
        if (next === getMode() || isBusy()) return;
        send({ action: "set_mode", mode: next });
    }

    // ---------------- Create-mode configuration ----------------
    const configPanel = document.createElement("div");
    configPanel.className = "slls-mmm-fields";
    root.appendChild(configPanel);

    const nameField = document.createElement("div");
    nameField.className = "slls-mmm-field";
    const nameLabel = document.createElement("div");
    nameLabel.className = "slls-mmm-label";
    nameLabel.textContent = "Mini model name";
    const nameInput = document.createElement("input");
    nameInput.type = "text";
    nameInput.className = "slls-mmm-input";
    nameInput.placeholder = "e.g. Sales (Mini)";
    nameField.appendChild(nameLabel);
    nameField.appendChild(nameInput);
    configPanel.appendChild(nameField);

    const targetField = document.createElement("div");
    targetField.className = "slls-mmm-field";
    const targetLabel = document.createElement("div");
    targetLabel.className = "slls-mmm-label";
    targetLabel.textContent = "Target workspace";
    const targetSelect = document.createElement("select");
    targetSelect.className = "slls-mmm-select";
    targetField.appendChild(targetLabel);
    targetField.appendChild(targetSelect);
    configPanel.appendChild(targetField);

    nameInput.addEventListener("input", () => renderFooter());
    targetSelect.addEventListener("change", () => renderFooter());

    function renderWorkspaces() {
        const list = model.get("workspaces") || [];
        const cur = targetSelect.value || model.get("workspace_id") || "";
        targetSelect.innerHTML = "";
        for (const w of list) {
            const opt = document.createElement("option");
            opt.value = w.id;
            opt.textContent = w.name;
            targetSelect.appendChild(opt);
        }
        if (list.some((w) => w.id === cur)) targetSelect.value = cur;
    }

    // ---------------- Manage-mode master panel ----------------
    const masterPanel = document.createElement("div");
    masterPanel.className = "slls-mmm-panel";
    root.appendChild(masterPanel);

    function renderMasterPanel() {
        if (getMode() !== "manage") {
            masterPanel.classList.add("slls-mmm-hide");
            return;
        }
        masterPanel.classList.remove("slls-mmm-hide");
        const err = model.get("mini_error") || "";
        if (err) {
            masterPanel.innerHTML =
                `<div class="slls-mmm-note warn" style="margin-top:0">${ALERT_SVG}` +
                `<div><b>Not a mini model.</b> ${escapeHtml(err)}</div></div>`;
            return;
        }
        const info = getMasterInfo();
        if (!info.datasetName) {
            masterPanel.innerHTML =
                `<div class="slls-mmm-note info" style="margin-top:0">${INFO_SVG}` +
                `<div>Reading the mini model's master reference…</div></div>`;
            return;
        }
        const url = info.datasetId && info.workspaceId
            ? `https://app.powerbi.com/onelake/details/${info.workspaceId}/dataset/${info.datasetId}/overview`
            : "";
        let html = '<div class="slls-mmm-meta">';
        html +=
            '<div class="slls-mmm-meta-item"><span class="slls-mmm-label">Master model</span>' +
            (url
                ? `<a class="slls-mmm-link" href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(info.datasetName)}${LINK_SVG}</a>`
                : `<span class="slls-mmm-meta-value">${escapeHtml(info.datasetName)}</span>`) +
            "</div>";
        html +=
            '<div class="slls-mmm-meta-item"><span class="slls-mmm-label">Master workspace</span>' +
            `<span class="slls-mmm-meta-value">${escapeHtml(info.workspaceName || "")}</span></div>`;
        html +=
            '<div class="slls-mmm-meta-item"><span class="slls-mmm-label">Last updated</span>' +
            `<span class="slls-mmm-meta-value">${escapeHtml(info.lastUpdatedDate || "Unknown")}</span></div>`;
        html += "</div>";

        const broken = model.get("broken_objects") || [];
        if (broken.length > 0) {
            const items = broken
                .map((b) => {
                    const label = b.name ? `${b.table}[${b.name}]` : `${b.table} (whole table)`;
                    return `<li>${escapeHtml(label)}</li>`;
                })
                .join("");
            html +=
                `<div class="slls-mmm-note error">${ERROR_SVG}<div>` +
                `<b>${broken.length} broken object${broken.length === 1 ? "" : "s"}.</b> ` +
                "These exist in the mini model but no longer in the master. " +
                "Updating from the master will drop them." +
                `<ul class="slls-mmm-broken-list">${items}</ul></div></div>`;
        }
        masterPanel.innerHTML = html;
    }

    // ---------------- Objects page ----------------
    const objectsPage = document.createElement("div");
    objectsPage.className = "slls-mmm-objects-page";
    root.appendChild(objectsPage);

    const toolbar = document.createElement("div");
    toolbar.className = "slls-mmm-toolbar";
    objectsPage.appendChild(toolbar);

    const search = document.createElement("input");
    search.type = "search";
    search.className = "slls-mmm-input slls-mmm-search";
    search.placeholder = "Search tables, columns, measures…";
    toolbar.appendChild(search);

    const expandAllBtn = document.createElement("button");
    expandAllBtn.className = "slls-mmm-btn slls-mmm-btn-icon";
    expandAllBtn.type = "button";
    expandAllBtn.innerHTML = EXPAND_ROWS_SVG;
    expandAllBtn.title = "Expand all tables";
    expandAllBtn.setAttribute("aria-label", expandAllBtn.title);
    toolbar.appendChild(expandAllBtn);

    const collapseAllBtn = document.createElement("button");
    collapseAllBtn.className = "slls-mmm-btn slls-mmm-btn-icon";
    collapseAllBtn.type = "button";
    collapseAllBtn.innerHTML = COLLAPSE_ROWS_SVG;
    collapseAllBtn.title = "Collapse all tables";
    collapseAllBtn.setAttribute("aria-label", collapseAllBtn.title);
    toolbar.appendChild(collapseAllBtn);

    const objectsFsBtn = document.createElement("button");
    objectsFsBtn.className = "slls-mmm-btn slls-mmm-btn-icon";
    objectsFsBtn.type = "button";
    function renderObjectsFsBtn() {
        objectsFsBtn.innerHTML = objectsFsMode ? OBJECT_COLLAPSE_SVG : OBJECT_EXPAND_SVG;
        objectsFsBtn.title = objectsFsMode
            ? "Exit object selection full screen"
            : "Expand object selection to full screen";
        objectsFsBtn.setAttribute("aria-label", objectsFsBtn.title);
    }
    function setObjectsFullscreen(on) {
        objectsFsMode = on;
        objectsPage.classList.toggle("slls-mmm-objects-fs", on);
        renderObjectsFsBtn();
    }
    objectsFsBtn.addEventListener("click", () => setObjectsFullscreen(!objectsFsMode));
    renderObjectsFsBtn();
    toolbar.appendChild(objectsFsBtn);

    const summaryEl = document.createElement("div");
    summaryEl.className = "slls-mmm-summary";
    toolbar.appendChild(summaryEl);

    const tree = document.createElement("div");
    tree.className = "slls-mmm-tree";
    objectsPage.appendChild(tree);

    search.addEventListener("input", () => {
        filterText = search.value;
        renderTree();
    });
    expandAllBtn.addEventListener("click", () => {
        for (const t of Object.keys(getMetadata())) expanded[t] = true;
        renderTree();
    });
    collapseAllBtn.addEventListener("click", () => {
        expanded = {};
        renderTree();
    });

    // ---------------- Filters page ----------------
    const filtersPage = document.createElement("div");
    filtersPage.className = "slls-mmm-filters-page";
    root.appendChild(filtersPage);

    const filtersDesc = document.createElement("div");
    filtersDesc.className = "slls-mmm-desc";
    filtersDesc.innerHTML =
        "Optionally restrict a Direct Lake table with a SQL <code>WHERE</code> expression " +
        "(referencing source column names). Verify checks every table at once — generating the SQL, " +
        "propagating filters through relationships (so a table inherits a related table's filter), " +
        "and validating each one on Spark.";
    filtersPage.appendChild(filtersDesc);

    const verifyBar = document.createElement("div");
    verifyBar.className = "slls-mmm-verifybar";
    filtersPage.appendChild(verifyBar);

    const verifyBtn = document.createElement("button");
    verifyBtn.className = "slls-mmm-btn slls-mmm-btn-primary";
    verifyBtn.type = "button";
    verifyBtn.innerHTML = `${CHECK_SVG}<span>Verify</span>`;
    verifyBar.appendChild(verifyBtn);

    const verifySummary = document.createElement("div");
    verifySummary.className = "slls-mmm-verifysummary";
    verifyBar.appendChild(verifySummary);

    const verifyProgress = document.createElement("div");
    verifyProgress.className = "slls-mmm-verifyprogress";
    verifyProgress.setAttribute("role", "progressbar");
    verifyProgress.setAttribute("aria-label", "Verifying filters");
    verifyProgress.innerHTML =
        '<div class="slls-mmm-verifytrack"></div><span>Verifying filters…</span>';
    filtersPage.appendChild(verifyProgress);

    const filtersStatus = document.createElement("div");
    filtersStatus.className = "slls-mmm-status";
    filtersPage.appendChild(filtersStatus);

    verifyBtn.addEventListener("click", () => {
        const active = activeFilters();
        if (Object.keys(active).length === 0) return;
        send({ action: "verify", filters: active, mini_name: nameInput.value.trim() });
    });

    const metadataSwitch = document.createElement("div");
    metadataSwitch.className = "slls-mmm-switch";
    metadataSwitch.innerHTML =
        '<div class="slls-mmm-switch-track"><div class="slls-mmm-switch-knob"></div></div>' +
        '<div class="slls-mmm-switch-text">' +
        '<div class="slls-mmm-switch-title">Metadata only</div>' +
        '<div class="slls-mmm-switch-desc"></div></div>';
    filtersPage.appendChild(metadataSwitch);
    metadataSwitch.addEventListener("click", () => {
        metadataOnly = !metadataOnly;
        renderMetadataSwitch();
    });
    function renderMetadataSwitch() {
        metadataSwitch.classList.toggle("on", metadataOnly);
        metadataSwitch.querySelector(".slls-mmm-switch-desc").textContent = metadataOnly
            ? "Deploys the semantic model metadata only — the materialized lake views are left as they are."
            : "Recreates the materialized lake views for the filtered tables, then deploys the semantic model metadata.";
    }

    const filterList = document.createElement("div");
    filterList.className = "slls-mmm-filterlist";
    filtersPage.appendChild(filterList);

    // ---------------- Deploy result ----------------
    const deployPanel = document.createElement("div");
    deployPanel.className = "slls-mmm-panel";
    root.appendChild(deployPanel);

    const updateProgress = document.createElement("div");
    updateProgress.className = "slls-mmm-updateprogress";
    updateProgress.setAttribute("role", "progressbar");
    updateProgress.setAttribute("aria-label", "Updating mini model from master");
    const updateProgressLabel = document.createElement("span");
    updateProgress.appendChild(updateProgressLabel);
    const updateProgressTrack = document.createElement("div");
    updateProgressTrack.className = "slls-mmm-verifytrack";
    updateProgress.appendChild(updateProgressTrack);
    root.appendChild(updateProgress);

    function renderUpdateProgress() {
        updateProgressLabel.textContent =
            model.get("update_progress") || "Preparing update from master...";
    }
    renderUpdateProgress();

    // ---------------- Footer ----------------
    const footer = document.createElement("div");
    footer.className = "slls-mmm-footer";
    root.appendChild(footer);

    const hint = document.createElement("div");
    hint.className = "slls-mmm-hint";
    footer.appendChild(hint);

    const backBtn = document.createElement("button");
    backBtn.className = "slls-mmm-btn";
    backBtn.type = "button";
    backBtn.innerHTML = `${PREV_SVG}<span>Back</span>`;
    footer.appendChild(backBtn);

    const nextBtn = document.createElement("button");
    nextBtn.className = "slls-mmm-btn slls-mmm-btn-primary";
    nextBtn.type = "button";
    nextBtn.innerHTML = `<span>Next</span>${NEXT_SVG}`;
    footer.appendChild(nextBtn);

    const deployBtn = document.createElement("button");
    deployBtn.className = "slls-mmm-btn slls-mmm-btn-primary";
    deployBtn.type = "button";
    footer.appendChild(deployBtn);

    backBtn.addEventListener("click", () => { step = "objects"; renderPages(); });
    nextBtn.addEventListener("click", () => { step = "filters"; renderPages(); });
    deployBtn.addEventListener("click", () => {
        if (!canDeploy()) return;
        originalSelection = deepClone(selection);
        send({
            action: "deploy",
            mode: getMode(),
            mini_name: nameInput.value.trim(),
            target_workspace_id: targetSelect.value || "",
            selection: buildSelectionPayload(),
            filters: hasFilterPage() ? activeFilters() : {},
            metadata_only: metadataOnly,
        });
    });

    // ---------------- Status + attribution ----------------
    const status = document.createElement("div");
    status.className = "slls-mmm-status";
    root.appendChild(status);

    const attribution = document.createElement("div");
    attribution.className = "slls-mmm-attribution";
    attribution.innerHTML =
        'Powered by <a href="https://github.com/microsoft/semantic-link-labs" target="_blank" rel="noopener noreferrer">Semantic Link Labs</a>';
    root.appendChild(attribution);

    // ================= Helpers =================
    function deepClone(o) { return JSON.parse(JSON.stringify(o)); }

    function emptySelection() { return { columns: {}, measures: {}, hierarchies: {} }; }

    function setCalculationGroupColumns(tbl, selected, target = selection) {
        const metadata = getMetadata()[tbl] || {};
        if (!metadata.calculation_group || !target[tbl]) return;
        for (const name of (metadata.columns || [])) target[tbl].columns[name] = selected;
    }

    function buildSelectionFromPreset() {
        const md = getMetadata();
        const preset = model.get("preset_selection") || {};
        const sel = {};
        for (const tbl of Object.keys(md)) {
            const chosen = preset[tbl] || {};
            sel[tbl] = emptySelection();
            for (const t of TYPES) {
                const set = new Set(chosen[t] || []);
                for (const n of (md[tbl][t] || [])) sel[tbl][t][n] = set.has(n);
            }
            if (md[tbl].calculation_group) {
                const groupSelected = TYPES.some((type) =>
                    Object.values(sel[tbl][type] || {}).some(Boolean));
                setCalculationGroupColumns(tbl, groupSelected, sel);
            }
        }
        return sel;
    }

    function isObjDirty(tbl, t, n) {
        if (getMode() !== "manage") return false;
        const cur = !!(selection[tbl] && selection[tbl][t] && selection[tbl][t][n]);
        const orig = !!(originalSelection[tbl] && originalSelection[tbl][t] && originalSelection[tbl][t][n]);
        return cur !== orig;
    }

    function isTableDirty(tbl) {
        if (getMode() !== "manage") return false;
        const md = getMetadata()[tbl] || {};
        for (const t of TYPES) {
            for (const n of (md[t] || [])) if (isObjDirty(tbl, t, n)) return true;
        }
        return false;
    }

    function tableState(tbl) {
        const sel = selection[tbl] || {};
        let total = 0, on = 0;
        for (const t of TYPES) {
            const obj = sel[t] || {};
            for (const k of Object.keys(obj)) { total++; if (obj[k]) on++; }
        }
        if (total === 0 || on === 0) return "none";
        return on === total ? "all" : "some";
    }

    function tableCounts(tbl) {
        const md = getMetadata()[tbl] || {};
        const sel = selection[tbl] || {};
        const c = {};
        for (const t of TYPES) {
            c[t] = [Object.values(sel[t] || {}).filter(Boolean).length, (md[t] || []).length];
        }
        return c;
    }

    function totalSelected() {
        let n = 0;
        for (const tbl of Object.keys(selection)) {
            for (const t of TYPES) n += Object.values(selection[tbl][t] || {}).filter(Boolean).length;
        }
        return n;
    }

    function selectedTables() {
        return Object.keys(selection).filter((t) => tableState(t) !== "none").sort();
    }

    /** Selected tables which are in Direct Lake mode on the master (only those take a filter). */
    function filterableTables() {
        const md = getMetadata();
        return selectedTables().filter((t) => md[t] && md[t].direct_lake);
    }

    /**
     * The filters page only exists when the whole model is made of objects it can
    * handle: Direct Lake tables (which take a filter), calculation groups, and
    * calculated tables (which hold no lake data), and at least one Direct Lake
    * table exists.
     */
    function modelIsFilterable() {
        const md = getMetadata();
        const names = Object.keys(md);
        if (names.length === 0) return false;
        const allOk = names.every((t) =>
            md[t].direct_lake || md[t].calculation_group || md[t].calculated_table);
        const anyDl = names.some((t) => md[t].direct_lake);
        return allOk && anyDl;
    }

    function hasFilterPage() {
        return modelIsFilterable() && filterableTables().length > 0;
    }

    function activeFilters() {
        const out = {};
        for (const t of filterableTables()) {
            const v = (filters[t] || "").trim();
            if (v) out[t] = v;
        }
        return out;
    }

    function buildSelectionPayload() {
        const md = getMetadata();
        const out = {};
        for (const tbl of Object.keys(selection)) {
            if (!md[tbl]) continue;
            const picked = { columns: [], measures: [], hierarchies: [] };
            let n = 0;
            for (const t of TYPES) {
                for (const name of Object.keys(selection[tbl][t] || {})) {
                    if (selection[tbl][t][name]) { picked[t].push(name); n++; }
                }
            }
            if (md[tbl].calculation_group && n > 0) {
                picked.columns = [...(md[tbl].columns || [])];
                n = picked.columns.length + picked.measures.length + picked.hierarchies.length;
            }
            if (n > 0) out[tbl] = picked;
        }
        return out;
    }

    function canDeploy() {
        if (isBusy()) return false;
        if (totalSelected() === 0) return false;
        if (getMode() === "create") {
            return nameInput.value.trim().length > 0 && Boolean(targetSelect.value);
        }
        return !model.get("mini_error") && Boolean(getMasterInfo().datasetName);
    }

    function setStatus(message, kind) {
        status.className = "slls-mmm-status";
        filtersStatus.className = "slls-mmm-status";
        if (!message) return;
        const target = step === "filters" ? filtersStatus : status;
        target.className = `slls-mmm-status show ${kind || "info"}`;
        target.textContent = message;
    }

    function setBusy(b) {
        root.classList.toggle("slls-mmm-busy", !!b);
        updateProgress.classList.toggle("show", updatingFromMaster && !!b);
    }

    function send(action) {
        if (action.action === "verify") {
            verifyingFilters = true;
            renderVerifyBar();
        }
        updatingFromMaster = action.action === "deploy" && getMode() === "manage";
        setBusy(true);
        model.set("pending_action", action);
        model.set("run", (model.get("run") || 0) + 1);
        model.save_changes();
    }

    // ================= Renderers =================
    function renderSegment() {
        const mode = getMode();
        createModeBtn.classList.toggle("active", mode === "create");
        manageModeBtn.classList.toggle("active", mode === "manage");
    }

    function renderConfig() {
        configPanel.classList.toggle("slls-mmm-hide", getMode() !== "create");
    }

    function renderConnectionState() {
        const picking = showPicker();
        renderPicker();
        changeModelBtn.classList.toggle("slls-mmm-hide", !isConnected() || picking);
        const managedElements = [segment, configPanel, masterPanel, objectsPage, filtersPage, deployPanel, footer];
        if (picking) {
            for (const element of managedElements) element.classList.add("slls-mmm-hide");
        } else {
            segment.classList.remove("slls-mmm-hide");
            footer.classList.remove("slls-mmm-hide");
        }
    }

    function renderTree() {
        tree.innerHTML = "";
        const md = getMetadata();
        const tblNames = Object.keys(md).sort((a, b) => a.localeCompare(b));
        if (tblNames.length === 0) {
            const empty = document.createElement("div");
            empty.className = "slls-mmm-empty";
            empty.textContent = model.get("mini_error")
                ? "Switch to 'Create new' to build a mini model from this model."
                : "No tables in this model.";
            tree.appendChild(empty);
            updateSummary();
            return;
        }
        const q = filterText.trim().toLowerCase();
        let anyVisible = false;

        for (const tblName of tblNames) {
            const data = md[tblName];
            const isHiddenTable = !!data.hidden_table;
            const block = document.createElement("div");
            block.className = "slls-mmm-table";
            if (expanded[tblName]) block.classList.add("expanded");

            const tblMatches = !q || tblName.toLowerCase().includes(q);
            let visibleChildren = 0;

            const row = document.createElement("div");
            row.className = "slls-mmm-row";
            if (isHiddenTable) row.classList.add("is-hidden");

            const caret = document.createElement("span");
            caret.className = "slls-mmm-caret";
            caret.innerHTML = CARET;
            row.appendChild(caret);

            const check = document.createElement("span");
            check.className = "slls-mmm-check";
            row.appendChild(check);

            const tblIcon = document.createElement("span");
            tblIcon.className = "slls-mmm-icon";
            tblIcon.innerHTML = data.calculation_group ? ICON_SVG.calc_group : ICON_SVG.table;
            row.appendChild(tblIcon);

            const name = document.createElement("span");
            name.className = "slls-mmm-name" + (isHiddenTable ? " hidden-obj" : "");
            name.textContent = tblName;
            row.appendChild(name);

            if (data.direct_lake) {
                const badge = document.createElement("span");
                badge.className = "slls-mmm-badge";
                badge.textContent = "Direct Lake";
                row.appendChild(badge);
            }

            const summary = document.createElement("span");
            summary.className = "slls-mmm-table-summary";
            row.appendChild(summary);

            const tblDirty = document.createElement("span");
            tblDirty.className = "slls-mmm-dirty";
            tblDirty.style.display = "none";
            tblDirty.title = "Changed since the last update from master";
            row.appendChild(tblDirty);

            block.appendChild(row);

            const childWrap = document.createElement("div");
            childWrap.className = "slls-mmm-children";
            block.appendChild(childWrap);

            for (const t of TYPES) {
                for (const n of (data[t] || [])) {
                    const objHidden = (data[`hidden_${t}`] || []).indexOf(n) >= 0
                        || (t !== "measures" && isHiddenTable);
                    const childRow = document.createElement("div");
                    childRow.className = "slls-mmm-row slls-mmm-child";
                    if (objHidden) childRow.classList.add("is-hidden");
                    const lockedCalculationGroupColumn = data.calculation_group && t === "columns";
                    if (lockedCalculationGroupColumn) {
                        childRow.classList.add("slls-mmm-disabled");
                        childRow.setAttribute("aria-disabled", "true");
                        childRow.title = "Calculation group columns are included automatically.";
                    }
                    childRow.dataset.type = t;
                    childRow.dataset.name = n;

                    const cb = document.createElement("span");
                    cb.className = "slls-mmm-check";
                    childRow.appendChild(cb);

                    const ic = document.createElement("span");
                    ic.className = "slls-mmm-icon";
                    ic.innerHTML = ICON_SVG[t];
                    childRow.appendChild(ic);

                    const lbl = document.createElement("span");
                    lbl.className = "slls-mmm-name" + (objHidden ? " hidden-obj" : "");
                    lbl.textContent = n;
                    childRow.appendChild(lbl);

                    const dot = document.createElement("span");
                    dot.className = "slls-mmm-dirty";
                    dot.style.display = "none";
                    dot.title = "Changed since the last update from master";
                    childRow.appendChild(dot);

                    const matches = tblMatches || n.toLowerCase().includes(q);
                    if (q && !matches) childRow.classList.add("filtered-out");
                    else visibleChildren++;

                    if (!lockedCalculationGroupColumn) {
                        childRow.addEventListener("click", (e) => {
                            e.stopPropagation();
                            if (!selection[tblName]) selection[tblName] = emptySelection();
                            selection[tblName][t][n] = !selection[tblName][t][n];
                            if (data.calculation_group) {
                                const groupSelected = ["measures", "hierarchies"].some((type) =>
                                    Object.values(selection[tblName][type] || {}).some(Boolean));
                                setCalculationGroupColumns(tblName, groupSelected);
                            }
                            updateRow();
                            renderFooter();
                        });
                    }

                    childWrap.appendChild(childRow);
                }
            }

            if (q && !tblMatches && visibleChildren === 0) {
                block.classList.add("hidden-match");
            } else {
                anyVisible = true;
                if (q && !tblMatches && visibleChildren > 0) block.classList.add("expanded");
            }

            function updateRow() {
                check.dataset.state = tableState(tblName);
                const c = tableCounts(tblName);
                summary.textContent =
                    ` ${c.columns[0]}/${c.columns[1]} cols · ${c.measures[0]}/${c.measures[1]} measures · ${c.hierarchies[0]}/${c.hierarchies[1]} hierarchies`;
                tblDirty.style.display = isTableDirty(tblName) ? "inline-block" : "none";
                for (const cr of childWrap.querySelectorAll(".slls-mmm-child")) {
                    const tt = cr.dataset.type;
                    const nn = cr.dataset.name;
                    cr.querySelector(".slls-mmm-check").dataset.state =
                        (selection[tblName] && selection[tblName][tt] && selection[tblName][tt][nn]) ? "all" : "none";
                    const dot = cr.querySelector(".slls-mmm-dirty");
                    if (dot) dot.style.display = isObjDirty(tblName, tt, nn) ? "inline-block" : "none";
                }
                updateSummary();
            }

            row.addEventListener("click", (e) => {
                if (e.target === check) return;
                block.classList.toggle("expanded");
                expanded[tblName] = block.classList.contains("expanded");
            });
            check.addEventListener("click", (e) => {
                e.stopPropagation();
                const turnOn = tableState(tblName) !== "all";
                if (!selection[tblName]) selection[tblName] = emptySelection();
                for (const t of TYPES) {
                    for (const n of (data[t] || [])) selection[tblName][t][n] = turnOn;
                }
                if (data.calculation_group) setCalculationGroupColumns(tblName, turnOn);
                updateRow();
                renderFooter();
            });

            updateRow();
            tree.appendChild(block);
        }

        if (!anyVisible) {
            const empty = document.createElement("div");
            empty.className = "slls-mmm-empty";
            empty.textContent = "No matches.";
            tree.appendChild(empty);
        }
        updateSummary();
    }

    function updateSummary() {
        const n = totalSelected();
        const t = selectedTables().length;
        summaryEl.textContent = n > 0
            ? `${n} object${n === 1 ? "" : "s"} in ${t} table${t === 1 ? "" : "s"}`
            : "Nothing selected";
    }

    function renderFilterList() {
        const tables = filterableTables();
        const verify = getVerify();
        filterList.innerHTML = "";
        if (tables.length === 0) {
            const empty = document.createElement("div");
            empty.className = "slls-mmm-empty";
            empty.textContent = "No Direct Lake tables are selected.";
            filterList.appendChild(empty);
            renderVerifySummary();
            return;
        }
        for (const table of tables) {
            const card = document.createElement("div");
            card.className = "slls-mmm-filtercard";

            const head = document.createElement("div");
            head.className = "slls-mmm-filterhead";
            head.innerHTML = `<span class="slls-mmm-icon">${ICON_SVG.table}</span>` +
                `<span class="slls-mmm-filtername">${escapeHtml(table)}</span>`;
            card.appendChild(head);

            const input = document.createElement("input");
            input.type = "text";
            input.className = "slls-mmm-input slls-mmm-mono";
            input.spellcheck = false;
            input.placeholder = "e.g. City = 'Seattle' AND SaleKey > 100";
            input.value = filters[table] || "";
            input.addEventListener("input", () => {
                const v = input.value;
                if (v) filters[table] = v;
                else delete filters[table];
                // A changed filter invalidates the table's previous verification.
                if (verifyResults[table]) {
                    delete verifyResults[table];
                    for (const node of card.querySelectorAll(".slls-mmm-sql, .slls-mmm-filterstate")) {
                        node.remove();
                    }
                }
                renderVerifyBar();
            });
            card.appendChild(input);

            const v = verify[table];
            if (v) {
                if (v.sql) {
                    const pre = document.createElement("pre");
                    pre.className = "slls-mmm-sql";
                    pre.textContent = v.sql;
                    card.appendChild(pre);
                }
                const state = document.createElement("div");
                state.className = "slls-mmm-filterstate";
                if (v.valid) {
                    state.classList.add("slls-mmm-ok");
                    state.innerHTML = `${CHECK_SVG}<span>Filter is valid.</span>`;
                } else if (v.error) {
                    state.classList.add("slls-mmm-bad");
                    state.innerHTML = `${ERROR_SVG}<span>${escapeHtml(v.error)}</span>`;
                } else if (v.note) {
                    state.classList.add("slls-mmm-warnc");
                    state.innerHTML = `${ALERT_SVG}<span>${escapeHtml(v.note)}</span>`;
                }
                if (state.innerHTML) card.appendChild(state);
            }

            filterList.appendChild(card);
        }
        renderVerifySummary();
    }

    function renderVerifySummary() {
        const verify = getVerify();
        const tables = filterableTables();
        let valid = 0, invalid = 0, generated = 0, reported = 0;
        for (const t of tables) {
            const v = verify[t];
            if (!v) continue;
            reported++;
            if (v.valid) valid++;
            else if (v.error) invalid++;
            else generated++;
        }
        if (reported === 0) { verifySummary.innerHTML = ""; return; }
        let html = "";
        if (valid > 0) html += `<span class="slls-mmm-ok">${CHECK_SVG}${valid} valid</span>`;
        if (invalid > 0) html += `<span class="slls-mmm-bad">${ERROR_SVG}${invalid} invalid</span>`;
        if (generated > 0) html += `<span class="slls-mmm-warnc">${ALERT_SVG}${generated} not validated</span>`;
        verifySummary.innerHTML = html;
    }

    function renderVerifyBar() {
        verifyBtn.disabled = isBusy() || Object.keys(activeFilters()).length === 0;
        verifyProgress.classList.toggle("show", verifyingFilters);
        renderVerifySummary();
    }

    function renderDeployPanel() {
        const d = getDeploy();
        if (!d.name) { deployPanel.classList.add("slls-mmm-hide"); return; }
        deployPanel.classList.remove("slls-mmm-hide");
        deployPanel.innerHTML =
            `<div class="slls-mmm-note success" style="margin-top:0">${CHECK_SVG}<div>` +
            `<b>Mini model deployed.</b> '${escapeHtml(d.name)}' is now available in ` +
            `${escapeHtml(d.workspaceName || "")}.` +
            (d.refreshRecommended
                ? "<div style=\"margin-top:6px\">The model was not refreshed. You may want to refresh it now.</div>"
                : "") +
            (d.url
                ? `<div style="margin-top:6px"><a class="slls-mmm-link" href="${escapeHtml(d.url)}" ` +
                  `target="_blank" rel="noopener noreferrer">View in Fabric${LINK_SVG}</a></div>`
                : "") +
            "</div></div>";
    }

    function renderPages() {
        const onFilters = step === "filters" && hasFilterPage();
        if (step === "filters" && !onFilters) step = "objects";
        objectsPage.classList.toggle("slls-mmm-hide", onFilters);
        filtersPage.classList.toggle("slls-mmm-hide", !onFilters);
        if (onFilters) { renderMetadataSwitch(); renderFilterList(); renderVerifyBar(); }
        const currentStatus = model.get("status") || {};
        setStatus(currentStatus.message || "", currentStatus.kind || "info");
        renderFooter();
    }

    function renderFooter() {
        const mode = getMode();
        const onFilters = step === "filters" && hasFilterPage();
        const showNext = !onFilters && hasFilterPage();

        backBtn.classList.toggle("slls-mmm-hide", !onFilters);
        nextBtn.classList.toggle("slls-mmm-hide", !showNext);
        deployBtn.classList.toggle("slls-mmm-hide", showNext);

        nextBtn.disabled = totalSelected() === 0 || isBusy();
        deployBtn.disabled = !canDeploy();
        backBtn.disabled = isBusy();
        createModeBtn.disabled = isBusy();
        manageModeBtn.disabled = isBusy();

        deployBtn.innerHTML = mode === "manage"
            ? `${SYNC_SVG}<span>Update from master</span>`
            : `${DEPLOY_SVG}<span>Deploy mini model</span>`;

        if (showNext) {
            hint.textContent =
                "Pick the objects to include, then continue to set optional Direct Lake table filters.";
        } else if (mode === "manage") {
            const nm = model.get("dataset_name") || "the mini model";
            hint.innerHTML =
                `Rebuilds <code>${escapeHtml(nm)}</code> from its master with the selection above; ` +
                "broken objects are dropped.";
        } else {
            const nm = nameInput.value.trim() || "Mini model";
            hint.innerHTML =
                `A perspective named <code>${escapeHtml(nm)}</code> is created on the master model; ` +
                "the master model's data is not changed.";
        }
    }

    function reloadFromModel() {
        selection = buildSelectionFromPreset();
        originalSelection = deepClone(selection);
        filters = Object.assign({}, model.get("saved_filters") || {});
        verifyResults = Object.assign({}, model.get("verify_results") || {});
        expanded = {};
        step = "objects";
        renderSegment();
        renderSubtitle();
        renderConfig();
        renderWorkspaces();
        renderMasterPanel();
        renderTree();
        renderDeployPanel();
        renderPages();
        renderConnectionState();
    }

    // ================= Model observers =================
    model.on("change:status", () => {
        const s = model.get("status") || {};
        verifyingFilters = false;
        updatingFromMaster = false;
        setBusy(false);
        setStatus(s.message || "", s.kind || "info");
        renderFooter();
        renderVerifyBar();
    });
    model.on("change:busy", () => {
        if (!isBusy()) verifyingFilters = false;
        if (!isBusy()) updatingFromMaster = false;
        setBusy(isBusy());
        renderFooter();
        renderPicker();
        renderVerifyBar();
    });
    model.on("change:metadata", reloadFromModel);
    model.on("change:mode", reloadFromModel);
    model.on("change:preset_selection", reloadFromModel);
    model.on("change:master_info", renderMasterPanel);
    model.on("change:broken_objects", renderMasterPanel);
    model.on("change:mini_error", () => { renderMasterPanel(); renderFooter(); });
    model.on("change:workspaces", renderWorkspaces);
    model.on("change:datasets", renderPicker);
    model.on("change:connected", reloadFromModel);
    model.on("change:connect_done", () => {
        pickerReopen = false;
        pickDs = "";
        reloadFromModel();
    });
    model.on("change:dataset_name", renderSubtitle);
    model.on("change:workspace_name", renderSubtitle);
    model.on("change:verify_results", () => {
        verifyResults = Object.assign({}, model.get("verify_results") || {});
        renderFilterList();
        renderVerifyBar();
    });
    model.on("change:deploy_result", renderDeployPanel);
    model.on("change:update_progress", renderUpdateProgress);

    reloadFromModel();
}
export default { render };
"""


# Inject SVG icons from the shared UI components module so they stay in
# sync with the other widgets (e.g. ``perspective_editor``).
from sempy_labs._ui_components import ICONS as _UI_ICONS  # noqa: E402

_WIDGET_JS = (
    _WIDGET_JS.replace("__SLLS_ICON_COLUMN__", _UI_ICONS["column"])
    .replace("__SLLS_ICON_MEASURE__", _UI_ICONS["measure"])
    .replace("__SLLS_ICON_HIERARCHY__", _UI_ICONS["hierarchy"])
    .replace("__SLLS_ICON_TABLE__", _UI_ICONS["table"])
    .replace("__SLLS_ICON_CALC_GROUP__", _UI_ICONS["calculation_group"])
    .replace("__SLLS_ICON_CARET__", _UI_ICONS["caret_right"])
    .replace("__SLLS_ICON_EXPAND_ROWS__", _UI_ICONS["expand_rows"])
    .replace("__SLLS_ICON_COLLAPSE_ROWS__", _UI_ICONS["collapse_rows"])
    .replace("__SLLS_ICON_SUN__", _UI_ICONS["sun"])
    .replace("__SLLS_ICON_MOON__", _UI_ICONS["moon"])
    .replace("__SLLS_ICON_FULLSCREEN__", _UI_ICONS["fullscreen"])
    .replace("__SLLS_ICON_FULLSCREEN_EXIT__", _UI_ICONS["fullscreen_exit"])
    .replace("__SLLS_ICON_CHECK_CIRCLE__", _UI_ICONS["check_circle"])
    .replace("__SLLS_ICON_ERROR_CIRCLE__", _UI_ICONS["error_circle"])
    .replace("__SLLS_ICON_ALERT__", _UI_ICONS["alert"])
    .replace("__SLLS_ICON_INFO__", _UI_ICONS["info"])
    .replace("__SLLS_ICON_EXTERNAL_LINK__", _UI_ICONS["external_link"])
    .replace("__SLLS_ICON_UPLOAD__", _UI_ICONS["upload"])
    .replace("__SLLS_ICON_SYNC__", _UI_ICONS["sync"])
    .replace("__SLLS_ICON_CHEVRON_LEFT__", _UI_ICONS["chevron_left"])
    .replace("__SLLS_ICON_CHEVRON_RIGHT__", _UI_ICONS["chevron_right"])
    .replace("__SLLS_ICON_MINI_MODEL_MANAGER__", _UI_ICONS["mini_model_manager"])
    .replace("__SLLS_ICON_SWAP__", _UI_ICONS["swap"])
    .replace("__SLLS_ICON_EXPAND_DIAGONAL__", _UI_ICONS["expand_diagonal"])
    .replace("__SLLS_ICON_COLLAPSE_DIAGONAL__", _UI_ICONS["collapse_diagonal"])
)

_OBJECT_TYPES = ["columns", "measures", "hierarchies"]


def _model_metadata(tom) -> dict:
    """
    Builds the table/column/measure/hierarchy metadata payload for a semantic model.
    """

    import Microsoft.AnalysisServices.Tabular as TOM

    columns_by_table = {}
    for column in tom.all_columns():
        columns_by_table.setdefault(column.Parent.Name, []).append(column)

    metadata = {}
    for table in tom.model.Tables:
        columns = columns_by_table.get(table.Name, [])
        metadata[table.Name] = {
            "columns": sorted(c.Name for c in columns),
            "measures": sorted(m.Name for m in table.Measures),
            "hierarchies": sorted(h.Name for h in table.Hierarchies),
            "hidden_table": bool(table.IsHidden),
            "hidden_columns": [c.Name for c in columns if c.IsHidden],
            "hidden_measures": [m.Name for m in table.Measures if m.IsHidden],
            "hidden_hierarchies": [h.Name for h in table.Hierarchies if h.IsHidden],
            "calculation_group": table.CalculationGroup is not None,
            "calculated_table": any(
                p.SourceType == TOM.PartitionSourceType.Calculated
                for p in table.Partitions
            ),
            "direct_lake": any(
                p.Mode == TOM.ModeType.DirectLake for p in table.Partitions
            ),
        }

    return metadata


@log
def mini_model_manager(
    dataset: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    dark_mode: bool = False,
):
    """
    Generates an interactive manager for creating and maintaining mini models.

    A mini model is a smaller semantic model derived from a master semantic model. The manager
    lets you pick the tables/columns/measures/hierarchies to include, optionally restrict the
    Direct Lake tables with a SQL WHERE expression (validating the generated SQL on Spark), and
    deploy the resulting mini model to a target workspace.

    If the specified semantic model is itself a mini model, the manager opens in 'Manage existing'
    mode which shows the mini model's master model, flags the objects which no longer exist in the
    master, and updates the mini model from its master. Otherwise, the manager opens in 'Create new'
    mode which creates a new mini model from the specified (master) semantic model.

    Requirements: Filters may only be verified/applied within a PySpark notebook and are only
    supported for single-sourced Direct Lake semantic models based on a lakehouse.

    Parameters
    ----------
    dataset : str | uuid.UUID, default=None
        Name or ID of the semantic model. This is either the master semantic model (from which a
        mini model is created) or an existing mini model (which is managed). Defaults to None,
        which opens a workspace / semantic model picker.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    dark_mode : bool, default=False
        If True, renders the manager with a dark color theme. If False, renders
        with a light color theme.
    """

    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "The 'mini_model_manager' function requires the 'anywidget' package. "
            "Install it with: pip install anywidget"
        ) from e

    import sempy.fabric as fabric
    from IPython.display import display
    from sempy_labs._helper_functions import (
        _pure_python_notebook,
        resolve_workspace_name_and_id,
        resolve_dataset_name_and_id,
        resolve_item_name_and_id,
    )
    from sempy_labs._generate_semantic_model import deploy_semantic_model
    from sempy_labs.tom import connect_semantic_model

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    workspace_id = str(workspace_id)
    workspace_name = str(workspace_name or "")
    connected = dataset is not None
    if connected:
        dataset_name, dataset_id = resolve_dataset_name_and_id(dataset, workspace_id)
        dataset_id = str(dataset_id)
        dataset_name = str(dataset_name)
    else:
        dataset_name, dataset_id = "", ""

    def _pick_columns(df, preferred_ids, preferred_names):
        columns = list(df.columns)
        if not columns:
            return None, None
        id_column = next((c for c in preferred_ids if c in columns), columns[0])
        name_column = next((c for c in preferred_names if c in columns), columns[-1])
        return id_column, name_column

    def _list_workspaces_payload():
        try:
            dfW = fabric.list_workspaces()
        except Exception:
            return [{"id": workspace_id, "name": workspace_name}]
        id_column, name_column = _pick_columns(
            dfW,
            ["Id", "ID", "Workspace Id", "Workspace ID"],
            ["Name", "Workspace Name"],
        )
        if id_column is None or name_column is None:
            return [{"id": workspace_id, "name": workspace_name}]
        rows = [
            {"id": str(row[id_column]), "name": str(row[name_column])}
            for _, row in dfW.iterrows()
        ]
        return sorted(rows, key=lambda item: item["name"].lower())

    def _list_datasets_payload(target_workspace_id):
        try:
            dfD = fabric.list_datasets(workspace=target_workspace_id)
        except Exception:
            return []
        id_column, name_column = _pick_columns(
            dfD,
            ["Dataset Id", "Dataset ID", "Id"],
            ["Dataset Name", "Name"],
        )
        if id_column is None or name_column is None:
            return []
        rows = [
            {"id": str(row[id_column]), "name": str(row[name_column])}
            for _, row in dfD.iterrows()
        ]
        return sorted(rows, key=lambda item: item["name"].lower())

    def _read_model(target_dataset_id, target_workspace_id):
        with connect_semantic_model(
            dataset=target_dataset_id,
            workspace=target_workspace_id,
            readonly=True,
        ) as tom:
            properties = tom.get_mini_model_properties() or {}
            metadata = _model_metadata(tom)
        return properties, metadata

    # ------------------------------------------------------------------
    # Initial load: read the model and determine whether it is a mini model
    # ------------------------------------------------------------------
    if connected:
        mini_properties, own_metadata = _read_model(dataset_id, workspace_id)
    else:
        mini_properties, own_metadata = {}, {}

    is_mini = bool(mini_properties)

    class MiniModelManagerWidget(anywidget.AnyWidget):
        _esm = _WIDGET_JS
        _css = _WIDGET_CSS

        mode = traitlets.Unicode("create").tag(sync=True)
        metadata = traitlets.Dict().tag(sync=True)
        master_info = traitlets.Dict().tag(sync=True)
        preset_selection = traitlets.Dict().tag(sync=True)
        broken_objects = traitlets.List().tag(sync=True)
        saved_filters = traitlets.Dict().tag(sync=True)
        workspaces = traitlets.List().tag(sync=True)
        datasets = traitlets.Dict().tag(sync=True)
        verify_results = traitlets.Dict().tag(sync=True)
        deploy_result = traitlets.Dict().tag(sync=True)
        update_progress = traitlets.Unicode("").tag(sync=True)
        mini_error = traitlets.Unicode("").tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        busy = traitlets.Bool(False).tag(sync=True)
        dataset_name = traitlets.Unicode("").tag(sync=True)
        workspace_name = traitlets.Unicode("").tag(sync=True)
        workspace_id = traitlets.Unicode("").tag(sync=True)
        connected = traitlets.Bool(False).tag(sync=True)
        connect_done = traitlets.Int(0).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)

    widget = MiniModelManagerWidget(
        dataset_name=dataset_name,
        workspace_name=workspace_name,
        workspace_id=workspace_id,
        workspaces=_list_workspaces_payload(),
        datasets=(
            {workspace_id: _list_datasets_payload(workspace_id)}
            if not connected
            else {}
        ),
        connected=connected,
        connect_done=0,
        dark_mode=bool(dark_mode),
    )

    # Each status message carries an incrementing id so the frontend always
    # observes a change (and therefore always clears its busy state).
    status_counter = {"n": 0}

    def _set_status(message: str, kind: str = "info"):
        status_counter["n"] += 1
        widget.status = {"message": message, "kind": kind, "id": status_counter["n"]}

    def _set_update_progress(message: str):
        if widget.mode == "manage":
            widget.update_progress = message

    def _master_reference() -> dict:
        """The master model of the current mode (the model itself in 'create' mode)."""

        if widget.mode == "create":
            return {
                "datasetId": dataset_id,
                "datasetName": dataset_name,
                "workspaceId": workspace_id,
                "workspaceName": workspace_name,
            }
        return dict(widget.master_info or {})

    def _perspective_name(mini_name: Optional[str] = None) -> str:
        if widget.mode == "manage":
            return str(mini_properties.get("miniModelPerspective") or dataset_name)
        return str(mini_name or "")

    def _warn_if_filters_require_pyspark(filters: dict) -> bool:
        if filters and _pure_python_notebook():
            _set_status(
                "Filter validation and creation require a PySpark notebook. "
                "Run the Mini Model Manager in a PySpark notebook to use filters.",
                "warning",
            )
            return True
        return False

    def _load_create_mode():
        widget.mini_error = ""
        widget.master_info = {}
        widget.broken_objects = []
        widget.saved_filters = {}
        widget.preset_selection = {}
        widget.verify_results = {}
        widget.deploy_result = {}
        widget.metadata = own_metadata
        widget.mode = "create"

    def _load_manage_mode():
        widget.verify_results = {}
        widget.deploy_result = {}
        widget.mode = "manage"

        if not is_mini:
            widget.mini_error = (
                f"'{dataset_name}' has no master-reference annotation, so it cannot be managed here. "
                "Switch to 'Create new' to create a mini model from it."
            )
            widget.master_info = {}
            widget.broken_objects = []
            widget.preset_selection = {}
            widget.saved_filters = {}
            widget.metadata = {}
            return

        widget.mini_error = ""
        master_workspace_id = str(
            mini_properties.get("masterSemanticModelWorkspaceId") or ""
        )
        master_dataset_id = str(mini_properties.get("masterSemanticModelId") or "")

        with connect_semantic_model(
            dataset=master_dataset_id, workspace=master_workspace_id, readonly=True
        ) as tom:
            master_metadata = _model_metadata(tom)

        # Pre-check the objects the mini model currently contains which still exist in
        # the master; the objects which no longer exist are surfaced as broken objects.
        preset = {}
        broken = []
        for table_name, mini_meta in own_metadata.items():
            master_meta = master_metadata.get(table_name)
            if master_meta is None:
                broken.append({"table": table_name})
                continue
            picked = {}
            for object_type in _OBJECT_TYPES:
                available = set(master_meta.get(object_type) or [])
                objects = mini_meta.get(object_type) or []
                picked[object_type] = [o for o in objects if o in available]
                broken.extend(
                    {"table": table_name, "type": object_type, "name": o}
                    for o in objects
                    if o not in available
                )
            if any(picked.values()):
                preset[table_name] = picked

        widget.master_info = {
            "datasetId": master_dataset_id,
            "datasetName": str(mini_properties.get("masterSemanticModelName") or ""),
            "workspaceId": master_workspace_id,
            "workspaceName": str(
                mini_properties.get("masterSemanticModelWorkspaceName") or ""
            ),
            "lastUpdatedDate": str(
                mini_properties.get("miniModelLastUpdatedDate") or ""
            ),
        }
        widget.broken_objects = broken
        widget.saved_filters = {
            k: v
            for k, v in (mini_properties.get("miniModelFilters") or {}).items()
            if isinstance(v, str)
        }
        widget.preset_selection = preset
        widget.metadata = master_metadata

    def _verify(filters: dict, mini_name: Optional[str] = None):
        if _warn_if_filters_require_pyspark(filters):
            return

        from sempy_labs.lakehouse._materialized_lake_views import (
            create_materialized_lake_view,
        )

        master = _master_reference()
        schema = _perspective_name(mini_name) or "mini_model"

        with connect_semantic_model(
            dataset=master.get("datasetId"),
            workspace=master.get("workspaceId"),
            readonly=True,
        ) as tom:
            queries, lakehouse_id, lakehouse_workspace_id = tom._generate_mlv_queries(
                filters=filters, schema=schema
            )

        if not queries:
            _set_status(
                "The filters could not be generated. Filters are only supported for single-sourced "
                "Direct Lake semantic models based on a lakehouse.",
                "error",
            )
            return

        results = {}
        for table_name, items in queries.items():
            sql = items.get("sql")
            try:
                is_valid = create_materialized_lake_view(
                    name=f"{schema}.{items.get('entityName')}",
                    query=sql,
                    lakehouse=lakehouse_id,
                    workspace=lakehouse_workspace_id,
                    replace=True,
                    test_run=True,
                )
            except Exception as e:
                results[table_name] = {"sql": sql, "valid": False, "note": str(e)}
                continue
            results[table_name] = {
                "sql": sql,
                "valid": bool(is_valid),
                "error": None if is_valid else "The filter is not valid.",
            }

        widget.verify_results = {}
        widget.verify_results = results
        invalid = [t for t, r in results.items() if not r.get("valid")]
        if invalid:
            _set_status(
                f"Verified {len(results)} table(s). {len(invalid)} could not be validated.",
                "error",
            )
        else:
            _set_status(f"Verified {len(results)} table(s). All filters are valid.")

    def _save_perspective(master: dict, perspective_name: str, selection: dict):
        """(Re)creates the perspective on the master model based on the selection."""

        metadata = widget.metadata or {}

        with connect_semantic_model(
            dataset=master.get("datasetId"),
            workspace=master.get("workspaceId"),
            readonly=False,
        ) as tom:
            if not tom.model.Perspectives.Find(perspective_name):
                tom.add_perspective(perspective_name)
            perspective = tom.model.Perspectives[perspective_name]
            perspective.PerspectiveTables.Clear()

            for table_name, objects in selection.items():
                table = tom.model.Tables[table_name]
                table_metadata = metadata.get(table_name, {})
                whole_table = all(
                    len(objects.get(object_type) or [])
                    >= len(table_metadata.get(object_type) or [])
                    for object_type in _OBJECT_TYPES
                )
                if whole_table:
                    tom.add_to_perspective(
                        object=table, perspective_name=perspective_name
                    )
                    continue
                for name in objects.get("columns") or []:
                    tom.add_to_perspective(
                        object=table.Columns[name], perspective_name=perspective_name
                    )
                for name in objects.get("measures") or []:
                    tom.add_to_perspective(
                        object=table.Measures[name], perspective_name=perspective_name
                    )
                for name in objects.get("hierarchies") or []:
                    tom.add_to_perspective(
                        object=table.Hierarchies[name],
                        perspective_name=perspective_name,
                    )

            tom.model.SaveChanges()

    def _deploy(data: dict):
        selection = data.get("selection") or {}
        if not selection:
            _set_status("Select at least one object to include.", "error")
            return

        filters = {
            k: str(v).strip()
            for k, v in (data.get("filters") or {}).items()
            if str(v).strip()
        }
        if _warn_if_filters_require_pyspark(filters):
            return

        master = _master_reference()

        if widget.mode == "manage":
            if not master.get("datasetId"):
                _set_status(
                    "The master model of this mini model could not be resolved.",
                    "error",
                )
                return
            mini_name = dataset_name
            target_workspace_id = workspace_id
            target_workspace_name = workspace_name
            overwrite = True
        else:
            mini_name = str(data.get("mini_name") or "").strip()
            if not mini_name:
                _set_status("Enter a name for the mini model.", "error")
                return
            target_workspace_id = str(data.get("target_workspace_id") or workspace_id)
            target_workspace_name = next(
                (
                    w.get("name")
                    for w in (widget.workspaces or [])
                    if w.get("id") == target_workspace_id
                ),
                "",
            )
            overwrite = False

        perspective_name = _perspective_name(mini_name)
        _set_update_progress("Updating the model perspective on the master...")
        _save_perspective(master, perspective_name, selection)

        if data.get("metadata_only"):
            _set_update_progress("Deploying semantic model metadata...")
        elif filters:
            _set_update_progress(
                "Creating materialized lake views and deploying the semantic model..."
            )
        else:
            _set_update_progress("Deploying the semantic model...")
        deploy_semantic_model(
            source_dataset=master.get("datasetId"),
            source_workspace=master.get("workspaceId"),
            target_dataset=mini_name,
            target_workspace=target_workspace_id,
            refresh_target_dataset=widget.mode != "manage",
            overwrite=overwrite,
            perspective=perspective_name,
            filters=filters or None,
            metadata_only=bool(data.get("metadata_only")),
        )

        _set_update_progress("Finalizing the deployment...")
        _, mini_id = resolve_item_name_and_id(
            item=mini_name, type="SemanticModel", workspace=target_workspace_id
        )
        widget.deploy_result = {
            "name": mini_name,
            "workspaceName": target_workspace_name,
            "url": f"https://app.powerbi.com/onelake/details/{target_workspace_id}/dataset/{mini_id}/overview",
            "refreshRecommended": widget.mode == "manage",
        }
        status_message = (
            f"The '{mini_name}' mini model has been deployed to the "
            f"'{target_workspace_name}' workspace."
        )
        if widget.mode == "manage":
            status_message += " The model was not refreshed; you may want to refresh it now."
        _set_status(status_message, "success")

    def _on_run(_change):
        nonlocal workspace_id, dataset_id, workspace_name, dataset_name
        nonlocal mini_properties, own_metadata, is_mini
        data = dict(widget.pending_action or {})
        action = data.get("action")
        if not action:
            return
        widget.busy = True
        try:
            if action == "list_datasets":
                target_workspace_id = data.get("workspace_id")
                if target_workspace_id:
                    datasets = dict(widget.datasets)
                    datasets[str(target_workspace_id)] = _list_datasets_payload(
                        target_workspace_id
                    )
                    widget.datasets = datasets
            elif action == "connect":
                target_workspace_id = str(data.get("workspace_id") or "")
                target_dataset_id = str(data.get("dataset_id") or "")
                if not target_workspace_id or not target_dataset_id:
                    _set_status("Select a workspace and semantic model.", "error")
                    return
                new_properties, new_metadata = _read_model(
                    target_dataset_id, target_workspace_id
                )
                workspace_id = target_workspace_id
                dataset_id = target_dataset_id
                workspace_name = str(data.get("workspace_name") or "")
                dataset_name = str(data.get("dataset_name") or "")
                mini_properties = new_properties
                own_metadata = new_metadata
                is_mini = bool(mini_properties)
                widget.workspace_id = workspace_id
                widget.workspace_name = workspace_name
                widget.dataset_name = dataset_name
                if is_mini:
                    _load_manage_mode()
                else:
                    _load_create_mode()
                widget.connected = True
                widget.connect_done = widget.connect_done + 1
                _set_status("")
            elif action == "set_mode":
                if data.get("mode") == "manage":
                    _load_manage_mode()
                else:
                    _load_create_mode()
                _set_status("")
            elif action == "verify":
                _verify(
                    data.get("filters") or {},
                    data.get("mini_name"),
                )
            elif action == "deploy":
                _deploy(data)
        except Exception as e:
            _set_status(f"Error: {e}", "error")
        finally:
            widget.busy = False

    widget.observe(_on_run, names=["run"])

    if connected:
        if is_mini:
            _load_manage_mode()
        else:
            _load_create_mode()

    # Keep a reference on the widget so the Python-side observer is not garbage
    # collected after this function returns. We intentionally do NOT return the
    # widget to avoid Jupyter auto-displaying it a second time after `display()`.
    display(widget)
