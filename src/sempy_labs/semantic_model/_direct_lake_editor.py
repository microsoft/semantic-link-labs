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
@media (prefers-color-scheme: dark) {
    .slls-dle.slls-dle-auto {
        --slls-bg-solid: #1c1c1e;
        --slls-surface: rgba(255, 255, 255, 0.04);
        --slls-surface-2: rgba(255, 255, 255, 0.03);
        --slls-border: rgba(255, 255, 255, 0.08);
        --slls-border-strong: rgba(255, 255, 255, 0.16);
        --slls-text: #f5f5f7;
        --slls-text-secondary: #a1a1a6;
        --slls-text-tertiary: #6e6e73;
        --slls-accent-soft: rgba(10, 132, 255, 0.18);
        --slls-accent: #0A84FF;
        --slls-shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.5);
    }
}
.slls-dle.slls-dle-dark {
    --slls-bg-solid: #1c1c1e;
    --slls-surface: rgba(255, 255, 255, 0.04);
    --slls-surface-2: rgba(255, 255, 255, 0.03);
    --slls-border: rgba(255, 255, 255, 0.08);
    --slls-border-strong: rgba(255, 255, 255, 0.16);
    --slls-text: #f5f5f7;
    --slls-text-secondary: #a1a1a6;
    --slls-text-tertiary: #6e6e73;
    --slls-accent-soft: rgba(10, 132, 255, 0.18);
    --slls-accent: #0A84FF;
    --slls-shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.5);
}
.slls-dle * { box-sizing: border-box; }

.slls-dle-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 18px;
    flex-wrap: wrap;
}
.slls-dle-titlewrap {
    display: flex;
    flex-direction: column;
    margin-right: auto;
    min-width: 0;
}
.slls-dle-title {
    font-size: 22px;
    font-weight: 600;
    letter-spacing: -0.01em;
    line-height: 1.15;
}
.slls-dle-subtitle {
    font-size: 12px;
    color: var(--slls-text-secondary);
    margin-top: 2px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 600px;
}
.slls-dle-subtitle .slls-dle-sep {
    color: var(--slls-text-tertiary);
    margin: 0 6px;
}
.slls-dle-subtitle b {
    color: var(--slls-text);
    font-weight: 500;
}

.slls-dle-select, .slls-dle-input {
    appearance: none;
    -webkit-appearance: none;
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
.slls-dle-select:focus, .slls-dle-input:focus {
    outline: none;
    border-color: var(--slls-accent);
    box-shadow: 0 0 0 3px var(--slls-accent-soft);
}
.slls-dle-select option {
    background-color: var(--slls-bg-solid);
    color: var(--slls-text);
}
.slls-dle-input::placeholder { color: var(--slls-text-tertiary); }

.slls-dle-btn {
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
    transition: background 120ms ease, border-color 120ms ease,
        transform 80ms ease, box-shadow 120ms ease, opacity 120ms ease;
}
.slls-dle-btn:hover { background: var(--slls-surface-2); border-color: var(--slls-text-tertiary); }
.slls-dle-btn:active { transform: scale(0.97); }
.slls-dle-btn:disabled { opacity: 0.4; cursor: not-allowed; }

.slls-dle-btn-primary {
    background: var(--slls-accent);
    border-color: var(--slls-accent);
    color: #fff;
}
.slls-dle-btn-primary:hover { background: var(--slls-accent-hover); border-color: var(--slls-accent-hover); }

.slls-dle-btn-danger {
    background: transparent;
    border-color: var(--slls-danger);
    color: var(--slls-danger);
}
.slls-dle-btn-danger:hover { background: var(--slls-danger-soft); }

.slls-dle-btn-icon {
    width: 32px; height: 32px;
    padding: 0;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border-radius: 50%;
    font-size: 18px;
    line-height: 1;
}

.slls-dle-toolbar {
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
}

.slls-dle-section {
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    background: var(--slls-surface);
    padding: 16px;
    margin-top: 14px;
}
.slls-dle-section h3 {
    margin: 0 0 12px 0;
    font-size: 14px;
    font-weight: 600;
    color: var(--slls-text);
    display: flex;
    align-items: center;
    gap: 8px;
}
.slls-dle-section h3 .slls-dle-count {
    color: var(--slls-text-tertiary);
    font-weight: 400;
    font-size: 12.5px;
}

.slls-dle-grid {
    display: grid;
    /* minmax(0, 1fr) lets columns shrink so long option labels in
       child <select> elements don't blow out the modal width. */
    grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
    gap: 12px;
}
.slls-dle-grid > .slls-dle-field {
    min-width: 0;
}
.slls-dle-grid > .slls-dle-field-wide {
    grid-column: 1 / -1;
}
.slls-dle-grid .slls-dle-select,
.slls-dle-grid .slls-dle-input {
    width: 100%;
    max-width: 100%;
    min-width: 0;
    text-overflow: ellipsis;
    overflow: hidden;
    white-space: nowrap;
}
.slls-dle-field {
    display: flex;
    flex-direction: column;
    gap: 4px;
}
.slls-dle-field label {
    font-size: 12px;
    color: var(--slls-text-secondary);
    padding-left: 10px;
}
.slls-dle-checkbox {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    font-size: 13.5px;
    color: var(--slls-text);
    cursor: pointer;
    user-select: none;
}

.slls-dle-list {
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius-sm);
    background: var(--slls-bg-solid);
    overflow: hidden;
    max-height: 360px;
    overflow-y: auto;
}
.slls-dle-list::-webkit-scrollbar { width: 10px; height: 10px; }
.slls-dle-list::-webkit-scrollbar-thumb {
    background: var(--slls-border-strong);
    border-radius: 999px;
    background-clip: padding-box;
    border: 2px solid transparent;
}
.slls-dle-list::-webkit-scrollbar-thumb:hover { background-color: var(--slls-text-tertiary); }
.slls-dle-item {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 14px;
    border-bottom: 1px solid var(--slls-border);
}
.slls-dle-item:last-child { border-bottom: none; }
.slls-dle-item-main {
    display: flex;
    flex-direction: column;
    gap: 2px;
    min-width: 0;
    flex: 1;
}
.slls-dle-item-name {
    font-size: 14px;
    font-weight: 500;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
.slls-dle-item-meta {
    font-size: 12px;
    color: var(--slls-text-tertiary);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
.slls-dle-item-actions { display: inline-flex; gap: 6px; flex-shrink: 0; }
.slls-dle-pill {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 999px;
    background: var(--slls-accent-soft);
    color: var(--slls-accent);
    font-size: 11.5px;
    font-weight: 500;
}

.slls-dle-empty {
    padding: 24px 16px;
    text-align: center;
    color: var(--slls-text-tertiary);
    font-size: 13.5px;
}

.slls-dle-status {
    margin-top: 14px;
    margin-bottom: 6px;
    padding: 10px 14px;
    border-radius: var(--slls-radius-sm);
    font-size: 13.5px;
    display: none;
}
.slls-dle-status.show { display: block; animation: slls-dle-fade-in 200ms ease; }
.slls-dle-status.success { background: var(--slls-success-soft); color: var(--slls-success); }
.slls-dle-status.error { background: var(--slls-danger-soft); color: var(--slls-danger); }
.slls-dle-status.info { background: var(--slls-accent-soft); color: var(--slls-accent); }

/* Orange "pending change" indicator for items modified since the last save. */
.slls-dle-pending-dot {
    width: 9px;
    height: 9px;
    border-radius: 50%;
    background: var(--slls-orange);
    display: inline-block;
    flex-shrink: 0;
    box-shadow: 0 0 0 2px rgba(255, 149, 0, 0.18);
}
.slls-dle-savebar {
    display: none;
    align-items: center;
    gap: 10px;
    padding: 10px 14px;
    margin-top: 10px;
    border-radius: var(--slls-radius-sm);
    background: rgba(255, 149, 0, 0.10);
    border: 1px solid rgba(255, 149, 0, 0.45);
    color: var(--slls-text);
}
.slls-dle-savebar.show { display: flex; }
.slls-dle-savebar-label {
    font-size: 13.5px;
    margin-right: auto;
    display: flex;
    align-items: center;
    gap: 8px;
}
.slls-dle-item-name .slls-dle-pending-dot { margin-right: 4px; }
.slls-dle-item.pending {
    background: rgba(255, 149, 0, 0.06);
}

/* Buffer above the manage toolbar so the Refresh button does not crowd
   any preceding cell output / status banner. */
.slls-dle-manage-top {
    margin-top: 8px;
    padding-top: 6px;
    border-top: 1px solid var(--slls-border);
}
@keyframes slls-dle-fade-in {
    from { opacity: 0; transform: translateY(-4px); }
    to { opacity: 1; transform: translateY(0); }
}

.slls-dle-busy {
    pointer-events: none;
    opacity: 0.55;
    transition: opacity 120ms ease;
}

.slls-dle-overlay {
    display: none;
    position: absolute;
    inset: 0;
    background: rgba(0, 0, 0, 0.45);
    z-index: 50;
    align-items: flex-start;
    justify-content: center;
    padding: 24px 16px;
    border-radius: var(--slls-radius);
    overflow-y: auto;
}
.slls-dle-overlay.show { display: flex; }
.slls-dle-modal {
    background: var(--slls-bg-solid);
    color: var(--slls-text);
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    box-shadow: 0 30px 80px rgba(0,0,0,0.35);
    width: 100%;
    max-width: 560px;
    padding: 22px;
    margin: auto;
}
.slls-dle-modal h2 {
    margin: 0 0 14px 0;
    font-size: 17px;
    font-weight: 600;
}
.slls-dle-modal-footer {
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    margin-top: 18px;
}
.slls-dle-modal-wide {
    max-width: 820px;
}
.slls-dle-columns-list {
    display: flex;
    flex-direction: column;
    gap: 8px;
    max-height: 60vh;
    overflow-y: auto;
    padding-right: 4px;
    margin-top: 4px;
}
.slls-dle-column-row {
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius-sm);
    background: var(--slls-surface-2);
    padding: 10px 12px;
    transition: border-color 120ms ease, background 120ms ease;
}
.slls-dle-column-row.pending {
    border-color: var(--slls-orange);
    background: rgba(255, 149, 0, 0.06);
}
.slls-dle-column-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 8px;
    margin-bottom: 6px;
}
.slls-dle-column-name {
    font-weight: 600;
    font-size: 13.5px;
    color: var(--slls-text);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
.slls-dle-column-type {
    font-size: 11.5px;
    color: var(--slls-text-tertiary);
    text-transform: uppercase;
    letter-spacing: 0.4px;
}
.slls-dle-column-fields {
    display: grid;
    grid-template-columns: minmax(0, 1.4fr) minmax(0, 1fr) minmax(0, 1fr);
    gap: 10px;
}
.slls-dle-column-fields > .slls-dle-field { min-width: 0; }
.slls-dle-column-fields .slls-dle-select,
.slls-dle-column-fields .slls-dle-input {
    width: 100%;
    max-width: 100%;
    min-width: 0;
    text-overflow: ellipsis;
    overflow: hidden;
    white-space: nowrap;
}

.slls-dle-tablerows {
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius-sm);
    max-height: 240px;
    overflow-y: auto;
    margin-top: 6px;
}
.slls-dle-tablerow {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 8px 12px;
    border-bottom: 1px solid var(--slls-border);
    font-size: 13px;
}
.slls-dle-tablerow:last-child { border-bottom: none; }
.slls-dle-tablerow label { flex: 1; cursor: pointer; }

.slls-dle-attribution {
    margin-top: 18px;
    text-align: right;
    font-size: 11.5px;
    color: var(--slls-text-tertiary);
}
.slls-dle-attribution a {
    color: var(--slls-text-tertiary);
    text-decoration: none;
    transition: color 120ms ease;
}
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

    const PLUS_SVG = `<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" aria-hidden="true"><path d="M8 3.25v9.5M3.25 8h9.5"/></svg>`;
    const BACK_SVG = `<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M10 3L5 8l5 5"/></svg>`;
    // Apple-style circular arrow refresh icon (SF Symbols `arrow.clockwise`).
    const REFRESH_SVG = `<svg width="15" height="15" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true"><path d="M8 2a6 6 0 0 1 5.196 3H11.5a.5.5 0 0 0 0 1h2.9A.6.6 0 0 0 15 5.4V2.5a.5.5 0 0 0-1 0v1.55A7 7 0 1 0 15 8a.5.5 0 0 0-1 0A6 6 0 1 1 8 2z"/></svg>`;
    const SUN_SVG = `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="8" cy="8" r="3"/><path d="M8 1.5v1.5M8 13v1.5M1.5 8h1.5M13 8h1.5M3.3 3.3l1.05 1.05M11.65 11.65l1.05 1.05M3.3 12.7l1.05-1.05M11.65 4.35l1.05-1.05"/></svg>`;
    const MOON_SVG = `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M13.5 9.5A5.5 5.5 0 0 1 6.5 2.5a5.5 5.5 0 1 0 7 7z"/></svg>`;

    // ----------- Header -----------
    const header = document.createElement("div");
    header.className = "slls-dle-header";
    root.appendChild(header);

    const titleWrap = document.createElement("div");
    titleWrap.className = "slls-dle-titlewrap";
    header.appendChild(titleWrap);

    const title = document.createElement("div");
    title.className = "slls-dle-title";
    title.textContent = "Direct Lake Editor";
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

    const newTablesInput = document.createElement("input");
    newTablesInput.type = "text";
    newTablesInput.className = "slls-dle-input";
    newTablesInput.placeholder = "dbo.sales, dbo.geography";
    createGrid.appendChild(makeField("Tables (comma-separated, schema.table)", newTablesInput));

    const toggleRow = document.createElement("div");
    toggleRow.style.marginTop = "10px";
    toggleRow.style.display = "flex";
    toggleRow.style.gap = "18px";
    toggleRow.style.flexWrap = "wrap";
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
    createFooter.style.display = "flex";
    createFooter.style.gap = "8px";
    createFooter.style.marginTop = "14px";
    createFooter.style.justifyContent = "flex-end";
    createSection.appendChild(createFooter);

    const cancelCreateBtn = document.createElement("button");
    cancelCreateBtn.className = "slls-dle-btn";
    cancelCreateBtn.textContent = "Cancel";
    cancelCreateBtn.addEventListener("click", () => {
        createSection.style.display = "none";
        selectSection.style.display = "";
    });
    createFooter.appendChild(cancelCreateBtn);

    const submitCreateBtn = document.createElement("button");
    submitCreateBtn.className = "slls-dle-btn slls-dle-btn-primary";
    submitCreateBtn.textContent = "Create";
    submitCreateBtn.addEventListener("click", () => {
        const name = (newNameInput.value || "").trim();
        const tables = (newTablesInput.value || "")
            .split(",").map(s => s.trim()).filter(Boolean);
        if (!name) { setStatus("Please provide a model name.", "error"); return; }
        if (tables.length === 0) { setStatus("Please specify at least one table.", "error"); return; }
        if (!newSrcItemSelect.value) { setStatus("Please select a source item.", "error"); return; }
        runAction("create_model", {
            dataset_name: name,
            workspace_id: newWsSelect.value,
            source_type: newSrcTypeSelect.value,
            source_workspace_id: newSrcWsSelect.value,
            source_id: newSrcItemSelect.value,
            tables: tables,
            use_sql_endpoint: useSqlToggle._input.checked,
            refresh: refreshToggle._input.checked,
        });
    });
    createFooter.appendChild(submitCreateBtn);

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
    newSrcTypeSelect.addEventListener("change", requestSourceItemsIfNeeded);
    newSrcWsSelect.addEventListener("change", requestSourceItemsIfNeeded);
    model.on("change:source_items", refreshSourceItems);

    newModelBtn.addEventListener("click", () => {
        selectSection.style.display = "none";
        createSection.style.display = "";
        syncWorkspacesIntoCreate();
        setStatus("", null);
        requestSourceItemsIfNeeded();
    });

    // ============================================================
    // MANAGE SCREEN
    // ============================================================
    const manageScreen = document.createElement("div");
    manageScreen.className = "slls-dle-screen";
    root.appendChild(manageScreen);

    const manageToolbar = document.createElement("div");
    manageToolbar.className = "slls-dle-toolbar slls-dle-manage-top";
    manageToolbar.style.marginBottom = "8px";
    manageScreen.appendChild(manageToolbar);

    const refreshModelBtn = document.createElement("button");
    refreshModelBtn.className = "slls-dle-btn";
    refreshModelBtn.innerHTML = `${REFRESH_SVG} <span style="margin-left:6px;">Refresh model</span>`;
    refreshModelBtn.title = "Refresh the semantic model";
    refreshModelBtn.addEventListener("click", () => {
        runAction("refresh_model", {});
    });
    manageToolbar.appendChild(refreshModelBtn);

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
                 (c.kind === "edit_columns" && c.key === tableName)
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
    // single { columnName -> { source_column?, data_type?, data_category? } }
    // map. Returns {} when there are no staged column edits.
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
                    if ("data_category" in col) cur.data_category = col.data_category;
                    merged[col.name] = cur;
                }
            }
        }
        return merged;
    }
    function pendingAddedSources() {
        return pendingState.changes.filter(c => c.kind === "add_source");
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
    const tablesHeading = document.createElement("h3");
    tablesSection.appendChild(tablesHeading);
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
        const added = pendingAddedSources();
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
            nm.className = "slls-dle-item-name";
            const dotHtml = dirty
                ? `<span class="slls-dle-pending-dot" aria-label="Unsaved changes" title="Unsaved changes"></span>`
                : "";
            nm.innerHTML = `${dotHtml}${escapeHtml(effItemName || "(unknown)")} <span class="slls-dle-pill">${escapeHtml(effItemType || "")}</span>`;
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
        for (const c of added) {
            const p = c.payload || {};
            const row = document.createElement("div");
            row.className = "slls-dle-item pending";
            const main = document.createElement("div");
            main.className = "slls-dle-item-main";
            const nm = document.createElement("div");
            nm.className = "slls-dle-item-name";
            nm.innerHTML =
                `<span class="slls-dle-pending-dot" aria-label="Unsaved" title="Unsaved"></span>` +
                `${escapeHtml(p.source_name || "(new source)")} <span class="slls-dle-pill">${escapeHtml(p.source_type || "")}</span>`;
            main.appendChild(nm);
            const meta = document.createElement("div");
            meta.className = "slls-dle-item-meta";
            meta.textContent = `Pending — will be added on save`;
            main.appendChild(meta);
            row.appendChild(main);
            const actions = document.createElement("div");
            actions.className = "slls-dle-item-actions";
            const revertBtn = document.createElement("button");
            revertBtn.className = "slls-dle-btn slls-dle-btn-danger";
            revertBtn.textContent = "Revert";
            revertBtn.title = "Remove this staged source";
            revertBtn.addEventListener("click", () => {
                revertChangesMatching(
                    ch => ch.id === c.id,
                    `Reverted staged source '${p.source_name || ""}'.`,
                );
            });
            actions.appendChild(revertBtn);
            row.appendChild(actions);
            sourcesList.appendChild(row);
        }
    }

    function renderTables() {
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
            const effExpression = staged.expression_name || t.expressionName || "";
            const effSchema = staged.schema != null
                ? (staged.schema || "")
                : (t.schemaName || "");
            const effEntity = staged.entity_name || t.entityName || "";
            const main = document.createElement("div");
            main.className = "slls-dle-item-main";
            const nm = document.createElement("div");
            nm.className = "slls-dle-item-name";
            const dotHtml = dirty
                ? `<span class="slls-dle-pending-dot" aria-label="Unsaved changes" title="Unsaved changes"></span>`
                : "";
            nm.innerHTML = `${dotHtml}${escapeHtml(t.name)}`;
            main.appendChild(nm);
            const meta = document.createElement("div");
            meta.className = "slls-dle-item-meta";
            const sourceLabel = effExpression || "(no expression)";
            const entity = effSchema
                ? `${effSchema}.${effEntity}`
                : effEntity;
            meta.textContent = `Entity: ${entity || "(unknown)"} · Source: ${sourceLabel}`;
            main.appendChild(meta);
            row.appendChild(main);
            const actions = document.createElement("div");
            actions.className = "slls-dle-item-actions";
            const colsBtn = document.createElement("button");
            colsBtn.className = "slls-dle-btn";
            colsBtn.textContent = "Columns";
            colsBtn.addEventListener("click", () => openColumnsModal(t));
            actions.appendChild(colsBtn);
            const reassignBtn = document.createElement("button");
            reassignBtn.className = "slls-dle-btn";
            reassignBtn.textContent = "Reassign";
            reassignBtn.addEventListener("click", () => openReassignModal(t));
            actions.appendChild(reassignBtn);
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
            nm.innerHTML =
                `<span class="slls-dle-pending-dot" aria-label="Unsaved" title="Unsaved"></span>` +
                escapeHtml(displayName);
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

    function openSourceModal(existing) {
        modal.innerHTML = "";
        const h = document.createElement("h2");
        h.textContent = existing ? `Edit source: ${existing.itemName}` : "Add a new source";
        modal.appendChild(h);

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

        const footer = document.createElement("div");
        footer.className = "slls-dle-modal-footer";
        modal.appendChild(footer);
        const cancel = document.createElement("button");
        cancel.className = "slls-dle-btn"; cancel.textContent = "Cancel";
        cancel.addEventListener("click", closeModal);
        footer.appendChild(cancel);
        const save = document.createElement("button");
        save.className = "slls-dle-btn slls-dle-btn-primary";
        save.textContent = existing ? "Stage changes" : "Stage source";
        save.addEventListener("click", () => {
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

    function openReassignModal(table) {
        modal.innerHTML = "";
        const h = document.createElement("h2");
        h.textContent = `Reassign table: ${table.name}`;
        modal.appendChild(h);

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

        const footer = document.createElement("div");
        footer.className = "slls-dle-modal-footer";
        modal.appendChild(footer);
        const cancel = document.createElement("button");
        cancel.className = "slls-dle-btn"; cancel.textContent = "Cancel";
        cancel.addEventListener("click", closeModal);
        footer.appendChild(cancel);
        // Show Revert if there are staged reassign changes for this table.
        if (pendingState.changes.some(
            c => c.kind === "reassign_table" && c.key === table.name,
        )) {
            const revert = document.createElement("button");
            revert.className = "slls-dle-btn slls-dle-btn-danger";
            revert.textContent = "Revert";
            revert.title = "Discard staged reassign for this table";
            revert.addEventListener("click", () => {
                revertChangesMatching(
                    c => c.kind === "reassign_table" && c.key === table.name,
                    `Reverted staged reassign for '${table.name}'.`,
                );
                closeModal();
            });
            footer.appendChild(revert);
        }
        const save = document.createElement("button");
        save.className = "slls-dle-btn slls-dle-btn-primary";
        save.textContent = "Stage changes";
        save.addEventListener("click", () => {
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
        });
        footer.appendChild(save);
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

    function openColumnsModal(table) {
        modal.innerHTML = "";
        modal.classList.add("slls-dle-modal-wide");
        registerModalCleanup(() => modal.classList.remove("slls-dle-modal-wide"));

        const h = document.createElement("h2");
        h.textContent = `Edit columns: ${table.name}`;
        modal.appendChild(h);

        const baseCols = (table.columns || []).map(c => ({
            name: c.name,
            sourceColumn: c.sourceColumn || "",
            dataType: c.dataType || "",
            dataCategory: c.dataCategory || "",
            columnType: c.columnType || "",
        }));
        const stagedMap = mergedPendingColumnEditsForTable(table.name);

        if (baseCols.length === 0) {
            const p = document.createElement("div");
            p.className = "slls-dle-empty";
            p.textContent = "This table has no editable columns.";
            modal.appendChild(p);
            const footer = document.createElement("div");
            footer.className = "slls-dle-modal-footer";
            modal.appendChild(footer);
            const close = document.createElement("button");
            close.className = "slls-dle-btn"; close.textContent = "Close";
            close.addEventListener("click", closeModal);
            footer.appendChild(close);
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
                sourceColumn: "source_column" in st ? (st.source_column || "") : bc.sourceColumn,
                dataType: "data_type" in st ? (st.data_type || "") : bc.dataType,
                dataCategory: "data_category" in st ? (st.data_category || "") : bc.dataCategory,
            };
        }

        for (const bc of baseCols) {
            const row = document.createElement("div");
            row.className = "slls-dle-column-row";
            const head = document.createElement("div");
            head.className = "slls-dle-column-head";
            const nm = document.createElement("div");
            nm.className = "slls-dle-column-name";
            nm.textContent = bc.name;
            head.appendChild(nm);
            const ty = document.createElement("div");
            ty.className = "slls-dle-column-type";
            ty.textContent = bc.columnType || "";
            head.appendChild(ty);
            row.appendChild(head);

            const fields = document.createElement("div");
            fields.className = "slls-dle-column-fields";
            row.appendChild(fields);

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

            const catInput = document.createElement("input");
            catInput.type = "text";
            catInput.className = "slls-dle-input";
            catInput.value = state[bc.name].dataCategory;
            catInput.placeholder = "(none)";
            const catField = makeField("Data category", catInput);
            fields.appendChild(catField);

            function refreshDots() {
                setFieldDirty(
                    srcField,
                    !srcInput.disabled && srcInput.value !== bc.sourceColumn,
                );
                setFieldDirty(typeField, typeSel.value !== bc.dataType);
                setFieldDirty(
                    catField,
                    (catInput.value || "") !== bc.dataCategory,
                );
                const rowDirty =
                    (!srcInput.disabled && srcInput.value !== bc.sourceColumn) ||
                    typeSel.value !== bc.dataType ||
                    (catInput.value || "") !== bc.dataCategory;
                row.classList.toggle("pending", rowDirty);
            }
            srcInput.addEventListener("input", () => {
                state[bc.name].sourceColumn = srcInput.value;
                refreshDots();
            });
            typeSel.addEventListener("change", () => {
                state[bc.name].dataType = typeSel.value;
                refreshDots();
            });
            catInput.addEventListener("input", () => {
                state[bc.name].dataCategory = catInput.value;
                refreshDots();
            });
            refreshDots();

            list.appendChild(row);
        }

        const footer = document.createElement("div");
        footer.className = "slls-dle-modal-footer";
        modal.appendChild(footer);
        const cancel = document.createElement("button");
        cancel.className = "slls-dle-btn"; cancel.textContent = "Cancel";
        cancel.addEventListener("click", closeModal);
        footer.appendChild(cancel);
        // Show Revert if there are staged column edits for this table.
        if (pendingState.changes.some(
            c => c.kind === "edit_columns" && c.key === table.name,
        )) {
            const revert = document.createElement("button");
            revert.className = "slls-dle-btn slls-dle-btn-danger";
            revert.textContent = "Revert";
            revert.title = "Discard staged column edits for this table";
            revert.addEventListener("click", () => {
                revertChangesMatching(
                    c => c.kind === "edit_columns" && c.key === table.name,
                    `Reverted staged column edits for '${table.name}'.`,
                );
                closeModal();
            });
            footer.appendChild(revert);
        }
        const save = document.createElement("button");
        save.className = "slls-dle-btn slls-dle-btn-primary";
        save.textContent = "Stage changes";
        save.addEventListener("click", () => {
            // Collect only the columns that diverge from baseline; for each
            // such column include only the properties that actually changed.
            const changed = [];
            for (const bc of baseCols) {
                const cur = state[bc.name];
                const entry = { name: bc.name };
                let dirty = false;
                if (bc.columnType === "Data" && cur.sourceColumn !== bc.sourceColumn) {
                    entry.source_column = cur.sourceColumn;
                    dirty = true;
                }
                if (cur.dataType !== bc.dataType) {
                    entry.data_type = cur.dataType;
                    dirty = true;
                }
                if (cur.dataCategory !== bc.dataCategory) {
                    entry.data_category = cur.dataCategory;
                    dirty = true;
                }
                if (dirty) changed.push(entry);
            }
            if (changed.length === 0) {
                setStatus("No column changes to stage.", "info");
                closeModal();
                return;
            }
            enqueuePendingChange({
                id: pendingId(),
                kind: "edit_columns",
                key: table.name,
                payload: { table_name: table.name, columns: changed },
            });
            closeModal();
        });
        footer.appendChild(save);
        openModal();
    }

    function openAddTablesModal() {
        modal.innerHTML = "";
        const h = document.createElement("h2");
        h.textContent = "Add tables to model";
        modal.appendChild(h);

        const sources = model.get("sources") || [];
        if (sources.length === 0) {
            const p = document.createElement("div");
            p.className = "slls-dle-empty";
            p.textContent = "Add a source to the model before adding tables.";
            modal.appendChild(p);
            const footer = document.createElement("div");
            footer.className = "slls-dle-modal-footer";
            modal.appendChild(footer);
            const close = document.createElement("button");
            close.className = "slls-dle-btn"; close.textContent = "Close";
            close.addEventListener("click", closeModal);
            footer.appendChild(close);
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

        const manualInput = document.createElement("input");
        manualInput.type = "text";
        manualInput.className = "slls-dle-input";
        manualInput.placeholder = "dbo.sales, dbo.geography";
        grid.appendChild(makeField("Tables to add (comma-separated)", manualInput));

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
            "Use schema.table (e.g. dbo.sales). All columns from each source table will be added. " +
            "The optional table name only applies when adding a single source table.";
        modal.appendChild(hint);

        const footer = document.createElement("div");
        footer.className = "slls-dle-modal-footer";
        modal.appendChild(footer);
        const cancel = document.createElement("button");
        cancel.className = "slls-dle-btn"; cancel.textContent = "Cancel";
        cancel.addEventListener("click", closeModal);
        footer.appendChild(cancel);
        const save = document.createElement("button");
        save.className = "slls-dle-btn slls-dle-btn-primary";
        save.textContent = "Stage tables";
        save.addEventListener("click", () => {
            const names = (manualInput.value || "")
                .split(",").map(s => s.trim()).filter(Boolean);
            if (names.length === 0) { setStatus("Please enter at least one table.", "error"); return; }
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
        });
        footer.appendChild(save);
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
                        "columnType": str(c.Type),
                    }
                )
            tables.append(
                {
                    "name": t.Name,
                    "expressionName": expression_name or "",
                    "entityName": entity_name or "",
                    "schemaName": schema_name or "",
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
def direct_lake_editor(
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
        Optional name or ID of a Direct Lake semantic model to open immediately.
        If None, the editor opens on the model-selection screen.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where datasets are listed and where new
        models are created by default. Defaults to None which resolves to the
        workspace of the attached lakehouse or, if no lakehouse is attached,
        the workspace of the notebook.
    dark_mode : bool, default=False
        If True, renders the editor with a dark color theme. If False, renders
        with a light color theme.
    """

    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "The 'direct_lake_editor' function requires the 'anywidget' package. "
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
            dfI = fabric.list_items(workspace=workspace_id, type=item_type)
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
                if not name or not src_id or not tables:
                    widget.status = {
                        "message": "Model name, source, and tables are required.",
                        "kind": "error",
                    }
                    return
                generate_direct_lake_semantic_model(
                    dataset=name,
                    tables=tables,
                    source=src_id,
                    source_type=src_type,
                    source_workspace=src_ws_id,
                    use_sql_endpoint=bool(data.get("use_sql_endpoint")),
                    workspace=ws_id,
                    refresh=bool(data.get("refresh")),
                )
                ds_name, ds_id_resolved = resolve_dataset_name_and_id(name, ws_id)
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
                summary = []
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
                                    custom_name = (spec.get("name") or "").strip() or None
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
                        elif kind == "edit_columns":
                            table_name = p.get("table_name")
                            cols = p.get("columns") or []
                            if not table_name:
                                raise ValueError(
                                    "Table is required to edit columns."
                                )
                            if not cols:
                                continue
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
                                tom.update_column(
                                    table_name=table_name,
                                    column_name=col_name,
                                    **kwargs,
                                )
                            summary.append(
                                f"updated {len(cols)} column(s) in "
                                f"'{table_name}'"
                            )
                        else:
                            raise ValueError(f"Unknown pending change kind: {kind!r}")
                    tom.model.SaveChanges()
                _load_model_state(ds_id, ws_id)
                widget.status = {
                    "message": (
                        f"Saved {len(changes)} change(s) to the model: "
                        f"{', '.join(summary)}."
                        if summary
                        else f"Saved {len(changes)} change(s) to the model."
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
