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
    grid-template-columns: 1fr 1fr;
    gap: 12px;
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
    padding: 10px 14px;
    border-radius: var(--slls-radius-sm);
    font-size: 13.5px;
    display: none;
}
.slls-dle-status.show { display: block; animation: slls-dle-fade-in 200ms ease; }
.slls-dle-status.success { background: var(--slls-success-soft); color: var(--slls-success); }
.slls-dle-status.error { background: var(--slls-danger-soft); color: var(--slls-danger); }
.slls-dle-status.info { background: var(--slls-accent-soft); color: var(--slls-accent); }
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
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.4);
    z-index: 9999;
    align-items: center;
    justify-content: center;
    padding: 16px;
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
    max-height: 90vh;
    overflow-y: auto;
    padding: 22px;
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
    const REFRESH_SVG = `<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M13 8a5 5 0 1 1-1.46-3.54M13 3v3h-3"/></svg>`;
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

    function makeField(labelText, inputEl) {
        const wrap = document.createElement("div");
        wrap.className = "slls-dle-field";
        const lab = document.createElement("label");
        lab.textContent = labelText;
        wrap.appendChild(lab);
        wrap.appendChild(inputEl);
        return wrap;
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
    createGrid.appendChild(makeField("Source workspace", newSrcWsSelect));

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
    manageToolbar.className = "slls-dle-toolbar";
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
        sourcesHeading.innerHTML = `Sources <span class="slls-dle-count">(${sources.length})</span>`;
        sourcesList.innerHTML = "";
        if (sources.length === 0) {
            const e = document.createElement("div");
            e.className = "slls-dle-empty";
            e.textContent = "No Direct Lake sources found.";
            sourcesList.appendChild(e);
            return;
        }
        for (const s of sources) {
            const row = document.createElement("div");
            row.className = "slls-dle-item";
            const main = document.createElement("div");
            main.className = "slls-dle-item-main";
            const nm = document.createElement("div");
            nm.className = "slls-dle-item-name";
            nm.innerHTML = `${escapeHtml(s.itemName || "(unknown)")} <span class="slls-dle-pill">${escapeHtml(s.itemType || "")}</span>`;
            main.appendChild(nm);
            const meta = document.createElement("div");
            meta.className = "slls-dle-item-meta";
            const sqlBit = s.usesSqlEndpoint ? " · SQL endpoint" : "";
            const tableCount = s.tableCount != null ? ` · ${s.tableCount} table${s.tableCount === 1 ? "" : "s"}` : "";
            meta.textContent = `${s.workspaceName || ""} · expression: ${s.expressionName || ""}${sqlBit}${tableCount}`;
            main.appendChild(meta);
            row.appendChild(main);
            const actions = document.createElement("div");
            actions.className = "slls-dle-item-actions";
            const editBtn = document.createElement("button");
            editBtn.className = "slls-dle-btn";
            editBtn.textContent = "Edit";
            editBtn.addEventListener("click", () => openSourceModal(s));
            actions.appendChild(editBtn);
            row.appendChild(actions);
            sourcesList.appendChild(row);
        }
    }

    function renderTables() {
        const tables = model.get("tables") || [];
        const sources = model.get("sources") || [];
        const exprToSource = {};
        for (const s of sources) exprToSource[s.expressionName] = s;
        tablesHeading.innerHTML = `Tables <span class="slls-dle-count">(${tables.length})</span>`;
        tablesList.innerHTML = "";
        if (tables.length === 0) {
            const e = document.createElement("div");
            e.className = "slls-dle-empty";
            e.textContent = "No tables in this model.";
            tablesList.appendChild(e);
            return;
        }
        for (const t of tables) {
            const row = document.createElement("div");
            row.className = "slls-dle-item";
            const main = document.createElement("div");
            main.className = "slls-dle-item-main";
            const nm = document.createElement("div");
            nm.className = "slls-dle-item-name";
            nm.textContent = t.name;
            main.appendChild(nm);
            const meta = document.createElement("div");
            meta.className = "slls-dle-item-meta";
            const src = exprToSource[t.expressionName];
            const sourceLabel = src
                ? `${src.itemName} (${src.itemType})`
                : (t.expressionName || "(no expression)");
            const entity = t.schemaName
                ? `${t.schemaName}.${t.entityName || ""}`
                : (t.entityName || "");
            meta.textContent = `Entity: ${entity || "(unknown)"} · Source: ${sourceLabel}`;
            main.appendChild(meta);
            row.appendChild(main);
            const actions = document.createElement("div");
            actions.className = "slls-dle-item-actions";
            const reassignBtn = document.createElement("button");
            reassignBtn.className = "slls-dle-btn";
            reassignBtn.textContent = "Reassign";
            reassignBtn.addEventListener("click", () => openReassignModal(t));
            actions.appendChild(reassignBtn);
            row.appendChild(actions);
            tablesList.appendChild(row);
        }
    }

    model.on("change:sources", () => { renderSources(); renderTables(); });
    model.on("change:tables", renderTables);

    // ----------- Modal infrastructure -----------
    const overlay = document.createElement("div");
    overlay.className = "slls-dle-overlay";
    el.appendChild(overlay);
    const modal = document.createElement("div");
    modal.className = "slls-dle-modal";
    overlay.appendChild(modal);
    overlay.addEventListener("click", (e) => { if (e.target === overlay) closeModal(); });
    function openModal() { overlay.classList.add("show"); }
    function closeModal() { overlay.classList.remove("show"); modal.innerHTML = ""; }

    function openSourceModal(existing) {
        modal.innerHTML = "";
        const h = document.createElement("h2");
        h.textContent = existing ? `Edit source: ${existing.itemName}` : "Add a new source";
        modal.appendChild(h);

        const grid = document.createElement("div");
        grid.className = "slls-dle-grid";
        modal.appendChild(grid);

        const typeSel = document.createElement("select");
        typeSel.className = "slls-dle-select";
        for (const t of (model.get("source_types") || [])) {
            const o = document.createElement("option"); o.value = t; o.textContent = t;
            if (existing && existing.itemType === t) o.selected = true;
            typeSel.appendChild(o);
        }
        grid.appendChild(makeField("Source type", typeSel));

        const wsSel = document.createElement("select");
        wsSel.className = "slls-dle-select";
        for (const ws of (model.get("workspaces") || [])) {
            const o = document.createElement("option"); o.value = ws.id; o.textContent = ws.name;
            if (existing && existing.workspaceId === ws.id) o.selected = true;
            wsSel.appendChild(o);
        }
        if (!existing) wsSel.value = model.get("workspace_id") || wsSel.value;
        grid.appendChild(makeField("Source workspace", wsSel));

        const itemSel = document.createElement("select");
        itemSel.className = "slls-dle-select";
        grid.appendChild(makeField("Source item", itemSel));

        const sqlToggle = makeToggle("Use SQL endpoint", existing ? !!existing.usesSqlEndpoint : false);
        modal.appendChild(sqlToggle);

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
                return;
            }
            for (const it of items) {
                const o = document.createElement("option");
                o.value = it.id; o.textContent = it.name;
                if (existing && existing.itemId === it.id) o.selected = true;
                itemSel.appendChild(o);
            }
        }
        typeSel.addEventListener("change", refreshItems);
        wsSel.addEventListener("change", refreshItems);
        const itemsListener = () => refreshItems();
        model.on("change:source_items", itemsListener);
        refreshItems();

        const footer = document.createElement("div");
        footer.className = "slls-dle-modal-footer";
        modal.appendChild(footer);
        const cancel = document.createElement("button");
        cancel.className = "slls-dle-btn"; cancel.textContent = "Cancel";
        cancel.addEventListener("click", () => {
            try { model.off("change:source_items", itemsListener); } catch(_) {}
            closeModal();
        });
        footer.appendChild(cancel);
        const save = document.createElement("button");
        save.className = "slls-dle-btn slls-dle-btn-primary";
        save.textContent = existing ? "Save" : "Add";
        save.addEventListener("click", () => {
            if (!itemSel.value) { setStatus("Please pick a source item.", "error"); return; }
            const payload = {
                source_type: typeSel.value,
                source_workspace_id: wsSel.value,
                source_id: itemSel.value,
                use_sql_endpoint: sqlToggle._input.checked,
            };
            try { model.off("change:source_items", itemsListener); } catch(_) {}
            if (existing) {
                payload.expression_name = existing.expressionName;
                runAction("update_source", payload);
            } else {
                runAction("add_source", payload);
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

        const sources = model.get("sources") || [];
        const grid = document.createElement("div");
        grid.className = "slls-dle-grid";
        modal.appendChild(grid);

        const exprSel = document.createElement("select");
        exprSel.className = "slls-dle-select";
        for (const s of sources) {
            const o = document.createElement("option");
            o.value = s.expressionName;
            o.textContent = `${s.expressionName} — ${s.itemName} (${s.itemType})`;
            if (s.expressionName === table.expressionName) o.selected = true;
            exprSel.appendChild(o);
        }
        grid.appendChild(makeField("Source (expression)", exprSel));

        const schemaInput = document.createElement("input");
        schemaInput.type = "text";
        schemaInput.className = "slls-dle-input";
        schemaInput.value = table.schemaName || "";
        schemaInput.placeholder = "dbo";
        grid.appendChild(makeField("Schema (optional)", schemaInput));

        const entityInput = document.createElement("input");
        entityInput.type = "text";
        entityInput.className = "slls-dle-input";
        entityInput.value = table.entityName || "";
        entityInput.placeholder = "source_table";
        grid.appendChild(makeField("Entity (source table) name", entityInput));

        const footer = document.createElement("div");
        footer.className = "slls-dle-modal-footer";
        modal.appendChild(footer);
        const cancel = document.createElement("button");
        cancel.className = "slls-dle-btn"; cancel.textContent = "Cancel";
        cancel.addEventListener("click", closeModal);
        footer.appendChild(cancel);
        const save = document.createElement("button");
        save.className = "slls-dle-btn slls-dle-btn-primary";
        save.textContent = "Save";
        save.addEventListener("click", () => {
            const entity = (entityInput.value || "").trim();
            if (!entity) { setStatus("Entity name is required.", "error"); return; }
            runAction("reassign_table", {
                table_name: table.name,
                expression_name: exprSel.value,
                entity_name: entity,
                schema: (schemaInput.value || "").trim(),
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
            o.textContent = `${s.expressionName} — ${s.itemName} (${s.itemType})`;
            exprSel.appendChild(o);
        }
        grid.appendChild(makeField("Source (expression)", exprSel));

        const manualInput = document.createElement("input");
        manualInput.type = "text";
        manualInput.className = "slls-dle-input";
        manualInput.placeholder = "dbo.sales, dbo.geography";
        grid.appendChild(makeField("Tables to add (comma-separated)", manualInput));

        const hint = document.createElement("div");
        hint.className = "slls-dle-item-meta";
        hint.style.marginTop = "6px";
        hint.textContent =
            "Use schema.table (e.g. dbo.sales). All columns from each source table will be added.";
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
        save.textContent = "Add";
        save.addEventListener("click", () => {
            const names = (manualInput.value || "")
                .split(",").map(s => s.trim()).filter(Boolean);
            if (names.length === 0) { setStatus("Please enter at least one table.", "error"); return; }
            runAction("add_tables", {
                expression_name: exprSel.value,
                tables: names,
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
    renderScreen();
}
export default { render };
"""


def _build_tables_payload(tom):
    """Return a list of dicts describing Direct Lake tables and their partitions."""
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
            tables.append(
                {
                    "name": t.Name,
                    "expressionName": expression_name or "",
                    "entityName": entity_name or "",
                    "schemaName": schema_name or "",
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
    workspace: Optional[str | UUID] = None,
    dataset: Optional[str | UUID] = None,
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
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where datasets are listed and where new
        models are created by default. Defaults to None which resolves to the
        workspace of the attached lakehouse or, if no lakehouse is attached,
        the workspace of the notebook.
    dataset : str | uuid.UUID, default=None
        Optional name or ID of a Direct Lake semantic model to open immediately.
        If None, the editor opens on the model-selection screen.
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
            return [{"id": initial_ws_id, "name": initial_ws_name}]
        id_col, name_col = _pick_columns(dfW, ["Id"], ["Name"])
        if id_col is None or name_col is None:
            return [{"id": initial_ws_id, "name": initial_ws_name}]
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
                widget.status = {
                    "message": f"Opened '{ds_name}'.",
                    "kind": "success",
                }

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

            elif action == "add_source":
                ds_id = widget.dataset_id
                ws_id = widget.workspace_id
                src_type = data.get("source_type")
                src_ws_id = data.get("source_workspace_id")
                src_id = data.get("source_id")
                use_sql = bool(data.get("use_sql_endpoint"))
                src_name, src_id_resolved = resolve_item_name_and_id(
                    item=src_id, type=src_type, workspace=src_ws_id
                )
                expr_text = generate_shared_expression(
                    item=src_id_resolved,
                    item_type=src_type,
                    workspace=src_ws_id,
                    use_sql_endpoint=use_sql,
                )
                with connect_semantic_model(
                    dataset=ds_id, workspace=ws_id, readonly=False
                ) as tom:
                    base = "DatabaseQuery" if use_sql else f"DL_{src_type}"
                    expr_name = _unique_expression_name(tom, base)
                    tom.add_expression(name=expr_name, expression=expr_text)
                    tom.model.SaveChanges()
                _load_model_state(ds_id, ws_id)
                widget.status = {
                    "message": f"Added source '{src_name}' as expression '{expr_name}'.",
                    "kind": "success",
                }

            elif action == "update_source":
                ds_id = widget.dataset_id
                ws_id = widget.workspace_id
                expr_name = data.get("expression_name")
                src_type = data.get("source_type")
                src_ws_id = data.get("source_workspace_id")
                src_id = data.get("source_id")
                use_sql = bool(data.get("use_sql_endpoint"))
                src_name, src_id_resolved = resolve_item_name_and_id(
                    item=src_id, type=src_type, workspace=src_ws_id
                )
                expr_text = generate_shared_expression(
                    item=src_id_resolved,
                    item_type=src_type,
                    workspace=src_ws_id,
                    use_sql_endpoint=use_sql,
                )
                with connect_semantic_model(
                    dataset=ds_id, workspace=ws_id, readonly=False
                ) as tom:
                    if not tom.model.Expressions.Find(expr_name):
                        raise ValueError(
                            f"Expression '{expr_name}' not found in model."
                        )
                    tom.model.Expressions[expr_name].Expression = expr_text
                    tom.model.SaveChanges()
                _load_model_state(ds_id, ws_id)
                widget.status = {
                    "message": f"Updated source for expression '{expr_name}' to '{src_name}'.",
                    "kind": "success",
                }

            elif action == "reassign_table":
                ds_id = widget.dataset_id
                ws_id = widget.workspace_id
                table_name = data.get("table_name")
                expr_name = data.get("expression_name")
                entity_name = data.get("entity_name")
                schema = data.get("schema") or None
                with connect_semantic_model(
                    dataset=ds_id, workspace=ws_id, readonly=False
                ) as tom:
                    if not any(t.Name == table_name for t in tom.model.Tables):
                        raise ValueError(f"Table '{table_name}' not found in model.")
                    if not tom.model.Expressions.Find(expr_name):
                        raise ValueError(
                            f"Expression '{expr_name}' not found in model."
                        )
                    tbl = tom.model.Tables[table_name]
                    part = next(iter(tbl.Partitions))
                    part.Source.EntityName = entity_name
                    part.Source.ExpressionSource = tom.model.Expressions[expr_name]
                    if schema:
                        part.Source.SchemaName = schema
                        tbl.SourceLineageTag = f"[{schema}].[{entity_name}]"
                    else:
                        tbl.SourceLineageTag = f"[dbo].[{entity_name}]"
                    tom.model.SaveChanges()
                _load_model_state(ds_id, ws_id)
                widget.status = {
                    "message": (
                        f"Reassigned '{table_name}' to expression '{expr_name}'."
                    ),
                    "kind": "success",
                }

            elif action == "add_tables":
                ds_id = widget.dataset_id
                ws_id = widget.workspace_id
                expr_name = data.get("expression_name")
                table_specs = data.get("tables") or []
                if not expr_name or not table_specs:
                    widget.status = {
                        "message": "Source and tables are required.",
                        "kind": "error",
                    }
                    return
                # Look up source details from cached sources.
                src_info = next(
                    (s for s in widget.sources if s.get("expressionName") == expr_name),
                    None,
                )
                if src_info is None:
                    raise ValueError(f"Source for '{expr_name}' could not be resolved.")
                src_id = src_info.get("itemId")
                src_ws_id = src_info.get("workspaceId")
                src_type = src_info.get("itemType")
                added_names = []
                with connect_semantic_model(
                    dataset=ds_id, workspace=ws_id, readonly=False
                ) as tom:
                    existing_names = {t.Name for t in tom.model.Tables}
                    for spec in table_specs:
                        spec = spec.strip()
                        if "." in spec:
                            schema_name, entity_name = spec.split(".", 1)
                        else:
                            schema_name = None
                            entity_name = spec
                        table_display = entity_name
                        if table_display in existing_names:
                            raise ValueError(
                                f"Table '{table_display}' already exists in the model."
                            )
                        # Discover columns from delta path for Fabric-native sources.
                        path = create_abfss_path(
                            lakehouse_id=src_id,
                            lakehouse_workspace_id=resolve_workspace_id(src_ws_id),
                            delta_table_name=entity_name,
                            schema=schema_name,
                        )
                        dfC = list_columns_from_path(path=path)
                        if dfC.empty:
                            raise ValueError(
                                f"Source table '{spec}' has no columns or does not exist."
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
                            if dtype == "Binary":
                                continue
                            tom.add_data_column(
                                table_name=table_display,
                                column_name=col_name,
                                data_type=dtype,
                                source_column=col_name,
                            )
                        existing_names.add(table_display)
                        added_names.append(table_display)
                    tom.model.SaveChanges()
                _load_model_state(ds_id, ws_id)
                widget.status = {
                    "message": (
                        f"Added {len(added_names)} table(s): "
                        f"{', '.join(added_names)}."
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
