from sempy_labs.tom import connect_semantic_model
from typing import Optional
from uuid import UUID
from sempy._utils._log import log


_WIDGET_CSS = """
.slls-pe {
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
    max-width: 920px;
    background: var(--slls-bg-solid);
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    box-shadow: var(--slls-shadow);
    padding: 24px;
    box-sizing: border-box;
}
@media (prefers-color-scheme: dark) {
    .slls-pe.slls-pe-auto {
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
.slls-pe.slls-pe-dark {
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
.slls-pe * { box-sizing: border-box; }

.slls-pe-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 18px;
    flex-wrap: wrap;
}
.slls-pe-title {
    font-size: 22px;
    font-weight: 600;
    letter-spacing: -0.01em;
    margin-right: auto;
    line-height: 1.15;
}
.slls-pe-titlewrap {
    display: flex;
    flex-direction: column;
    margin-right: auto;
    min-width: 0;
}
.slls-pe-subtitle {
    font-size: 12px;
    color: var(--slls-text-secondary);
    margin-top: 2px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 540px;
}
.slls-pe-subtitle .slls-pe-sep {
    color: var(--slls-text-tertiary);
    margin: 0 6px;
}
.slls-pe-subtitle b {
    color: var(--slls-text);
    font-weight: 500;
}
.slls-pe-select {
    appearance: none;
    -webkit-appearance: none;
    background: var(--slls-surface);
    border: 1px solid var(--slls-border-strong);
    border-radius: 999px;
    padding: 7px 32px 7px 14px;
    font-size: 13.5px;
    color: var(--slls-text);
    font-family: inherit;
    cursor: pointer;
    background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6'><path fill='%236e6e73' d='M0 0l5 6 5-6z'/></svg>");
    background-repeat: no-repeat;
    background-position: right 12px center;
    transition: border-color 120ms ease, box-shadow 120ms ease;
}
.slls-pe-select:hover { border-color: var(--slls-text-tertiary); }
.slls-pe-select:focus {
    outline: none;
    border-color: var(--slls-accent);
    box-shadow: 0 0 0 3px var(--slls-accent-soft);
}
.slls-pe-select option {
    background-color: var(--slls-bg-solid);
    color: var(--slls-text);
    padding: 6px 10px;
}
.slls-pe-select option:checked,
.slls-pe-select option:hover {
    background-color: var(--slls-accent-soft);
    color: var(--slls-text);
}

.slls-pe-input {
    appearance: none;
    background: var(--slls-surface);
    border: 1px solid var(--slls-border-strong);
    border-radius: 999px;
    padding: 7px 14px;
    font-size: 13.5px;
    color: var(--slls-text);
    font-family: inherit;
    transition: border-color 120ms ease, box-shadow 120ms ease;
}
.slls-pe-input::placeholder { color: var(--slls-text-tertiary); }
.slls-pe-input:focus {
    outline: none;
    border-color: var(--slls-accent);
    box-shadow: 0 0 0 3px var(--slls-accent-soft);
}

.slls-pe-btn {
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
.slls-pe-btn:hover { background: var(--slls-surface-2); border-color: var(--slls-text-tertiary); }
.slls-pe-btn:active { transform: scale(0.97); }
.slls-pe-btn:disabled { opacity: 0.4; cursor: not-allowed; }

.slls-pe-btn-primary {
    background: var(--slls-accent);
    border-color: var(--slls-accent);
    color: #fff;
}
.slls-pe-btn-primary:hover { background: var(--slls-accent-hover); border-color: var(--slls-accent-hover); }

.slls-pe-btn-danger {
    background: transparent;
    border-color: var(--slls-danger);
    color: var(--slls-danger);
}
.slls-pe-btn-danger:hover { background: var(--slls-danger-soft); }

.slls-pe-btn-icon {
    width: 32px; height: 32px;
    padding: 0;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border-radius: 50%;
    font-size: 18px;
    line-height: 1;
}

.slls-pe-toolbar {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 14px;
    flex-wrap: wrap;
}
.slls-pe-search { flex: 1; min-width: 180px; max-width: 360px; }
.slls-pe-summary {
    margin-left: auto;
    font-size: 12.5px;
    color: var(--slls-text-secondary);
}

.slls-pe-tree {
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    overflow: hidden;
    background: var(--slls-surface);
    max-height: 520px;
    overflow-y: auto;
}
.slls-pe-tree::-webkit-scrollbar { width: 10px; height: 10px; }
.slls-pe-tree::-webkit-scrollbar-thumb {
    background: var(--slls-border-strong);
    border-radius: 999px;
    background-clip: padding-box;
    border: 2px solid transparent;
}
.slls-pe-tree::-webkit-scrollbar-thumb:hover { background-color: var(--slls-text-tertiary); }

.slls-pe-table { border-bottom: 1px solid var(--slls-border); }
.slls-pe-table:last-child { border-bottom: none; }
.slls-pe-table.hidden-match { display: none; }

.slls-pe-row {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 14px;
    cursor: pointer;
    user-select: none;
    transition: background 100ms ease;
}
.slls-pe-row:hover { background: var(--slls-surface-2); }

.slls-pe-caret {
    width: 16px; height: 16px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    color: var(--slls-text-tertiary);
    transition: transform 160ms ease;
    flex-shrink: 0;
}
.slls-pe-table.expanded .slls-pe-caret { transform: rotate(90deg); }

.slls-pe-check {
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
.slls-pe-check[data-state="all"] {
    background: var(--slls-accent);
    border-color: var(--slls-accent);
}
.slls-pe-check[data-state="all"]::after {
    content: "";
    width: 10px; height: 6px;
    border-left: 2px solid #fff;
    border-bottom: 2px solid #fff;
    transform: rotate(-45deg) translate(1px, -1px);
}
.slls-pe-check[data-state="some"] {
    background: var(--slls-accent);
    border-color: var(--slls-accent);
}
.slls-pe-check[data-state="some"]::after {
    content: "";
    width: 10px; height: 2px;
    background: #fff;
    border-radius: 1px;
}

.slls-pe-icon {
    width: 22px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    color: var(--slls-text);
    flex-shrink: 0;
    opacity: 0.85;
}
.slls-pe-icon svg { display: block; }
.slls-pe-table-icon { color: var(--slls-text); opacity: 0.9; }
.slls-pe-row.is-hidden .slls-pe-icon,
.slls-pe-row.is-hidden .slls-pe-table-icon { color: var(--slls-text-tertiary); opacity: 0.7; }

.slls-pe-name {
    font-size: 14px;
    font-weight: 500;
}
.slls-pe-name.hidden-obj { color: var(--slls-text-tertiary); font-style: italic; }
.slls-pe-table-summary {
    font-size: 12px;
    color: var(--slls-text-tertiary);
    margin-left: 4px;
}

.slls-pe-children {
    display: none;
    padding: 2px 0 8px 0;
    background: var(--slls-surface-2);
    border-top: 1px solid var(--slls-border);
}
.slls-pe-table.expanded .slls-pe-children { display: block; }

.slls-pe-child {
    padding-left: 56px;
    padding-top: 6px;
    padding-bottom: 6px;
    gap: 10px;
}
.slls-pe-child:hover { background: rgba(0, 122, 255, 0.06); }
.slls-pe-child.filtered-out { display: none; }

.slls-pe-footer {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-top: 18px;
    flex-wrap: wrap;
}
.slls-pe-footer .slls-pe-spacer { flex: 1; }

.slls-pe-status {
    margin-top: 14px;
    padding: 10px 14px;
    border-radius: var(--slls-radius-sm);
    font-size: 13.5px;
    display: none;
}
.slls-pe-status.show { display: block; animation: slls-fade-in 200ms ease; }
.slls-pe-status.success { background: var(--slls-success-soft); color: var(--slls-success); }
.slls-pe-status.error { background: var(--slls-danger-soft); color: var(--slls-danger); }
.slls-pe-status.info { background: var(--slls-accent-soft); color: var(--slls-accent); }
@keyframes slls-fade-in {
    from { opacity: 0; transform: translateY(-4px); }
    to { opacity: 1; transform: translateY(0); }
}

.slls-pe-confirm {
    margin-top: 14px;
    padding: 12px 16px;
    border-radius: var(--slls-radius-sm);
    border: 1px solid var(--slls-danger);
    background: var(--slls-danger-soft);
    display: none;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
}
.slls-pe-confirm.show { display: flex; }
.slls-pe-confirm-text { flex: 1; font-size: 13.5px; color: var(--slls-text); }

.slls-pe-create-row {
    display: none;
    align-items: center;
    gap: 8px;
}
.slls-pe-create-row.show { display: inline-flex; }

.slls-pe-empty {
    padding: 32px 16px;
    text-align: center;
    color: var(--slls-text-tertiary);
    font-size: 13.5px;
}

.slls-pe-busy {
    pointer-events: none;
    opacity: 0.6;
    transition: opacity 120ms ease;
}

.slls-pe-dirty {
    display: inline-block;
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--slls-orange);
    margin-left: 6px;
    box-shadow: 0 0 0 0 rgba(255, 149, 0, 0.5);
    animation: slls-pulse 1.6s ease-in-out infinite;
    flex-shrink: 0;
    vertical-align: middle;
}
@keyframes slls-pulse {
    0%, 100% { box-shadow: 0 0 0 0 rgba(255, 149, 0, 0.45); }
    50% { box-shadow: 0 0 0 4px rgba(255, 149, 0, 0); }
}

.slls-pe-attribution {
    margin-top: 18px;
    text-align: right;
    font-size: 11.5px;
    color: var(--slls-text-tertiary);
}
.slls-pe-attribution a {
    color: var(--slls-text-tertiary);
    text-decoration: none;
    transition: color 120ms ease;
}
.slls-pe-attribution a:hover {
    color: var(--slls-accent);
    text-decoration: none;
}
"""


_WIDGET_JS = r"""
function render({ model, el }) {
    const root = document.createElement("div");
    root.className = "slls-pe";
    function applyTheme() {
        root.classList.remove("slls-pe-dark", "slls-pe-auto");
        const dm = model.get("dark_mode");
        if (dm === true) root.classList.add("slls-pe-dark");
        else if (dm === null || dm === undefined) root.classList.add("slls-pe-auto");
    }
    applyTheme();
    model.on("change:dark_mode", applyTheme);
    el.appendChild(root);

    const ICON_SVG = {
        // Column: a tall rounded vertical bar — like a database column.
        columns: `<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
            <rect x="6" y="2.5" width="4" height="11" rx="1.6"/>
        </svg>`,
        // Measure: a clean sigma (∑).
        measures: `<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
            <path d="M12 3H4l4.5 5L4 13h8"/>
        </svg>`,
        // Hierarchy: a parent node connecting down to two child nodes.
        hierarchies: `<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
            <circle cx="8" cy="3.25" r="1.4"/>
            <circle cx="3.75" cy="12.75" r="1.4"/>
            <circle cx="12.25" cy="12.75" r="1.4"/>
            <path d="M8 4.65V8M8 8H3.75v3.35M8 8h4.25v3.35"/>
        </svg>`,
        // Table: a 2x2 grid with a header row.
        tables: `<svg width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
            <rect x="2.5" y="3" width="11" height="10" rx="1.8"/>
            <path d="M2.5 6.75h11M8 6.75v6.25"/>
        </svg>`,
    };
    const CARET = "<svg width='8' height='10' viewBox='0 0 8 10' fill='currentColor'><path d='M1 0l6 5-6 5V0z'/></svg>";

    function getMetadata() { return model.get("metadata") || {}; }
    function getPerspectives() { return model.get("perspectives") || []; }
    function getMembers() { return model.get("perspective_members") || {}; }
    function getSelected() { return model.get("selected_perspective") || ""; }

    let selection = {};
    let originalSelection = {};
    let filterText = "";
    let expanded = {};
    // Names of perspectives created in the UI that have not yet been
    // persisted to the model. While present, Save is force-enabled.
    const pendingNewPerspectives = new Set();

    // ----------- Header -----------
    const header = document.createElement("div");
    header.className = "slls-pe-header";
    root.appendChild(header);

    const titleWrap = document.createElement("div");
    titleWrap.className = "slls-pe-titlewrap";
    header.appendChild(titleWrap);

    const title = document.createElement("div");
    title.className = "slls-pe-title";
    title.textContent = "Perspective Editor";
    titleWrap.appendChild(title);

    const subtitle = document.createElement("div");
    subtitle.className = "slls-pe-subtitle";
    titleWrap.appendChild(subtitle);
    function renderSubtitle() {
        const ds = model.get("dataset_name") || "";
        const ws = model.get("workspace_name") || "";
        if (!ds && !ws) { subtitle.textContent = ""; return; }
        subtitle.innerHTML =
            (ds ? `<b>${escapeHtml(ds)}</b>` : "") +
            (ds && ws ? `<span class="slls-pe-sep">·</span>` : "") +
            (ws ? escapeHtml(ws) : "");
    }

    // Theme toggle button (light/dark)
    const SUN_SVG = `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="8" cy="8" r="3"/><path d="M8 1.5v1.5M8 13v1.5M1.5 8h1.5M13 8h1.5M3.3 3.3l1.05 1.05M11.65 11.65l1.05 1.05M3.3 12.7l1.05-1.05M11.65 4.35l1.05-1.05"/></svg>`;
    const MOON_SVG = `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M13.5 9.5A5.5 5.5 0 0 1 6.5 2.5a5.5 5.5 0 1 0 7 7z"/></svg>`;
    const themeBtn = document.createElement("button");
    themeBtn.className = "slls-pe-btn slls-pe-btn-icon";
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

    const select = document.createElement("select");
    select.className = "slls-pe-select";
    header.appendChild(select);

    // "New perspective" button — sits next to the dropdown.
    const PLUS_SVG = `<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" aria-hidden="true"><path d="M8 3.25v9.5M3.25 8h9.5"/></svg>`;
    const newBtn = document.createElement("button");
    newBtn.className = "slls-pe-btn slls-pe-btn-icon";
    newBtn.type = "button";
    newBtn.innerHTML = PLUS_SVG;
    newBtn.title = "New perspective";
    newBtn.setAttribute("aria-label", "New perspective");
    header.appendChild(newBtn);

    const createRow = document.createElement("div");
    createRow.className = "slls-pe-create-row";
    const newInput = document.createElement("input");
    newInput.type = "text";
    newInput.className = "slls-pe-input";
    newInput.placeholder = "Perspective name";
    newInput.style.width = "180px";
    const okBtn = document.createElement("button");
    okBtn.className = "slls-pe-btn slls-pe-btn-primary";
    okBtn.textContent = "Create";
    const cancelBtn = document.createElement("button");
    cancelBtn.className = "slls-pe-btn slls-pe-btn-icon";
    cancelBtn.textContent = "✕";
    cancelBtn.title = "Cancel";
    createRow.appendChild(newInput);
    createRow.appendChild(okBtn);
    createRow.appendChild(cancelBtn);
    header.appendChild(createRow);

    // ----------- Toolbar -----------
    const toolbar = document.createElement("div");
    toolbar.className = "slls-pe-toolbar";
    root.appendChild(toolbar);

    const search = document.createElement("input");
    search.type = "search";
    search.className = "slls-pe-input slls-pe-search";
    search.placeholder = "Search tables and fields…";
    toolbar.appendChild(search);

    const expandAllBtn = document.createElement("button");
    expandAllBtn.className = "slls-pe-btn";
    expandAllBtn.textContent = "Expand All";
    toolbar.appendChild(expandAllBtn);

    const collapseAllBtn = document.createElement("button");
    collapseAllBtn.className = "slls-pe-btn";
    collapseAllBtn.textContent = "Collapse All";
    toolbar.appendChild(collapseAllBtn);

    const summaryEl = document.createElement("div");
    summaryEl.className = "slls-pe-summary";
    toolbar.appendChild(summaryEl);

    // ----------- Tree -----------
    const tree = document.createElement("div");
    tree.className = "slls-pe-tree";
    root.appendChild(tree);

    // ----------- Confirm Delete -----------
    const confirm = document.createElement("div");
    confirm.className = "slls-pe-confirm";
    const confirmText = document.createElement("div");
    confirmText.className = "slls-pe-confirm-text";
    const confirmYes = document.createElement("button");
    confirmYes.className = "slls-pe-btn slls-pe-btn-danger";
    confirmYes.textContent = "Yes, Delete";
    const confirmNo = document.createElement("button");
    confirmNo.className = "slls-pe-btn";
    confirmNo.textContent = "Cancel";
    confirm.appendChild(confirmText);
    confirm.appendChild(confirmYes);
    confirm.appendChild(confirmNo);
    root.appendChild(confirm);

    // ----------- Footer -----------
    const footer = document.createElement("div");
    footer.className = "slls-pe-footer";
    root.appendChild(footer);

    const spacer = document.createElement("div");
    spacer.className = "slls-pe-spacer";
    footer.appendChild(spacer);

    const deleteBtn = document.createElement("button");
    deleteBtn.className = "slls-pe-btn slls-pe-btn-danger";
    deleteBtn.textContent = "Delete";
    footer.appendChild(deleteBtn);

    const saveBtn = document.createElement("button");
    saveBtn.className = "slls-pe-btn slls-pe-btn-primary";
    saveBtn.textContent = "Save";
    footer.appendChild(saveBtn);

    // ----------- Status -----------
    const status = document.createElement("div");
    status.className = "slls-pe-status";
    root.appendChild(status);

    // ----------- Attribution -----------
    const attribution = document.createElement("div");
    attribution.className = "slls-pe-attribution";
    attribution.innerHTML =
        'Powered by <a href="https://github.com/microsoft/semantic-link-labs" target="_blank" rel="noopener noreferrer">Semantic Link Labs</a>';
    root.appendChild(attribution);

    // ============== Helpers ==============
    function buildSelectionFromMembers(perspectiveName) {
        const md = getMetadata();
        const members = getMembers()[perspectiveName] || {};
        const sel = {};
        for (const tbl of Object.keys(md)) {
            const tblMembers = members[tbl] || {};
            sel[tbl] = { columns: {}, measures: {}, hierarchies: {} };
            for (const t of ["columns", "measures", "hierarchies"]) {
                const set = new Set(tblMembers[t] || []);
                for (const n of (md[tbl][t] || [])) {
                    sel[tbl][t][n] = set.has(n);
                }
            }
        }
        return sel;
    }

    function isObjDirty(tbl, t, n) {
        const cur = !!(selection[tbl] && selection[tbl][t] && selection[tbl][t][n]);
        const orig = !!(originalSelection[tbl] && originalSelection[tbl][t] && originalSelection[tbl][t][n]);
        return cur !== orig;
    }

    function isTableDirty(tbl) {
        const md = getMetadata()[tbl] || {};
        for (const t of ["columns", "measures", "hierarchies"]) {
            for (const n of (md[t] || [])) {
                if (isObjDirty(tbl, t, n)) return true;
            }
        }
        return false;
    }

    function hasAnyDirty() {
        if (pendingNewPerspectives.has(getSelected())) return true;
        for (const tbl of Object.keys(getMetadata())) {
            if (isTableDirty(tbl)) return true;
        }
        return false;
    }

    function tableState(tbl) {
        const sel = selection[tbl] || {};
        let total = 0, on = 0;
        for (const t of ["columns", "measures", "hierarchies"]) {
            const obj = sel[t] || {};
            for (const k of Object.keys(obj)) {
                total++;
                if (obj[k]) on++;
            }
        }
        if (total === 0) return "none";
        if (on === 0) return "none";
        if (on === total) return "all";
        return "some";
    }

    function updateGlobalSummary() {
        const md = getMetadata();
        const totalTables = Object.keys(md).length;
        let selectedTables = 0;
        for (const t of Object.keys(md)) {
            if (tableState(t) !== "none") selectedTables++;
        }
        summaryEl.textContent = `${selectedTables} of ${totalTables} tables selected`;
    }

    function tableCounts(tbl) {
        const md = getMetadata()[tbl] || {};
        const sel = selection[tbl] || {};
        const c = {};
        for (const t of ["columns", "measures", "hierarchies"]) {
            const total = (md[t] || []).length;
            const on = Object.values(sel[t] || {}).filter(Boolean).length;
            c[t] = [on, total];
        }
        return c;
    }

    function setStatus(message, kind) {
        if (!message) {
            status.classList.remove("show");
            return;
        }
        status.className = `slls-pe-status show ${kind || "info"}`;
        status.textContent = message;
    }

    function setBusy(b) {
        if (b) root.classList.add("slls-pe-busy");
        else root.classList.remove("slls-pe-busy");
    }

    // ============== Renderers ==============
    function renderHeader() {
        const persps = getPerspectives();
        const cur = getSelected();
        select.innerHTML = "";
        for (const p of persps) {
            const opt = document.createElement("option");
            opt.value = p;
            opt.textContent = p;
            if (p === cur) opt.selected = true;
            select.appendChild(opt);
        }
        if (persps.length === 0) {
            const optEmpty = document.createElement("option");
            optEmpty.value = "";
            optEmpty.textContent = "No perspectives";
            optEmpty.disabled = true;
            optEmpty.selected = true;
            select.appendChild(optEmpty);
        }
        deleteBtn.disabled = persps.length === 0;
        saveBtn.disabled = persps.length === 0;
    }

    function renderTree() {
        tree.innerHTML = "";
        const md = getMetadata();
        const tblNames = Object.keys(md);
        if (tblNames.length === 0) {
            const empty = document.createElement("div");
            empty.className = "slls-pe-empty";
            empty.textContent = "No tables in this model.";
            tree.appendChild(empty);
            return;
        }
        const q = filterText.trim().toLowerCase();
        let anyVisible = false;
        for (const tblName of tblNames) {
            const data = md[tblName];
            const isHiddenTable = !!data.hidden_table;
            const block = document.createElement("div");
            block.className = "slls-pe-table";
            if (expanded[tblName]) block.classList.add("expanded");

            const tblMatches = !q || tblName.toLowerCase().includes(q);
            let visibleChildren = 0;

            const row = document.createElement("div");
            row.className = "slls-pe-row";
            if (isHiddenTable) row.classList.add("is-hidden");

            const caret = document.createElement("span");
            caret.className = "slls-pe-caret";
            caret.innerHTML = CARET;
            row.appendChild(caret);

            const check = document.createElement("span");
            check.className = "slls-pe-check";
            row.appendChild(check);

            const tblIcon = document.createElement("span");
            tblIcon.className = "slls-pe-icon slls-pe-table-icon";
            tblIcon.innerHTML = ICON_SVG.tables;
            row.appendChild(tblIcon);

            const name = document.createElement("span");
            name.className = "slls-pe-name";
            if (isHiddenTable) name.classList.add("hidden-obj");
            name.textContent = tblName;
            row.appendChild(name);

            const summary = document.createElement("span");
            summary.className = "slls-pe-table-summary";
            row.appendChild(summary);

            const tblDirty = document.createElement("span");
            tblDirty.className = "slls-pe-dirty";
            tblDirty.style.display = "none";
            tblDirty.title = "Unsaved changes";
            row.appendChild(tblDirty);

            block.appendChild(row);

            const childWrap = document.createElement("div");
            childWrap.className = "slls-pe-children";
            block.appendChild(childWrap);

            for (const t of ["columns", "measures", "hierarchies"]) {
                for (const n of (data[t] || [])) {
                    const objHidden = (data[`hidden_${t}`] || []).indexOf(n) >= 0
                        || (t !== "measures" && isHiddenTable);
                    const childRow = document.createElement("div");
                    childRow.className = "slls-pe-row slls-pe-child";
                    if (objHidden) childRow.classList.add("is-hidden");
                    childRow.dataset.type = t;
                    childRow.dataset.name = n;

                    const cb = document.createElement("span");
                    cb.className = "slls-pe-check";
                    childRow.appendChild(cb);

                    const ic = document.createElement("span");
                    ic.className = "slls-pe-icon";
                    ic.innerHTML = ICON_SVG[t];
                    childRow.appendChild(ic);

                    const lbl = document.createElement("span");
                    lbl.className = `slls-pe-name${objHidden ? " hidden-obj" : ""}`;
                    lbl.textContent = n;
                    childRow.appendChild(lbl);

                    const dirtyDot = document.createElement("span");
                    dirtyDot.className = "slls-pe-dirty";
                    dirtyDot.style.display = "none";
                    dirtyDot.title = "Unsaved change";
                    childRow.appendChild(dirtyDot);

                    const matches = tblMatches || n.toLowerCase().includes(q);
                    if (q && !matches) childRow.classList.add("filtered-out");
                    else visibleChildren++;

                    childRow.addEventListener("click", (e) => {
                        e.stopPropagation();
                        if (!selection[tblName]) selection[tblName] = { columns: {}, measures: {}, hierarchies: {} };
                        selection[tblName][t][n] = !selection[tblName][t][n];
                        updateRow();
                    });

                    childWrap.appendChild(childRow);
                }
            }

            if (q && !tblMatches && visibleChildren === 0) {
                block.classList.add("hidden-match");
            } else {
                anyVisible = true;
                if (q && !tblMatches && visibleChildren > 0) {
                    block.classList.add("expanded");
                }
            }

            function updateRow() {
                const st = tableState(tblName);
                check.dataset.state = st;
                const c = tableCounts(tblName);
                summary.textContent =
                    ` ${c.columns[0]}/${c.columns[1]} cols · ${c.measures[0]}/${c.measures[1]} measures · ${c.hierarchies[0]}/${c.hierarchies[1]} hierarchies`;
                tblDirty.style.display = isTableDirty(tblName) ? "inline-block" : "none";
                for (const cr of childWrap.querySelectorAll(".slls-pe-child")) {
                    const tt = cr.dataset.type;
                    const nn = cr.dataset.name;
                    const cbb = cr.querySelector(".slls-pe-check");
                    const dot = cr.querySelector(".slls-pe-dirty");
                    cbb.dataset.state = (selection[tblName] && selection[tblName][tt] && selection[tblName][tt][nn]) ? "all" : "none";
                    if (dot) dot.style.display = isObjDirty(tblName, tt, nn) ? "inline-block" : "none";
                }
                updateGlobalSummary();
                saveBtn.disabled = !hasAnyDirty();
            }

            row.addEventListener("click", (e) => {
                if (e.target === check) return;
                block.classList.toggle("expanded");
                expanded[tblName] = block.classList.contains("expanded");
            });
            check.addEventListener("click", (e) => {
                e.stopPropagation();
                const st = tableState(tblName);
                const turnOn = st !== "all";
                if (!selection[tblName]) selection[tblName] = { columns: {}, measures: {}, hierarchies: {} };
                for (const t of ["columns", "measures", "hierarchies"]) {
                    for (const n of (data[t] || [])) {
                        selection[tblName][t][n] = turnOn;
                    }
                }
                updateRow();
            });

            updateRow();
            tree.appendChild(block);
        }
        if (!anyVisible) {
            const empty = document.createElement("div");
            empty.className = "slls-pe-empty";
            empty.textContent = "No matches.";
            tree.appendChild(empty);
        }
        updateGlobalSummary();
    }

    function deepClone(o) { return JSON.parse(JSON.stringify(o)); }

    function reloadFromModel() {
        const cur = getSelected();
        selection = cur ? buildSelectionFromMembers(cur) : {};
        originalSelection = deepClone(selection);
        renderHeader();
        renderSubtitle();
        renderTree();
    }

    // ============== Actions ==============
    function buildSavePayload() {
        const md = getMetadata();
        const out = [];
        for (const tbl of Object.keys(md)) {
            const sel = selection[tbl] || {};
            const totals = ["columns", "measures", "hierarchies"].reduce(
                (s, t) => s + (md[tbl][t] || []).length, 0);
            let on = 0;
            for (const t of ["columns", "measures", "hierarchies"]) {
                on += Object.values(sel[t] || {}).filter(Boolean).length;
            }
            if (totals > 0 && on === totals) {
                out.push({ table: tbl, type: "table" });
            } else if (on > 0) {
                for (const t of ["columns", "measures", "hierarchies"]) {
                    for (const n of Object.keys(sel[t] || {})) {
                        if (sel[t][n]) out.push({ table: tbl, type: t, name: n });
                    }
                }
            }
        }
        return out;
    }

    function send(action) {
        setBusy(true);
        model.set("pending_action", action);
        model.set("run", (model.get("run") || 0) + 1);
        model.save_changes();
    }

    // ============== Events ==============
    select.addEventListener("change", () => {
        const v = select.value;
        model.set("selected_perspective", v);
        model.save_changes();
        reloadFromModel();
        setStatus("");
    });

    function enterCreate() {
        createRow.classList.add("show");
        select.style.display = "none";
        newBtn.style.display = "none";
        newInput.value = "";
        newInput.focus();
    }

    function exitCreate() {
        createRow.classList.remove("show");
        select.style.display = "";
        newBtn.style.display = "";
        select.value = getSelected() || "";
    }

    newBtn.addEventListener("click", enterCreate);
    cancelBtn.addEventListener("click", exitCreate);

    okBtn.addEventListener("click", () => {
        const name = newInput.value.trim();
        if (!name) {
            setStatus("Please enter a perspective name.", "error");
            return;
        }
        const existing = getPerspectives();
        if (existing.indexOf(name) >= 0) {
            setStatus(`Perspective '${name}' already exists.`, "error");
            return;
        }
        // Create locally only — persistence happens on Save.
        const newList = existing.slice();
        newList.push(name);
        newList.sort();
        const newMembers = Object.assign({}, getMembers());
        newMembers[name] = {};
        pendingNewPerspectives.add(name);
        model.set("perspective_members", newMembers);
        model.set("perspectives", newList);
        model.set("selected_perspective", name);
        model.save_changes();
        exitCreate();
        reloadFromModel();
        setStatus(
            `Perspective '${name}' created. Click Save to persist.`,
            "info"
        );
        saveBtn.disabled = false;
    });

    newInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter") okBtn.click();
        else if (e.key === "Escape") exitCreate();
    });

    deleteBtn.addEventListener("click", () => {
        const cur = getSelected();
        if (!cur) return;
        confirmText.innerHTML = `Delete <b>${escapeHtml(cur)}</b>? This cannot be undone.`;
        confirm.classList.add("show");
    });
    confirmNo.addEventListener("click", () => confirm.classList.remove("show"));
    confirmYes.addEventListener("click", () => {
        confirm.classList.remove("show");
        const cur = getSelected();
        if (!cur) return;
        if (pendingNewPerspectives.has(cur)) {
            // Not persisted yet — delete locally only.
            pendingNewPerspectives.delete(cur);
            const newList = getPerspectives().filter((x) => x !== cur);
            const newMembers = Object.assign({}, getMembers());
            delete newMembers[cur];
            model.set("perspective_members", newMembers);
            model.set("perspectives", newList);
            model.set("selected_perspective", newList.length ? newList[0] : "");
            model.save_changes();
            reloadFromModel();
            setStatus(`Perspective '${cur}' discarded.`, "info");
            return;
        }
        send({ action: "delete", perspective: cur });
    });

    saveBtn.addEventListener("click", () => {
        const cur = getSelected();
        if (!cur) return;
        send({ action: "save", perspective: cur, selected: buildSavePayload() });
    });

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

    // ============== Model observers ==============
    model.on("change:status", () => {
        const s = model.get("status") || {};
        setBusy(false);
        if (s.message) setStatus(s.message, s.kind || "info");
        // Once a save succeeds, the currently selected perspective is
        // persisted on the model, so clear its pending-new flag (if any).
        if ((s.kind || "info") === "success") {
            pendingNewPerspectives.delete(getSelected());
        }
        exitCreate();
    });
    model.on("change:perspectives", reloadFromModel);
    model.on("change:perspective_members", reloadFromModel);
    model.on("change:selected_perspective", reloadFromModel);

    function escapeHtml(s) {
        return String(s).replace(/[&<>"']/g, (c) => ({
            "&": "&amp;", "<": "&lt;", ">": "&gt;",
            '"': "&quot;", "'": "&#39;"
        }[c]));
    }

    reloadFromModel();
}
export default { render };
"""


@log
def perspective_editor(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    dark_mode: bool = False,
):
    """
    Generates an interactive editor for managing perspectives within a semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    dark_mode : bool, default=False
        If True, renders the editor with a dark color theme. If False, renders
        with a light color theme.
    """

    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "The 'perspective_editor' function requires the 'anywidget' package. "
            "Install it with: pip install anywidget"
        ) from e

    from IPython.display import display

    # -----------------------------
    # LOAD MODEL METADATA
    # -----------------------------
    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=True
    ) as tom:

        dataset_name = getattr(tom, "_dataset_name", "") or ""
        workspace_name = getattr(tom, "_workspace_name", "") or ""

        metadata = {}
        for table in tom.model.Tables:
            columns = sorted([c.Name for c in tom.all_columns() if c.Parent == table])
            measures = sorted([m.Name for m in table.Measures])
            hierarchies = sorted([h.Name for h in table.Hierarchies])
            hidden_columns = [
                c.Name
                for c in tom.all_columns()
                if c.Parent == table and c.IsHidden
            ]
            hidden_measures = [m.Name for m in table.Measures if m.IsHidden]
            hidden_hierarchies = [h.Name for h in table.Hierarchies if h.IsHidden]
            metadata[table.Name] = {
                "columns": columns,
                "measures": measures,
                "hierarchies": hierarchies,
                "hidden_table": bool(table.IsHidden),
                "hidden_columns": hidden_columns,
                "hidden_measures": hidden_measures,
                "hidden_hierarchies": hidden_hierarchies,
            }

        perspectives_list = sorted(p.Name for p in tom.model.Perspectives)

        perspective_members = {}
        for p in tom.model.Perspectives:
            members = {}
            for pt in p.PerspectiveTables:
                tbl = pt.Table.Name
                members[tbl] = {
                    "columns": sorted(pc.Column.Name for pc in pt.PerspectiveColumns),
                    "measures": sorted(pm.Measure.Name for pm in pt.PerspectiveMeasures),
                    "hierarchies": sorted(
                        ph.Hierarchy.Name for ph in pt.PerspectiveHierarchies
                    ),
                }
            perspective_members[p.Name] = members

    class PerspectiveEditorWidget(anywidget.AnyWidget):
        _esm = _WIDGET_JS
        _css = _WIDGET_CSS

        metadata = traitlets.Dict().tag(sync=True)
        perspectives = traitlets.List().tag(sync=True)
        perspective_members = traitlets.Dict().tag(sync=True)
        selected_perspective = traitlets.Unicode("").tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        dataset_name = traitlets.Unicode("").tag(sync=True)
        workspace_name = traitlets.Unicode("").tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)

    widget = PerspectiveEditorWidget(
        metadata=metadata,
        perspectives=perspectives_list,
        perspective_members=perspective_members,
        selected_perspective=perspectives_list[0] if perspectives_list else "",
        status={},
        pending_action={},
        run=0,
        dataset_name=dataset_name,
        workspace_name=workspace_name,
        dark_mode=bool(dark_mode),
    )

    def _membership_for_save(selected):
        new_members = {}
        for s in selected:
            tbl = s.get("table")
            if tbl is None:
                continue
            if tbl not in new_members:
                new_members[tbl] = {
                    "columns": [],
                    "measures": [],
                    "hierarchies": [],
                }
            if s.get("type") == "table":
                new_members[tbl] = {
                    "columns": list(metadata[tbl]["columns"]),
                    "measures": list(metadata[tbl]["measures"]),
                    "hierarchies": list(metadata[tbl]["hierarchies"]),
                }
            elif s.get("type") in ("columns", "measures", "hierarchies"):
                if s["name"] not in new_members[tbl][s["type"]]:
                    new_members[tbl][s["type"]].append(s["name"])
        return new_members

    def _on_run(change):
        data = dict(widget.pending_action or {})
        action = data.get("action")
        if not action:
            return
        try:
            with connect_semantic_model(
                dataset=dataset, workspace=workspace, readonly=False
            ) as tom:
                perspective_name = data.get("perspective")

                if action == "create":
                    if perspective_name in widget.perspectives:
                        widget.status = {
                            "message": f"Perspective '{perspective_name}' already exists.",
                            "kind": "error",
                        }
                        return
                    tom.add_perspective(perspective_name)
                    tom.model.SaveChanges()
                    new_list = sorted(list(widget.perspectives) + [perspective_name])
                    new_members = dict(widget.perspective_members)
                    new_members[perspective_name] = {}
                    widget.perspective_members = new_members
                    widget.perspectives = new_list
                    widget.selected_perspective = perspective_name
                    widget.status = {
                        "message": f"Created perspective '{perspective_name}'.",
                        "kind": "success",
                    }

                elif action == "delete":
                    obj = tom.model.Perspectives[perspective_name]
                    tom.remove_object(object=obj)
                    tom.model.SaveChanges()
                    new_list = [
                        x for x in widget.perspectives if x != perspective_name
                    ]
                    new_members = dict(widget.perspective_members)
                    new_members.pop(perspective_name, None)
                    widget.perspective_members = new_members
                    widget.perspectives = new_list
                    widget.selected_perspective = new_list[0] if new_list else ""
                    widget.status = {
                        "message": f"Deleted perspective '{perspective_name}'.",
                        "kind": "success",
                    }

                elif action == "save":
                    found = tom.model.Perspectives.Find(perspective_name)
                    if not found:
                        tom.add_perspective(perspective_name)
                    p = tom.model.Perspectives[perspective_name]
                    p.PerspectiveTables.Clear()

                    selected = data.get("selected", []) or []
                    whole_tables = {
                        s["table"] for s in selected if s.get("type") == "table"
                    }
                    for t in whole_tables:
                        tom.add_to_perspective(
                            object=tom.model.Tables[t],
                            perspective_name=perspective_name,
                        )
                    for s in selected:
                        if s.get("type") == "table":
                            continue
                        table = s.get("table")
                        obj_name = s.get("name")
                        obj_type = s.get("type")
                        if obj_type == "columns":
                            obj = tom.model.Tables[table].Columns[obj_name]
                        elif obj_type == "measures":
                            obj = tom.model.Tables[table].Measures[obj_name]
                        elif obj_type == "hierarchies":
                            obj = tom.model.Tables[table].Hierarchies[obj_name]
                        else:
                            continue
                        tom.add_to_perspective(
                            object=obj, perspective_name=perspective_name
                        )

                    tom.model.SaveChanges()

                    new_members = dict(widget.perspective_members)
                    new_members[perspective_name] = _membership_for_save(selected)
                    widget.perspective_members = new_members
                    if perspective_name not in widget.perspectives:
                        widget.perspectives = sorted(
                            list(widget.perspectives) + [perspective_name]
                        )
                    widget.status = {
                        "message": f"Saved perspective '{perspective_name}'.",
                        "kind": "success",
                    }

        except Exception as e:
            widget.status = {"message": f"Error: {e}", "kind": "error"}

    widget.observe(_on_run, names=["run"])

    # Keep a reference on the widget so the Python-side observer is not garbage
    # collected after this function returns. We intentionally do NOT return the
    # widget to avoid Jupyter auto-displaying it a second time after `display()`.
    display(widget)
