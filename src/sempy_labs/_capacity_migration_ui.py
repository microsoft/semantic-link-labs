from typing import Optional
from sempy._utils._log import log


_WIDGET_CSS = """
.slls-cm {
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
    max-width: 920px;
    background: var(--slls-bg-solid);
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    box-shadow: var(--slls-shadow);
    padding: 24px;
    box-sizing: border-box;
}
@media (prefers-color-scheme: dark) {
    .slls-cm.slls-cm-auto {
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
.slls-cm.slls-cm-dark {
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
.slls-cm * { box-sizing: border-box; }

.slls-cm-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 18px;
    flex-wrap: wrap;
}
.slls-cm-titlewrap {
    display: flex;
    flex-direction: column;
    margin-right: auto;
    min-width: 0;
}
.slls-cm-title {
    font-size: 22px;
    font-weight: 600;
    letter-spacing: -0.01em;
    line-height: 1.15;
}
.slls-cm-subtitle {
    font-size: 12px;
    color: var(--slls-text-secondary);
    margin-top: 2px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 600px;
}
.slls-cm-subtitle .slls-cm-sep {
    color: var(--slls-text-tertiary);
    margin: 0 6px;
}
.slls-cm-subtitle b {
    color: var(--slls-text);
    font-weight: 500;
}

.slls-cm-btn {
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
.slls-cm-btn:hover { background: var(--slls-surface-2); border-color: var(--slls-text-tertiary); }
.slls-cm-btn:active { transform: scale(0.97); }
.slls-cm-btn:disabled { opacity: 0.4; cursor: not-allowed; }
.slls-cm-btn-primary {
    background: var(--slls-accent);
    border-color: var(--slls-accent);
    color: #fff;
}
.slls-cm-btn-primary:hover { background: var(--slls-accent-hover); border-color: var(--slls-accent-hover); }
.slls-cm-btn-icon {
    width: 32px; height: 32px;
    padding: 0;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border-radius: 50%;
    font-size: 18px;
    line-height: 1;
}

.slls-cm-tabs {
    display: flex;
    gap: 4px;
    padding: 4px;
    background: var(--slls-surface-2);
    border: 1px solid var(--slls-border);
    border-radius: 999px;
    margin-bottom: 18px;
    width: fit-content;
}
.slls-cm-tab {
    appearance: none;
    border: none;
    background: transparent;
    color: var(--slls-text-secondary);
    font-family: inherit;
    font-size: 13px;
    font-weight: 500;
    padding: 7px 16px;
    border-radius: 999px;
    cursor: pointer;
    transition: background 120ms ease, color 120ms ease;
}
.slls-cm-tab:hover { color: var(--slls-text); }
.slls-cm-tab.active {
    background: var(--slls-bg-solid);
    color: var(--slls-text);
    box-shadow: 0 1px 2px rgba(0,0,0,0.08);
}

.slls-cm-section {
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    background: var(--slls-surface);
    padding: 16px 18px;
    margin-bottom: 14px;
}
.slls-cm-section-title {
    font-size: 13px;
    font-weight: 600;
    color: var(--slls-text);
    margin-bottom: 12px;
    letter-spacing: -0.005em;
}

.slls-cm-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 12px 16px;
}
.slls-cm-grid.slls-cm-grid-1 { grid-template-columns: 1fr; }
.slls-cm-field { display: flex; flex-direction: column; gap: 4px; }
.slls-cm-label {
    font-size: 12px;
    color: var(--slls-text-secondary);
    font-weight: 500;
}
.slls-cm-input,
.slls-cm-select {
    appearance: none;
    -webkit-appearance: none;
    background: var(--slls-bg-solid);
    border: 1px solid var(--slls-border-strong);
    border-radius: var(--slls-radius-sm);
    padding: 8px 12px;
    font-size: 13.5px;
    color: var(--slls-text);
    font-family: inherit;
    transition: border-color 120ms ease, box-shadow 120ms ease;
    width: 100%;
}
.slls-cm-select {
    padding-right: 32px;
    background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6'><path fill='%236e6e73' d='M0 0l5 6 5-6z'/></svg>");
    background-repeat: no-repeat;
    background-position: right 12px center;
    cursor: pointer;
}
.slls-cm-input::placeholder { color: var(--slls-text-tertiary); }
.slls-cm-input:focus,
.slls-cm-select:focus {
    outline: none;
    border-color: var(--slls-accent);
    box-shadow: 0 0 0 3px var(--slls-accent-soft);
}
.slls-cm-input[type="password"] { letter-spacing: 0.1em; }

.slls-cm-radio-row {
    display: flex;
    gap: 16px;
    flex-wrap: wrap;
    margin-bottom: 12px;
}
.slls-cm-radio {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    font-size: 13.5px;
    color: var(--slls-text);
    cursor: pointer;
    user-select: none;
}
.slls-cm-radio input { accent-color: var(--slls-accent); }

.slls-cm-checkbox {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    font-size: 13.5px;
    color: var(--slls-text);
    cursor: pointer;
    user-select: none;
    margin-top: 8px;
}
.slls-cm-checkbox input { accent-color: var(--slls-accent); }

.slls-cm-info {
    margin-top: 10px;
    padding: 10px 12px;
    border-radius: var(--slls-radius-sm);
    background: var(--slls-accent-soft);
    color: var(--slls-text);
    font-size: 12.5px;
    line-height: 1.5;
    display: none;
}
.slls-cm-info.show { display: block; }
.slls-cm-info b { color: var(--slls-text); font-weight: 600; }
.slls-cm-info-kv {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: 4px 12px;
    margin-top: 4px;
}
.slls-cm-info-kv .k { color: var(--slls-text-secondary); }
.slls-cm-info-kv .v { color: var(--slls-text); font-weight: 500; word-break: break-word; }

.slls-cm-warn {
    margin-top: 10px;
    padding: 10px 12px;
    border-radius: var(--slls-radius-sm);
    background: var(--slls-orange-soft);
    color: var(--slls-text);
    font-size: 12.5px;
    line-height: 1.5;
    border-left: 3px solid var(--slls-orange);
    display: none;
}
.slls-cm-warn.show { display: block; }

.slls-cm-list {
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius-sm);
    background: var(--slls-bg-solid);
    max-height: 280px;
    overflow-y: auto;
}
.slls-cm-list-empty {
    padding: 24px 16px;
    text-align: center;
    color: var(--slls-text-tertiary);
    font-size: 13px;
}
.slls-cm-list-item {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 9px 14px;
    border-bottom: 1px solid var(--slls-border);
    cursor: pointer;
    transition: background 100ms ease;
}
.slls-cm-list-item:last-child { border-bottom: none; }
.slls-cm-list-item:hover { background: var(--slls-surface-2); }
.slls-cm-list-item input { accent-color: var(--slls-accent); flex-shrink: 0; }
.slls-cm-list-name {
    font-size: 13.5px;
    font-weight: 500;
    color: var(--slls-text);
    margin-right: auto;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
.slls-cm-badge {
    display: inline-block;
    padding: 2px 8px;
    font-size: 11px;
    font-weight: 600;
    border-radius: 999px;
    background: var(--slls-surface-2);
    color: var(--slls-text-secondary);
    border: 1px solid var(--slls-border);
    margin-left: 6px;
}
.slls-cm-badge.sku { background: var(--slls-accent-soft); color: var(--slls-accent); border-color: transparent; }
.slls-cm-badge.region { background: var(--slls-surface-2); }

.slls-cm-list-toolbar {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 12px;
    border-bottom: 1px solid var(--slls-border);
    background: var(--slls-surface-2);
    font-size: 12.5px;
    color: var(--slls-text-secondary);
}
.slls-cm-list-toolbar .slls-cm-spacer { flex: 1; }
.slls-cm-link {
    appearance: none;
    background: transparent;
    border: none;
    color: var(--slls-accent);
    font-family: inherit;
    font-size: 12.5px;
    cursor: pointer;
    padding: 2px 6px;
}
.slls-cm-link:hover { text-decoration: underline; }

.slls-cm-footer {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-top: 18px;
    flex-wrap: wrap;
}
.slls-cm-footer .slls-cm-spacer { flex: 1; }

.slls-cm-status {
    margin-top: 14px;
    padding: 10px 14px;
    border-radius: var(--slls-radius-sm);
    font-size: 13.5px;
    display: none;
    white-space: pre-wrap;
    max-height: 280px;
    overflow-y: auto;
}
.slls-cm-status.show { display: block; animation: slls-cm-fade-in 200ms ease; }
.slls-cm-status.success { background: var(--slls-success-soft); color: var(--slls-success); }
.slls-cm-status.error { background: var(--slls-danger-soft); color: var(--slls-danger); }
.slls-cm-status.info { background: var(--slls-accent-soft); color: var(--slls-accent); }
@keyframes slls-cm-fade-in {
    from { opacity: 0; transform: translateY(-4px); }
    to { opacity: 1; transform: translateY(0); }
}

.slls-cm-confirm {
    margin-top: 14px;
    padding: 12px 16px;
    border-radius: var(--slls-radius-sm);
    border: 1px solid var(--slls-orange);
    background: var(--slls-orange-soft);
    display: none;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
}
.slls-cm-confirm.show { display: flex; }
.slls-cm-confirm-text { flex: 1; font-size: 13.5px; color: var(--slls-text); }

.slls-cm-busy {
    pointer-events: none;
    opacity: 0.6;
    transition: opacity 120ms ease;
}

.slls-cm-spinner {
    display: inline-block;
    width: 14px;
    height: 14px;
    border: 2px solid var(--slls-border-strong);
    border-top-color: var(--slls-accent);
    border-radius: 50%;
    animation: slls-cm-spin 0.8s linear infinite;
    vertical-align: middle;
    margin-right: 8px;
}
@keyframes slls-cm-spin {
    to { transform: rotate(360deg); }
}

.slls-cm-attribution {
    margin-top: 18px;
    text-align: right;
    font-size: 11.5px;
    color: var(--slls-text-tertiary);
}
.slls-cm-attribution a {
    color: var(--slls-text-tertiary);
    text-decoration: none;
    transition: color 120ms ease;
}
.slls-cm-attribution a:hover {
    color: var(--slls-accent);
    text-decoration: none;
}

.slls-cm-hidden { display: none !important; }
"""


_WIDGET_JS = r"""
function render({ model, el }) {
    const root = document.createElement("div");
    root.className = "slls-cm";
    function applyTheme() {
        root.classList.remove("slls-cm-dark", "slls-cm-auto");
        const dm = model.get("dark_mode");
        if (dm === true) root.classList.add("slls-cm-dark");
        else if (dm === null || dm === undefined) root.classList.add("slls-cm-auto");
    }
    applyTheme();
    model.on("change:dark_mode", applyTheme);
    el.appendChild(root);

    function escapeHtml(s) {
        return String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({
            "&": "&amp;", "<": "&lt;", ">": "&gt;",
            '"': "&quot;", "'": "&#39;"
        }[c]));
    }

    function getCapacities() { return model.get("capacities") || []; }
    function getSkuMapping() { return model.get("sku_mapping") || {}; }
    function getSuffix() { return model.get("fsku_suffix") || "fsku"; }
    function getSourceCaps() {
        return getCapacities().filter(c =>
            (c.Sku || "").startsWith("P") ||
            ((c.Sku || "").startsWith("A") && !(c.Sku || "").startsWith("AP"))
        ).filter(c => !((c.Sku || "").startsWith("PP")));
    }
    function getFSkuCaps() {
        return getCapacities().filter(c => (c.Sku || "").startsWith("F"));
    }

    // ----------- Header -----------
    const header = document.createElement("div");
    header.className = "slls-cm-header";
    root.appendChild(header);

    const titleWrap = document.createElement("div");
    titleWrap.className = "slls-cm-titlewrap";
    header.appendChild(titleWrap);

    const title = document.createElement("div");
    title.className = "slls-cm-title";
    title.textContent = "Capacity Migration";
    titleWrap.appendChild(title);

    const subtitle = document.createElement("div");
    subtitle.className = "slls-cm-subtitle";
    titleWrap.appendChild(subtitle);
    function renderSubtitle() {
        const caps = getCapacities();
        const sourceN = getSourceCaps().length;
        const fN = getFSkuCaps().length;
        subtitle.innerHTML =
            `<b>${caps.length}</b> capacities` +
            `<span class="slls-cm-sep">·</span>` +
            `${sourceN} P/A SKU${sourceN === 1 ? "" : "s"}` +
            `<span class="slls-cm-sep">·</span>` +
            `${fN} F SKU${fN === 1 ? "" : "s"}`;
    }

    const SUN_SVG = `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="8" cy="8" r="3"/><path d="M8 1.5v1.5M8 13v1.5M1.5 8h1.5M13 8h1.5M3.3 3.3l1.05 1.05M11.65 11.65l1.05 1.05M3.3 12.7l1.05-1.05M11.65 4.35l1.05-1.05"/></svg>`;
    const MOON_SVG = `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M13.5 9.5A5.5 5.5 0 0 1 6.5 2.5a5.5 5.5 0 1 0 7 7z"/></svg>`;
    const REFRESH_SVG = `<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M2.5 8a5.5 5.5 0 0 1 9.5-3.8M13.5 8a5.5 5.5 0 0 1-9.5 3.8"/><path d="M12 2v3h-3M4 14v-3h3"/></svg>`;

    const refreshBtn = document.createElement("button");
    refreshBtn.className = "slls-cm-btn slls-cm-btn-icon";
    refreshBtn.type = "button";
    refreshBtn.innerHTML = REFRESH_SVG;
    refreshBtn.title = "Refresh capacities";
    refreshBtn.setAttribute("aria-label", "Refresh capacities");
    refreshBtn.addEventListener("click", () => {
        send({ action: "refresh" });
    });
    header.appendChild(refreshBtn);

    const themeBtn = document.createElement("button");
    themeBtn.className = "slls-cm-btn slls-cm-btn-icon";
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

    // ----------- Tabs -----------
    const tabs = document.createElement("div");
    tabs.className = "slls-cm-tabs";
    root.appendChild(tabs);

    const TAB_LIST = [
        { id: "single", label: "Single capacity" },
        { id: "multiple", label: "Multiple capacities" },
        { id: "all", label: "All P/A capacities" },
    ];
    let activeTab = "single";
    const tabBtns = {};
    for (const t of TAB_LIST) {
        const b = document.createElement("button");
        b.className = "slls-cm-tab";
        b.type = "button";
        b.textContent = t.label;
        b.addEventListener("click", () => setActiveTab(t.id));
        tabs.appendChild(b);
        tabBtns[t.id] = b;
    }
    function setActiveTab(id) {
        activeTab = id;
        for (const t of TAB_LIST) {
            tabBtns[t.id].classList.toggle("active", t.id === id);
        }
        panels.single.classList.toggle("slls-cm-hidden", id !== "single");
        panels.multiple.classList.toggle("slls-cm-hidden", id !== "multiple");
        panels.all.classList.toggle("slls-cm-hidden", id !== "all");
        renderActionButton();
        setStatus("");
    }

    // ----------- Common: Azure section -----------
    const azureSection = document.createElement("div");
    azureSection.className = "slls-cm-section";
    azureSection.innerHTML = `
        <div class="slls-cm-section-title">Azure</div>
        <div class="slls-cm-grid">
            <div class="slls-cm-field">
                <label class="slls-cm-label" for="slls-cm-subId">Subscription ID</label>
                <input id="slls-cm-subId" class="slls-cm-input" type="text" placeholder="00000000-0000-0000-0000-000000000000"/>
            </div>
            <div class="slls-cm-field">
                <label class="slls-cm-label" for="slls-cm-rg">Resource Group</label>
                <input id="slls-cm-rg" class="slls-cm-input" type="text" placeholder="my-resource-group"/>
            </div>
        </div>
    `;
    root.appendChild(azureSection);
    const subIdInput = azureSection.querySelector("#slls-cm-subId");
    const rgInput = azureSection.querySelector("#slls-cm-rg");
    subIdInput.value = model.get("azure_subscription_id") || "";
    rgInput.value = model.get("resource_group") || "";

    // ----------- Service Principal section -----------
    const spSection = document.createElement("div");
    spSection.className = "slls-cm-section";
    spSection.innerHTML = `
        <div class="slls-cm-section-title">
            <label class="slls-cm-checkbox" style="margin: 0;">
                <input id="slls-cm-spEnable" type="checkbox"/>
                <span>Use Service Principal Authentication</span>
            </label>
        </div>
        <div id="slls-cm-spBody" class="slls-cm-hidden">
            <div class="slls-cm-grid">
                <div class="slls-cm-field">
                    <label class="slls-cm-label" for="slls-cm-kvUri">Key Vault URI</label>
                    <input id="slls-cm-kvUri" class="slls-cm-input" type="text" placeholder="https://my-vault.vault.azure.net/"/>
                </div>
                <div class="slls-cm-field">
                    <label class="slls-cm-label" for="slls-cm-kvTenant">Tenant ID secret name</label>
                    <input id="slls-cm-kvTenant" class="slls-cm-input" type="text" placeholder="tenant-id-secret"/>
                </div>
                <div class="slls-cm-field">
                    <label class="slls-cm-label" for="slls-cm-kvClient">Client ID secret name</label>
                    <input id="slls-cm-kvClient" class="slls-cm-input" type="text" placeholder="client-id-secret"/>
                </div>
                <div class="slls-cm-field">
                    <label class="slls-cm-label" for="slls-cm-kvSecret">Client Secret name</label>
                    <input id="slls-cm-kvSecret" class="slls-cm-input" type="text" placeholder="client-secret-secret"/>
                </div>
            </div>
        </div>
    `;
    root.appendChild(spSection);
    const spEnable = spSection.querySelector("#slls-cm-spEnable");
    const spBody = spSection.querySelector("#slls-cm-spBody");
    const kvUri = spSection.querySelector("#slls-cm-kvUri");
    const kvTenant = spSection.querySelector("#slls-cm-kvTenant");
    const kvClient = spSection.querySelector("#slls-cm-kvClient");
    const kvSecret = spSection.querySelector("#slls-cm-kvSecret");
    kvUri.value = model.get("key_vault_uri") || "";
    kvTenant.value = model.get("key_vault_tenant_id") || "";
    kvClient.value = model.get("key_vault_client_id") || "";
    kvSecret.value = model.get("key_vault_client_secret") || "";
    spEnable.checked = !!(kvUri.value && kvTenant.value && kvClient.value && kvSecret.value);
    spBody.classList.toggle("slls-cm-hidden", !spEnable.checked);
    spEnable.addEventListener("change", () => {
        spBody.classList.toggle("slls-cm-hidden", !spEnable.checked);
    });

    // ----------- Panels container -----------
    const panels = {};

    // ----------- SINGLE panel -----------
    panels.single = document.createElement("div");
    panels.single.className = "slls-cm-section";
    panels.single.innerHTML = `
        <div class="slls-cm-section-title">Single capacity migration</div>
        <div class="slls-cm-grid slls-cm-grid-1">
            <div class="slls-cm-field">
                <label class="slls-cm-label" for="slls-cm-src">Source capacity (P or A SKU)</label>
                <select id="slls-cm-src" class="slls-cm-select"></select>
            </div>
        </div>
        <div class="slls-cm-radio-row" style="margin-top: 14px;">
            <label class="slls-cm-radio">
                <input type="radio" name="slls-cm-tgt-mode" value="create" checked/>
                <span>Create new F SKU</span>
            </label>
            <label class="slls-cm-radio">
                <input type="radio" name="slls-cm-tgt-mode" value="existing"/>
                <span>Use existing F SKU</span>
            </label>
        </div>
        <div id="slls-cm-singleCreate" class="slls-cm-info show">
            <div>A new F SKU will be created using the parameters below:</div>
            <div class="slls-cm-info-kv" id="slls-cm-singlePreview"></div>
        </div>
        <div id="slls-cm-singleExisting" class="slls-cm-hidden">
            <div class="slls-cm-grid slls-cm-grid-1">
                <div class="slls-cm-field">
                    <label class="slls-cm-label" for="slls-cm-tgt">Target capacity (existing F SKU)</label>
                    <select id="slls-cm-tgt" class="slls-cm-select"></select>
                </div>
            </div>
            <div id="slls-cm-singleExistingWarn" class="slls-cm-warn">
                Cross-region migrations are not supported. Only F SKU capacities in the same region as the source are shown.
            </div>
        </div>
    `;
    root.appendChild(panels.single);

    const srcSelect = panels.single.querySelector("#slls-cm-src");
    const singleCreate = panels.single.querySelector("#slls-cm-singleCreate");
    const singleExisting = panels.single.querySelector("#slls-cm-singleExisting");
    const singlePreview = panels.single.querySelector("#slls-cm-singlePreview");
    const tgtSelect = panels.single.querySelector("#slls-cm-tgt");
    const singleExistingWarn = panels.single.querySelector("#slls-cm-singleExistingWarn");
    let tgtMode = "create";

    panels.single.querySelectorAll('input[name="slls-cm-tgt-mode"]').forEach((r) => {
        r.addEventListener("change", () => {
            tgtMode = r.value;
            singleCreate.classList.toggle("show", tgtMode === "create");
            singleCreate.classList.toggle("slls-cm-hidden", tgtMode !== "create");
            singleExisting.classList.toggle("slls-cm-hidden", tgtMode !== "existing");
            refreshTargets();
            renderActionButton();
        });
    });

    srcSelect.addEventListener("change", () => {
        refreshSinglePreview();
        refreshTargets();
        renderActionButton();
    });
    tgtSelect.addEventListener("change", renderActionButton);

    function refreshSinglePreview() {
        const src = srcSelect.value;
        const cap = getSourceCaps().find(c => c.Name === src);
        if (!cap) { singlePreview.innerHTML = ""; return; }
        const skuMap = getSkuMapping();
        const fSku = skuMap[cap.Sku] || "?";
        const suffix = getSuffix();
        const tgtName =
            (cap.Name || "").toLowerCase().replace(/[^a-z0-9]/g, "") + suffix;
        const exists = getFSkuCaps().some(c => (c.Name || "").toLowerCase() === tgtName.toLowerCase());
        const admins = Array.isArray(cap.Admins) ? cap.Admins : [];
        singlePreview.innerHTML = `
            <div class="k">Source SKU</div><div class="v">${escapeHtml(cap.Sku)} <span class="slls-cm-badge region">${escapeHtml(cap.Region || "")}</span></div>
            <div class="k">Target F SKU</div><div class="v"><span class="slls-cm-badge sku">${escapeHtml(fSku)}</span></div>
            <div class="k">Target name</div><div class="v">${escapeHtml(tgtName)}${exists ? ' <span class="slls-cm-badge">already exists — workspaces will be reassigned</span>' : ""}</div>
            <div class="k">Region</div><div class="v">${escapeHtml(cap.Region || "")} <span class="slls-cm-badge">same region</span></div>
            <div class="k">Admins</div><div class="v">${admins.length ? admins.map(escapeHtml).join(", ") : '<i style="opacity:0.6;">(inherited from source)</i>'}</div>
        `;
    }

    function refreshTargets() {
        const src = srcSelect.value;
        const cap = getSourceCaps().find(c => c.Name === src);
        const srcRegion = cap ? cap.Region : null;
        tgtSelect.innerHTML = "";
        const fCaps = getFSkuCaps();
        const sameRegion = fCaps.filter(c => !srcRegion || c.Region === srcRegion);
        if (sameRegion.length === 0) {
            const opt = document.createElement("option");
            opt.value = "";
            opt.textContent = srcRegion
                ? `No F SKU capacities in region '${srcRegion}'`
                : "No F SKU capacities available";
            opt.disabled = true;
            opt.selected = true;
            tgtSelect.appendChild(opt);
        } else {
            for (const c of sameRegion) {
                const opt = document.createElement("option");
                opt.value = c.Name;
                opt.textContent = `${c.Name}  —  ${c.Sku}  ·  ${c.Region}`;
                tgtSelect.appendChild(opt);
            }
        }
        const hidden = fCaps.length - sameRegion.length;
        singleExistingWarn.classList.toggle(
            "show",
            tgtMode === "existing" && hidden > 0,
        );
    }

    // ----------- MULTIPLE panel -----------
    panels.multiple = document.createElement("div");
    panels.multiple.className = "slls-cm-section slls-cm-hidden";
    panels.multiple.innerHTML = `
        <div class="slls-cm-section-title">Multiple capacities migration</div>
        <div class="slls-cm-list">
            <div class="slls-cm-list-toolbar">
                <span id="slls-cm-multiCount">0 selected</span>
                <div class="slls-cm-spacer"></div>
                <button type="button" class="slls-cm-link" id="slls-cm-multiAll">Select all</button>
                <button type="button" class="slls-cm-link" id="slls-cm-multiNone">Clear</button>
            </div>
            <div id="slls-cm-multiList"></div>
        </div>
        <label class="slls-cm-checkbox">
            <input type="checkbox" id="slls-cm-multiPSkuOnly" checked/>
            <span>Migrate P SKUs only (uncheck to also include A SKUs)</span>
        </label>
    `;
    root.appendChild(panels.multiple);

    const multiList = panels.multiple.querySelector("#slls-cm-multiList");
    const multiCount = panels.multiple.querySelector("#slls-cm-multiCount");
    const multiPSkuOnly = panels.multiple.querySelector("#slls-cm-multiPSkuOnly");
    const multiSelectAll = panels.multiple.querySelector("#slls-cm-multiAll");
    const multiSelectNone = panels.multiple.querySelector("#slls-cm-multiNone");

    function getMultiList() {
        const sourceCaps = getSourceCaps();
        return multiPSkuOnly.checked
            ? sourceCaps.filter(c => (c.Sku || "").startsWith("P"))
            : sourceCaps;
    }

    function renderMultiList() {
        const caps = getMultiList();
        const selected = getMultiSelected();
        multiList.innerHTML = "";
        if (caps.length === 0) {
            const empty = document.createElement("div");
            empty.className = "slls-cm-list-empty";
            empty.textContent = "No matching capacities found.";
            multiList.appendChild(empty);
        } else {
            for (const c of caps) {
                const row = document.createElement("label");
                row.className = "slls-cm-list-item";
                row.innerHTML = `
                    <input type="checkbox" data-name="${escapeHtml(c.Name)}" ${selected.has(c.Name) ? "checked" : ""}/>
                    <span class="slls-cm-list-name">${escapeHtml(c.Name)}</span>
                    <span class="slls-cm-badge sku">${escapeHtml(c.Sku)}</span>
                    <span class="slls-cm-badge region">${escapeHtml(c.Region || "")}</span>
                `;
                row.querySelector("input").addEventListener("change", (e) => {
                    if (e.target.checked) multiSelection.add(c.Name);
                    else multiSelection.delete(c.Name);
                    updateMultiCount();
                    renderActionButton();
                });
                multiList.appendChild(row);
            }
        }
        updateMultiCount();
    }
    const multiSelection = new Set();
    function getMultiSelected() {
        // Restrict to currently-visible items
        const visible = new Set(getMultiList().map(c => c.Name));
        const filtered = new Set();
        for (const n of multiSelection) if (visible.has(n)) filtered.add(n);
        return filtered;
    }
    function updateMultiCount() {
        const sel = getMultiSelected();
        multiCount.textContent = `${sel.size} selected`;
    }

    multiSelectAll.addEventListener("click", () => {
        for (const c of getMultiList()) multiSelection.add(c.Name);
        renderMultiList();
        renderActionButton();
    });
    multiSelectNone.addEventListener("click", () => {
        for (const c of getMultiList()) multiSelection.delete(c.Name);
        renderMultiList();
        renderActionButton();
    });
    multiPSkuOnly.addEventListener("change", () => {
        renderMultiList();
        renderActionButton();
    });

    // ----------- ALL panel -----------
    panels.all = document.createElement("div");
    panels.all.className = "slls-cm-section slls-cm-hidden";
    panels.all.innerHTML = `
        <div class="slls-cm-section-title">Migrate all P/A capacities</div>
        <div class="slls-cm-info show" id="slls-cm-allInfo"></div>
        <label class="slls-cm-checkbox">
            <input type="checkbox" id="slls-cm-allPSkuOnly" checked/>
            <span>Migrate P SKUs only (uncheck to also include A SKUs)</span>
        </label>
    `;
    root.appendChild(panels.all);

    const allInfo = panels.all.querySelector("#slls-cm-allInfo");
    const allPSkuOnly = panels.all.querySelector("#slls-cm-allPSkuOnly");

    function refreshAllInfo() {
        const sourceCaps = getSourceCaps();
        const caps = allPSkuOnly.checked
            ? sourceCaps.filter(c => (c.Sku || "").startsWith("P"))
            : sourceCaps;
        const byRegion = {};
        for (const c of caps) {
            byRegion[c.Region || "(unknown)"] = (byRegion[c.Region || "(unknown)"] || 0) + 1;
        }
        const regionStr = Object.keys(byRegion)
            .sort()
            .map(r => `${r} (${byRegion[r]})`)
            .join(", ");
        allInfo.innerHTML = `
            <div><b>${caps.length}</b> capacit${caps.length === 1 ? "y" : "ies"} will be migrated to F SKU equivalents.</div>
            <div class="slls-cm-info-kv">
                <div class="k">Regions</div><div class="v">${escapeHtml(regionStr || "—")}</div>
                <div class="k">F SKU names</div><div class="v">Auto-generated as <i>&lt;alphanumeric-lowercase&gt;${escapeHtml(getSuffix())}</i></div>
            </div>
        `;
    }

    allPSkuOnly.addEventListener("change", () => {
        refreshAllInfo();
        renderActionButton();
    });

    // ----------- Footer -----------
    const confirm = document.createElement("div");
    confirm.className = "slls-cm-confirm";
    const confirmText = document.createElement("div");
    confirmText.className = "slls-cm-confirm-text";
    const confirmYes = document.createElement("button");
    confirmYes.className = "slls-cm-btn slls-cm-btn-primary";
    confirmYes.textContent = "Yes, migrate";
    const confirmNo = document.createElement("button");
    confirmNo.className = "slls-cm-btn";
    confirmNo.textContent = "Cancel";
    confirm.appendChild(confirmText);
    confirm.appendChild(confirmYes);
    confirm.appendChild(confirmNo);
    root.appendChild(confirm);

    const footer = document.createElement("div");
    footer.className = "slls-cm-footer";
    root.appendChild(footer);
    const spacer = document.createElement("div");
    spacer.className = "slls-cm-spacer";
    footer.appendChild(spacer);

    const migrateBtn = document.createElement("button");
    migrateBtn.className = "slls-cm-btn slls-cm-btn-primary";
    migrateBtn.textContent = "Migrate";
    footer.appendChild(migrateBtn);

    const status = document.createElement("div");
    status.className = "slls-cm-status";
    root.appendChild(status);

    const attribution = document.createElement("div");
    attribution.className = "slls-cm-attribution";
    attribution.innerHTML =
        'Powered by <a href="https://github.com/microsoft/semantic-link-labs" target="_blank" rel="noopener noreferrer">Semantic Link Labs</a>';
    root.appendChild(attribution);

    // ============== Helpers ==============
    function setStatus(message, kind) {
        if (!message) {
            status.classList.remove("show");
            status.textContent = "";
            return;
        }
        status.className = `slls-cm-status show ${kind || "info"}`;
        status.textContent = message;
    }
    function setBusy(b) {
        if (b) root.classList.add("slls-cm-busy");
        else root.classList.remove("slls-cm-busy");
    }

    function buildAuth() {
        if (!spEnable.checked) return null;
        return {
            key_vault_uri: kvUri.value.trim(),
            key_vault_tenant_id: kvTenant.value.trim(),
            key_vault_client_id: kvClient.value.trim(),
            key_vault_client_secret: kvSecret.value.trim(),
        };
    }

    function validateCommon(requireRg) {
        if (!subIdInput.value.trim()) {
            setStatus("Azure subscription ID is required.", "error");
            return false;
        }
        if (requireRg && !rgInput.value.trim()) {
            setStatus("Resource group is required to create a new F SKU.", "error");
            return false;
        }
        if (spEnable.checked) {
            for (const [el, label] of [
                [kvUri, "Key Vault URI"],
                [kvTenant, "Tenant ID secret name"],
                [kvClient, "Client ID secret name"],
                [kvSecret, "Client Secret name"],
            ]) {
                if (!el.value.trim()) {
                    setStatus(`${label} is required when using Service Principal Authentication.`, "error");
                    return false;
                }
            }
        }
        return true;
    }

    function renderActionButton() {
        if (activeTab === "single") {
            const hasSrc = !!srcSelect.value;
            const hasTgt = tgtMode === "create" || !!tgtSelect.value;
            migrateBtn.disabled = !(hasSrc && hasTgt);
            migrateBtn.textContent = tgtMode === "existing"
                ? "Reassign workspaces"
                : "Migrate";
        } else if (activeTab === "multiple") {
            migrateBtn.disabled = getMultiSelected().size === 0;
            migrateBtn.textContent = "Migrate selected";
        } else {
            const caps = allPSkuOnly.checked
                ? getSourceCaps().filter(c => (c.Sku || "").startsWith("P"))
                : getSourceCaps();
            migrateBtn.disabled = caps.length === 0;
            migrateBtn.textContent = "Migrate all";
        }
    }

    function buildAction() {
        const auth = buildAuth();
        const common = {
            azure_subscription_id: subIdInput.value.trim(),
            resource_group: rgInput.value.trim(),
            auth: auth,
        };
        if (activeTab === "single") {
            if (tgtMode === "create") {
                return { ...common, action: "migrate_single_create", source: srcSelect.value };
            }
            return {
                action: "migrate_single_existing",
                source: srcSelect.value,
                target: tgtSelect.value,
                auth: auth,
            };
        }
        if (activeTab === "multiple") {
            return {
                ...common,
                action: "migrate_multiple",
                capacities: Array.from(getMultiSelected()),
                p_sku_only: multiPSkuOnly.checked,
            };
        }
        return {
            ...common,
            action: "migrate_all",
            p_sku_only: allPSkuOnly.checked,
        };
    }

    function confirmAction(message, onYes) {
        confirmText.textContent = message;
        confirm.classList.add("show");
        const cleanup = () => {
            confirm.classList.remove("show");
            confirmYes.removeEventListener("click", yes);
            confirmNo.removeEventListener("click", no);
        };
        const yes = () => { cleanup(); onYes(); };
        const no = () => { cleanup(); };
        confirmYes.addEventListener("click", yes);
        confirmNo.addEventListener("click", no);
    }

    migrateBtn.addEventListener("click", () => {
        if (activeTab === "single") {
            if (!validateCommon(tgtMode === "create")) return;
        } else {
            if (!validateCommon(true)) return;
        }
        const data = buildAction();
        let msg = "";
        if (activeTab === "single") {
            msg = data.action === "migrate_single_create"
                ? `Create a new F SKU and migrate '${data.source}'?`
                : `Reassign workspaces from '${data.source}' to '${data.target}'?`;
        } else if (activeTab === "multiple") {
            msg = `Migrate ${data.capacities.length} capacit${data.capacities.length === 1 ? "y" : "ies"} to F SKU equivalents?`;
        } else {
            const n = (allPSkuOnly.checked
                ? getSourceCaps().filter(c => (c.Sku || "").startsWith("P"))
                : getSourceCaps()).length;
            msg = `Migrate ALL ${n} P${allPSkuOnly.checked ? "" : "/A"} SKU capacit${n === 1 ? "y" : "ies"} in the tenant to F SKU equivalents?`;
        }
        confirmAction(msg, () => send(data));
    });

    function send(action) {
        setBusy(true);
        setStatus(`Running ${action.action}…`, "info");
        // Inject inline spinner into the status element.
        status.innerHTML = `<span class="slls-cm-spinner"></span>Running ${escapeHtml(action.action)}…`;
        model.set("pending_action", action);
        model.set("run", (model.get("run") || 0) + 1);
        model.save_changes();
    }

    // ============== Model event wiring ==============
    function reload() {
        renderSubtitle();
        // Rebuild source dropdown
        const prev = srcSelect.value;
        srcSelect.innerHTML = "";
        const sources = getSourceCaps();
        if (sources.length === 0) {
            const opt = document.createElement("option");
            opt.value = "";
            opt.textContent = "No P/A SKU capacities found";
            opt.disabled = true;
            opt.selected = true;
            srcSelect.appendChild(opt);
        } else {
            for (const c of sources) {
                const opt = document.createElement("option");
                opt.value = c.Name;
                opt.textContent = `${c.Name}  —  ${c.Sku}  ·  ${c.Region}`;
                if (c.Name === prev) opt.selected = true;
                srcSelect.appendChild(opt);
            }
        }
        refreshSinglePreview();
        refreshTargets();
        renderMultiList();
        refreshAllInfo();
        renderActionButton();
    }

    model.on("change:capacities", reload);
    model.on("change:status", () => {
        const s = model.get("status") || {};
        setBusy(false);
        if (s.message) setStatus(s.message, s.kind || "info");
    });

    setActiveTab("single");
    reload();
}
export default { render };
"""


@log
def capacity_migration(
    azure_subscription_id: Optional[str] = None,
    resource_group: Optional[str] = None,
    key_vault_uri: Optional[str] = None,
    key_vault_tenant_id: Optional[str] = None,
    key_vault_client_id: Optional[str] = None,
    key_vault_client_secret: Optional[str] = None,
    dark_mode: bool = False,
):
    """
    Generates an interactive UI for migrating Microsoft Fabric P or A SKU
    capacities to F SKU capacities.

    The UI supports three migration modes:

    1. **Single capacity** — migrate a single P/A SKU. The target F SKU can
       either be created (using the equivalent F SKU size and the source
       capacity's region and admins) or an existing F SKU can be selected
       (in which case the source capacity's workspaces are reassigned to it).
    2. **Multiple capacities** — multi-select a list of P/A SKU capacities;
       a new F SKU is created for each.
    3. **All P/A capacities** — migrate every P/A SKU capacity in the tenant.

    Cross-region migrations are not supported. When using an existing F SKU
    as the target, only F SKU capacities in the same region as the source are
    listed.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).
    Either fill in the Service Principal fields in the UI, or pass the
    ``key_vault_*`` parameters to pre-populate them. Without Service Principal
    Authentication, the underlying calls use the notebook's default
    authentication context.

    Parameters
    ----------
    azure_subscription_id : str, default=None
        Optional initial value for the Azure subscription ID field.
    resource_group : str, default=None
        Optional initial value for the resource group field. Used when
        creating new F SKU capacities.
    key_vault_uri : str, default=None
        Optional initial value for the Key Vault URI used for Service
        Principal Authentication.
    key_vault_tenant_id : str, default=None
        Optional initial value for the Key Vault secret name that stores the
        tenant ID for Service Principal Authentication.
    key_vault_client_id : str, default=None
        Optional initial value for the Key Vault secret name that stores the
        client (application) ID for Service Principal Authentication.
    key_vault_client_secret : str, default=None
        Optional initial value for the Key Vault secret name that stores the
        client secret for Service Principal Authentication.
    dark_mode : bool, default=False
        If True, renders the editor with a dark color theme. If False,
        renders with a light color theme.
    """

    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "The 'capacity_migration' function requires the 'anywidget' package. "
            "Install it with: pip install anywidget"
        ) from e

    from IPython.display import display
    import sempy_labs._icons as icons
    from sempy_labs._authentication import service_principal_authentication
    from sempy_labs._capacity_migration import migrate_capacities
    from sempy_labs.admin import (
        assign_workspaces_to_capacity,
        list_capacities,
    )

    def _load_capacities():
        try:
            df = list_capacities()
        except Exception as e:
            return [], f"Error loading capacities: {e}"
        caps = []
        for _, r in df.iterrows():
            caps.append(
                {
                    "Id": str(r.get("Capacity Id", "")),
                    "Name": str(r.get("Capacity Name", "")),
                    "Sku": str(r.get("Sku", "")),
                    "Region": str(r.get("Region", "")),
                    "State": str(r.get("State", "")),
                    "Admins": list(r.get("Admins") or []),
                }
            )
        return caps, None

    initial_caps, load_err = _load_capacities()

    class CapacityMigrationWidget(anywidget.AnyWidget):
        _esm = _WIDGET_JS
        _css = _WIDGET_CSS

        capacities = traitlets.List().tag(sync=True)
        sku_mapping = traitlets.Dict().tag(sync=True)
        fsku_suffix = traitlets.Unicode("").tag(sync=True)
        azure_subscription_id = traitlets.Unicode("").tag(sync=True)
        resource_group = traitlets.Unicode("").tag(sync=True)
        key_vault_uri = traitlets.Unicode("").tag(sync=True)
        key_vault_tenant_id = traitlets.Unicode("").tag(sync=True)
        key_vault_client_id = traitlets.Unicode("").tag(sync=True)
        key_vault_client_secret = traitlets.Unicode("").tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)

    widget = CapacityMigrationWidget(
        capacities=initial_caps,
        sku_mapping=dict(icons.sku_mapping),
        fsku_suffix=icons.migrate_capacity_suffix,
        azure_subscription_id=azure_subscription_id or "",
        resource_group=resource_group or "",
        key_vault_uri=key_vault_uri or "",
        key_vault_tenant_id=key_vault_tenant_id or "",
        key_vault_client_id=key_vault_client_id or "",
        key_vault_client_secret=key_vault_client_secret or "",
        status={"message": load_err, "kind": "error"} if load_err else {},
        pending_action={},
        run=0,
        dark_mode=bool(dark_mode),
    )

    def _run_with_service_principal(auth, fn):
        if auth and all(
            auth.get(k)
            for k in (
                "key_vault_uri",
                "key_vault_tenant_id",
                "key_vault_client_id",
                "key_vault_client_secret",
            )
        ):
            with service_principal_authentication(
                key_vault_uri=auth["key_vault_uri"],
                key_vault_tenant_id=auth["key_vault_tenant_id"],
                key_vault_client_id=auth["key_vault_client_id"],
                key_vault_client_secret=auth["key_vault_client_secret"],
            ):
                return fn()
        return fn()

    def _refresh():
        caps, err = _load_capacities()
        widget.capacities = caps
        if err:
            widget.status = {"message": err, "kind": "error"}
        else:
            widget.status = {
                "message": f"Refreshed: {len(caps)} capacities loaded.",
                "kind": "success",
            }

    def _on_run(change):
        data = dict(widget.pending_action or {})
        action = data.get("action")
        if not action:
            return
        try:
            auth = data.get("auth")
            sub_id = data.get("azure_subscription_id", "") or ""
            rg = data.get("resource_group", "") or ""

            if action == "refresh":
                _refresh()
                return

            if action == "migrate_single_create":
                source = data.get("source")
                if not source:
                    widget.status = {
                        "message": "No source capacity selected.",
                        "kind": "error",
                    }
                    return

                def _migrate_single():
                    migrate_capacities(
                        azure_subscription_id=sub_id,
                        resource_group=rg,
                        capacities=source,
                        p_sku_only=False,
                    )

                _run_with_service_principal(auth, _migrate_single)
                _refresh()
                widget.status = {
                    "message": f"Migration completed for '{source}'.",
                    "kind": "success",
                }
                return

            if action == "migrate_single_existing":
                source = data.get("source")
                target = data.get("target")
                if not source or not target:
                    widget.status = {
                        "message": "Both a source and target capacity must be selected.",
                        "kind": "error",
                    }
                    return

                # Cross-region guard (defense-in-depth; UI already filters).
                src_cap = next(
                    (c for c in widget.capacities if c["Name"] == source), None
                )
                tgt_cap = next(
                    (c for c in widget.capacities if c["Name"] == target), None
                )
                if (
                    src_cap
                    and tgt_cap
                    and src_cap.get("Region")
                    and tgt_cap.get("Region")
                    and src_cap["Region"] != tgt_cap["Region"]
                ):
                    widget.status = {
                        "message": (
                            f"Cross-region migrations are not supported. "
                            f"Source '{source}' is in '{src_cap['Region']}' but target "
                            f"'{target}' is in '{tgt_cap['Region']}'."
                        ),
                        "kind": "error",
                    }
                    return

                def _reassign_workspaces():
                    assign_workspaces_to_capacity(
                        source_capacity=source,
                        target_capacity=target,
                        workspace=None,
                    )

                _run_with_service_principal(auth, _reassign_workspaces)
                _refresh()
                widget.status = {
                    "message": (
                        f"Reassigned workspaces from '{source}' to '{target}'."
                    ),
                    "kind": "success",
                }
                return

            if action == "migrate_multiple":
                capacities = data.get("capacities") or []
                if not capacities:
                    widget.status = {
                        "message": "No capacities selected.",
                        "kind": "error",
                    }
                    return

                def _migrate_multiple():
                    migrate_capacities(
                        azure_subscription_id=sub_id,
                        resource_group=rg,
                        capacities=list(capacities),
                        p_sku_only=bool(data.get("p_sku_only", True)),
                    )

                _run_with_service_principal(auth, _migrate_multiple)
                _refresh()
                widget.status = {
                    "message": f"Migration completed for {len(capacities)} capacities.",
                    "kind": "success",
                }
                return

            if action == "migrate_all":

                def _migrate_all():
                    migrate_capacities(
                        azure_subscription_id=sub_id,
                        resource_group=rg,
                        capacities=None,
                        p_sku_only=bool(data.get("p_sku_only", True)),
                    )

                _run_with_service_principal(auth, _migrate_all)
                _refresh()
                widget.status = {
                    "message": "Migration completed for all P/A SKU capacities.",
                    "kind": "success",
                }
                return

            widget.status = {
                "message": f"Unknown action: {action}",
                "kind": "error",
            }
        except Exception as e:
            widget.status = {"message": f"Error: {e}", "kind": "error"}

    widget.observe(_on_run, names=["run"])

    # Keep a reference on the widget so the Python-side observer is not
    # garbage collected after this function returns. We intentionally do NOT
    # return the widget to avoid Jupyter auto-displaying it a second time
    # after ``display()``.
    display(widget)
