from typing import Optional, Tuple, List, Dict, Any
from uuid import UUID
from sempy._utils._log import log


# ---------------------------------------------------------------------------
# CSS — mirrors the look of `_perspective_editor` (slls-pe-* tokens) but uses
# a `slls-md-*` prefix and adds diff-specific styling (add / remove / modify
# colour tokens, two-pane preview, and source/target column chrome).
# ---------------------------------------------------------------------------
_WIDGET_CSS = """
.slls-md {
    --slls-bg-solid: #ffffff;
    --slls-surface: rgba(255, 255, 255, 0.85);
    --slls-surface-2: rgba(0, 0, 0, 0.025);
    --slls-surface-3: rgba(0, 0, 0, 0.05);
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
    --slls-success-soft: rgba(52, 199, 89, 0.16);
    --slls-success-strong: rgba(52, 199, 89, 0.28);
    --slls-danger: #ff3b30;
    --slls-danger-soft: rgba(255, 59, 48, 0.14);
    --slls-danger-strong: rgba(255, 59, 48, 0.26);
    --slls-warning: #b07b00;
    --slls-warning-soft: rgba(255, 149, 0, 0.16);
    --slls-warning-strong: rgba(255, 149, 0, 0.28);
    --slls-radius: 14px;
    --slls-radius-sm: 8px;
    --slls-shadow: 0 1px 2px rgba(0,0,0,0.04), 0 8px 24px rgba(0,0,0,0.06);
    --slls-mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas,
        "Liberation Mono", monospace;
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "SF Pro Display",
        "Helvetica Neue", Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    color: var(--slls-text);
    width: 100%;
    max-width: 1180px;
    background: var(--slls-bg-solid);
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    box-shadow: var(--slls-shadow);
    padding: 24px;
    box-sizing: border-box;
}
@media (prefers-color-scheme: dark) {
    .slls-md.slls-md-auto {
        --slls-bg-solid: #1c1c1e;
        --slls-surface: rgba(255, 255, 255, 0.04);
        --slls-surface-2: rgba(255, 255, 255, 0.03);
        --slls-surface-3: rgba(255, 255, 255, 0.06);
        --slls-border: rgba(255, 255, 255, 0.08);
        --slls-border-strong: rgba(255, 255, 255, 0.16);
        --slls-text: #f5f5f7;
        --slls-text-secondary: #a1a1a6;
        --slls-text-tertiary: #6e6e73;
        --slls-accent-soft: rgba(10, 132, 255, 0.18);
        --slls-accent: #0A84FF;
        --slls-warning: #ffb340;
        --slls-shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.5);
    }
}
.slls-md.slls-md-dark {
    --slls-bg-solid: #1c1c1e;
    --slls-surface: rgba(255, 255, 255, 0.04);
    --slls-surface-2: rgba(255, 255, 255, 0.03);
    --slls-surface-3: rgba(255, 255, 255, 0.06);
    --slls-border: rgba(255, 255, 255, 0.08);
    --slls-border-strong: rgba(255, 255, 255, 0.16);
    --slls-text: #f5f5f7;
    --slls-text-secondary: #a1a1a6;
    --slls-text-tertiary: #6e6e73;
    --slls-accent-soft: rgba(10, 132, 255, 0.18);
    --slls-accent: #0A84FF;
    --slls-warning: #ffb340;
    --slls-shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.5);
}
.slls-md * { box-sizing: border-box; }

.slls-md-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 18px;
    flex-wrap: wrap;
}
.slls-md-titlewrap {
    display: flex;
    flex-direction: column;
    margin-right: auto;
    min-width: 0;
}
.slls-md-title {
    font-size: 22px;
    font-weight: 600;
    letter-spacing: -0.01em;
    line-height: 1.15;
    display: flex;
    align-items: center;
    gap: 8px;
}
.slls-md-subtitle {
    font-size: 12px;
    color: var(--slls-text-secondary);
    margin-top: 2px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 720px;
}
.slls-md-subtitle b { color: var(--slls-text); font-weight: 500; }
.slls-md-subtitle .slls-md-sep {
    color: var(--slls-text-tertiary);
    margin: 0 6px;
}
.slls-md-subtitle .slls-md-arrow {
    color: var(--slls-accent);
    margin: 0 6px;
    font-weight: 600;
}

.slls-md-btn {
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
.slls-md-btn:hover { background: var(--slls-surface-2); border-color: var(--slls-text-tertiary); }
.slls-md-btn:active { transform: scale(0.97); }
.slls-md-btn:disabled { opacity: 0.4; cursor: not-allowed; }
.slls-md-btn-primary {
    background: var(--slls-accent);
    border-color: var(--slls-accent);
    color: #fff;
}
.slls-md-btn-primary:hover {
    background: var(--slls-accent-hover);
    border-color: var(--slls-accent-hover);
}
.slls-md-btn-icon {
    width: 32px; height: 32px;
    padding: 0;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border-radius: 50%;
    font-size: 18px;
    line-height: 1;
}

.slls-md-seg {
    display: inline-flex;
    align-items: center;
    background: var(--slls-surface);
    border: 1px solid var(--slls-border-strong);
    border-radius: 999px;
    padding: 2px;
    gap: 2px;
}
.slls-md-seg-btn {
    appearance: none;
    background: transparent;
    border: none;
    color: var(--slls-text-secondary);
    font-family: inherit;
    font-size: 12.5px;
    font-weight: 500;
    padding: 5px 12px;
    border-radius: 999px;
    cursor: pointer;
    transition: background 120ms ease, color 120ms ease;
}
.slls-md-seg-btn:hover { color: var(--slls-text); }
.slls-md-seg-btn.is-on {
    background: var(--slls-accent);
    color: #fff;
}

.slls-md-input {
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
.slls-md-input::placeholder { color: var(--slls-text-tertiary); }
.slls-md-input:focus {
    outline: none;
    border-color: var(--slls-accent);
    box-shadow: 0 0 0 3px var(--slls-accent-soft);
}

.slls-md-toolbar {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 12px;
    flex-wrap: wrap;
}
.slls-md-toolbar .slls-md-search { flex: 1; min-width: 200px; max-width: 360px; }
.slls-md-counts {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    font-size: 12.5px;
    color: var(--slls-text-secondary);
    margin-left: auto;
}
.slls-md-pill {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 3px 9px;
    border-radius: 999px;
    font-size: 12px;
    font-weight: 600;
    line-height: 1.2;
}
.slls-md-pill .slls-md-dot {
    width: 7px; height: 7px;
    border-radius: 50%;
    display: inline-block;
}
.slls-md-pill-added { background: var(--slls-success-soft); color: var(--slls-success); }
.slls-md-pill-added .slls-md-dot { background: var(--slls-success); }
.slls-md-pill-removed { background: var(--slls-danger-soft); color: var(--slls-danger); }
.slls-md-pill-removed .slls-md-dot { background: var(--slls-danger); }
.slls-md-pill-modified { background: var(--slls-warning-soft); color: var(--slls-warning); }
.slls-md-pill-modified .slls-md-dot { background: var(--slls-orange); }

.slls-md-body {
    display: grid;
    grid-template-columns: minmax(280px, 1fr) minmax(360px, 1.4fr);
    gap: 14px;
    align-items: stretch;
}
@media (max-width: 880px) {
    .slls-md-body { grid-template-columns: 1fr; }
}

.slls-md-tree {
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    background: var(--slls-surface);
    overflow: hidden;
    max-height: 560px;
    overflow-y: auto;
}
.slls-md-tree::-webkit-scrollbar { width: 10px; height: 10px; }
.slls-md-tree::-webkit-scrollbar-thumb {
    background: var(--slls-border-strong);
    border-radius: 999px;
    background-clip: padding-box;
    border: 2px solid transparent;
}
.slls-md-tree::-webkit-scrollbar-thumb:hover { background-color: var(--slls-text-tertiary); }

.slls-md-cat { border-bottom: 1px solid var(--slls-border); }
.slls-md-cat:last-child { border-bottom: none; }
.slls-md-cat.is-empty { display: none; }
.slls-md-cat.filtered-out { display: none; }

.slls-md-cat-row {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 14px;
    cursor: pointer;
    user-select: none;
    background: var(--slls-surface-3);
    transition: background 100ms ease;
}
.slls-md-cat-row:hover { background: var(--slls-surface-2); }
.slls-md-cat-name {
    font-size: 13.5px;
    font-weight: 600;
    letter-spacing: -0.01em;
}
.slls-md-cat-counts {
    margin-left: auto;
    display: inline-flex;
    gap: 6px;
}

.slls-md-caret {
    width: 14px; height: 14px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    color: var(--slls-text-tertiary);
    transition: transform 160ms ease;
    flex-shrink: 0;
}
.slls-md-cat.expanded > .slls-md-cat-row > .slls-md-caret { transform: rotate(90deg); }

.slls-md-items {
    display: none;
    background: var(--slls-bg-solid);
}
.slls-md-cat.expanded > .slls-md-items { display: block; }

.slls-md-item {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 8px 14px 8px 36px;
    cursor: pointer;
    user-select: none;
    border-top: 1px solid var(--slls-border);
    transition: background 100ms ease;
}
.slls-md-item:hover { background: var(--slls-surface-2); }
.slls-md-item.is-selected { background: var(--slls-accent-soft); }
.slls-md-item.filtered-out { display: none; }

.slls-md-check {
    width: 16px; height: 16px;
    border: 1.5px solid var(--slls-border-strong);
    border-radius: 4px;
    background: var(--slls-bg-solid);
    display: inline-flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    cursor: pointer;
    transition: background 100ms ease, border-color 100ms ease;
}
.slls-md-check[data-state="all"],
.slls-md-check[data-state="some"] {
    background: var(--slls-accent);
    border-color: var(--slls-accent);
}
.slls-md-check[data-state="all"]::after {
    content: "";
    width: 8px; height: 5px;
    border-left: 2px solid #fff;
    border-bottom: 2px solid #fff;
    transform: rotate(-45deg) translate(1px, -1px);
}
.slls-md-check[data-state="some"]::after {
    content: "";
    width: 8px; height: 2px;
    background: #fff;
    border-radius: 1px;
}

.slls-md-badge {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 18px; height: 18px;
    border-radius: 5px;
    font-size: 11px;
    font-weight: 700;
    flex-shrink: 0;
    line-height: 1;
    font-family: var(--slls-mono);
}
.slls-md-badge.added { background: var(--slls-success-strong); color: #fff; }
.slls-md-badge.removed { background: var(--slls-danger-strong); color: #fff; }
.slls-md-badge.modified { background: var(--slls-warning-strong); color: #fff; }

.slls-md-item-name {
    font-size: 13.5px;
    font-weight: 500;
    flex: 1;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
.slls-md-item-meta {
    font-size: 11.5px;
    color: var(--slls-text-tertiary);
    margin-left: 6px;
    flex-shrink: 0;
}

.slls-md-preview {
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    background: var(--slls-surface);
    display: flex;
    flex-direction: column;
    max-height: 560px;
    min-height: 320px;
    overflow: hidden;
}
.slls-md-preview-head {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 10px 14px;
    border-bottom: 1px solid var(--slls-border);
    background: var(--slls-surface-3);
    font-size: 12.5px;
    color: var(--slls-text-secondary);
}
.slls-md-preview-head b { color: var(--slls-text); font-weight: 600; }
.slls-md-preview-cols {
    display: grid;
    grid-template-columns: 1fr 1fr;
    flex: 1;
    overflow: hidden;
}
.slls-md-preview-col {
    display: flex;
    flex-direction: column;
    overflow: hidden;
    border-right: 1px solid var(--slls-border);
}
.slls-md-preview-col:last-child { border-right: none; }
.slls-md-preview-col-head {
    padding: 6px 12px;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--slls-text-secondary);
    border-bottom: 1px solid var(--slls-border);
    background: var(--slls-surface-2);
    display: flex;
    align-items: center;
    gap: 6px;
}
.slls-md-preview-col-head .slls-md-side-dot {
    width: 7px; height: 7px; border-radius: 50%;
    background: var(--slls-text-tertiary);
}
.slls-md-preview-col.is-source .slls-md-side-dot { background: var(--slls-accent); }
.slls-md-preview-col.is-target .slls-md-side-dot { background: var(--slls-orange); }
.slls-md-preview-pre {
    margin: 0;
    padding: 12px 14px;
    font-family: var(--slls-mono);
    font-size: 12px;
    line-height: 1.5;
    overflow: auto;
    flex: 1;
    color: var(--slls-text);
    white-space: pre;
    tab-size: 2;
}
.slls-md-preview-pre.is-empty {
    color: var(--slls-text-tertiary);
    font-style: italic;
    white-space: normal;
}
.slls-md-line { display: block; padding: 0 6px; border-radius: 3px; }
.slls-md-line.added { background: var(--slls-success-soft); }
.slls-md-line.removed { background: var(--slls-danger-soft); }
.slls-md-line.context { opacity: 0.85; }

.slls-md-empty {
    padding: 48px 16px;
    text-align: center;
    color: var(--slls-text-tertiary);
    font-size: 13.5px;
}
.slls-md-empty .slls-md-empty-icon {
    font-size: 28px;
    margin-bottom: 8px;
    opacity: 0.5;
}

.slls-md-footer {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-top: 16px;
    flex-wrap: wrap;
}
.slls-md-footer .slls-md-spacer { flex: 1; }
.slls-md-dir {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 6px 10px;
    border-radius: 999px;
    background: var(--slls-surface-3);
    border: 1px solid var(--slls-border);
    font-size: 12.5px;
    color: var(--slls-text-secondary);
}
.slls-md-dir b { color: var(--slls-text); font-weight: 600; }
.slls-md-dir .slls-md-dir-arrow {
    color: var(--slls-accent);
    font-weight: 700;
    cursor: pointer;
    padding: 0 2px;
    transition: transform 120ms ease;
    display: inline-block;
}
.slls-md-dir .slls-md-dir-arrow:hover { transform: scale(1.15); }

.slls-md-status {
    margin-top: 14px;
    padding: 10px 14px;
    border-radius: var(--slls-radius-sm);
    font-size: 13.5px;
    display: none;
}
.slls-md-status.show { display: block; animation: slls-md-fade-in 200ms ease; }
.slls-md-status.success { background: var(--slls-success-soft); color: var(--slls-success); }
.slls-md-status.error { background: var(--slls-danger-soft); color: var(--slls-danger); }
.slls-md-status.info { background: var(--slls-accent-soft); color: var(--slls-accent); }
.slls-md-status.warning { background: var(--slls-warning-soft); color: var(--slls-warning); }
@keyframes slls-md-fade-in {
    from { opacity: 0; transform: translateY(-4px); }
    to { opacity: 1; transform: translateY(0); }
}

.slls-md-confirm {
    margin-top: 14px;
    padding: 12px 16px;
    border-radius: var(--slls-radius-sm);
    border: 1px solid var(--slls-warning);
    background: var(--slls-warning-soft);
    display: none;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
}
.slls-md-confirm.show { display: flex; }
.slls-md-confirm-text { flex: 1; font-size: 13.5px; color: var(--slls-text); }

.slls-md-busy {
    pointer-events: none;
    opacity: 0.55;
    transition: opacity 120ms ease;
}

.slls-md-attribution {
    margin-top: 18px;
    text-align: right;
    font-size: 11.5px;
    color: var(--slls-text-tertiary);
}
"""


# ---------------------------------------------------------------------------
# JS — anywidget render function. Builds the diff browser UI: header (title,
# theme + format toggles, refresh), counts toolbar, categorised tree of diffs
# with per-item checkboxes, side-by-side preview of source vs. target content
# for the focused diff, and the merge footer (direction + apply).
# ---------------------------------------------------------------------------
_WIDGET_JS = r"""
function render({ model, el }) {
    const root = document.createElement("div");
    root.className = "slls-md";
    el.appendChild(root);

    function applyTheme() {
        root.classList.remove("slls-md-dark", "slls-md-auto");
        const dm = model.get("dark_mode");
        if (dm === true) root.classList.add("slls-md-dark");
        else if (dm === null || dm === undefined) root.classList.add("slls-md-auto");
    }
    applyTheme();
    model.on("change:dark_mode", applyTheme);

    function escapeHtml(s) {
        return String(s == null ? "" : s)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    // ---------- State ----------
    // selection: Set of item ids checked for merge
    let selection = new Set();
    // focused: id of the item currently shown in the preview pane
    let focusedId = null;
    let filterText = "";
    // expanded categories — default: all expanded that have items.
    let expandedCats = {};
    let kindFilter = { added: true, removed: true, modified: true };
    let direction = "s2t"; // "s2t" (source -> target) or "t2s"

    function getDiff() { return model.get("diff") || { categories: [] }; }
    function isBusy() { return !!model.get("busy"); }

    // ---------- Header ----------
    const header = document.createElement("div");
    header.className = "slls-md-header";
    root.appendChild(header);

    const titleWrap = document.createElement("div");
    titleWrap.className = "slls-md-titlewrap";
    header.appendChild(titleWrap);

    const title = document.createElement("div");
    title.className = "slls-md-title";
    title.innerHTML = `<span>Semantic Model Diff</span>`;
    titleWrap.appendChild(title);

    const subtitle = document.createElement("div");
    subtitle.className = "slls-md-subtitle";
    titleWrap.appendChild(subtitle);
    function renderSubtitle() {
        const src = model.get("source_label") || "";
        const tgt = model.get("target_label") || "";
        const fmt = (model.get("format") || "TMSL").toUpperCase();
        if (!src && !tgt) { subtitle.textContent = ""; return; }
        subtitle.innerHTML =
            `<b>${escapeHtml(src)}</b>` +
            `<span class="slls-md-arrow">↔</span>` +
            `<b>${escapeHtml(tgt)}</b>` +
            `<span class="slls-md-sep">·</span>` +
            `${escapeHtml(fmt)}`;
    }

    // Format segmented control (TMSL <-> TMDL)
    const seg = document.createElement("div");
    seg.className = "slls-md-seg";
    seg.title = "Diff format";
    const segTmsl = document.createElement("button");
    segTmsl.className = "slls-md-seg-btn";
    segTmsl.type = "button";
    segTmsl.textContent = "BIM";
    const segTmdl = document.createElement("button");
    segTmdl.className = "slls-md-seg-btn";
    segTmdl.type = "button";
    segTmdl.textContent = "TMDL";
    seg.appendChild(segTmsl);
    seg.appendChild(segTmdl);
    function renderSeg() {
        const fmt = (model.get("format") || "TMSL").toUpperCase();
        segTmsl.classList.toggle("is-on", fmt === "TMSL");
        segTmdl.classList.toggle("is-on", fmt === "TMDL");
    }
    segTmsl.addEventListener("click", () => {
        if (isBusy()) return;
        if ((model.get("format") || "TMSL").toUpperCase() === "TMSL") return;
        triggerAction({ action: "set_format", format: "TMSL" });
    });
    segTmdl.addEventListener("click", () => {
        if (isBusy()) return;
        if ((model.get("format") || "TMSL").toUpperCase() === "TMDL") return;
        triggerAction({ action: "set_format", format: "TMDL" });
    });
    header.appendChild(seg);

    // Refresh button
    const REFRESH_SVG = `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6"`
        + ` stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">`
        + `<path d="M3 8a5 5 0 0 1 8.6-3.5L13 6"/><path d="M13 3v3h-3"/>`
        + `<path d="M13 8a5 5 0 0 1-8.6 3.5L3 10"/><path d="M3 13v-3h3"/></svg>`;
    const refreshBtn = document.createElement("button");
    refreshBtn.className = "slls-md-btn slls-md-btn-icon";
    refreshBtn.type = "button";
    refreshBtn.innerHTML = REFRESH_SVG;
    refreshBtn.title = "Recompute diff";
    refreshBtn.setAttribute("aria-label", "Recompute diff");
    refreshBtn.addEventListener("click", () => {
        if (isBusy()) return;
        triggerAction({ action: "refresh" });
    });
    header.appendChild(refreshBtn);

    // Theme toggle
    const SUN_SVG = `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6"`
        + ` stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">`
        + `<circle cx="8" cy="8" r="3"/>`
        + `<path d="M8 1.5v1.5M8 13v1.5M1.5 8h1.5M13 8h1.5`
        + `M3.3 3.3l1.05 1.05M11.65 11.65l1.05 1.05`
        + `M3.3 12.7l1.05-1.05M11.65 4.35l1.05-1.05"/></svg>`;
    const MOON_SVG = `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6"`
        + ` stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">`
        + `<path d="M13.5 9.5A5.5 5.5 0 0 1 6.5 2.5a5.5 5.5 0 1 0 7 7z"/></svg>`;
    const themeBtn = document.createElement("button");
    themeBtn.className = "slls-md-btn slls-md-btn-icon";
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

    // ---------- Toolbar ----------
    const toolbar = document.createElement("div");
    toolbar.className = "slls-md-toolbar";
    root.appendChild(toolbar);

    const searchInput = document.createElement("input");
    searchInput.type = "text";
    searchInput.className = "slls-md-input slls-md-search";
    searchInput.placeholder = "Filter differences…";
    searchInput.addEventListener("input", () => {
        filterText = (searchInput.value || "").toLowerCase().trim();
        renderTree();
    });
    toolbar.appendChild(searchInput);

    // Kind filter pills (clickable to toggle)
    const counts = document.createElement("div");
    counts.className = "slls-md-counts";
    toolbar.appendChild(counts);

    function makePill(kind, label) {
        const pill = document.createElement("button");
        pill.type = "button";
        pill.className = `slls-md-pill slls-md-pill-${kind}`;
        pill.dataset.kind = kind;
        pill.dataset.label = label;
        pill.style.cursor = "pointer";
        pill.style.border = "1px solid transparent";
        pill.addEventListener("click", () => {
            kindFilter[kind] = !kindFilter[kind];
            renderTree();
            renderCountPills();
        });
        return pill;
    }
    const pillAdded = makePill("added", "added");
    const pillRemoved = makePill("removed", "removed");
    const pillModified = makePill("modified", "modified");

    function renderCountPills() {
        const c = countsByKind();
        const make = (pill, n, label) => {
            const off = !kindFilter[pill.dataset.kind];
            pill.innerHTML = `<span class="slls-md-dot"></span>${n} ${label}`;
            pill.style.opacity = off ? "0.4" : "1";
        };
        make(pillAdded, c.added, "added");
        make(pillRemoved, c.removed, "removed");
        make(pillModified, c.modified, "modified");
        counts.innerHTML = "";
        counts.appendChild(pillAdded);
        counts.appendChild(pillRemoved);
        counts.appendChild(pillModified);
    }

    function countsByKind() {
        const out = { added: 0, removed: 0, modified: 0 };
        for (const cat of (getDiff().categories || [])) {
            for (const it of (cat.items || [])) {
                if (out[it.kind] !== undefined) out[it.kind] += 1;
            }
        }
        return out;
    }

    // ---------- Body: tree + preview ----------
    const body = document.createElement("div");
    body.className = "slls-md-body";
    root.appendChild(body);

    const tree = document.createElement("div");
    tree.className = "slls-md-tree";
    body.appendChild(tree);

    const preview = document.createElement("div");
    preview.className = "slls-md-preview";
    body.appendChild(preview);

    const previewHead = document.createElement("div");
    previewHead.className = "slls-md-preview-head";
    preview.appendChild(previewHead);

    const previewCols = document.createElement("div");
    previewCols.className = "slls-md-preview-cols";
    preview.appendChild(previewCols);

    const colSource = document.createElement("div");
    colSource.className = "slls-md-preview-col is-source";
    colSource.innerHTML = `<div class="slls-md-preview-col-head"><span class="slls-md-side-dot"></span>Source</div>`;
    const preSource = document.createElement("pre");
    preSource.className = "slls-md-preview-pre";
    colSource.appendChild(preSource);
    previewCols.appendChild(colSource);

    const colTarget = document.createElement("div");
    colTarget.className = "slls-md-preview-col is-target";
    colTarget.innerHTML = `<div class="slls-md-preview-col-head"><span class="slls-md-side-dot"></span>Target</div>`;
    const preTarget = document.createElement("pre");
    preTarget.className = "slls-md-preview-pre";
    colTarget.appendChild(preTarget);
    previewCols.appendChild(colTarget);

    // ---------- Footer ----------
    const footer = document.createElement("div");
    footer.className = "slls-md-footer";
    root.appendChild(footer);

    const selectAllBtn = document.createElement("button");
    selectAllBtn.className = "slls-md-btn";
    selectAllBtn.type = "button";
    selectAllBtn.textContent = "Select all visible";
    selectAllBtn.addEventListener("click", () => {
        for (const cat of (getDiff().categories || [])) {
            for (const it of (cat.items || [])) {
                if (itemMatchesFilters(cat, it)) selection.add(it.id);
            }
        }
        renderTree();
        renderFooter();
    });
    footer.appendChild(selectAllBtn);

    const clearBtn = document.createElement("button");
    clearBtn.className = "slls-md-btn";
    clearBtn.type = "button";
    clearBtn.textContent = "Clear";
    clearBtn.addEventListener("click", () => {
        selection.clear();
        renderTree();
        renderFooter();
    });
    footer.appendChild(clearBtn);

    const dirEl = document.createElement("div");
    dirEl.className = "slls-md-dir";
    footer.appendChild(dirEl);
    function renderDir() {
        const src = model.get("source_label") || "Source";
        const tgt = model.get("target_label") || "Target";
        const arrow = direction === "s2t" ? "→" : "←";
        const left = direction === "s2t" ? src : tgt;
        const right = direction === "s2t" ? tgt : src;
        dirEl.innerHTML = `Apply: <b>${escapeHtml(left)}</b>` +
            ` <span class="slls-md-dir-arrow" title="Swap direction">${arrow}</span> ` +
            `<b>${escapeHtml(right)}</b>`;
        const arr = dirEl.querySelector(".slls-md-dir-arrow");
        if (arr) {
            arr.addEventListener("click", () => {
                direction = direction === "s2t" ? "t2s" : "s2t";
                renderDir();
                renderFooter();
            });
        }
    }

    const spacer = document.createElement("div");
    spacer.className = "slls-md-spacer";
    footer.appendChild(spacer);

    const applyBtn = document.createElement("button");
    applyBtn.className = "slls-md-btn slls-md-btn-primary";
    applyBtn.type = "button";
    applyBtn.textContent = "Apply selected";
    applyBtn.addEventListener("click", () => {
        if (selection.size === 0 || isBusy()) return;
        showConfirm();
    });
    footer.appendChild(applyBtn);

    // Confirmation banner
    const confirm = document.createElement("div");
    confirm.className = "slls-md-confirm";
    root.appendChild(confirm);
    const confirmText = document.createElement("div");
    confirmText.className = "slls-md-confirm-text";
    confirm.appendChild(confirmText);
    const confirmCancel = document.createElement("button");
    confirmCancel.className = "slls-md-btn";
    confirmCancel.type = "button";
    confirmCancel.textContent = "Cancel";
    confirmCancel.addEventListener("click", () => {
        confirm.classList.remove("show");
    });
    confirm.appendChild(confirmCancel);
    const confirmGo = document.createElement("button");
    confirmGo.className = "slls-md-btn slls-md-btn-primary";
    confirmGo.type = "button";
    confirmGo.textContent = "Confirm apply";
    confirmGo.addEventListener("click", () => {
        confirm.classList.remove("show");
        triggerAction({
            action: "apply",
            direction: direction,
            selected: Array.from(selection),
        });
    });
    confirm.appendChild(confirmGo);

    function showConfirm() {
        const src = model.get("source_label") || "Source";
        const tgt = model.get("target_label") || "Target";
        const dest = direction === "s2t" ? tgt : src;
        confirmText.innerHTML = `Apply <b>${selection.size}</b> change` +
            (selection.size === 1 ? "" : "s") +
            ` to <b>${escapeHtml(dest)}</b>. This will overwrite the destination model definition.`;
        confirm.classList.add("show");
    }

    // ---------- Status ----------
    const status = document.createElement("div");
    status.className = "slls-md-status";
    root.appendChild(status);

    const attribution = document.createElement("div");
    attribution.className = "slls-md-attribution";
    attribution.innerHTML = `<span>powered by semantic-link-labs</span>`;
    root.appendChild(attribution);

    // ---------- Renderers ----------
    function itemMatchesFilters(cat, item) {
        if (!kindFilter[item.kind]) return false;
        if (!filterText) return true;
        const hay = (
            (item.name || "") + " " +
            (item.path || "") + " " +
            (cat.label || "")
        ).toLowerCase();
        return hay.indexOf(filterText) !== -1;
    }

    function renderTree() {
        tree.innerHTML = "";
        const diff = getDiff();
        const cats = diff.categories || [];
        let visibleTotal = 0;

        for (const cat of cats) {
            const items = (cat.items || []).filter(it => itemMatchesFilters(cat, it));
            const catEl = document.createElement("div");
            catEl.className = "slls-md-cat";
            if (items.length === 0) catEl.classList.add("is-empty");
            if (expandedCats[cat.key] !== false) catEl.classList.add("expanded");

            const catRow = document.createElement("div");
            catRow.className = "slls-md-cat-row";

            const caret = document.createElement("span");
            caret.className = "slls-md-caret";
            caret.innerHTML = "<svg width='8' height='10' viewBox='0 0 8 10' fill='currentColor'><path d='M1 0l6 5-6 5V0z'/></svg>";
            catRow.appendChild(caret);

            // Category checkbox
            const catChk = document.createElement("span");
            catChk.className = "slls-md-check";
            const allSelected = items.length > 0 && items.every(it => selection.has(it.id));
            const someSelected = !allSelected && items.some(it => selection.has(it.id));
            catChk.dataset.state = allSelected ? "all" : (someSelected ? "some" : "none");
            catChk.addEventListener("click", (e) => {
                e.stopPropagation();
                if (allSelected) {
                    for (const it of items) selection.delete(it.id);
                } else {
                    for (const it of items) selection.add(it.id);
                }
                renderTree();
                renderFooter();
            });
            catRow.appendChild(catChk);

            const catName = document.createElement("div");
            catName.className = "slls-md-cat-name";
            catName.textContent = cat.label || cat.key;
            catRow.appendChild(catName);

            // Per-category kind counts
            const cKinds = { added: 0, removed: 0, modified: 0 };
            for (const it of (cat.items || [])) {
                if (cKinds[it.kind] !== undefined) cKinds[it.kind] += 1;
            }
            const catCounts = document.createElement("div");
            catCounts.className = "slls-md-cat-counts";
            for (const k of ["added", "removed", "modified"]) {
                if (cKinds[k] > 0) {
                    const p = document.createElement("span");
                    p.className = `slls-md-pill slls-md-pill-${k}`;
                    p.innerHTML = `<span class="slls-md-dot"></span>${cKinds[k]}`;
                    catCounts.appendChild(p);
                }
            }
            catRow.appendChild(catCounts);

            catRow.addEventListener("click", () => {
                expandedCats[cat.key] = !(expandedCats[cat.key] !== false);
                renderTree();
            });
            catEl.appendChild(catRow);

            const itemsWrap = document.createElement("div");
            itemsWrap.className = "slls-md-items";
            catEl.appendChild(itemsWrap);

            for (const it of items) {
                visibleTotal++;
                const itEl = document.createElement("div");
                itEl.className = "slls-md-item";
                if (focusedId === it.id) itEl.classList.add("is-selected");

                const chk = document.createElement("span");
                chk.className = "slls-md-check";
                chk.dataset.state = selection.has(it.id) ? "all" : "none";
                chk.addEventListener("click", (e) => {
                    e.stopPropagation();
                    if (selection.has(it.id)) selection.delete(it.id);
                    else selection.add(it.id);
                    renderTree();
                    renderFooter();
                });
                itEl.appendChild(chk);

                const badge = document.createElement("span");
                badge.className = `slls-md-badge ${it.kind}`;
                badge.textContent = it.kind === "added" ? "+"
                    : it.kind === "removed" ? "−" : "~";
                badge.title = it.kind;
                itEl.appendChild(badge);

                const name = document.createElement("span");
                name.className = "slls-md-item-name";
                name.textContent = it.name;
                name.title = it.path || it.name;
                itEl.appendChild(name);

                if (it.summary) {
                    const meta = document.createElement("span");
                    meta.className = "slls-md-item-meta";
                    meta.textContent = it.summary;
                    itEl.appendChild(meta);
                }

                itEl.addEventListener("click", () => {
                    focusedId = it.id;
                    renderTree();
                    renderPreview();
                });
                itemsWrap.appendChild(itEl);
            }

            tree.appendChild(catEl);
        }

        if (visibleTotal === 0) {
            const empty = document.createElement("div");
            empty.className = "slls-md-empty";
            const fmt = (model.get("format") || "TMSL").toUpperCase();
            empty.innerHTML = `<div class="slls-md-empty-icon">✓</div>` +
                `<div>No differences match the current filters.</div>` +
                `<div style="font-size:11.5px;margin-top:6px;opacity:0.8;">Format: ${escapeHtml(fmt)}</div>`;
            tree.appendChild(empty);
        }
    }

    function findItem(id) {
        if (!id) return null;
        for (const cat of (getDiff().categories || [])) {
            for (const it of (cat.items || [])) {
                if (it.id === id) return { cat, item: it };
            }
        }
        return null;
    }

    function renderPreview() {
        const found = findItem(focusedId);
        if (!found) {
            previewHead.innerHTML = `<span>Select a difference to preview source vs. target.</span>`;
            preSource.classList.add("is-empty");
            preTarget.classList.add("is-empty");
            preSource.textContent = "Nothing selected.";
            preTarget.textContent = "Nothing selected.";
            return;
        }
        const { cat, item } = found;
        const kindLabel = item.kind.charAt(0).toUpperCase() + item.kind.slice(1);
        previewHead.innerHTML =
            `<span class="slls-md-pill slls-md-pill-${item.kind}"><span class="slls-md-dot"></span>${kindLabel}</span>` +
            `<b>${escapeHtml(cat.label || cat.key)}</b>` +
            `<span class="slls-md-sep">·</span>` +
            `<span>${escapeHtml(item.path || item.name)}</span>`;

        renderPane(preSource, item.source, item, "source");
        renderPane(preTarget, item.target, item, "target");
    }

    function renderPane(preEl, content, item, side) {
        preEl.classList.remove("is-empty");
        if (content === null || content === undefined) {
            preEl.classList.add("is-empty");
            preEl.textContent = side === "source"
                ? "Not present in source."
                : "Not present in target.";
            return;
        }
        // Render with simple line-level diff highlighting when the
        // counterpart exists. For modified items we colour added/removed lines.
        const counterpart = side === "source" ? item.target : item.source;
        if (item.kind === "modified" && typeof content === "string"
                && typeof counterpart === "string") {
            preEl.innerHTML = renderLineDiff(content, counterpart, side);
        } else if (item.kind === "added" && side === "source"
                && typeof content === "string") {
            preEl.innerHTML = String(content).split("\n").map(l =>
                `<span class="slls-md-line added">${escapeHtml(l)}</span>`
            ).join("\n");
        } else if (item.kind === "removed" && side === "target"
                && typeof content === "string") {
            preEl.innerHTML = String(content).split("\n").map(l =>
                `<span class="slls-md-line removed">${escapeHtml(l)}</span>`
            ).join("\n");
        } else {
            preEl.textContent = typeof content === "string"
                ? content
                : JSON.stringify(content, null, 2);
        }
    }

    // Compute a simple LCS-based line diff so that, when viewing the source
    // pane, lines present only in source are highlighted as "added" (green)
    // and lines only present in target are surfaced as "removed" (red) on
    // the target pane. This keeps both panes informative without requiring
    // a full diff library.
    function renderLineDiff(a, b, side) {
        const A = String(a).split("\n");
        const B = String(b).split("\n");
        // For very large files, fall back to plain text.
        if (A.length * B.length > 250000) {
            return A.map(l => escapeHtml(l)).join("\n");
        }
        // LCS table
        const m = A.length, n = B.length;
        const dp = Array.from({ length: m + 1 }, () => new Int32Array(n + 1));
        for (let i = m - 1; i >= 0; i--) {
            for (let j = n - 1; j >= 0; j--) {
                if (A[i] === B[j]) dp[i][j] = dp[i + 1][j + 1] + 1;
                else dp[i][j] = Math.max(dp[i + 1][j], dp[i][j + 1]);
            }
        }
        // Walk to produce ops
        const ops = []; // { tag: 'eq'|'add'|'del', line }
        let i = 0, j = 0;
        while (i < m && j < n) {
            if (A[i] === B[j]) { ops.push({ tag: "eq", line: A[i] }); i++; j++; }
            else if (dp[i + 1][j] >= dp[i][j + 1]) { ops.push({ tag: "del", line: A[i] }); i++; }
            else { ops.push({ tag: "add", line: B[j] }); j++; }
        }
        while (i < m) { ops.push({ tag: "del", line: A[i++] }); }
        while (j < n) { ops.push({ tag: "add", line: B[j++] }); }
        // On the "source" pane we show A; 'del' lines are unique to source -> added on this side.
        // On the "target" pane we show B; 'add' lines are unique to target -> removed-from-source.
        const out = [];
        for (const op of ops) {
            if (side === "source") {
                if (op.tag === "eq") out.push(`<span class="slls-md-line context">${escapeHtml(op.line)}</span>`);
                else if (op.tag === "del") out.push(`<span class="slls-md-line added">${escapeHtml(op.line)}</span>`);
            } else {
                if (op.tag === "eq") out.push(`<span class="slls-md-line context">${escapeHtml(op.line)}</span>`);
                else if (op.tag === "add") out.push(`<span class="slls-md-line removed">${escapeHtml(op.line)}</span>`);
            }
        }
        return out.join("\n");
    }

    function renderFooter() {
        renderDir();
        const n = selection.size;
        applyBtn.textContent = n > 0 ? `Apply ${n} selected` : "Apply selected";
        applyBtn.disabled = n === 0 || isBusy();
    }

    function renderBusy() {
        if (isBusy()) root.classList.add("slls-md-busy");
        else root.classList.remove("slls-md-busy");
        renderFooter();
    }

    function renderStatus() {
        const s = model.get("status") || {};
        status.classList.remove("show", "success", "error", "info", "warning");
        if (!s.message) return;
        status.classList.add("show", s.kind || "info");
        status.textContent = s.message;
    }

    function triggerAction(payload) {
        model.set("pending_action", payload);
        model.set("run", (model.get("run") || 0) + 1);
        model.save_changes();
    }

    // ---------- Wire up ----------
    model.on("change:diff", () => {
        // Drop any selection / focus that no longer exists
        const validIds = new Set();
        for (const cat of (getDiff().categories || [])) {
            for (const it of (cat.items || [])) validIds.add(it.id);
        }
        for (const id of Array.from(selection)) {
            if (!validIds.has(id)) selection.delete(id);
        }
        if (focusedId && !validIds.has(focusedId)) focusedId = null;
        renderCountPills();
        renderTree();
        renderPreview();
        renderFooter();
    });
    model.on("change:status", renderStatus);
    model.on("change:busy", renderBusy);
    model.on("change:format", () => { renderSeg(); renderSubtitle(); });
    model.on("change:source_label", () => { renderSubtitle(); renderFooter(); });
    model.on("change:target_label", () => { renderSubtitle(); renderFooter(); });

    // Initial render
    renderSubtitle();
    renderSeg();
    renderCountPills();
    renderTree();
    renderPreview();
    renderFooter();
    renderBusy();
    renderStatus();
}
export default { render };
"""


# ---------------------------------------------------------------------------
# Diff engine
# ---------------------------------------------------------------------------
# The diff engine produces a JSON-serialisable structure consumed by the
# widget. Two diff modes are supported:
#
#   * TMSL / .bim — structural diff against the model.bim JSON document.
#     Diffs are produced per logical object category (tables, columns,
#     measures, hierarchies, partitions, roles, perspectives, cultures,
#     relationships, expressions, dataSources, queryGroups, plus model-level
#     scalar properties) so reviewers can reason about high-level model
#     changes the same way they would in Tabular Editor.
#
#   * TMDL / .tmdl — per-file diff over the collection of TMDL files. Each
#     file is one diff entry whose source/target content is the raw text.
#
# Each diff entry carries enough information for the apply step to perform
# a deterministic patch of the destination definition.


def _compute_bim_diff(
    source_bim: dict, target_bim: dict
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Compute a structural BIM diff.

    Returns
    -------
    tuple
        ``(diff_payload, patch_index)``. ``diff_payload`` is the widget-ready
        diff structure; ``patch_index`` is a list of dicts describing how to
        patch the destination BIM for each diff id (used by the apply step).
    """
    import json as _json

    s_model = (source_bim or {}).get("model", {}) or {}
    t_model = (target_bim or {}).get("model", {}) or {}

    categories: List[Dict[str, Any]] = []
    patch_index: List[Dict[str, Any]] = []

    def _pretty(obj: Any) -> str:
        if obj is None:
            return ""
        if isinstance(obj, str):
            return obj
        return _json.dumps(obj, indent=2, sort_keys=True, default=str)

    def _by_name(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for it in items or []:
            n = it.get("name")
            if n:
                out[n] = it
        return out

    def _diff_named(
        category_key: str,
        category_label: str,
        parent_path: str,
        source_list: List[Dict[str, Any]],
        target_list: List[Dict[str, Any]],
        patch_kind: str,
        patch_container_path: List[Any],
    ) -> Dict[str, Any]:
        """Diff a list of named objects (each must have a unique ``name``)."""
        s = _by_name(source_list)
        t = _by_name(target_list)
        all_names = sorted(set(s) | set(t))
        items: List[Dict[str, Any]] = []
        for name in all_names:
            sv = s.get(name)
            tv = t.get(name)
            display_path = f"{parent_path}.{name}" if parent_path else name
            item_id = f"{category_key}::{display_path}"
            if sv is not None and tv is None:
                items.append({
                    "id": item_id,
                    "name": name,
                    "path": display_path,
                    "kind": "added",
                    "summary": "",
                    "source": _pretty(sv),
                    "target": None,
                })
                patch_index.append({
                    "id": item_id,
                    "kind": patch_kind,
                    "diff_kind": "added",
                    "container_path": patch_container_path,
                    "name": name,
                    "source_value": sv,
                    "target_value": None,
                })
            elif sv is None and tv is not None:
                items.append({
                    "id": item_id,
                    "name": name,
                    "path": display_path,
                    "kind": "removed",
                    "summary": "",
                    "source": None,
                    "target": _pretty(tv),
                })
                patch_index.append({
                    "id": item_id,
                    "kind": patch_kind,
                    "diff_kind": "removed",
                    "container_path": patch_container_path,
                    "name": name,
                    "source_value": None,
                    "target_value": tv,
                })
            else:
                if sv == tv:
                    continue
                items.append({
                    "id": item_id,
                    "name": name,
                    "path": display_path,
                    "kind": "modified",
                    "summary": _summarise_modification(sv, tv),
                    "source": _pretty(sv),
                    "target": _pretty(tv),
                })
                patch_index.append({
                    "id": item_id,
                    "kind": patch_kind,
                    "diff_kind": "modified",
                    "container_path": patch_container_path,
                    "name": name,
                    "source_value": sv,
                    "target_value": tv,
                })
        return {"key": category_key, "label": category_label, "items": items}

    # --- Tables (and per-table nested categories) ---
    s_tables = _by_name(s_model.get("tables") or [])
    t_tables = _by_name(t_model.get("tables") or [])
    table_items: List[Dict[str, Any]] = []
    column_items: List[Dict[str, Any]] = []
    measure_items: List[Dict[str, Any]] = []
    hierarchy_items: List[Dict[str, Any]] = []
    partition_items: List[Dict[str, Any]] = []
    calc_group_items: List[Dict[str, Any]] = []

    all_table_names = sorted(set(s_tables) | set(t_tables))
    for tn in all_table_names:
        sv = s_tables.get(tn)
        tv = t_tables.get(tn)
        tid = f"tables::{tn}"
        if sv is not None and tv is None:
            table_items.append({
                "id": tid, "name": tn, "path": tn,
                "kind": "added", "summary": "new table",
                "source": _pretty(sv), "target": None,
            })
            patch_index.append({
                "id": tid, "kind": "table", "diff_kind": "added",
                "container_path": ["tables"], "name": tn,
                "source_value": sv, "target_value": None,
            })
            continue
        if sv is None and tv is not None:
            table_items.append({
                "id": tid, "name": tn, "path": tn,
                "kind": "removed", "summary": "table removed",
                "source": None, "target": _pretty(tv),
            })
            patch_index.append({
                "id": tid, "kind": "table", "diff_kind": "removed",
                "container_path": ["tables"], "name": tn,
                "source_value": None, "target_value": tv,
            })
            continue

        # Existing in both — drill into sub-collections.
        if sv == tv:
            continue

        # Per-sub-collection diffs
        for sub_key, sub_label, sub_items_acc, patch_kind in [
            ("columns", "columns", column_items, "table_child_columns"),
            ("measures", "measures", measure_items, "table_child_measures"),
            ("hierarchies", "hierarchies", hierarchy_items, "table_child_hierarchies"),
            ("partitions", "partitions", partition_items, "table_child_partitions"),
        ]:
            sub_diff = _diff_named(
                category_key=sub_key,
                category_label=sub_label.capitalize(),
                parent_path=tn,
                source_list=sv.get(sub_key) or [],
                target_list=tv.get(sub_key) or [],
                patch_kind=patch_kind,
                patch_container_path=["tables", tn, sub_key],
            )
            for it in sub_diff["items"]:
                sub_items_acc.append(it)

        # Calculation groups (per-table singleton)
        s_cg = sv.get("calculationGroup")
        t_cg = tv.get("calculationGroup")
        if s_cg != t_cg:
            cg_id = f"calculationGroups::{tn}"
            if s_cg is not None and t_cg is None:
                kind = "added"
            elif s_cg is None and t_cg is not None:
                kind = "removed"
            else:
                kind = "modified"
            calc_group_items.append({
                "id": cg_id, "name": tn, "path": f"{tn}.calculationGroup",
                "kind": kind, "summary": _summarise_modification(s_cg, t_cg),
                "source": _pretty(s_cg) if s_cg is not None else None,
                "target": _pretty(t_cg) if t_cg is not None else None,
            })
            patch_index.append({
                "id": cg_id, "kind": "table_calculation_group",
                "diff_kind": kind,
                "container_path": ["tables", tn], "name": "calculationGroup",
                "source_value": s_cg, "target_value": t_cg,
            })

        # Table-level properties diff (excluding the sub-collections we
        # already diffed). If anything else differs, surface a "modified"
        # entry at the table level so users can apply the whole table.
        nested_keys = {"columns", "measures", "hierarchies", "partitions",
                       "calculationGroup"}
        sv_props = {k: v for k, v in sv.items() if k not in nested_keys}
        tv_props = {k: v for k, v in tv.items() if k not in nested_keys}
        if sv_props != tv_props:
            table_items.append({
                "id": tid, "name": tn, "path": tn,
                "kind": "modified",
                "summary": _summarise_modification(sv_props, tv_props),
                "source": _pretty(sv), "target": _pretty(tv),
            })
            patch_index.append({
                "id": tid, "kind": "table", "diff_kind": "modified",
                "container_path": ["tables"], "name": tn,
                "source_value": sv, "target_value": tv,
            })

    if table_items:
        categories.append({"key": "tables", "label": "Tables", "items": table_items})
    if column_items:
        categories.append({"key": "columns", "label": "Columns", "items": column_items})
    if measure_items:
        categories.append({"key": "measures", "label": "Measures", "items": measure_items})
    if hierarchy_items:
        categories.append({"key": "hierarchies", "label": "Hierarchies", "items": hierarchy_items})
    if partition_items:
        categories.append({"key": "partitions", "label": "Partitions", "items": partition_items})
    if calc_group_items:
        categories.append({
            "key": "calculationGroups",
            "label": "Calculation groups",
            "items": calc_group_items,
        })

    # --- Top-level named collections ---
    for key, label in [
        ("relationships", "Relationships"),
        ("perspectives", "Perspectives"),
        ("roles", "Roles"),
        ("cultures", "Cultures"),
        ("expressions", "Expressions"),
        ("dataSources", "Data sources"),
        ("queryGroups", "Query groups"),
        ("annotations", "Model annotations"),
    ]:
        s_list = s_model.get(key) or []
        t_list = t_model.get(key) or []
        if not s_list and not t_list:
            continue
        cat = _diff_named(
            category_key=key,
            category_label=label,
            parent_path="",
            source_list=s_list,
            target_list=t_list,
            patch_kind=f"model_collection:{key}",
            patch_container_path=[key],
        )
        if cat["items"]:
            categories.append(cat)

    # --- Top-level scalar properties (everything not a collection) ---
    collection_keys = {
        "tables", "relationships", "perspectives", "roles", "cultures",
        "expressions", "dataSources", "queryGroups", "annotations",
    }
    scalar_items: List[Dict[str, Any]] = []
    for k in sorted(set(s_model) | set(t_model)):
        if k in collection_keys:
            continue
        sv = s_model.get(k)
        tv = t_model.get(k)
        if sv == tv:
            continue
        sid = f"modelProperties::{k}"
        if k not in t_model:
            kind = "added"
        elif k not in s_model:
            kind = "removed"
        else:
            kind = "modified"
        scalar_items.append({
            "id": sid, "name": k, "path": f"model.{k}",
            "kind": kind, "summary": _summarise_modification(sv, tv),
            "source": _pretty(sv) if sv is not None else None,
            "target": _pretty(tv) if tv is not None else None,
        })
        patch_index.append({
            "id": sid, "kind": "model_scalar", "diff_kind": kind,
            "container_path": [], "name": k,
            "source_value": sv, "target_value": tv,
        })
    if scalar_items:
        categories.append({
            "key": "modelProperties",
            "label": "Model properties",
            "items": scalar_items,
        })

    # --- Top-level BIM properties (outside model{}) ---
    bim_top_items: List[Dict[str, Any]] = []
    for k in sorted(set(source_bim or {}) | set(target_bim or {})):
        if k == "model":
            continue
        sv = (source_bim or {}).get(k)
        tv = (target_bim or {}).get(k)
        if sv == tv:
            continue
        bid = f"bimProperties::{k}"
        if sv is not None and tv is None:
            kind = "added"
        elif sv is None and tv is not None:
            kind = "removed"
        else:
            kind = "modified"
        bim_top_items.append({
            "id": bid, "name": k, "path": k,
            "kind": kind, "summary": _summarise_modification(sv, tv),
            "source": _pretty(sv) if sv is not None else None,
            "target": _pretty(tv) if tv is not None else None,
        })
        patch_index.append({
            "id": bid, "kind": "bim_top", "diff_kind": kind,
            "container_path": [], "name": k,
            "source_value": sv, "target_value": tv,
        })
    if bim_top_items:
        categories.append({
            "key": "bimProperties",
            "label": "Document properties",
            "items": bim_top_items,
        })

    diff_payload = {"format": "TMSL", "categories": categories}
    return diff_payload, patch_index


def _summarise_modification(sv: Any, tv: Any) -> str:
    """Short, human-friendly summary of how two values differ."""
    if isinstance(sv, dict) and isinstance(tv, dict):
        keys = sorted(set(sv) | set(tv))
        diffs = [k for k in keys if sv.get(k) != tv.get(k)]
        if not diffs:
            return ""
        if len(diffs) <= 3:
            return ", ".join(diffs) + " changed"
        return f"{len(diffs)} properties changed"
    if isinstance(sv, list) and isinstance(tv, list):
        return f"{len(sv)} → {len(tv)} items"
    return "value changed"


def _compute_tmdl_diff(
    source_files: List[Dict[str, str]],
    target_files: List[Dict[str, str]],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Compute a per-file diff for a TMDL definition."""
    s_map = {f["file_name"]: f["content"] for f in (source_files or [])}
    t_map = {f["file_name"]: f["content"] for f in (target_files or [])}

    # Group files by top-level directory for nicer browsing.
    def _category(path: str) -> Tuple[str, str]:
        head = path.split("/", 1)[0] if "/" in path else ""
        if head == "definition":
            sub = path.split("/")[1] if path.count("/") >= 1 else ""
            if sub in ("tables", "relationships", "cultures", "perspectives",
                       "roles", "expressions", "dataSources", "queryGroups"):
                return (f"tmdl:{sub}", f"TMDL — {sub}")
            return ("tmdl:definition", "TMDL — definition")
        return ("tmdl:other", "TMDL — other files")

    grouped: Dict[str, Dict[str, Any]] = {}
    patch_index: List[Dict[str, Any]] = []
    all_paths = sorted(set(s_map) | set(t_map))
    for path in all_paths:
        sv = s_map.get(path)
        tv = t_map.get(path)
        if sv == tv:
            continue
        if sv is not None and tv is None:
            kind = "added"
        elif sv is None and tv is not None:
            kind = "removed"
        else:
            kind = "modified"

        cat_key, cat_label = _category(path)
        if cat_key not in grouped:
            grouped[cat_key] = {"key": cat_key, "label": cat_label, "items": []}

        item_id = f"tmdlfile::{path}"
        summary = ""
        if kind == "modified":
            s_lines = (sv or "").splitlines()
            t_lines = (tv or "").splitlines()
            summary = f"{len(s_lines)} → {len(t_lines)} lines"

        grouped[cat_key]["items"].append({
            "id": item_id,
            "name": path.rsplit("/", 1)[-1],
            "path": path,
            "kind": kind,
            "summary": summary,
            "source": sv,
            "target": tv,
        })
        patch_index.append({
            "id": item_id,
            "kind": "tmdl_file",
            "diff_kind": kind,
            "path": path,
            "source_value": sv,
            "target_value": tv,
        })

    categories = [grouped[k] for k in sorted(grouped)]
    diff_payload = {"format": "TMDL", "categories": categories}
    return diff_payload, patch_index


# ---------------------------------------------------------------------------
# Apply engine
# ---------------------------------------------------------------------------
# Given a list of patch entries and a direction (source -> target or
# target -> source), produce a patched destination definition. The patched
# definition is then pushed to the destination via update_semantic_model_from_bim
# (for BIM mode) or via a TMDL updateDefinition payload.


def _patch_bim(
    source_bim: dict,
    target_bim: dict,
    patch_index: List[Dict[str, Any]],
    selected_ids: List[str],
    direction: str,
) -> dict:
    """Apply selected patch entries to the destination BIM.

    ``direction`` is ``"s2t"`` (apply source changes onto target) or
    ``"t2s"`` (apply target changes onto source).
    """
    import copy as _copy

    if direction == "s2t":
        dest = _copy.deepcopy(target_bim or {})
        source_side = source_bim or {}
    else:
        dest = _copy.deepcopy(source_bim or {})
        source_side = target_bim or {}

    if "model" not in dest:
        dest["model"] = {}
    if "model" not in source_side:
        source_side = dict(source_side)
        source_side["model"] = {}

    selected = set(selected_ids or [])
    index_by_id = {p["id"]: p for p in patch_index}

    def _find_named(items: List[Dict[str, Any]], name: str) -> int:
        for i, it in enumerate(items or []):
            if it.get("name") == name:
                return i
        return -1

    def _ensure_list(container: dict, key: str) -> list:
        v = container.get(key)
        if not isinstance(v, list):
            v = []
            container[key] = v
        return v

    def _resolve_container(
        root: dict, container_path: List[Any], create: bool = False
    ) -> Optional[dict]:
        """Walk down ``container_path``. Path alternates dict-key, list-name."""
        node: Any = root
        for i, segment in enumerate(container_path):
            if isinstance(node, dict):
                if segment not in node:
                    if not create:
                        return None
                    # Determine if next segment expects a list of named objects
                    if i + 1 < len(container_path):
                        node[segment] = []
                    else:
                        node[segment] = {}
                node = node[segment]
            elif isinstance(node, list):
                # Find item by name
                idx = _find_named(node, segment)
                if idx < 0:
                    if not create:
                        return None
                    new = {"name": segment}
                    node.append(new)
                    idx = len(node) - 1
                node = node[idx]
            else:
                return None
        return node if isinstance(node, dict) else None

    # The patch_index was built relative to (source_bim, target_bim). When the
    # direction is reversed (t2s) we swap the "source_value"/"target_value"
    # interpretation so that the "incoming" side is the destination's source.
    for pid in selected:
        entry = index_by_id.get(pid)
        if not entry:
            continue
        kind = entry["kind"]
        diff_kind = entry["diff_kind"]
        if direction == "s2t":
            incoming = entry.get("source_value")
        else:
            incoming = entry.get("target_value")
            # Mirror the diff kind for the destination perspective.
            if diff_kind == "added":
                diff_kind = "removed"
            elif diff_kind == "removed":
                diff_kind = "added"

        if kind == "bim_top":
            name = entry["name"]
            if diff_kind == "removed":
                dest.pop(name, None)
            else:
                dest[name] = _deep_copy(incoming)
            continue

        if kind == "model_scalar":
            name = entry["name"]
            if diff_kind == "removed":
                dest["model"].pop(name, None)
            else:
                dest["model"][name] = _deep_copy(incoming)
            continue

        # All remaining patch kinds operate inside dest["model"].
        if kind == "table":
            tables = _ensure_list(dest["model"], "tables")
            idx = _find_named(tables, entry["name"])
            if diff_kind == "removed":
                if idx >= 0:
                    tables.pop(idx)
            else:
                new_val = _deep_copy(incoming)
                if idx >= 0:
                    tables[idx] = new_val
                else:
                    tables.append(new_val)
            continue

        if kind in (
            "table_child_columns",
            "table_child_measures",
            "table_child_hierarchies",
            "table_child_partitions",
        ):
            # container_path is ["tables", <table>, <sub_key>]
            cp = entry["container_path"]
            tables = _ensure_list(dest["model"], "tables")
            tidx = _find_named(tables, cp[1])
            if tidx < 0:
                if diff_kind == "removed":
                    continue
                tables.append({"name": cp[1]})
                tidx = len(tables) - 1
            table_obj = tables[tidx]
            sub = _ensure_list(table_obj, cp[2])
            idx = _find_named(sub, entry["name"])
            if diff_kind == "removed":
                if idx >= 0:
                    sub.pop(idx)
            else:
                new_val = _deep_copy(incoming)
                if idx >= 0:
                    sub[idx] = new_val
                else:
                    sub.append(new_val)
            continue

        if kind == "table_calculation_group":
            cp = entry["container_path"]  # ["tables", <table>]
            tables = _ensure_list(dest["model"], "tables")
            tidx = _find_named(tables, cp[1])
            if tidx < 0:
                if diff_kind == "removed":
                    continue
                tables.append({"name": cp[1]})
                tidx = len(tables) - 1
            table_obj = tables[tidx]
            if diff_kind == "removed":
                table_obj.pop("calculationGroup", None)
            else:
                table_obj["calculationGroup"] = _deep_copy(incoming)
            continue

        if kind.startswith("model_collection:"):
            collection_key = kind.split(":", 1)[1]
            coll = _ensure_list(dest["model"], collection_key)
            idx = _find_named(coll, entry["name"])
            if diff_kind == "removed":
                if idx >= 0:
                    coll.pop(idx)
            else:
                new_val = _deep_copy(incoming)
                if idx >= 0:
                    coll[idx] = new_val
                else:
                    coll.append(new_val)
            continue

    return dest


def _patch_tmdl(
    source_files: List[Dict[str, str]],
    target_files: List[Dict[str, str]],
    patch_index: List[Dict[str, Any]],
    selected_ids: List[str],
    direction: str,
) -> List[Dict[str, str]]:
    """Apply selected per-file patches and return the patched destination
    file list (suitable for `updateDefinition`)."""
    if direction == "s2t":
        dest = {f["file_name"]: f["content"] for f in (target_files or [])}
    else:
        dest = {f["file_name"]: f["content"] for f in (source_files or [])}

    index_by_id = {p["id"]: p for p in patch_index}
    selected = set(selected_ids or [])

    for pid in selected:
        entry = index_by_id.get(pid)
        if not entry:
            continue
        path = entry["path"]
        diff_kind = entry["diff_kind"]
        if direction == "s2t":
            incoming = entry.get("source_value")
        else:
            incoming = entry.get("target_value")
            if diff_kind == "added":
                diff_kind = "removed"
            elif diff_kind == "removed":
                diff_kind = "added"

        if diff_kind == "removed":
            dest.pop(path, None)
        else:
            dest[path] = incoming if incoming is not None else ""

    return [{"file_name": p, "content": c} for p, c in sorted(dest.items())]


def _deep_copy(obj: Any) -> Any:
    """Lightweight deep copy for JSON-shaped values."""
    import copy as _copy
    return _copy.deepcopy(obj)


def _update_semantic_model_from_tmdl(
    dataset: str | UUID,
    workspace: Optional[str | UUID],
    files: List[Dict[str, str]],
) -> None:
    """Push a set of TMDL files to a semantic model via updateDefinition."""
    from sempy_labs._helper_functions import (
        resolve_workspace_name_and_id,
        resolve_dataset_name_and_id,
        _base_api,
    )

    (_, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    parts = []
    for f in files:
        # Match the encoding used by `get_semantic_model_definition` (raw text
        # is returned base64-decoded; here we re-encode raw text to base64).
        import base64

        path = f["file_name"]
        text = f.get("content") or ""
        payload = base64.b64encode(text.encode("utf-8")).decode("utf-8")
        parts.append({
            "path": path,
            "payload": payload,
            "payloadType": "InlineBase64",
        })

    payload = {
        "displayName": dataset_name,
        "definition": {"parts": parts},
    }
    _base_api(
        request=(
            f"v1/workspaces/{workspace_id}/semanticModels/{dataset_id}"
            f"/updateDefinition?updateMetadata=true"
        ),
        payload=payload,
        method="post",
        lro_return_status_code=True,
        status_codes=None,
    )


def _load_definition(
    dataset: str | UUID,
    workspace: Optional[str | UUID],
    format: str,
) -> Any:
    """Fetch the source/target definition in the requested format."""
    from sempy_labs._generate_semantic_model import (
        get_semantic_model_bim,
        get_semantic_model_definition,
    )

    fmt = (format or "TMSL").upper()
    if fmt == "TMSL":
        return get_semantic_model_bim(dataset=dataset, workspace=workspace)
    if fmt == "TMDL":
        return get_semantic_model_definition(
            dataset=dataset,
            workspace=workspace,
            format="TMDL",
            return_dataframe=False,
        )
    raise ValueError(f"Unsupported format '{format}'. Use 'TMSL' or 'TMDL'.")


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


@log
def semantic_model_diff(
    source_dataset: str | UUID,
    target_dataset: str | UUID,
    source_workspace: Optional[str | UUID] = None,
    target_workspace: Optional[str | UUID] = None,
    format: str = "TMSL",
    dark_mode: bool = False,
):
    """
    Generates an interactive UI for visualising the differences between two
    semantic models and selectively merging changes between them.

    The editor is rendered as an `anywidget` widget — designed to feel like
    a state-of-the-art CI/CD diff/merge tool tailored for Fabric / Power BI
    semantic modelers. Differences are categorised (tables, columns,
    measures, hierarchies, partitions, relationships, perspectives, roles,
    cultures, expressions, data sources, model-level properties, …) and
    colour-coded (green for additions, red for removals, amber for
    modifications). Each diff entry can be inspected side-by-side and
    selected for merging from source → target or target → source.

    Service Principal Authentication is supported (see
    `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_
    for examples).

    Parameters
    ----------
    source_dataset : str | uuid.UUID
        Name or ID of the source semantic model.
    target_dataset : str | uuid.UUID
        Name or ID of the target semantic model.
    source_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID containing the source semantic model.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    target_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID containing the target semantic model.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    format : str, default="TMSL"
        The comparison format. Valid options are ``"TMSL"`` (compare the
        ``model.bim`` files structurally, object-by-object), ``"BIM"`` (an
        alias for ``"TMSL"``) or ``"TMDL"`` (compare the collection of
        TMDL files file-by-file with line-level highlighting).
    dark_mode : bool, default=False
        If True, renders the editor with a dark color theme. If False,
        renders with a light color theme.
    """

    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "The 'semantic_model_diff' function requires the 'anywidget' package. "
            "Install it with: pip install anywidget"
        ) from e

    from IPython.display import display
    from sempy_labs._generate_semantic_model import (
        update_semantic_model_from_bim,
    )
    from sempy_labs._helper_functions import (
        resolve_workspace_name_and_id,
        resolve_dataset_name_and_id,
    )

    fmt = (format or "TMSL").upper()
    if fmt == "BIM":
        fmt = "TMSL"
    if fmt not in ("TMSL", "TMDL"):
        raise ValueError(
            f"Invalid format '{format}'. Valid options are 'TMSL' or 'TMDL'."
        )

    # Resolve labels up-front so the widget header is informative.
    (s_ws_name, s_ws_id) = resolve_workspace_name_and_id(source_workspace)
    (s_ds_name, s_ds_id) = resolve_dataset_name_and_id(source_dataset, s_ws_id)
    (t_ws_name, t_ws_id) = resolve_workspace_name_and_id(target_workspace)
    (t_ds_name, t_ds_id) = resolve_dataset_name_and_id(target_dataset, t_ws_id)

    source_label = f"{s_ds_name} ({s_ws_name})"
    target_label = f"{t_ds_name} ({t_ws_name})"

    # Cache of (format -> (definition_source, definition_target,
    # diff_payload, patch_index)). Avoids refetching when toggling formats.
    cache: Dict[str, Dict[str, Any]] = {}

    def _load_and_diff(active_format: str) -> Dict[str, Any]:
        if active_format in cache:
            return cache[active_format]
        s_def = _load_definition(s_ds_id, s_ws_id, active_format)
        t_def = _load_definition(t_ds_id, t_ws_id, active_format)
        if active_format == "TMSL":
            diff_payload, patch_index = _compute_bim_diff(s_def, t_def)
        else:
            diff_payload, patch_index = _compute_tmdl_diff(s_def, t_def)
        cache[active_format] = {
            "source_def": s_def,
            "target_def": t_def,
            "diff": diff_payload,
            "patch_index": patch_index,
        }
        return cache[active_format]

    initial = _load_and_diff(fmt)

    class SemanticModelDiffWidget(anywidget.AnyWidget):
        _esm = _WIDGET_JS
        _css = _WIDGET_CSS

        diff = traitlets.Dict().tag(sync=True)
        format = traitlets.Unicode("TMSL").tag(sync=True)
        source_label = traitlets.Unicode("").tag(sync=True)
        target_label = traitlets.Unicode("").tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)
        busy = traitlets.Bool(False).tag(sync=True)

    widget = SemanticModelDiffWidget(
        diff=initial["diff"],
        format=fmt,
        source_label=source_label,
        target_label=target_label,
        status={},
        pending_action={},
        run=0,
        dark_mode=bool(dark_mode),
        busy=False,
    )

    def _refresh(active_format: str):
        # Invalidate cache for the active format and reload.
        cache.pop(active_format, None)
        try:
            widget.busy = True
            entry = _load_and_diff(active_format)
            widget.diff = entry["diff"]
            counts = _diff_counts(entry["diff"])
            widget.status = {
                "message": (
                    f"Loaded diff in {active_format}: "
                    f"{counts['added']} added, "
                    f"{counts['removed']} removed, "
                    f"{counts['modified']} modified."
                ),
                "kind": "info" if sum(counts.values()) else "success",
            }
        finally:
            widget.busy = False

    def _on_run(change):
        data = dict(widget.pending_action or {})
        action = data.get("action")
        if not action:
            return
        try:
            if action == "refresh":
                _refresh(widget.format)
                return

            if action == "set_format":
                new_fmt = (data.get("format") or "TMSL").upper()
                if new_fmt not in ("TMSL", "TMDL"):
                    widget.status = {
                        "message": f"Unknown format '{new_fmt}'.",
                        "kind": "error",
                    }
                    return
                widget.busy = True
                try:
                    entry = _load_and_diff(new_fmt)
                    widget.diff = entry["diff"]
                    widget.format = new_fmt
                    counts = _diff_counts(entry["diff"])
                    widget.status = {
                        "message": (
                            f"Switched to {new_fmt}: "
                            f"{counts['added']} added, "
                            f"{counts['removed']} removed, "
                            f"{counts['modified']} modified."
                        ),
                        "kind": "info" if sum(counts.values()) else "success",
                    }
                finally:
                    widget.busy = False
                return

            if action == "apply":
                direction = data.get("direction") or "s2t"
                selected = data.get("selected") or []
                if not selected:
                    widget.status = {
                        "message": "Nothing selected to apply.",
                        "kind": "warning",
                    }
                    return

                active_format = widget.format
                entry = cache.get(active_format) or _load_and_diff(active_format)
                widget.busy = True
                try:
                    if active_format == "TMSL":
                        patched = _patch_bim(
                            source_bim=entry["source_def"],
                            target_bim=entry["target_def"],
                            patch_index=entry["patch_index"],
                            selected_ids=selected,
                            direction=direction,
                        )
                        if direction == "s2t":
                            update_semantic_model_from_bim(
                                dataset=t_ds_id,
                                bim_file=patched,
                                workspace=t_ws_id,
                            )
                            dest_label = target_label
                        else:
                            update_semantic_model_from_bim(
                                dataset=s_ds_id,
                                bim_file=patched,
                                workspace=s_ws_id,
                            )
                            dest_label = source_label
                    else:
                        patched_files = _patch_tmdl(
                            source_files=entry["source_def"],
                            target_files=entry["target_def"],
                            patch_index=entry["patch_index"],
                            selected_ids=selected,
                            direction=direction,
                        )
                        if direction == "s2t":
                            _update_semantic_model_from_tmdl(
                                dataset=t_ds_id,
                                workspace=t_ws_id,
                                files=patched_files,
                            )
                            dest_label = target_label
                        else:
                            _update_semantic_model_from_tmdl(
                                dataset=s_ds_id,
                                workspace=s_ws_id,
                                files=patched_files,
                            )
                            dest_label = source_label

                    # Invalidate caches and recompute diff for both formats
                    # that may now be stale.
                    cache.clear()
                    refreshed = _load_and_diff(active_format)
                    widget.diff = refreshed["diff"]
                    counts = _diff_counts(refreshed["diff"])
                    widget.status = {
                        "message": (
                            f"Applied {len(selected)} change"
                            + ("" if len(selected) == 1 else "s")
                            + f" to '{dest_label}'. "
                            + (
                                f"{counts['added']} added, "
                                f"{counts['removed']} removed, "
                                f"{counts['modified']} modified remaining."
                            )
                        ),
                        "kind": "success",
                    }
                finally:
                    widget.busy = False
                return

        except Exception as e:
            widget.busy = False
            widget.status = {
                "message": f"Error: {type(e).__name__}: {e}",
                "kind": "error",
            }

    widget.observe(_on_run, names=["run"])

    # Keep a reference on the widget so the Python-side observer is not
    # garbage-collected after this function returns.
    widget._slls_keepalive_observer = _on_run  # type: ignore[attr-defined]
    display(widget)


def _diff_counts(diff_payload: Dict[str, Any]) -> Dict[str, int]:
    out = {"added": 0, "removed": 0, "modified": 0}
    for cat in (diff_payload or {}).get("categories") or []:
        for it in cat.get("items") or []:
            k = it.get("kind")
            if k in out:
                out[k] += 1
    return out
