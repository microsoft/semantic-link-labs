from typing import Optional
from uuid import UUID
import difflib
from sempy._utils._log import log


def _split_lines(s: str) -> list:
    """Split a string on line boundaries, preserving blank lines and dropping the
    trailing newline (matching :py:meth:`str.splitlines`)."""
    return (s or "").splitlines()


def _join_lines(lines: list) -> str:
    """Join a list of lines back into text, appending a trailing newline if the
    list is non-empty (matches typical text-file conventions)."""
    if not lines:
        return ""
    return "\n".join(lines) + "\n"


def _compute_file_hunks(src_text: str, tgt_text: str) -> dict:
    """Compute hunk-level differences between two text blobs.

    Returns a dict with the per-hunk opcodes (``equal``, ``replace``, ``delete``,
    ``insert``) plus aggregate ``additions`` / ``deletions`` counts.
    """
    a = _split_lines(src_text)
    b = _split_lines(tgt_text)
    sm = difflib.SequenceMatcher(a=a, b=b, autojunk=False)
    hunks = []
    additions = 0
    deletions = 0
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        src_lines = a[i1:i2]
        tgt_lines = b[j1:j2]
        if tag != "equal":
            deletions += len(src_lines)
            additions += len(tgt_lines)
        hunks.append({
            "type": tag,  # "equal" | "replace" | "delete" | "insert"
            "source_start": i1,
            "target_start": j1,
            "source_lines": src_lines,
            "target_lines": tgt_lines,
        })
    return {
        "hunks": hunks,
        "additions": additions,
        "deletions": deletions,
        "source_lines": a,
        "target_lines": b,
    }


def _compute_diff(source_def: dict, target_def: dict, fmt: str) -> dict:
    """Compute a file-level diff between two semantic-model definitions.

    Each side is expected to have the shape
    ``{"files": [{"path": str, "content": str}, ...]}``. Returns a dict with a
    ``files`` list — one entry per unique path — annotated with a ``status``
    of ``added`` / ``removed`` / ``modified`` / ``unchanged``.
    """
    src_map = {f["path"]: f["content"] for f in source_def.get("files", [])}
    tgt_map = {f["path"]: f["content"] for f in target_def.get("files", [])}
    paths = sorted(set(src_map.keys()) | set(tgt_map.keys()))
    files = []
    for p in paths:
        if p in src_map and p in tgt_map:
            if src_map[p] == tgt_map[p]:
                files.append({
                    "path": p,
                    "status": "unchanged",
                    "hunks": [],
                    "additions": 0,
                    "deletions": 0,
                    "source_lines": _split_lines(src_map[p]),
                    "target_lines": _split_lines(tgt_map[p]),
                })
            else:
                h = _compute_file_hunks(src_map[p], tgt_map[p])
                files.append({
                    "path": p,
                    "status": "modified",
                    "hunks": h["hunks"],
                    "additions": h["additions"],
                    "deletions": h["deletions"],
                    "source_lines": h["source_lines"],
                    "target_lines": h["target_lines"],
                })
        elif p in src_map:
            files.append({
                "path": p,
                "status": "removed",  # present in source, missing in target
                "hunks": [],
                "additions": 0,
                "deletions": len(_split_lines(src_map[p])),
                "source_lines": _split_lines(src_map[p]),
                "target_lines": [],
            })
        else:
            files.append({
                "path": p,
                "status": "added",  # present in target, missing in source
                "hunks": [],
                "additions": len(_split_lines(tgt_map[p])),
                "deletions": 0,
                "source_lines": [],
                "target_lines": _split_lines(tgt_map[p]),
            })
    # Stable order: changed files first, unchanged last.
    order = {"modified": 0, "added": 1, "removed": 2, "unchanged": 3}
    files.sort(key=lambda f: (order.get(f["status"], 4), f["path"].lower()))
    return {"files": files, "format": fmt}


def _apply_hunks_to_lines(
    file_diff: dict,
    selected_hunk_indices: set,
    direction: str,
) -> list:
    """Build the resulting line list for a modified file given a hunk selection.

    ``direction == "target"``: start from target lines and pull the selected
    hunks back from source — i.e. revert pieces of the target so that they
    match the source.

    ``direction == "source"``: start from source lines and pull the selected
    hunks forward from target — i.e. apply pieces of the target onto the
    source.
    """
    out = []
    for idx, h in enumerate(file_diff.get("hunks", [])):
        tag = h["type"]
        if tag == "equal":
            # equal hunks are identical in both sides — keep them as-is.
            out.extend(h["source_lines"])
            continue
        picked = idx in selected_hunk_indices
        if direction == "target":
            # Default is keep target; if picked, take source instead.
            if picked:
                out.extend(h["source_lines"])
            else:
                out.extend(h["target_lines"])
        else:  # direction == "source"
            if picked:
                out.extend(h["target_lines"])
            else:
                out.extend(h["source_lines"])
    return out


_WIDGET_CSS = """
.slls-de {
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
    --slls-success-soft: rgba(52, 199, 89, 0.16);
    --slls-success-line: rgba(52, 199, 89, 0.20);
    --slls-success-gutter: rgba(52, 199, 89, 0.32);
    --slls-danger: #ff3b30;
    --slls-danger-soft: rgba(255, 59, 48, 0.14);
    --slls-danger-line: rgba(255, 59, 48, 0.18);
    --slls-danger-gutter: rgba(255, 59, 48, 0.30);
    --slls-warning: #FF9500;
    --slls-warning-soft: rgba(255, 149, 0, 0.16);
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
    max-width: 1280px;
    background: var(--slls-bg-solid);
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    box-shadow: var(--slls-shadow);
    padding: 24px;
    box-sizing: border-box;
}
@media (prefers-color-scheme: dark) {
    .slls-de.slls-de-auto {
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
        --slls-success-soft: rgba(52, 199, 89, 0.18);
        --slls-success-line: rgba(52, 199, 89, 0.22);
        --slls-success-gutter: rgba(52, 199, 89, 0.42);
        --slls-danger-soft: rgba(255, 69, 58, 0.20);
        --slls-danger-line: rgba(255, 69, 58, 0.22);
        --slls-danger-gutter: rgba(255, 69, 58, 0.40);
        --slls-shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.5);
    }
}
.slls-de.slls-de-dark {
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
    --slls-success-soft: rgba(52, 199, 89, 0.18);
    --slls-success-line: rgba(52, 199, 89, 0.22);
    --slls-success-gutter: rgba(52, 199, 89, 0.42);
    --slls-danger-soft: rgba(255, 69, 58, 0.20);
    --slls-danger-line: rgba(255, 69, 58, 0.22);
    --slls-danger-gutter: rgba(255, 69, 58, 0.40);
    --slls-shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.5);
}
.slls-de * { box-sizing: border-box; }

.slls-de-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 18px;
    flex-wrap: wrap;
}
.slls-de-title {
    font-size: 22px;
    font-weight: 600;
    letter-spacing: -0.01em;
    margin-right: auto;
    line-height: 1.15;
}
.slls-de-titlewrap {
    display: flex;
    flex-direction: column;
    margin-right: auto;
    min-width: 0;
}
.slls-de-subtitle {
    font-size: 12px;
    color: var(--slls-text-secondary);
    margin-top: 2px;
}

.slls-de-select, .slls-de-input {
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
    min-width: 0;
}
.slls-de-select {
    padding-right: 32px;
    cursor: pointer;
    background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6'><path fill='%236e6e73' d='M0 0l5 6 5-6z'/></svg>");
    background-repeat: no-repeat;
    background-position: right 12px center;
}
.slls-de-select:hover, .slls-de-input:hover { border-color: var(--slls-text-tertiary); }
.slls-de-select:focus, .slls-de-input:focus {
    outline: none;
    border-color: var(--slls-accent);
    box-shadow: 0 0 0 3px var(--slls-accent-soft);
}
.slls-de-select option {
    background-color: var(--slls-bg-solid);
    color: var(--slls-text);
}
.slls-de-input::placeholder { color: var(--slls-text-tertiary); }

.slls-de-btn {
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
    display: inline-flex;
    align-items: center;
    gap: 6px;
}
.slls-de-btn:hover { background: var(--slls-surface-2); border-color: var(--slls-text-tertiary); }
.slls-de-btn:active { transform: scale(0.97); }
.slls-de-btn:disabled { opacity: 0.4; cursor: not-allowed; }
.slls-de-btn-primary {
    background: var(--slls-accent);
    border-color: var(--slls-accent);
    color: #fff;
}
.slls-de-btn-primary:hover { background: var(--slls-accent-hover); border-color: var(--slls-accent-hover); }
.slls-de-btn-success {
    background: var(--slls-success);
    border-color: var(--slls-success);
    color: #fff;
}
.slls-de-btn-success:hover { filter: brightness(0.95); }
.slls-de-btn-danger {
    background: transparent;
    border-color: var(--slls-danger);
    color: var(--slls-danger);
}
.slls-de-btn-danger:hover { background: var(--slls-danger-soft); }
.slls-de-btn-icon {
    width: 32px; height: 32px;
    padding: 0;
    border-radius: 50%;
    justify-content: center;
}

/* Picker rows */
.slls-de-pickers {
    display: grid;
    grid-template-columns: 1fr auto 1fr;
    gap: 14px;
    align-items: stretch;
    margin-bottom: 14px;
}
.slls-de-side {
    background: var(--slls-surface);
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    padding: 14px;
    display: flex;
    flex-direction: column;
    gap: 10px;
}
.slls-de-side-label {
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--slls-text-secondary);
    display: flex;
    align-items: center;
    gap: 8px;
}
.slls-de-side-label .slls-de-dot {
    width: 8px; height: 8px; border-radius: 50%;
}
.slls-de-side.slls-de-source .slls-de-dot { background: var(--slls-danger); }
.slls-de-side.slls-de-target .slls-de-dot { background: var(--slls-success); }
.slls-de-side .slls-de-select { width: 100%; }
.slls-de-swap {
    display: flex;
    align-items: center;
    justify-content: center;
}

.slls-de-controls {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 14px;
    flex-wrap: wrap;
}
.slls-de-seg {
    display: inline-flex;
    background: var(--slls-surface-2);
    border: 1px solid var(--slls-border);
    border-radius: 999px;
    padding: 2px;
    gap: 2px;
}
.slls-de-seg button {
    appearance: none;
    border: none;
    background: transparent;
    color: var(--slls-text-secondary);
    font-family: inherit;
    font-size: 12.5px;
    font-weight: 500;
    padding: 5px 12px;
    border-radius: 999px;
    cursor: pointer;
    transition: background 120ms ease, color 120ms ease;
}
.slls-de-seg button.active {
    background: var(--slls-bg-solid);
    color: var(--slls-text);
    box-shadow: 0 1px 2px rgba(0,0,0,0.06);
}
.slls-de-seg button:hover:not(.active) { color: var(--slls-text); }

/* Summary stats */
.slls-de-stats {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    margin-bottom: 14px;
}
.slls-de-stat {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 5px 12px;
    border-radius: 999px;
    font-size: 12.5px;
    font-weight: 500;
    background: var(--slls-surface);
    border: 1px solid var(--slls-border);
    color: var(--slls-text-secondary);
}
.slls-de-stat b { color: var(--slls-text); font-weight: 600; }
.slls-de-stat.added { background: var(--slls-success-soft); color: var(--slls-success); border-color: transparent; }
.slls-de-stat.added b { color: var(--slls-success); }
.slls-de-stat.removed { background: var(--slls-danger-soft); color: var(--slls-danger); border-color: transparent; }
.slls-de-stat.removed b { color: var(--slls-danger); }
.slls-de-stat.modified { background: var(--slls-warning-soft); color: var(--slls-warning); border-color: transparent; }
.slls-de-stat.modified b { color: var(--slls-warning); }
.slls-de-stat.unchanged { background: var(--slls-accent-soft); color: var(--slls-accent); border-color: transparent; }
.slls-de-stat.unchanged b { color: var(--slls-accent); }

/* Toolbar */
.slls-de-toolbar {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 12px;
    flex-wrap: wrap;
}
.slls-de-search { flex: 1; min-width: 180px; max-width: 360px; }

/* File list */
.slls-de-files {
    border: 1px solid var(--slls-border);
    border-radius: var(--slls-radius);
    overflow: hidden;
    background: var(--slls-surface);
    max-height: 720px;
    overflow-y: auto;
}
.slls-de-files::-webkit-scrollbar { width: 10px; height: 10px; }
.slls-de-files::-webkit-scrollbar-thumb {
    background: var(--slls-border-strong);
    border-radius: 999px;
    background-clip: padding-box;
    border: 2px solid transparent;
}
.slls-de-files::-webkit-scrollbar-thumb:hover { background-color: var(--slls-text-tertiary); }

.slls-de-file { border-bottom: 1px solid var(--slls-border); }
.slls-de-file:last-child { border-bottom: none; }
.slls-de-file.hidden-match { display: none; }

.slls-de-file-row {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 14px;
    cursor: pointer;
    user-select: none;
    transition: background 100ms ease;
}
.slls-de-file-row:hover { background: var(--slls-surface-2); }

.slls-de-caret {
    width: 14px; height: 14px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    color: var(--slls-text-tertiary);
    transition: transform 160ms ease;
    flex-shrink: 0;
}
.slls-de-file.expanded .slls-de-caret { transform: rotate(90deg); }

.slls-de-check {
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
.slls-de-check[data-state="all"] {
    background: var(--slls-accent);
    border-color: var(--slls-accent);
}
.slls-de-check[data-state="all"]::after {
    content: "";
    width: 10px; height: 6px;
    border-left: 2px solid #fff;
    border-bottom: 2px solid #fff;
    transform: rotate(-45deg) translate(1px, -1px);
}
.slls-de-check[data-state="some"] {
    background: var(--slls-accent);
    border-color: var(--slls-accent);
}
.slls-de-check[data-state="some"]::after {
    content: "";
    width: 10px; height: 2px;
    background: #fff;
    border-radius: 1px;
}

.slls-de-badge {
    font-size: 10.5px;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 999px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    flex-shrink: 0;
}
.slls-de-badge.added { background: var(--slls-success-soft); color: var(--slls-success); }
.slls-de-badge.removed { background: var(--slls-danger-soft); color: var(--slls-danger); }
.slls-de-badge.modified { background: var(--slls-warning-soft); color: var(--slls-warning); }
.slls-de-badge.unchanged { background: var(--slls-accent-soft); color: var(--slls-accent); }

.slls-de-file-name {
    font-family: var(--slls-mono);
    font-size: 13px;
    font-weight: 500;
    flex: 1;
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
.slls-de-file-counts {
    font-size: 11.5px;
    color: var(--slls-text-tertiary);
    font-family: var(--slls-mono);
    flex-shrink: 0;
}
.slls-de-file-counts .plus { color: var(--slls-success); font-weight: 600; }
.slls-de-file-counts .minus { color: var(--slls-danger); font-weight: 600; }

.slls-de-file-body {
    display: none;
    background: var(--slls-surface-2);
    border-top: 1px solid var(--slls-border);
}
.slls-de-file.expanded .slls-de-file-body { display: block; }

.slls-de-hunk {
    border-bottom: 1px solid var(--slls-border);
}
.slls-de-hunk:last-child { border-bottom: none; }
.slls-de-hunk-header {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 6px 14px;
    font-family: var(--slls-mono);
    font-size: 11.5px;
    color: var(--slls-text-tertiary);
    background: var(--slls-surface);
    border-bottom: 1px solid var(--slls-border);
    user-select: none;
}
.slls-de-hunk-header .slls-de-check { width: 16px; height: 16px; }
.slls-de-hunk-label { font-weight: 600; color: var(--slls-text-secondary); }
.slls-de-hunk-spacer { flex: 1; }

.slls-de-diff {
    font-family: var(--slls-mono);
    font-size: 12px;
    line-height: 1.55;
    overflow-x: auto;
    background: var(--slls-bg-solid);
}
.slls-de-diff-row {
    display: grid;
    grid-template-columns: 44px 44px 1fr;
    align-items: stretch;
}
.slls-de-diff-row.add { background: var(--slls-success-line); }
.slls-de-diff-row.del { background: var(--slls-danger-line); }
.slls-de-diff-row.context { background: transparent; }
.slls-de-diff-gutter {
    text-align: right;
    padding: 0 8px;
    color: var(--slls-text-tertiary);
    user-select: none;
    border-right: 1px solid var(--slls-border);
    font-size: 11px;
    background: var(--slls-surface);
    white-space: nowrap;
}
.slls-de-diff-row.add .slls-de-diff-gutter { background: var(--slls-success-gutter); }
.slls-de-diff-row.del .slls-de-diff-gutter { background: var(--slls-danger-gutter); }
.slls-de-diff-sign {
    text-align: center;
    padding: 0 4px;
    font-weight: 700;
    color: var(--slls-text-tertiary);
    border-right: 1px solid var(--slls-border);
    user-select: none;
    background: var(--slls-surface);
}
.slls-de-diff-row.add .slls-de-diff-sign { color: var(--slls-success); background: var(--slls-success-gutter); }
.slls-de-diff-row.del .slls-de-diff-sign { color: var(--slls-danger); background: var(--slls-danger-gutter); }
.slls-de-diff-text {
    padding: 0 10px;
    white-space: pre;
    overflow-x: visible;
}

.slls-de-status {
    margin-top: 14px;
    padding: 10px 14px;
    border-radius: var(--slls-radius-sm);
    font-size: 13.5px;
    display: none;
}
.slls-de-status.show { display: block; animation: slls-de-fade-in 200ms ease; }
.slls-de-status.success { background: var(--slls-success-soft); color: var(--slls-success); }
.slls-de-status.error { background: var(--slls-danger-soft); color: var(--slls-danger); }
.slls-de-status.info { background: var(--slls-accent-soft); color: var(--slls-accent); }
.slls-de-status.warning { background: var(--slls-warning-soft); color: var(--slls-warning); }
@keyframes slls-de-fade-in {
    from { opacity: 0; transform: translateY(-4px); }
    to { opacity: 1; transform: translateY(0); }
}

.slls-de-footer {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-top: 18px;
    flex-wrap: wrap;
}
.slls-de-footer .slls-de-spacer { flex: 1; }

.slls-de-confirm {
    margin-top: 14px;
    padding: 14px 16px;
    border-radius: var(--slls-radius-sm);
    border: 1px solid var(--slls-warning);
    background: var(--slls-warning-soft);
    display: none;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
}
.slls-de-confirm.show { display: flex; }
.slls-de-confirm-text { flex: 1; font-size: 13.5px; color: var(--slls-text); }

.slls-de-empty {
    padding: 48px 16px;
    text-align: center;
    color: var(--slls-text-tertiary);
    font-size: 13.5px;
}
.slls-de-empty .slls-de-empty-title {
    font-size: 15px;
    color: var(--slls-text-secondary);
    margin-bottom: 6px;
    font-weight: 500;
}

.slls-de-busy {
    pointer-events: none;
    opacity: 0.6;
    transition: opacity 120ms ease;
}

.slls-de-spinner {
    display: inline-block;
    width: 14px;
    height: 14px;
    border: 2px solid currentColor;
    border-right-color: transparent;
    border-radius: 50%;
    animation: slls-de-spin 700ms linear infinite;
    vertical-align: -2px;
}
@keyframes slls-de-spin {
    to { transform: rotate(360deg); }
}

.slls-de-attribution {
    margin-top: 18px;
    text-align: right;
    font-size: 11.5px;
    color: var(--slls-text-tertiary);
}
"""


_WIDGET_JS = r"""
function render({ model, el }) {
    const root = document.createElement("div");
    root.className = "slls-de";

    function applyTheme() {
        root.classList.remove("slls-de-dark", "slls-de-light", "slls-de-auto");
        const dm = model.get("dark_mode");
        if (dm === true) root.classList.add("slls-de-dark");
        else if (dm === false) root.classList.add("slls-de-light");
        else root.classList.add("slls-de-auto");
    }
    applyTheme();
    model.on("change:dark_mode", () => { applyTheme(); renderThemeBtn(); });

    // ----- HEADER -----
    const header = document.createElement("div");
    header.className = "slls-de-header";

    const titleWrap = document.createElement("div");
    titleWrap.className = "slls-de-titlewrap";
    const title = document.createElement("div");
    title.className = "slls-de-title";
    title.textContent = "Semantic Model Diff";
    titleWrap.appendChild(title);
    const subtitle = document.createElement("div");
    subtitle.className = "slls-de-subtitle";
    subtitle.textContent = "Compare and selectively apply changes between two semantic models.";
    titleWrap.appendChild(subtitle);
    header.appendChild(titleWrap);

    const SUN_SVG = `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><circle cx="8" cy="8" r="3"/><path d="M8 1.5v1.5M8 13v1.5M1.5 8h1.5M13 8h1.5M3.3 3.3l1.05 1.05M11.65 11.65l1.05 1.05M3.3 12.7l1.05-1.05M11.65 4.35l1.05-1.05"/></svg>`;
    const MOON_SVG = `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M13.5 9.5A5.5 5.5 0 0 1 6.5 2.5a5.5 5.5 0 1 0 7 7z"/></svg>`;
    const themeBtn = document.createElement("button");
    themeBtn.className = "slls-de-btn slls-de-btn-icon";
    themeBtn.title = "Toggle dark mode";
    function renderThemeBtn() {
        themeBtn.innerHTML = model.get("dark_mode") === true ? SUN_SVG : MOON_SVG;
    }
    renderThemeBtn();
    themeBtn.onclick = () => model.set("dark_mode", !(model.get("dark_mode") === true)) || model.save_changes();
    header.appendChild(themeBtn);

    root.appendChild(header);

    // ----- PICKERS -----
    const pickers = document.createElement("div");
    pickers.className = "slls-de-pickers";

    function makeSide(side, labelText) {
        const box = document.createElement("div");
        box.className = `slls-de-side slls-de-${side}`;
        const label = document.createElement("div");
        label.className = "slls-de-side-label";
        const dot = document.createElement("span");
        dot.className = "slls-de-dot";
        label.appendChild(dot);
        label.appendChild(document.createTextNode(labelText));
        box.appendChild(label);

        const wsSelect = document.createElement("select");
        wsSelect.className = "slls-de-select";
        wsSelect.dataset.role = `${side}-ws`;
        box.appendChild(wsSelect);

        const dsSelect = document.createElement("select");
        dsSelect.className = "slls-de-select";
        dsSelect.dataset.role = `${side}-ds`;
        box.appendChild(dsSelect);

        return { box, wsSelect, dsSelect };
    }

    const sourceSide = makeSide("source", "Source");
    const targetSide = makeSide("target", "Target");

    const swapWrap = document.createElement("div");
    swapWrap.className = "slls-de-swap";
    const swapBtn = document.createElement("button");
    swapBtn.className = "slls-de-btn slls-de-btn-icon";
    swapBtn.title = "Swap source and target";
    swapBtn.innerHTML = `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M3 4h9M3 4l3-2.5M3 4l3 2.5M13 12H4M13 12l-3-2.5M13 12l-3 2.5"/></svg>`;
    swapWrap.appendChild(swapBtn);

    pickers.appendChild(sourceSide.box);
    pickers.appendChild(swapWrap);
    pickers.appendChild(targetSide.box);
    root.appendChild(pickers);

    // ----- CONTROLS -----
    const controls = document.createElement("div");
    controls.className = "slls-de-controls";

    const seg = document.createElement("div");
    seg.className = "slls-de-seg";
    const segTmdl = document.createElement("button");
    segTmdl.textContent = "TMDL";
    segTmdl.dataset.format = "TMDL";
    const segBim = document.createElement("button");
    segBim.textContent = "BIM (TMSL)";
    segBim.dataset.format = "TMSL";
    seg.appendChild(segTmdl);
    seg.appendChild(segBim);
    controls.appendChild(seg);

    const compareBtn = document.createElement("button");
    compareBtn.className = "slls-de-btn slls-de-btn-primary";
    compareBtn.textContent = "Compare";
    controls.appendChild(compareBtn);

    const refreshBtn = document.createElement("button");
    refreshBtn.className = "slls-de-btn";
    refreshBtn.textContent = "Reload";
    refreshBtn.title = "Re-fetch both models and recompute the diff";
    controls.appendChild(refreshBtn);

    const exportBtn = document.createElement("button");
    exportBtn.className = "slls-de-btn";
    exportBtn.textContent = "Download patch";
    exportBtn.title = "Download a unified diff (.patch) of all differences";
    controls.appendChild(exportBtn);

    root.appendChild(controls);

    // ----- STATS -----
    const stats = document.createElement("div");
    stats.className = "slls-de-stats";
    root.appendChild(stats);

    // ----- TOOLBAR (filter + select all) -----
    const toolbar = document.createElement("div");
    toolbar.className = "slls-de-toolbar";

    const search = document.createElement("input");
    search.className = "slls-de-input slls-de-search";
    search.placeholder = "Filter files…";
    toolbar.appendChild(search);

    const selectAllBtn = document.createElement("button");
    selectAllBtn.className = "slls-de-btn";
    selectAllBtn.textContent = "Select all";
    toolbar.appendChild(selectAllBtn);

    const selectNoneBtn = document.createElement("button");
    selectNoneBtn.className = "slls-de-btn";
    selectNoneBtn.textContent = "Select none";
    toolbar.appendChild(selectNoneBtn);

    const expandAllBtn = document.createElement("button");
    expandAllBtn.className = "slls-de-btn";
    expandAllBtn.textContent = "Expand all";
    toolbar.appendChild(expandAllBtn);

    const collapseAllBtn = document.createElement("button");
    collapseAllBtn.className = "slls-de-btn";
    collapseAllBtn.textContent = "Collapse all";
    toolbar.appendChild(collapseAllBtn);

    root.appendChild(toolbar);

    // ----- FILE LIST -----
    const files = document.createElement("div");
    files.className = "slls-de-files";
    root.appendChild(files);

    // ----- STATUS -----
    const statusEl = document.createElement("div");
    statusEl.className = "slls-de-status";
    root.appendChild(statusEl);

    // ----- CONFIRM -----
    const confirm = document.createElement("div");
    confirm.className = "slls-de-confirm";
    const confirmText = document.createElement("div");
    confirmText.className = "slls-de-confirm-text";
    const confirmYes = document.createElement("button");
    confirmYes.className = "slls-de-btn slls-de-btn-primary";
    confirmYes.textContent = "Apply";
    const confirmNo = document.createElement("button");
    confirmNo.className = "slls-de-btn";
    confirmNo.textContent = "Cancel";
    confirm.appendChild(confirmText);
    confirm.appendChild(confirmYes);
    confirm.appendChild(confirmNo);
    root.appendChild(confirm);

    // ----- FOOTER -----
    const footer = document.createElement("div");
    footer.className = "slls-de-footer";

    const applyToTargetBtn = document.createElement("button");
    applyToTargetBtn.className = "slls-de-btn slls-de-btn-success";
    applyToTargetBtn.innerHTML = `Apply selected → <b style="margin-left:4px;">Target</b>`;
    footer.appendChild(applyToTargetBtn);

    const applyToSourceBtn = document.createElement("button");
    applyToSourceBtn.className = "slls-de-btn slls-de-btn-danger";
    applyToSourceBtn.innerHTML = `Apply selected → <b style="margin-left:4px;">Source</b>`;
    footer.appendChild(applyToSourceBtn);

    const spacer = document.createElement("div");
    spacer.className = "slls-de-spacer";
    footer.appendChild(spacer);

    const selectionSummary = document.createElement("div");
    selectionSummary.style.fontSize = "12.5px";
    selectionSummary.style.color = "var(--slls-text-secondary)";
    footer.appendChild(selectionSummary);

    root.appendChild(footer);

    const attribution = document.createElement("div");
    attribution.className = "slls-de-attribution";
    attribution.innerHTML = `<span>Powered by Semantic Link Labs</span>`;
    root.appendChild(attribution);

    el.appendChild(root);

    // ====================== STATE & RENDERING ======================
    const checkedHunks = new Set();    // `${fileIdx}:${hunkIdx}` for modified files
    const includedNewFiles = new Set();  // fileIdx for added/removed files (full-file accept)
    let filterText = "";
    let expanded = new Set();

    function escapeHtml(s) {
        return String(s).replace(/[&<>"']/g, (c) => ({
            "&": "&amp;", "<": "&lt;", ">": "&gt;",
            '"': "&quot;", "'": "&#39;"
        }[c]));
    }

    function setBusy(b) { root.classList.toggle("slls-de-busy", !!b); }

    function showStatus(msg, kind) {
        if (!msg) {
            statusEl.classList.remove("show");
            return;
        }
        statusEl.textContent = msg;
        statusEl.className = "slls-de-status show " + (kind || "info");
    }
    function hideStatus() { statusEl.classList.remove("show"); }

    // ----- Workspace / Dataset selectors -----
    function fillWorkspaceSelect(sel, currentId) {
        const wss = model.get("workspaces") || [];
        sel.innerHTML = "";
        const placeholder = document.createElement("option");
        placeholder.value = "";
        placeholder.textContent = wss.length ? "Select workspace…" : "No workspaces available";
        sel.appendChild(placeholder);
        wss.forEach((w) => {
            const opt = document.createElement("option");
            opt.value = w.id;
            opt.textContent = w.name;
            sel.appendChild(opt);
        });
        sel.value = currentId || "";
    }

    function fillDatasetSelect(sel, datasets, currentId) {
        sel.innerHTML = "";
        const placeholder = document.createElement("option");
        placeholder.value = "";
        placeholder.textContent = (datasets && datasets.length) ? "Select semantic model…" : "—";
        sel.appendChild(placeholder);
        (datasets || []).forEach((d) => {
            const opt = document.createElement("option");
            opt.value = d.id;
            opt.textContent = d.name;
            sel.appendChild(opt);
        });
        sel.value = currentId || "";
    }

    function renderSelectors() {
        fillWorkspaceSelect(sourceSide.wsSelect, model.get("source_workspace_id"));
        fillWorkspaceSelect(targetSide.wsSelect, model.get("target_workspace_id"));
        fillDatasetSelect(sourceSide.dsSelect, model.get("source_datasets"), model.get("source_dataset_id"));
        fillDatasetSelect(targetSide.dsSelect, model.get("target_datasets"), model.get("target_dataset_id"));
    }

    function renderFormatSeg() {
        const f = model.get("format") || "TMDL";
        segTmdl.classList.toggle("active", f === "TMDL");
        segBim.classList.toggle("active", f === "TMSL");
    }

    function renderStats() {
        const d = model.get("diff") || { files: [] };
        const counts = { added: 0, removed: 0, modified: 0, unchanged: 0 };
        (d.files || []).forEach((f) => { counts[f.status] = (counts[f.status] || 0) + 1; });
        stats.innerHTML = "";
        if (!(d.files || []).length) return;
        const order = ["modified", "added", "removed", "unchanged"];
        const labels = { modified: "Modified", added: "Added", removed: "Removed", unchanged: "Unchanged" };
        order.forEach((k) => {
            if (!counts[k]) return;
            const s = document.createElement("span");
            s.className = `slls-de-stat ${k}`;
            s.innerHTML = `<b>${counts[k]}</b> ${labels[k]}`;
            stats.appendChild(s);
        });
        const fmtBadge = document.createElement("span");
        fmtBadge.className = "slls-de-stat";
        fmtBadge.innerHTML = `Format: <b>${escapeHtml(d.format || model.get("format") || "TMDL")}</b>`;
        stats.appendChild(fmtBadge);
    }

    function fileMatchesFilter(file) {
        if (!filterText) return true;
        return (file.path || "").toLowerCase().includes(filterText.toLowerCase());
    }

    function fileChangeKey(fIdx) { return `file:${fIdx}`; }
    function hunkKey(fIdx, hIdx) { return `${fIdx}:${hIdx}`; }

    function fileSelectionState(file, fIdx) {
        if (file.status === "unchanged") return "none";
        if (file.status === "added" || file.status === "removed") {
            return includedNewFiles.has(fIdx) ? "all" : "none";
        }
        // modified
        const changeHunks = (file.hunks || []).filter((h) => h.type !== "equal");
        if (!changeHunks.length) return "none";
        let checked = 0;
        changeHunks.forEach((h, hIdx) => {
            const realIdx = (file.hunks || []).indexOf(h);
            if (checkedHunks.has(hunkKey(fIdx, realIdx))) checked++;
        });
        if (checked === 0) return "none";
        if (checked === changeHunks.length) return "all";
        return "some";
    }

    function setFileSelection(file, fIdx, state) {
        if (file.status === "added" || file.status === "removed") {
            if (state === "all") includedNewFiles.add(fIdx);
            else includedNewFiles.delete(fIdx);
            return;
        }
        if (file.status !== "modified") return;
        (file.hunks || []).forEach((h, hIdx) => {
            if (h.type === "equal") return;
            const k = hunkKey(fIdx, hIdx);
            if (state === "all") checkedHunks.add(k);
            else checkedHunks.delete(k);
        });
    }

    function selectionTotals() {
        const d = model.get("diff") || { files: [] };
        let files = 0;
        let hunks = 0;
        (d.files || []).forEach((f, fIdx) => {
            if (f.status === "added" || f.status === "removed") {
                if (includedNewFiles.has(fIdx)) { files += 1; }
            } else if (f.status === "modified") {
                const sel = (f.hunks || []).filter((h, hIdx) => h.type !== "equal" && checkedHunks.has(hunkKey(fIdx, hIdx))).length;
                if (sel > 0) { files += 1; hunks += sel; }
            }
        });
        return { files, hunks };
    }

    function renderSelectionSummary() {
        const t = selectionTotals();
        if (!t.files && !t.hunks) {
            selectionSummary.textContent = "No changes selected";
        } else {
            selectionSummary.textContent = `${t.files} file${t.files === 1 ? "" : "s"}, ${t.hunks} hunk${t.hunks === 1 ? "" : "s"} selected`;
        }
    }

    function makeCheck(state, onClick, title) {
        const cb = document.createElement("span");
        cb.className = "slls-de-check";
        cb.setAttribute("data-state", state);
        if (title) cb.title = title;
        cb.onclick = (e) => { e.stopPropagation(); onClick(e); };
        return cb;
    }

    function renderHunk(file, fIdx, hunk, hIdx) {
        const wrap = document.createElement("div");
        wrap.className = "slls-de-hunk";

        const head = document.createElement("div");
        head.className = "slls-de-hunk-header";

        if (hunk.type !== "equal") {
            const isChecked = checkedHunks.has(hunkKey(fIdx, hIdx));
            const cb = makeCheck(isChecked ? "all" : "none", () => {
                if (isChecked) checkedHunks.delete(hunkKey(fIdx, hIdx));
                else checkedHunks.add(hunkKey(fIdx, hIdx));
                refreshFiles();
                renderSelectionSummary();
            }, "Include this hunk in the next Apply");
            head.appendChild(cb);
        } else {
            const placeholder = document.createElement("span");
            placeholder.style.width = "16px";
            placeholder.style.height = "16px";
            head.appendChild(placeholder);
        }

        const lbl = document.createElement("span");
        lbl.className = "slls-de-hunk-label";
        const typeText = {
            equal: "context",
            replace: "modify",
            insert: "add (in source)",
            delete: "remove (from source)"
        }[hunk.type] || hunk.type;
        const a1 = (hunk.source_start || 0) + 1;
        const a2 = (hunk.source_start || 0) + (hunk.source_lines ? hunk.source_lines.length : 0);
        const b1 = (hunk.target_start || 0) + 1;
        const b2 = (hunk.target_start || 0) + (hunk.target_lines ? hunk.target_lines.length : 0);
        lbl.textContent = `@@ ${typeText} · source ${a1}-${a2} · target ${b1}-${b2}`;
        head.appendChild(lbl);

        const spc = document.createElement("span");
        spc.className = "slls-de-hunk-spacer";
        head.appendChild(spc);

        wrap.appendChild(head);

        const diff = document.createElement("div");
        diff.className = "slls-de-diff";

        // Build inline unified rows from hunk lines.
        if (hunk.type === "equal") {
            (hunk.source_lines || []).forEach((line, i) => {
                diff.appendChild(makeRow(
                    "context",
                    (hunk.source_start || 0) + i + 1,
                    (hunk.target_start || 0) + i + 1,
                    " ", line));
            });
        } else if (hunk.type === "delete") {
            (hunk.source_lines || []).forEach((line, i) => {
                diff.appendChild(makeRow("del", (hunk.source_start || 0) + i + 1, "", "-", line));
            });
        } else if (hunk.type === "insert") {
            (hunk.target_lines || []).forEach((line, i) => {
                diff.appendChild(makeRow("add", "", (hunk.target_start || 0) + i + 1, "+", line));
            });
        } else if (hunk.type === "replace") {
            (hunk.source_lines || []).forEach((line, i) => {
                diff.appendChild(makeRow("del", (hunk.source_start || 0) + i + 1, "", "-", line));
            });
            (hunk.target_lines || []).forEach((line, i) => {
                diff.appendChild(makeRow("add", "", (hunk.target_start || 0) + i + 1, "+", line));
            });
        }

        wrap.appendChild(diff);
        return wrap;
    }

    function makeRow(kind, srcNo, tgtNo, sign, text) {
        const row = document.createElement("div");
        row.className = `slls-de-diff-row ${kind}`;
        const numCell = document.createElement("div");
        numCell.className = "slls-de-diff-gutter";
        numCell.textContent = (srcNo || "·") + " / " + (tgtNo || "·");
        const signCell = document.createElement("div");
        signCell.className = "slls-de-diff-sign";
        signCell.textContent = sign;
        const textCell = document.createElement("div");
        textCell.className = "slls-de-diff-text";
        textCell.textContent = text;
        row.appendChild(numCell);
        row.appendChild(signCell);
        row.appendChild(textCell);
        return row;
    }

    function renderFile(file, fIdx) {
        const row = document.createElement("div");
        row.className = "slls-de-file";
        if (expanded.has(fIdx)) row.classList.add("expanded");
        if (!fileMatchesFilter(file)) row.classList.add("hidden-match");

        const head = document.createElement("div");
        head.className = "slls-de-file-row";

        const caret = document.createElement("span");
        caret.className = "slls-de-caret";
        caret.innerHTML = `<svg width="8" height="10" viewBox="0 0 8 10" fill="currentColor"><path d="M1 0l6 5-6 5V0z"/></svg>`;
        head.appendChild(caret);

        const cbState = fileSelectionState(file, fIdx);
        const fileCb = makeCheck(cbState, () => {
            const next = cbState === "all" ? "none" : "all";
            setFileSelection(file, fIdx, next);
            refreshFiles();
            renderSelectionSummary();
        }, "Include the entire file");
        head.appendChild(fileCb);

        const badge = document.createElement("span");
        badge.className = `slls-de-badge ${file.status}`;
        badge.textContent = file.status;
        head.appendChild(badge);

        const name = document.createElement("span");
        name.className = "slls-de-file-name";
        name.textContent = file.path;
        head.appendChild(name);

        if (file.status === "modified") {
            const counts = document.createElement("span");
            counts.className = "slls-de-file-counts";
            counts.innerHTML = `<span class="plus">+${file.additions || 0}</span>  <span class="minus">-${file.deletions || 0}</span>`;
            head.appendChild(counts);
        } else if (file.status === "added") {
            const counts = document.createElement("span");
            counts.className = "slls-de-file-counts";
            counts.innerHTML = `<span class="plus">+${(file.target_lines || []).length}</span>`;
            head.appendChild(counts);
        } else if (file.status === "removed") {
            const counts = document.createElement("span");
            counts.className = "slls-de-file-counts";
            counts.innerHTML = `<span class="minus">-${(file.source_lines || []).length}</span>`;
            head.appendChild(counts);
        }

        head.onclick = () => {
            if (expanded.has(fIdx)) expanded.delete(fIdx);
            else expanded.add(fIdx);
            refreshFiles();
        };
        row.appendChild(head);

        const body = document.createElement("div");
        body.className = "slls-de-file-body";

        if (file.status === "modified") {
            (file.hunks || []).forEach((h, hIdx) => {
                body.appendChild(renderHunk(file, fIdx, h, hIdx));
            });
        } else if (file.status === "added") {
            const diff = document.createElement("div");
            diff.className = "slls-de-diff";
            (file.target_lines || []).forEach((line, i) => {
                diff.appendChild(makeRow("add", "", i + 1, "+", line));
            });
            body.appendChild(diff);
        } else if (file.status === "removed") {
            const diff = document.createElement("div");
            diff.className = "slls-de-diff";
            (file.source_lines || []).forEach((line, i) => {
                diff.appendChild(makeRow("del", i + 1, "", "-", line));
            });
            body.appendChild(diff);
        } else {
            const empty = document.createElement("div");
            empty.style.padding = "12px 16px";
            empty.style.color = "var(--slls-text-tertiary)";
            empty.style.fontSize = "12.5px";
            empty.textContent = "No differences.";
            body.appendChild(empty);
        }

        row.appendChild(body);
        return row;
    }

    function refreshFiles() {
        files.innerHTML = "";
        const d = model.get("diff") || { files: [] };
        const list = (d.files || []);
        if (!list.length) {
            const empty = document.createElement("div");
            empty.className = "slls-de-empty";
            empty.innerHTML = `<div class="slls-de-empty-title">No comparison loaded</div>
                <div>Select a source and target semantic model, then click <b>Compare</b>.</div>`;
            files.appendChild(empty);
            return;
        }
        list.forEach((f, idx) => {
            files.appendChild(renderFile(f, idx));
        });
    }

    function renderAll() {
        renderSelectors();
        renderFormatSeg();
        renderStats();
        refreshFiles();
        renderSelectionSummary();
    }

    // ====================== EVENTS ======================
    sourceSide.wsSelect.onchange = () => {
        model.set("source_workspace_id", sourceSide.wsSelect.value || "");
        model.set("source_dataset_id", "");
        model.set("source_datasets", []);
        model.save_changes();
        if (sourceSide.wsSelect.value) {
            sendAction({ action: "load_datasets", side: "source", workspace_id: sourceSide.wsSelect.value });
        }
    };
    targetSide.wsSelect.onchange = () => {
        model.set("target_workspace_id", targetSide.wsSelect.value || "");
        model.set("target_dataset_id", "");
        model.set("target_datasets", []);
        model.save_changes();
        if (targetSide.wsSelect.value) {
            sendAction({ action: "load_datasets", side: "target", workspace_id: targetSide.wsSelect.value });
        }
    };
    sourceSide.dsSelect.onchange = () => {
        model.set("source_dataset_id", sourceSide.dsSelect.value || "");
        model.save_changes();
    };
    targetSide.dsSelect.onchange = () => {
        model.set("target_dataset_id", targetSide.dsSelect.value || "");
        model.save_changes();
    };

    swapBtn.onclick = () => {
        const sw = model.get("source_workspace_id");
        const sd = model.get("source_dataset_id");
        const sdList = model.get("source_datasets") || [];
        const tw = model.get("target_workspace_id");
        const td = model.get("target_dataset_id");
        const tdList = model.get("target_datasets") || [];
        model.set("source_workspace_id", tw);
        model.set("source_dataset_id", td);
        model.set("source_datasets", tdList);
        model.set("target_workspace_id", sw);
        model.set("target_dataset_id", sd);
        model.set("target_datasets", sdList);
        // Swap diff sides too (so existing comparison stays consistent visually).
        const d = model.get("diff") || { files: [] };
        if ((d.files || []).length) {
            const swapped = JSON.parse(JSON.stringify(d));
            (swapped.files || []).forEach((f) => {
                const ssrc = f.source_lines; f.source_lines = f.target_lines; f.target_lines = ssrc;
                const sst = f.source_start;
                // status flip
                if (f.status === "added") f.status = "removed";
                else if (f.status === "removed") f.status = "added";
                (f.hunks || []).forEach((h) => {
                    const tmp = h.source_lines; h.source_lines = h.target_lines; h.target_lines = tmp;
                    const tmp2 = h.source_start; h.source_start = h.target_start; h.target_start = tmp2;
                    if (h.type === "insert") h.type = "delete";
                    else if (h.type === "delete") h.type = "insert";
                });
                const tmpA = f.additions; f.additions = f.deletions; f.deletions = tmpA;
            });
            model.set("diff", swapped);
        }
        // Clear selections because indices/semantics flip.
        checkedHunks.clear();
        includedNewFiles.clear();
        model.save_changes();
        renderAll();
    };

    segTmdl.onclick = () => { model.set("format", "TMDL"); model.save_changes(); renderFormatSeg(); };
    segBim.onclick = () => { model.set("format", "TMSL"); model.save_changes(); renderFormatSeg(); };

    compareBtn.onclick = () => {
        if (!model.get("source_workspace_id") || !model.get("source_dataset_id")
            || !model.get("target_workspace_id") || !model.get("target_dataset_id")) {
            showStatus("Please select a source and target semantic model.", "warning");
            return;
        }
        hideStatus();
        checkedHunks.clear();
        includedNewFiles.clear();
        expanded.clear();
        setBusy(true);
        sendAction({ action: "compare" });
    };

    refreshBtn.onclick = () => {
        const d = model.get("diff") || {};
        if (!(d.files || []).length) { compareBtn.click(); return; }
        setBusy(true);
        sendAction({ action: "compare" });
    };

    exportBtn.onclick = () => {
        const d = model.get("diff") || {};
        if (!(d.files || []).length) { showStatus("Nothing to export — run Compare first.", "warning"); return; }
        const lines = [];
        const srcName = (d.source && d.source.dataset_name) || "source";
        const tgtName = (d.target && d.target.dataset_name) || "target";
        (d.files || []).forEach((f) => {
            if (f.status === "unchanged") return;
            lines.push(`--- ${srcName}/${f.path}`);
            lines.push(`+++ ${tgtName}/${f.path}`);
            if (f.status === "added") {
                lines.push(`@@ -0,0 +1,${(f.target_lines || []).length} @@`);
                (f.target_lines || []).forEach((l) => lines.push(`+${l}`));
            } else if (f.status === "removed") {
                lines.push(`@@ -1,${(f.source_lines || []).length} +0,0 @@`);
                (f.source_lines || []).forEach((l) => lines.push(`-${l}`));
            } else if (f.status === "modified") {
                (f.hunks || []).forEach((h) => {
                    if (h.type === "equal") return;
                    const a1 = (h.source_start || 0) + 1;
                    const aL = (h.source_lines || []).length;
                    const b1 = (h.target_start || 0) + 1;
                    const bL = (h.target_lines || []).length;
                    lines.push(`@@ -${a1},${aL} +${b1},${bL} @@`);
                    (h.source_lines || []).forEach((l) => lines.push(`-${l}`));
                    (h.target_lines || []).forEach((l) => lines.push(`+${l}`));
                });
            }
        });
        const blob = new Blob([lines.join("\n") + "\n"], { type: "text/plain" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `${srcName}__vs__${tgtName}.patch`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        showStatus("Patch downloaded.", "success");
    };

    search.oninput = () => { filterText = search.value || ""; refreshFiles(); };

    selectAllBtn.onclick = () => {
        const d = model.get("diff") || { files: [] };
        (d.files || []).forEach((f, fIdx) => {
            if (f.status === "modified") {
                (f.hunks || []).forEach((h, hIdx) => {
                    if (h.type !== "equal") checkedHunks.add(hunkKey(fIdx, hIdx));
                });
            } else if (f.status === "added" || f.status === "removed") {
                includedNewFiles.add(fIdx);
            }
        });
        refreshFiles();
        renderSelectionSummary();
    };
    selectNoneBtn.onclick = () => {
        checkedHunks.clear();
        includedNewFiles.clear();
        refreshFiles();
        renderSelectionSummary();
    };
    expandAllBtn.onclick = () => {
        const d = model.get("diff") || { files: [] };
        expanded = new Set((d.files || []).map((_, i) => i));
        refreshFiles();
    };
    collapseAllBtn.onclick = () => {
        expanded.clear();
        refreshFiles();
    };

    let pendingDirection = null;
    applyToTargetBtn.onclick = () => askApply("target");
    applyToSourceBtn.onclick = () => askApply("source");
    function askApply(direction) {
        const t = selectionTotals();
        if (!t.files && !t.hunks) {
            showStatus("Select at least one change to apply.", "warning");
            return;
        }
        pendingDirection = direction;
        const destName = direction === "target"
            ? ((model.get("diff") || {}).target || {}).dataset_name || "target"
            : ((model.get("diff") || {}).source || {}).dataset_name || "source";
        confirmText.innerHTML = `Apply <b>${t.files}</b> file(s) / <b>${t.hunks}</b> hunk(s) to <b>${escapeHtml(destName)}</b>? This will rewrite the destination semantic model definition.`;
        confirm.classList.add("show");
    }
    confirmNo.onclick = () => { confirm.classList.remove("show"); pendingDirection = null; };
    confirmYes.onclick = () => {
        confirm.classList.remove("show");
        if (!pendingDirection) return;
        const payload = {
            action: "apply",
            direction: pendingDirection,
            selected_hunks: Array.from(checkedHunks),
            selected_files: Array.from(includedNewFiles)
        };
        pendingDirection = null;
        setBusy(true);
        showStatus("Applying changes…", "info");
        sendAction(payload);
    };

    function sendAction(payload) {
        model.set("pending_action", payload);
        model.set("run", (model.get("run") || 0) + 1);
        model.save_changes();
    }

    // ====================== MODEL CHANGE LISTENERS ======================
    model.on("change:workspaces", renderAll);
    model.on("change:source_datasets", () => fillDatasetSelect(sourceSide.dsSelect, model.get("source_datasets"), model.get("source_dataset_id")));
    model.on("change:target_datasets", () => fillDatasetSelect(targetSide.dsSelect, model.get("target_datasets"), model.get("target_dataset_id")));
    model.on("change:source_workspace_id", () => { sourceSide.wsSelect.value = model.get("source_workspace_id") || ""; });
    model.on("change:target_workspace_id", () => { targetSide.wsSelect.value = model.get("target_workspace_id") || ""; });
    model.on("change:source_dataset_id", () => { sourceSide.dsSelect.value = model.get("source_dataset_id") || ""; });
    model.on("change:target_dataset_id", () => { targetSide.dsSelect.value = model.get("target_dataset_id") || ""; });
    model.on("change:format", renderFormatSeg);
    model.on("change:diff", () => { renderStats(); refreshFiles(); renderSelectionSummary(); });
    model.on("change:status", () => {
        const s = model.get("status") || {};
        setBusy(false);
        if (s && s.message) showStatus(s.message, s.kind || "info");
    });

    renderAll();
}
export default { render };
"""


@log
def semantic_model_diff_editor(
    source_workspace: Optional[str | UUID] = None,
    source_dataset: Optional[str | UUID] = None,
    target_workspace: Optional[str | UUID] = None,
    target_dataset: Optional[str | UUID] = None,
    format: str = "TMDL",
    dark_mode: bool = False,
):
    """
    Generates an interactive diff editor for comparing two semantic models and
    selectively applying changes between them.

    The editor lets the user:

    * Choose a workspace and semantic model for both the *source* and *target*.
    * Compare them as a single ``model.bim`` file (``TMSL``) or as a collection
      of ``.tmdl`` files (``TMDL``).
    * Browse a file-by-file diff with colour-coded additions / deletions and
      per-hunk granularity.
    * Cherry-pick individual hunks (or whole added / removed files) and apply
      them either to the target model or back to the source model — a
      lightweight CI/CD workflow for Fabric / Power BI semantic models.
    * Filter / expand / collapse files, swap the two sides, reload the
      comparison, and download a unified ``.patch`` of all differences.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    source_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID for the source semantic model.
        If None, the user can choose it from the UI.
    source_dataset : str | uuid.UUID, default=None
        Name or ID of the source semantic model.
        If None, the user can choose it from the UI.
    target_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID for the target semantic model.
        If None, the user can choose it from the UI.
    target_dataset : str | uuid.UUID, default=None
        Name or ID of the target semantic model.
        If None, the user can choose it from the UI.
    format : str, default="TMDL"
        The format used to compare the two semantic models. Valid options are
        ``"TMDL"`` and ``"TMSL"`` (alias: ``"BIM"``). ``"TMDL"`` compares the
        collection of TMDL files; ``"TMSL"`` compares the single
        ``model.bim`` file.
    dark_mode : bool, default=False
        If True, renders the editor with a dark colour theme. If False, renders
        with a light colour theme.
    """

    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "The 'semantic_model_diff_editor' function requires the 'anywidget' package. "
            "Install it with: pip install anywidget"
        ) from e

    import json
    from IPython.display import display
    import sempy.fabric as fabric

    from sempy_labs._helper_functions import (
        resolve_workspace_name_and_id,
        resolve_dataset_name_and_id,
        _conv_b64,
        _decode_b64,
        _base_api,
    )

    # Normalize the requested format.
    fmt_in = (format or "TMDL").upper()
    if fmt_in == "BIM":
        fmt_in = "TMSL"
    if fmt_in not in ("TMDL", "TMSL"):
        raise ValueError("`format` must be one of 'TMDL', 'TMSL' (or its alias 'BIM').")

    # -----------------------------
    # HELPERS
    # -----------------------------
    def _list_workspaces() -> list:
        try:
            df = fabric.list_workspaces()
        except Exception:
            return []
        rows = []
        for _, r in df.iterrows():
            wid = r.get("Id") or r.get("id") or r.get("workspaceId")
            wname = r.get("Name") or r.get("name") or r.get("displayName")
            if wid and wname:
                rows.append({"id": str(wid), "name": str(wname)})
        # Sort by name for stable display.
        rows.sort(key=lambda x: x["name"].lower())
        return rows

    def _list_datasets(workspace_id: str) -> list:
        try:
            df = fabric.list_datasets(workspace=workspace_id, mode="rest")
        except Exception:
            return []
        rows = []
        for _, r in df.iterrows():
            did = r.get("Dataset Id") or r.get("Dataset ID") or r.get("id")
            dname = r.get("Dataset Name") or r.get("name") or r.get("displayName")
            if did and dname:
                rows.append({"id": str(did), "name": str(dname)})
        rows.sort(key=lambda x: x["name"].lower())
        return rows

    def _resolve_initial(ws, ds):
        """Resolve initial workspace / dataset pair into (id, name) without erroring out."""
        ws_id = ""
        ws_name = ""
        ds_id = ""
        ds_name = ""
        if ws is not None:
            try:
                (ws_name_r, ws_id_r) = resolve_workspace_name_and_id(ws)
                ws_id = str(ws_id_r)
                ws_name = str(ws_name_r)
            except Exception:
                pass
        if ws_id and ds is not None:
            try:
                (ds_name_r, ds_id_r) = resolve_dataset_name_and_id(ds, ws_id)
                ds_id = str(ds_id_r)
                ds_name = str(ds_name_r)
            except Exception:
                pass
        return ws_id, ws_name, ds_id, ds_name

    def _fetch_definition(workspace_id: str, dataset_id: str, fmt: str) -> dict:
        """Return {"files": [{"path": str, "content": str}, ...]} for the given model."""
        if not workspace_id or not dataset_id:
            return {"files": []}
        result = _base_api(
            request=(
                f"v1/workspaces/{workspace_id}/semanticModels/{dataset_id}"
                f"/getDefinition?format={fmt}"
            ),
            method="post",
            lro_return_json=True,
            status_codes=None,
        )
        parts = result.get("definition", {}).get("parts", []) or []
        if fmt == "TMSL":
            # Surface only the model.bim part (the .pbidataset wrapper is metadata).
            files = []
            for p in parts:
                if p.get("path") == "model.bim":
                    files.append({
                        "path": "model.bim",
                        "content": _decode_b64(p.get("payload") or ""),
                    })
            return {"files": files, "parts": parts}
        else:
            files = []
            for p in parts:
                path = p.get("path") or ""
                if not path.endswith(".tmdl"):
                    # Skip non-text parts (e.g. .pbidataset definition).
                    continue
                files.append({
                    "path": path,
                    "content": _decode_b64(p.get("payload") or ""),
                })
            return {"files": files, "parts": parts}

    # -----------------------------
    # INITIAL STATE
    # -----------------------------
    workspaces = _list_workspaces()
    src_ws_id, src_ws_name, src_ds_id, src_ds_name = _resolve_initial(
        source_workspace, source_dataset
    )
    tgt_ws_id, tgt_ws_name, tgt_ds_id, tgt_ds_name = _resolve_initial(
        target_workspace, target_dataset
    )
    src_datasets = _list_datasets(src_ws_id) if src_ws_id else []
    tgt_datasets = _list_datasets(tgt_ws_id) if tgt_ws_id else []

    # -----------------------------
    # WIDGET
    # -----------------------------
    class SemanticModelDiffWidget(anywidget.AnyWidget):
        _esm = _WIDGET_JS
        _css = _WIDGET_CSS

        workspaces = traitlets.List().tag(sync=True)
        source_workspace_id = traitlets.Unicode("").tag(sync=True)
        source_workspace_name = traitlets.Unicode("").tag(sync=True)
        source_dataset_id = traitlets.Unicode("").tag(sync=True)
        source_dataset_name = traitlets.Unicode("").tag(sync=True)
        source_datasets = traitlets.List().tag(sync=True)

        target_workspace_id = traitlets.Unicode("").tag(sync=True)
        target_workspace_name = traitlets.Unicode("").tag(sync=True)
        target_dataset_id = traitlets.Unicode("").tag(sync=True)
        target_dataset_name = traitlets.Unicode("").tag(sync=True)
        target_datasets = traitlets.List().tag(sync=True)

        format = traitlets.Unicode("TMDL").tag(sync=True)
        diff = traitlets.Dict().tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)

    widget = SemanticModelDiffWidget(
        workspaces=workspaces,
        source_workspace_id=src_ws_id,
        source_workspace_name=src_ws_name,
        source_dataset_id=src_ds_id,
        source_dataset_name=src_ds_name,
        source_datasets=src_datasets,
        target_workspace_id=tgt_ws_id,
        target_workspace_name=tgt_ws_name,
        target_dataset_id=tgt_ds_id,
        target_dataset_name=tgt_ds_name,
        target_datasets=tgt_datasets,
        format=fmt_in,
        diff={"files": []},
        status={},
        pending_action={},
        run=0,
        dark_mode=bool(dark_mode),
    )

    # Cache of last-fetched definitions so we can apply changes without a re-fetch
    # (and we still have the raw parts list — including non-text parts like
    # definition.pbidataset — that we must echo back when calling updateDefinition).
    cache = {"source": None, "target": None, "format": fmt_in}

    def _full_payload_parts(definition: dict, files_map: dict) -> list:
        """
        Rebuild the `parts` array for the updateDefinition API, substituting
        the text content for the files in `files_map` and echoing back any
        binary / metadata parts (e.g. definition.pbidataset) unchanged.
        """
        new_parts = []
        for p in definition.get("parts", []) or []:
            path = p.get("path") or ""
            if path in files_map:
                new_parts.append({
                    "path": path,
                    "payload": _conv_b64(files_map[path]),
                    "payloadType": "InlineBase64",
                })
            else:
                # Pass through unchanged.
                new_parts.append({
                    "path": path,
                    "payload": p.get("payload"),
                    "payloadType": p.get("payloadType", "InlineBase64"),
                })
        # Also include any wholly-new files that didn't exist in the original
        # definition (only possible for TMDL when the user accepts an "added"
        # file from the other side).
        existing = {p.get("path") for p in new_parts}
        for path, content in files_map.items():
            if path in existing:
                continue
            new_parts.append({
                "path": path,
                "payload": _conv_b64(content),
                "payloadType": "InlineBase64",
            })
        return new_parts

    def _on_run(change):
        data = dict(widget.pending_action or {})
        action = data.get("action")
        if not action:
            return
        try:
            if action == "load_datasets":
                side = data.get("side")
                ws_id = data.get("workspace_id") or ""
                ds_list = _list_datasets(ws_id) if ws_id else []
                if side == "source":
                    widget.source_datasets = ds_list
                else:
                    widget.target_datasets = ds_list
                widget.status = {}
                return

            if action == "compare":
                widget.status = {"message": "Loading models…", "kind": "info"}
                fmt = widget.format or "TMDL"
                src_def = _fetch_definition(
                    widget.source_workspace_id, widget.source_dataset_id, fmt
                )
                tgt_def = _fetch_definition(
                    widget.target_workspace_id, widget.target_dataset_id, fmt
                )
                cache["source"] = src_def
                cache["target"] = tgt_def
                cache["format"] = fmt
                diff = _compute_diff(src_def, tgt_def, fmt)
                # Attach dataset/workspace context to the diff payload.
                diff["source"] = {
                    "workspace_name": widget.source_workspace_name,
                    "workspace_id": widget.source_workspace_id,
                    "dataset_name": widget.source_dataset_name,
                    "dataset_id": widget.source_dataset_id,
                }
                diff["target"] = {
                    "workspace_name": widget.target_workspace_name,
                    "workspace_id": widget.target_workspace_id,
                    "dataset_name": widget.target_dataset_name,
                    "dataset_id": widget.target_dataset_id,
                }
                widget.diff = diff
                changed = sum(
                    1 for f in diff["files"] if f["status"] != "unchanged"
                )
                widget.status = {
                    "message": (
                        f"Compared {len(diff['files'])} file(s); "
                        f"{changed} differ."
                    ),
                    "kind": "success" if changed else "info",
                }
                return

            if action == "apply":
                direction = data.get("direction")
                if direction not in ("target", "source"):
                    widget.status = {
                        "message": "Invalid apply direction.",
                        "kind": "error",
                    }
                    return
                if not cache.get("source") or not cache.get("target"):
                    widget.status = {
                        "message": "Run Compare before applying changes.",
                        "kind": "error",
                    }
                    return

                selected_hunks = set(data.get("selected_hunks") or [])
                selected_files = set(int(i) for i in (data.get("selected_files") or []))

                d = widget.diff or {"files": []}
                files = d.get("files") or []

                # Build the per-file selection map.
                per_file_hunks: dict = {}
                for entry in selected_hunks:
                    try:
                        fi, hi = entry.split(":")
                        per_file_hunks.setdefault(int(fi), set()).add(int(hi))
                    except Exception:
                        continue

                fmt = cache.get("format") or widget.format or "TMDL"

                # Decide which definition we're rewriting.
                base_def = (
                    cache["target"] if direction == "target" else cache["source"]
                )
                base_files = {f["path"]: f["content"] for f in base_def.get("files", [])}

                new_files_map = dict(base_files)  # start with current dest content

                for fIdx, f in enumerate(files):
                    status = f.get("status")
                    path = f.get("path")
                    if status == "unchanged":
                        continue
                    if status == "modified":
                        sel = per_file_hunks.get(fIdx, set())
                        if not sel:
                            continue
                        new_lines = _apply_hunks_to_lines(f, sel, direction)
                        new_files_map[path] = _join_lines(new_lines)
                    elif status == "added":
                        # Present in target, missing in source.
                        if fIdx not in selected_files:
                            continue
                        if direction == "target":
                            # Already present in target — accepting "added"
                            # toward target means removing it from target.
                            new_files_map.pop(path, None)
                        else:
                            # Bring this file into source.
                            new_files_map[path] = _join_lines(f.get("target_lines") or [])
                    elif status == "removed":
                        # Present in source, missing in target.
                        if fIdx not in selected_files:
                            continue
                        if direction == "target":
                            # Bring it into target.
                            new_files_map[path] = _join_lines(f.get("source_lines") or [])
                        else:
                            # Already present in source — accepting "removed"
                            # toward source means removing it from source.
                            new_files_map.pop(path, None)

                # Determine destination IDs.
                if direction == "target":
                    dest_ws = widget.target_workspace_id
                    dest_ds = widget.target_dataset_id
                    dest_name = widget.target_dataset_name or "target"
                else:
                    dest_ws = widget.source_workspace_id
                    dest_ds = widget.source_dataset_id
                    dest_name = widget.source_dataset_name or "source"

                if fmt == "TMSL":
                    bim_text = new_files_map.get("model.bim")
                    if bim_text is None:
                        widget.status = {
                            "message": "Internal error: model.bim missing after merge.",
                            "kind": "error",
                        }
                        return
                    try:
                        bim_obj = json.loads(bim_text)
                    except Exception as e:
                        widget.status = {
                            "message": f"Merged model.bim is not valid JSON: {e}",
                            "kind": "error",
                        }
                        return
                    pbi_def = {"version": "1.0", "settings": {}}
                    payload = {
                        "definition": {
                            "parts": [
                                {
                                    "path": "model.bim",
                                    "payload": _conv_b64(bim_obj),
                                    "payloadType": "InlineBase64",
                                },
                                {
                                    "path": "definition.pbidataset",
                                    "payload": _conv_b64(pbi_def),
                                    "payloadType": "InlineBase64",
                                },
                            ]
                        }
                    }
                else:
                    # TMDL: rebuild parts, preserving any non-text parts from the destination.
                    new_parts = _full_payload_parts(base_def, new_files_map)
                    payload = {"definition": {"parts": new_parts}}

                _base_api(
                    request=(
                        f"v1/workspaces/{dest_ws}/semanticModels/{dest_ds}/updateDefinition"
                    ),
                    method="post",
                    payload=payload,
                    lro_return_status_code=True,
                    status_codes=None,
                )

                # Re-fetch and recompute so the UI reflects the applied changes.
                src_def = _fetch_definition(
                    widget.source_workspace_id, widget.source_dataset_id, fmt
                )
                tgt_def = _fetch_definition(
                    widget.target_workspace_id, widget.target_dataset_id, fmt
                )
                cache["source"] = src_def
                cache["target"] = tgt_def
                cache["format"] = fmt
                diff = _compute_diff(src_def, tgt_def, fmt)
                diff["source"] = {
                    "workspace_name": widget.source_workspace_name,
                    "workspace_id": widget.source_workspace_id,
                    "dataset_name": widget.source_dataset_name,
                    "dataset_id": widget.source_dataset_id,
                }
                diff["target"] = {
                    "workspace_name": widget.target_workspace_name,
                    "workspace_id": widget.target_workspace_id,
                    "dataset_name": widget.target_dataset_name,
                    "dataset_id": widget.target_dataset_id,
                }
                widget.diff = diff

                widget.status = {
                    "message": f"Applied selected changes to '{dest_name}'.",
                    "kind": "success",
                }
                return

        except Exception as e:
            widget.status = {"message": f"Error: {e}", "kind": "error"}

    widget.observe(_on_run, names=["run"])

    # If both source and target dataset names were resolved, automatically queue an
    # initial comparison so the editor opens with a useful view.
    if src_ds_id and tgt_ds_id:
        widget.pending_action = {"action": "compare"}
        widget.run = 1

    # Keep a reference on the widget so the Python-side observer is not garbage
    # collected after this function returns. We intentionally do NOT return the
    # widget to avoid Jupyter auto-displaying it a second time after `display()`.
    display(widget)
