"""An interactive manager for the contents of a Fabric lakehouse.

Browses the tables, folders and files of a lakehouse (including the
`soft-deleted <https://learn.microsoft.com/fabric/onelake/onelake-disaster-recovery#soft-delete-for-onelake-files>`_
ones), recovers deleted objects and runs table maintenance, all from one
widget.
"""

from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from sempy._utils._log import log

from sempy_labs._ui_components import (
    ICONS as _UI_ICONS,
    DARK_THEME_VARS as _UI_DARK_VARS,
    LIGHT_THEME_VARS as _UI_LIGHT_VARS,
    SEARCH_SELECT_CSS as _UI_SEARCH_SELECT_CSS,
    SEARCH_SELECT_JS as _UI_SEARCH_SELECT_JS,
    fullscreen_css as _ui_fullscreen_css,
    fullscreen_setup_js as _ui_fullscreen_setup_js,
    list_picker_lakehouses as _ui_list_picker_lakehouses,
    list_picker_workspaces as _ui_list_picker_workspaces,
    render_attribution_html as _ui_render_attribution_html,
    run_widget_task as _ui_run_widget_task,
    scoped_attribution_css as _ui_scoped_attribution_css,
    scoped_button_press_css as _ui_scoped_button_press_css,
    scoped_header_css as _ui_scoped_header_css,
)

# The two OneLake containers a lakehouse exposes; everything else (system
# folders such as TableMaintenance) is not part of its contents.
_CONTAINERS = ("Tables", "Files")


# ---------------------------------------------------------------------------
# Lakehouse contents (pure helpers, so they are testable without Fabric)
# ---------------------------------------------------------------------------
def _format_bytes(size: Optional[float]) -> str:
    """A byte count as a short, human readable string."""

    try:
        value = float(size or 0)
    except (TypeError, ValueError):
        return "—"
    if value <= 0:
        return "—"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:,.0f} {unit}" if unit == "B" else f"{value:,.1f} {unit}"
        value /= 1024
    return f"{value:,.1f} TB"


def _blob_path(blob_name: Any, lakehouse_id: str) -> str:
    """The path of a blob relative to the lakehouse root, e.g. ``Tables/sales``.

    OneLake returns the name either relative to the lakehouse or prefixed with
    the lakehouse ID, depending on the container the listing was scoped to.
    """

    path = str(blob_name or "").strip().strip("/")
    prefix = f"{str(lakehouse_id)}/"
    if lakehouse_id:
        path = path.removeprefix(prefix)
    return path.strip("/")


def _object_of(path: str, schema_enabled: bool) -> Optional[Tuple[str, str, str, bool]]:
    """The lakehouse object a blob belongs to.

    Returns ``(container, name, schema, is_leaf)`` where ``is_leaf`` is True
    when the blob *is* the object itself (a file) rather than one of its
    contents.
    """

    parts = [part for part in path.split("/") if part]
    if len(parts) < 2 or parts[0] not in _CONTAINERS:
        return None
    container = parts[0]
    if container == "Tables" and schema_enabled:
        if len(parts) < 3:
            return None
        return (container, parts[2], parts[1], len(parts) == 3)
    return (container, parts[1], "", len(parts) == 2)


def _summarize_blobs(
    blobs: List[dict],
    lakehouse_id: str,
    schema_enabled: bool = False,
    shortcut_paths: Optional[set] = None,
) -> List[dict]:
    """Roll a lakehouse's blobs up into one row per table, folder or file."""

    shortcuts = shortcut_paths or set()
    objects: Dict[str, dict] = {}

    for blob in blobs:
        path = _blob_path(blob.get("Blob Name"), lakehouse_id)
        located = _object_of(path, schema_enabled)
        if located is None:
            continue
        container, name, schema, is_leaf = located
        key = "/".join(part for part in (container, schema, name) if part)
        record = objects.get(key)
        if record is None:
            record = {
                "path": key,
                "name": name,
                "schema": schema,
                "container": container,
                "type": (
                    "Table"
                    if container == "Tables"
                    else ("File" if is_leaf else "Folder")
                ),
                "size": 0,
                "files": 0,
                "deleted_files": 0,
                "modified": "",
                "shortcut": key in shortcuts,
            }
            objects[key] = record
        elif not is_leaf and record["type"] == "File":
            record["type"] = "Folder"

        deleted = bool(blob.get("Is Deleted"))
        if deleted:
            record["deleted_files"] += 1
        else:
            record["files"] += 1
            try:
                record["size"] += int(blob.get("Content Length") or 0)
            except (TypeError, ValueError):
                pass
        modified = str(blob.get("Last Modified") or blob.get("Creation Time") or "")
        if modified > record["modified"]:
            record["modified"] = modified

    for record in objects.values():
        # Nothing live left: the object itself is in the soft-deleted state.
        record["deleted"] = record["files"] == 0 and record["deleted_files"] > 0

    return sorted(objects.values(), key=lambda row: row["path"].lower())


def _summarize_objects(objects: List[dict]) -> dict:
    """Headline counts for the summary cards."""

    live = [row for row in objects if not row.get("deleted")]
    return {
        "tables": sum(1 for row in live if row["container"] == "Tables"),
        "files": sum(1 for row in live if row["container"] == "Files"),
        "deleted": sum(1 for row in objects if row.get("deleted")),
        "recoverable": sum(int(row.get("deleted_files") or 0) for row in objects),
        "size": sum(int(row.get("size") or 0) for row in objects),
        "size_label": _format_bytes(sum(int(row.get("size") or 0) for row in objects)),
    }


def _shortcut_paths(lakehouse_id: str, workspace_id: str) -> set:
    """The paths of the lakehouse's OneLake shortcuts (``Tables/my_shortcut``)."""

    from sempy_labs.lakehouse._shortcuts import list_shortcuts

    try:
        df = list_shortcuts(lakehouse=lakehouse_id, workspace=workspace_id)
    except Exception:
        return set()
    paths = set()
    for _, row in df.iterrows():
        parent = str(row.get("Shortcut Path") or "").strip("/")
        name = str(row.get("Shortcut Name") or "").strip("/")
        if name:
            paths.add("/".join(part for part in (parent, name) if part))
    return paths


def _node_type(
    depth: int, container: str, schema_enabled: bool, has_children: bool
) -> str:
    """What a tree node at this depth represents."""

    if depth == 0:
        return "Container"
    if container == "Tables":
        if schema_enabled:
            if depth == 1:
                return "Schema"
            if depth == 2:
                return "Table"
        elif depth == 1:
            return "Table"
    return "Folder" if has_children else "File"


def _build_tree(
    blobs: List[dict],
    lakehouse_id: str,
    schema_enabled: bool = False,
    shortcut_paths: Optional[set] = None,
    max_children: int = 200,
) -> List[dict]:
    """The lakehouse's blobs as a Tables / Files tree, aggregated bottom-up.

    ``max_children`` bounds the payload: a delta table can hold thousands of
    parquet files, and the extras are collapsed into a single summary node.
    """

    shortcuts = shortcut_paths or set()
    roots: Dict[str, dict] = {}

    for blob in blobs:
        path = _blob_path(blob.get("Blob Name"), lakehouse_id)
        parts = [part for part in path.split("/") if part]
        if len(parts) < 2 or parts[0] not in _CONTAINERS:
            continue
        deleted = bool(blob.get("Is Deleted"))
        try:
            size = 0 if deleted else int(blob.get("Content Length") or 0)
        except (TypeError, ValueError):
            size = 0

        children = roots
        walked: List[str] = []
        for part in parts:
            walked.append(part)
            node = children.get(part)
            if node is None:
                node = {
                    "name": part,
                    "path": "/".join(walked),
                    "size": 0,
                    "files": 0,
                    "deleted_files": 0,
                    "children": {},
                }
                children[part] = node
            node["size"] += size
            if deleted:
                node["deleted_files"] += 1
            else:
                node["files"] += 1
            children = node["children"]

    def _finalize(node: dict, depth: int, container: str, schema: str) -> dict:
        raw_children = list(node["children"].values())
        node_type = _node_type(depth, container, schema_enabled, bool(raw_children))
        child_schema = node["name"] if node_type == "Schema" else schema
        # Folders and tables first, then files; each group by name.
        raw_children.sort(key=lambda c: (not c["children"], c["name"].lower()))
        kept = raw_children[:max_children]
        children = [
            _finalize(child, depth + 1, container, child_schema) for child in kept
        ]
        if len(raw_children) > len(kept):
            children.append(
                {
                    "name": f"+{len(raw_children) - len(kept)} more",
                    "path": f"{node['path']}/...",
                    "type": "More",
                    "size": 0,
                    "size_label": "",
                    "files": 0,
                    "deleted_files": 0,
                    "deleted": False,
                    "shortcut": False,
                    "schema": "",
                    "children": [],
                }
            )
        return {
            "name": node["name"],
            "path": node["path"],
            "type": node_type,
            "size": node["size"],
            "size_label": _format_bytes(node["size"]),
            "files": node["files"],
            "deleted_files": node["deleted_files"],
            "deleted": node["files"] == 0 and node["deleted_files"] > 0,
            "shortcut": node["path"] in shortcuts,
            "schema": schema if node_type == "Table" else "",
            "children": children,
        }

    return [
        _finalize(roots[container], 0, container, "")
        for container in _CONTAINERS
        if container in roots
    ]


def _lakehouse_objects(
    workspace_id: str, lakehouse_id: str
) -> Tuple[List[dict], List[dict], bool]:
    """The contents of a lakehouse, flat and as a tree, plus its schema mode."""

    from sempy_labs.lakehouse._blobs import list_blobs
    from sempy_labs.lakehouse._schemas import is_schema_enabled

    try:
        schema_enabled = bool(
            is_schema_enabled(lakehouse=lakehouse_id, workspace=workspace_id)
        )
    except Exception:
        schema_enabled = False

    df = list_blobs(lakehouse=lakehouse_id, workspace=workspace_id)
    blobs = df.to_dict("records") if df is not None and not df.empty else []
    shortcuts = _shortcut_paths(lakehouse_id, workspace_id)
    objects = _summarize_blobs(
        blobs,
        lakehouse_id=str(lakehouse_id),
        schema_enabled=schema_enabled,
        shortcut_paths=shortcuts,
    )
    tree = _build_tree(
        blobs,
        lakehouse_id=str(lakehouse_id),
        schema_enabled=schema_enabled,
        shortcut_paths=shortcuts,
    )
    return objects, tree, schema_enabled


# ---------------------------------------------------------------------------
# Widget assets
# ---------------------------------------------------------------------------
_LHM_CSS = (
    """
.slls-lhm {
    __LIGHT_VARS__
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "Helvetica Neue", Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased; position: relative; display: flex; flex-direction: column;
    min-height: min(680px, calc(100vh - 32px)); max-width: 1200px; margin: 16px auto;
    color: var(--ui-text); background: var(--ui-bg); border: 1px solid var(--ui-border);
    border-radius: 12px; box-shadow: var(--ui-shadow-lg); overflow: hidden;
}
.slls-lhm.slls-lhm-dark { __DARK_VARS__ }
.slls-lhm *, .slls-lhm *::before, .slls-lhm *::after { box-sizing: border-box; }
.slls-lhm-head { flex: 0 0 auto; padding: 20px 22px 16px; }
.slls-lhm-body { display: flex; flex: 1 1 auto; min-height: 0; flex-direction: column; gap: 14px; padding: 0 22px 20px; }
.slls-lhm-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; }
.slls-lhm-card { padding: 12px 14px; border: 1px solid var(--ui-border); border-radius: 10px; background: var(--ui-bg-secondary); }
.slls-lhm-card-label { color: var(--ui-text-tertiary); font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: .04em; }
.slls-lhm-card-value { margin-top: 6px; font-size: 20px; font-weight: 600; font-variant-numeric: tabular-nums; }
.slls-lhm-toolbar { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.slls-lhm-search {
    flex: 1 1 220px; min-width: 160px; height: 32px; padding: 0 12px;
    border: 1px solid var(--ui-border-strong); border-radius: 999px;
    background: var(--ui-surface); color: var(--ui-text); font: inherit; font-size: 13px;
}
.slls-lhm-search:focus { outline: none; border-color: var(--ui-accent); }
.slls-lhm-pills { display: flex; gap: 6px; flex-wrap: wrap; }
.slls-lhm-pill {
    padding: 5px 12px; border: 1px solid var(--ui-border-strong); border-radius: 999px;
    background: var(--ui-surface); color: var(--ui-text-secondary);
    font: inherit; font-size: 12px; font-weight: 500; cursor: pointer;
}
.slls-lhm-pill:hover { background: var(--ui-surface-2); color: var(--ui-text); }
.slls-lhm-pill.slls-lhm-on { border-color: var(--ui-accent); background: var(--ui-accent); color: var(--ui-on-accent); }
.slls-lhm-seg { display: inline-flex; gap: 2px; margin-left: auto; padding: 2px; border: 1px solid var(--ui-border-strong); border-radius: 8px; background: var(--ui-surface); }
.slls-lhm-seg button {
    display: inline-flex; align-items: center; gap: 5px; padding: 4px 10px;
    border: none; border-radius: 6px; background: transparent; color: var(--ui-text-secondary);
    font: inherit; font-size: 12px; font-weight: 500; cursor: pointer;
}
.slls-lhm-seg button svg { display: block; width: 14px; height: 14px; }
.slls-lhm-seg button:hover { color: var(--ui-text); }
.slls-lhm-seg button.slls-lhm-on { background: var(--ui-accent); color: var(--ui-on-accent); }
/* ---------------- Tree view ---------------- */
.slls-lhm-tree {
    padding: 6px 0; font-size: 13px;
    font-family: 'Segoe UI', 'Segoe UI Web (West European)', -apple-system,
        BlinkMacSystemFont, Roboto, 'Helvetica Neue', sans-serif;
}
.slls-lhm-node {
    display: flex; align-items: center; gap: 8px; padding: 5px 12px 5px 6px; min-width: 0;
}
.slls-lhm-node:hover { background: var(--ui-surface-2); }
.slls-lhm-caret {
    display: inline-flex; align-items: center; justify-content: center;
    width: 20px; height: 20px; flex: 0 0 auto; padding: 0; border: none;
    border-radius: 4px; background: transparent; color: var(--ui-text-tertiary); cursor: pointer;
}
.slls-lhm-caret svg { display: block; width: 14px; height: 14px; transition: transform 120ms ease; }
.slls-lhm-caret.slls-lhm-open svg { transform: rotate(90deg); }
.slls-lhm-caret.slls-lhm-leaf { visibility: hidden; cursor: default; }
.slls-lhm-node-icon { display: inline-flex; flex: 0 0 auto; color: var(--ui-text-tertiary); }
.slls-lhm-node-icon svg { display: block; width: 15px; height: 15px; }
.slls-lhm-node-name {
    min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    font-size: 13px;
}
.slls-lhm-node-check { width: 15px; height: 15px; flex: 0 0 auto; margin: 0; accent-color: var(--ui-accent); }
.slls-lhm-node-check-slot { width: 15px; flex: 0 0 auto; }
.slls-lhm-selection { display: flex; align-items: center; gap: 8px; }
.slls-lhm-selected { color: var(--ui-text-secondary); font-size: 12px; white-space: nowrap; }
.slls-lhm-iconbtn.slls-lhm-danger { border-color: var(--ui-danger-border); color: var(--ui-danger-text); }
.slls-lhm-iconbtn.slls-lhm-danger:hover:not(:disabled) { border-color: var(--ui-danger); background: var(--ui-danger-bg); color: var(--ui-danger-text); }
.slls-lhm-run.slls-lhm-danger { border-color: var(--ui-danger); background: var(--ui-danger); color: #ffffff; }
.slls-lhm-node.slls-lhm-root .slls-lhm-node-name { font-weight: 600; }
.slls-lhm-node-spacer { flex: 1 1 auto; }
.slls-lhm-bar {
    position: relative; width: 150px; height: 20px; flex: 0 0 auto; border-radius: 4px;
    background: transparent; overflow: hidden;
}
.slls-lhm-bar-fill { position: absolute; inset: 0 auto 0 0; background: var(--ui-accent-soft); }
.slls-lhm-bar-text {
    position: absolute; inset: 0; display: flex; align-items: center; justify-content: flex-end;
    padding-right: 8px; color: var(--ui-text-secondary); font-size: 12px; font-variant-numeric: tabular-nums;
}
.slls-lhm-node-more { color: var(--ui-text-tertiary); font-size: 12px; font-style: italic; }
.slls-lhm-list { flex: 1 1 auto; min-height: 220px; border: 1px solid var(--ui-border); border-radius: 10px; background: var(--ui-bg); overflow: auto; }
.slls-lhm-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.slls-lhm-table th {
    position: sticky; top: 0; z-index: 1; padding: 9px 12px;
    border-bottom: 1px solid var(--ui-border); background: var(--ui-bg-secondary);
    color: var(--ui-text-tertiary); font-size: 11px; font-weight: 600; text-align: left;
    text-transform: uppercase; letter-spacing: .04em; white-space: nowrap;
}
.slls-lhm-table td { padding: 9px 12px; border-bottom: 1px solid var(--ui-border); vertical-align: middle; }
.slls-lhm-table tr:last-child td { border-bottom: none; }
.slls-lhm-table tr:hover td { background: var(--ui-surface-2); }
.slls-lhm-num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
.slls-lhm-name { display: flex; align-items: center; gap: 8px; min-width: 0; }
.slls-lhm-name svg { display: block; width: 15px; height: 15px; flex: 0 0 auto; color: var(--ui-text-tertiary); }
.slls-lhm-name-text { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-lhm-schema { color: var(--ui-text-tertiary); }
.slls-lhm-badge {
    display: inline-flex; align-items: center; gap: 4px; padding: 2px 8px;
    border: 1px solid var(--ui-border); border-radius: 999px;
    background: var(--ui-surface-2); color: var(--ui-text-secondary);
    font-size: 11px; font-weight: 500; white-space: nowrap;
}
.slls-lhm-badge.slls-lhm-deleted { border-color: var(--ui-danger-border); background: var(--ui-danger-bg); color: var(--ui-danger-text); }
.slls-lhm-badge svg { display: block; width: 12px; height: 12px; }
.slls-lhm-rowbtns { display: flex; justify-content: flex-end; gap: 6px; }
.slls-lhm-iconbtn {
    display: inline-flex; align-items: center; justify-content: center;
    width: 28px; height: 28px; padding: 0; border: 1px solid var(--ui-border-strong);
    border-radius: 8px; background: var(--ui-surface); color: var(--ui-text); cursor: pointer;
}
.slls-lhm-iconbtn svg { display: block; width: 15px; height: 15px; }
.slls-lhm-iconbtn:hover:not(:disabled) { border-color: var(--ui-accent); color: var(--ui-accent); }
.slls-lhm-iconbtn:disabled { opacity: .45; cursor: not-allowed; }
.slls-lhm-empty { padding: 48px 16px; color: var(--ui-text-secondary); font-size: 13px; text-align: center; }
.slls-lhm-note { display: none; align-items: center; gap: 8px; padding: 10px 14px; border: 1px solid var(--ui-border); border-radius: 10px; background: var(--ui-bg-secondary); font-size: 12.5px; }
.slls-lhm-note.slls-lhm-show { display: flex; }
.slls-lhm-note.slls-lhm-bad { border-color: var(--ui-danger-border); background: var(--ui-danger-bg); color: var(--ui-danger-text); }
.slls-lhm-progress { display: none; position: relative; height: 3px; overflow: hidden; background: var(--ui-accent-soft); }
.slls-lhm-progress.slls-lhm-show { display: block; }
.slls-lhm-progress::after { content: ""; position: absolute; inset-block: 0; left: -35%; width: 35%; background: var(--ui-accent); animation: sllsLhmProgress 1s ease-in-out infinite; }
@keyframes sllsLhmProgress { from { transform: translateX(0); } to { transform: translateX(390%); } }
.slls-lhm-backdrop { display: flex; flex: 1 1 auto; }
.slls-lhm-backdrop.slls-lhm-modal {
    position: absolute; inset: 0; z-index: 120; align-items: center; justify-content: center;
    padding: 24px; overflow: auto;
    background: color-mix(in srgb, var(--ui-bg) 72%, transparent); backdrop-filter: blur(8px);
}
.slls-lhm-panel { flex: 1 1 auto; padding: 4px 22px 24px; }
.slls-lhm-backdrop.slls-lhm-modal .slls-lhm-panel {
    flex: 0 1 720px; padding: 20px; border: 1px solid var(--ui-border-strong);
    border-radius: 12px; background: var(--ui-bg); box-shadow: var(--ui-shadow-lg);
}
.slls-lhm-panel-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 14px; }
.slls-lhm-panel-title { margin: 0; font-size: 14px; font-weight: 600; }
.slls-lhm-panel-sub { margin-top: 3px; color: var(--ui-text-secondary); font-size: 12.5px; }
.slls-lhm-close {
    display: none; align-items: center; justify-content: center; width: 32px; height: 32px;
    padding: 0; border: 1px solid var(--ui-border-strong); border-radius: 50%;
    background: var(--ui-surface); color: var(--ui-text); cursor: pointer;
}
.slls-lhm-close svg { display: block; width: 16px; height: 16px; }
.slls-lhm-backdrop.slls-lhm-modal .slls-lhm-close, .slls-lhm-dialog .slls-lhm-close { display: inline-flex; }
.slls-lhm-fields { display: flex; align-items: flex-end; gap: 10px; flex-wrap: wrap; }
.slls-lhm-field { display: flex; flex: 1 1 220px; min-width: 0; flex-direction: column; gap: 5px; }
.slls-lhm-field label { padding-left: 4px; color: var(--ui-text-tertiary); font-size: 11px; font-weight: 600; text-transform: uppercase; }
.slls-lhm-field .slls-ss-btn { border-radius: 999px; padding: 7px 12px 7px 15px; background: var(--ui-surface); font-size: 13.5px; }
.slls-lhm-run {
    border: 1px solid var(--ui-accent); border-radius: 999px; padding: 7px 18px;
    background: var(--ui-accent); color: var(--ui-on-accent);
    font: inherit; font-size: 13.5px; font-weight: 500; cursor: pointer;
}
.slls-lhm-run:disabled { opacity: .5; cursor: default; }
.slls-lhm-ghost {
    border: 1px solid var(--ui-border-strong); border-radius: 999px; padding: 7px 16px;
    background: var(--ui-surface); color: var(--ui-text);
    font: inherit; font-size: 13.5px; cursor: pointer;
}
.slls-lhm-overlay {
    position: absolute; inset: 0; z-index: 140; display: flex;
    align-items: center; justify-content: center; padding: 24px;
    background: color-mix(in srgb, var(--ui-bg) 72%, transparent); backdrop-filter: blur(8px);
}
.slls-lhm-dialog { width: min(460px, 100%); padding: 20px; border: 1px solid var(--ui-border-strong); border-radius: 12px; background: var(--ui-bg); box-shadow: var(--ui-shadow-lg); }
.slls-lhm-check { display: flex; align-items: center; gap: 8px; margin-top: 10px; font-size: 13px; }
.slls-lhm-check input { width: 15px; height: 15px; accent-color: var(--ui-accent); }
.slls-lhm-text {
    width: 100%; height: 32px; margin-top: 6px; padding: 0 10px;
    border: 1px solid var(--ui-border-strong); border-radius: 8px;
    background: var(--ui-surface); color: var(--ui-text); font: inherit; font-size: 13px;
}
.slls-lhm-dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 18px; }
.slls-lhm-attr { padding: 0 22px 16px; }
@media (max-width: 700px) {
    .slls-lhm { min-height: calc(100vh - 16px); margin: 8px; }
    .slls-lhm-fields { align-items: stretch; flex-direction: column; }
}
__HEADER_CSS__
__SEARCH_CSS__
""".replace("__LIGHT_VARS__", _UI_LIGHT_VARS)
    .replace("__DARK_VARS__", _UI_DARK_VARS)
    .replace("__HEADER_CSS__", "")
    .replace("__SEARCH_CSS__", _UI_SEARCH_SELECT_CSS)
)

_LHM_CSS += _ui_scoped_header_css(".slls-lhm")
_LHM_CSS += _ui_scoped_button_press_css(".slls-lhm")
_LHM_CSS += _ui_scoped_attribution_css(".slls-lhm")
_LHM_CSS += "\n" + _ui_fullscreen_css(
    ".slls-lhm", "slls-lhm-fullscreen", bg_var="var(--ui-bg)"
)


_LHM_JS = (
    _UI_SEARCH_SELECT_JS
    + "\n"
    + _ui_fullscreen_setup_js("sllsLhmSetupFullscreen")
    + r"""
function render({ model, el }) {
    const root = document.createElement("div");
    root.className = "slls-lhm" + (model.get("dark_mode") ? " slls-lhm-dark" : "");
    el.appendChild(root);

    // ---------------- Header ----------------
    const headWrap = document.createElement("div"); headWrap.className = "slls-lhm-head";
    const header = document.createElement("div"); header.className = "sl-header";
    const titleIcon = document.createElement("span"); titleIcon.className = "sl-title-icon"; titleIcon.innerHTML = `__ICON_LAKEHOUSE__`;
    const titleWrap = document.createElement("div"); titleWrap.className = "sl-titlewrap";
    const title = document.createElement("div"); title.className = "sl-title"; title.textContent = "Lakehouse Manager";
    const subtitle = document.createElement("div"); subtitle.className = "sl-subtitle";
    titleWrap.append(title, subtitle);
    const spacer = document.createElement("div"); spacer.className = "sl-head-spacer";
    const changeBtn = document.createElement("button"); changeBtn.type = "button"; changeBtn.className = "sl-change-btn"; changeBtn.innerHTML = `__ICON_SWAP__`;
    changeBtn.title = "Change lakehouse"; changeBtn.setAttribute("aria-label", changeBtn.title);
    const reloadBtn = document.createElement("button"); reloadBtn.type = "button"; reloadBtn.className = "sl-reload-btn"; reloadBtn.innerHTML = `__ICON_REFRESH__`;
    reloadBtn.title = "Reload the lakehouse contents"; reloadBtn.setAttribute("aria-label", reloadBtn.title);
    const fsBtn = document.createElement("button"); fsBtn.type = "button"; fsBtn.className = "sl-theme-btn";
    const themeBtn = document.createElement("button"); themeBtn.type = "button"; themeBtn.className = "sl-theme-btn";
    header.append(titleIcon, titleWrap, changeBtn, spacer, reloadBtn, fsBtn, themeBtn);
    headWrap.appendChild(header);
    const progress = document.createElement("div"); progress.className = "slls-lhm-progress";
    progress.setAttribute("role", "progressbar"); progress.setAttribute("aria-label", "Working");

    function renderTheme() {
        const dark = root.classList.contains("slls-lhm-dark");
        themeBtn.innerHTML = dark ? `__ICON_SUN__` : `__ICON_MOON__`;
        const label = dark ? "Switch to light mode" : "Switch to dark mode";
        themeBtn.title = label; themeBtn.setAttribute("aria-label", label);
    }
    themeBtn.addEventListener("click", () => {
        root.classList.toggle("slls-lhm-dark");
        model.set("dark_mode", root.classList.contains("slls-lhm-dark")); model.save_changes();
        renderTheme();
    });
    sllsLhmSetupFullscreen(root, fsBtn, "slls-lhm-fullscreen", `__ICON_FS__`, `__ICON_FS_EXIT__`);
    renderTheme();

    // ---------------- Picker ----------------
    const backdrop = document.createElement("div"); backdrop.className = "slls-lhm-backdrop";
    const panel = document.createElement("div"); panel.className = "slls-lhm-panel";
    const panelHead = document.createElement("div"); panelHead.className = "slls-lhm-panel-head";
    panelHead.innerHTML = '<div><h2 class="slls-lhm-panel-title">Connect to a lakehouse</h2>'
        + '<div class="slls-lhm-panel-sub">Select a workspace and lakehouse to browse its tables, folders and files.</div></div>';
    const closePicker = document.createElement("button"); closePicker.type = "button"; closePicker.className = "slls-lhm-close";
    closePicker.innerHTML = `__ICON_CLOSE__`; closePicker.title = "Close"; closePicker.setAttribute("aria-label", "Close the lakehouse picker");
    panelHead.appendChild(closePicker);
    const fields = document.createElement("div"); fields.className = "slls-lhm-fields";
    function field(label, picker) {
        const wrap = document.createElement("div"); wrap.className = "slls-lhm-field";
        const lab = document.createElement("label"); lab.textContent = label;
        wrap.append(lab, picker.el); return wrap;
    }
    function act(action, extra) {
        model.set("pending_action", Object.assign({ action: action }, extra || {}));
        model.set("action_trigger", (model.get("action_trigger") || 0) + 1);
        model.save_changes();
    }
    const ws = createSearchSelect({
        placeholder: "Select a workspace…", searchPlaceholder: "Filter workspaces…",
        ariaLabel: "Workspace", emptyLabel: "Loading workspaces…",
        onChange: (option) => {
            model.set("selected_workspace_id", option.value);
            model.set("selected_lakehouse_id", "");
            model.set("available_lakehouses", []);
            act("workspace", { workspace: option.value });
        },
    });
    const lh = createSearchSelect({
        placeholder: "Select a lakehouse…", searchPlaceholder: "Filter lakehouses…",
        ariaLabel: "Lakehouse", emptyLabel: "Select a workspace first…",
        onChange: (option) => {
            model.set("selected_lakehouse_id", option.value); model.save_changes(); renderState();
        },
    });
    const openBtn = document.createElement("button"); openBtn.type = "button"; openBtn.className = "slls-lhm-run"; openBtn.textContent = "Connect";
    fields.append(field("Workspace", ws), field("Lakehouse", lh), openBtn);
    panel.append(panelHead, fields); backdrop.appendChild(panel);

    // ---------------- Contents ----------------
    const body = document.createElement("div"); body.className = "slls-lhm-body";
    const cards = document.createElement("div"); cards.className = "slls-lhm-cards";
    const cardValues = {};
    for (const [key, label] of [["tables", "Tables"], ["files", "Files"], ["deleted", "Deleted"], ["size_label", "Size"]]) {
        const card = document.createElement("div"); card.className = "slls-lhm-card";
        const lab = document.createElement("div"); lab.className = "slls-lhm-card-label"; lab.textContent = label;
        const val = document.createElement("div"); val.className = "slls-lhm-card-value"; val.textContent = "—";
        card.append(lab, val); cards.appendChild(card); cardValues[key] = val;
    }
    const toolbar = document.createElement("div"); toolbar.className = "slls-lhm-toolbar";
    const search = document.createElement("input"); search.type = "search"; search.className = "slls-lhm-search";
    search.setAttribute("aria-label", "Filter objects by name");
    const pills = document.createElement("div"); pills.className = "slls-lhm-pills"; pills.setAttribute("role", "group");
    pills.setAttribute("aria-label", "Filter objects");
    let filter = "All";
    const pillButtons = ["All", "Tables", "Files", "Deleted"].map((name) => {
        const pill = document.createElement("button"); pill.type = "button"; pill.className = "slls-lhm-pill"; pill.textContent = name;
        pill.addEventListener("click", () => { filter = name; renderRows(); });
        pills.appendChild(pill); return { name: name, el: pill };
    });
    let view = "Tree";
    const selection = document.createElement("div"); selection.className = "slls-lhm-selection";
    const selectedText = document.createElement("span"); selectedText.className = "slls-lhm-selected";
    const recoverSelected = document.createElement("button"); recoverSelected.type = "button";
    recoverSelected.className = "slls-lhm-iconbtn"; recoverSelected.innerHTML = `__ICON_UNDO__`;
    recoverSelected.title = "Recover the selected objects";
    recoverSelected.setAttribute("aria-label", recoverSelected.title);
    const deleteSelected = document.createElement("button"); deleteSelected.type = "button";
    deleteSelected.className = "slls-lhm-iconbtn slls-lhm-danger"; deleteSelected.innerHTML = `__ICON_TRASH__`;
    deleteSelected.title = "Delete the selected objects";
    deleteSelected.setAttribute("aria-label", deleteSelected.title);
    selection.append(selectedText, recoverSelected, deleteSelected);
    const segment = document.createElement("div"); segment.className = "slls-lhm-seg";
    segment.setAttribute("role", "group"); segment.setAttribute("aria-label", "View");
    const viewButtons = [["Tree", `__ICON_TREE__`], ["Table", `__ICON_TABLE__`]].map(([name, icon]) => {
        const button = document.createElement("button"); button.type = "button";
        button.innerHTML = icon;
        const label = document.createElement("span"); label.textContent = name;
        button.appendChild(label);
        button.addEventListener("click", () => { view = name; renderRows(); });
        segment.appendChild(button); return { name: name, el: button };
    });
    toolbar.append(search, selection, pills, segment);
    const listWrap = document.createElement("div"); listWrap.className = "slls-lhm-list";
    const note = document.createElement("div"); note.className = "slls-lhm-note";
    const noteIcon = document.createElement("span"); const noteText = document.createElement("span");
    note.append(noteIcon, noteText);
    body.append(cards, toolbar, listWrap, note);

    const attribution = document.createElement("div"); attribution.className = "slls-lhm-attr";
    attribution.innerHTML = `__ATTRIBUTION__`;
    root.append(headWrap, progress, backdrop, body, attribution);

    // ---------------- Maintenance dialog ----------------
    let maintenanceTarget = null;
    const overlay = document.createElement("div"); overlay.className = "slls-lhm-overlay"; overlay.style.display = "none";
    const dialog = document.createElement("div"); dialog.className = "slls-lhm-dialog";
    const dialogHead = document.createElement("div"); dialogHead.className = "slls-lhm-panel-head";
    const dialogTitle = document.createElement("div");
    dialogTitle.innerHTML = '<h2 class="slls-lhm-panel-title">Table maintenance</h2>'
        + '<div class="slls-lhm-panel-sub"></div>';
    const dialogClose = document.createElement("button"); dialogClose.type = "button"; dialogClose.className = "slls-lhm-close";
    dialogClose.innerHTML = `__ICON_CLOSE__`; dialogClose.title = "Close"; dialogClose.setAttribute("aria-label", "Close table maintenance");
    dialogHead.append(dialogTitle, dialogClose);
    function check(label, checked) {
        const wrap = document.createElement("label"); wrap.className = "slls-lhm-check";
        const input = document.createElement("input"); input.type = "checkbox"; input.checked = checked;
        const text = document.createElement("span"); text.textContent = label;
        wrap.append(input, text); return { el: wrap, input: input };
    }
    const optimizeChk = check("Optimize (compact small files)", true);
    const vorderChk = check("Apply V-Order", true);
    const vacuumChk = check("Vacuum (remove unreferenced files)", false);
    const retention = document.createElement("input"); retention.type = "text"; retention.className = "slls-lhm-text";
    retention.placeholder = "Retention period, e.g. 7:00:00:00"; retention.setAttribute("aria-label", "Vacuum retention period");
    retention.style.display = "none";
    vacuumChk.input.addEventListener("change", () => { retention.style.display = vacuumChk.input.checked ? "" : "none"; });
    const dialogActions = document.createElement("div"); dialogActions.className = "slls-lhm-dialog-actions";
    const cancelBtn = document.createElement("button"); cancelBtn.type = "button"; cancelBtn.className = "slls-lhm-ghost"; cancelBtn.textContent = "Cancel";
    const runBtn = document.createElement("button"); runBtn.type = "button"; runBtn.className = "slls-lhm-run"; runBtn.textContent = "Run";
    dialogActions.append(cancelBtn, runBtn);
    dialog.append(dialogHead, optimizeChk.el, vorderChk.el, vacuumChk.el, retention, dialogActions);
    overlay.appendChild(dialog); root.appendChild(overlay);

    function closeDialog() { maintenanceTarget = null; overlay.style.display = "none"; }
    cancelBtn.addEventListener("click", closeDialog);
    dialogClose.addEventListener("click", closeDialog);
    overlay.addEventListener("click", (event) => { if (event.target === overlay) closeDialog(); });
    runBtn.addEventListener("click", () => {
        if (!maintenanceTarget) return;
        const payload = {
            path: maintenanceTarget.path, table: maintenanceTarget.name,
            schema: maintenanceTarget.schema || "",
            optimize: optimizeChk.input.checked, v_order: vorderChk.input.checked,
            vacuum: vacuumChk.input.checked, retention_period: retention.value.trim(),
        };
        closeDialog();
        act("maintenance", payload);
    });

    // ---------------- Delete confirmation ----------------
    const confirmOverlay = document.createElement("div"); confirmOverlay.className = "slls-lhm-overlay";
    confirmOverlay.style.display = "none";
    const confirmDialog = document.createElement("div"); confirmDialog.className = "slls-lhm-dialog";
    const confirmHead = document.createElement("div"); confirmHead.className = "slls-lhm-panel-head";
    const confirmTitle = document.createElement("div");
    confirmTitle.innerHTML = '<h2 class="slls-lhm-panel-title">Delete objects</h2>'
        + '<div class="slls-lhm-panel-sub"></div>';
    const confirmClose = document.createElement("button"); confirmClose.type = "button"; confirmClose.className = "slls-lhm-close";
    confirmClose.innerHTML = `__ICON_CLOSE__`; confirmClose.title = "Close";
    confirmClose.setAttribute("aria-label", "Close the delete confirmation");
    confirmHead.append(confirmTitle, confirmClose);
    const confirmList = document.createElement("div"); confirmList.className = "slls-lhm-panel-sub";
    const confirmActions = document.createElement("div"); confirmActions.className = "slls-lhm-dialog-actions";
    const confirmCancel = document.createElement("button"); confirmCancel.type = "button";
    confirmCancel.className = "slls-lhm-ghost"; confirmCancel.textContent = "Cancel";
    const confirmDelete = document.createElement("button"); confirmDelete.type = "button";
    confirmDelete.className = "slls-lhm-run slls-lhm-danger"; confirmDelete.textContent = "Delete";
    confirmActions.append(confirmCancel, confirmDelete);
    confirmDialog.append(confirmHead, confirmList, confirmActions);
    confirmOverlay.appendChild(confirmDialog); root.appendChild(confirmOverlay);

    function closeConfirm() { confirmOverlay.style.display = "none"; }
    confirmCancel.addEventListener("click", closeConfirm);
    confirmClose.addEventListener("click", closeConfirm);
    confirmOverlay.addEventListener("click", (event) => {
        if (event.target === confirmOverlay) closeConfirm();
    });
    confirmDelete.addEventListener("click", () => {
        const paths = [...selected];
        closeConfirm();
        if (paths.length) act("delete", { paths: paths });
    });

    // ---------------- State ----------------
    let pickerOpen = !(model.get("selected_lakehouse_id") || "") || !(model.get("objects") || []).length;
    // Tree objects ticked for a bulk recover or delete.
    const selected = new Set();

    function busy() {
        return model.get("loading") === true || model.get("busy") === true
            || model.get("picker_loading") === true;
    }

    // Names seen in the pickers, so the loaded lakehouse stays labelled after
    // the picker moves on to another workspace.
    const knownNames = new Map();
    function nameOf(list, id) {
        for (const item of model.get(list) || []) knownNames.set(item.id, item.name);
        return knownNames.get(id) || "";
    }

    // Tree nodes which are open; both containers start expanded.
    const expanded = new Set(["Tables", "Files"]);

    function rowActions(row) {
        const buttons = document.createElement("div"); buttons.className = "slls-lhm-rowbtns";
        // Containers and schemas are groupings, not objects which can be restored.
        const grouping = row.type === "Container" || row.type === "Schema";
        if ((row.deleted || row.deleted_files) && !grouping) {
            const recover = document.createElement("button"); recover.type = "button"; recover.className = "slls-lhm-iconbtn";
            recover.innerHTML = `__ICON_UNDO__`; recover.title = "Recover this object";
            recover.setAttribute("aria-label", "Recover " + row.path);
            recover.disabled = busy();
            recover.addEventListener("click", () => act("recover", { path: row.path }));
            buttons.appendChild(recover);
        }
        if (row.type === "Table" && !row.deleted && !row.shortcut) {
            const maintain = document.createElement("button"); maintain.type = "button"; maintain.className = "slls-lhm-iconbtn";
            maintain.innerHTML = `__ICON_WRENCH__`; maintain.title = "Run table maintenance";
            maintain.setAttribute("aria-label", "Run table maintenance on " + row.path);
            maintain.disabled = busy();
            maintain.addEventListener("click", () => {
                maintenanceTarget = row;
                dialogTitle.querySelector(".slls-lhm-panel-sub").textContent = row.path;
                retention.style.display = vacuumChk.input.checked ? "" : "none";
                overlay.style.display = "flex";
            });
            buttons.appendChild(maintain);
        }
        return buttons;
    }

    function renderRows() {
        for (const entry of pillButtons) entry.el.classList.toggle("slls-lhm-on", entry.name === filter);
        for (const entry of viewButtons) entry.el.classList.toggle("slls-lhm-on", entry.name === view);
        // The tree already groups by container, so the pills only apply to the table.
        pills.style.display = view === "Tree" ? "none" : "";
        selection.style.display = view === "Tree" ? "flex" : "none";
        search.placeholder = view === "Tree" ? "Search tree…" : "Search tables and files…";
        renderSelection();
        listWrap.innerHTML = "";
        if (!(model.get("objects") || []).length) {
            listWrap.appendChild(emptyState(model.get("loading")
                ? "Reading the lakehouse…" : "This lakehouse is empty."));
            return;
        }
        if (view === "Tree") renderTree(); else renderTable();
    }

    // Selectable: everything the user can act on, so not the containers and
    // schemas (groupings) nor the "+N more" placeholders.
    function selectable(node) {
        return node.type !== "Container" && node.type !== "Schema" && node.type !== "More";
    }

    function selectedNodes() {
        const found = [];
        const walk = (nodes) => {
            for (const node of nodes) {
                if (selected.has(node.path)) found.push(node);
                walk(node.children || []);
            }
        };
        walk(model.get("tree") || []);
        return found;
    }

    function renderSelection() {
        const nodes = selectedNodes();
        // Selections made before a reload may no longer exist.
        if (nodes.length !== selected.size) {
            selected.clear();
            for (const node of nodes) selected.add(node.path);
        }
        selectedText.textContent = nodes.length
            ? `${nodes.length} item${nodes.length === 1 ? "" : "s"} selected`
            : "No items selected";
        const recoverable = nodes.some((node) => node.deleted || node.deleted_files);
        recoverSelected.disabled = busy() || !recoverable;
        deleteSelected.disabled = busy() || !nodes.length;
    }

    recoverSelected.addEventListener("click", () => {
        const paths = selectedNodes()
            .filter((node) => node.deleted || node.deleted_files)
            .map((node) => node.path);
        if (paths.length) act("recover", { paths: paths });
    });
    deleteSelected.addEventListener("click", () => {
        const nodes = selectedNodes();
        if (!nodes.length) return;
        confirmTitle.querySelector(".slls-lhm-panel-sub").textContent =
            `${nodes.length} object${nodes.length === 1 ? "" : "s"} will be deleted from the lakehouse.`;
        confirmList.textContent = nodes.slice(0, 10).map((node) => node.path).join(", ")
            + (nodes.length > 10 ? `, +${nodes.length - 10} more` : "");
        confirmOverlay.style.display = "flex";
    });

    function emptyState(text) {
        const empty = document.createElement("div"); empty.className = "slls-lhm-empty";
        empty.textContent = text; return empty;
    }

    function collectNodes(nodes, depth, out, term) {
        let matched = false;
        for (const node of nodes) {
            const children = node.children || [];
            const buffer = [];
            const childMatch = collectNodes(children, depth + 1, buffer, term);
            const selfMatch = !term || node.name.toLowerCase().indexOf(term) >= 0;
            if (!selfMatch && !childMatch) continue;
            matched = true;
            // A search opens the branches it matched inside.
            const open = children.length > 0
                && ((term && childMatch) || expanded.has(node.path));
            out.push({ node: node, depth: depth, open: open, hasChildren: children.length > 0 });
            if (open) for (const row of buffer) out.push(row);
        }
        return matched;
    }

    function treeIcon(type) {
        if (type === "Table") return `__ICON_TABLE__`;
        if (type === "Schema") return `__ICON_SCHEMA__`;
        if (type === "File") return `__ICON_FILE__`;
        return `__ICON_FOLDER__`;
    }

    function renderTree() {
        const rows = [];
        collectNodes(model.get("tree") || [], 0, rows, search.value.trim().toLowerCase());
        if (!rows.length) { listWrap.appendChild(emptyState("No objects match this search.")); return; }
        const largest = rows.reduce((max, row) => Math.max(max, row.node.size || 0), 0) || 1;
        const wrap = document.createElement("div"); wrap.className = "slls-lhm-tree";
        for (const row of rows) {
            const node = row.node;
            const el = document.createElement("div");
            el.className = "slls-lhm-node" + (row.depth === 0 ? " slls-lhm-root" : "");
            el.style.paddingLeft = (6 + row.depth * 18) + "px";

            const caret = document.createElement("button"); caret.type = "button";
            caret.className = "slls-lhm-caret" + (row.hasChildren ? (row.open ? " slls-lhm-open" : "") : " slls-lhm-leaf");
            caret.innerHTML = `__ICON_CARET__`;
            if (row.hasChildren) {
                const label = (row.open ? "Collapse " : "Expand ") + node.path;
                caret.title = row.open ? "Collapse" : "Expand";
                caret.setAttribute("aria-label", label);
                caret.setAttribute("aria-expanded", String(row.open));
                caret.addEventListener("click", () => {
                    if (expanded.has(node.path)) expanded.delete(node.path); else expanded.add(node.path);
                    renderRows();
                });
            } else {
                caret.tabIndex = -1; caret.setAttribute("aria-hidden", "true");
            }
            el.appendChild(caret);

            if (node.type === "More") {
                const more = document.createElement("span"); more.className = "slls-lhm-node-more";
                more.textContent = node.name; el.appendChild(more); wrap.appendChild(el); continue;
            }

            if (selectable(node)) {
                const check = document.createElement("input"); check.type = "checkbox";
                check.className = "slls-lhm-node-check"; check.checked = selected.has(node.path);
                check.setAttribute("aria-label", "Select " + node.path);
                check.addEventListener("change", () => {
                    if (check.checked) selected.add(node.path); else selected.delete(node.path);
                    renderSelection();
                });
                el.appendChild(check);
            } else {
                const slot = document.createElement("span"); slot.className = "slls-lhm-node-check-slot";
                el.appendChild(slot);
            }

            const icon = document.createElement("span"); icon.className = "slls-lhm-node-icon";
            icon.innerHTML = treeIcon(node.type);
            const name = document.createElement("span"); name.className = "slls-lhm-node-name";
            name.textContent = node.name; name.title = node.path;
            el.append(icon, name);
            if (node.deleted) el.appendChild(badge("Deleted", `__ICON_TRASH__`, true));
            else if (node.deleted_files) el.appendChild(badge(node.deleted_files + " deleted", `__ICON_TRASH__`, true));
            if (node.shortcut) el.appendChild(badge("Shortcut", `__ICON_LINK__`, false));

            const gap = document.createElement("span"); gap.className = "slls-lhm-node-spacer";
            el.append(gap, rowActions(node));

            const bar = document.createElement("span"); bar.className = "slls-lhm-bar";
            const fill = document.createElement("span"); fill.className = "slls-lhm-bar-fill";
            fill.style.width = Math.min(100, ((node.size || 0) / largest) * 100) + "%";
            const barText = document.createElement("span"); barText.className = "slls-lhm-bar-text";
            barText.textContent = node.size_label || "";
            bar.append(fill, barText); el.appendChild(bar);
            wrap.appendChild(el);
        }
        listWrap.appendChild(wrap);
    }

    function renderTable() {
        const term = search.value.trim().toLowerCase();
        const rows = (model.get("objects") || []).filter((row) => {
            if (filter === "Tables" && row.container !== "Tables") return false;
            if (filter === "Files" && row.container !== "Files") return false;
            if (filter === "Deleted" && !row.deleted && !row.deleted_files) return false;
            return !term || row.path.toLowerCase().indexOf(term) >= 0;
        });
        if (!rows.length) { listWrap.appendChild(emptyState("No objects match this filter.")); return; }
        const table = document.createElement("table"); table.className = "slls-lhm-table";
        const thead = document.createElement("thead"); const headRow = document.createElement("tr");
        for (const [label, cls] of [["Name", ""], ["Type", ""], ["Size", "slls-lhm-num"], ["Files", "slls-lhm-num"], ["Last modified", ""], ["Status", ""], ["", "slls-lhm-num"]]) {
            const th = document.createElement("th"); th.textContent = label; if (cls) th.className = cls; headRow.appendChild(th);
        }
        thead.appendChild(headRow); table.appendChild(thead);
        const tbody = document.createElement("tbody");
        for (const row of rows) {
            const tr = document.createElement("tr");
            const nameCell = document.createElement("td");
            const name = document.createElement("div"); name.className = "slls-lhm-name";
            const icon = document.createElement("span");
            icon.innerHTML = row.type === "Table" ? `__ICON_TABLE__` : (row.type === "Folder" ? `__ICON_FOLDER__` : `__ICON_FILE__`);
            const label = document.createElement("span"); label.className = "slls-lhm-name-text";
            if (row.schema) {
                const schema = document.createElement("span"); schema.className = "slls-lhm-schema"; schema.textContent = row.schema + ".";
                label.append(schema, document.createTextNode(row.name));
            } else { label.textContent = row.name; }
            label.title = row.path;
            name.append(icon, label);
            nameCell.appendChild(name); tr.appendChild(nameCell);

            const typeCell = document.createElement("td"); typeCell.textContent = row.type; tr.appendChild(typeCell);
            const sizeCell = document.createElement("td"); sizeCell.className = "slls-lhm-num"; sizeCell.textContent = row.size_label || "—"; tr.appendChild(sizeCell);
            const filesCell = document.createElement("td"); filesCell.className = "slls-lhm-num"; filesCell.textContent = String(row.files || 0); tr.appendChild(filesCell);
            const modCell = document.createElement("td"); modCell.textContent = row.modified_label || "—"; tr.appendChild(modCell);

            const statusCell = document.createElement("td");
            if (row.deleted) statusCell.appendChild(badge("Deleted", `__ICON_TRASH__`, true));
            else if (row.deleted_files) statusCell.appendChild(badge(row.deleted_files + " deleted file(s)", `__ICON_TRASH__`, true));
            else if (row.shortcut) statusCell.appendChild(badge("Shortcut", `__ICON_LINK__`, false));
            else statusCell.textContent = "";
            tr.appendChild(statusCell);

            const actionsCell = document.createElement("td");
            actionsCell.appendChild(rowActions(row)); tr.appendChild(actionsCell);
            tbody.appendChild(tr);
        }
        table.appendChild(tbody); listWrap.appendChild(table);
    }

    function badge(text, iconSvg, bad) {
        const span = document.createElement("span");
        span.className = "slls-lhm-badge" + (bad ? " slls-lhm-deleted" : "");
        span.innerHTML = iconSvg; span.appendChild(document.createTextNode(text));
        return span;
    }

    function renderState() {
        const loading = model.get("picker_loading") === true;
        const working = busy();
        const workspaceId = model.get("selected_workspace_id") || "";
        const lakehouseId = model.get("selected_lakehouse_id") || "";
        ws.setEmptyLabel(loading ? "Loading workspaces…" : "No workspaces");
        ws.setOptions((model.get("available_workspaces") || []).map((x) => ({ value: x.id, label: x.name })), workspaceId);
        lh.setEmptyLabel(!workspaceId ? "Select a workspace first…" : (loading ? "Loading lakehouses…" : "No lakehouses"));
        lh.setOptions((model.get("available_lakehouses") || []).map((x) => ({ value: x.id, label: x.name })), lakehouseId);
        ws.setDisabled(working); lh.setDisabled(!workspaceId || working);
        openBtn.disabled = !lakehouseId || working;

        // Contents and actions belong to the loaded lakehouse, not the picker selection.
        const loadedWorkspaceId = model.get("loaded_workspace_id") || "";
        const loadedLakehouseId = model.get("loaded_lakehouse_id") || "";
        const hasContents = Boolean(loadedLakehouseId || lakehouseId) && !pickerOpen;
        backdrop.style.display = pickerOpen ? "flex" : "none";
        backdrop.classList.toggle("slls-lhm-modal", pickerOpen && Boolean((model.get("objects") || []).length));
        closePicker.style.display = (model.get("objects") || []).length ? "" : "none";
        body.style.display = hasContents ? "flex" : "none";
        changeBtn.style.display = hasContents ? "" : "none";
        reloadBtn.style.display = hasContents ? "" : "none";
        reloadBtn.disabled = working;
        reloadBtn.classList.toggle("sl-spinning", model.get("loading") === true);
        progress.classList.toggle("slls-lhm-show", working);

        const lakehouseName = nameOf("available_lakehouses", loadedLakehouseId);
        const workspaceName = nameOf("available_workspaces", loadedWorkspaceId);
        subtitle.innerHTML = "";
        if (hasContents && lakehouseName) {
            const b = document.createElement("b"); b.textContent = lakehouseName;
            const sep = document.createElement("span"); sep.className = "sl-sep"; sep.textContent = "·";
            subtitle.append(b, sep, document.createTextNode(workspaceName));
            if (model.get("schema_enabled") === true) {
                const sep2 = document.createElement("span"); sep2.className = "sl-sep"; sep2.textContent = "·";
                subtitle.append(sep2, document.createTextNode("Schema-enabled"));
            }
        }

        const summary = model.get("summary") || {};
        for (const key of Object.keys(cardValues)) {
            const value = summary[key];
            cardValues[key].textContent = value === undefined || value === null || value === "" ? "—" : String(value);
        }

        const error = model.get("error_message") || "";
        const status = model.get("status_message") || "";
        noteText.textContent = error || status;
        noteIcon.innerHTML = error ? `__ICON_ALERT__` : `__ICON_CHECK__`;
        noteIcon.style.display = (error || status) ? "" : "none";
        note.classList.toggle("slls-lhm-show", Boolean(error || status));
        note.classList.toggle("slls-lhm-bad", Boolean(error));
    }

    openBtn.addEventListener("click", () => {
        if (!model.get("selected_lakehouse_id")) return;
        pickerOpen = false; renderState();
        act("open", { workspace: model.get("selected_workspace_id"), lakehouse: model.get("selected_lakehouse_id") });
    });
    closePicker.addEventListener("click", () => { pickerOpen = false; renderState(); });
    changeBtn.addEventListener("click", () => { pickerOpen = true; renderState(); });
    reloadBtn.addEventListener("click", () => act("reload", {}));
    backdrop.addEventListener("click", (event) => {
        if (event.target === backdrop && backdrop.classList.contains("slls-lhm-modal")) { pickerOpen = false; renderState(); }
    });
    root.addEventListener("keydown", (event) => {
        if (event.key !== "Escape") return;
        if (confirmOverlay.style.display !== "none") closeConfirm();
        else if (overlay.style.display !== "none") closeDialog();
        else if (backdrop.classList.contains("slls-lhm-modal")) { pickerOpen = false; renderState(); }
    });
    search.addEventListener("input", renderRows);

    for (const name of ["available_workspaces", "available_lakehouses", "selected_workspace_id",
        "selected_lakehouse_id", "picker_loading", "loading", "busy", "summary",
        "schema_enabled", "status_message", "error_message", "tree"]) {
        model.on("change:" + name, () => { renderState(); renderRows(); });
    }
    // A different lakehouse was loaded: selections made in the previous one
    // must never carry over to it.
    model.on("change:loaded_lakehouse_id", () => {
        selected.clear();
        expanded.clear(); expanded.add("Tables"); expanded.add("Files");
        renderState(); renderRows();
    });
    model.on("change:objects", () => {
        if ((model.get("objects") || []).length) pickerOpen = false;
        renderState(); renderRows();
    });

    renderState(); renderRows();
}
export default { render };
""".replace("__ICON_LAKEHOUSE__", _UI_ICONS["warehouse"])
    .replace("__ICON_SWAP__", _UI_ICONS["swap"])
    .replace("__ICON_REFRESH__", _UI_ICONS["refresh"])
    .replace("__ICON_SUN__", _UI_ICONS["sun"])
    .replace("__ICON_MOON__", _UI_ICONS["moon"])
    .replace("__ICON_FS_EXIT__", _UI_ICONS["fullscreen_exit"])
    .replace("__ICON_FS__", _UI_ICONS["fullscreen"])
    .replace("__ICON_CLOSE__", _UI_ICONS["close"])
    .replace("__ICON_TABLE__", _UI_ICONS["table"])
    .replace("__ICON_TREE__", _UI_ICONS["list_tree"])
    .replace("__ICON_CARET__", _UI_ICONS["chevron_right"])
    .replace("__ICON_SCHEMA__", _UI_ICONS["database"])
    .replace("__ICON_FOLDER__", _UI_ICONS["folder"])
    .replace("__ICON_FILE__", _UI_ICONS["file"])
    .replace("__ICON_TRASH__", _UI_ICONS["trash"])
    .replace("__ICON_LINK__", _UI_ICONS["link"])
    .replace("__ICON_UNDO__", _UI_ICONS["undo"])
    .replace("__ICON_WRENCH__", _UI_ICONS["wrench"])
    .replace("__ICON_ALERT__", _UI_ICONS["alert"])
    .replace("__ICON_CHECK__", _UI_ICONS["check_circle"])
    .replace("__ATTRIBUTION__", _ui_render_attribution_html())
)


def _display_rows(objects: List[dict]) -> List[dict]:
    """The object rows as sent to the frontend, with display labels resolved."""

    rows = []
    for row in objects:
        display = dict(row)
        display["size_label"] = _format_bytes(row.get("size"))
        display["modified_label"] = str(row.get("modified") or "")[:19].replace(
            "T", " "
        )
        rows.append(display)
    return rows


def _action_paths(data: dict) -> List[str]:
    """The object paths an action applies to (one row, or a tree selection)."""

    paths = [str(path).strip("/") for path in (data.get("paths") or [])]
    single = str(data.get("path") or "").strip("/")
    if single and single not in paths:
        paths.append(single)
    return [path for path in paths if path]


def _is_safe_object_path(path: str) -> bool:
    """Whether a path is confined beneath the lakehouse's Tables or Files.

    Paths come from the browser, so any form which could escape the lakehouse
    once appended to its ABFSS URI (``..`` segments, percent-encoding,
    backslashes, query strings, fragments, control characters) is refused.
    """

    if not isinstance(path, str) or not path:
        return False
    if any(ch in path for ch in "\\?#%") or any(ord(ch) < 32 for ch in path):
        return False
    parts = path.split("/")
    if len(parts) < 2 or parts[0] not in _CONTAINERS:
        return False
    return all(part not in ("", ".", "..") for part in parts)


def _actionable_paths(objects: List[dict], tree: List[dict]) -> set:
    """The paths of the loaded objects which recover / delete may target.

    Containers and schemas are groupings and the "+N more" nodes are not
    objects, so none of them can be targeted.
    """

    allowed = {str(row.get("path") or "") for row in objects}
    groupings = set()

    def _walk(nodes: List[dict]):
        for node in nodes:
            path = str(node.get("path") or "")
            if node.get("type") in ("Container", "Schema", "More"):
                groupings.add(path)
            else:
                allowed.add(path)
            _walk(node.get("children") or [])

    _walk(tree)
    return {path for path in allowed - groupings if _is_safe_object_path(path)}


def _authorized_paths(paths: List[str], allowed: set) -> Tuple[List[str], List[str]]:
    """Split requested paths into those which may be acted on and the rest."""

    accepted, rejected = [], []
    for path in paths:
        if _is_safe_object_path(path) and path in allowed:
            accepted.append(path)
        else:
            rejected.append(path)
    return accepted, rejected


@log
def lakehouse_manager(
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    dark_mode: bool = False,
):
    """
    Displays an interactive manager for the contents of a lakehouse.

    Browses the lakehouse's tables, folders and files, flags OneLake shortcuts
    and `soft-deleted <https://learn.microsoft.com/fabric/onelake/onelake-disaster-recovery#soft-delete-for-onelake-files>`_
    objects, recovers those deleted objects, and runs table maintenance
    (optimize, V-Order, vacuum) on a table.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook,
        or opens the tool on its lakehouse picker.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    dark_mode : bool, default=False
        If True, renders the widget with a dark color theme.
    """

    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "The Lakehouse Manager requires the 'anywidget' package. "
            "Install it with: pip install anywidget"
        ) from e

    from IPython.display import display
    from sempy_labs._helper_functions import (
        resolve_lakehouse_name_and_id,
        resolve_workspace_name_and_id,
    )

    workspace_id = ""
    workspace_name = ""
    try:
        workspace_name, resolved_workspace_id = resolve_workspace_name_and_id(workspace)
        workspace_id = str(resolved_workspace_id)
    except Exception:
        pass

    lakehouse_id = ""
    if workspace_id:
        try:
            _, resolved_lakehouse_id = resolve_lakehouse_name_and_id(
                lakehouse=lakehouse, workspace=workspace_id
            )
            lakehouse_id = str(resolved_lakehouse_id)
        except Exception:
            lakehouse_id = ""

    # Seeded as initial widget state: a picker list assigned after display()
    # races the comm handshake and never reaches a Fabric PySpark notebook.
    initial_workspaces = _ui_list_picker_workspaces(workspace_id, workspace_name)
    initial_lakehouses = (
        _ui_list_picker_lakehouses(workspace_id) if workspace_id else []
    )

    class LakehouseManagerWidget(anywidget.AnyWidget):
        _esm = _LHM_JS
        _css = _LHM_CSS

        available_workspaces = traitlets.List([]).tag(sync=True)
        available_lakehouses = traitlets.List([]).tag(sync=True)
        selected_workspace_id = traitlets.Unicode("").tag(sync=True)
        selected_lakehouse_id = traitlets.Unicode("").tag(sync=True)
        # The lakehouse whose contents are shown (display only; see `loaded`).
        loaded_workspace_id = traitlets.Unicode("").tag(sync=True)
        loaded_lakehouse_id = traitlets.Unicode("").tag(sync=True)
        objects = traitlets.List([]).tag(sync=True)
        tree = traitlets.List([]).tag(sync=True)
        summary = traitlets.Dict({}).tag(sync=True)
        schema_enabled = traitlets.Bool(False).tag(sync=True)
        picker_loading = traitlets.Bool(False).tag(sync=True)
        loading = traitlets.Bool(False).tag(sync=True)
        busy = traitlets.Bool(False).tag(sync=True)
        status_message = traitlets.Unicode("").tag(sync=True)
        error_message = traitlets.Unicode("").tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)
        pending_action = traitlets.Dict({}).tag(sync=True)
        action_trigger = traitlets.Int(0).tag(sync=True)

    widget = LakehouseManagerWidget(
        available_workspaces=initial_workspaces,
        available_lakehouses=initial_lakehouses,
        selected_workspace_id=workspace_id,
        selected_lakehouse_id=lakehouse_id,
        dark_mode=bool(dark_mode),
    )

    def _load_lakehouses(data: dict):
        widget.picker_loading = True
        widget.error_message = ""
        try:
            widget.available_lakehouses = _ui_list_picker_lakehouses(
                widget.selected_workspace_id
            )
        except Exception as e:
            widget.error_message = f"Could not list the lakehouses: {e}"
        finally:
            widget.picker_loading = False

    # The lakehouse whose contents are shown. Every action targets it, never
    # the picker selection, which can change without a Connect (the picker can
    # be closed after choosing another lakehouse). Held on the kernel only, so
    # the browser cannot redirect an action to another lakehouse.
    loaded: Dict[str, Any] = {"workspace_id": "", "lakehouse_id": "", "paths": set()}

    def _read_lakehouse(workspace_id: str, lakehouse_id: str) -> bool:
        """Load a lakehouse's contents into the widget; True when it succeeded."""

        widget.loading = True
        try:
            objects, tree, schema_enabled = _lakehouse_objects(
                workspace_id, lakehouse_id
            )
        except Exception as e:
            message = f"Could not read the lakehouse: {e}"
            widget.error_message = "; ".join(
                part for part in (widget.error_message, message) if part
            )
            return False
        finally:
            widget.loading = False
        loaded.update(
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            paths=_actionable_paths(objects, tree),
        )
        widget.schema_enabled = schema_enabled
        widget.summary = _summarize_objects(objects)
        widget.objects = _display_rows(objects)
        widget.tree = tree
        widget.loaded_workspace_id = workspace_id
        widget.loaded_lakehouse_id = lakehouse_id
        return True

    def _load_objects(data: dict):
        workspace_id = str(data.get("workspace") or widget.selected_workspace_id)
        lakehouse_id = str(data.get("lakehouse") or widget.selected_lakehouse_id)
        if not (workspace_id and lakehouse_id):
            return
        widget.error_message = ""
        widget.status_message = ""
        _read_lakehouse(workspace_id, lakehouse_id)

    def _reload(data: dict):
        if not loaded["lakehouse_id"]:
            return
        widget.error_message = ""
        widget.status_message = ""
        _read_lakehouse(loaded["workspace_id"], loaded["lakehouse_id"])

    def _refresh_after_action():
        """Reload the contents, keeping the action's outcome messages."""

        if loaded["lakehouse_id"]:
            _read_lakehouse(loaded["workspace_id"], loaded["lakehouse_id"])

    def _target_paths(data: dict, verb: str) -> List[str]:
        """The requested paths which belong to the loaded lakehouse."""

        paths, rejected = _authorized_paths(_action_paths(data), loaded["paths"])
        if rejected:
            widget.error_message = (
                f"Could not {verb} " + ", ".join(rejected) + ": not an object "
                "of the loaded lakehouse."
            )
        return paths

    def _recover(data: dict):
        widget.error_message = ""
        widget.status_message = ""
        paths = _target_paths(data, "recover")
        if not paths:
            return
        widget.busy = True
        failures = []
        try:
            from sempy_labs.lakehouse._blobs import recover_lakehouse_object

            for path in paths:
                try:
                    recover_lakehouse_object(
                        file_path=path,
                        lakehouse=loaded["lakehouse_id"],
                        workspace=loaded["workspace_id"],
                    )
                except Exception as e:
                    failures.append(f"{path}: {e}")
            widget.status_message = (
                f"Recovery was requested for {len(paths) - len(failures)} object(s)."
            )
        except Exception as e:
            failures.append(f"the selected objects: {e}")
        finally:
            widget.busy = False
        if failures:
            widget.error_message = "; ".join(
                part
                for part in (
                    widget.error_message,
                    "Could not recover " + "; ".join(failures),
                )
                if part
            )
        _refresh_after_action()

    def _delete(data: dict):
        widget.error_message = ""
        widget.status_message = ""
        paths = _target_paths(data, "delete")
        if not paths:
            return
        widget.busy = True
        failures = []
        deleted = 0
        try:
            import notebookutils
            from sempy_labs._helper_functions import create_abfss_path_from_path

            for path in paths:
                try:
                    removed = notebookutils.fs.rm(
                        create_abfss_path_from_path(
                            loaded["lakehouse_id"],
                            loaded["workspace_id"],
                            path,
                        ),
                        True,
                    )
                except Exception as e:
                    failures.append(f"{path}: {e}")
                    continue
                # fs.rm reports some failures by returning False, not raising.
                if removed is False:
                    failures.append(f"{path}: the object was not deleted")
                else:
                    deleted += 1
            widget.status_message = f"Deleted {deleted} object(s)."
        except Exception as e:
            failures.append(f"the selected objects: {e}")
        finally:
            widget.busy = False
        if failures:
            widget.error_message = "; ".join(
                part
                for part in (
                    widget.error_message,
                    "Could not delete " + "; ".join(failures),
                )
                if part
            )
        _refresh_after_action()

    def _maintenance(data: dict):
        table = str(data.get("table") or "")
        if not table or not loaded["lakehouse_id"]:
            return
        schema = str(data.get("schema") or "")
        # The table comes from the browser: it must be a table of the loaded
        # lakehouse, consistent with the path the action was requested for.
        table_path = "/".join(part for part in ("Tables", schema, table) if part)
        if (
            str(data.get("path") or "").strip("/") != table_path
            or not _authorized_paths([table_path], loaded["paths"])[0]
        ):
            widget.error_message = (
                f"Could not run table maintenance on '{table_path}': not a "
                "table of the loaded lakehouse."
            )
            widget.status_message = ""
            return
        optimize = bool(data.get("optimize"))
        v_order = bool(data.get("v_order"))
        vacuum = bool(data.get("vacuum"))
        if not (optimize or v_order or vacuum):
            widget.error_message = "Select at least one maintenance operation."
            return
        retention = str(data.get("retention_period") or "").strip()
        widget.busy = True
        widget.error_message = ""
        widget.status_message = ""
        try:
            from sempy_labs.lakehouse._lakehouse import run_table_maintenance

            run_table_maintenance(
                table_name=table,
                optimize=optimize,
                v_order=v_order,
                vacuum=vacuum,
                retention_period=retention if (vacuum and retention) else None,
                schema=schema or None,
                lakehouse=loaded["lakehouse_id"],
                workspace=loaded["workspace_id"],
            )
            widget.status_message = f"Table maintenance finished for '{table}'."
        except Exception as e:
            widget.error_message = f"Table maintenance failed for '{table}': {e}"
        finally:
            widget.busy = False
        _refresh_after_action()

    handlers = {
        "workspace": _load_lakehouses,
        "open": _load_objects,
        "reload": _reload,
        "recover": _recover,
        "delete": _delete,
        "maintenance": _maintenance,
    }

    def _on_action(change):
        if change["new"] == change["old"]:
            return
        data = dict(widget.pending_action or {})
        handler = handlers.get(str(data.get("action") or ""))
        if handler is None:
            return
        _ui_run_widget_task(handler, (data,))

    widget.observe(_on_action, names="action_trigger")
    display(widget)
