# Report Prototype Generator — standalone module.
# Generates an SVG diagram and Excalidraw JSON of all report pages with optional screenshots.

import base64
import json
import os
import uuid
from typing import Optional
from uuid import UUID


def generate_report_prototype(
    report: str,
    workspace: Optional[str | UUID] = None,
    screenshots: bool = False,
    include_hidden: bool = False,
    cols: int = 4,
    thumb_width: int = 480,
    thumb_height: int = 270,
    on_progress=None,
) -> dict:
    """
    Generates a visual prototype of a Power BI report as SVG + Excalidraw.

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    screenshots : bool, default=False
        If True, exports each page as PNG via the Export API and embeds as images.
    include_hidden : bool, default=False
        If True, includes hidden pages in the diagram.
    cols : int, default=4
        Number of columns in the page grid layout.
    thumb_width : int, default=480
        Width of each page thumbnail in the diagram.
    thumb_height : int, default=270
        Height of each page thumbnail in the diagram.

    Returns
    -------
    dict
        A dictionary with keys:
        - "svg": str — the SVG diagram as a string
        - "excalidraw": str — the Excalidraw JSON as a string
        - "pages": list — page metadata
        - "screenshots": int — number of screenshots captured
        - "errors": list — export error messages
    """
    from sempy_labs.report import connect_report

    # Layout constants
    pad_x = 40
    pad_y = 60
    header_h = 30
    footer_h = 25

    # 1. Load page metadata + navigation edges
    pages_data = []
    nav_edges = []  # (source_page_name, target_page_name)
    with connect_report(report=report, readonly=True, workspace=workspace) as rw:
        try:
            pages_df = rw.list_pages()
        except Exception:
            # Fallback for PBIRLegacy or other issues — parse pages directly from parts
            import pandas as pd
            pages_df = pd.DataFrame(columns=["Page Name", "Page Display Name", "Width", "Height",
                                             "Hidden", "Drillthrough Target Page", "Visual Count", "Data Visual Count"])
            _fallback_rows = []
            for part in rw._report_definition.get("parts", []):
                path = part.get("path", "")
                if not path.endswith("/page.json"):
                    continue
                payload = part.get("payload")
                if not isinstance(payload, dict):
                    continue
                pg_name = payload.get("name", "")
                is_hidden = payload.get("visibility", "") == "HiddenInViewMode"
                # Check drillthrough from filterConfig
                is_dt = False
                for f in payload.get("filterConfig", {}).get("filters", []):
                    if f.get("howCreated") == "Drillthrough":
                        is_dt = True
                        break
                # Count visuals for this page
                page_prefix = path[:-9]  # strip "page.json"
                v_count = sum(1 for p in rw._report_definition.get("parts", [])
                              if p.get("path", "").startswith(page_prefix) and p.get("path", "").endswith("/visual.json"))
                _fallback_rows.append({
                    "Page Name": pg_name,
                    "Page Display Name": payload.get("displayName", pg_name),
                    "Width": payload.get("width", 1280),
                    "Height": payload.get("height", 720),
                    "Hidden": is_hidden,
                    "Drillthrough Target Page": is_dt,
                    "Visual Count": v_count,
                    "Data Visual Count": 0,
                })
            if _fallback_rows:
                pages_df = pd.DataFrame(_fallback_rows)

        for _, row in pages_df.iterrows():
            pages_data.append({
                "name": str(row.get("Page Name", "")),
                "display_name": str(row.get("Page Display Name", "")),
                "hidden": bool(row.get("Hidden", False)),
                "width": int(row.get("Width", 1280)),
                "height": int(row.get("Height", 720)),
                "drillthrough": bool(row.get("Drillthrough Target Page", False)),
                "visual_count": int(row.get("Visual Count", 0)),
                "data_visual_count": int(row.get("Data Visual Count", 0)),
            })

        # Extract page navigation from visualLink on all visuals
        # (cf. https://actionablereporting.com/2026/03/09/power-bi-report-prototyping-with-ai/)
        page_names = {p["name"] for p in pages_data}
        page_name_lookup = {}  # folder_id -> page_name (in case they differ)
        for part in rw._report_definition.get("parts", []):
            path = part.get("path", "")
            if path.endswith("/page.json"):
                payload = part.get("payload")
                if isinstance(payload, dict):
                    pg_folder = path.replace("\\", "/").split("/")
                    try:
                        pi = pg_folder.index("pages")
                        folder_id = pg_folder[pi + 1]
                        pg_name = payload.get("name", folder_id)
                        page_name_lookup[folder_id] = pg_name
                    except (ValueError, IndexError):
                        pass

        drillthrough_pages = {p["name"] for p in pages_data if p.get("drillthrough")}

        # Extract page navigation from button visuals only (actionButton, image)
        # Excludes Back buttons. cf. blog post referenced above.
        _BUTTON_TYPES = {"actionButton", "image"}

        for part in rw._report_definition.get("parts", []):
            path = part.get("path", "")
            if not path.endswith("/visual.json"):
                continue
            payload = part.get("payload")
            if not isinstance(payload, dict):
                continue
            # Only consider button-like visuals
            visual_type = payload.get("visual", {}).get("visualType", "")
            if visual_type not in _BUTTON_TYPES:
                continue
            # Derive source page from path: .../pages/<page_id>/visuals/<vid>/visual.json
            segments = path.replace("\\", "/").split("/")
            try:
                pi = segments.index("pages")
                folder_id = segments[pi + 1]
            except (ValueError, IndexError):
                continue
            source_page = page_name_lookup.get(folder_id, folder_id)
            if source_page not in page_names:
                continue
            try:
                vis_links = (
                    payload.get("visual", {})
                    .get("visualContainerObjects", {})
                    .get("visualLink", [])
                )
                for link in vis_links:
                    props = link.get("properties", {})
                    show_val = (
                        props.get("show", {})
                        .get("expr", {})
                        .get("Literal", {})
                        .get("Value", "false")
                    )
                    if show_val != "true":
                        continue
                    action_type = (
                        props.get("type", {})
                        .get("expr", {})
                        .get("Literal", {})
                        .get("Value", "")
                        .strip("'")
                    )
                    # Skip Back buttons
                    if action_type == "Back":
                        continue
                    target_page = (
                        props.get("navigationSection", {})
                        .get("expr", {})
                        .get("Literal", {})
                        .get("Value", "")
                        .strip("'")
                    )
                    # Resolve target page name if it's a folder ID
                    target_page = page_name_lookup.get(target_page, target_page)
                    if action_type == "PageNavigation" and target_page and target_page in page_names:
                        nav_edges.append((source_page, target_page))
            except Exception:
                continue  # skip visuals with unexpected payload structure

    if not pages_data:
        return {"svg": "", "excalidraw": "", "pages": [], "screenshots": 0, "errors": ["No pages found"]}

    # 2. Export screenshots (optional, parallel)
    page_images = {}
    export_errors = []
    if screenshots:
        from sempy_labs.report import export_report
        import sempy.fabric as fabric
        import sys
        import io as _io
        import threading

        # Pre-resolve report ID once (Win 1: avoids N×list_items calls)
        _resolved_report_id = None
        try:
            dfI = fabric.list_items(workspace=workspace)
            dfI_filt = dfI[
                (dfI["Type"] == "Report") & (dfI["Display Name"] == report)
            ]
            if not dfI_filt.empty:
                _resolved_report_id = dfI_filt["Id"].iloc[0]
        except Exception:
            pass

        target_pages = [(idx, pg) for idx, pg in enumerate(pages_data) if include_hidden or not pg["hidden"]]
        total_pages = len(target_pages)
        _lock = threading.Lock()
        _done_count = [0]

        def _export_one(idx, pg):
            # Suppress all output inside each thread
            import sys as _tsys
            import io as _tio
            _t_old_stdout = _tsys.stdout
            _tsys.stdout = _tio.StringIO()
            try:
                import IPython.display as _tipd
                _tipd_orig = _tipd.display
                _tipd.display = lambda *a, **kw: None
            except Exception:
                _tipd = None
                _tipd_orig = None
            try:
                # Win 3: get PNG bytes directly, skip file I/O
                png_bytes = export_report(
                    report=report,
                    export_format="PNG",
                    file_name=f"_prototype_{idx:02d}",
                    page_name=pg["name"],
                    workspace=workspace,
                    _report_id=_resolved_report_id,
                    _return_bytes=True,
                )
                if png_bytes:
                    with _lock:
                        page_images[pg["name"]] = base64.b64encode(png_bytes).decode("ascii")
                else:
                    with _lock:
                        export_errors.append(f"'{pg['display_name']}': empty response")
            except Exception as e:
                with _lock:
                    export_errors.append(f"'{pg['display_name']}': {str(e)[:200]}")
            finally:
                _tsys.stdout = _t_old_stdout
                if _tipd and _tipd_orig:
                    _tipd.display = _tipd_orig
                with _lock:
                    _done_count[0] += 1
                    done = _done_count[0]
                if on_progress:
                    try:
                        on_progress(done, total_pages, pg["display_name"])
                    except Exception:
                        pass

        # Redirect stdout AND suppress IPython.display to prevent notebook output overflow
        _real_stdout = sys.stdout
        sys.stdout = _io.StringIO()

        # Monkey-patch IPython display to a no-op during exports
        _ipd = None
        _ipd_orig = None
        _idf = None
        _idf_orig = None
        try:
            import IPython.display as _ipd_mod
            _ipd = _ipd_mod
            _ipd_orig = _ipd_mod.display
            _ipd_mod.display = lambda *a, **kw: None
        except Exception:
            pass
        try:
            import IPython.core.display_functions as _idf_mod
            _idf = _idf_mod
            _idf_orig = _idf_mod.display
            _idf_mod.display = lambda *a, **kw: None
        except Exception:
            pass

        try:
            if on_progress:
                on_progress(0, total_pages, "starting exports...")
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=min(5, total_pages)) as pool:
                pool.map(lambda args: _export_one(*args), target_pages)
        finally:
            sys.stdout = _real_stdout
            if _ipd and _ipd_orig:
                _ipd.display = _ipd_orig
            if _idf and _idf_orig:
                _idf.display = _idf_orig

    # Deduplicate navigation edges
    nav_edges = list(set(nav_edges))

    # 3. Build diagram
    svg_str, excalidraw_str = _build_diagram(
        pages_data, page_images, include_hidden,
        cols, thumb_width, thumb_height, pad_x, pad_y, header_h, footer_h,
        nav_edges,
    )

    print(f"\u2713 Prototype: {len(pages_data)} pages, {len(page_images)} screenshots, {len(nav_edges)} nav links.")
    dt_count = sum(1 for p in pages_data if p.get("drillthrough"))
    if dt_count:
        print(f"  \u2192 {dt_count} drillthrough target page(s)")
    if export_errors:
        for err in export_errors[:3]:
            print(f"  \u26a0 {err}")

    return {
        "svg": svg_str,
        "excalidraw": excalidraw_str,
        "pages": pages_data,
        "screenshots": len(page_images),
        "errors": export_errors,
        "nav_edges": nav_edges,
    }


def save_report_prototype(
    report: str,
    workspace: Optional[str | UUID] = None,
    screenshots: bool = False,
    include_hidden: bool = False,
    cols: int = 4,
):
    """
    Generates and saves a report prototype to the lakehouse.

    Saves both .excalidraw and .svg files to lakehouse Files/.

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    screenshots : bool, default=False
        If True, exports each page as PNG via the Export API.
    include_hidden : bool, default=False
        If True, includes hidden pages.
    cols : int, default=4
        Number of columns in the grid layout.
    """
    result = generate_report_prototype(
        report=report, workspace=workspace,
        screenshots=screenshots, include_hidden=include_hidden, cols=cols,
    )

    safe_name = report.replace(" ", "_").replace("/", "_")
    from sempy_labs._helper_functions import _mount
    local_path = _mount()

    exc_path = f"{local_path}/Files/{safe_name}_prototype.excalidraw"
    with open(exc_path, "w", encoding="utf-8") as f:
        f.write(result["excalidraw"])
    print(f"\u2713 Saved {safe_name}_prototype.excalidraw")

    svg_path = f"{local_path}/Files/{safe_name}_prototype.svg"
    with open(svg_path, "w", encoding="utf-8") as f:
        f.write(result["svg"])
    print(f"\u2713 Saved {safe_name}_prototype.svg")

    return result


def _build_diagram(pages, images, include_hidden, cols, thumb_w, thumb_h, pad_x, pad_y, header_h, footer_h, nav_edges=None):
    """Build SVG + Excalidraw JSON from page metadata and images.

    Layout: pages are grouped into columns based on navigation edges (visualLink).
    Each connected component (BFS from root pages) becomes one column.
    Pages are stacked vertically within each column in BFS order.
    Standalone pages (no edges) go into a final column.
    """
    # --- Constants matching reference Excalidraw layout ---
    IMG_W = 960       # screenshot width (50% of 1920)
    IMG_H = 472       # screenshot height (50% of 1080 minus some)
    COL_W = 1040      # column spacing (IMG_W + 80px gap)
    TITLE_FS = 20     # page title font size
    DESC_FS = 14      # description font size
    CAT_FS = 36       # category header font size
    SLOT_GAP = 574    # vertical spacing per page slot
    LABEL_Y_OFF = 60  # title y offset from category top
    DESC_Y_OFF = 90   # description y offset from category top
    IMG_Y_OFF = 122   # image y offset from category top

    visible = [p for p in pages if include_hidden or not p["hidden"]]
    if not visible:
        return "", json.dumps({"type": "excalidraw", "version": 2, "source": "pbi_fixer", "elements": [], "files": {}})

    page_map = {p["name"]: p for p in visible}

    # --- Build navigation graph ---
    # Only use forward navigation edges (button→target), back buttons are already excluded.
    # Remove "back to overview" edges: if a page links back to the first page,
    # that's navigational convenience, not a hierarchy edge.
    children_map = {}
    has_parent = set()
    edge_set = set()

    # The first visible page is the landing/overview page
    first_page = visible[0]["name"] if visible else None

    for src, tgt in (nav_edges or []):
        if src in page_map and tgt in page_map:
            # Skip edges pointing back to the first page (overview) from child pages
            # Only keep "first_page -> X" edges, not "X -> first_page"
            if tgt == first_page and src != first_page:
                continue
            key = (src, tgt)
            if key not in edge_set:
                edge_set.add(key)
                children_map.setdefault(src, []).append(tgt)
                has_parent.add(tgt)

    # Root is the first page if it has outgoing edges, otherwise find roots by in-degree
    roots = []
    if first_page and first_page in children_map:
        roots = [first_page]
    else:
        roots = [p["name"] for p in visible
                 if p["name"] not in has_parent and p["name"] in children_map]
    if not roots and children_map:
        roots = [max(children_map, key=lambda k: len(children_map[k]))]

    # BFS to build branches (one per root / connected component)
    visited = set()
    branches = []  # list of [(page_name, level), ...]
    for root in roots:
        if root in visited:
            continue
        branch = []
        queue = [(root, 0)]
        visited.add(root)
        i = 0
        while i < len(queue):
            node, level = queue[i]
            i += 1
            branch.append((node, level))
            for child in children_map.get(node, []):
                if child not in visited:
                    visited.add(child)
                    queue.append((child, level + 1))
        branches.append(branch)

    # Standalone pages (no nav edges)
    standalone = [p["name"] for p in visible if p["name"] not in visited]
    if standalone:
        branches.append([(name, 0) for name in standalone])

    # --- Build Excalidraw elements ---
    exc_elements = []
    exc_files = {}

    # Level labels for display
    LEVEL_LABELS = ["L1", "L2", "L3", "L4", "L5"]
    LEVEL_COLORS = {
        "L1": "#0d9488",  # Teal
        "L2": "#2563eb",  # Blue
        "L3": "#7c3aed",  # Purple
        "L4": "#c2410c",  # Orange
        "L5": "#dc2626",  # Red
        "TT": "#9ca3af",  # Gray (tooltip)
        "H": "#9ca3af",   # Gray (hidden)
    }

    col_x = 0
    page_positions = {}  # page_name -> (x, y, col_idx, slot_idx)

    for branch_idx, branch in enumerate(branches):
        if not branch:
            continue

        # Category header: use display name of first page in branch
        first_pg = page_map[branch[0][0]]
        if len(branch) == 1 and branch[0][0] in standalone:
            cat_label = first_pg["display_name"]
        else:
            cat_label = first_pg["display_name"]

        exc_elements.append({
            "type": "text", "id": str(uuid.uuid4()),
            "x": col_x, "y": 0, "width": IMG_W, "height": CAT_FS + 10,
            "text": cat_label, "fontSize": CAT_FS, "fontFamily": 1,
            "textAlign": "left", "verticalAlign": "top",
            "strokeColor": "#1e293b", "roughness": 0, "rawText": cat_label,
        })

        for slot_idx, (page_name, level) in enumerate(branch):
            pg = page_map[page_name]
            x = col_x
            y_base = slot_idx * SLOT_GAP

            # Determine level label
            if pg["hidden"]:
                lvl_label = "H"
            elif pg.get("drillthrough"):
                # Drillthrough pages keep their BFS level
                lvl_label = LEVEL_LABELS[min(level, len(LEVEL_LABELS) - 1)]
            else:
                lvl_label = LEVEL_LABELS[min(level, len(LEVEL_LABELS) - 1)]

            lvl_color = LEVEL_COLORS.get(lvl_label, "#333")

            # Title: [L#] Page Display Name
            title_text = f"[{lvl_label}] {pg['display_name']}"
            exc_elements.append({
                "type": "text", "id": str(uuid.uuid4()),
                "x": x, "y": y_base + LABEL_Y_OFF, "width": IMG_W, "height": TITLE_FS + 4,
                "text": title_text, "fontSize": TITLE_FS, "fontFamily": 1,
                "textAlign": "left", "verticalAlign": "top",
                "strokeColor": lvl_color, "roughness": 0, "rawText": title_text,
            })

            # Description line
            desc_parts = []
            desc_parts.append(f"{pg['visual_count']}v / {pg['data_visual_count']}d")
            desc_parts.append(f"{pg['width']}\u00d7{pg['height']}")
            if pg.get("drillthrough"):
                desc_parts.append("Drillthrough")
            if pg["hidden"]:
                desc_parts.append("Hidden")
            desc_text = " \u2022 ".join(desc_parts)
            exc_elements.append({
                "type": "text", "id": str(uuid.uuid4()),
                "x": x, "y": y_base + DESC_Y_OFF, "width": IMG_W, "height": DESC_FS + 4,
                "text": desc_text, "fontSize": DESC_FS, "fontFamily": 1,
                "textAlign": "left", "verticalAlign": "top",
                "strokeColor": "#6b7280", "roughness": 0, "rawText": desc_text,
            })

            # Screenshot image or placeholder rectangle
            img_x = x
            img_y = y_base + IMG_Y_OFF
            if pg["name"] in images:
                b64 = images[pg["name"]]
                file_id = str(uuid.uuid4())
                exc_files[file_id] = {"mimeType": "image/png", "id": file_id, "dataURL": f"data:image/png;base64,{b64}"}
                exc_elements.append({
                    "type": "image", "id": str(uuid.uuid4()),
                    "x": img_x, "y": img_y, "width": IMG_W, "height": IMG_H,
                    "fileId": file_id, "status": "saved", "scale": [1, 1],
                })
            else:
                # Placeholder box
                exc_elements.append({
                    "type": "rectangle", "id": str(uuid.uuid4()),
                    "x": img_x, "y": img_y, "width": IMG_W, "height": IMG_H,
                    "backgroundColor": "#f9fafb", "strokeColor": "#e5e7eb",
                    "fillStyle": "solid", "strokeWidth": 1, "roughness": 0,
                    "roundness": {"type": 3},
                })
                exc_elements.append({
                    "type": "text", "id": str(uuid.uuid4()),
                    "x": img_x + IMG_W // 2 - 80, "y": img_y + IMG_H // 2 - 10,
                    "width": 160, "height": 20,
                    "text": "(no screenshot)", "fontSize": 14, "fontFamily": 1,
                    "textAlign": "center", "verticalAlign": "top",
                    "strokeColor": "#d1d5db", "roughness": 0, "rawText": "(no screenshot)",
                })

            page_positions[page_name] = (x, y_base, branch_idx, slot_idx)

        col_x += COL_W

    # --- Build SVG (simplified — only inline preview, Excalidraw is the primary output) ---
    n_cols = len(branches)
    max_slots = max((len(b) for b in branches), default=1)
    svg_w = max(n_cols * COL_W, 800)
    svg_h = max(max_slots * SLOT_GAP + IMG_Y_OFF + IMG_H + 40, 400)

    svg_parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" '
        f'width="{svg_w}" height="{svg_h}" viewBox="0 0 {svg_w} {svg_h}">',
        f'<rect width="{svg_w}" height="{svg_h}" fill="#ffffff"/>',
    ]
    font_family = "-apple-system,BlinkMacSystemFont,sans-serif"

    svg_col_x = 0
    for branch_idx, branch in enumerate(branches):
        for slot_idx, (page_name, level) in enumerate(branch):
            pg = page_map[page_name]
            x = svg_col_x
            y_base = slot_idx * SLOT_GAP

            lvl = LEVEL_LABELS[min(level, len(LEVEL_LABELS) - 1)]
            if pg["hidden"]:
                lvl = "H"
            lvl_color = LEVEL_COLORS.get(lvl, "#333")

            title = f"[{lvl}] {pg['display_name']}"
            svg_parts.append(f'<text x="{x}" y="{y_base + LABEL_Y_OFF + 16}" font-family="{font_family}" font-size="{TITLE_FS}" font-weight="600" fill="{lvl_color}">{title[:50]}</text>')

            if pg["name"] in images:
                b64 = images[pg["name"]]
                svg_parts.append(f'<image x="{x}" y="{y_base + IMG_Y_OFF}" width="{IMG_W}" height="{IMG_H}" href="data:image/png;base64,{b64}" preserveAspectRatio="xMidYMid meet"/>')
            else:
                svg_parts.append(f'<rect x="{x}" y="{y_base + IMG_Y_OFF}" width="{IMG_W}" height="{IMG_H}" rx="4" fill="#f9fafb" stroke="#e5e7eb" stroke-width="1"/>')
                svg_parts.append(f'<text x="{x + IMG_W // 2}" y="{y_base + IMG_Y_OFF + IMG_H // 2}" font-family="{font_family}" font-size="14" fill="#9ca3af" text-anchor="middle">{pg["display_name"]}</text>')

        svg_col_x += COL_W

    svg_parts.append('</svg>')

    # --- Add navigation map (box diagram) below screenshots ---
    nav_map_y = max_slots * SLOT_GAP + IMG_Y_OFF + IMG_H + 120
    _add_nav_map(exc_elements, branches, page_map, edge_set, nav_map_y)

    excalidraw_json = {
        "type": "excalidraw",
        "version": 2,
        "source": "pbi_fixer",
        "elements": exc_elements,
        "files": exc_files,
    }

    return "\n".join(svg_parts), json.dumps(excalidraw_json, indent=2)


def _add_nav_map(elements, branches, page_map, edge_set, start_y):
    """Add navigation map box diagrams below the screenshots.

    Each branch gets a BEFORE column with page hierarchy boxes + arrows.
    First column shows a TOTAL SUMMARY.
    Layout matches the reference Excalidraw: colored boxes (280×36) with
    50px indentation per level, arrows connecting parent→child.
    """
    if not branches:
        return

    LEVEL_COLORS_BOX = [
        {"bg": "#ccfbf1", "stroke": "#0d9488"},  # L1 Teal
        {"bg": "#dbeafe", "stroke": "#2563eb"},  # L2 Blue
        {"bg": "#ede9fe", "stroke": "#7c3aed"},  # L3 Purple
        {"bg": "#ffedd5", "stroke": "#c2410c"},  # L4 Orange
        {"bg": "#fee2e2", "stroke": "#dc2626"},  # L5 Red
    ]
    HIDDEN_BOX = {"bg": "#f3f4f6", "stroke": "#9ca3af"}

    BOX_W = 280
    BOX_H = 36
    INDENT = 50
    V_GAP = 16
    COL_W = 1040

    # --- TOTAL SUMMARY (column 0) ---
    total_pages = sum(len(b) for b in branches)
    max_level = 0
    for branch in branches:
        for _, lvl in branch:
            if lvl > max_level:
                max_level = lvl
    max_level_display = max_level + 1  # 0-based → 1-based

    elements.append({
        "type": "text", "id": str(uuid.uuid4()),
        "x": 0, "y": start_y, "width": 400, "height": 36,
        "text": "TOTAL SUMMARY", "fontSize": 28, "fontFamily": 1,
        "textAlign": "left", "verticalAlign": "top",
        "strokeColor": "#1e293b", "roughness": 0, "rawText": "TOTAL SUMMARY",
    })
    edge_pages = {s for s, _ in edge_set} | {t for _, t in edge_set}
    nav_groups = sum(1 for b in branches if b[0][0] in edge_pages)
    standalone_count = sum(1 for b in branches for n, l in b if n not in edge_pages)
    summary_lines = [
        (18, f"Pages: {total_pages}"),
        (18, f"Max depth: {max_level_display} level(s)"),
        (16, f"Navigation groups: {nav_groups}"),
        (16, f"Standalone pages: {standalone_count}"),
    ]
    sy = start_y + 40
    for fs, txt in summary_lines:
        elements.append({
            "type": "text", "id": str(uuid.uuid4()),
            "x": 0, "y": sy, "width": 500, "height": fs + 6,
            "text": txt, "fontSize": fs, "fontFamily": 1,
            "textAlign": "left", "verticalAlign": "top",
            "strokeColor": "#374151", "roughness": 0, "rawText": txt,
        })
        sy += fs + 10

    # --- Per-branch BEFORE columns ---
    for branch_idx, branch in enumerate(branches):
        if not branch:
            continue

        col_x = (branch_idx + 1) * COL_W  # +1 to skip summary column

        # Section header
        first_pg = page_map[branch[0][0]]
        n_pages = len(branch)
        max_branch_lvl = max(lvl for _, lvl in branch) + 1
        header = f"{first_pg['display_name']} ({n_pages} pages, {max_branch_lvl} levels)"
        elements.append({
            "type": "text", "id": str(uuid.uuid4()),
            "x": col_x, "y": start_y, "width": COL_W - 80, "height": 36,
            "text": header, "fontSize": 28, "fontFamily": 1,
            "textAlign": "left", "verticalAlign": "top",
            "strokeColor": "#1e293b", "roughness": 0, "rawText": header,
        })

        box_y = start_y + 52
        box_positions = {}

        for page_name, level in branch:
            pg = page_map[page_name]
            bx = col_x + level * INDENT

            if pg["hidden"]:
                color = HIDDEN_BOX
                lvl_label = "TT" if "tt" in pg["display_name"].lower() or "tooltip" in pg["display_name"].lower() else "H"
            else:
                color = LEVEL_COLORS_BOX[min(level, len(LEVEL_COLORS_BOX) - 1)]
                lvl_label = f"L{min(level + 1, 5)}"

            # Box
            elements.append({
                "type": "rectangle", "id": str(uuid.uuid4()),
                "x": bx, "y": box_y, "width": BOX_W, "height": BOX_H,
                "backgroundColor": color["bg"], "strokeColor": color["stroke"],
                "fillStyle": "solid", "strokeWidth": 2, "roughness": 0,
                "roundness": {"type": 3},
            })

            # Label
            label = f"[{lvl_label}] {pg['display_name']}"
            elements.append({
                "type": "text", "id": str(uuid.uuid4()),
                "x": bx + 10, "y": box_y + 8, "width": BOX_W - 20, "height": 20,
                "text": label, "fontSize": 14, "fontFamily": 1,
                "textAlign": "left", "verticalAlign": "top",
                "strokeColor": color["stroke"], "roughness": 0, "rawText": label,
            })

            box_positions[page_name] = (bx, box_y)
            box_y += BOX_H + V_GAP

        # Arrows within this branch
        branch_levels = {n: l for n, l in branch}
        for src, tgt in edge_set:
            if src not in box_positions or tgt not in box_positions:
                continue
            sx, sy = box_positions[src]
            tx, ty = box_positions[tgt]

            # Arrow from left edge of source box to top of target box
            ax = sx
            ay = sy + BOX_H
            dx = tx - ax
            dy = ty - ay

            if abs(dy) < 2:
                continue

            tgt_lvl = branch_levels.get(tgt, 0)
            arrow_color = LEVEL_COLORS_BOX[min(tgt_lvl, len(LEVEL_COLORS_BOX) - 1)]["stroke"]

            elements.append({
                "type": "arrow", "id": str(uuid.uuid4()),
                "x": ax, "y": ay,
                "width": max(abs(dx), 1), "height": max(abs(dy), 1),
                "strokeColor": arrow_color, "strokeWidth": 2,
                "strokeStyle": "solid", "roughness": 0, "opacity": 100,
                "roundness": {"type": 2},
                "points": [[0, 0], [dx, dy]],
                "startBinding": None, "endBinding": None,
                "startArrowhead": None, "endArrowhead": "arrow",
            })
