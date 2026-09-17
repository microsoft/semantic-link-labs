# Report Explorer tab for PBI Fixer.
# Provides a tree view of report pages and visuals with iframe preview,
# properties, and fixer action dropdown.

import ipywidgets as widgets
import time

from sempy_labs._ui_components import (
    FONT_FAMILY,
    BORDER_COLOR,
    GRAY_COLOR,
    ICON_ACCENT,
    SECTION_BG,
    ICONS,
    EXPANDED,
    COLLAPSED,
    build_tree_items,
    status_html,
    set_status,
    panel_box,
)

_LOAD_TIMEOUT = 300  # 5 minutes


def _list_workspace_reports(workspace):
    """List all report names in a workspace via REST API."""
    from sempy_labs._helper_functions import (
        resolve_workspace_name_and_id,
        _base_api,
    )
    _, ws_id = resolve_workspace_name_and_id(workspace)
    url = f"/v1.0/myorg/groups/{ws_id}/reports"
    response = _base_api(request=url, client="fabric_sp")
    return [
        (r.get("name"), r.get("format", ""))
        for r in response.json().get("value", [])
        if r.get("name")
    ]


def _load_report_data(report, workspace):
    """Load report structure via connect_report."""
    from sempy_labs.report import connect_report

    def _safe_int(val, default=0):
        if val is None:
            return default
        try:
            import pandas as pd
            if pd.isna(val):
                return default
        except (TypeError, ValueError):
            pass
        try:
            return int(val)
        except (TypeError, ValueError, OverflowError):
            return default

    def _safe_bool(val, default=False):
        try:
            import pandas as pd
            if pd.isna(val):
                return default
        except (TypeError, ValueError):
            pass
        try:
            return bool(val)
        except (TypeError, ValueError):
            return default

    def _safe_str(val, default=""):
        try:
            import pandas as pd
            if pd.isna(val):
                return default
        except (TypeError, ValueError):
            pass
        return str(val) if val is not None else default

    report_data = {"pages": {}, "format": "", "report_id": "", "workspace_id": ""}

    with connect_report(report=report, readonly=True, workspace=workspace) as rw:
        report_data["format"] = str(getattr(rw, "format", ""))
        report_data["report_id"] = str(getattr(rw, "_report_id", "") or "")
        report_data["workspace_id"] = str(getattr(rw, "_workspace_id", "") or "")

        # Read page order from pages.json
        _page_order = {}
        try:
            pages_meta = rw.get(file_path="definition/pages/pages.json")
            for idx, pn in enumerate(pages_meta.get("pageOrder", [])):
                _page_order[pn] = idx
        except Exception:
            pass

        # list_pages/list_visuals may fail on NaN→int conversion inside upstream code
        try:
            pages_df = rw.list_pages()
        except (ValueError, TypeError):
            # Fallback: get pages from the definition files
            import pandas as pd
            pages_df = pd.DataFrame(columns=["Page Name", "Page Display Name", "Width", "Height", "Hidden", "Visual Count"])

        try:
            visuals_df = rw.list_visuals()
        except (ValueError, TypeError):
            import pandas as pd
            visuals_df = pd.DataFrame(columns=["Page Name", "Visual Name", "Type", "Display Type", "X", "Y", "Width", "Height", "Hidden", "Title"])

        for _row_idx, row in pages_df.iterrows():
            p_name = _safe_str(row.get("Page Name", row.get("Page Display Name", "")))
            display_name = _safe_str(row.get("Page Display Name", p_name))
            p_info = {
                "display_name": display_name,
                "width": _safe_int(row.get("Width")),
                "height": _safe_int(row.get("Height")),
                "hidden": _safe_bool(row.get("Hidden")),
                "visual_count": _safe_int(row.get("Visual Count")),
                "ordinal": _page_order.get(p_name, 9999),
                "visuals": {},
            }
            report_data["pages"][p_name] = p_info

        for _, row in visuals_df.iterrows():
            p_name = _safe_str(row.get("Page Name", row.get("Page Display Name", "")))
            if p_name not in report_data["pages"]:
                continue
            v_name = _safe_str(row.get("Visual Name"))
            v_type = _safe_str(row.get("Type"))
            display_type = _safe_str(row.get("Display Type", v_type))
            report_data["pages"][p_name]["visuals"][v_name] = {
                "type": v_type,
                "display_type": display_type,
                "x": _safe_int(row.get("X")),
                "y": _safe_int(row.get("Y")),
                "width": _safe_int(row.get("Width")),
                "height": _safe_int(row.get("Height")),
                "hidden": _safe_bool(row.get("Hidden")),
                "title": _safe_str(row.get("Title")),
            }

    return report_data


def _build_tree(report_data, expanded_pages, scan_results=None):
    """Build tree items, optionally annotating with scan violation counts."""
    scan_results = scan_results or {}
    items = []
    reports = report_data.get("reports", {})
    if reports:
        for r_name in sorted(reports):
            r = reports[r_name]
            is_rpt_expanded = r_name in expanded_pages
            marker = EXPANDED if is_rpt_expanded else COLLAPSED
            fmt = r.get("format", "")
            fmt_str = f" ({fmt})" if fmt else ""
            p_count = len(r.get("pages", {}))
            # Count total violations for this report
            rpt_violations = sum(v for k, v in scan_results.items() if k.startswith(f"report:{r_name}") or k.startswith(f"page:{r_name}\x1f") or k.startswith(f"visual:{r_name}\x1f"))
            badge = f" \u26a0\ufe0f{rpt_violations}" if rpt_violations > 0 else ""
            items.append((0, "report", f"{marker} {r_name}{fmt_str}  [{p_count} pages]{badge}", f"report:{r_name}"))
            if not is_rpt_expanded:
                continue
            for p_name in sorted(r["pages"], key=lambda pn: r["pages"][pn].get("ordinal", 9999)):
                p = r["pages"][p_name]
                full_key = f"{r_name}\x1f{p_name}"
                is_expanded = full_key in expanded_pages
                p_marker = EXPANDED if is_expanded else COLLAPSED
                hidden_suffix = " (hidden)" if p["hidden"] else ""
                v_count = len(p["visuals"])
                page_violations = scan_results.get(f"page:{full_key}", 0)
                badge = f" \u26a0\ufe0f{page_violations}" if page_violations > 0 else ""
                items.append((1, "page", f"{p_marker} {p['display_name']}{hidden_suffix}  [{v_count} visuals]{badge}", f"page:{full_key}"))
                if not is_expanded:
                    continue
                for v_name in sorted(p["visuals"]):
                    v = p["visuals"][v_name]
                    label = v["display_type"] or v["type"]
                    if v["title"]:
                        label = f"{label}: {v['title']}"
                    if v["hidden"]:
                        label += " (hidden)"
                    v_key = f"visual:{r_name}\x1f{p_name}:{v_name}"
                    if v_key in scan_results:
                        label += f" \u26a0\ufe0f{scan_results[v_key]}"
                    items.append((2, "visual", label, v_key))
    else:
        for p_name in sorted(report_data.get("pages", {}), key=lambda pn: report_data["pages"][pn].get("ordinal", 9999)):
            p = report_data["pages"][p_name]
            is_expanded = p_name in expanded_pages
            marker = EXPANDED if is_expanded else COLLAPSED
            hidden_suffix = " (hidden)" if p["hidden"] else ""
            v_count = len(p["visuals"])
            page_violations = scan_results.get(f"page:{p_name}", 0)
            badge = f" \u26a0\ufe0f{page_violations}" if page_violations > 0 else ""
            items.append(
                (0, "page", f"{marker} {p['display_name']}{hidden_suffix}  [{v_count} visuals]{badge}", f"page:{p_name}")
            )
            if not is_expanded:
                continue
            for v_name in sorted(p["visuals"]):
                v = p["visuals"][v_name]
                label = v["display_type"] or v["type"]
                if v["title"]:
                    label = f"{label}: {v['title']}"
                if v["hidden"]:
                    label += " (hidden)"
                v_key = f"visual:{p_name}:{v_name}"
                if v_key in scan_results:
                    label += f" \u26a0\ufe0f{scan_results[v_key]}"
                items.append((1, "visual", label, v_key))
    return build_tree_items(items)


def _prop_row(label, value):
    return (
        f'<tr><td style="padding:3px 10px 3px 0; font-weight:600; color:#555; '
        f'white-space:nowrap; vertical-align:top;">{label}</td>'
        f'<td style="padding:3px 0; word-break:break-word;">{value}</td></tr>'
    )


def _props_table(rows_html):
    return (
        f'<table style="font-size:13px; font-family:{FONT_FAMILY}; '
        f'border-collapse:collapse; width:100%;">'
        f'{rows_html}</table>'
    )


def _resolve_page(report_data, page_key):
    """Resolve a page key to its data dict. Handles both single and multi-report keys."""
    if "\x1f" in page_key:
        r_name, p_name = page_key.split("\x1f", 1)
        return report_data.get("reports", {}).get(r_name, {}).get("pages", {}).get(p_name)
    return report_data.get("pages", {}).get(page_key)


def _get_properties_html(report_data, key):
    """Return combined properties HTML (stats + metadata)."""
    parts = key.split(":", 2)
    node_type = parts[0]

    if node_type == "report":
        return ""

    if node_type == "page":
        p = _resolve_page(report_data, parts[1]) or {}
        display_name = parts[1].split("\x1f")[-1] if "\x1f" in parts[1] else parts[1]
        rows = _prop_row("Internal Name", display_name)
        rows += _prop_row("Visual Count", str(len(p.get("visuals", {}))))
        type_counts = {}
        for v in p.get("visuals", {}).values():
            dt = v.get("display_type") or v.get("type", "unknown")
            type_counts[dt] = type_counts.get(dt, 0) + 1
        if type_counts:
            summary = ", ".join(f"{count}\u00d7 {t}" for t, count in sorted(type_counts.items(), key=lambda x: -x[1]))
            rows += _prop_row("Visual Types", summary)
        return _props_table(rows)

    if node_type == "visual":
        p_key = parts[1]  # may contain \x1f for multi-report
        v_name = parts[2] if len(parts) > 2 else ""
        p = _resolve_page(report_data, p_key) or {}
        v = p.get("visuals", {}).get(v_name, {})
        rows = ""

        # Show used semantic model objects
        p_name_raw = p_key.split("\x1f")[-1] if "\x1f" in p_key else p_key
        vo_key = f"{p_name_raw}:{v_name}"
        objects = report_data.get("visual_objects", {}).get(vo_key, [])
        if objects:
            obj_lines = []
            for obj in objects:
                icon = "\U0001F4D0" if obj["type"] == "Measure" else "\U0001F4CF"
                obj_lines.append(f'{icon} {obj["table"]}[{obj["object"]}] ({obj["type"]})')
            rows += _prop_row("Used Objects", "<br>".join(obj_lines))
            return _props_table(rows)

        return ""

    return ""


def _get_embed_html(report_data, key):
    """Try to build an embed iframe for the selected page/visual."""
    return ""


def report_explorer_tab(workspace_input=None, report_input=None, fixer_callbacks=None, navigate_to_sm=None):
    """Build the Report Explorer tab widget."""
    _report_data = {}
    _key_map = {}
    _expanded = set()
    _current_key = [None]
    _scan_results = {}  # key -> count (for tree badges)
    _scan_details = {}  # key -> [(fixer_name, description), ...]

    load_btn = widgets.Button(description="Load Report", button_style="primary", layout=widgets.Layout(width="110px"))
    stop_btn = widgets.Button(description="\u23f9 Stop", button_style="warning", layout=widgets.Layout(width="80px", display="none"))
    _cancel_load = [False]
    expand_btn = widgets.Button(description="Expand All", layout=widgets.Layout(width="100px"))
    collapse_btn = widgets.Button(description="Collapse All", layout=widgets.Layout(width="100px"))
    scan_btn = widgets.Button(description="\U0001F50D Scan", layout=widgets.Layout(width="100px"))

    fixer_callbacks = fixer_callbacks or {}

    def _run_fixer_with_pbir_gate(fixer_fn, report, page_name, workspace, scan_only=False, **extra_kw):
        """Run a fixer function. If it fails with PBIR error, attempt conversion then retry."""
        try:
            result = fixer_fn(report=report, page_name=page_name, workspace=workspace, scan_only=scan_only, **extra_kw)
            return result
        except Exception as e:
            err_msg = str(e)
            if "PBIR format" not in err_msg and "ReportWrapper" not in err_msg:
                raise
            set_status(conn_status, f"\u26a0\ufe0f '{report}' is PBIRLegacy \u2014 attempting conversion\u2026", "#ff9500")
            converted = False
            try:
                import sempy_labs.report as _rep
                resolved_ws = workspace
                if resolved_ws is None:
                    from sempy_labs._helper_functions import resolve_workspace_name_and_id
                    resolved_ws = resolve_workspace_name_and_id(None)[0]
                _rep.upgrade_to_pbir(report=report, workspace=resolved_ws)
                converted = True
            except Exception:
                pass
            if not converted:
                try:
                    from sempy_labs.report._Fix_UpgradeToPbir import fix_upgrade_to_pbir
                    fix_upgrade_to_pbir(report=report, workspace=workspace, scan_only=False)
                    converted = True
                except Exception:
                    pass
            if converted:
                set_status(conn_status, f"Retrying fixer on '{report}'\u2026", GRAY_COLOR)
                result = fixer_fn(report=report, page_name=page_name, workspace=workspace, scan_only=scan_only, **extra_kw)
                return result
            set_status(conn_status, f"\u26a0\ufe0f '{report}' could not be converted to PBIR. Convert manually in Power BI Desktop.", "#ff3b30")
            return False
    fixer_dropdown = widgets.Dropdown(
        options=["Select action..."] + list(fixer_callbacks.keys()),
        value="Select action...",
        layout=widgets.Layout(width="250px"),
    )
    run_action_btn = widgets.Button(
        description="\u26A1 Run",
        button_style="danger",
        layout=widgets.Layout(width="100px"),
    )
    tolerance_input = widgets.BoundedFloatText(
        value=2.0, min=0.1, max=50.0, step=0.5,
        description="%",
        layout=widgets.Layout(width="90px", display="none"),
        style={"description_width": "15px"},
    )

    def _on_fixer_change(change):
        tolerance_input.layout.display = "flex" if change["new"] == "Fix Visual Alignment" else "none"

    fixer_dropdown.observe(_on_fixer_change, names="value")

    def _on_stop(_):
        _cancel_load[0] = True

    stop_btn.on_click(_on_stop)

    conn_status = status_html()
    nav_row = widgets.HBox(
        [load_btn, stop_btn, expand_btn, collapse_btn, conn_status],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 4px 0"),
    )

    action_row = widgets.HBox(
        [scan_btn, fixer_dropdown, tolerance_input, run_action_btn],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 8px 0"),
    )
    crud_output_box = widgets.VBox(
        layout=widgets.Layout(display="none", gap="4px", max_height="400px",
                              overflow_y="auto", border="1px solid #e0e0e0",
                              border_radius="8px", padding="8px", margin="0 0 8px 0"),
    )

    tree = widgets.SelectMultiple(options=[], rows=18, layout=widgets.Layout(width="320px", height="520px", font_family="monospace"))
    tree_search = widgets.Text(placeholder="\U0001F50D Filter tree\u2026", layout=widgets.Layout(width="320px"))
    _all_tree_options = []

    def _refresh_tree():
        nonlocal _key_map
        options, _key_map = _build_tree(_report_data, _expanded, _scan_results)
        _all_tree_options.clear()
        _all_tree_options.extend(options)
        _apply_tree_filter()

    def _apply_tree_filter(query=None):
        query = (query or tree_search.value).lower().strip()
        tree.unobserve(on_select, names="value")
        try:
            tree.index = ()
        except Exception:
            pass
        if query:
            # Include parent nodes (report/page) above any matching item
            all_opts = _all_tree_options
            matched = set()
            for i, o in enumerate(all_opts):
                if query in o.lower():
                    matched.add(i)
                    cur_indent = len(o) - len(o.lstrip())
                    for j in range(i - 1, -1, -1):
                        p = all_opts[j]
                        p_indent = len(p) - len(p.lstrip())
                        if p_indent < cur_indent:
                            matched.add(j)
                            cur_indent = p_indent
                            if p_indent == 0:
                                break
            tree.options = [all_opts[i] for i in sorted(matched)]
        else:
            tree.options = _all_tree_options
        tree.observe(on_select, names="value")

    def _on_tree_search(change):
        _apply_tree_filter(change.get("new", ""))
    tree_search.observe(_on_tree_search, names="value")

    def _reselect_key_in_tree(key):
        """Re-select a tree item by its key after a tree refresh, keeping it highlighted."""
        rev_map = {v: k for k, v in _key_map.items()}
        label = rev_map.get(key)
        if label and label in tree.options:
            tree.unobserve(on_select, names="value")
            try:
                tree.value = (label,)
            except Exception:
                pass
            tree.observe(on_select, names="value")

    # -- preview (top-right, powerbiclient Report widget) --
    preview_label = widgets.HTML(
        value=f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; font-family:{FONT_FAMILY}; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:2px;">Preview</div>'
    )
    preview_placeholder = widgets.HTML(
        value=f'<div style="padding:16px; color:{GRAY_COLOR}; font-size:13px; font-family:{FONT_FAMILY}; font-style:italic;">Load a report to see the live preview</div>',
    )
    _report_widget = [None]  # current active widget
    _widget_cache = {}  # report_id -> PBIReport widget (don't recreate)
    _widget_ws = {}    # report_id -> workspace_id (for refresh)
    refresh_btn = widgets.Button(description="\U0001F504 Refresh", layout=widgets.Layout(width="100px"))
    # Use a VBox as the container — we swap its children to show the Report widget
    preview_content = widgets.VBox([preview_placeholder], layout=widgets.Layout(width="100%", min_height="400px"))
    preview_box = panel_box([preview_label, widgets.HBox([refresh_btn], layout=widgets.Layout(justify_content="flex-end", margin="0 0 4px 0")), preview_content], flex="1", min_height="520px")

    def _get_or_create_widget(report_id, workspace_id):
        """Get cached widget or create new one. Returns widget or None."""
        if report_id in _widget_cache:
            return _widget_cache[report_id]
        try:
            from powerbiclient import Report as PBIReport
            rpt_widget = PBIReport(group_id=workspace_id, report_id=report_id)
            rpt_widget.layout = widgets.Layout(width="100%", height="480px")
            _widget_cache[report_id] = rpt_widget
            _widget_ws[report_id] = workspace_id
            return rpt_widget
        except Exception:
            return None

    def _show_widget(report_id, workspace_id):
        """Show the cached widget for a report."""
        w = _get_or_create_widget(report_id, workspace_id)
        if w is not None:
            _report_widget[0] = w
            preview_content.children = [w]

    def on_refresh(_):
        """Force re-create the current report widget."""
        if _report_widget[0] is None:
            return
        for rid, w in list(_widget_cache.items()):
            if w is _report_widget[0]:
                ws_id = _widget_ws.get(rid, "")
                del _widget_cache[rid]
                if rid in _widget_ws:
                    del _widget_ws[rid]
                _show_widget(rid, ws_id)
                break

    refresh_btn.on_click(on_refresh)

    # -- editable properties (bottom-right) --
    props_label = widgets.HTML(
        value=f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; font-family:{FONT_FAMILY}; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:2px;">Properties</div>'
    )
    props_html = widgets.HTML(
        value=f'<div style="padding:12px; color:{GRAY_COLOR}; font-size:13px; font-family:{FONT_FAMILY}; font-style:italic;">Select an object to view properties</div>',
    )

    # Editable property inputs (for pages and visuals)
    def _rprop(label_text, width="100%", disabled=False):
        lbl = widgets.HTML(value=f'<span style="font-size:10px; font-weight:600; color:#555; font-family:{FONT_FAMILY};">{label_text}</span>')
        inp = widgets.Text(layout=widgets.Layout(width=width, height="28px"), disabled=disabled)
        row = widgets.VBox([lbl, inp], layout=widgets.Layout(gap="0px", margin="0"))
        return inp, row

    def _rprop_int(label_text, width="100%", disabled=False):
        lbl = widgets.HTML(value=f'<span style="font-size:10px; font-weight:600; color:#555; font-family:{FONT_FAMILY};">{label_text}</span>')
        inp = widgets.IntText(layout=widgets.Layout(width=width, height="28px"), disabled=disabled)
        row = widgets.VBox([lbl, inp], layout=widgets.Layout(gap="0px", margin="0"))
        return inp, row

    def _rprop_bool(label_text):
        cb = widgets.Checkbox(value=False, description=label_text, indent=False, layout=widgets.Layout(width="auto", height="28px"))
        return cb

    rp_display_name, rp_display_name_row = _rprop("Display Name")
    rp_type, rp_type_row = _rprop("Type", disabled=True)
    rp_internal_name, rp_internal_name_row = _rprop("Internal Name", disabled=True)
    rp_page, rp_page_row = _rprop("Page", disabled=True)
    rp_visual_count, rp_visual_count_row = _rprop("Visual Count", disabled=True)
    rp_visual_types, rp_visual_types_row = _rprop("Visual Types", disabled=True)
    rp_width, rp_width_row = _rprop_int("Width")
    rp_height, rp_height_row = _rprop_int("Height")
    rp_x, rp_x_row = _rprop_int("X Position")
    rp_y, rp_y_row = _rprop_int("Y Position")
    rp_title, rp_title_row = _rprop("Title")
    rp_hidden = _rprop_bool("Hidden")

    _rp_pending = {}  # {key: {field: value, ...}}
    _rp_suppressing = [False]
    _rp_dirty = [False]
    _undo_stack = []  # list of _rp_pending snapshots
    _redo_stack = []

    undo_btn = widgets.Button(description="↩ Undo", layout=widgets.Layout(width="70px"), disabled=True)
    redo_btn = widgets.Button(description="↪ Redo", layout=widgets.Layout(width="70px"), disabled=True)
    save_btn = widgets.Button(description="✓ No changes", button_style="success", disabled=True, layout=widgets.Layout(width="180px"))
    discard_btn = widgets.Button(description="✘ Discard", button_style="warning", layout=widgets.Layout(width="90px", display="none"))
    save_status = status_html()
    save_row = widgets.HBox([undo_btn, redo_btn, save_btn, discard_btn, save_status], layout=widgets.Layout(align_items="center", gap="4px", margin="4px 0 0 0"))

    # Page-specific props container
    page_props = widgets.VBox([rp_internal_name_row, rp_display_name_row, rp_type_row, rp_visual_count_row, rp_visual_types_row, rp_width_row, rp_height_row, rp_hidden], layout=widgets.Layout(gap="2px", display="none"))
    # Visual-specific props container
    visual_props = widgets.VBox([rp_type_row, rp_internal_name_row, rp_page_row, rp_title_row, rp_x_row, rp_y_row, rp_width_row, rp_height_row, rp_hidden], layout=widgets.Layout(gap="2px", display="none"))

    def _update_undo_redo_btns():
        undo_btn.disabled = len(_undo_stack) == 0
        redo_btn.disabled = len(_redo_stack) == 0

    def _rp_mark_dirty(*_):
        if _rp_suppressing[0]:
            return
        # Snapshot before this change
        import copy
        _undo_stack.append(copy.deepcopy(_rp_pending))
        _redo_stack.clear()
        _rp_dirty[0] = True
        # Capture current values
        key = _current_key[0]
        if key:
            node_type = key.split(":")[0]
            if node_type == "page":
                _rp_pending[key] = {
                    "display_name": rp_display_name.value,
                    "width": rp_width.value,
                    "height": rp_height.value,
                    "hidden": rp_hidden.value,
                }
            elif node_type == "visual":
                _rp_pending[key] = {
                    "title": rp_title.value,
                    "x": rp_x.value,
                    "y": rp_y.value,
                    "width": rp_width.value,
                    "height": rp_height.value,
                    "hidden": rp_hidden.value,
                }
        n = len(_rp_pending)
        save_btn.description = f"⚠️ {n} unsaved change(s)"
        save_btn.button_style = "danger"
        save_btn.disabled = False
        discard_btn.layout.display = ""
        _update_undo_redo_btns()

    def _rp_mark_clean():
        _rp_dirty[0] = False
        _rp_pending.clear()
        _undo_stack.clear()
        _redo_stack.clear()
        save_btn.description = "✓ No changes"
        save_btn.button_style = "success"
        save_btn.disabled = True
        discard_btn.layout.display = "none"
        save_status.value = ""
        _update_undo_redo_btns()

    def _restore_pending(snapshot):
        """Restore _rp_pending from snapshot and update UI inputs."""
        import copy
        _rp_pending.clear()
        _rp_pending.update(copy.deepcopy(snapshot))
        n = len(_rp_pending)
        if n == 0:
            _rp_dirty[0] = False
            save_btn.description = "✓ No changes"
            save_btn.button_style = "success"
            save_btn.disabled = True
            discard_btn.layout.display = "none"
        else:
            _rp_dirty[0] = True
            save_btn.description = f"⚠️ {n} unsaved change(s)"
            save_btn.button_style = "danger"
            save_btn.disabled = False
            discard_btn.layout.display = ""
        # Refresh current key inputs
        key = _current_key[0]
        if key and key in _rp_pending:
            _rp_suppressing[0] = True
            vals = _rp_pending[key]
            node_type = key.split(":")[0]
            if node_type == "page":
                rp_display_name.value = vals.get("display_name", rp_display_name.value)
                rp_width.value = vals.get("width", rp_width.value)
                rp_height.value = vals.get("height", rp_height.value)
                rp_hidden.value = vals.get("hidden", rp_hidden.value)
            elif node_type == "visual":
                rp_title.value = vals.get("title", rp_title.value)
                rp_x.value = vals.get("x", rp_x.value)
                rp_y.value = vals.get("y", rp_y.value)
                rp_width.value = vals.get("width", rp_width.value)
                rp_height.value = vals.get("height", rp_height.value)
                rp_hidden.value = vals.get("hidden", rp_hidden.value)
            _rp_suppressing[0] = False
        elif key and key not in _rp_pending:
            # Key was undone — reload original values
            _rp_suppressing[0] = True
            _populate_report_props(key)
            _rp_suppressing[0] = False
        _update_undo_redo_btns()

    def on_undo(_):
        if not _undo_stack:
            return
        import copy
        _redo_stack.append(copy.deepcopy(_rp_pending))
        snapshot = _undo_stack.pop()
        _restore_pending(snapshot)

    def on_redo(_):
        if not _redo_stack:
            return
        import copy
        _undo_stack.append(copy.deepcopy(_rp_pending))
        snapshot = _redo_stack.pop()
        _restore_pending(snapshot)

    undo_btn.on_click(on_undo)
    redo_btn.on_click(on_redo)

    for w in [rp_display_name, rp_width, rp_height, rp_title, rp_x, rp_y]:
        w.observe(_rp_mark_dirty, names="value")
    rp_hidden.observe(_rp_mark_dirty, names="value")

    def _populate_report_props(key):
        """Fill editable property widgets from report data."""
        _rp_suppressing[0] = True
        node_type = key.split(":")[0]
        page_props.layout.display = "none"
        visual_props.layout.display = "none"

        if node_type == "page":
            p_name = key.split(":", 1)[1]
            if "\x1f" in p_name:
                _, p_name = p_name.split("\x1f", 1)
            pages = _report_data.get("pages", {})
            if not pages and _report_data.get("reports"):
                for rd in _report_data["reports"].values():
                    pages.update(rd.get("pages", {}))
            p = pages.get(p_name, {})
            rp_internal_name.value = p_name
            rp_display_name.value = p.get("display_name", p_name)
            rp_type.value = "Page"
            visuals = p.get("visuals", {})
            rp_visual_count.value = str(len(visuals))
            type_counts = {}
            for v in visuals.values():
                dt = v.get("display_type") or v.get("type", "unknown")
                type_counts[dt] = type_counts.get(dt, 0) + 1
            rp_visual_types.value = ", ".join(f"{c}\u00d7 {t}" for t, c in sorted(type_counts.items(), key=lambda x: -x[1])) if type_counts else ""
            rp_width.value = int(p.get("width", 1280))
            rp_height.value = int(p.get("height", 720))
            rp_hidden.value = bool(p.get("hidden", False))
            page_props.layout.display = ""

        elif node_type == "visual":
            parts = key.split(":", 2)
            p_key = parts[1] if len(parts) > 1 else ""
            v_name = parts[2] if len(parts) > 2 else ""
            pages = _report_data.get("pages", {})
            if not pages and _report_data.get("reports"):
                for rd in _report_data["reports"].values():
                    pages.update(rd.get("pages", {}))
            v = {}
            # resolve page via _resolve_page-style lookup
            p = None
            if "\x1f" in p_key:
                _, raw_pname = p_key.split("\x1f", 1)
                p = pages.get(raw_pname, {})
            else:
                p = pages.get(p_key, {})
            if p:
                v = p.get("visuals", {}).get(v_name, {})
            if not v:
                for p_data in pages.values():
                    if v_name in p_data.get("visuals", {}):
                        v = p_data["visuals"][v_name]
                        break
            rp_type.value = v.get("display_type", "") or v.get("type", "")
            rp_internal_name.value = v_name
            p_display = p_key.split("\x1f")[-1] if "\x1f" in p_key else p_key
            rp_page.value = (p or {}).get("display_name", p_display) if p else p_display
            rp_title.value = v.get("title", "")
            rp_x.value = int(v.get("x", 0))
            rp_y.value = int(v.get("y", 0))
            rp_width.value = int(v.get("width", 0))
            rp_height.value = int(v.get("height", 0))
            rp_hidden.value = bool(v.get("hidden", False))
            visual_props.layout.display = ""

        _rp_suppressing[0] = False

    def on_rp_save(_):
        if not _rp_pending:
            return
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        rpt_input = report_input.value.strip() if report_input else ""
        rpt = rpt_input.split(",")[0].strip()
        for pfx in ("\U0001F4C4 ", "\U0001F4CA "):
            if rpt.startswith(pfx):
                rpt = rpt[len(pfx):]
        if not rpt:
            set_status(save_status, "No report loaded.", "#ff3b30")
            return
        save_btn.disabled = True
        save_btn.description = "Saving…"
        set_status(save_status, f"Writing {len(_rp_pending)} change(s) via connect_report…", GRAY_COLOR)

        def _save_bg():
            try:
                from sempy_labs.report import connect_report
                import sys, io as _sio
                _old = sys.stdout; sys.stdout = _sio.StringIO()
                try:
                    with connect_report(report=rpt, readonly=False, workspace=ws) as rw:
                        for key, changes in _rp_pending.items():
                            node_type = key.split(":")[0]
                            if node_type == "page":
                                p_name = key.split(":", 1)[1]
                                if "\x1f" in p_name:
                                    _, p_name = p_name.split("\x1f", 1)
                                pages_df = rw.list_pages()
                                for _, row in pages_df.iterrows():
                                    if str(row.get("Page Name", "")) == p_name or str(row.get("Page Display Name", "")) == p_name:
                                        page_name = str(row.get("Page Name", ""))
                                        if "display_name" in changes:
                                            rw.set_page_property(page_name=page_name, property_name="displayName", property_value=changes["display_name"])
                                        if "width" in changes:
                                            rw.set_page_property(page_name=page_name, property_name="width", property_value=changes["width"])
                                        if "height" in changes:
                                            rw.set_page_property(page_name=page_name, property_name="height", property_value=changes["height"])
                                        if "hidden" in changes:
                                            rw.set_page_property(page_name=page_name, property_name="visibility", property_value=1 if changes["hidden"] else 0)
                                        break
                            elif node_type == "visual":
                                v_raw = key.split(":", 1)[1]
                                v_parts = v_raw.rsplit("\x1f", 1)
                                v_name = v_parts[-1]
                                # Find page for this visual
                                p_name = None
                                pages_df = rw.list_pages()
                                visuals_df = rw.list_visuals()
                                for _, vr in visuals_df.iterrows():
                                    if str(vr.get("Visual Name", "")) == v_name:
                                        p_name = str(vr.get("Page Name", ""))
                                        break
                                if p_name and v_name:
                                    if "x" in changes:
                                        rw.set_visual_property(page_name=p_name, visual_name=v_name, property_name="x", property_value=changes["x"])
                                    if "y" in changes:
                                        rw.set_visual_property(page_name=p_name, visual_name=v_name, property_name="y", property_value=changes["y"])
                                    if "width" in changes:
                                        rw.set_visual_property(page_name=p_name, visual_name=v_name, property_name="width", property_value=changes["width"])
                                    if "height" in changes:
                                        rw.set_visual_property(page_name=p_name, visual_name=v_name, property_name="height", property_value=changes["height"])
                finally:
                    sys.stdout = _old
                _rp_mark_clean()
                set_status(save_status, f"✓ Saved {len(_rp_pending)} change(s).", "#34c759")
            except Exception as e:
                set_status(save_status, f"Error: {str(e)[:200]}", "#ff3b30")
            finally:
                save_btn.disabled = False
                if _rp_dirty[0]:
                    n = len(_rp_pending)
                    save_btn.description = f"⚠️ {n} unsaved change(s)"
                    save_btn.button_style = "danger"
                else:
                    save_btn.description = "✓ No changes"
                    save_btn.button_style = "success"

        import threading
        threading.Thread(target=_save_bg, daemon=True).start()

    def on_rp_discard(_):
        _rp_mark_clean()
        key = _current_key[0]
        if key:
            _populate_report_props(key)

    save_btn.on_click(on_rp_save)
    discard_btn.on_click(on_rp_discard)
    # Violations panel (shown below properties when scan results exist)
    violations_box = widgets.VBox(layout=widgets.Layout(display="none", gap="4px"))
    # Navigation panel for visual → SM object linking
    nav_objects_box = widgets.VBox(layout=widgets.Layout(display="none", gap="4px"))
    props_box = widgets.VBox(
        [props_label, props_html, page_props, visual_props, save_row, violations_box, nav_objects_box],
        layout=widgets.Layout(
            flex="0 0 240px",
            min_height="450px",
            border=f"1px solid {BORDER_COLOR}",
            border_radius="8px",
            padding="6px",
            background_color=SECTION_BG,
        ),
    )

    # Three-column layout: Tree (with search) | Properties | Preview (side by side)
    tree_col = widgets.VBox([tree_search, tree], layout=widgets.Layout(width="320px", gap="2px"))
    panels = widgets.HBox(
        [tree_col, props_box, preview_box],
        layout=widgets.Layout(width="100%", gap="8px"),
    )
    tree_header = widgets.HTML(
        value=f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; font-family:{FONT_FAMILY}; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:2px;">Report Structure</div>'
    )

    def _load_with_pbir_gate(rpt_name, ws):
        """Try loading report, auto-convert to PBIR if needed, fall back to basic info."""
        try:
            return _load_report_data(report=rpt_name, workspace=ws)
        except Exception as e:
            err_msg = str(e)
            if "PBIR format" not in err_msg and "ReportWrapper" not in err_msg:
                raise
            # Report is PBIRLegacy — try conversion
            set_status(conn_status, f"\u26a0\ufe0f '{rpt_name}' is PBIRLegacy \u2014 attempting conversion\u2026", "#ff9500")
            converted = False
            # Method 1: upstream upgrade_to_pbir (needs resolved workspace)
            try:
                import sempy_labs.report as _rep
                resolved_ws = ws
                if resolved_ws is None:
                    from sempy_labs._helper_functions import resolve_workspace_name_and_id
                    resolved_ws = resolve_workspace_name_and_id(None)[0]
                _rep.upgrade_to_pbir(report=rpt_name, workspace=resolved_ws)
                converted = True
            except Exception:
                pass
            # Method 2: REST round-trip
            if not converted:
                try:
                    from sempy_labs.report._Fix_UpgradeToPbir import fix_upgrade_to_pbir
                    fix_upgrade_to_pbir(report=rpt_name, workspace=ws, scan_only=False)
                    converted = True
                except Exception:
                    pass
            # Try loading again after conversion
            if converted:
                try:
                    set_status(conn_status, f"Retrying load for '{rpt_name}'\u2026", GRAY_COLOR)
                    return _load_report_data(report=rpt_name, workspace=ws)
                except Exception:
                    pass
            # Fall back: return basic stub with format info but no pages/visuals
            set_status(conn_status, f"\u26a0\ufe0f '{rpt_name}' is PBIRLegacy \u2014 loaded in limited mode (no pages/visuals). Convert to PBIR in Power BI Desktop.", "#ff9500")
            return {"pages": {}, "format": "PBIRLegacy", "report_id": "", "workspace_id": ""}

    def on_load(_):
        nonlocal _report_data, _key_map
        _expanded.clear()
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        rpt_input = report_input.value.strip() if report_input else ""

        # Parse comma-separated items, or list all if blank
        if rpt_input:
            items = [x.strip() for x in rpt_input.split(",") if x.strip()]
        else:
            load_btn.disabled = True
            load_btn.description = "Listing\u2026"
            set_status(conn_status, "Listing reports\u2026", GRAY_COLOR)
            try:
                rpt_list = _list_workspace_reports(ws)
                items = [name for name, _ in rpt_list]
            except Exception as e:
                set_status(conn_status, f"Error listing reports: {e}", "#ff3b30")
                load_btn.disabled = False
                load_btn.description = "Load Report"
                return
            if not items:
                set_status(conn_status, "No reports found in workspace.", "#ff9500")
                load_btn.disabled = False
                load_btn.description = "Load Report"
                return

        load_btn.disabled = True
        load_btn.description = "Loading\u2026"
        stop_btn.layout.display = ""
        _cancel_load[0] = False
        set_status(conn_status, f"Loading {len(items)} report(s)\u2026", GRAY_COLOR)

        start_time = time.time()
        loaded = 0
        errors = 0

        try:
            if len(items) == 1:
                _report_data = _load_with_pbir_gate(rpt_name=items[0], ws=ws)
                loaded = 1
            else:
                merged = {"pages": {}, "reports": {}, "report_id": "", "workspace_id": ""}
                for i, rpt in enumerate(items):
                    if _cancel_load[0]:
                        set_status(conn_status, f"\u23f9 Stopped after {loaded}/{len(items)} reports.", "#ff9500")
                        break
                    if time.time() - start_time > _LOAD_TIMEOUT:
                        set_status(conn_status, f"\u23f1\ufe0f Timeout after {loaded}/{len(items)}.", "#ff9500")
                        break
                    set_status(conn_status, f"Report {i+1}/{len(items)}: loading '{rpt}'\u2026", GRAY_COLOR)
                    try:
                        data = _load_with_pbir_gate(rpt_name=rpt, ws=ws)
                        merged["reports"][rpt] = data
                        if not merged["report_id"]:
                            merged["report_id"] = data.get("report_id", "")
                            merged["workspace_id"] = data.get("workspace_id", "")
                        loaded += 1
                    except Exception:
                        errors += 1
                        set_status(conn_status, f"Report {i+1}/{len(items)}: '{rpt}' failed", "#ff9500")
                _report_data = merged

            if _report_data.get("reports"):
                for r_name, r_data in _report_data["reports"].items():
                    _expanded.add(r_name)
                    for p_name in r_data.get("pages", {}):
                        _expanded.add(f"{r_name}\x1f{p_name}")
            else:
                _expanded.update(_report_data.get("pages", {}).keys())

            _refresh_tree()

            total_pages = 0
            total_visuals = 0
            if _report_data.get("reports"):
                for r in _report_data["reports"].values():
                    total_pages += len(r.get("pages", {}))
                    total_visuals += sum(len(p["visuals"]) for p in r.get("pages", {}).values())
            else:
                total_pages = len(_report_data.get("pages", {}))
                total_visuals = sum(len(p["visuals"]) for p in _report_data.get("pages", {}).values())

            elapsed = int(time.time() - start_time)
            err_str = f", {errors} error(s)" if errors else ""
            set_status(conn_status, f"Loaded {loaded}/{len(items)}: {total_pages} pages, {total_visuals} visuals ({elapsed}s{err_str})", "#34c759")

            report_id = _report_data.get("report_id", "")
            workspace_id = _report_data.get("workspace_id", "")
            if not report_id and _report_data.get("reports"):
                first = next(iter(_report_data["reports"].values()), {})
                report_id = first.get("report_id", "")
                workspace_id = first.get("workspace_id", "")
            if report_id and workspace_id:
                # Clear cached widget so preview reflects latest report state
                if report_id in _widget_cache:
                    del _widget_cache[report_id]
                    _widget_ws.pop(report_id, None)
                _show_widget(report_id, workspace_id)
        except Exception as e:
            set_status(conn_status, f"Error: {e}", "#ff3b30")
        finally:
            load_btn.disabled = False
            load_btn.description = "Load Report"
            stop_btn.layout.display = "none"
            _cancel_load[0] = False

    def on_select(change):
        selected = change.get("new", ())
        if not selected:
            return
        last = selected[-1]
        if last not in _key_map:
            return
        key = _key_map[last]
        _current_key[0] = key
        # Single-click on a parent node: toggle expand/collapse
        if len(selected) == 1:
            if key.startswith("report:"):
                r_name = key.split(":", 1)[1]
                if r_name in _expanded:
                    _expanded.discard(r_name)
                else:
                    _expanded.add(r_name)
                _refresh_tree()
                _reselect_key_in_tree(key)
                return
            if key.startswith("page:"):
                p_name = key.split(":", 1)[1]
                if p_name in _expanded:
                    _expanded.discard(p_name)
                else:
                    _expanded.add(p_name)
                _refresh_tree()
                _reselect_key_in_tree(key)
        # Update properties + preview navigation
        node_type = key.split(":")[0]
        if node_type in ("page", "visual"):
            props_html.value = ""
        else:
            props_html.value = _get_properties_html(_report_data, key)
        _populate_report_props(key)

        # Show violation details with Fix buttons if scan results exist
        details = _scan_details.get(key, [])
        if details:
            violation_widgets = []
            violation_widgets.append(widgets.HTML(
                value=f'<div style="font-size:12px; font-weight:600; color:#ff3b30; font-family:{FONT_FAMILY}; '
                f'text-transform:uppercase; letter-spacing:0.5px; margin:8px 0 4px 0;">'
                f'\u26a0\ufe0f {len(details)} Violation(s)</div>'
            ))
            for fixer_name, desc in details:
                fix_btn = widgets.Button(
                    description=f"Fix: {fixer_name}",
                    button_style="warning",
                    layout=widgets.Layout(width="auto"),
                )
                fix_label = widgets.HTML(
                    value=f'<span style="font-size:12px; color:#555; font-family:{FONT_FAMILY};">{desc}</span>'
                )
                # Capture fixer_name for closure
                def _make_fix_handler(fn, k):
                    def _handler(_):
                        ws = workspace_input.value.strip() if workspace_input else None
                        ws = ws or None
                        # Extract report + page from key
                        rpt = ""
                        page = None
                        if k.startswith("visual:"):
                            v_raw = k.split(":", 1)[1]
                            if "\x1f" in v_raw:
                                rpt, rest = v_raw.split("\x1f", 1)
                                page = rest.rsplit(":", 1)[0] if ":" in rest else rest
                            else:
                                rpt = report_input.value.strip() if report_input else ""
                                page = v_raw.rsplit(":", 1)[0] if ":" in v_raw else v_raw
                        elif k.startswith("page:"):
                            p_raw = k.split(":", 1)[1]
                            if "\x1f" in p_raw:
                                rpt, page = p_raw.split("\x1f", 1)
                            else:
                                rpt = report_input.value.strip() if report_input else ""
                                page = p_raw
                        if not rpt:
                            rpt = report_input.value.strip() if report_input else ""
                        if rpt and fn in fixer_callbacks:
                            set_status(conn_status, f"Fixing: {fn}\u2026", GRAY_COLOR)
                            try:
                                import io as _io
                                from contextlib import redirect_stdout as _redirect
                                extra = {}
                                if fn == "Fix Visual Alignment":
                                    extra["tolerance_pct"] = tolerance_input.value
                                buf = _io.StringIO()
                                with _redirect(buf):
                                    _run_fixer_with_pbir_gate(fixer_callbacks[fn], report=rpt, page_name=page, workspace=ws, scan_only=False, **extra)
                                set_status(conn_status, f"\u2713 {fn} applied.", "#34c759")
                            except Exception as e:
                                set_status(conn_status, f"Error: {e}", "#ff3b30")
                    return _handler
                fix_btn.on_click(_make_fix_handler(fixer_name, key))
                violation_widgets.append(widgets.HBox(
                    [fix_btn, fix_label],
                    layout=widgets.Layout(align_items="center", gap="8px"),
                ))
            violations_box.children = violation_widgets
            violations_box.layout.display = ""
        else:
            violations_box.children = []
            violations_box.layout.display = "none"

        # Show navigation buttons for visual → SM object linking
        if key.startswith("visual:") and navigate_to_sm is not None:
            v_parts = key.split(":", 2)
            p_key = v_parts[1] if len(v_parts) > 1 else ""
            v_name = v_parts[2] if len(v_parts) > 2 else ""
            p_name_raw = p_key.split("\x1f")[-1] if "\x1f" in p_key else p_key
            vo_key = f"{p_name_raw}:{v_name}"
            objects = _report_data.get("visual_objects", {})
            # Also check multi-report visual_objects
            if not objects and _report_data.get("reports"):
                for r_data in _report_data["reports"].values():
                    objects.update(r_data.get("visual_objects", {}))
            vo_list = objects.get(vo_key, [])
            if vo_list:
                nav_widgets = []
                nav_widgets.append(widgets.HTML(
                    value=f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; font-family:{FONT_FAMILY}; '
                    f'text-transform:uppercase; letter-spacing:0.5px; margin:8px 0 4px 0;">'
                    f'\U0001F517 Navigate to Semantic Model</div>'
                ))
                seen = set()
                for obj in vo_list:
                    obj_id = f"{obj['table']}.{obj['object']}"
                    if obj_id in seen:
                        continue
                    seen.add(obj_id)
                    icon = "\U0001F4D0" if obj["type"] == "Measure" else "\U0001F4CF"
                    nav_btn = widgets.Button(
                        description=f"{icon} {obj['table']}[{obj['object']}]",
                        layout=widgets.Layout(width="auto"),
                    )
                    def _make_nav(table, obj_name, obj_type):
                        def _handler(_):
                            navigate_to_sm(obj_name, table, obj_type)
                        return _handler
                    nav_btn.on_click(_make_nav(obj["table"], obj["object"], obj["type"]))
                    nav_widgets.append(nav_btn)
                nav_objects_box.children = nav_widgets
                nav_objects_box.layout.display = ""
            else:
                nav_objects_box.children = []
                nav_objects_box.layout.display = "none"
        else:
            nav_objects_box.children = []
            nav_objects_box.layout.display = "none"

        # Page navigation via powerbiclient (if widget loaded)
        if _report_widget[0] is not None and key.startswith("page:"):
            p_raw = key.split(":", 1)[1]
            if "\x1f" in p_raw:
                r_name, p_name = p_raw.split("\x1f", 1)
                p = _report_data.get("reports", {}).get(r_name, {}).get("pages", {}).get(p_name, {})
                r_data = _report_data["reports"].get(r_name, {})
                rid = r_data.get("report_id", "")
                wid = r_data.get("workspace_id", "")
                if rid and wid:
                    _show_widget(rid, wid)
            else:
                p = _report_data.get("pages", {}).get(p_raw, {})
            page_display = p.get("display_name", p_raw.split("\x1f")[-1] if "\x1f" in p_raw else p_raw)
            try:
                _report_widget[0].set_active_page(page_display)
            except Exception:
                pass

        # Switch preview widget when selecting a report node in multi-report mode
        if key.startswith("report:") and _report_data.get("reports"):
            r_name = key.split(":", 1)[1]
            r_data = _report_data["reports"].get(r_name, {})
            rid = r_data.get("report_id", "")
            wid = r_data.get("workspace_id", "")
            if rid and wid:
                _show_widget(rid, wid)

        # Switch preview widget when selecting a visual in multi-report mode
        if key.startswith("visual:") and _report_data.get("reports"):
            v_raw = key.split(":", 1)[1]
            if "\x1f" in v_raw:
                r_name = v_raw.split("\x1f")[0]
                r_data = _report_data["reports"].get(r_name, {})
                rid = r_data.get("report_id", "")
                wid = r_data.get("workspace_id", "")
                if rid and wid:
                    _show_widget(rid, wid)

    def on_expand_all(_):
        if _report_data:
            reports = _report_data.get("reports", {})
            if reports:
                for r_name, r_data in reports.items():
                    _expanded.add(r_name)
                    for p_name in r_data.get("pages", {}):
                        _expanded.add(f"{r_name}\x1f{p_name}")
            else:
                _expanded.update(_report_data.get("pages", {}).keys())
            _refresh_tree()

    def on_collapse_all(_):
        _expanded.clear()
        if _report_data:
            _refresh_tree()

    def on_run_action(_):
        """Run the action selected in the dropdown."""
        action = fixer_dropdown.value
        if action == "Select action..." or action.startswith("──") or action not in fixer_callbacks:
            set_status(conn_status, "Select an action from the dropdown first.", "#ff9500")
            return
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        # Collect unique (report, page) pairs from all selected items
        targets = []
        for opt in tree.value:
            if opt not in _key_map:
                continue
            key = _key_map[opt]
            rpt = ""
            page = None
            if key.startswith("report:"):
                rpt = key.split(":", 1)[1]
            elif key.startswith("page:"):
                p_raw = key.split(":", 1)[1]
                if "\x1f" in p_raw:
                    rpt, page = p_raw.split("\x1f", 1)
                else:
                    rpt = report_input.value.strip() if report_input else ""
                    page = p_raw
            elif key.startswith("visual:"):
                # visual key formats:
                #   multi-report: visual:{report}\x1f{page}:{visual}
                #   single-report: visual:{page}:{visual}
                v_raw = key.split(":", 1)[1]
                if "\x1f" in v_raw:
                    rpt, rest = v_raw.split("\x1f", 1)
                    # rest = "{page}:{visual}" — extract page only
                    page = rest.rsplit(":", 1)[0] if ":" in rest else rest
                else:
                    rpt = report_input.value.strip() if report_input else ""
                    # v_raw = "{page}:{visual}" — extract page only
                    page = v_raw.rsplit(":", 1)[0] if ":" in v_raw else v_raw
            if rpt:
                targets.append((rpt, page))
        # Deduplicate
        seen = set()
        unique = []
        for t in targets:
            if t not in seen:
                seen.add(t)
                unique.append(t)
        if not unique:
            rpt = report_input.value.strip() if report_input else ""
            if rpt:
                unique = [(rpt, None)]
        if not unique:
            set_status(conn_status, "No report selected.", "#ff3b30")
            return
        set_status(conn_status, f"Running {action} on {len(unique)} target(s)\u2026", GRAY_COLOR)
        import io as _io
        from contextlib import redirect_stdout as _redirect
        errors = 0
        all_output = []
        extra_kw = {}
        if action == "Fix Visual Alignment":
            extra_kw["tolerance_pct"] = tolerance_input.value
        # Pass selection context only for CRUD operations (Delete/Duplicate)
        if action in ("Delete Selected", "Duplicate Selected"):
            sel_keys = [_key_map[opt] for opt in tree.value if opt in _key_map]
            extra_kw["selected_keys"] = sel_keys
            extra_kw["report_data"] = _report_data
        crud_output_box.layout.display = "none"
        for rpt, page in unique:
            try:
                buf = _io.StringIO()
                with _redirect(buf):
                    result = _run_fixer_with_pbir_gate(fixer_callbacks[action], report=rpt, page_name=page, workspace=ws, scan_only=False, **extra_kw)
                # If callback returns a widget, show it in crud_output_box
                if result is not None and hasattr(result, 'children'):
                    crud_output_box.children = [result]
                    crud_output_box.layout.display = ""
                captured = buf.getvalue().rstrip()
                if captured:
                    all_output.append(captured)
            except Exception as e:
                errors += 1
                all_output.append(f"\u274c Error: {e}")
        summary = f"\u2713 {action} on {len(unique)} target(s)."
        if errors:
            summary = f"\u26a0\ufe0f {action}: {len(unique) - errors} OK, {errors} error(s)."
        if all_output:
            last_lines = all_output[-1].splitlines()
            last_line = last_lines[-1][:80] if last_lines else ""
            summary += f" {last_line}"
        set_status(conn_status, summary, "#34c759" if not errors else "#ff9500")

    def on_scan(_):
        """Fast local scan — checks loaded visual types for fixable issues without API calls."""
        if not _report_data or (not _report_data.get("pages") and not _report_data.get("reports")):
            set_status(conn_status, "No report loaded. Load first.", "#ff3b30")
            return
        scan_btn.disabled = True
        scan_btn.description = "Scanning\u2026"
        _scan_results.clear()
        _scan_details.clear()

        # Fixer rules: flag fixable per-visual violations
        fixer_rules = {
            "Fix Pie Charts": (lambda v: v.get("type", "").lower() in ("piechart", "donutchart") or v.get("display_type", "").lower() in ("pie chart", "donut chart"), "Replace pie chart \u2192 bar chart"),
            "Fix Bar Charts": (lambda v: v.get("type", "").lower() in ("barchart", "stackedbarchart", "hundredpercentstackedbarchart") or v.get("display_type", "").lower() in ("bar chart", "stacked bar chart", "100% stacked bar chart"), "Apply bar chart formatting fixes"),
            "Fix Column Charts": (lambda v: v.get("type", "").lower() in ("columnchart", "clusteredcolumnchart", "stackedcolumnchart", "hundredpercentstackedcolumnchart") or v.get("display_type", "").lower() in ("column chart", "clustered column chart", "stacked column chart", "100% stacked column chart"), "Apply column chart formatting fixes"),
        }

        # Only use rules for fixers that are actually available
        active_rules = {k: v for k, v in fixer_rules.items() if k in fixer_callbacks}

        total_violations = 0

        # Count total pages for progress
        all_pages = []
        if _report_data.get("reports"):
            for r_name, r_data in _report_data["reports"].items():
                for p_name in r_data.get("pages", {}):
                    all_pages.append((r_name, p_name))
        else:
            for p_name in _report_data.get("pages", {}):
                all_pages.append((None, p_name))
        total_pages = len(all_pages)

        def _scan_pages(pages, report_prefix="", page_offset=0):
            nonlocal total_violations
            for p_idx, (p_name, p) in enumerate(pages.items()):
                current = page_offset + p_idx + 1
                scan_btn.description = f"{current}/{total_pages}"
                set_status(conn_status, f"Scanning page '{p_name}' ({current}/{total_pages})", GRAY_COLOR)
                page_key = f"page:{report_prefix}{p_name}" if report_prefix else f"page:{p_name}"
                page_count = 0
                for v_name, v in p.get("visuals", {}).items():
                    v_key = f"visual:{report_prefix}{p_name}:{v_name}" if report_prefix else f"visual:{p_name}:{v_name}"
                    v_violations = []
                    for rule_name, (rule_fn, rule_desc) in active_rules.items():
                        try:
                            if rule_fn(v):
                                v_violations.append((rule_name, rule_desc))
                        except Exception:
                            pass
                    if v_violations:
                        _scan_results[v_key] = len(v_violations)
                        _scan_details[v_key] = v_violations
                        page_count += len(v_violations)
                        total_violations += len(v_violations)
                if page_count > 0:
                    _scan_results[page_key] = page_count

        if _report_data.get("reports"):
            offset = 0
            for r_name, r_data in _report_data["reports"].items():
                prefix = f"{r_name}\x1f"
                pages = r_data.get("pages", {})
                _scan_pages(pages, report_prefix=prefix, page_offset=offset)
                offset += len(pages)
                # Aggregate to report level
                rpt_total = sum(v for k, v in _scan_results.items() if k.startswith(f"page:{prefix}"))
                if rpt_total > 0:
                    _scan_results[f"report:{r_name}"] = rpt_total
        else:
            _scan_pages(_report_data.get("pages", {}))

        _refresh_tree()
        scan_btn.disabled = False
        scan_btn.description = "\U0001F50D Scan"
        if total_violations > 0:
            n_reports = len(_report_data.get("reports", {})) or 1
            set_status(conn_status, f"\U0001F50D Scan complete: {total_violations} finding(s) across {n_reports} report(s).", "#ff9500")
        else:
            set_status(conn_status, f"\u2713 Scan complete: no issues found.", "#34c759")

    load_btn.on_click(on_load)
    tree.observe(on_select, names="value")
    expand_btn.on_click(on_expand_all)
    collapse_btn.on_click(on_collapse_all)
    scan_btn.on_click(on_scan)
    run_action_btn.on_click(on_run_action)

    # --- PBIR Status panel (rendered below PBI Fixer container) ---
    format_btn = widgets.Button(description="\U0001F4CB PBIR Status", layout=widgets.Layout(width="130px"))
    convert_all_btn = widgets.Button(description="\u26A1 Convert All Legacy", button_style="danger", layout=widgets.Layout(width="180px", display="none"))
    format_html = widgets.HTML(value="")
    _fmt_close_btn = widgets.Button(
        description="\u2715", layout=widgets.Layout(width="28px", height="28px"),
        tooltip="Close",
    )
    _fmt_header = widgets.HBox(
        [widgets.HTML(value=f'<b style="font-size:16px; line-height:1.4;">\U0001F4CB PBIR Status</b>'), _fmt_close_btn],
        layout=widgets.Layout(justify_content="space-between", align_items="center", min_height="32px"),
    )
    format_container = widgets.VBox(
        [_fmt_header, format_html],
        layout=widgets.Layout(
            display="none", max_height="300px", overflow_y="auto",
            border=f"1px solid {BORDER_COLOR}", border_radius="8px",
            padding="10px 8px 8px 8px", background_color=SECTION_BG,
        ),
    )
    _fmt_close_btn.on_click(lambda _: setattr(format_container.layout, "display", "none"))
    _format_data = []  # [(name, report_id, format), ...]

    def _on_format_overview(_):
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        format_btn.disabled = True
        format_btn.description = "Loading\u2026"
        _format_data.clear()
        try:
            rpt_list = _list_workspace_reports(ws)
            html = '<table style="border-collapse:collapse; font-size:12px; font-family:monospace;">'
            html += '<tr style="background:#f5f5f5;"><th style="padding:3px 12px 3px 8px; text-align:left;">Report</th><th style="padding:3px 8px; text-align:left;">Format</th></tr>'
            n_legacy = 0
            for name, fmt in rpt_list:
                _format_data.append((name, fmt))
                if fmt == "PBIR":
                    badge = '<span style="color:#34c759; font-weight:600;">PBIR</span>'
                elif "Legacy" in fmt:
                    badge = f'<span style="color:#ff9500; font-weight:600;">{fmt} \u26a0\ufe0f</span>'
                    n_legacy += 1
                else:
                    badge = f'<span style="color:#888;">{fmt or "unknown"}</span>'
                html += f'<tr><td style="padding:2px 12px 2px 8px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{name}</td>'
                html += f'<td style="padding:2px 8px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{badge}</td></tr>'
            html += '</table>'
            html += f'<div style="font-size:11px; color:#555; margin-top:4px;">{len(rpt_list)} report(s), {n_legacy} legacy</div>'
            format_html.value = html
            format_container.layout.display = ""
            if n_legacy > 0:
                convert_all_btn.layout.display = ""
            else:
                convert_all_btn.layout.display = "none"
            set_status(conn_status, f"\u2713 {len(rpt_list)} reports, {n_legacy} PBIRLegacy.", "#34c759" if n_legacy == 0 else "#ff9500")
        except Exception as e:
            format_html.value = f'<div style="color:#ff3b30;">Error: {str(e)[:100]}</div>'
            format_container.layout.display = ""
        format_btn.disabled = False
        format_btn.description = "\U0001F4CB PBIR Status"

    def _on_convert_all(_):
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        legacy = [name for name, fmt in _format_data if "Legacy" in fmt]
        if not legacy:
            return
        convert_all_btn.disabled = True
        convert_all_btn.description = "Converting\u2026"
        converted = 0
        for rpt in legacy:
            try:
                set_status(conn_status, f"Converting '{rpt}'\u2026", GRAY_COLOR)
                try:
                    import sempy_labs.report as _rep
                    _rep.upgrade_to_pbir(report=rpt, workspace=ws)
                    converted += 1
                except Exception:
                    from sempy_labs.report._Fix_UpgradeToPbir import fix_upgrade_to_pbir
                    fix_upgrade_to_pbir(report=rpt, workspace=ws, scan_only=False)
                    converted += 1
            except Exception:
                pass
        set_status(conn_status, f"\u2713 Converted {converted}/{len(legacy)} reports.", "#34c759")
        convert_all_btn.disabled = False
        convert_all_btn.description = "\u26A1 Convert All Legacy"
        # Refresh overview
        _on_format_overview(None)

    format_btn.on_click(_on_format_overview)
    convert_all_btn.on_click(_on_convert_all)

    format_row = widgets.HBox(
        [format_btn, convert_all_btn],
        layout=widgets.Layout(align_items="center", gap="8px", margin="4px 0 0 0"),
    )

    widget = widgets.VBox([nav_row, action_row, crud_output_box, tree_header, panels, format_row], layout=widgets.Layout(padding="12px", gap="4px"))
    # Expose format_container for external placement (below the main PBI Fixer UI)
    widget._format_container = format_container
    return widget, on_load