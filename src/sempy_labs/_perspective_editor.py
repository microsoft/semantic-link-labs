# Perspective Editor tab for PBI Fixer.
# Based on Michael Kovalsky's perspective_editor — adapted for tab integration.
# Provides an expand/collapse checkbox tree with tri-state indicators
# to create, modify, and delete perspectives on a semantic model.

import ipywidgets as widgets

from sempy_labs._ui_components import (
    FONT_FAMILY,
    BORDER_COLOR,
    GRAY_COLOR,
    ICON_ACCENT,
    SECTION_BG,
    ICONS,
    status_html,
    set_status,
)


def _load_perspective_data(dataset, workspace):
    """Load model objects and existing perspectives via TOM (single connection)."""
    from sempy_labs.tom import connect_semantic_model

    data = {
        "tables": {},
        "perspectives": [],
        "perspective_members": {},
        "hidden_objects": set(),
        "hidden_tables": set(),
    }

    with connect_semantic_model(dataset=dataset, readonly=True, workspace=workspace) as tm:
        # Perspectives
        data["perspectives"] = sorted(p.Name for p in tm.model.Perspectives)

        # Build membership: {perspective: {table: {type: set(names)}}}
        for p in tm.model.Perspectives:
            members = {}
            for pt in p.PerspectiveTables:
                tbl = pt.Table.Name
                members[tbl] = {
                    "columns": set(pc.Column.Name for pc in pt.PerspectiveColumns),
                    "measures": set(pm.Measure.Name for pm in pt.PerspectiveMeasures),
                    "hierarchies": set(ph.Hierarchy.Name for ph in pt.PerspectiveHierarchies),
                }
            data["perspective_members"][p.Name] = members

        # Tables and objects
        for table in tm.model.Tables:
            t_name = table.Name
            if table.IsHidden:
                data["hidden_tables"].add(t_name)
            columns = sorted(c.Name for c in tm.all_columns() if c.Parent == table)
            measures = sorted(m.Name for m in table.Measures)
            hierarchies = sorted(h.Name for h in table.Hierarchies)
            for c in tm.all_columns():
                if c.Parent == table and c.IsHidden:
                    data["hidden_objects"].add((t_name, "columns", c.Name))
            for m in table.Measures:
                if m.IsHidden:
                    data["hidden_objects"].add((t_name, "measures", m.Name))
            for h in table.Hierarchies:
                if h.IsHidden:
                    data["hidden_objects"].add((t_name, "hierarchies", h.Name))
            data["tables"][t_name] = {
                "columns": columns,
                "measures": measures,
                "hierarchies": hierarchies,
            }

    return data


def perspective_editor_tab(workspace_input=None, report_input=None):
    """Build the Perspective Editor tab widget."""
    _data = {}

    # Mutable state containers
    _table_cbs = {}          # table_name -> checkbox
    _child_cbs_map = {}      # table_name -> [(cb, obj_type, obj_name), ...]
    _table_summary_labels = {}
    _status_icons = {}
    _tree_sections = []

    # --- Theme ---
    text_color = "inherit"
    summary_color = "#666"
    table_summary_color = "#888"

    icon_map = {"columns": ICONS["column"], "measures": ICONS["measure"], "hierarchies": ICONS["hierarchy"]}
    table_icon = ICONS["table"]
    expand_arrow = "\u25B6"
    collapse_arrow = "\u25BC"

    # Selection state icons (orange circles)
    _icon_style = f"color:{ICON_ACCENT}; display:inline-block; width:30px; text-align:center; vertical-align:middle;"
    ICON_ALL = f'<span style="{_icon_style} font-size:30px;" title="All selected">\u25CF</span>'
    ICON_SOME = f'<span style="{_icon_style} font-size:18px;" title="Partially selected">\u25D0</span>'
    ICON_NONE = f'<span style="{_icon_style} font-size:18px;" title="None selected">\u25CB</span>'

    # --- Widgets ---
    load_btn = widgets.Button(description="Load Model", button_style="primary", layout=widgets.Layout(width="110px"))
    select_all_btn = widgets.Button(description="Select All", layout=widgets.Layout(width="100px"))
    deselect_all_btn = widgets.Button(description="Deselect All", layout=widgets.Layout(width="100px"))
    conn_status = status_html()

    NEW_PERSPECTIVE = "+ New Perspective"

    persp_dropdown = widgets.Dropdown(
        options=[],
        layout=widgets.Layout(width="200px"),
    )
    new_name_input = widgets.Text(
        placeholder="Perspective name",
        layout=widgets.Layout(width="180px", display="none"),
    )
    confirm_create_btn = widgets.Button(
        description="OK", button_style="success",
        layout=widgets.Layout(width="50px", display="none"),
    )
    cancel_create_btn = widgets.Button(
        description="\u2716",
        layout=widgets.Layout(width="36px", display="none"),
    )

    load_row = widgets.HBox(
        [load_btn, select_all_btn, deselect_all_btn, conn_status],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 8px 0"),
    )

    header_label = widgets.HTML(
        value=f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; '
        f'font-family:{FONT_FAMILY}; text-transform:uppercase; letter-spacing:0.5px; '
        f'margin-bottom:2px;">Perspective Editor</div>'
        f'<div style="font-size:11px; color:#888; font-family:{FONT_FAMILY}; '
        f'font-style:italic; margin-bottom:4px;">'
        f'\u2139\ufe0f Works with a single model. If multiple models are entered, the first one is used.</div>'
    )

    persp_row = widgets.HBox(
        [persp_dropdown, new_name_input, confirm_create_btn, cancel_create_btn],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 8px 0"),
    )

    # Global summary
    _summary_style = (
        f"font-size:13px; color:{summary_color}; font-family:{FONT_FAMILY};"
    )
    summary_label = widgets.HTML(value="")

    tree_container = widgets.VBox(
        layout=widgets.Layout(
            max_height="500px", overflow_y="auto",
            border=f"1px solid {BORDER_COLOR}", border_radius="8px",
            padding="8px", background_color=SECTION_BG,
        ),
    )

    # Action buttons
    delete_btn = widgets.Button(description="Delete", button_style="danger", layout=widgets.Layout(width="100px"))
    save_btn = widgets.Button(description="Save", button_style="primary", disabled=True, layout=widgets.Layout(width="100px"))
    save_status = status_html()

    # Delete confirmation
    confirm_delete_label = widgets.HTML()
    confirm_delete_yes = widgets.Button(description="Yes, Delete", button_style="danger", layout=widgets.Layout(width="110px"))
    confirm_delete_no = widgets.Button(description="Cancel", layout=widgets.Layout(width="80px"))
    confirm_delete_box = widgets.HBox(
        [confirm_delete_label, confirm_delete_yes, confirm_delete_no],
        layout=widgets.Layout(
            display="none", align_items="center", gap="10px",
            padding="10px 14px", margin="8px 0",
            border="1px solid #ff3b30", border_radius="8px",
        ),
    )

    button_row = widgets.HBox(
        [delete_btn, save_btn, save_status],
        layout=widgets.Layout(justify_content="flex-end", align_items="center", gap="10px", margin="8px 0 0 0"),
    )

    _previous_persp = [None]
    _loading = [False]  # Guard to prevent observer churn during bulk load

    # ---------------------------------------------------------
    # Build tree UI from loaded data
    # ---------------------------------------------------------
    def _build_tree():
        _table_cbs.clear()
        _child_cbs_map.clear()
        _table_summary_labels.clear()
        _status_icons.clear()

        total_tables = len(_data.get("tables", {}))
        summary_label.value = f'<span style="{_summary_style}">0/{total_tables} tables</span>'

        sections = []
        for table_name in sorted(_data["tables"]):
            table_data = _data["tables"][table_name]

            table_cb = widgets.Checkbox(value=False, indent=False, layout=widgets.Layout(width="22px", margin="0"))
            status_icon = widgets.HTML(value=ICON_NONE, layout=widgets.Layout(width="16px", margin="0 0 0 -4px"))

            expand_btn = widgets.Button(
                description=expand_arrow,
                layout=widgets.Layout(width="32px", border="none"),
            )
            expand_btn.style.button_color = "transparent"
            expand_btn.style.font_weight = "500"

            table_color = GRAY_COLOR if table_name in _data.get("hidden_tables", set()) else text_color
            table_label = widgets.HTML(
                value=f'<span style="font-size:20px; vertical-align:middle; color:{table_color};">{table_icon}</span>'
                f' <span style="font-size:14px; font-weight:500; vertical-align:middle; color:{table_color};">{table_name}</span>'
            )

            n_cols = len(table_data["columns"])
            n_meas = len(table_data["measures"])
            n_hier = len(table_data["hierarchies"])
            table_summary = widgets.HTML(
                value=f'<span style="font-size:12px; color:{table_summary_color}; margin-left:8px;">'
                f'0/{n_cols} columns, 0/{n_meas} measures, 0/{n_hier} hierarchies</span>'
            )

            children_box = widgets.VBox(layout=widgets.Layout(display="none", margin="0 0 0 32px"))

            child_list = []
            child_widget_list = []
            for obj_type in ["columns", "measures", "hierarchies"]:
                icon = icon_map[obj_type]
                for obj_name in table_data[obj_type]:
                    is_hidden = (table_name, obj_type, obj_name) in _data.get("hidden_objects", set())
                    if not is_hidden and obj_type != "measures":
                        is_hidden = table_name in _data.get("hidden_tables", set())
                    cb = widgets.Checkbox(value=False, indent=False, layout=widgets.Layout(width="22px", margin="0"))
                    label_color = GRAY_COLOR if is_hidden else text_color
                    label = widgets.HTML(
                        value=f'<span style="font-size:16px; color:{label_color}; vertical-align:middle;">{icon} {obj_name}</span>'
                    )
                    row = widgets.HBox([cb, label], layout=widgets.Layout(align_items="center", gap="4px"))
                    child_widget_list.append(row)
                    child_list.append((cb, obj_type, obj_name))

            children_box.children = child_widget_list

            # Toggle expand/collapse
            def make_toggle(btn, box):
                def toggle(_):
                    if box.layout.display == "none":
                        box.layout.display = "flex"
                        btn.description = collapse_arrow
                    else:
                        box.layout.display = "none"
                        btn.description = expand_arrow
                return toggle

            expand_btn.on_click(make_toggle(expand_btn, children_box))

            # Link table checkbox <-> children with tri-state
            def make_linker(t_cb, s_icon, c_list, tbl_name, tbl_summary):
                updating = [False]
                tbl_data = _data["tables"][tbl_name]
                totals = {
                    "columns": len(tbl_data["columns"]),
                    "measures": len(tbl_data["measures"]),
                    "hierarchies": len(tbl_data["hierarchies"]),
                }

                def _update_icon():
                    vals = [cb.value for cb, _, _ in c_list]
                    if all(vals) and vals:
                        s_icon.value = ICON_ALL
                    elif any(vals):
                        s_icon.value = ICON_SOME
                    else:
                        s_icon.value = ICON_NONE

                def _update_table_summary():
                    counts = {"columns": 0, "measures": 0, "hierarchies": 0}
                    for cb, obj_type, _ in c_list:
                        if cb.value:
                            counts[obj_type] += 1
                    tbl_summary.value = (
                        f'<span style="font-size:12px; color:{table_summary_color}; margin-left:8px;">'
                        f'{counts["columns"]}/{totals["columns"]} columns, '
                        f'{counts["measures"]}/{totals["measures"]} measures, '
                        f'{counts["hierarchies"]}/{totals["hierarchies"]} hierarchies</span>'
                    )

                def on_table(change):
                    if updating[0] or _loading[0]:
                        return
                    updating[0] = True
                    for cb, _, _ in c_list:
                        cb.value = change["new"]
                    _update_icon()
                    _update_table_summary()
                    _update_global_summary()
                    updating[0] = False

                def on_child(_change):
                    if updating[0] or _loading[0]:
                        return
                    updating[0] = True
                    vals = [cb.value for cb, _, _ in c_list]
                    t_cb.value = bool(any(vals))
                    _update_icon()
                    _update_table_summary()
                    _update_global_summary()
                    updating[0] = False

                t_cb.observe(on_table, names="value")
                for cb, _, _ in c_list:
                    cb.observe(on_child, names="value")

            if child_list:
                make_linker(table_cb, status_icon, child_list, table_name, table_summary)

            table_row = widgets.HBox(
                [table_cb, status_icon, expand_btn, table_label, table_summary],
                layout=widgets.Layout(align_items="center"),
            )
            sections.append(table_row)
            sections.append(children_box)

            _table_cbs[table_name] = table_cb
            _child_cbs_map[table_name] = child_list
            _table_summary_labels[table_name] = table_summary
            _status_icons[table_name] = status_icon

        tree_container.children = sections
        save_btn.disabled = False

    def _update_global_summary():
        total_tables = len(_data.get("tables", {}))
        selected = sum(1 for t in _data["tables"] if any(cb.value for cb, _, _ in _child_cbs_map.get(t, [])))
        summary_label.value = f'<span style="{_summary_style}">{selected}/{total_tables} tables</span>'

    # ---------------------------------------------------------
    # Load perspective state into checkboxes
    # ---------------------------------------------------------
    def _load_perspective(persp_name):
        _loading[0] = True
        members = _data.get("perspective_members", {}).get(persp_name, {})
        for table_name in _data.get("tables", {}):
            tbl_members = members.get(table_name, {})
            c_list = _child_cbs_map.get(table_name, [])
            for cb, obj_type, obj_name in c_list:
                cb.value = obj_name in tbl_members.get(obj_type, set())
        _loading[0] = False
        # Manually sync all UI state (observers were suppressed)
        for table_name in _data.get("tables", {}):
            c_list = _child_cbs_map.get(table_name, [])
            vals = [cb.value for cb, _, _ in c_list]
            # Table checkbox
            if table_name in _table_cbs:
                _table_cbs[table_name].value = bool(any(vals))
            # Status icon
            if table_name in _status_icons:
                if all(vals) and vals:
                    _status_icons[table_name].value = ICON_ALL
                elif any(vals):
                    _status_icons[table_name].value = ICON_SOME
                else:
                    _status_icons[table_name].value = ICON_NONE
            # Table summary
            if table_name in _table_summary_labels:
                tbl_data = _data["tables"][table_name]
                totals = {k: len(v) for k, v in tbl_data.items()}
                counts = {"columns": 0, "measures": 0, "hierarchies": 0}
                for cb, obj_type, _ in c_list:
                    if cb.value:
                        counts[obj_type] += 1
                _table_summary_labels[table_name].value = (
                    f'<span style="font-size:12px; color:{table_summary_color}; margin-left:8px;">'
                    f'{counts["columns"]}/{totals["columns"]} columns, '
                    f'{counts["measures"]}/{totals["measures"]} measures, '
                    f'{counts["hierarchies"]}/{totals["hierarchies"]} hierarchies</span>'
                )
        _update_global_summary()

    def _clear_all():
        _loading[0] = True
        for table_name in _child_cbs_map:
            for cb, _, _ in _child_cbs_map[table_name]:
                cb.value = False
        _loading[0] = False
        for table_name in _data.get("tables", {}):
            if table_name in _table_cbs:
                _table_cbs[table_name].value = False
            if table_name in _status_icons:
                _status_icons[table_name].value = ICON_NONE
            if table_name in _table_summary_labels:
                tbl_data = _data["tables"][table_name]
                totals = {k: len(v) for k, v in tbl_data.items()}
                _table_summary_labels[table_name].value = (
                    f'<span style="font-size:12px; color:{table_summary_color}; margin-left:8px;">'
                    f'0/{totals["columns"]} columns, '
                    f'0/{totals["measures"]} measures, '
                    f'0/{totals["hierarchies"]} hierarchies</span>'
                )
        _update_global_summary()

    # ---------------------------------------------------------
    # Dropdown options
    # ---------------------------------------------------------
    def _dropdown_options():
        return sorted(_data.get("perspectives", [])) + [NEW_PERSPECTIVE]

    def on_persp_change(change):
        if change["new"] == NEW_PERSPECTIVE:
            new_name_input.layout.display = "inline-flex"
            confirm_create_btn.layout.display = "inline-flex"
            cancel_create_btn.layout.display = "inline-flex"
        else:
            new_name_input.layout.display = "none"
            confirm_create_btn.layout.display = "none"
            cancel_create_btn.layout.display = "none"
            if change["new"]:
                _previous_persp[0] = change["new"]
                _load_perspective(change["new"])

    persp_dropdown.observe(on_persp_change, names="value")

    # ---------------------------------------------------------
    # Button handlers
    # ---------------------------------------------------------
    def on_load(_):
        nonlocal _data
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds = report_input.value.strip() if report_input else ""
        if "," in ds:
            ds = ds.split(",")[0].strip()
            set_status(conn_status, f"Using first model: '{ds}'", "#ff9500")
        if not ds:
            set_status(conn_status, "Enter a semantic model name in the top bar.", "#ff3b30")
            return
        load_btn.disabled = True
        load_btn.description = "Loading\u2026"
        set_status(conn_status, f"Connecting to '{ds}'\u2026", GRAY_COLOR)
        try:
            _data = _load_perspective_data(ds, ws)
            _build_tree()
            persp_dropdown.options = _dropdown_options()
            n_t = len(_data["tables"])
            n_p = len(_data["perspectives"])
            set_status(conn_status, f"Loaded: {n_t} tables, {n_p} perspectives", "#34c759")
            # Load first perspective if exists
            if _data["perspectives"]:
                persp_dropdown.value = _data["perspectives"][0]
                # Force-load in case the observer didn't fire (value unchanged)
                _load_perspective(_data["perspectives"][0])
        except Exception as e:
            set_status(conn_status, f"Error: {e}", "#ff3b30")
        finally:
            load_btn.disabled = False
            load_btn.description = "Load Model"

    def on_save(_):
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds = report_input.value.strip() if report_input else ""
        if "," in ds:
            ds = ds.split(",")[0].strip()
        p_name = persp_dropdown.value
        if p_name == NEW_PERSPECTIVE:
            p_name = new_name_input.value.strip()
        if not ds:
            set_status(save_status, "No model loaded.", "#ff3b30")
            return
        if not p_name or p_name == NEW_PERSPECTIVE:
            set_status(save_status, "Select or create a perspective.", "#ff3b30")
            return
        save_btn.disabled = True
        save_btn.description = "Saving\u2026"
        set_status(save_status, "Writing via XMLA\u2026", GRAY_COLOR)
        try:
            from sempy_labs.tom import connect_semantic_model
            with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tm:
                p = tm.model.Perspectives.Find(p_name)
                if not p:
                    p = tm.add_perspective(p_name)
                # Clear existing membership
                p.PerspectiveTables.Clear()

                # Collect selected items
                selected = []
                for table_name in _data["tables"]:
                    c_list = _child_cbs_map.get(table_name, [])
                    checked = [(cb, t, n) for cb, t, n in c_list if cb.value]
                    if len(checked) == len(c_list) and len(c_list) > 0:
                        # All checked -> add whole table
                        selected.append({"table": table_name, "type": "table"})
                    elif checked:
                        for _, obj_type, obj_name in checked:
                            selected.append({"table": table_name, "type": obj_type, "name": obj_name})

                # Apply whole tables first
                for s in selected:
                    if s.get("type") == "table":
                        tm.add_to_perspective(
                            object=tm.model.Tables[s["table"]],
                            perspective_name=p_name,
                        )
                # Apply individual objects
                for s in selected:
                    obj_type = s.get("type")
                    if obj_type == "table":
                        continue
                    table = s.get("table")
                    obj_name = s.get("name")
                    if obj_type == "columns":
                        tm.add_to_perspective(object=tm.model.Tables[table].Columns[obj_name], perspective_name=p_name)
                    elif obj_type == "measures":
                        tm.add_to_perspective(object=tm.model.Tables[table].Measures[obj_name], perspective_name=p_name)
                    elif obj_type == "hierarchies":
                        tm.add_to_perspective(object=tm.model.Tables[table].Hierarchies[obj_name], perspective_name=p_name)

                tm.model.SaveChanges()

            # Update local cache
            new_members = {}
            for s in selected:
                tbl = s.get("table")
                if tbl not in new_members:
                    new_members[tbl] = {"columns": set(), "measures": set(), "hierarchies": set()}
                if s.get("type") == "table":
                    new_members[tbl] = {
                        "columns": set(_data["tables"][tbl]["columns"]),
                        "measures": set(_data["tables"][tbl]["measures"]),
                        "hierarchies": set(_data["tables"][tbl]["hierarchies"]),
                    }
                elif s.get("type") in ("columns", "measures", "hierarchies"):
                    new_members[tbl][s["type"]].add(s["name"])
            _data["perspective_members"][p_name] = new_members

            if p_name not in _data["perspectives"]:
                _data["perspectives"].append(p_name)
                persp_dropdown.options = _dropdown_options()
                persp_dropdown.value = p_name

            set_status(save_status, f"\u2713 Perspective '{p_name}' saved.", "#34c759")
        except Exception as e:
            set_status(save_status, f"Error: {e}", "#ff3b30")
        finally:
            save_btn.disabled = False
            save_btn.description = "Save"

    def on_delete(_):
        p_name = persp_dropdown.value
        if not p_name or p_name == NEW_PERSPECTIVE:
            set_status(save_status, "No perspective selected.", "#ff3b30")
            return
        confirm_delete_label.value = (
            f'<span style="font-size:14px; color:{text_color}; font-family:{FONT_FAMILY};">'
            f'Are you sure you want to delete <b>{p_name}</b>?</span>'
        )
        confirm_delete_box.layout.display = "flex"

    def on_confirm_delete(_):
        confirm_delete_box.layout.display = "none"
        p_name = persp_dropdown.value
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds = report_input.value.strip() if report_input else ""
        if "," in ds:
            ds = ds.split(",")[0].strip()
        if not ds or not p_name:
            return
        try:
            from sempy_labs.tom import connect_semantic_model
            with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tm:
                p = tm.model.Perspectives[p_name]
                tm.remove_object(object=p)
                tm.model.SaveChanges()
            _data["perspective_members"].pop(p_name, None)
            _data["perspectives"].remove(p_name)
            persp_dropdown.options = _dropdown_options()
            real_opts = [o for o in persp_dropdown.options if o != NEW_PERSPECTIVE]
            if real_opts:
                persp_dropdown.value = real_opts[0]
            set_status(save_status, f"\u2713 Deleted perspective '{p_name}'.", "#34c759")
        except Exception as e:
            set_status(save_status, f"Error: {e}", "#ff3b30")

    def on_cancel_delete(_):
        confirm_delete_box.layout.display = "none"

    def _hide_create_inputs():
        new_name_input.value = ""
        new_name_input.layout.display = "none"
        confirm_create_btn.layout.display = "none"
        cancel_create_btn.layout.display = "none"

    def on_confirm_create(_):
        name = new_name_input.value.strip()
        if not name:
            set_status(save_status, "Enter a perspective name.", "#ff3b30")
            return
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds = report_input.value.strip() if report_input else ""
        if "," in ds:
            ds = ds.split(",")[0].strip()
        if not ds:
            set_status(save_status, "No model loaded.", "#ff3b30")
            return
        try:
            from sempy_labs.tom import connect_semantic_model
            with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tm:
                tm.add_perspective(name)
                tm.model.SaveChanges()
            _data["perspective_members"][name] = {}
            _data["perspectives"].append(name)
            persp_dropdown.options = _dropdown_options()
            persp_dropdown.value = name
            _hide_create_inputs()
            set_status(save_status, f"\u2713 Created perspective '{name}'.", "#34c759")
        except Exception as e:
            set_status(save_status, f"Error: {e}", "#ff3b30")

    def on_cancel_create(_):
        _hide_create_inputs()
        if _previous_persp[0]:
            persp_dropdown.value = _previous_persp[0]

    def on_select_all(_):
        for t_name in _table_cbs:
            _table_cbs[t_name].value = True

    def on_deselect_all(_):
        _clear_all()

    load_btn.on_click(on_load)
    save_btn.on_click(on_save)
    delete_btn.on_click(on_delete)
    confirm_delete_yes.on_click(on_confirm_delete)
    confirm_delete_no.on_click(on_cancel_delete)
    confirm_create_btn.on_click(on_confirm_create)
    cancel_create_btn.on_click(on_cancel_create)
    select_all_btn.on_click(on_select_all)
    deselect_all_btn.on_click(on_deselect_all)

    return widgets.VBox(
        [load_row, header_label, persp_row, summary_label, tree_container, button_row, confirm_delete_box],
        layout=widgets.Layout(padding="12px", gap="4px"),
    )
