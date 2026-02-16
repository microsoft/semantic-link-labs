import ipywidgets as widgets
from IPython.display import display
from sempy_labs.tom import connect_semantic_model
from typing import Optional
from uuid import UUID


def perspective_editor(dataset: str | UUID, workspace: Optional[str | UUID] = None):

    # -----------------------------
    # LOAD MODEL METADATA
    # -----------------------------
    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=True
    ) as tom:

        metadata = {}
        hidden_objects = set()  # (table, obj_type, obj_name) tuples
        hidden_tables = set()
        for table in tom.model.Tables:
            if table.IsHidden:
                hidden_tables.add(table.Name)
            columns = sorted([c.Name for c in tom.all_columns() if c.Parent == table])
            measures = sorted([m.Name for m in table.Measures])
            hierarchies = sorted([h.Name for h in table.Hierarchies])
            for c in tom.all_columns():
                if c.Parent == table and c.IsHidden:
                    hidden_objects.add((table.Name, "columns", c.Name))
            for m in table.Measures:
                if m.IsHidden:
                    hidden_objects.add((table.Name, "measures", m.Name))
            for h in table.Hierarchies:
                if h.IsHidden:
                    hidden_objects.add((table.Name, "hierarchies", h.Name))
            metadata[table.Name] = {
                "columns": columns,
                "measures": measures,
                "hierarchies": hierarchies,
            }

        perspectives = sorted(p.Name for p in tom.model.Perspectives)

        # Build perspective membership: {perspective: {table: {type: set(names)}}}
        perspective_members = {}
        for p in tom.model.Perspectives:
            members = {}
            for pt in p.PerspectiveTables:
                tbl = pt.Table.Name
                members[tbl] = {
                    "columns": set(pc.Column.Name for pc in pt.PerspectiveColumns),
                    "measures": set(pm.Measure.Name for pm in pt.PerspectiveMeasures),
                    "hierarchies": set(
                        ph.Hierarchy.Name for ph in pt.PerspectiveHierarchies
                    ),
                }
            perspective_members[p.Name] = members

    # -----------------------------
    # STATUS
    # -----------------------------
    status = widgets.HTML(value="")

    def show_status(msg, color):
        status.value = (
            f'<div style="padding:8px 12px; border-radius:8px; '
            f"background:{color}1a; color:{color}; font-size:14px; "
            f'font-family:-apple-system,BlinkMacSystemFont,sans-serif;">{msg}</div>'
        )

    # -----------------------------
    # HEADER ROW
    # -----------------------------
    title_label = widgets.HTML(
        value='<div style="font-size:20px; font-weight:600; '
        'font-family:-apple-system,BlinkMacSystemFont,sans-serif;">Perspective</div>'
    )

    NEW_PERSPECTIVE_SENTINEL = "+ New Perspective"

    def _dropdown_options(extras=None):
        opts = sorted(list(perspectives) + (extras or []))
        return opts + [NEW_PERSPECTIVE_SENTINEL]

    perspective_dropdown = widgets.Dropdown(
        options=_dropdown_options(),
        description="",
        layout=widgets.Layout(width="200px"),
    )

    new_name_input = widgets.Text(
        placeholder="Perspective name",
        layout=widgets.Layout(width="180px", display="none"),
    )
    confirm_btn = widgets.Button(
        description="OK",
        button_style="success",
        layout=widgets.Layout(width="50px", display="none"),
    )
    cancel_btn = widgets.Button(
        description="✕",
        layout=widgets.Layout(width="36px", display="none"),
    )

    header = widgets.HBox(
        [
            title_label,
            perspective_dropdown,
            new_name_input,
            confirm_btn,
            cancel_btn,
        ],
        layout=widgets.Layout(
            justify_content="flex-start",
            align_items="center",
            gap="8px",
            margin="0 0 16px 0",
        ),
    )

    # -----------------------------
    # TREE
    # -----------------------------
    icon_map = {"columns": "▯", "measures": "∑", "hierarchies": "≣"}
    table_cbs = {}
    child_cbs_map = {}
    table_summary_labels = {}
    sections = []
    table_icon = "⊞"  # "☷" ≣ ≡
    expand_arrow = "▶"
    collapse_arrow = "▼"
    gray_color = "#999"

    # Selection state icons (orange circles)
    _icon_style = "color:#FF9500; display:inline-block; width:30px; text-align:center; vertical-align:middle;"
    ICON_ALL = (
        f'<span style="{_icon_style} font-size:30px;" title="All selected">●</span>'
    )
    ICON_SOME = f'<span style="{_icon_style} font-size:18px;" title="Partially selected">◐</span>'
    ICON_NONE = (
        f'<span style="{_icon_style} font-size:18px;" title="None selected">○</span>'
    )

    # Global summary
    total_tables = len(metadata)
    _summary_style = (
        "font-size:13px; color:#666; "
        "font-family:-apple-system,BlinkMacSystemFont,sans-serif;"
    )
    summary_label = widgets.HTML(
        value=f'<span style="{_summary_style}">0/{total_tables} tables</span>',
    )

    def _update_global_summary():
        selected_tables = sum(
            1 for t in metadata if any(cb.value for cb, _, _ in child_cbs_map[t])
        )
        summary_label.value = (
            f'<span style="{_summary_style}">'
            f"{selected_tables}/{total_tables} tables</span>"
        )

    for table_name, table_data in metadata.items():
        table_cb = widgets.Checkbox(
            value=False,
            indent=False,
            layout=widgets.Layout(width="22px", margin="0"),
        )
        status_icon = widgets.HTML(
            value=ICON_NONE,
            layout=widgets.Layout(width="16px", margin="0 0 0 -4px"),
        )

        expand_btn = widgets.Button(
            description=expand_arrow,
            layout=widgets.Layout(width="32px", border="none"),
        )
        expand_btn.style.button_color = "transparent"
        expand_btn.style.font_weight = "500"

        table_color = gray_color if table_name in hidden_tables else "inherit"
        table_label = widgets.HTML(
            value=(
                f'<span style="font-size:20px; vertical-align:middle; color:{table_color};">{table_icon}</span>'
                f' <span style="font-size:14px; font-weight:500; vertical-align:middle; color:{table_color};">{table_name}</span>'
            ),
        )

        # Per-table summary label (x/y columns, x/y measures, x/y hierarchies)
        n_cols = len(table_data["columns"])
        n_meas = len(table_data["measures"])
        n_hier = len(table_data["hierarchies"])
        table_summary = widgets.HTML(
            value=(
                f'<span style="font-size:12px; color:#888; margin-left:8px;">'
                f"0/{n_cols} columns, 0/{n_meas} measures, 0/{n_hier} hierarchies</span>"
            ),
        )
        table_summary_labels[table_name] = table_summary

        children_box = widgets.VBox(
            layout=widgets.Layout(display="none", margin="0 0 0 32px"),
        )

        child_list = []
        child_widget_list = []
        for obj_type in ["columns", "measures", "hierarchies"]:
            icon = icon_map[obj_type]
            for obj_name in table_data[obj_type]:
                is_hidden = (
                    table_name in hidden_tables
                    or (table_name, obj_type, obj_name) in hidden_objects
                )
                cb = widgets.Checkbox(
                    value=False,
                    indent=False,
                    layout=widgets.Layout(width="22px", margin="0"),
                )
                label_color = gray_color if is_hidden else "inherit"
                label = widgets.HTML(
                    value=(
                        f'<span style="font-size:16px; color:{label_color};'
                        f' vertical-align:middle;">'
                        f"{icon} {obj_name}</span>"
                    ),
                )
                row = widgets.HBox(
                    [cb, label],
                    layout=widgets.Layout(align_items="center", gap="4px"),
                )
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

        # Link table checkbox ↔ children
        def make_linker(t_cb, s_icon, c_list, tbl_name, tbl_summary):
            updating = [False]
            tbl_data = metadata[tbl_name]
            totals = {
                "columns": len(tbl_data["columns"]),
                "measures": len(tbl_data["measures"]),
                "hierarchies": len(tbl_data["hierarchies"]),
            }

            def _update_icon():
                vals = [cb.value for cb, _, _ in c_list]
                if all(vals):
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
                    f'<span style="font-size:12px; color:#888; margin-left:8px;">'
                    f'{counts["columns"]}/{totals["columns"]} columns, '
                    f'{counts["measures"]}/{totals["measures"]} measures, '
                    f'{counts["hierarchies"]}/{totals["hierarchies"]} hierarchies</span>'
                )

            def on_table(change):
                if updating[0]:
                    return
                updating[0] = True
                for cb, _, _ in c_list:
                    cb.value = change["new"]
                _update_icon()
                _update_table_summary()
                _update_global_summary()
                updating[0] = False

            def on_child(_change):
                if updating[0]:
                    return
                updating[0] = True
                vals = [cb.value for cb, _, _ in c_list]
                if all(vals):
                    t_cb.value = True
                elif any(vals):
                    t_cb.value = True
                else:
                    t_cb.value = False
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

        table_cbs[table_name] = table_cb
        child_cbs_map[table_name] = child_list

    tree_container = widgets.VBox(
        sections,
        layout=widgets.Layout(max_height="500px", overflow_y="auto"),
    )

    # -----------------------------
    # LOAD PERSPECTIVE STATE
    # -----------------------------
    def load_perspective(perspective_name):
        """Set checkbox states to match current perspective membership."""
        members = perspective_members.get(perspective_name, {})
        for table_name in metadata:
            tbl_members = members.get(table_name, {})
            c_list = child_cbs_map[table_name]
            for cb, obj_type, obj_name in c_list:
                cb.value = obj_name in tbl_members.get(obj_type, set())
            # Table checkbox is auto-updated by the linker observers

    _previous_perspective = [perspectives[0] if perspectives else None]

    def on_perspective_change(change):
        if change["new"] == NEW_PERSPECTIVE_SENTINEL:
            new_name_input.layout.display = "inline-flex"
            confirm_btn.layout.display = "inline-flex"
            cancel_btn.layout.display = "inline-flex"
        else:
            if change["new"]:
                _previous_perspective[0] = change["new"]
                load_perspective(change["new"])

    perspective_dropdown.observe(on_perspective_change, names="value")

    # Set initial state
    if perspectives:
        load_perspective(perspectives[0])

    # -----------------------------
    # ACTION BUTTONS
    # -----------------------------
    delete_btn = widgets.Button(description="Delete", button_style="danger")
    save_btn = widgets.Button(description="Save", button_style="primary")
    button_row = widgets.HBox(
        [delete_btn, save_btn],
        layout=widgets.Layout(
            justify_content="flex-end",
            gap="10px",
            margin="16px 0 0 0",
        ),
    )

    # -----------------------------
    # PYTHON CALLBACK
    # -----------------------------
    def execute_action(data):
        try:
            with connect_semantic_model(
                dataset=dataset, workspace=workspace, readonly=False
            ) as tom:
                perspective_name = data["perspective"]
                action = data["action"]

                if action == "create":
                    tom.add_perspective(perspective_name)
                    perspective_members[perspective_name] = {}
                    show_status(f"Created perspective '{perspective_name}'.", "#34c759")

                elif action == "delete":
                    p = tom.model.Perspectives[perspective_name]
                    tom.remove_object(object=p)
                    perspective_members.pop(perspective_name, None)
                    show_status(f"Deleted perspective '{perspective_name}'.", "#34c759")

                elif action == "save":
                    p = tom.model.Perspectives.Find(perspective_name)
                    if not p:
                        p = tom.add_perspective(perspective_name)

                    # Clear existing perspective membership
                    p.PerspectiveTables.Clear()

                    selected = data.get("selected", [])
                    whole_tables = [
                        s.get("table") for s in selected if s.get("type") == "table"
                    ]
                    for t in whole_tables:
                        tom.add_to_perspective(
                            object=tom.model.Tables[t],
                            perspective_name=perspective_name,
                        )

                    for s in selected:
                        obj_name = s.get("name")
                        obj_type = s.get("type")
                        table = s.get("table")

                        if obj_type == "columns":
                            obj = tom.model.Tables[table].Columns[obj_name]
                            tom.add_to_perspective(
                                object=obj, perspective_name=perspective_name
                            )
                        elif obj_type == "measures":
                            obj = tom.model.Tables[table].Measures[obj_name]
                            tom.add_to_perspective(
                                object=obj, perspective_name=perspective_name
                            )
                        elif obj_type == "hierarchies":
                            obj = tom.model.Tables[table].Hierarchies[obj_name]
                            tom.add_to_perspective(
                                object=obj, perspective_name=perspective_name
                            )

                    show_status(f"Saved perspective '{perspective_name}'.", "#34c759")

                    # Update local cache from current checkbox state
                    new_members = {}
                    selected = data.get("selected", [])
                    for s in selected:
                        tbl = s.get("table")
                        if tbl not in new_members:
                            new_members[tbl] = {
                                "columns": set(),
                                "measures": set(),
                                "hierarchies": set(),
                            }
                        if s.get("type") == "table":
                            new_members[tbl] = {
                                "columns": set(metadata[tbl]["columns"]),
                                "measures": set(metadata[tbl]["measures"]),
                                "hierarchies": set(metadata[tbl]["hierarchies"]),
                            }
                        elif s.get("type") in ("columns", "measures", "hierarchies"):
                            new_members[tbl][s["type"]].add(s["name"])
                    perspective_members[perspective_name] = new_members

        except Exception as e:
            show_status(f"Error: {e}", "#ff3b30")

    # -----------------------------
    # BUTTON HANDLERS
    # -----------------------------
    def on_save(_):
        perspective_name = perspective_dropdown.value
        if not perspective_name:
            show_status("No perspective selected.", "#ff3b30")
            return

        selected = []
        for table_name in metadata:
            c_list = child_cbs_map[table_name]
            checked = [(cb, t, n) for cb, t, n in c_list if cb.value]

            if len(checked) == len(c_list) and len(c_list) > 0:
                selected.append({"table": table_name, "type": "table"})
            elif checked:
                for _, obj_type, obj_name in checked:
                    selected.append(
                        {"table": table_name, "type": obj_type, "name": obj_name}
                    )

        execute_action(
            {"action": "save", "perspective": perspective_name, "selected": selected}
        )

    # -- Delete confirmation dialog --
    confirm_delete_label = widgets.HTML()
    confirm_delete_yes = widgets.Button(
        description="Yes, Delete",
        button_style="danger",
        layout=widgets.Layout(width="110px"),
    )
    confirm_delete_no = widgets.Button(
        description="Cancel",
        layout=widgets.Layout(width="80px"),
    )
    confirm_delete_box = widgets.HBox(
        [
            confirm_delete_label,
            confirm_delete_yes,
            confirm_delete_no,
        ],
        layout=widgets.Layout(
            display="none",
            align_items="center",
            gap="10px",
            padding="10px 14px",
            margin="8px 0",
            border="1px solid #ff3b30",
            border_radius="8px",
        ),
    )

    def _hide_confirm_delete():
        confirm_delete_box.layout.display = "none"

    def on_delete(_):
        perspective_name = perspective_dropdown.value
        if not perspective_name:
            show_status("No perspective selected.", "#ff3b30")
            return
        confirm_delete_label.value = (
            f'<span style="font-size:14px; '
            f'font-family:-apple-system,BlinkMacSystemFont,sans-serif;">'
            f'Are you sure you want to delete <b>{perspective_name}</b>?</span>'
        )
        confirm_delete_box.layout.display = "flex"

    def on_confirm_delete(_):
        _hide_confirm_delete()
        perspective_name = perspective_dropdown.value
        execute_action({"action": "delete", "perspective": perspective_name})
        perspectives.remove(perspective_name)
        perspective_dropdown.options = _dropdown_options()
        real_opts = [
            o for o in perspective_dropdown.options if o != NEW_PERSPECTIVE_SENTINEL
        ]
        if real_opts:
            perspective_dropdown.value = real_opts[0]

    def on_cancel_delete(_):
        _hide_confirm_delete()

    confirm_delete_yes.on_click(on_confirm_delete)
    confirm_delete_no.on_click(on_cancel_delete)

    def _hide_create_inputs():
        new_name_input.value = ""
        new_name_input.layout.display = "none"
        confirm_btn.layout.display = "none"
        cancel_btn.layout.display = "none"

    def on_confirm_create(_):
        name = new_name_input.value.strip()
        if not name:
            show_status("Please enter a perspective name.", "#ff3b30")
            return
        execute_action({"action": "create", "perspective": name})
        perspectives.append(name)
        perspective_dropdown.options = _dropdown_options()
        perspective_dropdown.value = name
        _hide_create_inputs()

    def on_cancel_create(_):
        _hide_create_inputs()
        # Restore previous selection
        if _previous_perspective[0]:
            perspective_dropdown.value = _previous_perspective[0]

    save_btn.on_click(on_save)
    delete_btn.on_click(on_delete)
    confirm_btn.on_click(on_confirm_create)
    cancel_btn.on_click(on_cancel_create)

    # -----------------------------
    # DISPLAY
    # -----------------------------
    container = widgets.VBox(
        [header, summary_label, tree_container, button_row, confirm_delete_box, status],
        layout=widgets.Layout(
            width="900px",
            padding="20px",
            border="1px solid #e0e0e0",
        ),
    )
    display(container)
