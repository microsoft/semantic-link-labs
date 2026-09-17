# Model Explorer tab for PBI Fixer.
# Provides a tree view of tables, columns, measures, hierarchies and
# calculation groups with DAX expression preview and editable properties.

import ipywidgets as widgets
import time
import threading

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


def _list_workspace_datasets(workspace):
    """List all semantic model names in a workspace via REST API."""
    from sempy_labs._helper_functions import (
        resolve_workspace_name_and_id,
        _base_api,
    )
    _, ws_id = resolve_workspace_name_and_id(workspace)
    url = f"/v1.0/myorg/groups/{ws_id}/datasets"
    response = _base_api(request=url, client="fabric_sp")
    return [d.get("name") for d in response.json().get("value", []) if d.get("name")]


def _load_model_data_fast(dataset, workspace):
    """
    Load model metadata using sempy.fabric DataFrames (fast REST API).
    Falls back to TOM for hierarchies, calc groups, and table type detection.
    """
    import sempy.fabric as fabric
    from sempy_labs.tom import connect_semantic_model

    model_data = {"tables": {}, "relationships": []}
    try:
        tables_df = fabric.list_tables(dataset, workspace)
        columns_df = fabric.list_columns(dataset, workspace, extended=True)
        measures_df = fabric.list_measures(dataset, workspace)
    except Exception:
        return _load_model_data_tom(dataset, workspace)

    for _, row in tables_df.iterrows():
        t_name = str(row.get("Name", ""))
        if not t_name:
            continue
        model_data["tables"][t_name] = {
            "description": str(row.get("Description", "") or ""),
            "is_hidden": bool(row.get("Is Hidden", False)),
            "type": "Table",
            "columns": {},
            "measures": {},
            "hierarchies": {},
            "calc_items": {},
        }

    for _, row in columns_df.iterrows():
        t_name = str(row.get("Table Name", ""))
        c_name = str(row.get("Column Name", row.get("Name", "")))
        if t_name not in model_data["tables"] or not c_name:
            continue
        model_data["tables"][t_name]["columns"][c_name] = {
            "data_type": str(row.get("Data Type", "")),
            "is_hidden": bool(row.get("Is Hidden", False)),
            "expression": str(row.get("Expression", "")) if row.get("Expression") else None,
            "type": str(row.get("Column Type", row.get("Type", ""))),
            "summarize_by": str(row.get("Summarize By", "")) if row.get("Summarize By") else "",
            "display_folder": str(row.get("Display Folder", "") or ""),
        }

    for _, row in measures_df.iterrows():
        t_name = str(row.get("Table Name", ""))
        m_name = str(row.get("Measure Name", row.get("Name", "")))
        if t_name not in model_data["tables"] or not m_name:
            continue
        model_data["tables"][t_name]["measures"][m_name] = {
            "expression": str(row.get("Measure Expression", row.get("Expression", "")) or ""),
            "format_string": str(row.get("Measure Format String", row.get("Format String", "")) or ""),
            "description": str(row.get("Measure Description", row.get("Description", "")) or ""),
            "display_folder": str(row.get("Measure Display Folder", row.get("Display Folder", "")) or ""),
        }

    # Hierarchies, calc groups, table types require TOM (short connection)
    try:
        with connect_semantic_model(dataset=dataset, readonly=True, workspace=workspace) as tm:
            for table in tm.model.Tables:
                t_name = table.Name
                if t_name not in model_data["tables"]:
                    continue
                t_info = model_data["tables"][t_name]
                try:
                    if table.CalculationGroup is not None:
                        t_info["type"] = "CalculationGroup"
                        for ci in table.CalculationGroup.CalculationItems:
                            t_info["calc_items"][ci.Name] = {
                                "expression": str(ci.Expression) if ci.Expression else "",
                                "ordinal": int(ci.Ordinal) if hasattr(ci, "Ordinal") else 0,
                            }
                        continue
                except Exception:
                    pass
                partitions = []
                try:
                    for p in table.Partitions:
                        if "Calculated" in str(p.SourceType):
                            t_info["type"] = "CalculatedTable"
                        src_type = str(p.SourceType) if hasattr(p, "SourceType") else ""
                        expr = ""
                        try:
                            if hasattr(p, "Source") and hasattr(p.Source, "Expression"):
                                expr = str(p.Source.Expression) if p.Source.Expression else ""
                        except Exception:
                            pass
                        partitions.append({
                            "name": str(p.Name),
                            "source_type": src_type,
                            "expression": expr,
                        })
                except Exception:
                    pass
                t_info["partitions"] = partitions
                for h in table.Hierarchies:
                    t_info["hierarchies"][h.Name] = {"levels": [str(lvl.Name) for lvl in h.Levels]}

                # Augment column metadata from TOM (sort_by, is_key, data_category, etc.)
                for col in table.Columns:
                    c_name = str(col.Name)
                    if c_name in t_info["columns"]:
                        sort_by = ""
                        try:
                            if col.SortByColumn is not None:
                                sort_by = str(col.SortByColumn.Name)
                        except Exception:
                            pass
                        t_info["columns"][c_name]["is_key"] = bool(col.IsKey) if hasattr(col, "IsKey") else False
                        t_info["columns"][c_name]["data_category"] = str(col.DataCategory) if hasattr(col, "DataCategory") and col.DataCategory else ""
                        t_info["columns"][c_name]["sort_by_column"] = sort_by
                        t_info["columns"][c_name]["encoding_hint"] = str(col.EncodingHint) if hasattr(col, "EncodingHint") else ""
                        t_info["columns"][c_name]["is_nullable"] = bool(col.IsNullable) if hasattr(col, "IsNullable") else True
                        t_info["columns"][c_name]["display_folder"] = str(col.DisplayFolder) if hasattr(col, "DisplayFolder") and col.DisplayFolder else t_info["columns"][c_name].get("display_folder", "")

                # Augment measure metadata from TOM
                for m in table.Measures:
                    m_name = str(m.Name)
                    if m_name in t_info["measures"]:
                        t_info["measures"][m_name]["is_hidden"] = bool(m.IsHidden) if hasattr(m, "IsHidden") else False

            # Load relationships with extended props
            for rel in tm.model.Relationships:
                try:
                    model_data["relationships"].append({
                        "from_table": str(rel.FromTable.Name),
                        "from_column": str(rel.FromColumn.Name),
                        "to_table": str(rel.ToTable.Name),
                        "to_column": str(rel.ToColumn.Name),
                        "cross_filter": str(rel.CrossFilteringBehavior) if hasattr(rel, "CrossFilteringBehavior") else "",
                        "is_active": bool(rel.IsActive) if hasattr(rel, "IsActive") else True,
                        "multiplicity": str(rel.FromCardinality) + " → " + str(rel.ToCardinality) if hasattr(rel, "FromCardinality") else "",
                        "security_filtering": str(rel.SecurityFilteringBehavior) if hasattr(rel, "SecurityFilteringBehavior") else "",
                        "rely_on_rri": bool(rel.RelyOnReferentialIntegrity) if hasattr(rel, "RelyOnReferentialIntegrity") else False,
                    })
                except Exception:
                    pass

            # Load perspectives
            for p in tm.model.Perspectives:
                model_data["perspectives"].append(str(p.Name))

            # Load model properties
            model_data["model_properties"] = {
                "compatibility_level": str(tm.model.Model.Database.CompatibilityLevel) if hasattr(tm.model.Model, "Database") else "",
                "default_mode": str(tm.model.DefaultMode) if hasattr(tm.model, "DefaultMode") else "",
            }
    except Exception:
        pass

    return model_data


def _load_model_data_tom(dataset, workspace):
    """Fallback: load everything via TOM (slower but complete)."""
    from sempy_labs.tom import connect_semantic_model

    model_data = {"tables": {}, "relationships": [], "perspectives": []}
    with connect_semantic_model(dataset=dataset, readonly=True, workspace=workspace) as tm:
        for table in tm.model.Tables:
            t_name = table.Name
            t_info = {
                "description": str(table.Description or ""),
                "is_hidden": bool(table.IsHidden),
                "type": "Table",
                "columns": {},
                "measures": {},
                "hierarchies": {},
                "calc_items": {},
            }
            is_calc_group = False
            try:
                if table.CalculationGroup is not None:
                    is_calc_group = True
                    t_info["type"] = "CalculationGroup"
            except Exception:
                pass
            if not is_calc_group:
                partitions = []
                try:
                    for p in table.Partitions:
                        if "Calculated" in str(p.SourceType):
                            t_info["type"] = "CalculatedTable"
                        src_type = str(p.SourceType) if hasattr(p, "SourceType") else ""
                        expr = ""
                        try:
                            if hasattr(p, "Source") and hasattr(p.Source, "Expression"):
                                expr = str(p.Source.Expression) if p.Source.Expression else ""
                        except Exception:
                            pass
                        partitions.append({
                            "name": str(p.Name),
                            "source_type": src_type,
                            "expression": expr,
                        })
                except Exception:
                    pass
                t_info["partitions"] = partitions
            for col in table.Columns:
                sort_by = ""
                try:
                    if col.SortByColumn is not None:
                        sort_by = str(col.SortByColumn.Name)
                except Exception:
                    pass
                t_info["columns"][col.Name] = {
                    "data_type": str(col.DataType) if hasattr(col, "DataType") else "",
                    "is_hidden": bool(col.IsHidden),
                    "expression": str(col.Expression) if hasattr(col, "Expression") and col.Expression else None,
                    "type": str(col.Type) if hasattr(col, "Type") else "",
                    "summarize_by": str(col.SummarizeBy) if hasattr(col, "SummarizeBy") else "",
                    "is_key": bool(col.IsKey) if hasattr(col, "IsKey") else False,
                    "data_category": str(col.DataCategory) if hasattr(col, "DataCategory") and col.DataCategory else "",
                    "sort_by_column": sort_by,
                    "encoding_hint": str(col.EncodingHint) if hasattr(col, "EncodingHint") else "",
                    "is_nullable": bool(col.IsNullable) if hasattr(col, "IsNullable") else True,
                    "display_folder": str(col.DisplayFolder) if hasattr(col, "DisplayFolder") and col.DisplayFolder else "",
                }
            for m in table.Measures:
                t_info["measures"][m.Name] = {
                    "expression": str(m.Expression) if m.Expression else "",
                    "format_string": str(m.FormatString) if m.FormatString else "",
                    "description": str(m.Description) if m.Description else "",
                    "display_folder": str(m.DisplayFolder) if m.DisplayFolder else "",
                    "is_hidden": bool(m.IsHidden) if hasattr(m, "IsHidden") else False,
                }
            for h in table.Hierarchies:
                t_info["hierarchies"][h.Name] = {"levels": [str(lvl.Name) for lvl in h.Levels]}
            if is_calc_group:
                try:
                    for ci in table.CalculationGroup.CalculationItems:
                        t_info["calc_items"][ci.Name] = {
                            "expression": str(ci.Expression) if ci.Expression else "",
                            "ordinal": int(ci.Ordinal) if hasattr(ci, "Ordinal") else 0,
                        }
                except Exception:
                    pass
            model_data["tables"][t_name] = t_info

        # Load model properties
        model_data["model_properties"] = {
            "compatibility_level": str(tm.model.Model.Database.CompatibilityLevel) if hasattr(tm.model.Model, "Database") else "",
            "default_mode": str(tm.model.DefaultMode) if hasattr(tm.model, "DefaultMode") else "",
        }

        # Load relationships with extended props
        for rel in tm.model.Relationships:
            try:
                model_data["relationships"].append({
                    "from_table": str(rel.FromTable.Name),
                    "from_column": str(rel.FromColumn.Name),
                    "to_table": str(rel.ToTable.Name),
                    "to_column": str(rel.ToColumn.Name),
                    "cross_filter": str(rel.CrossFilteringBehavior) if hasattr(rel, "CrossFilteringBehavior") else "",
                    "is_active": bool(rel.IsActive) if hasattr(rel, "IsActive") else True,
                    "multiplicity": str(rel.FromCardinality) + " \u2192 " + str(rel.ToCardinality) if hasattr(rel, "FromCardinality") else "",
                    "security_filtering": str(rel.SecurityFilteringBehavior) if hasattr(rel, "SecurityFilteringBehavior") else "",
                    "rely_on_rri": bool(rel.RelyOnReferentialIntegrity) if hasattr(rel, "RelyOnReferentialIntegrity") else False,
                })
            except Exception:
                pass

        # Load perspectives
        for p in tm.model.Perspectives:
            model_data["perspectives"].append(str(p.Name))
    return model_data


def _table_summary(t):
    """Return total child count for a table."""
    return str(len(t.get("columns", {})) + len(t.get("measures", {})) + len(t.get("hierarchies", {})) + len(t.get("calc_items", {})))


def _build_measures_with_folders(measures, table_key, base_indent, expanded, pending_changes):
    """Build tree items for measures grouped by display folder (nested).
    Returns list of (indent, icon, label, key) tuples."""
    items = []
    # Group measures by display folder path
    folders = {}  # folder_path -> [measure_names]
    no_folder = []
    for mn in sorted(measures):
        df = measures[mn].get("display_folder", "")
        if df:
            folders.setdefault(df, []).append(mn)
        else:
            no_folder.append(mn)

    # Measures without folder — directly under table
    for mn in no_folder:
        mk = f"measure:{table_key}:{mn}"
        pfx = "\u270f " if mk in pending_changes else ""
        items.append((base_indent, "measure", f"{pfx}{mn}", mk))

    # Collect all unique folder paths and build nested structure
    # e.g. "Orders\PY" → ["Orders", "Orders\PY"]
    all_folder_paths = set()
    for folder_path in folders:
        parts = folder_path.replace("/", "\\").split("\\")
        for i in range(len(parts)):
            all_folder_paths.add("\\".join(parts[: i + 1]))

    # Count measures per folder (including nested — leaf folders only have direct measures)
    def _count_under(prefix):
        """Count measures in this folder and all subfolders."""
        total = 0
        for fp, ms in folders.items():
            fp_norm = fp.replace("/", "\\")
            if fp_norm == prefix or fp_norm.startswith(prefix + "\\"):
                total += len(ms)
        return total

    # Build sorted unique folder paths and emit tree nodes
    emitted_folders = set()

    for folder_path in sorted(folders):
        parts = folder_path.replace("/", "\\").split("\\")
        # Emit ancestor folders that haven't been emitted yet
        for depth in range(len(parts)):
            ancestor = "\\".join(parts[: depth + 1])
            if ancestor not in emitted_folders:
                emitted_folders.add(ancestor)
                folder_key = f"folder:{table_key}:{ancestor}"
                is_exp = folder_key in expanded
                marker = EXPANDED if is_exp else COLLAPSED
                count = _count_under(ancestor)
                label_name = parts[depth]
                items.append((base_indent + depth, "folder", f"{marker} {label_name}  [{count}]", folder_key))

        # Emit measures in this folder (only if deepest folder is expanded)
        deepest_key = f"folder:{table_key}:{folder_path.replace('/', chr(92))}"
        # Check if all ancestors + this folder are expanded
        all_expanded = True
        for depth in range(len(parts)):
            ancestor = "\\".join(parts[: depth + 1])
            if f"folder:{table_key}:{ancestor}" not in expanded:
                all_expanded = False
                break
        if all_expanded:
            for mn in sorted(folders[folder_path]):
                mk = f"measure:{table_key}:{mn}"
                pfx = "\u270f " if mk in pending_changes else ""
                items.append((base_indent + len(parts), "measure", f"{pfx}{mn}", mk))

    return items


def _build_columns_with_folders(columns, table_key, base_indent, expanded, pending_changes):
    """Build tree items for columns grouped by display folder (nested).
    Returns list of (indent, icon, label, key) tuples."""
    items = []
    folders = {}
    no_folder = []
    for cn in sorted(columns):
        df = columns[cn].get("display_folder", "")
        if df:
            # Handle semicolon-separated multi-folder assignments — use the first folder
            first_folder = df.split(";")[0].strip()
            folders.setdefault(first_folder, []).append(cn)
        else:
            no_folder.append(cn)

    # Columns without folder — directly under table
    for cn in no_folder:
        c = columns[cn]
        hidden = " (hidden)" if c["is_hidden"] else ""
        ck = f"column:{table_key}:{cn}"
        pfx = "\u270f " if ck in pending_changes else ""
        items.append((base_indent, "column", f"{pfx}{cn} [{c['data_type']}]{hidden}", ck))

    # Build nested folder structure
    all_folder_paths = set()
    for folder_path in folders:
        parts = folder_path.replace("/", "\\").split("\\")
        for i in range(len(parts)):
            all_folder_paths.add("\\".join(parts[: i + 1]))

    def _count_under(prefix):
        total = 0
        for fp, cs in folders.items():
            fp_norm = fp.replace("/", "\\")
            if fp_norm == prefix or fp_norm.startswith(prefix + "\\"):
                total += len(cs)
        return total

    emitted_folders = set()
    for folder_path in sorted(folders):
        parts = folder_path.replace("/", "\\").split("\\")
        for depth in range(len(parts)):
            ancestor = "\\".join(parts[: depth + 1])
            if ancestor not in emitted_folders:
                emitted_folders.add(ancestor)
                folder_key = f"colfolder:{table_key}:{ancestor}"
                is_exp = folder_key in expanded
                marker = EXPANDED if is_exp else COLLAPSED
                count = _count_under(ancestor)
                label_name = parts[depth]
                items.append((base_indent + depth, "folder", f"{marker} {label_name}  [{count}]", folder_key))

        deepest_key = f"colfolder:{table_key}:{folder_path.replace('/', chr(92))}"
        all_expanded = True
        for depth in range(len(parts)):
            ancestor = "\\".join(parts[: depth + 1])
            if f"colfolder:{table_key}:{ancestor}" not in expanded:
                all_expanded = False
                break
        if all_expanded:
            for cn in sorted(folders[folder_path]):
                c = columns[cn]
                hidden = " (hidden)" if c["is_hidden"] else ""
                ck = f"column:{table_key}:{cn}"
                pfx = "\u270f " if ck in pending_changes else ""
                items.append((base_indent + len(parts), "column", f"{pfx}{cn} [{c['data_type']}]{hidden}", ck))

    return items


def _build_tree(model_data, expanded_tables, scan_results=None, pending_changes=None):
    """Build tree items, optionally annotating with scan findings."""
    scan_results = scan_results or {}
    pending_changes = pending_changes or {}
    items = []
    models = model_data.get("models", {})
    if models:
        for m_name in sorted(models):
            m_tables = models[m_name]
            is_model_expanded = m_name in expanded_tables
            marker = EXPANDED if is_model_expanded else COLLAPSED
            t_count = len(m_tables)
            model_findings = scan_results.get(f"model:{m_name}", 0)
            badge = f" \u26a0\ufe0f{model_findings}" if model_findings > 0 else ""
            items.append((0, "model", f"{marker} {m_name}  [{t_count} tables]{badge}", f"model:{m_name}"))
            if not is_model_expanded:
                continue
            for t_name in sorted(m_tables):
                t = m_tables[t_name]
                icon = "calc_group" if t["type"] == "CalculationGroup" else "table"
                full_key = f"{m_name}\x1f{t_name}"
                is_expanded = full_key in expanded_tables
                t_marker = EXPANDED if is_expanded else COLLAPSED
                suffix = " (hidden)" if t["is_hidden"] else ""
                summary = _table_summary(t)
                items.append((1, icon, f"{t_marker} {t_name}{suffix}  [{summary}]", f"table:{full_key}"))
                if not is_expanded:
                    continue
                items.extend(_build_measures_with_folders(t["measures"], full_key, 2, expanded_tables, pending_changes))
                items.extend(_build_columns_with_folders(t["columns"], full_key, 2, expanded_tables, pending_changes))
                for hn in sorted(t["hierarchies"]):
                    lvl_str = " \u2192 ".join(t["hierarchies"][hn]["levels"])
                    items.append((2, "hierarchy", f"{hn}  ({lvl_str})", f"hierarchy:{full_key}:{hn}"))
                for ci_name in sorted(t.get("calc_items", {}), key=lambda n: t["calc_items"][n]["ordinal"]):
                    items.append((2, "calc_item", ci_name, f"calc_item:{full_key}:{ci_name}"))
                # Partitions
                for pt in t.get("partitions", []):
                    items.append((2, "partition", f"{pt['name']} ({pt['source_type']})", f"partition:{full_key}:{pt['name']}"))
            # Relationships for this model
            m_rels = model_data.get("model_relationships", {}).get(m_name, [])
            if m_rels:
                rel_key = f"rels:{m_name}"
                is_rels_exp = rel_key in expanded_tables
                r_marker = EXPANDED if is_rels_exp else COLLAPSED
                items.append((1, "relationship", f"{r_marker} Relationships  [{len(m_rels)}]", rel_key))
                if is_rels_exp:
                    for i, rel in enumerate(m_rels):
                        active = "" if rel.get("is_active", True) else " (inactive)"
                        label = f"{rel['from_table']}[{rel['from_column']}] \u2194 {rel['to_table']}[{rel['to_column']}]{active}"
                        items.append((2, "relationship", label, f"rel:{m_name}:{i}"))
            # Perspectives for this model
            m_persps = model_data.get("model_perspectives", {}).get(m_name, [])
            if m_persps:
                items.append((1, "folder", f"Perspectives  [{len(m_persps)}]", f"persps:{m_name}"))
                for pname in sorted(m_persps):
                    items.append((2, "calc_item", pname, f"persp:{m_name}:{pname}"))
    else:
        # Single model: show model node (always visible for refresh/properties)
        ds_input = model_data.get("_dataset_name", "Model")
        props = model_data.get("model_properties", {})
        compat = props.get("compatibility_level", "")
        mode = props.get("default_mode", "")
        prop_str = f" ({mode}, CL {compat})" if compat else ""
        t_count = len(model_data.get("tables", {}))
        is_model_exp = ds_input in expanded_tables
        marker = EXPANDED if is_model_exp else COLLAPSED
        items.append((0, "model", f"{marker} {ds_input}{prop_str}  [{t_count} tables]", f"model:{ds_input}"))
        if not is_model_exp:
            pass  # collapsed
        else:
          for t_name in sorted(model_data["tables"]):
            t = model_data["tables"][t_name]
            icon = "calc_group" if t["type"] == "CalculationGroup" else "table"
            is_expanded = t_name in expanded_tables
            marker = EXPANDED if is_expanded else COLLAPSED
            suffix = " (hidden)" if t["is_hidden"] else ""
            summary = _table_summary(t)
            items.append((1, icon, f"{marker} {t_name}{suffix}  [{summary}]", f"table:{t_name}"))
            if not is_expanded:
                continue
            items.extend(_build_measures_with_folders(t["measures"], t_name, 2, expanded_tables, pending_changes))
            items.extend(_build_columns_with_folders(t["columns"], t_name, 2, expanded_tables, pending_changes))
            for hn in sorted(t["hierarchies"]):
                lvl_str = " \u2192 ".join(t["hierarchies"][hn]["levels"])
                items.append((2, "hierarchy", f"{hn}  ({lvl_str})", f"hierarchy:{t_name}:{hn}"))
            for ci_name in sorted(t.get("calc_items", {}), key=lambda n: t["calc_items"][n]["ordinal"]):
                items.append((2, "calc_item", ci_name, f"calc_item:{t_name}:{ci_name}"))
            # Partitions
            for pt in t.get("partitions", []):
                items.append((2, "partition", f"{pt['name']} ({pt['source_type']})", f"partition:{t_name}:{pt['name']}"))
          # Relationships (single model) — under model node
          rels = model_data.get("relationships", [])
          if rels:
            rel_key = "rels:_single"
            is_rels_exp = rel_key in expanded_tables
            r_marker = EXPANDED if is_rels_exp else COLLAPSED
            items.append((1, "relationship", f"{r_marker} Relationships  [{len(rels)}]", rel_key))
            if is_rels_exp:
                for i, rel in enumerate(rels):
                    active = "" if rel.get("is_active", True) else " (inactive)"
                    label = f"{rel['from_table']}[{rel['from_column']}] \u2194 {rel['to_table']}[{rel['to_column']}]{active}"
                    items.append((2, "relationship", label, f"rel:_single:{i}"))
          # Perspectives (single model) — under model node
          persps = model_data.get("perspectives", [])
          if persps:
            items.append((1, "folder", f"Perspectives  [{len(persps)}]", "persps:_single"))
            for pname in sorted(persps):
                items.append((2, "calc_item", pname, f"persp:_single:{pname}"))
    return build_tree_items(items)


def _resolve_table(model_data, table_key):
    """Resolve a table key to its data dict. Handles both single and multi-model keys."""
    if "\x1f" in table_key:
        m_name, t_name = table_key.split("\x1f", 1)
        return model_data.get("models", {}).get(m_name, {}).get(t_name)
    return model_data.get("tables", {}).get(table_key)


def _get_preview_text(model_data, key):
    parts = key.split(":", 2)
    node_type = parts[0]
    if node_type in ("rels",):
        return ""
    if node_type == "model":
        # Show model properties
        props = model_data.get("model_properties", {})
        if not props:
            for m_data in model_data.get("models", {}).values():
                break  # multi-model: can't show single model props here
        lines = []
        for k, v in props.items():
            lines.append(f"{k}: {v}")
        return "\n".join(lines) if lines else ""
    if node_type == "partition":
        # Show M script for partition
        raw_table = parts[1] if len(parts) > 1 else ""
        p_name = parts[2] if len(parts) > 2 else ""
        t = _resolve_table(model_data, raw_table)
        if t:
            for pt in t.get("partitions", []):
                if pt["name"] == p_name:
                    return pt.get("expression", "")
        return ""
    if node_type == "rel":
        # Show relationship details
        m_name = parts[1] if len(parts) > 1 else ""
        idx = int(parts[2]) if len(parts) > 2 else -1
        if m_name == "_single":
            rels = model_data.get("relationships", [])
        else:
            rels = model_data.get("model_relationships", {}).get(m_name, [])
        if 0 <= idx < len(rels):
            r = rels[idx]
            return (
                f"From: '{r['from_table']}'[{r['from_column']}]\n"
                f"To:   '{r['to_table']}'[{r['to_column']}]\n"
                f"Multiplicity: {r.get('multiplicity', '')}\n"
                f"Cross-filter: {r.get('cross_filter', '')}\n"
                f"Security filtering: {r.get('security_filtering', '')}\n"
                f"Active: {r.get('is_active', True)}\n"
                f"Rely on RRI: {r.get('rely_on_rri', False)}"
            )
        return ""
    if node_type == "measure":
        t = _resolve_table(model_data, parts[1])
        return t["measures"].get(parts[2], {}).get("expression", "") if t else ""
    if node_type == "column":
        t = _resolve_table(model_data, parts[1])
        return (t["columns"].get(parts[2], {}).get("expression") or "") if t else ""
    if node_type == "calc_item":
        t = _resolve_table(model_data, parts[1])
        return t["calc_items"].get(parts[2], {}).get("expression", "") if t else ""
    return ""


def model_explorer_tab(workspace_input=None, report_input=None, fixer_callbacks=None):
    """Build the Model Explorer tab widget."""
    _model_data = {}
    _key_map = {}
    _expanded = set()
    _current_key = [None]
    _selected_keys = []  # all currently selected keys for fixer actions
    _scan_results = {}  # key -> violation count

    load_btn = widgets.Button(description="Load Model", button_style="primary", layout=widgets.Layout(width="110px"))
    expand_btn = widgets.Button(description="Expand All", layout=widgets.Layout(width="100px"))
    collapse_btn = widgets.Button(description="Collapse All", layout=widgets.Layout(width="100px"))
    scan_btn = widgets.Button(description="\U0001F50D Scan", layout=widgets.Layout(width="110px"))

    fixer_callbacks = fixer_callbacks or {}
    fixer_dropdown = widgets.Dropdown(
        options=["Select action..."] + list(fixer_callbacks.keys()),
        value="Select action...",
        layout=widgets.Layout(width="208px"),
    )
    run_action_btn = widgets.Button(
        description="\u26A1 Run",
        button_style="danger",
        layout=widgets.Layout(width="100px"),
    )

    conn_status = status_html()
    nav_row = widgets.HBox(
        [load_btn, expand_btn, collapse_btn, conn_status],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 4px 0"),
    )
    action_row = widgets.HBox(
        [scan_btn, fixer_dropdown, run_action_btn],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 8px 0"),
    )

    tree = widgets.SelectMultiple(options=[], rows=18, layout=widgets.Layout(width="100%", height="420px", font_family="monospace"))
    tree_search = widgets.Text(placeholder="\U0001F50D Filter tree\u2026", layout=widgets.Layout(width="100%"))
    _all_tree_options = []  # unfiltered options

    def _refresh_tree():
        nonlocal _key_map
        options, _key_map = _build_tree(_model_data, _expanded, _scan_results, _pending_changes)
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
            # Include parent nodes (model/table/folder) above any matching item
            all_opts = _all_tree_options
            matched = set()
            for i, o in enumerate(all_opts):
                if query in o.lower():
                    matched.add(i)
                    # Walk backwards to find parent nodes (lower indent level)
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

    # -- expression panel --
    preview = widgets.Textarea(value="", placeholder="Select a measure to view its DAX expression.", disabled=True, layout=widgets.Layout(width="100%", height="160px", font_family="monospace"))

    # Table data preview (HTML, shown when a table node is selected)
    table_preview_html = widgets.HTML(value="", layout=widgets.Layout(width="100%", display="none"))
    table_row_dropdown = widgets.Dropdown(
        options=[("Top 10", 10), ("Top 100", 100), ("All", 0)],
        value=10,
        layout=widgets.Layout(width="100px", display="none"),
    )
    _table_preview_name = [None]  # track which table is previewed
    _table_preview_ds = [None]    # track which dataset

    def _load_table_preview(ds_name, table_name, top_n=10):
        """Load top N rows of a table via DAX and render as HTML."""
        try:
            import sempy.fabric as fabric
            ws = workspace_input.value.strip() if workspace_input else None
            ws = ws or None
            if top_n > 0:
                dax = f"EVALUATE TOPN({top_n}, '{table_name}')"
            else:
                dax = f"EVALUATE '{table_name}'"
            df = fabric.evaluate_dax(dataset=ds_name, dax_string=dax, workspace=ws)
            if df is None or len(df) == 0:
                return '<div style="color:#999; font-size:12px;">No rows returned.</div>'
            # Strip table prefix from column names
            df.columns = [c.split("[")[-1].rstrip("]") if "[" in c else c for c in df.columns]
            html = '<div style="overflow-x:auto; max-height:250px; overflow-y:auto;">'
            html += '<table style="border-collapse:collapse; font-size:11px; font-family:monospace; width:100%;">'
            html += '<tr style="background:#f5f5f5; position:sticky; top:0;">'
            for col in df.columns:
                html += f'<th style="padding:3px 6px; border-bottom:2px solid #e0e0e0; text-align:left; white-space:nowrap;">{col}</th>'
            html += '</tr>'
            for _, row in df.iterrows():
                html += '<tr>'
                for col in df.columns:
                    val = row[col]
                    if val is None or (isinstance(val, float) and val != val):
                        val = ""
                    html += f'<td style="padding:2px 6px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{val}</td>'
                html += '</tr>'
            html += f'</table></div>'
            html += f'<div style="font-size:10px; color:#999; margin-top:2px;">{len(df)} row(s) shown</div>'
            return html
        except Exception as e:
            return f'<div style="color:#ff3b30; font-size:12px;">Error: {str(e)[:100]}</div>'

    def _on_table_row_change(change):
        if _table_preview_name[0] and _table_preview_ds[0]:
            table_preview_html.value = '<div style="color:#999; font-size:12px;">Loading\u2026</div>'
            table_preview_html.value = _load_table_preview(_table_preview_ds[0], _table_preview_name[0], change.get("new", 10))

    table_row_dropdown.observe(_on_table_row_change, names="value")
    fmt_long_btn = widgets.Button(description="Format Long", layout=widgets.Layout(width="110px"))
    fmt_short_btn = widgets.Button(description="Format Short", layout=widgets.Layout(width="110px"))

    def _do_format_dax(max_line_length, btn):
        """Format the current DAX expression via daxformatter.com API."""
        expr = preview.value.strip()
        if not expr:
            return
        btn.disabled = True
        orig = btn.description
        btn.description = "Formatting\u2026"
        try:
            import requests
            from sempy_labs._a_lib_info import lib_name, lib_version
            payload = {
                "Dax": [f"x :={expr}"],
                "MaxLineLength": max_line_length,
                "SkipSpaceAfterFunctionName": False,
                "ListSeparator": ",",
                "DecimalSeparator": ".",
            }
            headers = {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Content-Type": "application/json; charset=UTF-8",
                "Host": "daxformatter.azurewebsites.net",
                "CallerApp": lib_name,
                "CallerVersion": lib_version,
            }
            resp = requests.post("https://daxformatter.azurewebsites.net/api/daxformatter/daxtextformatmulti", json=payload, headers=headers)
            result = resp.json()
            if result and result[0].get("formatted"):
                txt = result[0]["formatted"]
                if txt.startswith("x :="):
                    txt = txt[4:]
                if txt.startswith("\r\n"):
                    txt = txt[2:]
                elif txt.startswith("\n"):
                    txt = txt[1:]
                old_val = preview.value
                _suppressing_observe[0] = True
                preview.value = txt
                _suppressing_observe[0] = False
                # Mark as dirty if formatting actually changed the text
                if txt != old_val:
                    _capture_current()
                    _tree_stale[0] = True
                    n = len(_pending_changes)
                    save_btn.description = f"\u26a0\ufe0f {n} unsaved change(s)"
                    save_btn.button_style = "danger"
                    save_btn.disabled = False
                    discard_btn.layout.display = ""
        except Exception:
            pass
        btn.disabled = False
        btn.description = orig

    def on_format_long(_):
        _do_format_dax(0, fmt_long_btn)

    def on_format_short(_):
        _do_format_dax(80, fmt_short_btn)

    fmt_long_btn.on_click(on_format_long)
    fmt_short_btn.on_click(on_format_short)

    copy_ref_btn = widgets.Button(description="📋 Copy Ref", layout=widgets.Layout(width="100px"), disabled=True)
    ref_output = widgets.Text(value="", disabled=True, layout=widgets.Layout(width="250px", display="none"))

    def _get_dax_ref(key):
        """Return DAX reference string for a measure or column key."""
        if not key:
            return ""
        parts = key.split(":", 2)
        node_type = parts[0]
        if node_type == "measure":
            return f"[{parts[2]}]" if len(parts) > 2 else ""
        if node_type == "column":
            raw_table = parts[1] if len(parts) > 1 else ""
            table_name = raw_table.split("\x1f")[-1] if "\x1f" in raw_table else raw_table
            c_name = parts[2] if len(parts) > 2 else ""
            return f"'{table_name}'[{c_name}]"
        return ""

    def on_copy_ref(_):
        """Show DAX reference in a text field for easy copy."""
        ref = _get_dax_ref(_current_key[0])
        if ref:
            ref_output.value = ref
            ref_output.layout.display = ""

    copy_ref_btn.on_click(on_copy_ref)
    format_row = widgets.HBox([fmt_long_btn, fmt_short_btn, copy_ref_btn, ref_output], layout=widgets.Layout(gap="8px", align_items="center"))

    preview_label = widgets.HTML(
        value=f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; font-family:{FONT_FAMILY}; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:2px;">Expression</div>'
    )

    # -- Prep for AI section (shown when model root is selected) --
    _p4ai_label = widgets.HTML(
        value=f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; font-family:{FONT_FAMILY}; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:2px;">🤖 Prep for AI</div>'
    )
    _p4ai_load_btn = widgets.Button(description="🔄 Load", layout=widgets.Layout(width="100px", min_width="100px", height="34px", min_height="34px"))
    _p4ai_save_btn = widgets.Button(description="💾 Save", button_style="success", layout=widgets.Layout(width="100px", min_width="100px", height="34px", min_height="34px"))
    _p4ai_gen_btn = widgets.Button(description="🪄 Generate", button_style="warning", layout=widgets.Layout(width="120px", min_width="120px", height="34px", min_height="34px"))
    _p4ai_append_cb = widgets.Checkbox(value=False, description="Append", indent=False, layout=widgets.Layout(width="100px"))
    _p4ai_status = status_html()
    _p4ai_btn_row = widgets.HBox(
        [_p4ai_load_btn, _p4ai_gen_btn, _p4ai_save_btn, _p4ai_append_cb],
        layout=widgets.Layout(gap="8px", align_items="center", margin="4px 0", min_height="40px"),
    )
    _p4ai_textarea = widgets.Textarea(
        value="", placeholder="Click 🔄 Load to fetch CustomInstructions from the semantic model.",
        layout=widgets.Layout(width="100%", height="280px", font_family="monospace"),
    )
    _p4ai_info = widgets.HTML(value="")
    _p4ai_qna_btn = widgets.Button(
        description="⚡ Enable Q&A", button_style="danger",
        layout=widgets.Layout(width="130px", min_width="130px", height="30px", min_height="30px", display="none"),
    )
    _p4ai_container = widgets.VBox(
        [_p4ai_label, _p4ai_btn_row, _p4ai_status, _p4ai_textarea, _p4ai_info, _p4ai_qna_btn],
        layout=widgets.Layout(display="none", gap="4px"),
    )
    _p4ai_loaded_ds = [None]  # track which dataset was loaded
    _p4ai_selected_model = [None]  # track currently selected model root in tree

    def _on_p4ai_load(_):
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds = _p4ai_selected_model[0]
        if not ds:
            set_status(_p4ai_status, "No model selected in tree.", "#ff3b30")
            return
        _p4ai_load_btn.disabled = True
        _p4ai_load_btn.description = "Loading…"
        set_status(_p4ai_status, "Reading Prep for AI…", GRAY_COLOR)
        try:
            from sempy_labs.semantic_model._Add_PrepForAI import read_prep_for_ai
            result = read_prep_for_ai(dataset=ds, workspace=ws)
            _p4ai_textarea.value = result.get("custom_instructions", "")
            va = result.get("verified_answers", [])
            va_count = len(va) if isinstance(va, list) else 0
            stale = result.get("is_stale", False)
            qna = result.get("qna_enabled")
            info_parts = []
            if qna is False:
                info_parts.append("⚠️ Q&A is disabled")
                _p4ai_qna_btn.layout.display = ""
            elif qna is True:
                info_parts.append("✅ Q&A enabled")
                _p4ai_qna_btn.layout.display = "none"
            else:
                _p4ai_qna_btn.layout.display = "none"
            if va_count:
                info_parts.append(f"{va_count} verified answer(s)")
            if stale:
                info_parts.append("⚠️ schema is stale")
            err = result.get("error")
            if err:
                info_parts.append(f"⚠️ {err}")
            _p4ai_info.value = (
                f'<div style="font-size:11px; color:#888; font-family:{FONT_FAMILY}; margin-top:2px;">'
                f'{" · ".join(info_parts) if info_parts else "No verified answers configured."}</div>'
            )
            _p4ai_loaded_ds[0] = ds
            ci = result.get("custom_instructions", "")
            char_count = len(ci) if ci else 0
            set_status(_p4ai_status, f"Loaded ({char_count} chars)", "#34c759")
        except Exception as e:
            set_status(_p4ai_status, f"Error: {e}", "#ff3b30")
        _p4ai_load_btn.disabled = False
        _p4ai_load_btn.description = "🔄 Load"

    def _on_p4ai_save(_):
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds = _p4ai_loaded_ds[0]
        if not ds:
            set_status(_p4ai_status, "Load first before saving.", "#ff9500")
            return
        instructions = _p4ai_textarea.value.strip()
        if not instructions and not _p4ai_append_cb.value:
            set_status(_p4ai_status, "Instructions are empty. Write something first.", "#ff9500")
            return
        _p4ai_save_btn.disabled = True
        _p4ai_save_btn.description = "Saving…"
        set_status(_p4ai_status, "Saving Prep for AI… this may take up to 60s", GRAY_COLOR)
        try:
            from sempy_labs.semantic_model._Add_PrepForAI import write_prep_for_ai
            import io as _io
            from contextlib import redirect_stdout as _redirect
            buf = _io.StringIO()
            with _redirect(buf):
                write_prep_for_ai(
                    dataset=ds, workspace=ws,
                    instructions=instructions,
                    append=_p4ai_append_cb.value,
                )
            output = buf.getvalue().strip()
            if "successfully" in output.lower() or "green" in output.lower():
                set_status(_p4ai_status, "✓ Saved", "#34c759")
            else:
                set_status(_p4ai_status, output[:80] if output else "✓ Saved", "#34c759")
        except Exception as e:
            set_status(_p4ai_status, f"Error: {e}", "#ff3b30")
        _p4ai_save_btn.disabled = False
        _p4ai_save_btn.description = "💾 Save"

    _p4ai_load_btn.on_click(_on_p4ai_load)
    _p4ai_save_btn.on_click(_on_p4ai_save)

    def _on_p4ai_generate(_):
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds = _p4ai_selected_model[0]
        if not ds:
            set_status(_p4ai_status, "No model selected in tree.", "#ff3b30")
            return
        _p4ai_gen_btn.disabled = True
        _p4ai_gen_btn.description = "Generating…"
        set_status(_p4ai_status, "Analyzing model structure…", GRAY_COLOR)
        try:
            from sempy_labs.semantic_model._Add_PrepForAI import generate_prep_for_ai_text
            text = generate_prep_for_ai_text(dataset=ds, workspace=ws)
            _p4ai_textarea.value = text
            _p4ai_loaded_ds[0] = ds
            set_status(_p4ai_status, f"Generated ({len(text)} chars) — review and 💾 Save", "#34c759")
        except Exception as e:
            set_status(_p4ai_status, f"Error: {e}", "#ff3b30")
        _p4ai_gen_btn.disabled = False
        _p4ai_gen_btn.description = "🪄 Generate"

    _p4ai_gen_btn.on_click(_on_p4ai_generate)

    def _on_p4ai_enable_qna(_):
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds = _p4ai_selected_model[0]
        if not ds:
            set_status(_p4ai_status, "No model selected in tree.", "#ff3b30")
            return
        _p4ai_qna_btn.disabled = True
        _p4ai_qna_btn.description = "Enabling…"
        set_status(_p4ai_status, "Enabling Q&A on model…", GRAY_COLOR)
        try:
            from sempy_labs.semantic_model._Add_PrepForAI import enable_qna
            enable_qna(dataset=ds, workspace=ws)
            _p4ai_qna_btn.layout.display = "none"
            set_status(_p4ai_status, "✅ Q&A enabled successfully", "#34c759")
            # Update info line
            _p4ai_info.value = _p4ai_info.value.replace("⚠️ Q&A is disabled", "✅ Q&A enabled")
        except Exception as e:
            set_status(_p4ai_status, f"Error enabling Q&A: {e}", "#ff3b30")
        _p4ai_qna_btn.disabled = False
        _p4ai_qna_btn.description = "⚡ Enable Q&A"

    _p4ai_qna_btn.on_click(_on_p4ai_enable_qna)

    preview_box = panel_box([preview_label, format_row, table_row_dropdown, preview, table_preview_html, _p4ai_container], flex="1")

    # -- editable properties --
    props_label = widgets.HTML(
        value=f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; font-family:{FONT_FAMILY}; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:2px;">Properties</div>'
    )
    def _prop_input(label_text, width="100%", disabled=False):
        lbl = widgets.HTML(value=f'<span style="font-size:10px; font-weight:600; color:#555; font-family:{FONT_FAMILY};">{label_text}</span>')
        inp = widgets.Text(layout=widgets.Layout(width=width), disabled=disabled)
        row = widgets.VBox([lbl, inp], layout=widgets.Layout(gap="0px", margin="0"))
        return inp, row

    prop_name, prop_name_row = _prop_input("Name")
    prop_table, prop_table_row = _prop_input("Table", disabled=True)
    prop_obj_type, prop_type_row = _prop_input("Object Type", disabled=True)
    prop_format_str, prop_format_row = _prop_input("Format String")
    prop_display_folder, prop_folder_row = _prop_input("Display Folder")
    prop_description, prop_desc_row = _prop_input("Description")
    prop_summarize_by, prop_summarize_row = _prop_input("Summarize By", disabled=True)
    # Extended column properties (read-only, shown for columns)
    prop_is_key, prop_is_key_row = _prop_input("Is Key", disabled=True)
    prop_sort_by, prop_sort_by_row = _prop_input("Sort By Column", disabled=True)
    prop_data_category, prop_data_cat_row = _prop_input("Data Category", disabled=True)
    prop_encoding, prop_encoding_row = _prop_input("Encoding Hint", disabled=True)
    prop_nullable, prop_nullable_row = _prop_input("Is Nullable", disabled=True)
    # Extended measure property
    prop_is_hidden, prop_is_hidden_row = _prop_input("Is Hidden", disabled=True)
    # Extended relationship properties (shown in expression panel via _get_preview_text)
    # These use the existing expression textarea, no new widgets needed

    # Unified save button with dirty state + pending changes buffer
    _is_dirty = [False]
    _pending_changes = {}  # key -> {expression, name, format_string, display_folder, description}
    _suppressing_observe = [False]  # prevent observe triggers during programmatic updates
    _tree_stale = [False]  # set when pending_changes updated, cleared after tree refresh
    _undo_stack = []  # list of _pending_changes snapshots
    _redo_stack = []
    undo_btn = widgets.Button(description="\u21a9 Undo", layout=widgets.Layout(width="70px"), disabled=True)
    redo_btn = widgets.Button(description="\u21aa Redo", layout=widgets.Layout(width="70px"), disabled=True)
    save_btn = widgets.Button(description="\u2713 No changes", button_style="success", disabled=True, layout=widgets.Layout(width="200px"))
    discard_btn = widgets.Button(description="\u2718 Discard", button_style="warning", layout=widgets.Layout(width="100px", display="none"))
    save_status = status_html()
    save_row = widgets.HBox([undo_btn, redo_btn, save_btn, discard_btn, save_status], layout=widgets.Layout(align_items="center", gap="4px", margin="8px 0 0 0"))

    # Refresh controls
    refresh_type_dd = widgets.Dropdown(
        options=["full", "dataOnly", "calculate", "automatic"],
        value="automatic",
        layout=widgets.Layout(width="130px"),
    )
    refresh_btn = widgets.Button(description="🔄 Refresh Model", layout=widgets.Layout(width="150px"))
    refresh_status = status_html()

    def on_refresh(_):
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds_input = report_input.value.strip() if report_input else ""
        key = _current_key[0] or ""
        refresh_btn.disabled = True
        refresh_btn.description = "Refreshing…"
        try:
            from sempy_labs import refresh_semantic_model
            r_type = refresh_type_dd.value

            # Determine what to refresh based on selection
            if key.startswith("partition:"):
                # Refresh single partition
                parts = key.split(":", 2)
                raw_table = parts[1] if len(parts) > 1 else ""
                p_name = parts[2] if len(parts) > 2 else ""
                if "\x1f" in raw_table:
                    ds, table_name = raw_table.split("\x1f", 1)
                else:
                    ds = ds_input
                    table_name = raw_table
                partition_ref = f"'{table_name}'[{p_name}]"
                set_status(refresh_status, f"Refreshing partition {partition_ref}…", GRAY_COLOR)
                refresh_semantic_model(dataset=ds, refresh_type=r_type, workspace=ws, partitions=[partition_ref])
                set_status(refresh_status, f"✓ Partition {partition_ref} refreshed.", "#34c759")
            elif key.startswith("table:"):
                raw_table = key.split(":", 1)[1]
                if "\x1f" in raw_table:
                    ds, table_name = raw_table.split("\x1f", 1)
                else:
                    ds = ds_input
                    table_name = raw_table
                set_status(refresh_status, f"Refreshing table '{table_name}'…", GRAY_COLOR)
                refresh_semantic_model(dataset=ds, refresh_type=r_type, workspace=ws, tables=[table_name])
                set_status(refresh_status, f"✓ Table '{table_name}' refreshed.", "#34c759")
            else:
                # Model-level refresh
                if key.startswith("model:"):
                    ds = key.split(":", 1)[1]
                elif _model_data.get("_dataset_name"):
                    ds = _model_data["_dataset_name"]
                else:
                    items_list = [x.strip() for x in ds_input.split(",") if x.strip()]
                    ds = items_list[0] if items_list else ""
                if not ds:
                    set_status(refresh_status, "No model selected.", "#ff3b30")
                    return
                set_status(refresh_status, f"Refreshing '{ds}'…", GRAY_COLOR)
                refresh_semantic_model(dataset=ds, refresh_type=r_type, workspace=ws)
                set_status(refresh_status, f"✓ Model '{ds}' refreshed.", "#34c759")
        except Exception as e:
            set_status(refresh_status, f"Error: {e}", "#ff3b30")
        finally:
            refresh_btn.disabled = False
            # Restore label based on current selection
            cur = _current_key[0] or ""
            if cur.startswith("partition:"):
                refresh_btn.description = "🔄 Refresh Partition"
            elif cur.startswith("table:"):
                refresh_btn.description = "🔄 Refresh Table"
            else:
                refresh_btn.description = "🔄 Refresh Model"

    refresh_btn.on_click(on_refresh)
    refresh_row = widgets.HBox([refresh_type_dd, refresh_btn, refresh_status], layout=widgets.Layout(align_items="center", gap="8px", margin="4px 0 0 0"))

    def _capture_current():
        """Store current field values as pending changes for the current key."""
        key = _current_key[0]
        if key and not _suppressing_observe[0]:
            node_type = key.split(":")[0]
            if node_type in ("measure", "calc_item", "column", "table", "partition"):
                _pending_changes[key] = {
                    "expression": preview.value,
                    "name": prop_name.value,
                    "format_string": prop_format_str.value,
                    "display_folder": prop_display_folder.value,
                    "description": prop_description.value,
                }

    def _update_undo_redo_btns():
        undo_btn.disabled = len(_undo_stack) == 0
        redo_btn.disabled = len(_redo_stack) == 0

    def _mark_dirty(*_):
        if _suppressing_observe[0]:
            return
        # Snapshot before this change
        import copy
        _undo_stack.append(copy.deepcopy(_pending_changes))
        _redo_stack.clear()
        if not _is_dirty[0]:
            _is_dirty[0] = True
        # Capture changes immediately
        _capture_current()
        _tree_stale[0] = True
        n = len(_pending_changes)
        save_btn.description = f"\u26a0\ufe0f {n} unsaved change(s)"
        save_btn.button_style = "danger"
        save_btn.disabled = False
        discard_btn.layout.display = ""
        _update_undo_redo_btns()

    def _mark_clean():
        _is_dirty[0] = False
        _pending_changes.clear()
        _undo_stack.clear()
        _redo_stack.clear()
        _tree_stale[0] = True
        save_btn.description = "\u2713 No changes"
        save_btn.button_style = "success"
        save_btn.disabled = True
        discard_btn.layout.display = "none"
        save_status.value = ""
        _update_undo_redo_btns()

    def _restore_pending(snapshot):
        """Restore _pending_changes from snapshot and update UI."""
        import copy
        _pending_changes.clear()
        _pending_changes.update(copy.deepcopy(snapshot))
        _tree_stale[0] = True
        n = len(_pending_changes)
        if n == 0:
            _is_dirty[0] = False
            save_btn.description = "\u2713 No changes"
            save_btn.button_style = "success"
            save_btn.disabled = True
            discard_btn.layout.display = "none"
        else:
            _is_dirty[0] = True
            save_btn.description = f"\u26a0\ufe0f {n} unsaved change(s)"
            save_btn.button_style = "danger"
            save_btn.disabled = False
            discard_btn.layout.display = ""
        # Refresh inputs for current key
        key = _current_key[0]
        _suppressing_observe[0] = True
        if key and key in _pending_changes:
            vals = _pending_changes[key]
            preview.value = vals.get("expression", preview.value)
            prop_name.value = vals.get("name", prop_name.value)
            prop_format_str.value = vals.get("format_string", prop_format_str.value)
            prop_display_folder.value = vals.get("display_folder", prop_display_folder.value)
            prop_description.value = vals.get("description", prop_description.value)
        elif key and key not in _pending_changes:
            preview.value = _get_preview_text(_model_data, key)
            _populate_props(key)
        _suppressing_observe[0] = False
        _refresh_tree()
        _update_undo_redo_btns()

    def on_undo(_):
        if not _undo_stack:
            return
        import copy
        _redo_stack.append(copy.deepcopy(_pending_changes))
        _restore_pending(_undo_stack.pop())

    def on_redo(_):
        if not _redo_stack:
            return
        import copy
        _undo_stack.append(copy.deepcopy(_pending_changes))
        _restore_pending(_redo_stack.pop())

    undo_btn.on_click(on_undo)
    redo_btn.on_click(on_redo)

    def on_discard(_):
        """Discard all pending changes and reload current item from cache."""
        _pending_changes.clear()
        _mark_clean()
        _refresh_tree()
        # Reload current item from original data
        key = _current_key[0]
        if key:
            _suppressing_observe[0] = True
            preview.value = _get_preview_text(_model_data, key)
            preview.disabled = key.split(":")[0] not in ("measure", "calc_item", "rel", "partition")
            _populate_props(key)
            _suppressing_observe[0] = False

    discard_btn.on_click(on_discard)

    # Observe editable fields for changes
    preview.observe(_mark_dirty, names="value")
    prop_name.observe(_mark_dirty, names="value")
    prop_format_str.observe(_mark_dirty, names="value")
    prop_display_folder.observe(_mark_dirty, names="value")
    prop_description.observe(_mark_dirty, names="value")

    props_container = widgets.VBox(
        [prop_name_row, prop_table_row, prop_type_row, prop_format_row, prop_folder_row,
         prop_summarize_row, prop_is_key_row, prop_sort_by_row, prop_data_cat_row,
         prop_encoding_row, prop_nullable_row, prop_is_hidden_row, prop_desc_row],
        layout=widgets.Layout(gap="4px"),
    )
    props_placeholder = widgets.HTML(
        value=f'<div style="padding:12px; color:{GRAY_COLOR}; font-size:13px; font-family:{FONT_FAMILY}; font-style:italic;">Select an object to view properties</div>',
    )
    props_container.layout.display = "none"
    # CSS: kill ALL scrollbars on children, keep panel-level scrollbar
    _props_css = widgets.HTML(
        value=(
            '<style>'
            '.pbi-props * { scrollbar-width: none !important; -ms-overflow-style: none !important; }'
            '.pbi-props *::-webkit-scrollbar { display: none !important; width: 0 !important; height: 0 !important; }'
            '.pbi-props > .widget-container { overflow-y: auto !important; scrollbar-width: thin !important; }'
            '.pbi-props > .widget-container::-webkit-scrollbar { display: block !important; width: 6px !important; }'
            '.pbi-props > .widget-container::-webkit-scrollbar-thumb { background: #ccc !important; border-radius: 3px !important; }'
            '</style>'
        )
    )
    props_box = widgets.VBox(
        [_props_css, props_label, props_placeholder, props_container],
        layout=widgets.Layout(
            flex="0 0 220px",
            overflow_y="auto",
            overflow_x="hidden",
            border=f"1px solid {BORDER_COLOR}",
            border_radius="8px",
            padding="6px",
            background_color=SECTION_BG,
        ),
    )
    props_box.add_class("pbi-props")

    # Three-column layout: Tree (with search) | Properties | Preview (side by side)
    tree_col = widgets.VBox([tree_search, tree], layout=widgets.Layout(width="320px", min_width="280px", max_width="400px", gap="2px", overflow_x="hidden"))
    panels = widgets.HBox(
        [tree_col, props_box, preview_box],
        layout=widgets.Layout(width="100%", gap="8px", height="460px", align_items="stretch"),
    )
    tree_header = widgets.HTML(
        value=f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; font-family:{FONT_FAMILY}; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:2px;">Model Objects</div>'
    )

    def _populate_props(key):
        parts = key.split(":", 2)
        node_type = parts[0]
        props_placeholder.layout.display = "none"
        props_container.layout.display = ""
        prop_format_row.layout.display = ""
        prop_folder_row.layout.display = ""
        prop_summarize_row.layout.display = "none"
        # Hide Prep for AI by default; show expression widgets
        _p4ai_container.layout.display = "none"
        preview_label.layout.display = ""
        format_row.layout.display = ""
        preview.layout.display = ""
        # Restore default labels (may have been changed for model root node)
        prop_format_row.children[0].value = f'<span style="font-size:10px; font-weight:600; color:#555; font-family:{FONT_FAMILY};">Format String</span>'
        prop_folder_row.children[0].value = f'<span style="font-size:10px; font-weight:600; color:#555; font-family:{FONT_FAMILY};">Display Folder</span>'
        prop_format_str.disabled = False
        prop_display_folder.disabled = False
        # Hide extended props by default
        prop_is_key_row.layout.display = "none"
        prop_sort_by_row.layout.display = "none"
        prop_data_cat_row.layout.display = "none"
        prop_encoding_row.layout.display = "none"
        prop_nullable_row.layout.display = "none"
        prop_is_hidden_row.layout.display = "none"

        # Strip model prefix from table key for display
        raw_table = parts[1] if len(parts) > 1 else ""
        display_table = raw_table.split("\x1f")[-1] if "\x1f" in raw_table else raw_table

        if node_type == "measure":
            t = _resolve_table(_model_data, raw_table)
            m = t["measures"].get(parts[2], {}) if t else {}
            prop_name.value, prop_table.value, prop_obj_type.value = parts[2], display_table, "Measure"
            prop_format_str.value = m.get("format_string", "")
            prop_display_folder.value = m.get("display_folder", "")
            prop_description.value = m.get("description", "")
            prop_is_hidden.value = str(m.get("is_hidden", False))
            prop_is_hidden_row.layout.display = ""
        elif node_type == "column":
            t = _resolve_table(_model_data, raw_table)
            c = t["columns"].get(parts[2], {}) if t else {}
            prop_name.value, prop_table.value = parts[2], display_table
            prop_obj_type.value = c.get("type", "Column")
            prop_summarize_by.value = c.get("summarize_by", "")
            prop_format_str.value, prop_description.value = "", ""
            prop_display_folder.value = c.get("display_folder", "")
            prop_format_row.layout.display = "none"
            prop_folder_row.layout.display = ""
            prop_summarize_row.layout.display = ""
            # Show extended column properties
            prop_is_key.value = str(c.get("is_key", False))
            prop_is_key_row.layout.display = ""
            prop_sort_by.value = c.get("sort_by_column", "") or "—"
            prop_sort_by_row.layout.display = ""
            prop_data_category.value = c.get("data_category", "") or "—"
            prop_data_cat_row.layout.display = ""
            prop_encoding.value = c.get("encoding_hint", "") or "Default"
            prop_encoding_row.layout.display = ""
            prop_nullable.value = str(c.get("is_nullable", True))
            prop_nullable_row.layout.display = ""
        elif node_type == "table":
            t = _resolve_table(_model_data, raw_table) or {}
            prop_name.value, prop_table.value = display_table, ""
            prop_obj_type.value = t.get("type", "Table")
            prop_format_str.value, prop_display_folder.value = "", ""
            prop_format_row.layout.display = "none"
            prop_folder_row.layout.display = "none"
            prop_description.value = t.get("description", "")
        elif node_type == "calc_item":
            prop_name.value, prop_table.value = parts[2], display_table
            prop_obj_type.value = "Calculation Item"
            prop_format_str.value, prop_display_folder.value, prop_description.value = "", "", ""
            prop_format_row.layout.display = "none"
            prop_folder_row.layout.display = "none"
        elif node_type == "hierarchy":
            prop_name.value = parts[2] if len(parts) > 2 else ""
            prop_table.value, prop_obj_type.value = display_table, "Hierarchy"
            prop_format_str.value, prop_display_folder.value, prop_description.value = "", "", ""
            prop_format_row.layout.display = "none"
            prop_folder_row.layout.display = "none"
        elif node_type == "partition":
            p_name = parts[2] if len(parts) > 2 else ""
            t = _resolve_table(_model_data, raw_table)
            src_type = ""
            if t:
                for pt in t.get("partitions", []):
                    if pt["name"] == p_name:
                        src_type = pt.get("source_type", "")
                        break
            prop_name.value, prop_table.value = p_name, display_table
            prop_obj_type.value = f"Partition ({src_type})" if src_type else "Partition"
            prop_format_str.value, prop_display_folder.value, prop_description.value = "", "", ""
            prop_format_row.layout.display = "none"
            prop_folder_row.layout.display = "none"
        elif node_type == "model":
            m_name = parts[1] if len(parts) > 1 else ""
            props_data = _model_data.get("model_properties", {})
            prop_name.value = m_name
            prop_table.value = ""
            prop_obj_type.value = "Semantic Model"
            prop_format_str.value = props_data.get("compatibility_level", "")
            prop_display_folder.value = props_data.get("default_mode", "")
            prop_description.value = ""
            # Relabel: Format String → Compat Level, Display Folder → Default Mode
            prop_format_row.children[0].value = f'<span style="font-size:10px; font-weight:600; color:#555; font-family:{FONT_FAMILY};">Compat Level</span>'
            prop_folder_row.children[0].value = f'<span style="font-size:10px; font-weight:600; color:#555; font-family:{FONT_FAMILY};">Default Mode</span>'
            prop_format_str.disabled = True
            prop_display_folder.disabled = True
            prop_format_row.layout.display = ""
            prop_folder_row.layout.display = ""
            # Show Prep for AI section; hide expression widgets
            _p4ai_container.layout.display = ""
            preview_label.layout.display = "none"
            format_row.layout.display = "none"
            preview.layout.display = "none"
            table_preview_html.layout.display = "none"
            table_row_dropdown.layout.display = "none"
            # Track the currently selected model and reset UI if switching
            _p4ai_selected_model[0] = m_name
            if _p4ai_loaded_ds[0] != m_name:
                _p4ai_textarea.value = ""
                _p4ai_info.value = ""
                _p4ai_qna_btn.layout.display = "none"
                set_status(_p4ai_status, "", "transparent")
                _p4ai_loaded_ds[0] = None
        else:
            props_container.layout.display = "none"
            props_placeholder.layout.display = ""

    def on_load(_):
        nonlocal _model_data, _key_map
        _expanded.clear()
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds_input = report_input.value.strip() if report_input else ""

        # Parse comma-separated items, or list all if blank
        if ds_input:
            items = [x.strip() for x in ds_input.split(",") if x.strip()]
        else:
            # Blank = load all semantic models in the workspace
            load_btn.disabled = True
            load_btn.description = "Listing\u2026"
            set_status(conn_status, "Listing semantic models\u2026", GRAY_COLOR)
            try:
                items = _list_workspace_datasets(ws)
            except Exception as e:
                set_status(conn_status, f"Error listing models: {e}", "#ff3b30")
                load_btn.disabled = False
                load_btn.description = "Load Model"
                return
            if not items:
                set_status(conn_status, "No semantic models found in workspace.", "#ff9500")
                load_btn.disabled = False
                load_btn.description = "Load Model"
                return

        load_btn.disabled = True
        load_btn.description = "Loading\u2026"
        set_status(conn_status, f"Loading {len(items)} model(s)\u2026", GRAY_COLOR)

        start_time = time.time()
        merged_data = {"tables": {}, "models": {}, "relationships": [], "model_relationships": {}, "perspectives": [], "model_perspectives": {}}
        loaded = 0
        errors = 0
        last_error = ""

        try:
            for i, ds in enumerate(items):
                if time.time() - start_time > _LOAD_TIMEOUT:
                    set_status(conn_status, f"\u23f1\ufe0f Timeout after {loaded}/{len(items)} models.", "#ff9500")
                    break
                set_status(conn_status, f"Model {i+1}/{len(items)}: loading '{ds}'\u2026", GRAY_COLOR)
                try:
                    data = _load_model_data_fast(dataset=ds, workspace=ws)
                    if len(items) > 1:
                        merged_data["models"][ds] = data["tables"]
                        merged_data["model_relationships"][ds] = data.get("relationships", [])
                        merged_data["model_perspectives"][ds] = data.get("perspectives", [])
                    else:
                        merged_data["tables"].update(data["tables"])
                        merged_data["relationships"] = data.get("relationships", [])
                        merged_data["perspectives"] = data.get("perspectives", [])
                        merged_data["_dataset_name"] = ds
                        merged_data["model_properties"] = data.get("model_properties", {})
                    loaded += 1
                except Exception as e:
                    errors += 1
                    last_error = str(e)

            _model_data = merged_data

            # Auto-expand all items after load
            models = _model_data.get("models", {})
            if models:
                for m_name, m_tables in models.items():
                    _expanded.add(m_name)
                    for t_name in m_tables:
                        _expanded.add(f"{m_name}\x1f{t_name}")
            else:
                ds_name = _model_data.get("_dataset_name", "Model")
                _expanded.add(ds_name)
                _expanded.update(_model_data.get("tables", {}).keys())

            _refresh_tree()
            all_tables = {}
            all_tables.update(_model_data.get("tables", {}))
            for m_tables in _model_data.get("models", {}).values():
                all_tables.update(m_tables)
            n_t = len(all_tables)
            n_m = sum(len(t["measures"]) for t in all_tables.values())
            n_c = sum(len(t["columns"]) for t in all_tables.values())
            elapsed = int(time.time() - start_time)
            if loaded == 0 and last_error:
                set_status(conn_status, f"Error: {last_error}", "#ff3b30")
            else:
                err_str = f", {errors} error(s)" if errors else ""
                set_status(conn_status, f"Loaded {loaded}/{len(items)} model(s): {n_t} tables, {n_c} columns, {n_m} measures ({elapsed}s{err_str})", "#34c759" if not errors else "#ff9500")
            preview.value = ""
            preview.disabled = True
        except Exception as e:
            set_status(conn_status, f"Error: {e}", "#ff3b30")
        finally:
            load_btn.disabled = False
            load_btn.description = "Load Model"

    def on_select(change):
        selected = change.get("new", ())
        if not selected:
            return
        last = selected[-1]
        if last not in _key_map:
            return
        key = _key_map[last]
        # Refresh tree to update \u270f markers if pending changes were added
        if _tree_stale[0]:
            _tree_stale[0] = False
            _refresh_tree()
            return  # refresh resets selection, next click will proceed
        _current_key[0] = key
        # Single-click on a parent node: toggle expand/collapse
        if len(selected) == 1:
            if key.startswith("model:"):
                m_name = key.split(":", 1)[1]
                if m_name in _expanded:
                    _expanded.discard(m_name)
                else:
                    _expanded.add(m_name)
                _refresh_tree()
                # Re-select the model node so it stays highlighted
                _reselect_key_in_tree(key)
                # Fall through to show model properties/expression
            elif key.startswith("table:"):
                t_name = key.split(":", 1)[1]
                if t_name in _expanded:
                    _expanded.discard(t_name)
                else:
                    _expanded.add(t_name)
                _refresh_tree()
                _reselect_key_in_tree(key)
            if key.startswith("rels:"):
                if key in _expanded:
                    _expanded.discard(key)
                else:
                    _expanded.add(key)
                _refresh_tree()
                _reselect_key_in_tree(key)
                return
            if key.startswith("folder:"):
                if key in _expanded:
                    _expanded.discard(key)
                else:
                    _expanded.add(key)
                _refresh_tree()
                _reselect_key_in_tree(key)
                return
            if key.startswith("colfolder:"):
                if key in _expanded:
                    _expanded.discard(key)
                else:
                    _expanded.add(key)
                _refresh_tree()
                _reselect_key_in_tree(key)
                return
        # Update properties/expression for last selected item
        # Restore pending changes if this item was previously edited
        _suppressing_observe[0] = True
        if key in _pending_changes:
            pending = _pending_changes[key]
            preview.value = pending.get("expression", "")
            prop_name.value = pending.get("name", "")
            prop_format_str.value = pending.get("format_string", "")
            prop_display_folder.value = pending.get("display_folder", "")
            prop_description.value = pending.get("description", "")
            preview.disabled = key.split(":")[0] not in ("measure", "calc_item", "rel", "partition")
            _populate_props(key)
            # Re-apply pending values (populate_props may overwrite)
            prop_name.value = pending.get("name", prop_name.value)
            prop_format_str.value = pending.get("format_string", prop_format_str.value)
            prop_display_folder.value = pending.get("display_folder", prop_display_folder.value)
            prop_description.value = pending.get("description", prop_description.value)
            preview.value = pending.get("expression", preview.value)
        else:
            preview.value = _get_preview_text(_model_data, key)
            preview.disabled = key.split(":")[0] not in ("measure", "calc_item", "rel", "partition")
            _populate_props(key)

        # Table data preview: show HTML table for table nodes, hide for others
        node_type = key.split(":")[0]
        if node_type == "table":
            raw_table = key.split(":", 1)[1]
            if "\x1f" in raw_table:
                ds_name, table_name = raw_table.split("\x1f", 1)
            else:
                ds_name = _model_data.get("_dataset_name", "")
                table_name = raw_table
            if ds_name and table_name:
                _table_preview_ds[0] = ds_name
                _table_preview_name[0] = table_name
                table_preview_html.value = '<div style="color:#999; font-size:12px;">Loading preview\u2026</div>'
                preview.layout.display = "none"
                table_preview_html.layout.display = ""
                table_row_dropdown.layout.display = ""
                format_row.layout.display = "none"
                preview_label.value = f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; font-family:{FONT_FAMILY}; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:2px;">Table Preview</div>'
                table_preview_html.value = _load_table_preview(ds_name, table_name, table_row_dropdown.value)
            else:
                _table_preview_name[0] = None
                preview.layout.display = ""
                table_preview_html.layout.display = "none"
                table_row_dropdown.layout.display = "none"
                format_row.layout.display = ""
                preview_label.value = f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; font-family:{FONT_FAMILY}; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:2px;">Expression</div>'
        else:
            _table_preview_name[0] = None
            table_preview_html.layout.display = "none"
            table_row_dropdown.layout.display = "none"
            if node_type != "model":
                preview.layout.display = ""
                format_row.layout.display = ""
                preview_label.value = f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; font-family:{FONT_FAMILY}; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:2px;">Expression</div>'

        # Enable copy ref button for measures and columns
        copy_ref_btn.disabled = key.split(":")[0] not in ("measure", "column")
        if key.split(":")[0] not in ("measure", "column"):
            ref_output.layout.display = "none"

        # Dynamic refresh button label based on selection
        if node_type == "partition":
            refresh_btn.description = "🔄 Refresh Partition"
        elif node_type == "table":
            refresh_btn.description = "🔄 Refresh Table"
        else:
            refresh_btn.description = "🔄 Refresh Model"

        _suppressing_observe[0] = False

    def _expand_folders(tables, table_prefix=""):
        """Add all display folder keys (including nested subfolders) to _expanded."""
        for t_name, t in tables.items():
            key_base = f"{table_prefix}\x1f{t_name}" if table_prefix else t_name
            for mn in t.get("measures", {}):
                df = t["measures"][mn].get("display_folder", "")
                if df:
                    parts = df.replace("/", "\\").split("\\")
                    for i in range(len(parts)):
                        ancestor = "\\".join(parts[: i + 1])
                        _expanded.add(f"folder:{key_base}:{ancestor}")
            for cn in t.get("columns", {}):
                df = t["columns"][cn].get("display_folder", "")
                if df:
                    first_folder = df.split(";")[0].strip()
                    parts = first_folder.replace("/", "\\").split("\\")
                    for i in range(len(parts)):
                        ancestor = "\\".join(parts[: i + 1])
                        _expanded.add(f"colfolder:{key_base}:{ancestor}")

    def on_expand_all(_):
        if _model_data:
            # Expand all models and all tables
            models = _model_data.get("models", {})
            if models:
                for m_name, m_tables in models.items():
                    _expanded.add(m_name)
                    for t_name in m_tables:
                        _expanded.add(f"{m_name}\x1f{t_name}")
                    _expand_folders(m_tables, m_name)
            else:
                ds_name = _model_data.get("_dataset_name", "Model")
                _expanded.add(ds_name)
                _expanded.update(_model_data.get("tables", {}).keys())
                _expand_folders(_model_data.get("tables", {}))
            _refresh_tree()

    def on_collapse_all(_):
        _expanded.clear()
        if _model_data:
            _refresh_tree()

    def on_save(_):
        """Save ALL pending changes across all modified items."""
        # Also capture current item's latest state
        _capture_current()
        if not _pending_changes:
            return
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        save_btn.disabled = True
        save_btn.description = "Saving\u2026"
        set_status(save_status, f"Writing {len(_pending_changes)} change(s) via XMLA\u2026", GRAY_COLOR)
        saved = 0
        errors = 0
        # Group by dataset
        by_ds = {}
        for pkey, changes in _pending_changes.items():
            parts = pkey.split(":", 2)
            raw_table = parts[1] if len(parts) > 1 else ""
            if "\x1f" in raw_table:
                ds, table_name = raw_table.split("\x1f", 1)
            else:
                ds = report_input.value.strip() if report_input else ""
                table_name = raw_table
            if ds not in by_ds:
                by_ds[ds] = []
            by_ds[ds].append((pkey, parts, table_name, changes))

        for ds, items_list in by_ds.items():
            if not ds:
                continue
            try:
                from sempy_labs.tom import connect_semantic_model
                with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tm:
                    for pkey, parts, table_name, changes in items_list:
                        node_type = parts[0]
                        try:
                            if node_type == "measure":
                                m_obj = tm.model.Tables[table_name].Measures[parts[2]]
                                m_obj.Expression = changes.get("expression", m_obj.Expression)
                                m_obj.Name = changes.get("name", m_obj.Name)
                                m_obj.FormatString = changes.get("format_string", "")
                                m_obj.DisplayFolder = changes.get("display_folder", "")
                                m_obj.Description = changes.get("description", "")
                                # Update local cache
                                raw_table = parts[1]
                                t = _resolve_table(_model_data, raw_table)
                                if t:
                                    old_name = parts[2]
                                    entry = t["measures"].pop(old_name, {})
                                    entry["expression"] = changes.get("expression", "")
                                    entry["format_string"] = changes.get("format_string", "")
                                    entry["display_folder"] = changes.get("display_folder", "")
                                    entry["description"] = changes.get("description", "")
                                    t["measures"][changes.get("name", old_name)] = entry
                                saved += 1
                            elif node_type == "calc_item":
                                tm.model.Tables[table_name].CalculationGroup.CalculationItems[parts[2]].Expression = changes.get("expression", "")
                                t = _resolve_table(_model_data, parts[1])
                                if t:
                                    t["calc_items"][parts[2]]["expression"] = changes.get("expression", "")
                                saved += 1
                            elif node_type == "partition":
                                p_name = parts[2] if len(parts) > 2 else ""
                                for p_obj in tm.model.Tables[table_name].Partitions:
                                    if p_obj.Name == p_name:
                                        expr = changes.get("expression", "")
                                        import Microsoft.AnalysisServices.Tabular as TOM
                                        if p_obj.SourceType == TOM.PartitionSourceType.M:
                                            p_obj.Source.Expression = expr
                                        elif p_obj.SourceType == TOM.PartitionSourceType.Calculated:
                                            p_obj.Source.Expression = expr
                                        # Update local cache
                                        t = _resolve_table(_model_data, parts[1])
                                        if t:
                                            for pt in t.get("partitions", []):
                                                if pt["name"] == p_name:
                                                    pt["expression"] = expr
                                        saved += 1
                                        break
                            elif node_type == "table":
                                tm.model.Tables[table_name].Description = changes.get("description", "")
                                t = _resolve_table(_model_data, parts[1])
                                if t:
                                    t["description"] = changes.get("description", "")
                                saved += 1
                        except Exception:
                            errors += 1
                    tm.model.SaveChanges()
            except Exception as e:
                errors += len(items_list)
                set_status(save_status, f"Error on '{ds}': {e}", "#ff3b30")

        _mark_clean()
        if errors:
            set_status(save_status, f"\u26a0\ufe0f Saved {saved}, {errors} error(s).", "#ff9500")
        else:
            set_status(save_status, f"\u2713 Saved {saved} change(s).", "#34c759")
        _refresh_tree()

    load_btn.on_click(on_load)
    tree.observe(on_select, names="value")
    expand_btn.on_click(on_expand_all)
    collapse_btn.on_click(on_collapse_all)
    save_btn.on_click(on_save)

    def on_run_action(_):
        """Run the action selected in the dropdown."""
        action = fixer_dropdown.value
        if action == "Select action..." or action.startswith("──") or action not in fixer_callbacks:
            set_status(conn_status, "Select an action from the dropdown first.", "#ff9500")
            return
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds = report_input.value.strip() if report_input else ""
        if not ds:
            set_status(conn_status, "No model loaded.", "#ff3b30")
            return

        # Extract selected measure and column names from tree selection
        sel_measures = []
        sel_columns = []
        for key in _selected_keys:
            parts = key.split(":", 2)
            if parts[0] == "measure" and len(parts) > 2:
                sel_measures.append(parts[2])
            elif parts[0] == "column" and len(parts) > 2:
                sel_columns.append(parts[2])

        # Determine target model(s) from tree selection
        all_items = [x.strip() for x in ds.split(",") if x.strip()]
        cur_key = _current_key[0] or ""
        selected_model = None
        # Extract model name from current selection key
        if cur_key.startswith("model:"):
            selected_model = cur_key.split(":", 1)[1]
        elif "\x1f" in cur_key:
            # Keys like table:ModelName\x1fTableName or measure:ModelName\x1fTable\x1fMeasure
            raw = cur_key.split(":", 1)[1] if ":" in cur_key else cur_key
            selected_model = raw.split("\x1f")[0]

        if selected_model and len(all_items) > 1:
            items = [selected_model]
        else:
            items = all_items
        set_status(conn_status, f"Running {action} on {len(items)} model(s)\u2026", GRAY_COLOR)
        succeeded = 0
        failed = 0
        last_error = ""
        try:
            import io as _io
            from contextlib import redirect_stdout as _redirect
            for i, item in enumerate(items):
                if len(items) > 1:
                    set_status(conn_status, f"Running {action} on '{item}' ({i+1}/{len(items)})\u2026", GRAY_COLOR)
                buf = _io.StringIO()
                kwargs = {"report": item, "workspace": ws, "scan_only": False,
                          "selected_keys": list(_selected_keys), "model_data": _model_data}
                if sel_measures and action in ("Add PY Measures (Y-1)",):
                    kwargs["measures"] = sel_measures
                if sel_columns and action in ("Auto-Create Measures from Columns",):
                    kwargs["columns"] = sel_columns
                try:
                    with _redirect(buf):
                        result = fixer_callbacks[action](**kwargs)
                    # If callback returns a widget, show it in scan_results_box
                    if result is not None and hasattr(result, 'children'):
                        scan_results_box.children = [result]
                        scan_results_box.layout.display = ""
                    succeeded += 1
                except Exception as e:
                    failed += 1
                    last_error = str(e)[:200]
                finally:
                    # Show captured output in scan_results_box
                    output_text = buf.getvalue().strip()
                    if output_text:
                        from ipywidgets import HTML as _OHTML
                        escaped = output_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                        html_out = f'<pre style="font-size:12px;white-space:pre-wrap;margin:0;color:#333;">{escaped}</pre>'
                        scan_results_box.children = [_OHTML(html_out)]
                        scan_results_box.layout.display = ""
            if failed == 0:
                msg = f"\u2713 {action} complete on {succeeded} model(s)."
            else:
                msg = f"\u2713 {succeeded} ok, \u2717 {failed} failed. Last error: {last_error}"
            set_status(conn_status, msg, "#34c759" if failed == 0 else "#ff9500")
        except Exception as e:
            set_status(conn_status, f"Error: {e}", "#ff3b30")

    run_action_btn.on_click(on_run_action)

    # Scan results detail panel (below save row)
    scan_results_box = widgets.VBox(layout=widgets.Layout(display="none", gap="4px",
        max_height="500px", overflow_y="auto", overflow_x="auto",
        border=f"1px solid {BORDER_COLOR}", border_radius="8px",
        padding="8px", background_color=SECTION_BG, margin="8px 0 0 0"))

    def on_scan(_):
        """Run all SM fixers in scan_only mode, collect detailed findings."""
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds_input = report_input.value.strip() if report_input else ""
        items = [x.strip() for x in ds_input.split(",") if x.strip()] if ds_input else []
        if not items and not _model_data.get("models") and not _model_data.get("tables"):
            set_status(conn_status, "No model loaded. Load first.", "#ff3b30")
            return
        if not items:
            items = list(_model_data.get("models", {}).keys())
            if not items:
                items = [ds_input] if ds_input else []

        scan_btn.disabled = True
        scan_btn.description = "Scanning\u2026"
        _scan_results.clear()

        import io as _io
        from contextlib import redirect_stdout as _redirect

        total_findings = 0
        all_findings = []  # [(model, fixer_name, detail_line), ...]
        # Skip these from scan (they're additive actions, not violations)
        skip_fixers = {"  Auto-Create Measures from Columns", "  Add PY Measures (Y-1)", "  Format All DAX"}
        fixer_names = [k for k in fixer_callbacks if k not in skip_fixers and k != "Select action..." and not k.startswith("──")]

        total_steps = len(items) * len(fixer_names)
        step = 0
        for ds in items:
            for fixer_name in fixer_names:
                step += 1
                scan_btn.description = f"{step}/{total_steps}"
                set_status(conn_status, f"Scanning '{ds}' — {fixer_name.strip()} ({step}/{total_steps})", GRAY_COLOR)
                try:
                    buf = _io.StringIO()
                    with _redirect(buf):
                        fixer_callbacks[fixer_name](report=ds, workspace=ws, scan_only=True)
                    output = buf.getvalue()
                    for line in output.splitlines():
                        line = line.strip()
                        if line:
                            all_findings.append((ds, fixer_name, line))
                            total_findings += 1
                            model_key = f"model:{ds}"
                            _scan_results[model_key] = _scan_results.get(model_key, 0) + 1
                except Exception:
                    pass

        _refresh_tree()

        # Build results panel with Fix buttons
        if all_findings:
            # Build an HTML table for the findings (no grid, proper wrapping)
            tbl_rows = []
            for idx, (ds, fixer_name, detail) in enumerate(all_findings):
                no_action = "no action needed" in detail.lower()
                severity_icon = "🟢" if no_action else "🟠"
                bg = "#fff" if idx % 2 == 0 else "#fafafa"
                tbl_rows.append(
                    f'<tr style="background:{bg};">'
                    f'<td style="padding:4px 6px;font-size:11px;white-space:nowrap;vertical-align:top;">{severity_icon}</td>'
                    f'<td style="padding:4px 6px;font-size:11px;white-space:nowrap;vertical-align:top;color:#333;">{ds}</td>'
                    f'<td style="padding:4px 6px;font-size:11px;white-space:nowrap;vertical-align:top;color:{ICON_ACCENT};font-weight:600;">{fixer_name.strip()}</td>'
                    f'<td style="padding:4px 6px;font-size:11px;vertical-align:top;color:#555;word-break:break-word;">{detail}</td>'
                    f'</tr>'
                )
            html_table = (
                f'<div style="font-size:12px;font-weight:600;color:{ICON_ACCENT};font-family:{FONT_FAMILY};'
                f'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px;">'
                f'\u26a0\ufe0f {total_findings} Finding(s)</div>'
                f'<table style="width:100%;border-collapse:collapse;font-family:{FONT_FAMILY};">'
                f'<thead><tr style="border-bottom:2px solid {BORDER_COLOR};">'
                f'<th style="padding:4px 6px;font-size:11px;text-align:left;color:#555;"></th>'
                f'<th style="padding:4px 6px;font-size:11px;text-align:left;color:#555;">Model</th>'
                f'<th style="padding:4px 6px;font-size:11px;text-align:left;color:#555;">Check</th>'
                f'<th style="padding:4px 6px;font-size:11px;text-align:left;color:#555;">Finding</th>'
                f'</tr></thead><tbody>{"".join(tbl_rows)}</tbody></table>'
            )
            # Build Fix buttons separately (one per non-green finding)
            result_widgets = [widgets.HTML(value=html_table)]
            # Add Fix buttons row
            fixable_findings = [(ds, fn, det) for ds, fn, det in all_findings if "no action needed" not in det.lower()]
            if fixable_findings:
                fix_btns = []
                seen = set()
                for ds, fixer_name, _det in fixable_findings:
                    key = (ds, fixer_name)
                    if key in seen:
                        continue
                    seen.add(key)
                    fix_btn = widgets.Button(
                        description=f"Fix: {fixer_name.strip()}",
                        button_style="warning",
                        layout=widgets.Layout(width="auto", height="28px"),
                    )
                    def _make_fix(fn, model):
                        def _handler(_):
                            _ws = ws
                            set_status(conn_status, f"Fixing: {fn} on '{model}'\u2026", GRAY_COLOR)
                            try:
                                buf2 = _io.StringIO()
                                with _redirect(buf2):
                                    fixer_callbacks[fn](report=model, workspace=_ws, scan_only=False)
                                set_status(conn_status, f"\u2713 {fn} applied to '{model}'.", "#34c759")
                            except Exception as e:
                                set_status(conn_status, f"Error: {e}", "#ff3b30")
                        return _handler
                    fix_btn.on_click(_make_fix(fixer_name, ds))
                    fix_btns.append(fix_btn)
                result_widgets.append(widgets.HBox(fix_btns, layout=widgets.Layout(gap="6px", flex_wrap="wrap", margin="8px 0 0 0")))
            scan_results_box.children = result_widgets
            scan_results_box.layout.display = ""
        else:
            scan_results_box.children = []
            scan_results_box.layout.display = "none"

        scan_btn.disabled = False
        scan_btn.description = "\U0001F50D Scan"
        if total_findings > 0:
            set_status(conn_status, f"\U0001F50D Scan: {total_findings} finding(s) across {len(items)} model(s).", "#ff9500")
        else:
            set_status(conn_status, f"\u2713 Scan complete: no issues found.", "#34c759")

    scan_btn.on_click(on_scan)

    widget = widgets.VBox([nav_row, action_row, tree_header, panels, save_row, refresh_row, scan_results_box], layout=widgets.Layout(padding="12px", gap="4px"))
    return widget, on_load