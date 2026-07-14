# Interactive PBI Report Fixer UI (ipywidgets)
# Orchestrates report visual fixers and semantic model fixers via a single notebook widget.

__version__ = "1.2.323"

import ipywidgets as widgets
import io
import time
from contextlib import redirect_stdout
from typing import Optional
from uuid import UUID
import warnings as _warnings

def _lazy_import(module_path, name):
    """Import a symbol from a module, returning None + warning on failure."""
    try:
        mod = __import__(module_path, fromlist=[name])
        return getattr(mod, name)
    except Exception as _e:
        _warnings.warn(f"PBI Fixer: could not load {module_path}.{name}: {type(_e).__name__}: {_e}")
        return None

# add_measures_from_columns and add_py_measures are imported lazily inside pbi_fixer()

# model_explorer_tab, report_explorer_tab, perspective_editor_tab
# are imported lazily inside pbi_fixer()


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Vertipaq Analyzer tab - native DataFrame rendering
# ---------------------------------------------------------------------------
def _vertipaq_tab(workspace_input=None, report_input=None):
    """Build the Memory Analyzer tab with native DataFrame rendering."""
    from sempy_labs._ui_components import (
        FONT_FAMILY, BORDER_COLOR, GRAY_COLOR, ICON_ACCENT, SECTION_BG,
        status_html, set_status,
    )

    _vp_data = {}
    _current_model = [None]

    load_btn = widgets.Button(description="Load Memory", button_style="primary", layout=widgets.Layout(width="120px"))
    stop_mem_btn = widgets.Button(description="\u23f9 Stop", button_style="warning", layout=widgets.Layout(width="80px", display="none"))
    _cancel_mem = [False]
    read_stats_cb = widgets.Checkbox(value=False, description="Read stats from data (Direct Lake)", indent=False, layout=widgets.Layout(width="auto"))
    conn_status = status_html()

    def _on_stop_mem(_):
        _cancel_mem[0] = True
    stop_mem_btn.on_click(_on_stop_mem)

    model_dropdown = widgets.Dropdown(
        options=["(no models loaded)"],
        value="(no models loaded)",
        layout=widgets.Layout(width="300px"),
    )

    _SUBTABS = ["Model Summary", "Tables", "Partitions", "Columns", "Relationships", "Hierarchies"]
    _DF_KEY_MAP = {
        "Model Summary": "Model", "Tables": "Tables", "Partitions": "Partitions",
        "Columns": "Columns", "Relationships": "Relationships", "Hierarchies": "Hierarchies",
    }

    subtab_selector = widgets.ToggleButtons(
        options=_SUBTABS, value="Model Summary",
        layout=widgets.Layout(width="100%"),
        style={"button_width": "auto", "font_weight": "bold"},
    )

    df_html = widgets.HTML(
        value=f'<div style="padding:20px; color:{GRAY_COLOR}; font-size:14px; font-family:{FONT_FAMILY}; text-align:center; font-style:italic;">Click Load Memory to analyze.</div>',
    )
    df_container = widgets.VBox(
        [df_html],
        layout=widgets.Layout(
            max_height="500px", overflow_y="auto", overflow_x="auto",
            border=f"1px solid {BORDER_COLOR}", border_radius="8px",
            padding="8px", background_color=SECTION_BG,
        ),
    )

    def _fmt_val(val, col_name):
        if val is None or (isinstance(val, float) and val != val):
            return "\u2014"
        if "Size" in col_name and isinstance(val, (int, float)):
            n = int(val)
            if n < 1024: return f"{n} B"
            if n < 1024**2: return f"{n/1024:.1f} KB"
            if n < 1024**3: return f"{n/1024**2:.1f} MB"
            return f"{n/1024**3:.2f} GB"
        if "%" in col_name:
            try: return f"{float(val):.1f}%"
            except: return str(val)
        if isinstance(val, (int, float)):
            if isinstance(val, float) and val == int(val): return f"{int(val):,}"
            if isinstance(val, float): return f"{val:,.1f}"
            return f"{val:,}"
        return str(val)

    def _df_to_html(df, sort_by=None):
        if df is None or len(df) == 0:
            return f'<div style="color:{GRAY_COLOR};">No data.</div>'
        if sort_by and sort_by in df.columns:
            df = df.sort_values(sort_by, ascending=False)
        html = '<table style="border-collapse:collapse; width:100%; font-size:12px; font-family:monospace;">'
        html += '<tr style="background:#f5f5f5; position:sticky; top:0; z-index:1;">'
        for col in df.columns:
            a = "right" if any(k in col for k in ("Size","Count","%","Cardinality","Rows","Temperature","Segment","Max","Missing")) else "left"
            html += f'<th style="text-align:{a}; padding:5px 8px; border-bottom:2px solid {BORDER_COLOR}; white-space:nowrap;">{col}</th>'
        html += '</tr>'
        for _, row in df.iterrows():
            html += '<tr>'
            for col in df.columns:
                val = row.get(col, "")
                a = "right" if any(k in col for k in ("Size","Count","%","Cardinality","Rows","Temperature","Segment","Max","Missing")) else "left"
                fmt = _fmt_val(val, col)
                ex = ""
                if "% DB" in col or "% Table" in col:
                    try:
                        p = float(val) if val == val else 0
                        bc = "#ff3b30" if p > 30 else "#ff9500" if p > 10 else "#34c759"
                        ex = f'<div style="height:3px; width:{min(p*2,100):.0f}%; background:{bc}; border-radius:1px; margin-top:1px;"></div>'
                    except: pass
                html += f'<td style="text-align:{a}; padding:4px 8px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{fmt}{ex}</td>'
            html += '</tr>'
        html += '</table>'
        return html

    def _render_subtab(tab_name=None):
        tab_name = tab_name or subtab_selector.value
        m = _current_model[0]
        if not m or m not in _vp_data:
            df_html.value = f'<div style="color:{GRAY_COLOR};">No data loaded.</div>'
            return
        dfs = _vp_data[m]
        df_key = _DF_KEY_MAP.get(tab_name, tab_name)
        df = dfs.get(df_key)
        if tab_name == "Model Summary" and df is not None and len(df) > 0:
            r = df.iloc[0]
            html = f'<table style="border-collapse:collapse; font-size:13px; font-family:{FONT_FAMILY}; width:100%;">'
            for col in df.columns:
                val = _fmt_val(r.get(col, ""), col)
                html += f'<tr><td style="padding:6px 12px; font-weight:600; color:#555; border-bottom:1px solid #f0f0f0; width:200px;">{col}</td><td style="padding:6px 12px; border-bottom:1px solid #f0f0f0;">{val}</td></tr>'
            html += '</table>'
            df_html.value = html
            return
        sort_by = "Total Size" if df is not None and "Total Size" in df.columns else None
        df_html.value = _df_to_html(df, sort_by=sort_by)

    def on_subtab_change(change):
        _render_subtab(change.get("new"))
    subtab_selector.observe(on_subtab_change, names="value")

    def on_model_change(change):
        m = change.get("new", "")
        if m and m != "(no models loaded)" and m in _vp_data:
            _current_model[0] = m
            _render_subtab()
    model_dropdown.observe(on_model_change, names="value")

    def on_load(_):
        nonlocal _vp_data
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds_input = report_input.value.strip() if report_input else ""
        items = [x.strip() for x in ds_input.split(",") if x.strip()] if ds_input else []
        if not items:
            set_status(conn_status, "Enter a semantic model name.", "#ff3b30")
            return
        load_btn.disabled = True
        load_btn.description = "Loading\u2026"
        stop_mem_btn.layout.display = ""
        _cancel_mem[0] = False
        _vp_data = {}
        import io as _io
        from contextlib import redirect_stdout as _redirect
        for i, ds in enumerate(items):
            if _cancel_mem[0]:
                set_status(conn_status, f"\u23f9 Stopped after {len(_vp_data)}/{len(items)} models.", "#ff9500")
                break
            set_status(conn_status, f"Memory Analyzer {i+1}/{len(items)}: '{ds}'\u2026", GRAY_COLOR)
            try:
                import IPython.display as _ipd
                _orig = _ipd.display
                _ipd.display = lambda *a, **kw: None
                try:
                    import IPython.core.display_functions as _idf
                    _orig2 = _idf.display
                    _idf.display = lambda *a, **kw: None
                except: _idf = None; _orig2 = None
                import sempy_labs._vertipaq as _vp_mod
                _orig_vp = getattr(_vp_mod, 'display', None)
                _vp_mod.display = lambda *a, **kw: None
                try:
                    buf = _io.StringIO()
                    with _redirect(buf):
                        from sempy_labs import vertipaq_analyzer
                        result = vertipaq_analyzer(dataset=ds, workspace=ws, read_stats_from_data=read_stats_cb.value)
                finally:
                    _ipd.display = _orig
                    if _idf and _orig2: _idf.display = _orig2
                    if _orig_vp: _vp_mod.display = _orig_vp
                _vp_data[ds] = result
                _current_model[0] = ds
            except Exception as e:
                set_status(conn_status, f"Error loading '{ds}': {e}", "#ff3b30")
        if _vp_data:
            model_dropdown.options = list(_vp_data.keys())
            model_dropdown.value = _current_model[0] or next(iter(_vp_data))
        _render_subtab()
        set_status(conn_status, f"\u2713 Loaded {len(_vp_data)} model(s).", "#34c759")
        load_btn.disabled = False
        load_btn.description = "Load Memory"
        stop_mem_btn.layout.display = "none"
        _cancel_mem[0] = False

    load_btn.on_click(on_load)

    nav_row = widgets.HBox(
        [load_btn, stop_mem_btn, model_dropdown, read_stats_cb, conn_status],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 8px 0"),
    )
    widget = widgets.VBox([nav_row, subtab_selector, df_container], layout=widgets.Layout(padding="12px", gap="4px"))
    return widget


# ---------------------------------------------------------------------------
# Translations Editor tab (inline)
# ---------------------------------------------------------------------------
def _translations_tab(workspace_input=None, report_input=None):
    """Build the Translations Editor tab — view, auto-translate, edit, and apply translations."""
    from sempy_labs._ui_components import (
        FONT_FAMILY, BORDER_COLOR, GRAY_COLOR, ICON_ACCENT, SECTION_BG,
        status_html, set_status,
    )

    _trans_data = {}  # {obj_key: {lang: value, ...}}
    _original = {}    # snapshot of original translations for diff
    _objects = []     # [(obj_type, table_name, obj_name, tom_path), ...]
    _languages = []   # ["de-DE", "fr-FR", ...]
    _ds_name = [None]

    # Common language codes
    _LANG_OPTIONS = [
        "en-US", "de-DE", "fr-FR", "es-ES", "it-IT", "pt-BR", "nl-NL", "pl-PL",
        "ja-JP", "zh-CN", "ko-KR", "ru-RU", "tr-TR", "ar-SA", "sv-SE",
        "da-DK", "nb-NO", "fi-FI", "cs-CZ", "hu-HU", "ro-RO",
    ]

    load_btn = widgets.Button(description="Load Translations", button_style="primary", layout=widgets.Layout(width="150px"))
    lang_dropdown = widgets.Dropdown(options=_LANG_OPTIONS, value="de-DE", layout=widgets.Layout(width="100px"))
    add_lang_btn = widgets.Button(description="+ Add Language", layout=widgets.Layout(width="120px"))
    # Auto-translate disabled — SynapseML cold start takes too long; manual edit instead
    # auto_translate_btn = widgets.Button(description="🌐 Auto-Translate", button_style="info", layout=widgets.Layout(width="150px"))
    apply_btn = widgets.Button(description="✓ Apply Changes", button_style="success", layout=widgets.Layout(width="140px"), disabled=True)
    conn_status = status_html()

    nav_row = widgets.HBox(
        [load_btn, lang_dropdown, add_lang_btn, apply_btn, conn_status],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 8px 0", flex_wrap="wrap"),
    )

    # Grid container — holds the translations table as widget rows with editable Text inputs
    grid_placeholder = widgets.HTML(
        value=f'<div style="padding:20px; color:{GRAY_COLOR}; font-size:14px; font-family:{FONT_FAMILY}; text-align:center; font-style:italic;">Click Load Translations to view/edit model translations.</div>',
    )
    grid_container = widgets.VBox(
        [grid_placeholder],
        layout=widgets.Layout(
            max_height="500px", overflow_y="auto", overflow_x="auto",
            border=f"1px solid {BORDER_COLOR}", border_radius="8px",
            padding="8px", background_color=SECTION_BG,
        ),
    )

    # Preview of pending changes
    preview_html = widgets.HTML(value="")
    preview_container = widgets.VBox(
        [preview_html],
        layout=widgets.Layout(
            max_height="200px", overflow_y="auto",
            border=f"1px solid {BORDER_COLOR}", border_radius="8px",
            padding="8px", background_color=SECTION_BG,
            display="none",
        ),
    )

    def _obj_key(obj_type, table_name, obj_name):
        return f"{obj_type}:{table_name}:{obj_name}"

    _text_widgets = {}  # {(obj_key, lang): Text widget}

    def _render_grid():
        """Render translations as widget rows with editable Text inputs for each language."""
        _text_widgets.clear()
        if not _objects:
            grid_container.children = [widgets.HTML(f'<div style="color:{GRAY_COLOR};">No objects loaded.</div>')]
            return
        langs = _languages
        col_w_type = "40px"
        col_w_table = "200px"
        col_w_name = "220px"
        col_w_lang = "200px"

        header_style = f"font-size:11px; font-weight:700; font-family:monospace; padding:4px 6px; border-bottom:2px solid {BORDER_COLOR};"
        # Header row
        header_items = [
            widgets.HTML(f'<div style="{header_style} width:{col_w_type};">Type</div>'),
            widgets.HTML(f'<div style="{header_style} width:{col_w_table};">Table</div>'),
            widgets.HTML(f'<div style="{header_style} width:{col_w_name};">Object Name</div>'),
        ]
        for lang in langs:
            header_items.append(widgets.HTML(f'<div style="{header_style} width:{col_w_lang}; color:{ICON_ACCENT};">{lang}</div>'))
        header_row = widgets.HBox(header_items, layout=widgets.Layout(gap="0px"))

        rows = [header_row]
        cell_style = "font-size:11px; font-family:monospace; padding:3px 6px; border-bottom:1px solid #f0f0f0;"
        for obj_type, table_name, obj_name, _ in _objects:
            key = _obj_key(obj_type, table_name, obj_name)
            trans = _trans_data.get(key, {})
            type_icon = "📊" if obj_type == "Table" else "🔢" if obj_type == "Column" else "Σ" if obj_type == "Measure" else "📁"
            row_items = [
                widgets.HTML(f'<div style="{cell_style} width:{col_w_type}; color:#888;">{type_icon}</div>'),
                widgets.HTML(f'<div style="{cell_style} width:{col_w_table};">{table_name}</div>'),
                widgets.HTML(f'<div style="{cell_style} width:{col_w_name}; font-weight:600;">{obj_name}</div>'),
            ]
            for lang in langs:
                val = trans.get(lang, "")
                txt = widgets.Text(
                    value=val,
                    placeholder=obj_name,
                    layout=widgets.Layout(width=col_w_lang),
                )
                txt._trans_key = key
                txt._trans_lang = lang

                def _on_edit(change, _key=key, _lang=lang):
                    _trans_data[_key][_lang] = change["new"]
                    _render_preview()

                txt.observe(_on_edit, names="value")
                _text_widgets[(key, lang)] = txt
                row_items.append(txt)
            rows.append(widgets.HBox(row_items, layout=widgets.Layout(gap="0px", align_items="center")))
        grid_container.children = rows

    def _render_preview():
        """Show pending changes as a diff."""
        changes = []
        for obj_type, table_name, obj_name, _ in _objects:
            key = _obj_key(obj_type, table_name, obj_name)
            trans = _trans_data.get(key, {})
            orig = _original.get(key, {})
            for lang in _languages:
                new_val = trans.get(lang, "")
                old_val = orig.get(lang, "")
                if new_val != old_val:
                    changes.append((obj_type, table_name, obj_name, lang, old_val, new_val))

        if not changes:
            preview_container.layout.display = "none"
            apply_btn.disabled = True
            return

        preview_container.layout.display = ""
        apply_btn.disabled = False
        html = f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; margin-bottom:4px;">PENDING CHANGES ({len(changes)})</div>'
        html += '<table style="border-collapse:collapse; width:100%; font-size:11px;">'
        html += '<tr style="background:#f5f5f5;"><th style="padding:3px 6px;">Object</th><th style="padding:3px 6px;">Language</th><th style="padding:3px 6px;">Old</th><th style="padding:3px 6px;">New</th></tr>'
        for obj_type, table_name, obj_name, lang, old_val, new_val in changes:
            old_display = old_val or "—"
            html += f'<tr><td style="padding:3px 6px; border-bottom:1px solid #f0f0f0;">{table_name}.{obj_name}</td>'
            html += f'<td style="padding:3px 6px; border-bottom:1px solid #f0f0f0; color:{ICON_ACCENT};">{lang}</td>'
            html += f'<td style="padding:3px 6px; border-bottom:1px solid #f0f0f0; color:#888; text-decoration:line-through;">{old_display}</td>'
            html += f'<td style="padding:3px 6px; border-bottom:1px solid #f0f0f0; color:#34c759; font-weight:600;">{new_val}</td></tr>'
        html += '</table>'
        preview_html.value = html

    def on_load(_):
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds_input = report_input.value.strip() if report_input else ""
        if not ds_input:
            set_status(conn_status, "Enter a semantic model name.", "#ff3b30")
            return
        ds = ds_input.split(",")[0].strip()
        # Strip icon prefix
        for pfx in ("\U0001F4C4 ", "\U0001F4CA "):
            if ds.startswith(pfx):
                ds = ds[len(pfx):]
        load_btn.disabled = True
        load_btn.description = "Loading…"
        _ds_name[0] = ds

        try:
            set_status(conn_status, f"Loading translations for '{ds}'…", GRAY_COLOR)
            from sempy_labs.tom import connect_semantic_model
            import Microsoft.AnalysisServices.Tabular as TOM

            _objects.clear()
            _trans_data.clear()
            _original.clear()
            _languages.clear()

            with connect_semantic_model(dataset=ds, readonly=True, workspace=ws) as tom:
                for c in tom.model.Cultures:
                    _languages.append(str(c.Name))
                for table in tom.model.Tables:
                    t_name = str(table.Name)
                    _objects.append(("Table", t_name, t_name, ("table", t_name)))
                    for col in table.Columns:
                        if col.Type == TOM.ColumnType.RowNumber:
                            continue
                        _objects.append(("Column", t_name, str(col.Name), ("column", t_name, str(col.Name))))
                    for m in table.Measures:
                        _objects.append(("Measure", t_name, str(m.Name), ("measure", t_name, str(m.Name))))
                    for h in table.Hierarchies:
                        _objects.append(("Hierarchy", t_name, str(h.Name), ("hierarchy", t_name, str(h.Name))))
                for obj_type, table_name, obj_name, tom_path in _objects:
                    key = _obj_key(obj_type, table_name, obj_name)
                    _trans_data[key] = {}
                    for lang in _languages:
                        try:
                            culture = tom.model.Cultures[lang]
                            if tom_path[0] == "table":
                                obj = tom.model.Tables[table_name]
                            elif tom_path[0] == "column":
                                obj = tom.model.Tables[table_name].Columns[obj_name]
                            elif tom_path[0] == "measure":
                                obj = tom.model.Tables[table_name].Measures[obj_name]
                            elif tom_path[0] == "hierarchy":
                                obj = tom.model.Tables[table_name].Hierarchies[obj_name]
                            else:
                                continue
                            t = culture.ObjectTranslations[obj, TOM.TranslatedProperty.Caption]
                            _trans_data[key][lang] = str(t.Value) if t and t.Value else ""
                        except Exception:
                            _trans_data[key][lang] = ""

            import copy
            _original.update(copy.deepcopy(_trans_data))
            _render_grid()
            n_obj = len(_objects)
            n_lang = len(_languages)
            n_existing = sum(1 for key in _trans_data for lang in _trans_data[key] if _trans_data[key][lang])
            set_status(conn_status, f"✓ {n_obj} objects, {n_lang} language(s), {n_existing} existing translations.", "#34c759")
        except Exception as e:
            set_status(conn_status, f"Error: {str(e)[:300]}", "#ff3b30")
        finally:
            load_btn.disabled = False
            load_btn.description = "Load Translations"

    def on_add_lang(_):
        lang = lang_dropdown.value
        if lang in _languages:
            set_status(conn_status, f"'{lang}' already exists.", "#ff9500")
            return
        _languages.append(lang)
        for key in _trans_data:
            _trans_data[key][lang] = ""
            _original.setdefault(key, {})[lang] = ""
        _render_grid()
        _render_preview()
        set_status(conn_status, f"Added '{lang}'. Edit cells directly to fill translations.", "#34c759")

    # Auto-translate commented out — SynapseML cold start is too slow (~5-10 min).
    # Manual editing via Text widgets instead.
    # def on_auto_translate(_):
    #     ...

    def on_apply(_):
        ds = _ds_name[0]
        if not ds:
            set_status(conn_status, "No model loaded.", "#ff3b30")
            return
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        apply_btn.disabled = True
        apply_btn.description = "Applying…"

        try:
            changes = []
            for obj_type, table_name, obj_name, tom_path in _objects:
                key = _obj_key(obj_type, table_name, obj_name)
                trans = _trans_data.get(key, {})
                orig = _original.get(key, {})
                for lang in _languages:
                    new_val = trans.get(lang, "")
                    old_val = orig.get(lang, "")
                    if new_val != old_val:
                        changes.append((obj_type, table_name, obj_name, tom_path, lang, new_val))

            if not changes:
                set_status(conn_status, "No changes to apply.", "#ff9500")
                apply_btn.disabled = False
                apply_btn.description = "✓ Apply Changes"
                return

            set_status(conn_status, f"Applying {len(changes)} translation(s) via XMLA…", GRAY_COLOR)

            import sys, io as _sio
            _old = sys.stdout
            sys.stdout = _sio.StringIO()
            try:
                from sempy_labs.tom import connect_semantic_model
                with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
                    for obj_type, table_name, obj_name, tom_path, lang, new_val in changes:
                        if tom_path[0] == "table":
                            obj = tom.model.Tables[table_name]
                        elif tom_path[0] == "column":
                            obj = tom.model.Tables[table_name].Columns[obj_name]
                        elif tom_path[0] == "measure":
                            obj = tom.model.Tables[table_name].Measures[obj_name]
                        elif tom_path[0] == "hierarchy":
                            obj = tom.model.Tables[table_name].Hierarchies[obj_name]
                        else:
                            continue
                        tom.set_translation(object=obj, language=lang, property="Name", value=new_val)
                    tom.model.SaveChanges()
            finally:
                sys.stdout = _old

            import copy
            _original.clear()
            _original.update(copy.deepcopy(_trans_data))
            _render_grid()
            _render_preview()
            set_status(conn_status, f"✓ Applied {len(changes)} translation(s).", "#34c759")
        except Exception as e:
            set_status(conn_status, f"Error: {str(e)[:300]}", "#ff3b30")
        finally:
            apply_btn.disabled = False
            apply_btn.description = "✓ Apply Changes"

    load_btn.on_click(on_load)
    add_lang_btn.on_click(on_add_lang)
    # auto_translate_btn.on_click(on_auto_translate)
    apply_btn.on_click(on_apply)

    widget = widgets.VBox(
        [nav_row, grid_container, preview_container],
        layout=widgets.Layout(padding="12px", gap="4px"),
    )
    return widget


# ---------------------------------------------------------------------------
# Best Practice Analyzer tab (inline)
# ---------------------------------------------------------------------------
def _bpa_tab(workspace_input=None, report_input=None, container_ref=None):
    """Build the BPA tab with fix buttons per violation."""
    from sempy_labs._ui_components import (
        FONT_FAMILY, BORDER_COLOR, GRAY_COLOR, ICON_ACCENT, SECTION_BG,
        status_html, set_status,
    )

    # BPA fix functions — imported from standalone files (with inline fallbacks)
    def _make_bpa_fixer(module_path, func_name, inline_fn):
        """Try to import standalone fixer; fall back to inline function."""
        fn = _lazy_import(module_path, func_name)
        return fn if fn is not None else inline_fn

    def _fix_floating_point_inline(ds, ws, table, obj):
        from sempy_labs.tom import connect_semantic_model
        with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
            col = tom.model.Tables[table].Columns[obj]
            import Microsoft.AnalysisServices.Tabular as TOM
            col.DataType = TOM.DataType.Decimal
            tom.model.SaveChanges()
        return f"Changed '{table}'[{obj}] from Double to Decimal"

    def _fix_isavailableinmdx_inline(ds, ws, table, obj):
        from sempy_labs.tom import connect_semantic_model
        with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
            col = tom.model.Tables[table].Columns[obj]
            col.IsAvailableInMDX = False
            tom.model.SaveChanges()
        return f"Set IsAvailableInMDX=False on '{table}'[{obj}]"

    def _fix_description_measure_inline(ds, ws, table, obj):
        from sempy_labs.tom import connect_semantic_model
        with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
            m = tom.model.Tables[table].Measures[obj]
            m.Description = str(m.Expression) if m.Expression else ""
            tom.model.SaveChanges()
        return f"Set description of [{obj}] to its DAX expression"

    def _fix_date_format_inline(ds, ws, table, obj):
        from sempy_labs.tom import connect_semantic_model
        with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
            col = tom.model.Tables[table].Columns[obj]
            col.FormatString = "mm/dd/yyyy"
            tom.model.SaveChanges()
        return f"Set format of '{table}'[{obj}] to mm/dd/yyyy"

    def _fix_month_format_inline(ds, ws, table, obj):
        from sempy_labs.tom import connect_semantic_model
        with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
            col = tom.model.Tables[table].Columns[obj]
            col.FormatString = "MMMM yyyy"
            tom.model.SaveChanges()
        return f"Set format of '{table}'[{obj}] to MMMM yyyy"

    def _fix_integer_format_inline(ds, ws, table, obj):
        from sempy_labs.tom import connect_semantic_model
        with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
            m = tom.model.Tables[table].Measures[obj]
            m.FormatString = "#,0"
            tom.model.SaveChanges()
        return f"Set format of [{obj}] to #,0"

    def _fix_hide_foreign_key_inline(ds, ws, table, obj):
        from sempy_labs.tom import connect_semantic_model
        with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
            col = tom.model.Tables[table].Columns[obj]
            col.IsHidden = True
            tom.model.SaveChanges()
        return f"Hidden '{table}'[{obj}]"

    # Use standalone files when available, with inline fallbacks
    _fix_floating_point = _fix_floating_point_inline
    _fix_isavailableinmdx = _fix_isavailableinmdx_inline
    _fix_description_measure = _fix_description_measure_inline
    _fix_date_format = _fix_date_format_inline
    _fix_month_format = _fix_month_format_inline
    _fix_integer_format = _fix_integer_format_inline
    _fix_hide_foreign_key = _fix_hide_foreign_key_inline

    # Map BPA Rule Names to fix functions (lowercase keys for fuzzy matching)
    # Each fix fn takes (ds, ws, table_name, object_name) and fixes one object.

    def _fix_trim_name(ds, ws, table, obj):
        from sempy_labs.tom import connect_semantic_model
        with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
            if table and obj:
                # Try column, then measure
                try:
                    c = tom.model.Tables[table].Columns[obj]
                    c.Name = c.Name.strip()
                except Exception:
                    try:
                        m = tom.model.Tables[table].Measures[obj]
                        m.Name = m.Name.strip()
                    except Exception:
                        pass
            elif obj:
                try:
                    t = tom.model.Tables[obj]
                    t.Name = t.Name.strip()
                except Exception:
                    pass
            tom.model.SaveChanges()
        return f"Trimmed '{obj}'"

    def _fix_capitalize_name(ds, ws, table, obj):
        from sempy_labs.tom import connect_semantic_model
        with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
            if table and obj:
                try:
                    c = tom.model.Tables[table].Columns[obj]
                    c.Name = c.Name[0].upper() + c.Name[1:]
                except Exception:
                    try:
                        m = tom.model.Tables[table].Measures[obj]
                        m.Name = m.Name[0].upper() + m.Name[1:]
                    except Exception:
                        pass
            elif obj:
                try:
                    t = tom.model.Tables[obj]
                    t.Name = t.Name[0].upper() + t.Name[1:]
                except Exception:
                    pass
            tom.model.SaveChanges()
        return f"Capitalized '{obj}'"

    def _fix_do_not_summarize(ds, ws, table, obj):
        from sempy_labs.tom import connect_semantic_model
        with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
            import Microsoft.AnalysisServices.Tabular as TOM
            col = tom.model.Tables[table].Columns[obj]
            col.SummarizeBy = TOM.AggregateFunction.Default
            tom.model.SaveChanges()
        return f"Set SummarizeBy=None on '{table}'[{obj}]"

    def _fix_mark_pk(ds, ws, table, obj):
        from sempy_labs.tom import connect_semantic_model
        with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
            col = tom.model.Tables[table].Columns[obj]
            col.IsKey = True
            tom.model.SaveChanges()
        return f"Marked '{table}'[{obj}] as primary key"

    def _fix_pct_format(ds, ws, table, obj):
        from sempy_labs.tom import connect_semantic_model
        with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
            m = tom.model.Tables[table].Measures[obj]
            m.FormatString = "#,0.0%;-#,0.0%;#,0.0%"
            tom.model.SaveChanges()
        return f"Set percentage format on [{obj}]"

    def _fix_flag_format(ds, ws, table, obj):
        from sempy_labs.tom import connect_semantic_model
        with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
            col = tom.model.Tables[table].Columns[obj]
            col.FormatString = '"Yes";"Yes";"No"'
            tom.model.SaveChanges()
        return f"Set Yes/No format on '{table}'[{obj}]"

    def _fix_mdx_true(ds, ws, table, obj):
        from sempy_labs.tom import connect_semantic_model
        with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
            col = tom.model.Tables[table].Columns[obj]
            col.IsAvailableInMDX = True
            tom.model.SaveChanges()
        return f"Set IsAvailableInMDX=True on '{table}'[{obj}]"

    _fix_map_raw = {
        "Do not use floating point data types": _fix_floating_point,
        "Do not use floating point data type": _fix_floating_point,
        "Set IsAvailableInMdx to false on non-attribute columns": _fix_isavailableinmdx,
        "Set IsAvailableInMdx to true on necessary columns": _fix_mdx_true,
        "Provide format string for 'Date' columns": _fix_date_format,
        "Provide format string for 'Month' columns": _fix_month_format,
        "Provide format string for measures": _fix_integer_format,
        "Hide foreign keys": _fix_hide_foreign_key,
        "Objects should not start or end with a space": _fix_trim_name,
        "First letter of objects must be capitalized": _fix_capitalize_name,
        "Do not summarize numeric columns": _fix_do_not_summarize,
        "Mark primary keys": _fix_mark_pk,
        "Percentages should be formatted with thousands separators and 1 decimal": _fix_pct_format,
        "Format flag columns as Yes/No value strings": _fix_flag_format,
    }
    _fix_map = {k.lower().strip(): v for k, v in _fix_map_raw.items()}
    _desc_fix_rule = "visible objects with no description"

    def _parse_table_object(obj_name, obj_type):
        """Parse table and object from BPA Object Name. Columns are 'Table'[Col], measures are just Name."""
        import re
        m = re.match(r"'([^']+)'\[([^\]]+)\]", obj_name)
        if m:
            return m.group(1), m.group(2)
        # Measure or table — just the name
        return "", obj_name

    def _is_fixable(rule_name, obj_type):
        key = rule_name.lower().strip()
        return key in _fix_map or (key == _desc_fix_rule and obj_type == "Measure")

    def _apply_fix(ds, ws, rule_name, obj_type, obj_name):
        """Apply a single BPA fix. Returns message or raises."""
        table_name, item_name = _parse_table_object(obj_name, obj_type)
        key = rule_name.lower().strip()
        if key in _fix_map:
            return _fix_map[key](ds, ws, table_name, item_name)
        if key == _desc_fix_rule and obj_type == "Measure":
            return _fix_description_measure(ds, ws, table_name, item_name)
        return None

    load_btn = widgets.Button(description="Run BPA", button_style="primary", layout=widgets.Layout(width="120px"))
    stop_bpa_btn = widgets.Button(description="\u23f9 Stop", button_style="warning", layout=widgets.Layout(width="80px", display="none"))
    _cancel_bpa = [False]
    fix_all_btn = widgets.Button(description="\u26a1 Fix All", button_style="danger", layout=widgets.Layout(width="100px"))
    show_full_btn = widgets.Button(description="\U0001F4CB Show Full BPA", layout=widgets.Layout(width="150px"))
    conn_status = status_html()

    def _on_stop_bpa(_):
        _cancel_bpa[0] = True
    stop_bpa_btn.on_click(_on_stop_bpa)

    nav_row = widgets.HBox(
        [load_btn, stop_bpa_btn, fix_all_btn, show_full_btn, conn_status],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 8px 0"),
    )

    # Fix by rule dropdown
    rule_dropdown = widgets.Dropdown(options=["(no findings)"], value="(no findings)", layout=widgets.Layout(width="320px"))
    fix_rule_btn = widgets.Button(description="\u26a1 Fix Rule", button_style="warning", layout=widgets.Layout(width="100px"))
    # Fix single row
    row_input = widgets.IntText(value=1, layout=widgets.Layout(width="60px"))
    fix_row_btn = widgets.Button(description="Fix Row", button_style="warning", layout=widgets.Layout(width="80px"))
    row_label = widgets.HTML(value=f'<span style="font-size:11px; color:#555; font-family:{FONT_FAMILY};">Row #:</span>')
    fix_row = widgets.HBox(
        [rule_dropdown, fix_rule_btn, row_label, row_input, fix_row_btn],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 8px 0"),
    )

    header_label = widgets.HTML(
        value=f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; font-family:{FONT_FAMILY}; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:2px;">Best Practice Analyzer</div>'
    )
    results_box = widgets.VBox(layout=widgets.Layout(
        max_height="400px", overflow_y="auto",
        border=f"1px solid {BORDER_COLOR}", border_radius="8px",
        padding="8px", background_color=SECTION_BG,
    ))
    results_box.children = [widgets.HTML(
        value=f'<div style="padding:12px; color:{GRAY_COLOR}; font-size:13px; font-family:{FONT_FAMILY}; font-style:italic;">Click Run BPA to scan.</div>'
    )]

    # Native BPA display: category selector + per-category HTML
    _bpa_categories = {}  # {category: html_string}
    bpa_cat_selector = widgets.ToggleButtons(
        options=["(run BPA first)"],
        value="(run BPA first)",
        layout=widgets.Layout(width="100%"),
        style={"button_width": "auto", "font_weight": "bold"},
    )
    native_html_box = widgets.HTML(value="")

    def _on_bpa_cat_change(change):
        cat = change.get("new", "")
        if cat in _bpa_categories:
            native_html_box.value = _bpa_categories[cat]

    bpa_cat_selector.observe(_on_bpa_cat_change, names="value")

    _all_findings = []  # [(ds, rule_name, category, obj_name, obj_type, severity), ...]

    def _render_native_bpa(findings):
        """Render BPA findings as native-style HTML tables grouped by category with row numbers."""
        from collections import OrderedDict
        cats = OrderedDict()
        # Build a global row index for Fix Row to work across categories
        row_idx_map = {}  # (cat_name, item_idx) -> global_row_1based
        global_row = 0
        for ds, rule_name, category, obj_name, obj_type, severity in findings:
            if rule_name.startswith("ERROR"):
                cats.setdefault("Errors", []).append((ds, rule_name, obj_type, obj_name, severity, global_row + 1))
            else:
                cats.setdefault(category, []).append((ds, rule_name, obj_type, obj_name, severity, global_row + 1))
            global_row += 1

        _bpa_categories.clear()
        cat_labels = []
        for cat_name, items in cats.items():
            sev_counts = {}
            for _, _, _, _, sev, _ in items:
                sev_counts[sev] = sev_counts.get(sev, 0) + 1
            sev_badge = " + ".join(f"\u26a0 ({v})" if k in ("2", "3") else f"\u2139 ({v})" for k, v in sorted(sev_counts.items()))
            cat_labels.append(f"{cat_name}\n{sev_badge}")

            # Build per-category table with row numbers
            html = '<table border="1" style="border-collapse:collapse; width:100%; font-size:12px;">'
            html += '<tr><th style="padding:4px 6px; text-align:center; width:35px;">#</th><th style="padding:4px 8px; text-align:left;">Rule Name</th><th style="padding:4px 8px;">Object Type</th><th style="padding:4px 8px;">Object Name</th><th style="padding:4px 8px; text-align:center;">Severity</th></tr>'
            for ds, rule_name, obj_type, obj_name, severity, row_num in items:
                if rule_name.startswith("ERROR"):
                    html += f'<tr><td style="padding:4px 6px; text-align:center; color:#888;">{row_num}</td><td colspan="4" style="color:#ff3b30; padding:4px 8px;">\u274c {ds}: {rule_name}</td></tr>'
                    continue
                sev_icon = "\u26a0\ufe0f" if severity in ("2", "3") else "\u2139\ufe0f"
                has_fix = _is_fixable(rule_name, obj_type)
                fix_badge = ' <span style="color:#34c759; font-size:10px;">[fixable]</span>' if has_fix else ''
                html += f'<tr><td style="padding:4px 6px; text-align:center; color:#888; font-size:11px;">{row_num}</td>'
                html += f'<td style="padding:4px 8px; color:{ICON_ACCENT};">{rule_name}{fix_badge}</td>'
                html += f'<td style="padding:4px 8px;">{obj_type}</td>'
                html += f'<td style="padding:4px 8px;">{obj_name}</td>'
                html += f'<td style="padding:4px 8px; text-align:center;">{sev_icon}</td></tr>'
            html += '</table>'
            _bpa_categories[f"{cat_name}\n{sev_badge}"] = html

        # Update category selector
        if cat_labels:
            bpa_cat_selector.options = cat_labels
            bpa_cat_selector.value = cat_labels[0]
            native_html_box.value = _bpa_categories.get(cat_labels[0], "")
        else:
            bpa_cat_selector.options = ["(no findings)"]
            native_html_box.value = ""

    def on_load(_):
        nonlocal _all_findings
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds_input = report_input.value.strip() if report_input else ""
        items = [x.strip() for x in ds_input.split(",") if x.strip()] if ds_input else []
        if not items:
            set_status(conn_status, "Enter a semantic model name.", "#ff3b30")
            return
        load_btn.disabled = True
        load_btn.description = "Scanning\u2026"
        stop_bpa_btn.layout.display = ""
        _cancel_bpa[0] = False
        import io as _io
        from contextlib import redirect_stdout as _redirect
        import IPython.display as _ipd
        _orig_display = _ipd.display

        _all_findings = []

        for i, ds in enumerate(items):
            if _cancel_bpa[0]:
                set_status(conn_status, f"\u23f9 Stopped after {i}/{len(items)} models.", "#ff9500")
                break
            set_status(conn_status, f"BPA {i+1}/{len(items)}: '{ds}'\u2026", GRAY_COLOR)
            try:
                buf = _io.StringIO()
                _ipd.display = lambda *a, **kw: None
                try:
                    with _redirect(buf):
                        from sempy_labs import run_model_bpa
                        df = run_model_bpa(dataset=ds, workspace=ws, return_dataframe=True)
                finally:
                    _ipd.display = _orig_display

                if df is not None and len(df) > 0:
                    for _, row in df.iterrows():
                        rule_name = str(row.get("Rule Name", ""))
                        category = str(row.get("Category", ""))
                        obj_name = str(row.get("Object Name", ""))
                        obj_type = str(row.get("Object Type", ""))
                        severity = str(row.get("Severity", ""))
                        _all_findings.append((ds, rule_name, category, obj_name, obj_type, severity))
            except Exception as e:
                _all_findings.append((ds, f"ERROR: {e}", "Error", "", "", "3"))

        # Render native-style HTML with ipywidgets category tabs
        _render_native_bpa(_all_findings)

        _update_rule_dropdown()
        n = len([f for f in _all_findings if not f[1].startswith("ERROR")])
        n_fixable = sum(1 for f in _all_findings if _is_fixable(f[1], f[4]))
        results_box.children = [widgets.HTML(
            value=f'<div style="font-size:12px; font-family:{FONT_FAMILY}; color:#555; margin:8px 0 4px 0;">'
            f'{n} finding(s), <b>{n_fixable}</b> auto-fixable</div>'
        )]
        set_status(conn_status, f"\u2713 BPA: {n} finding(s) across {len(items)} model(s).", "#34c759" if n == 0 else "#ff9500")
        load_btn.disabled = False
        load_btn.description = "Run BPA"
        stop_bpa_btn.layout.display = "none"
        _cancel_bpa[0] = False

    def _update_rule_dropdown():
        """Populate rule dropdown with fixable rules + counts."""
        from collections import Counter
        fixable = [(f[1], f[4]) for f in _all_findings if _is_fixable(f[1], f[4])]
        counts = Counter(r for r, _ in fixable)
        if counts:
            opts = [f"{name} ({count})" for name, count in sorted(counts.items())]
            rule_dropdown.options = opts
            rule_dropdown.value = opts[0]
        else:
            rule_dropdown.options = ["(no fixable rules)"]
            rule_dropdown.value = "(no fixable rules)"

    def _build_results(ws):
        if not _all_findings:
            results_box.children = [widgets.HTML(
                value=f'<div style="color:#34c759; font-size:14px; font-weight:600;">\u2713 No violations found.</div>'
            )]
            return

        # Group findings by category (native BPA style with tabs)
        from collections import OrderedDict
        cats = OrderedDict()
        for ds, rule_name, category, obj_name, obj_type, severity in _all_findings:
            if rule_name.startswith("ERROR"):
                cats.setdefault("Errors", []).append((ds, rule_name, category, obj_name, obj_type, severity))
                continue
            cats.setdefault(category, []).append((ds, rule_name, category, obj_name, obj_type, severity))

        # Use unique IDs to avoid JS collision with other tabs
        import random as _rnd
        uid = _rnd.randint(1000, 9999)

        # CSS + JS (scoped with uid)
        styles = f'''<style>
        .bpa-tab-{uid} {{ overflow:hidden; border:1px solid #ccc; background:#f1f1f1; display:flex; flex-wrap:wrap; }}
        .bpa-tab-{uid} button {{ background:inherit; border:none; outline:none; cursor:pointer; padding:8px 12px; transition:0.3s; font-size:11px; }}
        .bpa-tab-{uid} button:hover {{ background:#ddd; }}
        .bpa-tab-{uid} button.active {{ background:#ccc; font-weight:bold; }}
        .bpa-tc-{uid} {{ display:none; padding:4px 8px; border:1px solid #ccc; border-top:none; max-height:350px; overflow-y:auto; }}
        .bpa-tc-{uid}.active {{ display:block; }}
        .bpa-tt {{ position:relative; display:inline-block; }}
        .bpa-tt .bpa-ttp {{ visibility:hidden; width:280px; background:#555; color:#fff; text-align:center; border-radius:6px; padding:5px; position:absolute; z-index:1; bottom:125%; left:50%; margin-left:-140px; opacity:0; transition:opacity 0.3s; font-size:11px; }}
        .bpa-tt:hover .bpa-ttp {{ visibility:visible; opacity:1; }}
        </style>'''

        script = f'''<script>
        function bpaTab{uid}(evt, tabId) {{
            var tc = document.querySelectorAll('.bpa-tc-{uid}');
            for (var i=0; i<tc.length; i++) tc[i].style.display='none';
            var btns = document.querySelectorAll('.bpa-tab-{uid} button');
            for (var i=0; i<btns.length; i++) btns[i].className = btns[i].className.replace(' active','');
            document.getElementById(tabId).style.display='block';
            evt.currentTarget.className += ' active';
        }}
        </script>'''

        tab_html = f'<div class="bpa-tab-{uid}">'
        content_html = ""
        n_fixable = 0

        for cat_idx, (cat_name, findings) in enumerate(cats.items()):
            tab_id = f"bpa{uid}_{cat_idx}"
            active = " active" if cat_idx == 0 else ""
            # Severity summary
            sev_counts = {}
            for _, _, _, _, _, sev in findings:
                sev_counts[sev] = sev_counts.get(sev, 0) + 1
            summary = " + ".join(f"{v} (Sev {k})" for k, v in sorted(sev_counts.items()))
            tab_html += f'<button class="{active}" onclick="bpaTab{uid}(event,\'{tab_id}\')">{cat_name}<br><span style="font-size:10px;color:#888;">{summary}</span></button>'

            content_html += f'<div id="{tab_id}" class="bpa-tc-{uid}{active}">'
            content_html += '<table style="border-collapse:collapse; width:100%; font-size:11px; font-family:monospace;">'
            content_html += '<tr style="background:#f5f5f5;"><th style="padding:3px 6px; text-align:left;">Model</th><th style="padding:3px 6px; text-align:left;">Rule</th><th style="padding:3px 6px; text-align:left;">Type</th><th style="padding:3px 6px; text-align:left;">Object</th><th style="padding:3px 6px; text-align:center;">Sev</th><th style="padding:3px 6px; text-align:center;">Fix</th></tr>'
            for ds, rule_name, category, obj_name, obj_type, severity in findings:
                if rule_name.startswith("ERROR"):
                    content_html += f'<tr><td colspan="6" style="color:#ff3b30; padding:2px 6px;">\u274c {ds}: {rule_name}</td></tr>'
                    continue
                sev_color = "#ff3b30" if severity in ("3",) else "#ff9500" if severity in ("2",) else "#888"
                has_fix = _is_fixable(rule_name, obj_type)
                if has_fix:
                    n_fixable += 1
                fix_icon = '<span style="color:#34c759;">\u2713</span>' if has_fix else '\u2014'
                content_html += f'<tr><td style="padding:2px 6px; border-bottom:1px solid #f0f0f0;" title="{ds}">{ds[:16]}</td>'
                content_html += f'<td style="padding:2px 6px; border-bottom:1px solid #f0f0f0; color:{ICON_ACCENT};" title="{rule_name}">{rule_name[:40]}</td>'
                content_html += f'<td style="padding:2px 6px; border-bottom:1px solid #f0f0f0; color:#888;">{obj_type[:10]}</td>'
                content_html += f'<td style="padding:2px 6px; border-bottom:1px solid #f0f0f0;" title="{obj_name}">{obj_name[:40]}</td>'
                content_html += f'<td style="padding:2px 6px; border-bottom:1px solid #f0f0f0; text-align:center; color:{sev_color}; font-weight:600;">{severity}</td>'
                content_html += f'<td style="padding:2px 6px; border-bottom:1px solid #f0f0f0; text-align:center;">{fix_icon}</td></tr>'
            content_html += '</table></div>'
        tab_html += '</div>'

        full_html = styles + tab_html + content_html + script

        summary = widgets.HTML(
            value=f'<div style="font-size:12px; font-family:{FONT_FAMILY}; color:#555; margin:8px 0 4px 0;">'
            f'{len(_all_findings)} finding(s), <b>{n_fixable}</b> auto-fixable</div>'
        )
        results_box.children = [widgets.HTML(value=full_html), summary]

    def on_fix_all(_):
        """Fix all fixable violations."""
        if not _all_findings:
            return
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        fix_all_btn.disabled = True
        fix_all_btn.description = "Fixing\u2026"
        fixed = 0
        errors = 0
        for ds, rule_name, category, obj_name, obj_type, severity in _all_findings:
            if rule_name.startswith("ERROR"):
                continue
            try:
                result = _apply_fix(ds, ws, rule_name, obj_type, obj_name)
                if result:
                    fixed += 1
            except Exception:
                errors += 1
        set_status(conn_status, f"\u2713 Fixed {fixed}, {errors} error(s).", "#34c759" if errors == 0 else "#ff9500")
        fix_all_btn.disabled = False
        fix_all_btn.description = "\u26a1 Fix All"

    def on_fix_rule(_):
        """Fix all violations of the selected rule."""
        selected = rule_dropdown.value
        if not selected or selected.startswith("("):
            return
        # Extract rule name (strip count suffix)
        import re
        m = re.match(r"(.+)\s+\(\d+\)$", selected)
        target_rule = m.group(1) if m else selected
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        fix_rule_btn.disabled = True
        fix_rule_btn.description = "Fixing\u2026"
        fixed = 0
        errors = 0
        for ds, rule_name, category, obj_name, obj_type, severity in _all_findings:
            if rule_name != target_rule:
                continue
            try:
                result = _apply_fix(ds, ws, rule_name, obj_type, obj_name)
                if result:
                    fixed += 1
            except Exception:
                errors += 1
        set_status(conn_status, f"\u2713 '{target_rule}': fixed {fixed}, {errors} error(s).", "#34c759" if errors == 0 else "#ff9500")
        fix_rule_btn.disabled = False
        fix_rule_btn.description = "\u26a1 Fix Rule"

    def on_fix_row(_):
        """Fix a single violation by row number."""
        idx = row_input.value - 1  # 1-based to 0-based
        if idx < 0 or idx >= len(_all_findings):
            set_status(conn_status, f"Row {idx+1} out of range (1-{len(_all_findings)}).", "#ff3b30")
            return
        ds, rule_name, category, obj_name, obj_type, severity = _all_findings[idx]
        if rule_name.startswith("ERROR"):
            set_status(conn_status, "Cannot fix error row.", "#ff3b30")
            return
        if not _is_fixable(rule_name, obj_type):
            set_status(conn_status, f"Row {idx+1} has no auto-fix for '{rule_name}'.", "#ff9500")
            return
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        try:
            msg = _apply_fix(ds, ws, rule_name, obj_type, obj_name)
            set_status(conn_status, f"\u2713 Row {idx+1}: {msg}", "#34c759")
        except Exception as e:
            set_status(conn_status, f"Error row {idx+1}: {e}", "#ff3b30")

    load_btn.on_click(on_load)
    fix_all_btn.on_click(on_fix_all)
    fix_rule_btn.on_click(on_fix_rule)
    fix_row_btn.on_click(on_fix_row)

    # Output area for the native BPA HTML (rendered below the widget)
    bpa_output = widgets.Output()

    def on_show_full(_):
        """Run run_model_bpa natively — closes PBI Fixer and renders directly."""
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds_input = report_input.value.strip() if report_input else ""
        items = [x.strip() for x in ds_input.split(",") if x.strip()] if ds_input else []
        if not items:
            set_status(conn_status, "Enter a semantic model name.", "#ff3b30")
            return
        show_full_btn.disabled = True
        # Countdown and close main container
        import time as _time
        for sec in (3, 2, 1):
            show_full_btn.description = f"Closing in {sec}\u2026"
            _time.sleep(1)
        if container_ref and container_ref[0] is not None:
            container_ref[0].close()
        # Render directly into notebook cell output (container is closed)
        for ds in items:
            try:
                from sempy_labs import run_model_bpa
                run_model_bpa(dataset=ds, workspace=ws)
            except Exception as e:
                from IPython.display import display, HTML
                display(HTML(f'<div style="color:red;">Error for {ds}: {e}</div>'))

    show_full_btn.on_click(on_show_full)

    native_html_container = widgets.VBox(
        [native_html_box],
        layout=widgets.Layout(
            max_height="400px", overflow_y="auto",
            border=f"1px solid {BORDER_COLOR}", border_radius="8px",
            padding="8px", background_color=SECTION_BG,
        ),
    )
    widget = widgets.VBox([nav_row, fix_row, header_label, bpa_cat_selector, native_html_container, results_box, bpa_output], layout=widgets.Layout(padding="12px", gap="4px"))
    return widget


# ---------------------------------------------------------------------------
# Report BPA tab (inline)
# ---------------------------------------------------------------------------
def _report_bpa_tab(workspace_input=None, report_input=None, container_ref=None):
    """Build the Report BPA tab — runs run_report_bpa and shows results."""
    from sempy_labs._ui_components import (
        FONT_FAMILY, BORDER_COLOR, GRAY_COLOR, ICON_ACCENT, SECTION_BG,
        status_html, set_status,
    )

    load_btn = widgets.Button(description="Run Report BPA", button_style="primary", layout=widgets.Layout(width="140px"))
    show_full_btn = widgets.Button(description="\U0001F4CB Show Native", layout=widgets.Layout(width="130px"))
    conn_status = status_html()
    nav_row = widgets.HBox(
        [load_btn, show_full_btn, conn_status],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 8px 0"),
    )
    header_label = widgets.HTML(
        value=f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; font-family:{FONT_FAMILY}; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:2px;">Report Best Practice Analyzer</div>'
        f'<div style="font-size:11px; color:#888; font-family:{FONT_FAMILY}; font-style:italic; margin-bottom:4px;">'
        f'\u2139\ufe0f Requires PBIR format. Auto-converts PBIRLegacy if needed.</div>'
    )
    results_box = widgets.VBox(layout=widgets.Layout(
        max_height="400px", overflow_y="auto",
        border=f"1px solid {BORDER_COLOR}", border_radius="8px",
        padding="8px", background_color=SECTION_BG,
    ))
    results_box.children = [widgets.HTML(
        value=f'<div style="padding:12px; color:{GRAY_COLOR}; font-size:13px; font-family:{FONT_FAMILY}; font-style:italic;">Click Run Report BPA to scan.</div>'
    )]
    native_output = widgets.Output()

    _all_findings = []

    def on_load(_):
        nonlocal _all_findings
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        rpt_input = report_input.value.strip() if report_input else ""
        items = [x.strip() for x in rpt_input.split(",") if x.strip()] if rpt_input else []
        if not items:
            set_status(conn_status, "Enter a report name.", "#ff3b30")
            return
        load_btn.disabled = True
        load_btn.description = "Scanning\u2026"
        import io as _io
        from contextlib import redirect_stdout as _redirect
        import IPython.display as _ipd
        _orig_display = _ipd.display

        _all_findings = []

        for i, rpt in enumerate(items):
            set_status(conn_status, f"Report BPA {i+1}/{len(items)}: '{rpt}'\u2026", GRAY_COLOR)
            try:
                buf = _io.StringIO()
                _ipd.display = lambda *a, **kw: None
                try:
                    with _redirect(buf):
                        from sempy_labs.report import run_report_bpa
                        df = run_report_bpa(report=rpt, workspace=ws, return_dataframe=True)
                finally:
                    _ipd.display = _orig_display
                if df is not None and len(df) > 0:
                    for _, row in df.iterrows():
                        _all_findings.append((
                            rpt,
                            str(row.get("Rule Name", "")),
                            str(row.get("Category", "")),
                            str(row.get("Object Name", "")),
                            str(row.get("Object Type", "")),
                            str(row.get("Severity", "")),
                        ))
            except Exception as e:
                err_msg = str(e)
                if "PBIR format" in err_msg or "ReportWrapper" in err_msg:
                    set_status(conn_status, f"\u26a0\ufe0f '{rpt}' is PBIRLegacy \u2014 converting\u2026", "#ff9500")
                    try:
                        import sempy_labs.report as _rep
                        _rep.upgrade_to_pbir(report=rpt, workspace=ws)
                        set_status(conn_status, f"\u2713 Converted. Retrying scan\u2026", "#34c759")
                        buf = _io.StringIO()
                        _ipd.display = lambda *a, **kw: None
                        try:
                            with _redirect(buf):
                                from sempy_labs.report import run_report_bpa
                                df = run_report_bpa(report=rpt, workspace=ws, return_dataframe=True)
                        finally:
                            _ipd.display = _orig_display
                        if df is not None and len(df) > 0:
                            for _, row in df.iterrows():
                                _all_findings.append((
                                    rpt,
                                    str(row.get("Rule Name", "")),
                                    str(row.get("Category", "")),
                                    str(row.get("Object Name", "")),
                                    str(row.get("Object Type", "")),
                                    str(row.get("Severity", "")),
                                ))
                    except Exception as e2:
                        _all_findings.append((rpt, f"ERROR: {e2}", "Error", "", "", "3"))
                else:
                    _all_findings.append((rpt, f"ERROR: {e}", "Error", "", "", "3"))

        _build_results()
        n = len([f for f in _all_findings if not f[1].startswith("ERROR")])
        set_status(conn_status, f"\u2713 Report BPA: {n} finding(s) across {len(items)} report(s).", "#34c759" if n == 0 else "#ff9500")
        load_btn.disabled = False
        load_btn.description = "Run Report BPA"

    def _build_results():
        if not _all_findings:
            results_box.children = [widgets.HTML(
                value=f'<div style="color:#34c759; font-size:14px; font-weight:600;">\u2713 No violations found.</div>'
            )]
            return
        html = '<div style="overflow-x:auto;"><table style="border-collapse:collapse; min-width:100%; font-size:11px; font-family:monospace;">'
        html += '<tr style="background:#f5f5f5; position:sticky; top:0; z-index:1;">'
        for hdr in ["#", "Report", "Rule", "Type", "Object", "Sev"]:
            html += f'<th style="text-align:left; padding:4px 8px; border-bottom:2px solid {BORDER_COLOR}; white-space:nowrap;">{hdr}</th>'
        html += '</tr>'
        for idx, (rpt, rule_name, category, obj_name, obj_type, severity) in enumerate(_all_findings):
            if rule_name.startswith("ERROR"):
                html += f'<tr><td colspan="6" style="color:#ff3b30; padding:3px 8px;">\u274c {rpt}: {rule_name}</td></tr>'
                continue
            sev_color = "#ff3b30" if severity in ("3",) else "#ff9500" if severity in ("2",) else "#888"
            html += '<tr>'
            html += f'<td style="padding:3px 8px; border-bottom:1px solid #f0f0f0; color:#888;">{idx+1}</td>'
            html += f'<td style="padding:3px 8px; border-bottom:1px solid #f0f0f0; white-space:nowrap;" title="{rpt}">{rpt[:16]}</td>'
            html += f'<td style="padding:3px 8px; border-bottom:1px solid #f0f0f0; color:{ICON_ACCENT}; white-space:nowrap;" title="{rule_name}">{rule_name[:40]}</td>'
            html += f'<td style="padding:3px 8px; border-bottom:1px solid #f0f0f0; color:#888;">{obj_type[:12]}</td>'
            html += f'<td style="padding:3px 8px; border-bottom:1px solid #f0f0f0; white-space:nowrap;" title="{obj_name}">{obj_name[:40]}</td>'
            html += f'<td style="padding:3px 8px; border-bottom:1px solid #f0f0f0; color:{sev_color}; font-weight:600;">{severity}</td>'
            html += '</tr>'
        html += '</table></div>'
        summary = widgets.HTML(
            value=f'<div style="font-size:12px; font-family:{FONT_FAMILY}; color:#555; margin:8px 0 4px 0;">'
            f'{len(_all_findings)} finding(s)</div>'
        )
        results_box.children = [widgets.HTML(value=html), summary]

    def on_show_native(_):
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        rpt_input = report_input.value.strip() if report_input else ""
        items = [x.strip() for x in rpt_input.split(",") if x.strip()] if rpt_input else []
        if not items:
            set_status(conn_status, "Enter a report name.", "#ff3b30")
            return
        show_full_btn.disabled = True
        # Countdown and close main container
        import time as _time
        for sec in (3, 2, 1):
            show_full_btn.description = f"Closing in {sec}\u2026"
            _time.sleep(1)
        if container_ref and container_ref[0] is not None:
            container_ref[0].close()
        # Render directly into notebook cell output (container is closed)
        for rpt in items:
            try:
                from sempy_labs.report import run_report_bpa
                run_report_bpa(report=rpt, workspace=ws)
            except Exception as e:
                from IPython.display import display, HTML
                display(HTML(f'<div style="color:red;">Error for {rpt}: {e}</div>'))

    load_btn.on_click(on_load)
    show_full_btn.on_click(on_show_native)

    widget = widgets.VBox([nav_row, header_label, results_box, native_output], layout=widgets.Layout(padding="12px", gap="4px"))
    return widget


# ---------------------------------------------------------------------------
# Delta Analyzer tab (inline)
# ---------------------------------------------------------------------------
def _delta_analyzer_tab(workspace_input=None, report_input=None, container_ref=None):
    """Build the Delta Analyzer tab with full DataFrame subtabs."""
    from sempy_labs._ui_components import (
        FONT_FAMILY, BORDER_COLOR, GRAY_COLOR, ICON_ACCENT, SECTION_BG,
        status_html, set_status,
    )

    _da_data = {}  # dict of DataFrames from delta_analyzer

    table_input = widgets.Text(placeholder="Delta table name", layout=widgets.Layout(width="200px"))
    lakehouse_input = widgets.Text(placeholder="Lakehouse (optional)", layout=widgets.Layout(width="180px"))
    schema_input = widgets.Text(placeholder="Schema (optional)", layout=widgets.Layout(width="130px"))
    col_stats_cb = widgets.Checkbox(value=True, description="Column stats", indent=False, layout=widgets.Layout(width="120px"))
    cardinality_cb = widgets.Checkbox(value=False, description="Cardinality", indent=False, layout=widgets.Layout(width="110px"))
    load_btn = widgets.Button(description="Analyze", button_style="primary", layout=widgets.Layout(width="100px"))
    show_native_btn = widgets.Button(description="\U0001F4CB Show Native", layout=widgets.Layout(width="130px"))
    conn_status = status_html()

    input_row = widgets.HBox(
        [table_input, lakehouse_input, schema_input, col_stats_cb, cardinality_cb],
        layout=widgets.Layout(align_items="center", gap="6px", margin="0 0 4px 0"),
    )
    nav_row = widgets.HBox(
        [load_btn, show_native_btn, conn_status],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 8px 0"),
    )

    _DF_TABS = ["Summary", "Parquet Files", "Row Groups", "Column Chunks", "Columns"]
    subtab_selector = widgets.ToggleButtons(
        options=_DF_TABS,
        value="Summary",
        layout=widgets.Layout(width="100%"),
        style={"button_width": "auto", "font_weight": "bold"},
    )
    df_html = widgets.HTML(
        value=f'<div style="padding:12px; color:{GRAY_COLOR}; font-size:13px; font-family:{FONT_FAMILY}; font-style:italic;">Enter a delta table name and click Analyze.</div>',
    )
    df_container = widgets.VBox(
        [df_html],
        layout=widgets.Layout(
            max_height="450px", overflow_y="auto", overflow_x="auto",
            border=f"1px solid {BORDER_COLOR}", border_radius="8px",
            padding="8px", background_color=SECTION_BG,
        ),
    )
    native_output = widgets.Output()

    def _fmt_bytes(n):
        try:
            if n is None or (isinstance(n, float) and n != n):
                return "\u2014"
            n = int(n)
        except (TypeError, ValueError):
            return "\u2014"
        if n < 1024:
            return f"{n} B"
        if n < 1024 * 1024:
            return f"{n / 1024:.1f} KB"
        if n < 1024 * 1024 * 1024:
            return f"{n / (1024 * 1024):.1f} MB"
        return f"{n / (1024 * 1024 * 1024):.2f} GB"

    def _fmt_val(val, col_name):
        if val is None or (isinstance(val, float) and val != val):
            return "\u2014"
        if "Size" in col_name or "Bytes" in col_name:
            return _fmt_bytes(val)
        if isinstance(val, (int, float)):
            if isinstance(val, float) and val == int(val):
                try:
                    return f"{int(val):,}"
                except (TypeError, ValueError):
                    return str(val)
            if isinstance(val, float):
                return f"{val:,.1f}"
            return f"{val:,}"
        return str(val)

    def _df_to_html(df, sort_by=None):
        if df is None or len(df) == 0:
            return f'<div style="color:{GRAY_COLOR}; font-size:13px;">No data available.</div>'
        if sort_by and sort_by in df.columns:
            df = df.sort_values(sort_by, ascending=False)
        html = '<div style="overflow-x:auto;"><table style="border-collapse:collapse; min-width:100%; font-size:11px; font-family:monospace;">'
        html += '<tr style="background:#f5f5f5; position:sticky; top:0; z-index:1;">'
        for col in df.columns:
            align = "right" if any(k in col for k in ("Size", "Count", "Bytes", "Rows", "%", "Cardinality", "Min", "Max", "Avg")) else "left"
            html += f'<th style="text-align:{align}; padding:4px 8px; border-bottom:2px solid {BORDER_COLOR}; white-space:nowrap;">{col}</th>'
        html += '</tr>'
        for _, row in df.iterrows():
            html += '<tr>'
            for col in df.columns:
                val = row.get(col, "")
                align = "right" if any(k in col for k in ("Size", "Count", "Bytes", "Rows", "%", "Cardinality", "Min", "Max", "Avg")) else "left"
                html += f'<td style="text-align:{align}; padding:3px 8px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_fmt_val(val, col)}</td>'
            html += '</tr>'
        html += '</table></div>'
        return html

    def _render_subtab(tab_name=None):
        tab_name = tab_name or subtab_selector.value
        df = _da_data.get(tab_name)
        # Summary is single-row — render vertically
        if tab_name == "Summary" and df is not None and len(df) > 0:
            r = df.iloc[0]
            html = f'<table style="border-collapse:collapse; font-size:13px; font-family:{FONT_FAMILY}; width:100%;">'
            for col in df.columns:
                val = _fmt_val(r.get(col, ""), col)
                html += f'<tr><td style="padding:6px 12px; font-weight:600; color:#555; border-bottom:1px solid #f0f0f0; width:250px;">{col}</td>'
                html += f'<td style="padding:6px 12px; border-bottom:1px solid #f0f0f0;">{val}</td></tr>'
            html += '</table>'
            df_html.value = html
            return
        sort_by = None
        if df is not None:
            for c in ["Compressed Size", "Total Size", "Size", "Uncompressed Size"]:
                if c in df.columns:
                    sort_by = c
                    break
        df_html.value = _df_to_html(df, sort_by=sort_by)

    def on_subtab_change(change):
        _render_subtab(change.get("new"))

    subtab_selector.observe(on_subtab_change, names="value")

    def on_load(_):
        nonlocal _da_data
        t_name = table_input.value.strip()
        if not t_name:
            set_status(conn_status, "Enter a delta table name.", "#ff3b30")
            return
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        lh = lakehouse_input.value.strip() or None
        sch = schema_input.value.strip() or None
        load_btn.disabled = True
        load_btn.description = "Analyzing\u2026"
        set_status(conn_status, f"Analyzing '{t_name}'\u2026", GRAY_COLOR)
        try:
            import io as _io
            from contextlib import redirect_stdout as _redirect
            import IPython.display as _ipd
            _orig_display = _ipd.display
            _ipd.display = lambda *a, **kw: None
            try:
                buf = _io.StringIO()
                with _redirect(buf):
                    from sempy_labs import delta_analyzer as _da_fn
                    result = _da_fn(
                        table_name=t_name,
                        lakehouse=lh,
                        workspace=ws,
                        column_stats=col_stats_cb.value,
                        skip_cardinality=not cardinality_cb.value,
                        schema=sch,
                        visualize=False,
                    )
            finally:
                _ipd.display = _orig_display
            _da_data = result
            _render_subtab()
            n_keys = len(result)
            set_status(conn_status, f"\u2713 Delta Analyzer: {n_keys} views loaded for '{t_name}'.", "#34c759")
        except Exception as e:
            set_status(conn_status, f"Error: {e}", "#ff3b30")
        finally:
            load_btn.disabled = False
            load_btn.description = "Analyze"

    def on_show_native(_):
        """Run delta_analyzer with visualize=True and show the native HTML output below."""
        t_name = table_input.value.strip()
        if not t_name:
            set_status(conn_status, "Enter a delta table name.", "#ff3b30")
            return
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        lh = lakehouse_input.value.strip() or None
        sch = schema_input.value.strip() or None
        show_native_btn.disabled = True
        # Countdown and close main container
        import time as _time
        for sec in (3, 2, 1):
            show_native_btn.description = f"Closing in {sec}\u2026"
            _time.sleep(1)
        if container_ref and container_ref[0] is not None:
            container_ref[0].close()
        # Render directly into notebook cell output (container is closed)
        try:
            from sempy_labs import delta_analyzer as _da_fn
            _da_fn(
                table_name=t_name,
                lakehouse=lh,
                workspace=ws,
                column_stats=col_stats_cb.value,
                skip_cardinality=not cardinality_cb.value,
                schema=sch,
                visualize=True,
            )
        except Exception as e:
            from IPython.display import display, HTML
            display(HTML(f'<div style="color:red;">Error: {e}</div>'))

    load_btn.on_click(on_load)
    show_native_btn.on_click(on_show_native)

    header_label = widgets.HTML(
        value=f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; font-family:{FONT_FAMILY}; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:2px;">Delta Analyzer</div>'
        f'<div style="font-size:11px; color:#888; font-family:{FONT_FAMILY}; font-style:italic; margin-bottom:4px;">'
        f'Analyzes delta table structure: parquet files, row groups, column chunks, and column statistics.</div>'
    )

    tab_widget = widgets.VBox([input_row, nav_row, header_label, subtab_selector, df_container, native_output], layout=widgets.Layout(padding="12px", gap="4px"))
    return tab_widget

# ---------------------------------------------------------------------------
# Timeout constants
# ---------------------------------------------------------------------------
_TOTAL_TIMEOUT = 300  # 5 minutes hard wall-clock limit


def _check_report_format(report_name, workspace):
    """
    Check report format via Fabric REST API.
    Returns 'PBIR', 'PBIRLegacy', etc., or None on error.
    """
    try:
        from sempy_labs._helper_functions import (
            resolve_workspace_name_and_id,
            resolve_item_name_and_id,
            _base_api,
        )
        _, ws_id = resolve_workspace_name_and_id(workspace)
        _, rpt_id = resolve_item_name_and_id(
            item=report_name, type="Report", workspace=ws_id
        )
        url = f"/v1.0/myorg/groups/{ws_id}/reports"
        response = _base_api(request=url, client="fabric_sp")
        for rpt in response.json().get("value", []):
            if rpt.get("id") == str(rpt_id):
                return rpt.get("format")
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Report Prototype tab (inline)
# ---------------------------------------------------------------------------
def _prototype_tab(workspace_input=None, report_input=None):
    """Build the Report Prototype tab that generates an SVG diagram of all report pages."""
    from sempy_labs._ui_components import (
        FONT_FAMILY, BORDER_COLOR, GRAY_COLOR, ICON_ACCENT, SECTION_BG,
        status_html, set_status,
    )

    generate_btn = widgets.Button(description="\u270F\uFE0F Generate Prototype", button_style="primary", layout=widgets.Layout(width="200px"))
    screenshots_cb = widgets.Checkbox(value=False, description="Screenshots", indent=False, layout=widgets.Layout(width="auto"))
    hidden_cb = widgets.Checkbox(value=False, description="Include hidden pages", indent=False, layout=widgets.Layout(width="auto"))
    export_excalidraw_btn = widgets.Button(description="\u2B07 Save .excalidraw", layout=widgets.Layout(width="150px", display="none"))
    export_svg_btn = widgets.Button(description="\u2B07 Save .svg", layout=widgets.Layout(width="120px", display="none"))
    conn_status = status_html()

    nav_row = widgets.HBox(
        [generate_btn, screenshots_cb, hidden_cb, export_excalidraw_btn, export_svg_btn, conn_status],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 8px 0"),
    )

    svg_display = widgets.HTML(value=f'<div style="padding:20px; color:{GRAY_COLOR}; font-size:14px; font-family:{FONT_FAMILY}; text-align:center; font-style:italic;">Click "Generate Prototype" to create a visual map of the report pages.</div>')
    svg_container = widgets.VBox(
        [svg_display],
        layout=widgets.Layout(
            overflow_x="auto", overflow_y="auto",
            max_height="600px",
            border=f"1px solid {BORDER_COLOR}", border_radius="8px",
            padding="8px", background_color="#fff",
        ),
    )

    _svg_cache = [None]
    _excalidraw_cache = [None]
    _page_images = {}  # page_name -> base64 png
    _rpt_name = [None]  # cached report name for export filenames

    # Layout constants
    _THUMB_W = 480
    _THUMB_H = 270
    _PAD_X = 40
    _PAD_Y = 60
    _COLS = 4
    _HEADER_H = 30
    _FOOTER_H = 25

    def _generate(_):
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        rpt_input = report_input.value.strip() if report_input else ""
        if not rpt_input:
            set_status(conn_status, "Enter a report name.", "#ff3b30")
            return
        rpt = rpt_input.split(",")[0].strip()
        # Strip icon prefix
        for pfx in ("\U0001F4C4 ", "\U0001F4CA "):
            if rpt.startswith(pfx):
                rpt = rpt[len(pfx):]

        generate_btn.disabled = True
        generate_btn.description = "Generating\u2026"
        _page_images.clear()
        _rpt_name[0] = rpt

        def _proto_progress(done, total, page_name):
            set_status(conn_status, f"Exporting screenshots: {done}/{total} \u2014 {page_name}", GRAY_COLOR)

        try:
            set_status(conn_status, "Generating prototype\u2026", GRAY_COLOR)

            # Use standalone module — redirect stdout to prevent Fabric output overflow
            from sempy_labs.report._report_prototype import generate_report_prototype
            import io as _io, sys as _sys, re as _re
            _old_stdout = _sys.stdout
            _sys.stdout = _io.StringIO()
            try:
                result = generate_report_prototype(
                    report=rpt,
                    workspace=ws,
                    screenshots=screenshots_cb.value,
                    include_hidden=hidden_cb.value,
                    on_progress=_proto_progress,
                )
            finally:
                _sys.stdout = _old_stdout

            svg = result["svg"]
            excalidraw = result["excalidraw"]
            total = len(result["pages"])
            n_screenshots = result["screenshots"]
            export_errors = result["errors"]
            n_nav = len(result.get("nav_edges", []))

            _svg_cache[0] = svg
            _excalidraw_cache[0] = excalidraw

            # Large SVGs (>2MB, e.g. 20+ pages with screenshots) exceed Fabric's
            # cell output limit.  Strip base64 images for the inline display;
            # full data is still available via the export buttons.
            _MAX_DISPLAY_BYTES = 2 * 1024 * 1024
            if len(svg.encode("utf-8", errors="ignore")) > _MAX_DISPLAY_BYTES:
                display_svg = _re.sub(
                    r'href="data:image/png;base64,[^"]*"',
                    'href=""',
                    svg,
                )
                svg_display.value = display_svg
                size_msg = " (inline preview without images \u2014 use export buttons for full version)"
            else:
                svg_display.value = svg
                size_msg = ""

            export_excalidraw_btn.layout.display = ""
            export_svg_btn.layout.display = ""
            err_msg = f" Export errors: {'; '.join(export_errors[:2])}" if export_errors else ""
            nav_msg = f", {n_nav} nav links" if n_nav else ""
            set_status(conn_status, f"\u2713 Prototype: {total} pages, {n_screenshots} screenshots{nav_msg}.{err_msg}{size_msg}", "#34c759" if not export_errors else "#ff9500")

        except Exception as e:
            set_status(conn_status, f"Error: {str(e)[:300]}", "#ff3b30")
        finally:
            generate_btn.disabled = False
            generate_btn.description = "\u270F\uFE0F Generate Prototype"

    def _on_export_excalidraw(_):
        if not _excalidraw_cache[0]:
            return
        try:
            from sempy_labs._helper_functions import _mount
            local_path = _mount()
            safe_name = (_rpt_name[0] or "report").replace(" ", "_").replace("/", "_")
            fname = f"{safe_name}_prototype.excalidraw"
            path = f"{local_path}/Files/{fname}"
            with open(path, "w", encoding="utf-8") as f:
                f.write(_excalidraw_cache[0])
            set_status(conn_status, f"\u2713 Saved {fname} to lakehouse Files.", "#34c759")
        except Exception as e:
            set_status(conn_status, f"Error saving: {e}", "#ff3b30")

    def _on_export_svg(_):
        if not _svg_cache[0]:
            return
        try:
            from sempy_labs._helper_functions import _mount
            local_path = _mount()
            safe_name = (_rpt_name[0] or "report").replace(" ", "_").replace("/", "_")
            fname = f"{safe_name}_prototype.svg"
            path = f"{local_path}/Files/{fname}"
            with open(path, "w", encoding="utf-8") as f:
                f.write(_svg_cache[0])
            set_status(conn_status, f"\u2713 Saved {fname} to lakehouse Files.", "#34c759")
        except Exception as e:
            set_status(conn_status, f"Error saving: {e}", "#ff3b30")

    generate_btn.on_click(_generate)
    export_excalidraw_btn.on_click(_on_export_excalidraw)
    export_svg_btn.on_click(_on_export_svg)

    widget = widgets.VBox([nav_row, svg_container], layout=widgets.Layout(padding="12px", gap="4px"))
    return widget


# ---------------------------------------------------------------------------
# Model Diagram tab (inline)
# ---------------------------------------------------------------------------
def _diagram_tab(workspace_input=None, report_input=None):
    """Build the Model Diagram tab — SVG visualization of table relationships."""
    from sempy_labs._ui_components import (
        FONT_FAMILY, BORDER_COLOR, GRAY_COLOR, ICON_ACCENT, SECTION_BG,
        status_html, set_status,
    )

    generate_btn = widgets.Button(description="\U0001F5FA Generate Diagram", button_style="primary", layout=widgets.Layout(width="200px"))
    model_dd = widgets.Dropdown(options=["(load a model first)"], value="(load a model first)", layout=widgets.Layout(width="300px"))
    conn_status = status_html()

    nav_row = widgets.HBox(
        [generate_btn, model_dd, conn_status],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 8px 0"),
    )

    svg_display = widgets.HTML(
        value=f'<div style="padding:20px; color:{GRAY_COLOR}; font-size:14px; font-family:{FONT_FAMILY}; text-align:center; font-style:italic;">Click "Generate Diagram" after loading a model in the Model Explorer tab.</div>'
    )
    svg_container = widgets.VBox(
        [svg_display],
        layout=widgets.Layout(
            overflow_x="auto", overflow_y="auto", max_height="600px",
            border=f"1px solid {BORDER_COLOR}", border_radius="8px",
            padding="8px", background_color="#fff",
        ),
    )

    # Layout constants
    _BOX_W = 200
    _HEADER_H = 24
    _COL_LINE_H = 14
    _COL_PAD = 6
    _MIN_BODY = 20
    _PAD_X = 80
    _PAD_Y = 100
    _COLS = 5

    def _box_height(t):
        body_h = max(_MIN_BODY, len(t["cols"]) * _COL_LINE_H + _COL_PAD)
        return _HEADER_H + body_h

    def _generate(_):
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds_input = report_input.value.strip() if report_input else ""
        if not ds_input:
            set_status(conn_status, "Enter a model name.", "#ff3b30")
            return
        ds = ds_input.split(",")[0].strip()
        for pfx in ("\U0001F4C4 ", "\U0001F4CA "):
            if ds.startswith(pfx):
                ds = ds[len(pfx):]

        generate_btn.disabled = True
        generate_btn.description = "Loading\u2026"
        set_status(conn_status, f"Loading relationships for '{ds}'\u2026", GRAY_COLOR)

        try:
            from sempy_labs.tom import connect_semantic_model
            tables = {}
            relationships = []

            with connect_semantic_model(dataset=ds, readonly=True, workspace=ws) as tom:
                for table in tom.model.Tables:
                    cols = []
                    for col in table.Columns:
                        if col.IsKey:
                            cols.append(f"\U0001F511 {col.Name}")
                        elif any(r.FromColumn.Name == col.Name and str(r.FromTable.Name) == table.Name for r in tom.model.Relationships):
                            cols.append(f"\U0001F517 {col.Name}")
                    tables[table.Name] = {
                        "name": table.Name,
                        "cols": cols[:5],  # limit to 5 key/FK cols
                        "is_hidden": bool(table.IsHidden),
                        "rel_count": 0,
                    }

                for rel in tom.model.Relationships:
                    from_t = str(rel.FromTable.Name)
                    from_c = str(rel.FromColumn.Name)
                    to_t = str(rel.ToTable.Name)
                    to_c = str(rel.ToColumn.Name)
                    card_from = str(rel.FromCardinality) if hasattr(rel, "FromCardinality") else "*"
                    card_to = str(rel.ToCardinality) if hasattr(rel, "ToCardinality") else "1"
                    is_active = bool(rel.IsActive) if hasattr(rel, "IsActive") else True
                    cross = str(rel.CrossFilteringBehavior) if hasattr(rel, "CrossFilteringBehavior") else ""
                    relationships.append({
                        "from_table": from_t, "from_col": from_c,
                        "to_table": to_t, "to_col": to_c,
                        "cardinality": f"{card_from}:{card_to}",
                        "active": is_active, "cross_filter": cross,
                    })
                    if from_t in tables:
                        tables[from_t]["rel_count"] += 1
                    if to_t in tables:
                        tables[to_t]["rel_count"] += 1

            # Layout: sort by relationship count (fact tables first), then alphabetical
            sorted_tables = sorted(tables.values(), key=lambda t: (-t["rel_count"], t["name"]))
            visible_tables = [t for t in sorted_tables if not t["is_hidden"]]

            # Build positions — use actual box heights for row placement
            positions = {}
            row_heights = {}  # track max height per row for proper spacing
            for idx, t in enumerate(visible_tables):
                row = idx // _COLS
                bh = _box_height(t)
                row_heights[row] = max(row_heights.get(row, 0), bh)

            for idx, t in enumerate(visible_tables):
                col = idx % _COLS
                row = idx // _COLS
                y_offset = 40 + sum(row_heights.get(r, 0) + _PAD_Y for r in range(row))
                positions[t["name"]] = (40 + col * (_BOX_W + _PAD_X), y_offset)

            # Build SVG
            n = len(visible_tables)
            cols = min(_COLS, n) if n > 0 else 1
            rows_count = (n + cols - 1) // cols
            svg_w = cols * (_BOX_W + _PAD_X) + 40
            svg_h = 40 + sum(row_heights.get(r, 0) + _PAD_Y for r in range(rows_count))

            # Precompute actual heights per table for connection points
            table_heights = {t["name"]: _box_height(t) for t in visible_tables}

            svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{svg_w}" height="{svg_h}" viewBox="0 0 {svg_w} {svg_h}">']
            svg.append(f'<rect width="{svg_w}" height="{svg_h}" fill="#fff"/>')
            svg.append('<defs><marker id="rel-arrow" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><polygon points="0 0, 8 3, 0 6" fill="#888"/></marker></defs>')

            # Draw relationship lines first (behind boxes)
            for rel in relationships:
                if rel["from_table"] not in positions or rel["to_table"] not in positions:
                    continue
                fx, fy = positions[rel["from_table"]]
                tx, ty = positions[rel["to_table"]]
                fh = table_heights[rel["from_table"]]
                th = table_heights[rel["to_table"]]

                # Connect from nearest edge (bottom/top/left/right) based on relative position
                fcx, fcy = fx + _BOX_W // 2, fy + fh // 2
                tcx, tcy = tx + _BOX_W // 2, ty + th // 2

                if abs(fcy - tcy) > abs(fcx - tcx):
                    # Vertical: connect bottom→top or top→bottom
                    if fcy < tcy:
                        x1, y1 = fcx, fy + fh
                        x2, y2 = tcx, ty
                    else:
                        x1, y1 = fcx, fy
                        x2, y2 = tcx, ty + th
                else:
                    # Horizontal: connect right→left or left→right
                    if fcx < tcx:
                        x1, y1 = fx + _BOX_W, fcy
                        x2, y2 = tx, tcy
                    else:
                        x1, y1 = fx, fcy
                        x2, y2 = tx + _BOX_W, tcy

                color = "#888" if rel["active"] else "#ddd"
                dash = "" if rel["active"] else ' stroke-dasharray="4,3"'
                svg.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="1.5"{dash} marker-end="url(#rel-arrow)"/>')
                # Cardinality label at midpoint
                mx, my = (x1 + x2) // 2, (y1 + y2) // 2
                svg.append(f'<text x="{mx}" y="{my - 4}" font-family="{FONT_FAMILY}" font-size="9" fill="#888" text-anchor="middle">{rel["cardinality"]}</text>')

            # Draw table boxes
            for t in visible_tables:
                x, y = positions[t["name"]]
                # Header
                svg.append(f'<rect x="{x}" y="{y}" width="{_BOX_W}" height="{_HEADER_H}" rx="4" fill="#2563eb" stroke="#1e40af" stroke-width="1"/>')
                svg.append(f'<text x="{x + 8}" y="{y + 16}" font-family="{FONT_FAMILY}" font-size="12" font-weight="600" fill="#fff">{t["name"][:22]}</text>')
                # Body
                body_h = max(_MIN_BODY, len(t["cols"]) * _COL_LINE_H + _COL_PAD)
                svg.append(f'<rect x="{x}" y="{y + _HEADER_H}" width="{_BOX_W}" height="{body_h}" fill="#f8fafc" stroke="#e2e8f0" stroke-width="1"/>')
                for ci, col_name in enumerate(t["cols"]):
                    svg.append(f'<text x="{x + 8}" y="{y + _HEADER_H + 14 + ci * _COL_LINE_H}" font-family="{FONT_FAMILY}" font-size="10" fill="#555">{col_name[:25]}</text>')
                if not t["cols"]:
                    svg.append(f'<text x="{x + 8}" y="{y + _HEADER_H + 14}" font-family="{FONT_FAMILY}" font-size="10" fill="#bbb" font-style="italic">(no key/FK cols)</text>')

            svg.append('</svg>')
            svg_display.value = "\n".join(svg)

            set_status(conn_status, f"\u2713 Diagram: {len(visible_tables)} tables, {len(relationships)} relationships.", "#34c759")

        except Exception as e:
            set_status(conn_status, f"Error: {str(e)[:300]}", "#ff3b30")
        finally:
            generate_btn.disabled = False
            generate_btn.description = "\U0001F5FA Generate Diagram"

    generate_btn.on_click(_generate)

    widget = widgets.VBox([nav_row, svg_container], layout=widgets.Layout(padding="12px", gap="4px"))
    return widget


# ---------------------------------------------------------------------------
# Script Runner tab (inline)
# ---------------------------------------------------------------------------
def _script_tab(workspace_input=None, report_input=None):
    """Build the Script Runner tab for executing Python scripts with TOM model access."""
    from sempy_labs._ui_components import (
        FONT_FAMILY, BORDER_COLOR, GRAY_COLOR, ICON_ACCENT, SECTION_BG,
        status_html, set_status,
    )

    _TEMPLATES = {
        "-- Select template --": "",
        "List all measures": """for table in model.Tables:
    for m in table.Measures:
        print(f"[{table.Name}][{m.Name}] = {m.Expression[:60]}")
""",
        "List all columns with types": """for table in model.Tables:
    for c in table.Columns:
        print(f"'{table.Name}'[{c.Name}] ({c.DataType})")
""",
        "List relationships": """for r in model.Relationships:
    print(f"'{r.FromTable.Name}'[{r.FromColumn.Name}] -> '{r.ToTable.Name}'[{r.ToColumn.Name}]  Active={r.IsActive}")
""",
        "List tables with row counts": """import sempy.fabric as fabric
ws = workspace  # from scope
for table in model.Tables:
    print(f"{table.Name}: {len(table.Columns)} columns, {len(table.Measures)} measures")
""",
        "Find unused measures": """used = set()
for table in model.Tables:
    for m in table.Measures:
        expr = str(m.Expression) if m.Expression else ""
        for t2 in model.Tables:
            for m2 in t2.Measures:
                if m2.Name != m.Name and f"[{m.Name}]" in str(m2.Expression or ""):
                    used.add(m.Name)
for table in model.Tables:
    for m in table.Measures:
        if m.Name not in used and not m.IsHidden:
            print(f"Possibly unused: [{m.Name}]")
""",
        "Hide all columns starting with 'ID'": """count = 0
for table in model.Tables:
    for c in table.Columns:
        if c.Name.startswith("ID") and not c.IsHidden:
            if not readonly:
                c.IsHidden = True
                count += 1
            else:
                print(f"Would hide: '{table.Name}'[{c.Name}]")
                count += 1
if not readonly:
    model.SaveChanges()
print(f"Hidden {count} column(s).")
""",
    }

    template_dd = widgets.Dropdown(
        options=list(_TEMPLATES.keys()),
        value="-- Select template --",
        layout=widgets.Layout(width="300px"),
    )
    run_btn = widgets.Button(description="\u25B6 Run Script", button_style="primary", layout=widgets.Layout(width="120px"))
    write_cb = widgets.Checkbox(value=False, description="Enable write mode (XMLA)", indent=False, layout=widgets.Layout(width="auto"))
    conn_status = status_html()

    script_input = widgets.Textarea(
        value="# Python script with TOM model access\n# Variables in scope: model, tom, workspace, readonly\n\nfor table in model.Tables:\n    print(table.Name)\n",
        layout=widgets.Layout(width="100%", height="200px", font_family="monospace"),
    )

    output_html = widgets.HTML(value="")
    output_container = widgets.VBox(
        [output_html],
        layout=widgets.Layout(
            max_height="300px", overflow_y="auto",
            border=f"1px solid {BORDER_COLOR}", border_radius="8px",
            padding="8px", background_color="#f8f9fa",
        ),
    )

    def _on_template(change):
        tpl = change.get("new", "")
        code = _TEMPLATES.get(tpl, "")
        if code:
            script_input.value = code

    template_dd.observe(_on_template, names="value")

    def _on_run(_):
        ws = workspace_input.value.strip() if workspace_input else None
        ws = ws or None
        ds_input = report_input.value.strip() if report_input else ""
        if not ds_input:
            set_status(conn_status, "Enter a model name.", "#ff3b30")
            return
        ds = ds_input.split(",")[0].strip()
        for pfx in ("\U0001F4C4 ", "\U0001F4CA "):
            if ds.startswith(pfx):
                ds = ds[len(pfx):]

        code = script_input.value
        if not code.strip():
            set_status(conn_status, "Enter a script.", "#ff3b30")
            return

        readonly = not write_cb.value
        run_btn.disabled = True
        run_btn.description = "Running\u2026"
        set_status(conn_status, f"Executing on '{ds}' ({'readonly' if readonly else 'WRITE'})\u2026", GRAY_COLOR)

        import io as _io
        from contextlib import redirect_stdout as _redirect

        try:
            from sempy_labs.tom import connect_semantic_model
            buf = _io.StringIO()
            with connect_semantic_model(dataset=ds, readonly=readonly, workspace=ws) as tom:
                scope = {
                    "tom": tom,
                    "model": tom.model,
                    "workspace": ws,
                    "readonly": readonly,
                    "print": lambda *a, **kw: _io.StringIO.write(buf, " ".join(str(x) for x in a) + kw.get("end", "\n")),
                }
                # Add common imports
                try:
                    import sempy.fabric as fabric
                    scope["fabric"] = fabric
                except Exception:
                    pass
                try:
                    import pandas as pd
                    scope["pd"] = pd
                except Exception:
                    pass
                try:
                    import Microsoft.AnalysisServices.Tabular as TOM
                    scope["TOM"] = TOM
                except Exception:
                    pass

                exec(compile(code, "<script>", "exec"), scope)

            captured = buf.getvalue()
            if captured:
                lines = captured.rstrip().split("\n")
                html = '<pre style="font-family:monospace; font-size:12px; margin:0; white-space:pre-wrap;">'
                for line in lines:
                    html += f"{line}\n"
                html += "</pre>"
                output_html.value = html
            else:
                output_html.value = '<div style="color:#888; font-size:12px;">Script completed with no output.</div>'
            set_status(conn_status, f"\u2713 Script executed ({len(captured.splitlines())} lines output).", "#34c759")

        except Exception as e:
            output_html.value = f'<pre style="color:#ff3b30; font-size:12px;">{str(e)}</pre>'
            set_status(conn_status, f"Error: {str(e)[:80]}", "#ff3b30")
        finally:
            run_btn.disabled = False
            run_btn.description = "\u25B6 Run Script"

    run_btn.on_click(_on_run)

    nav_row = widgets.HBox(
        [template_dd, run_btn, write_cb, conn_status],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 8px 0"),
    )
    header = widgets.HTML(
        value=f'<div style="font-size:12px; font-weight:600; color:{ICON_ACCENT}; font-family:{FONT_FAMILY}; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:4px;">'
        f'Script Runner — Python with TOM Model Access</div>'
        f'<div style="font-size:11px; color:#888; font-family:{FONT_FAMILY}; margin-bottom:8px;">'
        f'Variables: <code>model</code>, <code>tom</code>, <code>TOM</code>, <code>fabric</code>, <code>pd</code>, <code>workspace</code>, <code>readonly</code></div>'
    )

    widget = widgets.VBox([nav_row, header, script_input, output_container], layout=widgets.Layout(padding="12px", gap="4px"))
    return widget


def pbi_fixer(
    all_tabs: bool = False,
    workspace: Optional[str | UUID] = None,
    report: Optional[str | UUID] = None,
    page_name: Optional[str] = None,
    show_fixer_tab: bool = False,
):
    """
    Launches an interactive UI for scanning and fixing Power BI report visuals.

    Parameters
    ----------
    all_tabs : bool, default=False
        If False (default), only shows Fix All, Semantic Model, Report, and About tabs for fast startup.
        If True, shows all tabs (Perspectives, Translations, Memory Analyzer, BPA, etc.).
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    report : str | uuid.UUID, default=None
        Name(s) or ID(s) of the report(s). Supports comma-separated values.
        Pre-populates the report input field.
    page_name : str, default=None
        The display name of the page. Pre-populates the page input field.
    show_fixer_tab : bool, default=False
        If True, shows the Fixer tab. By default hidden since all fixers
        are accessible via action dropdowns in the Report and SM tabs.

    Examples
    --------
    >>> pbi_fixer()                                    # Minimal: SM + Report + About
    >>> pbi_fixer(True)                                # All tabs
    >>> pbi_fixer(False, "", "Bad Report")             # Pre-filled report name
    >>> pbi_fixer(True, "My Workspace", "My Report")   # All tabs, pre-filled
    """

    # Suppress stale widget comm errors from previous runs (cosmetic log spam)
    try:
        import logging as _lg
        _lg.getLogger("ipykernel.comm").setLevel(_lg.CRITICAL)
    except Exception:
        pass

    # ---------------------------------------------------------------------------
    # Lazy imports — deferred to function call time to avoid circular imports.
    # Each fixer is optional; the UI degrades gracefully if not available.
    # ---------------------------------------------------------------------------
    fix_piecharts = _lazy_import("sempy_labs.report._Fix_PieChart", "fix_piecharts")
    fix_barcharts = _lazy_import("sempy_labs.report._Fix_BarChart", "fix_barcharts")
    fix_columncharts = _lazy_import("sempy_labs.report._Fix_ColumnChart", "fix_columncharts")
    fix_linecharts = _lazy_import("sempy_labs.report._Fix_Charts", "fix_linecharts")
    fix_charts = _lazy_import("sempy_labs.report._Fix_Charts", "fix_charts")
    fix_column_to_line = _lazy_import("sempy_labs.report._Fix_ColumnToLine", "fix_column_to_line")
    fix_column_to_bar = _lazy_import("sempy_labs.report._Fix_Charts", "fix_column_to_bar")
    fix_bar_to_column = _lazy_import("sempy_labs.report._Fix_Charts", "fix_bar_to_column")
    fix_ibcs_variance = _lazy_import("sempy_labs.report._Fix_IBCSVariance", "fix_ibcs_variance")
    fix_page_size = _lazy_import("sempy_labs.report._Fix_PageSize", "fix_page_size")
    fix_hide_visual_filters = _lazy_import("sempy_labs.report._Fix_HideVisualFilters", "fix_hide_visual_filters")
    fix_upgrade_to_pbir = _lazy_import("sempy_labs.report._Fix_UpgradeToPbir", "fix_upgrade_to_pbir")
    fix_remove_unused_cv = _lazy_import("sempy_labs.report._Fix_RemoveUnusedCustomVisuals", "fix_remove_unused_custom_visuals")
    fix_disable_show_no_data = _lazy_import("sempy_labs.report._Fix_DisableShowItemsNoData", "fix_disable_show_items_no_data")
    fix_migrate_rlm = _lazy_import("sempy_labs.report._Fix_MigrateReportLevelMeasures", "fix_migrate_report_level_measures")
    add_calculated_calendar = _lazy_import("sempy_labs.semantic_model._Add_CalculatedTable_Calendar", "add_calculated_calendar")
    fix_discourage_implicit_measures = _lazy_import("sempy_labs.semantic_model._Fix_DiscourageImplicitMeasures", "fix_discourage_implicit_measures")
    add_last_refresh_table = _lazy_import("sempy_labs.semantic_model._Add_Table_LastRefresh", "add_last_refresh_table")
    add_calc_group_units = _lazy_import("sempy_labs.semantic_model._Add_CalcGroup_Units", "add_calc_group_units")
    add_calc_group_time_intelligence = _lazy_import("sempy_labs.semantic_model._Add_CalcGroup_TimeIntelligence", "add_calc_group_time_intelligence")
    add_measure_table = _lazy_import("sempy_labs.semantic_model._Add_CalculatedTable_MeasureTable", "add_measure_table")
    add_measures_from_columns = _lazy_import("sempy_labs.semantic_model._Add_MeasuresFromColumns", "add_measures_from_columns")
    add_py_measures = _lazy_import("sempy_labs.semantic_model._Add_PYMeasures", "add_py_measures")
    add_prep_for_ai = _lazy_import("sempy_labs.semantic_model._Add_PrepForAI", "add_prep_for_ai")

    # Inline fallbacks for MeasuresFromColumns and PYMeasures
    if add_measures_from_columns is None:
        def add_measures_from_columns(dataset, workspace=None, target_table=None, scan_only=False, **kw):
            from sempy_labs.tom import connect_semantic_model
            created = 0
            with connect_semantic_model(dataset=dataset, readonly=scan_only, workspace=workspace) as tom:
                # Auto-detect measure table if not specified
                auto_dest = None
                if not target_table:
                    for t in tom.model.Tables:
                        if "measure" in t.Name.lower():
                            auto_dest = t.Name
                            print(f"  Auto-detected measure table: '{t.Name}'")
                            break
                for table in tom.model.Tables:
                    dest_name = target_table or auto_dest or table.Name
                    for col in table.Columns:
                        summarize_by = str(col.SummarizeBy) if hasattr(col, "SummarizeBy") else "None"
                        if summarize_by in ("None", "Default"):
                            continue
                        agg_fn = summarize_by.upper()
                        dax_expr = f"{agg_fn}('{table.Name}'[{col.Name}])"
                        dest_tbl = tom.model.Tables[dest_name]
                        if dest_tbl.Measures.Find(col.Name) is not None:
                            continue
                        if scan_only:
                            print(f"  Would create: [{col.Name}] = {dax_expr}")
                            created += 1
                            continue
                        tom.add_measure(table_name=dest_name, measure_name=col.Name, expression=dax_expr, format_string="0.0", display_folder=table.Name)
                        col.IsHidden = True
                        created += 1
                        print(f"  Created [{col.Name}] = {dax_expr}")
                if not scan_only and created > 0:
                    tom.model.SaveChanges()
            print(f"  {'Would create' if scan_only else 'Created'} {created} measure(s) from columns.")
            return created

    if add_py_measures is None:
        def add_py_measures(dataset, workspace=None, measures=None, calendar_table=None, date_column=None, target_table=None, scan_only=False, **kw):
            from sempy_labs.tom import connect_semantic_model
            created = 0
            with connect_semantic_model(dataset=dataset, readonly=scan_only, workspace=workspace) as tom:
                cal = None
                if calendar_table:
                    cal = tom.model.Tables.Find(calendar_table)
                else:
                    for t in tom.model.Tables:
                        if str(getattr(t, "DataCategory", "")) == "Time":
                            cal = t; break
                if cal is None:
                    print("  No calendar table found."); return 0
                dt_col = date_column
                if not dt_col:
                    for c in cal.Columns:
                        if getattr(c, "IsKey", False): dt_col = c.Name; break
                    if not dt_col:
                        for c in cal.Columns:
                            if "date" in c.Name.lower(): dt_col = c.Name; break
                if not dt_col:
                    print(f"  No date column found in '{cal.Name}'."); return 0
                dest_tbl = tom.model.Tables.Find(target_table) if target_table else None
                if not dest_tbl and not target_table:
                    for t in tom.model.Tables:
                        if "measure" in t.Name.lower():
                            dest_tbl = t
                            print(f"  Auto-detected measure table: '{t.Name}'")
                            break
                src = [m for table in tom.model.Tables for m in table.Measures if measures is None or m.Name in measures]
                if not src:
                    print("  No measures found."); return 0
                for m in src:
                    n, fmt = m.Name, str(m.FormatString) if m.FormatString else ""
                    folder = str(m.DisplayFolder) if m.DisplayFolder else ""
                    py_folder = f"{folder}\\\\PY" if folder else "PY"
                    dest = dest_tbl or m.Table
                    for v_name, v_expr in [
                        (f"{n} PY", f"CALCULATE([{n}], SAMEPERIODLASTYEAR('{cal.Name}'[{dt_col}]))"),
                        (f"{n} \u0394 PY", f"[{n}] - [{n} PY]"),
                        (f"{n} \u0394 PY %", f"DIVIDE([{n}] - [{n} PY], [{n}])"),
                        (f"{n} Max Green PY", f"IF([{n} \u0394 PY] > 0, MAX([{n}], [{n} PY]))"),
                        (f"{n} Max Red AC", f"IF([{n} \u0394 PY] < 0, MAX([{n}], [{n} PY]))"),
                    ]:
                        if dest.Measures.Find(v_name) is not None: continue
                        if scan_only: print(f"  Would create: [{v_name}]"); created += 1; continue
                        tom.add_measure(table_name=dest.Name, measure_name=v_name, expression=v_expr, format_string=fmt, display_folder=py_folder)
                        created += 1
                if not scan_only and created > 0:
                    tom.model.SaveChanges()
            print(f"  {'Would create' if scan_only else 'Created'} {created} PY measure(s).")
            return created

    # Tab modules (deferred)
    model_explorer_tab = _lazy_import("sempy_labs._model_explorer", "model_explorer_tab")
    report_explorer_tab = _lazy_import("sempy_labs._report_explorer", "report_explorer_tab")
    perspective_editor_tab = _lazy_import("sempy_labs._perspective_editor", "perspective_editor_tab")

    # -----------------------------
    # COLOR THEME (matches perspective_editor)
    # -----------------------------
    text_color = "inherit"
    border_color = "#e0e0e0"
    icon_accent = "#FF9500"
    gray_color = "#999"
    section_bg = "#fafafa"

    # -----------------------------
    # STATUS + PROGRESS
    # -----------------------------
    status = widgets.HTML(value="")
    progress = widgets.HTML(
        value="",
        layout=widgets.Layout(
            display="none",
            border=f"1px solid {border_color}",
            border_radius="8px",
            margin="4px 0 0 0",
        ),
    )
    _progress_lines = []  # mutable list to accumulate lines

    def show_status(msg, color):
        status.value = (
            f'<div style="padding:8px 12px; border-radius:8px; '
            f"background:{color}1a; color:{color}; font-size:14px; "
            f'font-family:-apple-system,BlinkMacSystemFont,sans-serif;">{msg}</div>'
        )

    # -----------------------------
    # HEADER
    # -----------------------------
    title = widgets.HTML(
        value=f'<div style="font-size:22px; font-weight:700; color:{icon_accent}; '
        f'font-family:-apple-system,BlinkMacSystemFont,sans-serif;">'
        f'Power BI Fixer</div>'
    )
    subtitle = widgets.HTML(
        value=f'<div style="font-size:13px; color:{gray_color}; '
        f'font-family:-apple-system,BlinkMacSystemFont,sans-serif; margin-top:2px;">'
        f'Scan, fix, and explore your Power BI reports and semantic models</div>'
    )
    header = widgets.VBox(
        [title, subtitle],
        layout=widgets.Layout(margin="0 0 12px 0", padding="0 0 8px 0",
                              border_bottom=f"2px solid {icon_accent}"),
    )

    # -----------------------------
    # MODE SELECTOR (Fix | Scan | Scan + Fix)
    # Scan modes are placeholders for future implementation
    # -----------------------------
    mode_label = widgets.HTML(
        value=f'<span style="font-size:13px; font-weight:500; color:{text_color}; '
        f'font-family:-apple-system,BlinkMacSystemFont,sans-serif;">Mode</span>'
    )
    mode_toggle = widgets.ToggleButtons(
        options=["Fix", "Scan", "Scan + Fix"],
        value="Fix",
        layout=widgets.Layout(margin="0"),
    )
    mode_toggle.style.button_width = "100px"

    def on_mode_change(change):
        status.value = ""
        _on_sm_cb_change()

    mode_toggle.observe(on_mode_change, names="value")

    mode_row = widgets.HBox(
        [mode_label, mode_toggle],
        layout=widgets.Layout(align_items="center", gap="12px", margin="0 0 16px 0"),
    )

    # -----------------------------
    # SHARED INPUTS (top-level, used by all tabs)
    # -----------------------------
    def _input_label(text):
        return widgets.HTML(
            value=f'<span style="font-size:13px; font-weight:500; color:{text_color}; '
            f'font-family:-apple-system,BlinkMacSystemFont,sans-serif; '
            f'min-width:90px; display:inline-block;">{text}</span>'
        )

    workspace_input = widgets.Text(
        value=str(workspace) if workspace else "",
        placeholder="Leave empty for notebook workspace",
        layout=widgets.Layout(width="400px"),
    )
    report_input = widgets.Combobox(
        value=str(report) if report else "",
        placeholder="Type, select, or comma-separate (blank = all)",
        options=[],
        ensure_option=False,
        layout=widgets.Layout(width="400px"),
    )
    page_input = widgets.Text(
        value=page_name if page_name else "",
        placeholder="Leave empty for all pages",
        layout=widgets.Layout(width="300px"),
    )

    list_items_btn = widgets.Button(
        description="\U0001F4CB List Items",
        layout=widgets.Layout(width="110px"),
    )
    list_items_status = widgets.HTML(value="")

    def _on_list_items(_):
        """Fetch all reports + datasets in the workspace and populate the Combobox options."""
        ws = workspace_input.value.strip() or None
        list_items_btn.disabled = True
        list_items_btn.description = "Listing\u2026"
        list_items_status.value = ""
        try:
            from sempy_labs._helper_functions import resolve_workspace_name_and_id, _base_api
            _, ws_id = resolve_workspace_name_and_id(ws)

            # Fetch reports
            rpt_url = f"/v1.0/myorg/groups/{ws_id}/reports"
            rpt_resp = _base_api(request=rpt_url, client="fabric_sp")
            rpt_names = [r.get("name") for r in rpt_resp.json().get("value", []) if r.get("name")]

            # Fetch datasets
            ds_url = f"/v1.0/myorg/groups/{ws_id}/datasets"
            ds_resp = _base_api(request=ds_url, client="fabric_sp")
            ds_names = [d.get("name") for d in ds_resp.json().get("value", []) if d.get("name")]

            # Deduplicate: if a name appears in both, show once; otherwise prefix with icon
            shared = set(n.lower() for n in rpt_names) & set(n.lower() for n in ds_names)
            combined = []
            seen_lower = set()
            for name in sorted(set(rpt_names + ds_names), key=str.lower):
                nl = name.lower()
                if nl in seen_lower:
                    continue
                seen_lower.add(nl)
                if nl in shared:
                    combined.append(name)
                elif name in rpt_names:
                    combined.append(f"\U0001F4CA {name}")
                else:
                    combined.append(f"\U0001F4C4 {name}")

            report_input.options = combined
            _base_options.clear()
            _base_options.extend(combined)
            list_items_status.value = (
                f'<span style="font-size:12px; color:#34c759;">'
                f'{len(rpt_names)} report(s), {len(ds_names)} model(s)</span>'
            )
        except Exception as e:
            list_items_status.value = (
                f'<span style="font-size:12px; color:#ff3b30;">Error: {str(e)[:60]}</span>'
            )
        finally:
            list_items_btn.disabled = False
            list_items_btn.description = "\U0001F4CB List Items"

    list_items_btn.on_click(_on_list_items)

    # Store base options separately (the full unmodified list from List Items)
    _base_options = []

    def _strip_item_prefix(name):
        """Strip icon prefixes (ðŸ“„ / ðŸ“Š) from dropdown selections."""
        for prefix in ("\U0001F4C4 ", "\U0001F4CA "):
            if name.startswith(prefix):
                return name[len(prefix):]
        return name

    def _on_report_input_change(change):
        """Update Combobox options to support autocomplete after commas. Auto-strip icon prefixes."""
        val = change.get("new", "")
        # Auto-strip icon prefixes from the value (📄 / 📊) so all consumers get clean names
        stripped = val
        for _pfx in ("\U0001F4C4 ", "\U0001F4CA "):
            stripped = stripped.replace(_pfx, "")
        if stripped != val:
            report_input.value = stripped
            return  # the recursive call will handle the rest
        if not _base_options:
            return
        if "," in val:
            # Everything up to and including the last comma + space
            prefix = val.rsplit(",", 1)[0] + ", "
            # Already-entered items (for dedup)
            already = {_strip_item_prefix(x.strip()).lower() for x in prefix.split(",") if x.strip()}
            # Build new options: prefix + each remaining base option
            new_opts = []
            for opt in _base_options:
                clean = _strip_item_prefix(opt).lower()
                if clean not in already:
                    new_opts.append(f"{prefix}{opt}")
            report_input.options = new_opts
        else:
            # No comma — restore base options
            report_input.options = _base_options

    report_input.observe(_on_report_input_change, names="value")

    shared_inputs_box = widgets.VBox(
        [
            widgets.HBox(
                [_input_label("Workspace"), workspace_input],
                layout=widgets.Layout(align_items="center", gap="8px"),
            ),
            widgets.HBox(
                [_input_label("Report"), report_input, list_items_btn, list_items_status],
                layout=widgets.Layout(align_items="center", gap="8px"),
            ),
        ],
        layout=widgets.Layout(
            gap="8px",
            padding="12px",
            margin="0 0 12px 0",
            border=f"1px solid {border_color}",
            border_radius="8px",
            background_color="#fafafa",
        ),
    )

    # -----------------------------
    # DOWNLOAD BUTTONS
    # -----------------------------
    download_pbix_btn = widgets.Button(description="\u2B07 Download .pbix", layout=widgets.Layout(width="140px"))
    download_pbip_btn = widgets.Button(description="\u2B07 Download .pbip", layout=widgets.Layout(width="140px"))
    download_status = widgets.HTML(value="")

    def _on_download_pbix(_):
        rpt = report_input.value.strip()
        ws = workspace_input.value.strip() or None
        if not rpt:
            download_status.value = f'<span style="color:#ff3b30; font-size:12px;">Enter a report name.</span>'
            return
        rpt = rpt.split(",")[0].strip()
        download_pbix_btn.disabled = True
        download_pbix_btn.description = "Downloading\u2026"
        download_status.value = f'<span style="color:#999; font-size:12px;">Downloading .pbix\u2026</span>'
        try:
            from sempy_labs.report import download_report
            download_report(report=rpt, workspace=ws)
            download_status.value = f'<span style="color:#34c759; font-size:12px;">\u2713 .pbix saved to lakehouse Files.</span>'
        except Exception as e:
            download_status.value = f'<span style="color:#ff3b30; font-size:12px;">Error: {str(e)[:80]}</span>'
        download_pbix_btn.disabled = False
        download_pbix_btn.description = "\u2B07 Download .pbix"

    def _on_download_pbip(_):
        rpt = report_input.value.strip()
        ws = workspace_input.value.strip() or None
        if not rpt:
            download_status.value = f'<span style="color:#ff3b30; font-size:12px;">Enter a report name.</span>'
            return
        rpt = rpt.split(",")[0].strip()
        download_pbip_btn.disabled = True
        download_pbip_btn.description = "Downloading\u2026"
        download_status.value = f'<span style="color:#999; font-size:12px;">Saving .pbip\u2026</span>'
        try:
            from sempy_labs.report import save_report_as_pbip
            save_report_as_pbip(report=rpt, workspace=ws)
            download_status.value = f'<span style="color:#34c759; font-size:12px;">\u2713 .pbip saved to lakehouse Files.</span>'
        except Exception as e:
            download_status.value = f'<span style="color:#ff3b30; font-size:12px;">Error: {str(e)[:80]}</span>'
        download_pbip_btn.disabled = False
        download_pbip_btn.description = "\u2B07 Download .pbip"

    download_pbix_btn.on_click(_on_download_pbix)
    download_pbip_btn.on_click(_on_download_pbip)

    # -----------------------------
    # CLONE BUTTONS (next to downloads)
    # -----------------------------
    clone_both_btn = widgets.Button(description="\U0001F4CB Clone Both", layout=widgets.Layout(width="120px"))
    clone_rpt_btn = widgets.Button(description="\U0001F4CB Clone Report", layout=widgets.Layout(width="130px"))
    clone_model_btn = widgets.Button(description="\U0001F4CB Clone Model", layout=widgets.Layout(width="130px"))

    def _resolve_report_model_name(rpt_name, ws):
        """Resolve the semantic model name backing a report."""
        try:
            from sempy_labs._helper_functions import resolve_workspace_name_and_id, _base_api
            _, ws_id = resolve_workspace_name_and_id(ws)
            url = f"/v1.0/myorg/groups/{ws_id}/reports"
            resp = _base_api(request=url, client="fabric_sp")
            for r in resp.json().get("value", []):
                if r.get("name") == rpt_name:
                    ds_id = r.get("datasetId", "")
                    if ds_id:
                        ds_url = f"/v1.0/myorg/groups/{ws_id}/datasets/{ds_id}"
                        ds_resp = _base_api(request=ds_url, client="fabric_sp")
                        return ds_resp.json().get("name", "")
        except Exception:
            pass
        return ""

    def _on_clone_report(_):
        rpt = _strip_item_prefix(report_input.value.strip())
        ws = workspace_input.value.strip() or None
        if not rpt:
            download_status.value = '<span style="color:#ff3b30; font-size:12px;">Enter a report name.</span>'
            return
        rpt = rpt.split(",")[0].strip()
        clone_rpt_btn.disabled = True
        download_status.value = f'<span style="color:#999; font-size:12px;">Cloning report\u2026</span>'
        try:
            from sempy_labs.report._report_functions import clone_report as _clone_rpt
            cloned = f"{rpt}_copy"
            import warnings as _w, sys as _s, io as _sio
            _old = _s.stdout; _s.stdout = _sio.StringIO()
            with _w.catch_warnings():
                _w.simplefilter("ignore")
                _clone_rpt(report=rpt, cloned_report=cloned, workspace=ws)
            _s.stdout = _old
            download_status.value = f'<span style="color:#34c759; font-size:12px;">\u2713 Report cloned as \'{cloned}\'.</span>'
        except Exception as e:
            _s.stdout = _old
            download_status.value = f'<span style="color:#ff3b30; font-size:12px;">Error: {str(e)[:200]}</span>'
        clone_rpt_btn.disabled = False

    def _on_clone_model(_):
        rpt = _strip_item_prefix(report_input.value.strip())
        ws = workspace_input.value.strip() or None
        if not rpt:
            download_status.value = '<span style="color:#ff3b30; font-size:12px;">Enter a model name.</span>'
            return
        ds = rpt.split(",")[0].strip()
        clone_model_btn.disabled = True
        download_status.value = f'<span style="color:#999; font-size:12px;">Cloning model\u2026</span>'
        import warnings as _w, sys as _s, io as _sio
        _old = _s.stdout; _s.stdout = _sio.StringIO()
        try:
            with _w.catch_warnings():
                _w.simplefilter("ignore")
                cloned_name = _clone_semantic_model_impl(ds, ws)
            _s.stdout = _old
            download_status.value = f'<span style="color:#34c759; font-size:12px;">\u2713 Model cloned as \'{cloned_name}\'.</span>'
        except Exception as e:
            _s.stdout = _old
            download_status.value = f'<span style="color:#ff3b30; font-size:12px;">Error: {str(e)[:200]}</span>'
        clone_model_btn.disabled = False

    def _clone_semantic_model_impl(ds, ws):
        """Clone a semantic model by name."""
        from sempy_labs._helper_functions import get_item_definition
        from sempy_labs._generate_semantic_model import create_semantic_model_from_bim
        import json

        # Request TMSL format to get model.bim (default may return TMDL which has no model.bim)
        result = get_item_definition(item=ds, type="SemanticModel", workspace=ws, decode=True, format="TMSL")

        # Find model.bim part
        bim_part = None
        for part in result.get("definition", {}).get("parts", []):
            path = part.get("path", "")
            payload = part.get("payload", "")
            if path.endswith("model.bim"):
                if isinstance(payload, dict):
                    bim_part = payload
                elif isinstance(payload, str):
                    bim_part = json.loads(payload)
                elif isinstance(payload, bytes):
                    bim_part = json.loads(payload.decode("utf-8"))
                break
        if bim_part is None:
            raise ValueError(f"Could not extract model.bim. Got {len(result.get('definition', {}).get('parts', []))} part(s): {[p.get('path') for p in result.get('definition', {}).get('parts', [])]}")

        # Find unique name (append _copy, _copy2, _copy3, etc.)
        import sempy.fabric as fabric
        existing = set()
        try:
            df = fabric.list_datasets(workspace=ws, mode="rest")
            existing = set(df["Dataset Name"].tolist())
        except Exception:
            pass
        cloned_name = f"{ds}_copy"
        counter = 2
        while cloned_name in existing:
            cloned_name = f"{ds}_copy{counter}"
            counter += 1

        create_semantic_model_from_bim(dataset=cloned_name, bim_file=bim_part, workspace=ws)
        return cloned_name

    def _on_clone_both(_):
        rpt = _strip_item_prefix(report_input.value.strip())
        ws = workspace_input.value.strip() or None
        if not rpt:
            download_status.value = '<span style="color:#ff3b30; font-size:12px;">Enter a report name.</span>'
            return
        rpt = rpt.split(",")[0].strip()
        clone_both_btn.disabled = True

        # Resolve the model name backing this report
        model_name = _resolve_report_model_name(rpt, ws)

        # Name mismatch warning
        if model_name and model_name != rpt:
            download_status.value = (
                f'<span style="color:#ff9500; font-size:12px;">'
                f'\u26a0\ufe0f Report \'{rpt}\' uses model \'{model_name}\'. '
                f'Cloning both\u2026</span>'
            )
        else:
            download_status.value = f'<span style="color:#999; font-size:12px;">Cloning model + report\u2026</span>'

        ds = model_name or rpt
        import warnings as _w, sys as _s, io as _sio
        _old = _s.stdout; _s.stdout = _sio.StringIO()
        try:
            with _w.catch_warnings():
                _w.simplefilter("ignore")
                # 1. Clone model
                download_status.value = f'<span style="color:#999; font-size:12px;">Cloning model \'{ds}\'\u2026</span>'
                cloned_ds = _clone_semantic_model_impl(ds, ws)
                # 2. Clone report, rebound to new model
                download_status.value = f'<span style="color:#999; font-size:12px;">Cloning report \'{rpt}\'\u2026</span>'
                from sempy_labs.report._report_functions import clone_report as _clone_rpt
                _clone_rpt(report=rpt, cloned_report=f"{rpt}_copy", workspace=ws, target_dataset=cloned_ds)
            _s.stdout = _old
            download_status.value = (
                f'<span style="color:#34c759; font-size:12px;">'
                f'\u2713 Cloned \'{rpt}_copy\' + \'{cloned_ds}\'.</span>'
            )
        except Exception as e:
            _s.stdout = _old
            download_status.value = f'<span style="color:#ff3b30; font-size:12px;">Error: {str(e)[:200]}</span>'
        clone_both_btn.disabled = False

    clone_both_btn.on_click(_on_clone_both)
    clone_rpt_btn.on_click(_on_clone_report)
    clone_model_btn.on_click(_on_clone_model)

    download_row = widgets.HBox(
        [download_pbix_btn, download_pbip_btn, clone_both_btn, clone_rpt_btn, clone_model_btn, download_status],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 8px 0", flex_wrap="wrap"),
    )

    # -----------------------------
    # ⚡ FIX ALL TAB (God Mode — report + model + BPA + Report BPA)
    # -----------------------------
    from sempy_labs._ui_components import status_html, set_status, FONT_FAMILY
    font_family = FONT_FAMILY
    _fa_scan_btn = widgets.Button(description="\U0001F50D Scan Everything", button_style="danger", layout=widgets.Layout(width="170px"))
    _fa_fix_btn = widgets.Button(description="\u2705 Fix Selected", button_style="success", layout=widgets.Layout(width="140px"))
    _fa_select_all_btn = widgets.Button(description="Select All", layout=widgets.Layout(width="100px"))
    _fa_deselect_all_btn = widgets.Button(description="Deselect All", layout=widgets.Layout(width="100px"))
    _fa_status = status_html()
    _fa_tree = widgets.SelectMultiple(options=[], rows=22, layout=widgets.Layout(width="100%", height="520px", font_family="monospace"))
    _fa_findings = []   # [(category, item_name, fixer_name, detail_line), ...]
    _fa_key_map = {}    # display_line -> (kind, category, item, fixer, idx)

    _fa_btn_row = widgets.HBox(
        [_fa_scan_btn, _fa_fix_btn, _fa_select_all_btn, _fa_deselect_all_btn, _fa_status],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 8px 0"),
    )
    _fa_header = widgets.HTML(
        value=(
            f'<div style="font-size:12px; font-weight:600; color:{icon_accent}; font-family:{font_family}; '
            f'text-transform:uppercase; letter-spacing:0.5px; margin-bottom:2px;">\u26A1 Fix All</div>'
            f'<div style="font-size:11px; color:#888; font-family:{font_family}; font-style:italic; margin-bottom:4px;">'
            f'\u2139\ufe0f Scans all report fixers, model BPA fixers, Model BPA, and Report BPA across all items. '
            f'Deselect findings to exclude, then Fix Selected.</div>'
        )
    )

    def _fa_do_scan():
        """Scan everything: report fixers + model fixers + Model BPA + Report BPA."""
        _fa_findings.clear()
        _fa_key_map.clear()
        ws = workspace_input.value.strip() or None
        rpt_input = report_input.value.strip()
        items = [x.strip() for x in rpt_input.split(",") if x.strip()] if rpt_input else []
        if not items:
            return "Enter report/model name(s)."

        import io as _io
        from contextlib import redirect_stdout as _redirect
        import IPython.display as _ipd
        _orig_display = _ipd.display

        _skip_rpt = {"Convert to PBIR", "Show Theme Summary", "Apply IBCS Theme", "Delete Selected", "Duplicate Selected"}
        _skip_sm = {"── Add Objects ──", "── Formatting & Setup ──", "── BPA Fixers ──", "── Create & Delete ──",
                     "  Format All DAX", "  Add Incremental Refresh", "  Direct Lake Pre-warm Cache",
                     "  Add Calendar Table", "  Add Last Refresh Table", "  Add Measure Table",
                     "  Add Units Calc Group", "  Add Time Intelligence",
                     "  Auto-Create Measures from Columns", "  Add PY Measures (Y-1)"}
        scannable_rpt = {k: v for k, v in _rpt_fixer_cbs.items()
                        if k not in _skip_rpt and not k.startswith("──")}
        scannable_sm = {k: v for k, v in _model_fixer_cbs.items()
                        if k not in _skip_sm and not k.startswith("──")
                        and _model_fixer_cbs[k] is not _noop
                        and "Delete" not in k and "Create" not in k}

        total_steps = len(items) * (len(scannable_rpt) + len(scannable_sm) + 2)  # +2 for Model BPA + Report BPA
        step = 0

        for item_idx, item in enumerate(items):
            item_label = f"[{item_idx+1}/{len(items)}] {item}"

            # 1) Report fixers (scan_only)
            for fixer_name, fixer_fn in scannable_rpt.items():
                step += 1
                set_status(_fa_status, f"{item_label} \u2014 Report: {fixer_name} ({step}/{total_steps})", gray_color)
                try:
                    buf = _io.StringIO()
                    with _redirect(buf):
                        fixer_fn(report=item, page_name=None, workspace=ws, scan_only=True)
                    output = buf.getvalue().strip()
                    if output and "no changes" not in output.lower() and "0 changes" not in output.lower():
                        for line in output.splitlines():
                            line = line.strip()
                            if line:
                                _fa_findings.append(("\U0001F4CA Report Fixers", item, fixer_name, line))
                except Exception:
                    pass

            # 2) Model fixers (scan_only)
            for fixer_name, fixer_fn in scannable_sm.items():
                step += 1
                set_status(_fa_status, f"{item_label} \u2014 Model: {fixer_name.strip()} ({step}/{total_steps})", gray_color)
                try:
                    buf = _io.StringIO()
                    with _redirect(buf):
                        fixer_fn(report=item, workspace=ws, scan_only=True)
                    output = buf.getvalue().strip()
                    if output and "no changes" not in output.lower() and "0 changes" not in output.lower():
                        for line in output.splitlines():
                            line = line.strip()
                            if line:
                                _fa_findings.append(("\U0001F4C4 Model Fixers", item, fixer_name, line))
                except Exception:
                    pass

            # 3) Model BPA
            step += 1
            set_status(_fa_status, f"{item_label} \u2014 Model BPA ({step}/{total_steps})", gray_color)
            try:
                from sempy_labs._fix_model_bpa import _RULE_TO_FIXER as _mbpa_map
                _ipd.display = lambda *a, **kw: None
                try:
                    from sempy_labs import run_model_bpa
                    df = run_model_bpa(dataset=item, workspace=ws, return_dataframe=True)
                finally:
                    _ipd.display = _orig_display
                if df is not None and len(df) > 0:
                    for _, row in df.iterrows():
                        rule = str(row.get("Rule Name", ""))
                        obj = str(row.get("Object Name", ""))
                        sev = str(row.get("Severity", ""))
                        has_fix = rule.lower().strip() in _mbpa_map
                        fix_tag = "" if has_fix else " (no auto-fix)"
                        detail = f"[Sev {sev}] {rule}: {obj}{fix_tag}"
                        _fa_findings.append(("\U0001F4CB Model BPA", item, rule, detail))
            except Exception:
                pass

            # 4) Report BPA
            step += 1
            set_status(_fa_status, f"{item_label} \u2014 Report BPA ({step}/{total_steps})", gray_color)
            try:
                from sempy_labs.report._fix_report_bpa import _RULE_TO_FIXER as _rbpa_map
                _ipd.display = lambda *a, **kw: None
                try:
                    from sempy_labs.report import run_report_bpa
                    df = run_report_bpa(report=item, workspace=ws, return_dataframe=True)
                finally:
                    _ipd.display = _orig_display
                if df is not None and len(df) > 0:
                    for _, row in df.iterrows():
                        rule = str(row.get("Rule Name", ""))
                        obj = str(row.get("Object Name", ""))
                        sev = str(row.get("Severity", ""))
                        has_fix = rule.lower().strip() in _rbpa_map
                        fix_tag = "" if has_fix else " (no auto-fix)"
                        detail = f"[Sev {sev}] {rule}: {obj}{fix_tag}"
                        _fa_findings.append(("\U0001F4C4 Report BPA", item, rule, detail))
            except Exception:
                pass
        return None

    def _fa_build_tree():
        """Build tree: Category → Item → Fixer/Rule → Finding."""
        from sempy_labs._ui_components import EXPANDED as _EXP
        options = []
        _fa_key_map.clear()

        from collections import OrderedDict
        grouped = OrderedDict()
        for i, (cat, item, fixer, detail) in enumerate(_fa_findings):
            grouped.setdefault(cat, OrderedDict()).setdefault(item, OrderedDict()).setdefault(fixer, []).append((i, detail))

        for cat, items in grouped.items():
            cat_count = sum(len(fs) for it in items.values() for fs in it.values())
            cat_line = f"{_EXP} {cat}  [{cat_count}]"
            options.append(cat_line)
            _fa_key_map[cat_line] = ("category", cat, None, None, None)
            for item_name, fixers in items.items():
                item_count = sum(len(fs) for fs in fixers.values())
                item_line = f"    {_EXP} {item_name}  [{item_count}]"
                while item_line in _fa_key_map:
                    item_line += "\u200b"
                options.append(item_line)
                _fa_key_map[item_line] = ("item", cat, item_name, None, None)
                for fixer_name, findings_list in fixers.items():
                    fixer_label = fixer_name.strip()
                    fixer_line = f"        {_EXP} {fixer_label}  [{len(findings_list)}]"
                    while fixer_line in _fa_key_map:
                        fixer_line += "\u200b"
                    options.append(fixer_line)
                    _fa_key_map[fixer_line] = ("fixer", cat, item_name, fixer_name, None)
                    for idx, detail in findings_list:
                        d_line = f"            \u2022 {detail}"
                        while d_line in _fa_key_map:
                            d_line += "\u200b"
                        options.append(d_line)
                        _fa_key_map[d_line] = ("detail", cat, item_name, fixer_name, idx)
        _fa_tree.options = options
        _fa_tree.value = list(options)

    def _on_fa_scan(_):
        ws = workspace_input.value.strip() or None
        rpt_input = report_input.value.strip()
        items = [x.strip() for x in rpt_input.split(",") if x.strip()] if rpt_input else []
        if not items:
            set_status(_fa_status, "Enter report/model name(s) above.", "#ff3b30")
            return
        _fa_scan_btn.disabled = True
        _fa_scan_btn.description = "Scanning\u2026"
        set_status(_fa_status, f"Scanning {len(items)} item(s)\u2026", gray_color)
        err = _fa_do_scan()
        if err:
            set_status(_fa_status, err, "#ff3b30")
            _fa_scan_btn.disabled = False
            _fa_scan_btn.description = "\U0001F50D Scan Everything"
            return
        if not _fa_findings:
            set_status(_fa_status, "\u2713 No fixable issues found across all categories.", "#34c759")
            _fa_tree.options = []
        else:
            _fa_build_tree()
            set_status(_fa_status, f"\U0001F50D {len(_fa_findings)} finding(s). Deselect items to exclude, then Fix Selected.", "#ff9500")
        _fa_scan_btn.disabled = False
        _fa_scan_btn.description = "\U0001F50D Scan Everything"

    def _on_fa_fix(_):
        ws = workspace_input.value.strip() or None
        selected = set(_fa_tree.value)
        if not selected:
            set_status(_fa_status, "Nothing selected.", "#ff9500")
            return

        # Collect (category, item, fixer) tuples to run
        from collections import OrderedDict
        to_run = OrderedDict()
        for opt in selected:
            if opt not in _fa_key_map:
                continue
            kind, cat, item_name, fixer_name, idx = _fa_key_map[opt]
            if kind == "category":
                for v in _fa_key_map.values():
                    if v[1] == cat and v[0] in ("fixer", "detail"):
                        to_run[(cat, v[2], v[3])] = True
            elif kind == "item":
                for v in _fa_key_map.values():
                    if v[1] == cat and v[2] == item_name and v[0] in ("fixer", "detail"):
                        to_run[(cat, item_name, v[3])] = True
            elif kind == "fixer":
                to_run[(cat, item_name, fixer_name)] = True
            elif kind == "detail":
                to_run[(cat, item_name, fixer_name)] = True

        if not to_run:
            set_status(_fa_status, "Nothing actionable selected.", "#ff9500")
            return

        _fa_fix_btn.disabled = True
        _fa_fix_btn.description = "Fixing\u2026"
        import io as _io
        from contextlib import redirect_stdout as _redirect
        ok = 0
        err = 0
        # Track already-run BPA fixers to avoid duplicates (BPA fixers run per-model, not per-rule)
        _bpa_model_done = set()  # (item, fixer_key)
        _bpa_report_done = set()

        for (cat, item_name, fixer_name) in to_run:
            try:
                ran = False
                buf = _io.StringIO()
                with _redirect(buf):
                    if cat == "\U0001F4CA Report Fixers":
                        if fixer_name in _rpt_fixer_cbs:
                            _rpt_fixer_cbs[fixer_name](report=item_name, page_name=None, workspace=ws, scan_only=False)
                            ran = True
                    elif cat == "\U0001F4C4 Model Fixers":
                        if fixer_name in _model_fixer_cbs:
                            _model_fixer_cbs[fixer_name](report=item_name, workspace=ws, scan_only=False)
                            ran = True
                    elif cat == "\U0001F4CB Model BPA":
                        # Use standalone fixer via rule mapping
                        from sempy_labs._fix_model_bpa import _RULE_TO_FIXER as _mbpa_map
                        rule_key = fixer_name.lower().strip()
                        if rule_key in _mbpa_map and (item_name, rule_key) not in _bpa_model_done:
                            _bpa_model_done.add((item_name, rule_key))
                            import importlib
                            mod_path, fn_name = _mbpa_map[rule_key]
                            mod = importlib.import_module(mod_path)
                            fn = getattr(mod, fn_name)
                            fn(dataset=item_name, workspace=ws, scan_only=False)
                            ran = True
                    elif cat == "\U0001F4C4 Report BPA":
                        from sempy_labs.report._fix_report_bpa import _RULE_TO_FIXER as _rbpa_map
                        rule_key = fixer_name.lower().strip()
                        if rule_key in _rbpa_map and (item_name, rule_key) not in _bpa_report_done:
                            _bpa_report_done.add((item_name, rule_key))
                            import importlib
                            mod_path, fn_name = _rbpa_map[rule_key]
                            mod = importlib.import_module(mod_path)
                            fn = getattr(mod, fn_name)
                            fn(report=item_name, workspace=ws, scan_only=False)
                            ran = True
                if ran:
                    ok += 1
            except Exception:
                err += 1

        summary = f"\u2713 Applied {ok} fixer(s) across {len(set(i for _, i, _ in to_run))} item(s)."
        if err:
            summary = f"\u26a0\ufe0f {ok} OK, {err} error(s)."
        set_status(_fa_status, summary, "#34c759" if not err else "#ff9500")
        _fa_fix_btn.disabled = False
        _fa_fix_btn.description = "\u2705 Fix Selected"

    def _on_fa_select_all(_):
        _fa_tree.value = list(_fa_tree.options)
        set_status(_fa_status, f"\u2705 All {len(_fa_tree.options)} items selected.", "#34c759")
    def _on_fa_deselect_all(_):
        _fa_tree.value = []
        set_status(_fa_status, "\u2B1C All items deselected.", gray_color)

    _fa_scan_btn.on_click(_on_fa_scan)
    _fa_fix_btn.on_click(_on_fa_fix)
    _fa_select_all_btn.on_click(_on_fa_select_all)
    _fa_deselect_all_btn.on_click(_on_fa_deselect_all)

    fix_all_content = widgets.VBox(
        [_fa_header, _fa_btn_row, _fa_tree],
        layout=widgets.Layout(
            padding="12px", gap="6px",
            border=f"1px solid {border_color}",
            border_radius="8px",
            background_color=section_bg,
        ),
    )

    # -----------------------------
    # TAB SELECTOR (ToggleButtons — more reliable than widgets.Tab in Fabric)
    # -----------------------------
    _fixer_visible = show_fixer_tab
    _tab_options = ["\u26A1 Fix All"]
    if model_explorer_tab is not None:
        _tab_options.append("\U0001F4C4 Model")
    if report_explorer_tab is not None:
        _tab_options.append("\U0001F4CA Report")
    if _fixer_visible:
        _tab_options.append("\u26A1 Fixer")

    # Extra tab specs (used by both all_tabs=True and the dynamic "Show All Tabs" button)
    _extra_tab_specs = []
    if perspective_editor_tab is not None:
        _extra_tab_specs.append(("\U0001F441 Perspectives", None))
    _extra_tab_specs.extend([
        ("\U0001F310 Translations", None),
        ("\U0001F4BE Memory Analyzer", None),
        ("\U0001F4CB Model BPA", None),
        ("\U0001F4C4 Report BPA", None),
        ("\U0001F4D0 Delta Analyzer", None),
        ("\u270F\uFE0F Prototype", None),
        ("\U0001F5FA Model Diagram", None),
    ])

    if all_tabs:
        for label, _ in _extra_tab_specs:
            _tab_options.append(label)
    _tab_options.append("\u2139\ufe0f About")
    if not _tab_options:
        _tab_options = ["\u26A1 Fixer"]
        _fixer_visible = True

    tab_selector = widgets.ToggleButtons(
        options=_tab_options,
        value=_tab_options[1] if len(_tab_options) > 1 else _tab_options[0],
        layout=widgets.Layout(margin="0"),
    )
    tab_selector.style.button_width = "110px"

    # -----------------------------
    # SECTION HEADING HELPER
    # -----------------------------
    def _section_heading(text):
        return widgets.HTML(
            value=f'<div style="font-size:13px; font-weight:600; color:{icon_accent}; '
            f'font-family:-apple-system,BlinkMacSystemFont,sans-serif; '
            f'text-transform:uppercase; letter-spacing:0.5px; margin-bottom:6px;">'
            f'{text}</div>'
        )

    def _fixer_label(title, description):
        return widgets.HTML(
            value=f'<div style="font-family:-apple-system,BlinkMacSystemFont,sans-serif;">'
            f'<span style="font-size:14px; font-weight:500; color:{text_color};">{title}</span>'
            f'<span style="font-size:12px; color:{gray_color}; margin-left:8px;">{description}</span>'
            f'</div>'
        )

    # -----------------------------
    # REPORT FIXERS — VISUALS
    # -----------------------------
    cb_pie = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))
    cb_bar = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))
    cb_col = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))
    cb_line = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))
    cb_col2line = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))
    cb_col2bar = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))
    cb_ibcs_var = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))
    cb_page_size = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))
    cb_hide_filters = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))
    cb_remove_cv = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))
    cb_no_data = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))
    cb_migrate_rlm = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))

    pie_row = widgets.HBox(
        [cb_pie, _fixer_label("Fix Pie Charts", "replaces all pie charts â†’ Clustered Bar Chart (default)")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )
    bar_row = widgets.HBox(
        [cb_bar, _fixer_label("Fix Bar Charts", "remove axis titles/values · add data labels · remove gridlines")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )
    col_row = widgets.HBox(
        [cb_col, _fixer_label("Fix Column Charts", "remove axis titles/values · add data labels · remove gridlines")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )
    col2line_row = widgets.HBox(
        [cb_col2line, _fixer_label("Fix Column→Line", "converts column charts to line charts when category axis is Date/DateTime")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )
    col2bar_row = widgets.HBox(
        [cb_col2bar, _fixer_label("Fix Column→Bar (IBCS)", "converts non-time column charts to bar charts (IBCS: structural = horizontal)")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )
    ibcs_var_row = widgets.HBox(
        [cb_ibcs_var, _fixer_label("Fix IBCS Variance", "adds PY measures + error bars + IBCS colors to bar/column charts")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )
    line_row = widgets.HBox(
        [cb_line, _fixer_label("Fix Line Charts", "remove axis titles · add data labels · remove gridlines · keep Y value axis")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )
    page_size_row = widgets.HBox(
        [cb_page_size, _fixer_label("Fix Page Size", "changes default 720×1280 pages to 1080×1920 (Full HD)")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )
    hide_filters_row = widgets.HBox(
        [cb_hide_filters, _fixer_label("Hide Visual Filters", "sets isHiddenInViewMode on all visual-level filters")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )
    remove_cv_row = widgets.HBox(
        [cb_remove_cv, _fixer_label("Remove Unused Custom Visuals", "removes custom visuals not used by any visual")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )
    no_data_row = widgets.HBox(
        [cb_no_data, _fixer_label("Disable Show Items No Data", "disables 'Show items with no data' on all visuals")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )
    migrate_rlm_row = widgets.HBox(
        [cb_migrate_rlm, _fixer_label("Migrate Report-Level Measures", "moves report-level measures into the semantic model")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )

    cb_upgrade = widgets.Checkbox(value=False, indent=False, layout=widgets.Layout(width="22px"))
    upgrade_row = widgets.HBox(
        [cb_upgrade, _fixer_label("Upgrade to PBIR", "converts PBIRLegacy \u2192 PBIR via REST round-trip (runs first)")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )

    # Only show rows for available fixers
    _report_fixer_rows = [_section_heading("Report — Visuals")]
    if fix_upgrade_to_pbir is not None:
        _report_fixer_rows.append(upgrade_row)
    if fix_piecharts is not None:
        _report_fixer_rows.append(pie_row)
    if fix_barcharts is not None:
        _report_fixer_rows.append(bar_row)
    if fix_columncharts is not None:
        _report_fixer_rows.append(col_row)
    if fix_column_to_line is not None:
        _report_fixer_rows.append(col2line_row)
    if fix_column_to_bar is not None:
        _report_fixer_rows.append(col2bar_row)
    if fix_ibcs_variance is not None:
        _report_fixer_rows.append(ibcs_var_row)
    if fix_linecharts is not None:
        _report_fixer_rows.append(line_row)
    if fix_page_size is not None:
        _report_fixer_rows.append(page_size_row)
    if fix_hide_visual_filters is not None:
        _report_fixer_rows.append(hide_filters_row)
    if fix_remove_unused_cv is not None:
        _report_fixer_rows.append(remove_cv_row)
    if fix_disable_show_no_data is not None:
        _report_fixer_rows.append(no_data_row)
    if fix_migrate_rlm is not None:
        _report_fixer_rows.append(migrate_rlm_row)

    report_fixers_box = widgets.VBox(
        _report_fixer_rows,
        layout=widgets.Layout(
            gap="6px",
            padding="12px",
            margin="0 0 16px 0",
            border=f"1px solid {border_color}",
            border_radius="8px",
            background_color=section_bg,
        ),
    )

    # -----------------------------
    # SEMANTIC MODEL FIXERS
    # -----------------------------
    cb_calendar = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))
    cb_discourage = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))
    cb_last_refresh = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))
    cb_units = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))
    cb_time_intel = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))
    cb_measure_tbl = widgets.Checkbox(value=True, indent=False, layout=widgets.Layout(width="22px"))

    # datasource_version_row removed — requires Large SM storage format enabled first
    calendar_row = widgets.HBox(
        [cb_calendar, _fixer_label("Add Calendar Table", "adds \"CalcCalendar\" calculated table if no table has been \"marked\" as a date table")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )
    discourage_row = widgets.HBox(
        [cb_discourage, _fixer_label("Discourage Implicit Measures", "sets DiscourageImplicitMeasures to True (recommended &amp; required for calc groups)")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )
    last_refresh_row = widgets.HBox(
        [cb_last_refresh, _fixer_label("Add Last Refresh Table", "adds a \"Last Refresh\" table with M partition &amp; measure showing refresh timestamp")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )
    units_row = widgets.HBox(
        [cb_units, _fixer_label("Add Units Calc Group", "Thousand &amp; Million items · skips % / ratio measures · ⚡ can impact report performance")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )
    time_intel_row = widgets.HBox(
        [cb_time_intel, _fixer_label("Add Time Intelligence Calc Group", "AC · Y-1/Y-2/Y-3 · YTD · abs/rel/achiev. variances · requires calendar table")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )
    measure_tbl_row = widgets.HBox(
        [cb_measure_tbl, _fixer_label("Add Measure Table", "adds an empty \"Measure\" calculated table to centralise measures")],
        layout=widgets.Layout(align_items="center", gap="6px"),
    )

    # XMLA warning + confirmation — shown only when ≥1 SM fixer is checked
    cb_sm_confirm = widgets.Checkbox(
        value=False, indent=False, layout=widgets.Layout(width="22px"),
    )
    sm_warning_text = widgets.HTML(
        value=f'<span style="font-size:12px; color:#856404; '
        f'font-family:-apple-system,BlinkMacSystemFont,sans-serif;">'
        f'⚠️ <b>XMLA write</b> — Semantic model fixers use the XMLA endpoint. '
        f'Once modified, the model can no longer be downloaded as a .pbix with embedded data. '
        f'This is irreversible. <b>Tick to confirm.</b></span>'
    )
    sm_warning_confirm = widgets.HBox(
        [cb_sm_confirm, sm_warning_text],
        layout=widgets.Layout(
            align_items="center", gap="8px",
            padding="6px 10px",
            border="1px solid #ffc107", border_radius="6px",
            display="none",
        ),
    )
    sm_warning_confirm.add_class("sm-xmla-warning")

    # Show/hide warning when any SM fixer checkbox changes
    _sm_checkboxes = [cb_calendar, cb_discourage, cb_last_refresh, cb_units, cb_time_intel, cb_measure_tbl]

    def _on_sm_cb_change(change=None):
        any_checked = any(cb.value for cb in _sm_checkboxes)
        writes = mode_toggle.value != "Scan"
        sm_warning_confirm.layout.display = "flex" if (any_checked and writes) else "none"
        if not any_checked or not writes:
            cb_sm_confirm.value = False

    for _cb in _sm_checkboxes:
        _cb.observe(_on_sm_cb_change, names="value")
    _on_sm_cb_change()  # evaluate initial state so warning shows if checkboxes start checked

    # Only show rows for available fixers
    _sm_fixer_rows = [_section_heading("Semantic Model")]
    if fix_discourage_implicit_measures is not None:
        _sm_fixer_rows.append(discourage_row)
    if add_calculated_calendar is not None:
        _sm_fixer_rows.append(calendar_row)
    if add_last_refresh_table is not None:
        _sm_fixer_rows.append(last_refresh_row)
    if add_measure_table is not None:
        _sm_fixer_rows.append(measure_tbl_row)
    if add_calc_group_units is not None:
        _sm_fixer_rows.append(units_row)
    if add_calc_group_time_intelligence is not None:
        _sm_fixer_rows.append(time_intel_row)
    _sm_fixer_rows.append(sm_warning_confirm)

    semantic_model_box = widgets.VBox(
        _sm_fixer_rows,
        layout=widgets.Layout(
            gap="6px",
            padding="12px",
            margin="0 0 16px 0",
            border=f"1px solid {border_color}",
            border_radius="8px",
            background_color=section_bg,
        ),
    )

    # -----------------------------
    # RUN BUTTON
    # -----------------------------
    run_btn = widgets.Button(
        description="Run",
        button_style="primary",
        layout=widgets.Layout(width="100px"),
    )

    god_btn = widgets.Button(
        description="\u26A1 Fix Everything",
        button_style="danger",
        layout=widgets.Layout(width="160px"),
    )

    button_row = widgets.HBox(
        [god_btn, run_btn],
        layout=widgets.Layout(justify_content="flex-end", gap="8px", margin="0 0 8px 0"),
    )

    # -----------------------------
    # RUN HANDLER
    # -----------------------------
    report_fixers = [
        # (checkbox, label, callable) — Upgrade to PBIR runs first
        x for x in [
            (cb_upgrade, "Upgrade to PBIR", lambda r, p, w, s: fix_upgrade_to_pbir(report=r, page_name=p, workspace=w, scan_only=s)) if fix_upgrade_to_pbir else None,
            (cb_pie, "Fix Pie Charts", lambda r, p, w, s: fix_piecharts(report=r, page_name=p, workspace=w, scan_only=s)) if fix_piecharts else None,
            (cb_bar, "Fix Bar Charts", lambda r, p, w, s: fix_barcharts(report=r, page_name=p, workspace=w, scan_only=s)) if fix_barcharts else None,
            (cb_col, "Fix Column Charts", lambda r, p, w, s: fix_columncharts(report=r, page_name=p, workspace=w, scan_only=s)) if fix_columncharts else None,
            (cb_col2line, "Fix Column→Line", lambda r, p, w, s: fix_column_to_line(report=r, page_name=p, workspace=w, scan_only=s)) if fix_column_to_line else None,
            (cb_col2bar, "Fix Column→Bar (IBCS)", lambda r, p, w, s: fix_column_to_bar(report=r, page_name=p, workspace=w, scan_only=s)) if fix_column_to_bar else None,
            (cb_ibcs_var, "Fix IBCS Variance", lambda r, p, w, s: fix_ibcs_variance(report=r, page_name=p, workspace=w, scan_only=s)) if fix_ibcs_variance else None,
            (cb_line, "Fix Line Charts", lambda r, p, w, s: fix_linecharts(report=r, page_name=p, workspace=w, scan_only=s)) if fix_linecharts else None,
            (cb_page_size, "Fix Page Size", lambda r, p, w, s: fix_page_size(report=r, page_name=p, workspace=w, scan_only=s)) if fix_page_size else None,
            (cb_hide_filters, "Hide Visual Filters", lambda r, p, w, s: fix_hide_visual_filters(report=r, page_name=p, workspace=w, scan_only=s)) if fix_hide_visual_filters else None,
            (cb_remove_cv, "Remove Unused Custom Visuals", lambda r, p, w, s: fix_remove_unused_cv(report=r, page_name=p, workspace=w, scan_only=s)) if fix_remove_unused_cv else None,
            (cb_no_data, "Disable Show Items No Data", lambda r, p, w, s: fix_disable_show_no_data(report=r, page_name=p, workspace=w, scan_only=s)) if fix_disable_show_no_data else None,
            (cb_migrate_rlm, "Migrate Report-Level Measures", lambda r, p, w, s: fix_migrate_rlm(report=r, page_name=p, workspace=w, scan_only=s)) if fix_migrate_rlm else None,
        ] if x is not None
    ]

    sm_fixers = [
        x for x in [
            (cb_discourage, "Discourage Implicit Measures", lambda r, w, s: fix_discourage_implicit_measures(report=r, workspace=w, scan_only=s)) if fix_discourage_implicit_measures else None,
            (cb_calendar, "Add Calendar Table", lambda r, w, s: add_calculated_calendar(report=r, workspace=w, scan_only=s)) if add_calculated_calendar else None,
            (cb_measure_tbl, "Add Measure Table", lambda r, w, s: add_measure_table(report=r, workspace=w, scan_only=s)) if add_measure_table else None,
            (cb_last_refresh, "Add Last Refresh Table", lambda r, w, s: add_last_refresh_table(report=r, workspace=w, scan_only=s)) if add_last_refresh_table else None,
            (cb_units, "Add Units Calc Group", lambda r, w, s: add_calc_group_units(report=r, workspace=w, scan_only=s)) if add_calc_group_units else None,
            (cb_time_intel, "Add Time Intelligence Calc Group", lambda r, w, s: add_calc_group_time_intelligence(report=r, workspace=w, scan_only=s)) if add_calc_group_time_intelligence else None,
        ] if x is not None
    ]

    def on_run(_):
        ws = workspace_input.value.strip() or None
        report_val = report_input.value.strip()
        page = page_input.value.strip() or None
        mode = mode_toggle.value

        # Parse comma-separated items
        items = [_strip_item_prefix(x.strip()) for x in report_val.split(",") if x.strip()] if report_val else []

        if not items:
            show_status("Please enter at least one report / SM name or ID.", "#ff3b30")
            return

        status.value = ""
        run_btn.disabled = True
        god_btn.disabled = True
        run_btn.description = "Running…"

        rpt_selected = [(cb, label, fn) for cb, label, fn in report_fixers if cb.value]
        sm_selected  = [(cb, label, fn) for cb, label, fn in sm_fixers if cb.value]
        total_fixers = len(rpt_selected) + len(sm_selected)

        if total_fixers == 0:
            show_status("Please select at least one fixer.", "#ff3b30")
            run_btn.disabled = False
            god_btn.disabled = False
            run_btn.description = "Run"
            return

        # Require confirmation when SM fixers are selected in a mode that writes
        if sm_selected and mode != "Scan" and not cb_sm_confirm.value:
            show_status(
                "⚠️  Please tick the XMLA confirmation checkbox before running semantic model fixers.",
                "#ff9500",
            )
            run_btn.disabled = False
            god_btn.disabled = False
            run_btn.description = "Run"
            return

        def _do_work():
            """Runs selected fixers on each item, with timeout and PBIR gate."""
            start_time = time.time()
            try:
                _progress_lines.clear()
                progress.layout.display = ""
                total = total_fixers * len(items)

                def _log(text=""):
                    _progress_lines.append(text)
                    progress.value = (
                        '<div style="font-family:-apple-system,BlinkMacSystemFont,sans-serif; '
                        'font-size:13px; margin:0; padding:10px; '
                        'max-height:540px; overflow-y:auto; white-space:pre-wrap;">'
                        + "\n".join(_progress_lines)
                        + "</div>"
                    )

                _log(f"{total_fixers} Fixer(s) × {len(items)} Item(s) = {total} total  [Mode: {mode}]")
                _log()
                _log(f"  Workspace: {ws or 'Notebook workspace'}")
                _log(f"  Items:     {', '.join(items)}")
                _log(f"  Page:      {page or 'All'}")
                _log()

                idx = 0
                errors = 0
                timed_out = False

                def _check_timeout():
                    nonlocal timed_out
                    if time.time() - start_time > _TOTAL_TIMEOUT:
                        _log(f"⏱️  5-minute timeout reached ({int(time.time() - start_time)}s). Aborting.")
                        timed_out = True
                        return True
                    return False

                # PBIR gate state
                upgrade_selected = any(label == "Upgrade to PBIR" for _, label, _ in rpt_selected)
                non_upgrade_rpt = [x for x in rpt_selected if x[1] != "Upgrade to PBIR"]

                def _run_report_fixers(scan: bool):
                    nonlocal idx, errors
                    prefix = "🔍" if scan else "▶"
                    for item in items:
                        if _check_timeout():
                            return
                        # PBIR gate (fix mode only)
                        if not scan and non_upgrade_rpt:
                            fmt = _check_report_format(item, ws)
                            if fmt == "PBIRLegacy" and not upgrade_selected:
                                _log(f"⚠️  '{item}' is in PBIRLegacy format — skipping report fixers.")
                                _log(f"    â†’ Enable 'Upgrade to PBIR' to convert automatically.")
                                _log()
                                idx += len(rpt_selected)
                                errors += len(rpt_selected)
                                continue
                        for cb, label, fn in rpt_selected:
                            if _check_timeout():
                                return
                            idx += 1
                            suffix = f" on '{item}'" if len(items) > 1 else ""
                            _log(f"{prefix} [{idx}/{total}] {'Scanning ' if scan else ''}{label}{suffix}...")
                            try:
                                buf = io.StringIO()
                                with redirect_stdout(buf):
                                    fn(item, page, ws, scan)
                                captured = buf.getvalue().rstrip()
                                if captured:
                                    for line in captured.splitlines():
                                        _log(f"   {line}")
                            except Exception as e:
                                errors += 1
                                _log(f"   âŒ Error: {e}")
                            _log()

                def _run_sm_fixers(scan: bool):
                    nonlocal idx, errors
                    prefix = "🔍" if scan else "▶"
                    for item in items:
                        if _check_timeout():
                            return
                        for _, label, fn in sm_selected:
                            if _check_timeout():
                                return
                            idx += 1
                            suffix = f" on '{item}'" if len(items) > 1 else ""
                            _log(f"{prefix} [{idx}/{total}] {'Scanning ' if scan else ''}{label}{suffix}...")
                            try:
                                buf = io.StringIO()
                                with redirect_stdout(buf):
                                    fn(item, ws, scan)
                                captured = buf.getvalue().rstrip()
                                if captured:
                                    for line in captured.splitlines():
                                        _log(f"   {line}")
                            except Exception as e:
                                errors += 1
                                _log(f"   âŒ Error: {e}")
                            _log()

                if mode == "Scan":
                    _run_report_fixers(scan=True)
                    _run_sm_fixers(scan=True)
                elif mode == "Fix":
                    _run_report_fixers(scan=False)
                    _run_sm_fixers(scan=False)
                else:  # Scan + Fix
                    _log("â”€" * 40)
                    _log("PHASE 1: Scan")
                    _log()
                    _run_report_fixers(scan=True)
                    _run_sm_fixers(scan=True)
                    idx = 0
                    _log("â”€" * 40)
                    _log("PHASE 2: Fix")
                    _log()
                    _run_report_fixers(scan=False)
                    _run_sm_fixers(scan=False)

                elapsed = int(time.time() - start_time)
                if timed_out:
                    show_status(
                        f"⏱️  Timed out after {elapsed}s. {idx}/{total} run(s), {errors} error(s).",
                        "#ff9500",
                    )
                elif errors > 0:
                    show_status(
                        f"⚠️  Completed with {errors} error(s) out of {total} run(s) in {elapsed}s.",
                        "#ff9500",
                    )
                elif mode == "Scan":
                    show_status(f"✓  Scan complete — {total} run(s) in {elapsed}s.", "#007aff")
                elif mode == "Fix":
                    show_status(f"✓  All {total} run(s) completed in {elapsed}s.", "#34c759")
                else:
                    show_status(f"✓  Scan + Fix complete — {total} run(s) in {elapsed}s.", "#34c759")

            except Exception as e:
                show_status(f"Error: {e}", "#ff3b30")

            finally:
                run_btn.disabled = False
                god_btn.disabled = False
                run_btn.description = "Run"

        _do_work()

    run_btn.on_click(on_run)

    def on_god_btn(_):
        """Select all fixers and run."""
        all_cbs = [cb_pie, cb_bar, cb_col, cb_page_size, cb_hide_filters, cb_remove_cv, cb_no_data, cb_migrate_rlm]
        if fix_upgrade_to_pbir is not None:
            all_cbs.append(cb_upgrade)
        for cb in all_cbs:
            cb.value = True
        for cb in _sm_checkboxes:
            cb.value = True
        cb_sm_confirm.value = True
        on_run(None)

    god_btn.on_click(on_god_btn)

    # -----------------------------
    # ASSEMBLE & DISPLAY
    # -----------------------------
    version_footer = widgets.HTML(
        value=f'<div style="text-align:right; font-size:11px; color:{gray_color}; '
        f'font-family:-apple-system,BlinkMacSystemFont,sans-serif; '
        f'margin-top:8px; padding-top:8px; border-top:1px solid {border_color};">'
        f'v{__version__} \u2022 Alexander Korn \u2022 '
        f'<a href="https://actionablereporting.com" target="_blank" '
        f'style="color:{icon_accent}; text-decoration:none;">actionablereporting.com</a></div>'
    )

    # -- Fixer tab content (existing UI, minus inputs which are now shared) --
    page_input_row = widgets.HBox(
        [_input_label("Page (opt.)"), page_input],
        layout=widgets.Layout(align_items="center", gap="8px", margin="0 0 12px 0"),
    )
    fixer_content = widgets.VBox(
        [
            mode_row,
            page_input_row,
            report_fixers_box,
            semantic_model_box,
            button_row,
            progress,
            status,
        ],
        layout=widgets.Layout(padding="4px 0"),
    )

    # -- Build fixer callbacks for Report Explorer actions dropdown (grouped) --
    _rpt_fixer_cbs = {}
    _rpt_noop = lambda **kw: None

    # ── Format ──
    _rpt_fixer_cbs["── Format ──"] = _rpt_noop
    if fix_upgrade_to_pbir is not None:
        _rpt_fixer_cbs["Convert to PBIR"] = lambda **kw: fix_upgrade_to_pbir(**kw)

    # ── Chart Fixers ──
    _rpt_fixer_cbs["── Chart Fixers ──"] = _rpt_noop
    if fix_piecharts is not None:
        _rpt_fixer_cbs["Fix Pie Charts"] = lambda **kw: fix_piecharts(**kw)
    if fix_charts is not None:
        _rpt_fixer_cbs["Fix All Charts"] = lambda **kw: fix_charts(**kw)
    if fix_ibcs_variance is not None:
        _rpt_fixer_cbs["Fix IBCS Variance"] = lambda **kw: fix_ibcs_variance(**kw)

    # ── Layout & Cleanup ──
    _rpt_fixer_cbs["── Layout & Cleanup ──"] = _rpt_noop
    if fix_page_size is not None:
        _rpt_fixer_cbs["Fix Page Size"] = lambda **kw: fix_page_size(**kw)
    if fix_hide_visual_filters is not None:
        _rpt_fixer_cbs["Hide Visual Filters"] = lambda **kw: fix_hide_visual_filters(**kw)
    if fix_remove_unused_cv is not None:
        _rpt_fixer_cbs["Remove Unused Custom Visuals"] = lambda **kw: fix_remove_unused_cv(**kw)
    if fix_disable_show_no_data is not None:
        _rpt_fixer_cbs["Disable Show Items No Data"] = lambda **kw: fix_disable_show_no_data(**kw)
    if fix_migrate_rlm is not None:
        _rpt_fixer_cbs["Migrate Report-Level Measures"] = lambda **kw: fix_migrate_rlm(**kw)

    # Visual Alignment fixer
    fix_visual_alignment = _lazy_import("sempy_labs.report._Fix_VisualAlignment", "fix_visual_alignment")
    if fix_visual_alignment is not None:
        _rpt_fixer_cbs["Fix Visual Alignment"] = lambda **kw: fix_visual_alignment(**kw)

    # ── Theme ──
    _rpt_fixer_cbs["── Theme ──"] = _rpt_noop
    _show_theme = _lazy_import("sempy_labs.report._report_theme", "show_theme_summary")
    _update_theme = _lazy_import("sempy_labs.report._report_theme", "update_theme_colors")
    if _show_theme is not None:
        _rpt_fixer_cbs["Show Theme Summary"] = lambda **kw: _show_theme(report=kw.get("report", ""), workspace=kw.get("workspace"))
    if _update_theme is not None:
        def _apply_ibcs_theme(**kw):
            """Apply IBCS-style data colors (black/grey/red/green)."""
            _update_theme(
                report=kw.get("report", ""), workspace=kw.get("workspace"),
                data_colors=["#404040", "#808080", "#C0C0C0", "#CC0000", "#00CC00", "#FFB800", "#0066CC", "#993399"],
                background="#FFFFFF", foreground="#404040", table_accent="#404040",
                scan_only=kw.get("scan_only", False),
            )
        _rpt_fixer_cbs["Apply IBCS Theme"] = lambda **kw: _apply_ibcs_theme(**kw)

    # ── Create & Delete ──
    _rpt_fixer_cbs["── Create & Delete ──"] = _rpt_noop
    def _rpt_delete_selected(**kw):
        """Delete selected visuals or pages from the report."""
        import ipywidgets as _w
        rpt = kw.get("report", "")
        ws = kw.get("workspace")
        sel_keys = kw.get("selected_keys", [])
        if not rpt:
            print("[!] No report loaded.")
            return None
        if not sel_keys:
            print("[!] Select one or more visuals or pages in the tree first.")
            return None

        # Parse selected keys into visual/page references
        items_to_delete = []  # [(type, page_name, visual_name), ...]
        for key in sel_keys:
            if key.startswith("visual:"):
                v_raw = key.split(":", 1)[1]
                if "\x1f" in v_raw:
                    _, rest = v_raw.split("\x1f", 1)
                    page_name, visual_name = rest.rsplit(":", 1)
                else:
                    page_name, visual_name = v_raw.rsplit(":", 1)
                items_to_delete.append(("visual", page_name, visual_name))
            elif key.startswith("page:"):
                p_raw = key.split(":", 1)[1]
                if "\x1f" in p_raw:
                    _, page_name = p_raw.split("\x1f", 1)
                else:
                    page_name = p_raw
                items_to_delete.append(("page", page_name, None))

        if not items_to_delete:
            print("[!] No visuals or pages selected for deletion.")
            return None

        header = _w.HTML(
            value=f'<div style="font-size:13px; font-weight:600; color:#ff3b30; '
            f'font-family:-apple-system,BlinkMacSystemFont,sans-serif; margin-bottom:4px;">'
            f'Delete {len(items_to_delete)} item(s) from \'{rpt}\'</div>'
        )
        items_html = ""
        for item_type, page_name, visual_name in items_to_delete:
            if item_type == "page":
                items_html += f'<div style="font-size:12px; font-family:monospace; padding:2px 0;">\u2022 Page: \'{page_name}\'</div>'
            else:
                items_html += f'<div style="font-size:12px; font-family:monospace; padding:2px 0;">\u2022 Visual: \'{visual_name}\' on page \'{page_name}\'</div>'
        items_list = _w.HTML(value=items_html)
        warning = _w.HTML(
            value='<div style="font-size:11px; color:#856404; padding:4px 8px; '
            'background:#fff3cd; border-radius:4px; margin:4px 0;">'
            'This will permanently delete the selected items from the report definition.</div>'
        )
        delete_btn = _w.Button(description="Confirm Delete", button_style="danger", layout=_w.Layout(width="180px", margin="8px 0 0 0"))
        status_lbl = _w.HTML(value="")

        def on_delete(_btn):
            _btn.disabled = True
            _btn.description = "Deleting\u2026"
            try:
                import json, base64
                from sempy_labs.report._reportwrapper import connect_report
                deleted = 0

                with connect_report(report=rpt, workspace=ws, readonly=False, show_diffs=False) as rw:
                    # Build page folder lookup from report definition
                    page_folders = {}  # page_display_name -> folder_name
                    for part in rw._report_definition.get("parts", []):
                        fp = part.get("path", "")
                        if fp.endswith("/page.json"):
                            page_json = json.loads(base64.b64decode(part["payload"]).decode("utf-8"))
                            display_name = page_json.get("displayName", "")
                            folder = fp.split("/")[-2]
                            page_folders[display_name] = folder

                    for item_type, page_name, visual_name in items_to_delete:
                        try:
                            page_folder = page_folders.get(page_name, "")
                            if not page_folder:
                                print(f"[ERR] Page folder not found for '{page_name}'")
                                continue

                            if item_type == "page":
                                # Remove all files under this page folder
                                pattern = f"definition/pages/{page_folder}/"
                                parts_before = len(rw._report_definition["parts"])
                                rw._report_definition["parts"] = [
                                    p for p in rw._report_definition["parts"]
                                    if not p.get("path", "").startswith(pattern)
                                ]
                                # Update pages.json to remove from pageOrder
                                for part in rw._report_definition["parts"]:
                                    if part.get("path", "") == "definition/pages/pages.json":
                                        pj = json.loads(base64.b64decode(part["payload"]).decode("utf-8"))
                                        if "pageOrder" in pj:
                                            pj["pageOrder"] = [p for p in pj["pageOrder"] if p != page_folder]
                                        part["payload"] = base64.b64encode(json.dumps(pj).encode("utf-8")).decode("utf-8")
                                        break
                                removed = parts_before - len(rw._report_definition["parts"])
                                if removed > 0:
                                    deleted += 1
                                    print(f"[OK] Deleted page '{page_name}' ({removed} files)")
                            else:
                                # Delete visual - find visual folder by matching visual name
                                visual_folder = None
                                for part in rw._report_definition["parts"]:
                                    fp = part.get("path", "")
                                    if fp.startswith(f"definition/pages/{page_folder}/visuals/") and fp.endswith("/visual.json"):
                                        v_folder = fp.split("/")[-2]
                                        if v_folder == visual_name:
                                            visual_folder = v_folder
                                            break
                                        # Also check name field in visual.json
                                        vj = json.loads(base64.b64decode(part["payload"]).decode("utf-8"))
                                        if vj.get("name", "") == visual_name:
                                            visual_folder = v_folder
                                            break
                                if not visual_folder:
                                    visual_folder = visual_name

                                pattern = f"definition/pages/{page_folder}/visuals/{visual_folder}/"
                                parts_before = len(rw._report_definition["parts"])
                                rw._report_definition["parts"] = [
                                    p for p in rw._report_definition["parts"]
                                    if not p.get("path", "").startswith(pattern)
                                ]
                                removed = parts_before - len(rw._report_definition["parts"])
                                if removed > 0:
                                    deleted += 1
                                    print(f"[OK] Deleted visual '{visual_name}' from page '{page_name}'")
                                else:
                                    print(f"[ERR] Visual '{visual_name}' not found in page '{page_name}'")
                        except Exception as e:
                            print(f"[ERR] Failed to delete: {e}")

                    if deleted > 0:
                        print(f"\n[OK] {deleted} item(s) deleted and saved.")

                status_lbl.value = f'<pre style="font-size:11px; font-family:monospace; color:#333; white-space:pre-wrap; margin-top:8px;">{deleted} item(s) deleted.</pre>'
                _btn.description = "\u2713 Deleted"
                _btn.button_style = ""
            except Exception as e:
                status_lbl.value = f'<div style="color:#ff3b30; font-size:12px; margin-top:4px;">Error: {e}</div>'
                _btn.disabled = False
                _btn.description = "Confirm Delete"

        delete_btn.on_click(on_delete)
        return _w.VBox([header, items_list, warning, delete_btn, status_lbl], layout=_w.Layout(padding="8px"))

    _rpt_fixer_cbs["Delete Selected"] = _rpt_delete_selected

    def _rpt_duplicate_selected(**kw):
        """Duplicate selected visual or page in the report."""
        import ipywidgets as _w
        rpt = kw.get("report", "")
        ws = kw.get("workspace")
        sel_keys = kw.get("selected_keys", [])
        if not rpt:
            print("[!] No report loaded.")
            return None
        if not sel_keys:
            print("[!] Select a visual or page in the tree first.")
            return None

        # Pick the first selected visual or page
        target = None
        for key in sel_keys:
            if key.startswith("visual:"):
                v_raw = key.split(":", 1)[1]
                if "\x1f" in v_raw:
                    _, rest = v_raw.split("\x1f", 1)
                    page_name, visual_name = rest.rsplit(":", 1)
                else:
                    page_name, visual_name = v_raw.rsplit(":", 1)
                target = ("visual", page_name, visual_name)
                break
            elif key.startswith("page:"):
                p_raw = key.split(":", 1)[1]
                if "\x1f" in p_raw:
                    _, page_name = p_raw.split("\x1f", 1)
                else:
                    page_name = p_raw
                target = ("page", page_name, None)
                break

        if not target:
            print("[!] Select a visual or page to duplicate.")
            return None

        item_type, page_name, visual_name = target
        label = f"page '{page_name}'" if item_type == "page" else f"visual '{visual_name}' on page '{page_name}'"

        header = _w.HTML(
            value=f'<div style="font-size:13px; font-weight:600; color:{icon_accent}; '
            f'font-family:-apple-system,BlinkMacSystemFont,sans-serif; margin-bottom:4px;">'
            f'Duplicate {label}</div>'
        )
        dup_btn = _w.Button(description="Duplicate", button_style="success", layout=_w.Layout(width="180px", margin="8px 0 0 0"))
        status_lbl = _w.HTML(value="")

        def on_duplicate(_btn):
            _btn.disabled = True
            _btn.description = "Duplicating\u2026"
            try:
                import json, base64, uuid
                from sempy_labs.report._reportwrapper import connect_report

                with connect_report(report=rpt, workspace=ws, readonly=False, show_diffs=False) as rw:
                    # Build page folder lookup
                    page_folders = {}
                    for part in rw._report_definition.get("parts", []):
                        fp = part.get("path", "")
                        if fp.endswith("/page.json"):
                            pj = json.loads(base64.b64decode(part["payload"]).decode("utf-8"))
                            page_folders[pj.get("displayName", "")] = fp.split("/")[-2]

                    page_folder = page_folders.get(page_name, "")
                    if not page_folder:
                        status_lbl.value = '<div style="color:#ff3b30; font-size:12px;">Page not found.</div>'
                        _btn.disabled = False
                        _btn.description = "Duplicate"
                        return

                    if item_type == "page":
                        # Duplicate page: copy all parts under this page with a new folder name
                        new_folder = str(uuid.uuid4()).replace("-", "")[:16]
                        src_prefix = f"definition/pages/{page_folder}/"
                        new_parts = []
                        new_display_name = None
                        for part in rw._report_definition["parts"]:
                            fp = part.get("path", "")
                            if fp.startswith(src_prefix):
                                new_path = fp.replace(src_prefix, f"definition/pages/{new_folder}/")
                                new_payload = part["payload"]
                                # Update page.json display name
                                if fp.endswith("/page.json"):
                                    pj = json.loads(base64.b64decode(new_payload).decode("utf-8"))
                                    new_display_name = pj.get("displayName", "Page") + " (Copy)"
                                    pj["displayName"] = new_display_name
                                    new_payload = base64.b64encode(json.dumps(pj).encode("utf-8")).decode("utf-8")
                                new_parts.append({"path": new_path, "payload": new_payload, "payloadType": part.get("payloadType", "InlineBase64")})
                        rw._report_definition["parts"].extend(new_parts)
                        # Add to pages.json pageOrder
                        for part in rw._report_definition["parts"]:
                            if part.get("path", "") == "definition/pages/pages.json":
                                pj = json.loads(base64.b64decode(part["payload"]).decode("utf-8"))
                                if "pageOrder" in pj:
                                    # Insert after the original page
                                    idx = pj["pageOrder"].index(page_folder) if page_folder in pj["pageOrder"] else len(pj["pageOrder"])
                                    pj["pageOrder"].insert(idx + 1, new_folder)
                                part["payload"] = base64.b64encode(json.dumps(pj).encode("utf-8")).decode("utf-8")
                                break
                        # connect_report auto-saves on exit
                        status_lbl.value = f'<div style="color:#34c759; font-size:12px; margin-top:4px;">\u2713 Page duplicated as \'{new_display_name}\'.</div>'

                    else:
                        # Duplicate visual: copy visual.json with new folder name, offset position
                        new_visual_folder = str(uuid.uuid4()).replace("-", "")[:16]
                        src_visual_prefix = None
                        # Find the visual's folder by matching visual name
                        for part in rw._report_definition["parts"]:
                            fp = part.get("path", "")
                            if fp.startswith(f"definition/pages/{page_folder}/visuals/") and fp.endswith("/visual.json"):
                                v_folder = fp.split("/")[-2]
                                if v_folder == visual_name:
                                    src_visual_prefix = f"definition/pages/{page_folder}/visuals/{v_folder}/"
                                    break
                        if not src_visual_prefix:
                            # Try matching by checking name in visual.json
                            for part in rw._report_definition["parts"]:
                                fp = part.get("path", "")
                                if fp.startswith(f"definition/pages/{page_folder}/visuals/") and fp.endswith("/visual.json"):
                                    vj = json.loads(base64.b64decode(part["payload"]).decode("utf-8"))
                                    if vj.get("name", "") == visual_name:
                                        v_folder = fp.split("/")[-2]
                                        src_visual_prefix = f"definition/pages/{page_folder}/visuals/{v_folder}/"
                                        break

                        if not src_visual_prefix:
                            status_lbl.value = '<div style="color:#ff3b30; font-size:12px;">Visual not found.</div>'
                            _btn.disabled = False
                            _btn.description = "Duplicate"
                            return

                        new_parts = []
                        for part in rw._report_definition["parts"]:
                            fp = part.get("path", "")
                            if fp.startswith(src_visual_prefix):
                                new_path = fp.replace(src_visual_prefix, f"definition/pages/{page_folder}/visuals/{new_visual_folder}/")
                                new_payload = part["payload"]
                                # Offset visual position for the copy
                                if fp.endswith("/visual.json"):
                                    vj = json.loads(base64.b64decode(new_payload).decode("utf-8"))
                                    if "position" in vj:
                                        vj["position"]["x"] = vj["position"].get("x", 0) + 30
                                        vj["position"]["y"] = vj["position"].get("y", 0) + 30
                                    new_payload = base64.b64encode(json.dumps(vj).encode("utf-8")).decode("utf-8")
                                new_parts.append({"path": new_path, "payload": new_payload, "payloadType": part.get("payloadType", "InlineBase64")})
                        rw._report_definition["parts"].extend(new_parts)
                        # connect_report auto-saves on exit
                        status_lbl.value = f'<div style="color:#34c759; font-size:12px; margin-top:4px;">\u2713 Visual duplicated (offset +30px).</div>'

                _btn.description = "\u2713 Done"
                _btn.button_style = ""
            except Exception as e:
                status_lbl.value = f'<div style="color:#ff3b30; font-size:12px; margin-top:4px;">Error: {e}</div>'
                _btn.disabled = False
                _btn.description = "Duplicate"

        dup_btn.on_click(on_duplicate)
        return _w.VBox([header, dup_btn, status_lbl], layout=_w.Layout(padding="8px"))

    _rpt_fixer_cbs["Duplicate Selected"] = _rpt_duplicate_selected

    # -- Build fixer callbacks for Model Explorer actions dropdown (grouped) --
    _model_fixer_cbs = {}
    _noop = lambda **kw: None  # no-op for separator labels

    # ── Additive Actions ──
    _model_fixer_cbs["── Add Objects ──"] = _noop
    if add_calculated_calendar is not None:
        def _calendar_with_proposals(**kw):
            """Calendar table action with interactive relationship proposal."""
            report = kw.get("report", "")
            workspace = kw.get("workspace")
            scan_only = kw.get("scan_only", False)

            # In scan mode, just run the normal check
            if scan_only:
                add_calculated_calendar(report=report, workspace=workspace, scan_only=True)
                return None

            # Detect Date/DateTime columns for relationship proposals
            from sempy_labs._helper_functions import resolve_dataset_from_report, resolve_workspace_name_and_id
            from sempy_labs.tom import connect_semantic_model

            (_, workspace_id) = resolve_workspace_name_and_id(workspace)
            dataset_id, dataset_name, dataset_workspace_id, _ = resolve_dataset_from_report(
                report=report, workspace=workspace_id
            )

            date_cols = []  # [(table, column), ...]
            has_calendar = False
            with connect_semantic_model(dataset=dataset_id, readonly=True, workspace=dataset_workspace_id) as tom:
                # Check if calendar already exists
                for t in tom.model.Tables:
                    if t.DataCategory == "Time":
                        has_calendar = True
                        break
                if has_calendar:
                    print("ℹ️ Calendar table already exists. Skipping.")
                    return None

                # Find all Date/DateTime columns (skip calc table candidates)
                existing_rels = set()
                for r in tom.model.Relationships:
                    existing_rels.add((str(r.FromColumn.Table.Name), str(r.FromColumn.Name)))
                for t in tom.model.Tables:
                    if t.Name == "CalcCalendar":
                        continue
                    for c in t.Columns:
                        dt = str(c.DataType)
                        if dt in ("DateTime", "DateTimeOffset"):
                            if (t.Name, c.Name) not in existing_rels:
                                date_cols.append((t.Name, c.Name))

            if not date_cols:
                # No date columns found — just create the calendar without relationships
                add_calculated_calendar(report=report, workspace=workspace, scan_only=False)
                return None

            # Build proposal widget
            import ipywidgets as _w

            header = _w.HTML(
                value=f'<div style="font-size:13px; font-weight:600; color:{icon_accent}; '
                f'font-family:-apple-system,BlinkMacSystemFont,sans-serif; margin-bottom:4px;">'
                f'📅 Calendar Relationship Proposal — {len(date_cols)} Date column(s) found in \'{dataset_name}\'</div>'
            )
            hint = _w.HTML(
                value='<div style="font-size:11px; color:#666; font-family:-apple-system,BlinkMacSystemFont,sans-serif; margin-bottom:8px;">'
                'Edit the calendar column name, or clear the field to skip a relationship. '
                'Click <b>Create Calendar & Connect</b> to create the table and all accepted relationships.</div>'
            )

            rows = []
            text_fields = []
            for tbl, col in date_cols:
                label = _w.HTML(
                    value=f'<span style="font-size:12px; font-family:monospace; color:#333; min-width:280px;">'
                    f"'{tbl}'[{col}]</span>",
                    layout=_w.Layout(min_width="280px"),
                )
                arrow = _w.HTML(
                    value='<span style="font-size:12px; color:#999;">→ CalcCalendar[</span>',
                )
                field = _w.Text(
                    value="Date",
                    layout=_w.Layout(width="120px"),
                    continuous_update=False,
                )
                bracket = _w.HTML(value='<span style="font-size:12px; color:#999;">]</span>')
                text_fields.append((tbl, col, field))
                row = _w.HBox(
                    [label, arrow, field, bracket],
                    layout=_w.Layout(align_items="center", gap="4px", padding="2px 0"),
                )
                rows.append(row)

            create_btn = _w.Button(
                description="📅 Create Calendar & Connect",
                button_style="success",
                layout=_w.Layout(width="240px", margin="8px 0 0 0"),
            )
            status_html = _w.HTML(value="")

            def on_create(_btn):
                _btn.disabled = True
                _btn.description = "Creating…"
                status_html.value = ""
                # Collect non-empty relationships
                rels = []
                for tbl, col, field in text_fields:
                    cal_col = field.value.strip()
                    if cal_col:
                        rels.append((tbl, col, cal_col))
                try:
                    import io as _io
                    from contextlib import redirect_stdout as _redirect
                    buf = _io.StringIO()
                    with _redirect(buf):
                        add_calculated_calendar(
                            report=report, workspace=workspace, scan_only=False,
                            relationships=rels if rels else None,
                        )
                    output = buf.getvalue()
                    status_html.value = (
                        f'<pre style="font-size:11px; font-family:monospace; color:#333; '
                        f'white-space:pre-wrap; margin-top:8px;">{output}</pre>'
                    )
                    _btn.description = "✓ Done"
                    _btn.button_style = ""
                except Exception as e:
                    status_html.value = (
                        f'<div style="color:#ff3b30; font-size:12px; margin-top:4px;">Error: {e}</div>'
                    )
                    _btn.disabled = False
                    _btn.description = "📅 Create Calendar & Connect"

            create_btn.on_click(on_create)

            proposal = _w.VBox(
                [header, hint] + rows + [create_btn, status_html],
                layout=_w.Layout(padding="8px"),
            )
            return proposal

        _model_fixer_cbs["  Add Calendar Table"] = _calendar_with_proposals
    if add_last_refresh_table is not None:
        _model_fixer_cbs["  Add Last Refresh Table"] = lambda **kw: add_last_refresh_table(
            report=kw.get("report", ""), workspace=kw.get("workspace"), scan_only=kw.get("scan_only", False))
    if add_measure_table is not None:
        _model_fixer_cbs["  Add Measure Table"] = lambda **kw: add_measure_table(
            report=kw.get("report", ""), workspace=kw.get("workspace"), scan_only=kw.get("scan_only", False))
    if add_calc_group_units is not None:
        _model_fixer_cbs["  Add Units Calc Group"] = lambda **kw: add_calc_group_units(
            report=kw.get("report", ""), workspace=kw.get("workspace"), scan_only=kw.get("scan_only", False))
    if add_calc_group_time_intelligence is not None:
        _model_fixer_cbs["  Add Time Intelligence"] = lambda **kw: add_calc_group_time_intelligence(
            report=kw.get("report", ""), workspace=kw.get("workspace"), scan_only=kw.get("scan_only", False))
    if add_measures_from_columns is not None:
        def _run_measures_from_columns(**kw):
            ds = kw.get("report", "")
            ws = kw.get("workspace")
            scan = kw.get("scan_only", False)
            sel_columns = kw.get("columns", [])
            if sel_columns:
                # Extract unique table names from selected column keys
                sel_tables = set()
                for c in sel_columns:
                    # Column key is "TableName.ColumnName" or just "ColumnName"
                    if "." in c:
                        sel_tables.add(c.split(".", 1)[0])
                if sel_tables:
                    print(f"Running on selected tables: {', '.join(sorted(sel_tables))}")
                    for tbl in sorted(sel_tables):
                        add_measures_from_columns(dataset=ds, workspace=ws, target_table=tbl, scan_only=scan)
                    return
            add_measures_from_columns(dataset=ds, workspace=ws, scan_only=scan)
        _model_fixer_cbs["  Auto-Create Measures from Columns"] = lambda **kw: _run_measures_from_columns(**kw)
    if add_py_measures is not None:
        def _run_py_measures(**kw):
            ds = kw.get("report", "")
            ws = kw.get("workspace")
            scan = kw.get("scan_only", False)
            sel_measures = kw.get("measures", [])
            if sel_measures:
                print(f"Running on selected measures: {', '.join(sel_measures)}")
                add_py_measures(dataset=ds, workspace=ws, measures=sel_measures, scan_only=scan)
            else:
                add_py_measures(dataset=ds, workspace=ws, scan_only=scan)
        _model_fixer_cbs["  Add PY Measures (Y-1)"] = lambda **kw: _run_py_measures(**kw)

    def _format_all_dax(**kw):
        """Format all DAX expressions via daxformatter.com API."""
        ds = kw.get("report", "")
        ws = kw.get("workspace")
        if not ds:
            print("No model specified.")
            return
        from sempy_labs.tom import connect_semantic_model
        print(f"Formatting DAX in '{ds}'...")
        with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
            tom.format_dax()
        print(f"\u2713 All DAX expressions formatted.")

    # ── Formatting & Setup ──
    _model_fixer_cbs["── Formatting & Setup ──"] = _noop
    _model_fixer_cbs["  Format All DAX"] = lambda **kw: _format_all_dax(**kw)
    if fix_discourage_implicit_measures is not None:
        _model_fixer_cbs["  Discourage Implicit Measures"] = lambda **kw: fix_discourage_implicit_measures(
            report=kw.get("report", ""), workspace=kw.get("workspace"), scan_only=kw.get("scan_only", False))

    # Incremental Refresh setup
    _add_ir = _lazy_import("sempy_labs.semantic_model._Add_IncrementalRefresh", "add_incremental_refresh")
    if _add_ir is not None:
        def _run_ir(**kw):
            ds = kw.get("report", "")
            ws = kw.get("workspace")
            scan = kw.get("scan_only", False)
            print(f"Setting up incremental refresh for all tables in '{ds}'\u2026")
            from sempy_labs.tom import connect_semantic_model
            import Microsoft.AnalysisServices.Tabular as TOM
            with connect_semantic_model(dataset=ds, readonly=True, workspace=ws) as tom:
                for table in tom.model.Tables:
                    # Skip calculated tables
                    try:
                        if table.CalculationGroup is not None:
                            continue
                    except Exception:
                        pass
                    # Skip tables with existing refresh policy
                    try:
                        if table.RefreshPolicy is not None:
                            print(f"  '{table.Name}': already has refresh policy, skipping.")
                            continue
                    except Exception:
                        pass
                    has_date = any(col.DataType == TOM.DataType.DateTime for col in table.Columns)
                    if not has_date:
                        continue
                    _add_ir(dataset=ds, table_name=table.Name, workspace=ws, scan_only=scan)
        _model_fixer_cbs["  Add Incremental Refresh"] = lambda **kw: _run_ir(**kw)

    # Direct Lake Pre-warm Cache
    _add_cache_warming = _lazy_import("sempy_labs.semantic_model._Add_CacheWarming", "add_cache_warming")
    if _add_cache_warming is not None:
        # Monkey-patch: ensure _Add_CacheWarming has endDateTime in schedule payload
        # (the installed version may be missing this required field)
        try:
            import sempy_labs.semantic_model._Add_CacheWarming as _cw_mod
            import types, textwrap, inspect
            _orig_src = inspect.getsource(_cw_mod.add_cache_warming)
            if "endDateTime" not in _orig_src:
                # Patch: wrap the original to inject endDateTime into its _base_api calls
                _orig_base = _cw_mod._base_api
                def _patched_base(request="", method="get", payload=None, status_codes=None, **kw):
                    if payload and isinstance(payload, dict) and "configuration" in payload:
                        cfg = payload.get("configuration", {})
                        if "startDateTime" in cfg and "endDateTime" not in cfg:
                            cfg["endDateTime"] = "2099-12-31T23:59:59Z"
                    if status_codes:
                        return _orig_base(request=request, method=method, payload=payload, status_codes=status_codes, **kw)
                    return _orig_base(request=request, method=method, payload=payload, **kw)
                _cw_mod._base_api = _patched_base
        except Exception:
            pass
        _model_fixer_cbs["  Direct Lake Pre-warm Cache"] = lambda **kw: _add_cache_warming(
            dataset=kw.get("report", ""), workspace=kw.get("workspace"), scan_only=kw.get("scan_only", False)
        )

    # ── BPA Auto-Fixers ──
    _model_fixer_cbs["── BPA Fixers ──"] = _noop

    # BPA standalone fixers — also available as Model Explorer actions
    _bpa_fix_floating = _lazy_import("sempy_labs.semantic_model._Fix_FloatingPointDataType", "fix_floating_point_datatype")
    _bpa_fix_mdx = _lazy_import("sempy_labs.semantic_model._Fix_IsAvailableInMdx", "fix_isavailable_in_mdx")
    _bpa_fix_desc = _lazy_import("sempy_labs.semantic_model._Fix_MeasureDescriptions", "fix_measure_descriptions")
    _bpa_fix_date = _lazy_import("sempy_labs.semantic_model._Fix_DateColumnFormat", "fix_date_column_format")
    _bpa_fix_month = _lazy_import("sempy_labs.semantic_model._Fix_MonthColumnFormat", "fix_month_column_format")
    _bpa_fix_fmt = _lazy_import("sempy_labs.semantic_model._Fix_MeasureFormat", "fix_measure_format")
    _bpa_fix_fk = _lazy_import("sempy_labs.semantic_model._Fix_HideForeignKeys", "fix_hide_foreign_keys")
    # Phase 18 fixers
    _bpa_fix_divide = _lazy_import("sempy_labs.semantic_model._Fix_UseDivideFunction", "fix_use_divide_function")
    _bpa_fix_zero = _lazy_import("sempy_labs.semantic_model._Fix_AvoidAdding0", "fix_avoid_adding_zero")
    _bpa_fix_summarize = _lazy_import("sempy_labs.semantic_model._Fix_DoNotSummarize", "fix_do_not_summarize")
    _bpa_fix_pk = _lazy_import("sempy_labs.semantic_model._Fix_MarkPrimaryKeys", "fix_mark_primary_keys")
    _bpa_fix_trim = _lazy_import("sempy_labs.semantic_model._Fix_TrimObjectNames", "fix_trim_object_names")
    _bpa_fix_cap = _lazy_import("sempy_labs.semantic_model._Fix_CapitalizeObjectNames", "fix_capitalize_object_names")
    _bpa_fix_pct = _lazy_import("sempy_labs.semantic_model._Fix_PercentageFormat", "fix_percentage_format")
    _bpa_fix_whole = _lazy_import("sempy_labs.semantic_model._Fix_WholeNumberFormat", "fix_whole_number_format")
    _bpa_fix_flag = _lazy_import("sempy_labs.semantic_model._Fix_FlagColumnFormat", "fix_flag_column_format")
    _bpa_fix_mdx_true = _lazy_import("sempy_labs.semantic_model._Fix_IsAvailableInMdxTrue", "fix_isavailable_in_mdx_true")
    _bpa_fix_sort_month = _lazy_import("sempy_labs.semantic_model._Fix_SortMonthColumn", "fix_sort_month_column")
    _bpa_fix_data_cat = _lazy_import("sempy_labs.semantic_model._Fix_DataCategory", "fix_data_category")

    def _sm_action(fn):
        return lambda **kw: fn(dataset=kw.get("report", ""), workspace=kw.get("workspace"), scan_only=kw.get("scan_only", False))

    if _bpa_fix_floating is not None:
        _model_fixer_cbs["  Fix Floating Point Types"] = _sm_action(_bpa_fix_floating)
    if _bpa_fix_mdx is not None:
        _model_fixer_cbs["  Fix IsAvailableInMDX (False)"] = _sm_action(_bpa_fix_mdx)
    if _bpa_fix_desc is not None:
        _model_fixer_cbs["  Fix Measure Descriptions"] = _sm_action(_bpa_fix_desc)
    if _bpa_fix_fk is not None:
        _model_fixer_cbs["  Hide Foreign Keys"] = _sm_action(_bpa_fix_fk)
    if _bpa_fix_divide is not None:
        _model_fixer_cbs["  Fix DIVIDE Function"] = _sm_action(_bpa_fix_divide)
    if _bpa_fix_zero is not None:
        _model_fixer_cbs["  Fix Avoid Adding 0"] = _sm_action(_bpa_fix_zero)
    if _bpa_fix_summarize is not None:
        _model_fixer_cbs["  Fix Do Not Summarize"] = _sm_action(_bpa_fix_summarize)
    if _bpa_fix_pk is not None:
        _model_fixer_cbs["  Mark Primary Keys"] = _sm_action(_bpa_fix_pk)
    if _bpa_fix_trim is not None:
        _model_fixer_cbs["  Trim Object Names"] = _sm_action(_bpa_fix_trim)
    if _bpa_fix_cap is not None:
        _model_fixer_cbs["  Capitalize Object Names"] = _sm_action(_bpa_fix_cap)
    if _bpa_fix_pct is not None:
        _model_fixer_cbs["  Fix Percentage Format"] = _sm_action(_bpa_fix_pct)
    if _bpa_fix_whole is not None:
        _model_fixer_cbs["  Fix Whole Number Format"] = _sm_action(_bpa_fix_whole)
    if _bpa_fix_flag is not None:
        _model_fixer_cbs["  Fix Flag Column Format"] = _sm_action(_bpa_fix_flag)
    if _bpa_fix_mdx_true is not None:
        _model_fixer_cbs["  Fix IsAvailableInMDX (True)"] = _sm_action(_bpa_fix_mdx_true)
    if _bpa_fix_sort_month is not None:
        _model_fixer_cbs["  Fix Sort Month Column"] = _sm_action(_bpa_fix_sort_month)
    if _bpa_fix_data_cat is not None:
        _model_fixer_cbs["  Fix Data Category"] = _sm_action(_bpa_fix_data_cat)

    # ── AI ──
    _model_fixer_cbs["── AI ──"] = _noop
    if add_prep_for_ai is not None:
        _model_fixer_cbs["  Auto-Generate Prep for AI"] = lambda **kw: add_prep_for_ai(
            dataset=kw.get("report", ""), workspace=kw.get("workspace"), scan_only=kw.get("scan_only", False)
        )

    # ── CRUD: Create & Delete ──
    _model_fixer_cbs["── Create & Delete ──"] = _noop

    def _crud_delete_selected(**kw):
        """Delete selected objects from the semantic model with dependency check."""
        import ipywidgets as _w
        ds = kw.get("report", "")
        ws = kw.get("workspace")
        if not ds:
            print("[!] No model loaded.")
            return None

        # Get selected items from kwargs (passed by on_run_action)
        sel_keys = kw.get("selected_keys", [])
        if not sel_keys:
            print("[!] Select one or more objects in the tree first.")
            return None

        # Parse selected keys into object references
        objects_to_delete = []  # [(type, model, table, name), ...]
        for key in sel_keys:
            if ":" not in key:
                continue
            obj_type, rest = key.split(":", 1)
            parts = rest.split("\x1f")
            if obj_type == "measure" and len(parts) >= 3:
                objects_to_delete.append(("measure", parts[0], parts[1], parts[2]))
            elif obj_type == "column" and len(parts) >= 3:
                objects_to_delete.append(("column", parts[0], parts[1], parts[2]))
            elif obj_type == "table" and len(parts) >= 2:
                objects_to_delete.append(("table", parts[0], parts[1], None))
            elif obj_type == "hierarchy" and len(parts) >= 3:
                objects_to_delete.append(("hierarchy", parts[0], parts[1], parts[2]))
            elif obj_type == "calc_item" and len(parts) >= 3:
                objects_to_delete.append(("calc_item", parts[0], parts[1], parts[2]))
            elif obj_type == "relationship" and len(parts) >= 2:
                objects_to_delete.append(("relationship", parts[0], parts[1], None))

        if not objects_to_delete:
            print("[!] No deletable objects selected. Select measures, columns, tables, hierarchies, or calc items.")
            return None

        # Build confirmation widget
        header = _w.HTML(
            value=f'<div style="font-size:13px; font-weight:600; color:#ff3b30; '
            f'font-family:-apple-system,BlinkMacSystemFont,sans-serif; margin-bottom:4px;">'
            f'🗑 Delete {len(objects_to_delete)} object(s) from \'{ds}\'</div>'
        )

        items_html = ""
        for obj_type, model, table, name in objects_to_delete:
            if obj_type == "table":
                items_html += f'<div style="font-size:12px; font-family:monospace; padding:2px 0;">• Table: \'{table}\'</div>'
            elif obj_type == "relationship":
                items_html += f'<div style="font-size:12px; font-family:monospace; padding:2px 0;">• Relationship: {table}</div>'
            else:
                items_html += f'<div style="font-size:12px; font-family:monospace; padding:2px 0;">• {obj_type.title()}: \'{table}\'[{name}]</div>'
        items_list = _w.HTML(value=items_html)

        warning = _w.HTML(
            value='<div style="font-size:11px; color:#856404; padding:4px 8px; '
            'background:#fff3cd; border-radius:4px; margin:4px 0;">'
            '⚠️ <b>XMLA write</b> — This will permanently delete the selected objects. '
            'Dependent measures may break. This cannot be undone.</div>'
        )

        delete_btn = _w.Button(description="🗑 Confirm Delete", button_style="danger", layout=_w.Layout(width="180px", margin="8px 0 0 0"))
        status_html = _w.HTML(value="")

        def on_delete(_btn):
            _btn.disabled = True
            _btn.description = "Deleting…"
            try:
                import io as _io
                from contextlib import redirect_stdout as _redirect
                from sempy_labs.tom import connect_semantic_model
                buf = _io.StringIO()
                with _redirect(buf):
                    with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
                        deleted = 0
                        for obj_type, model, table, name in objects_to_delete:
                            try:
                                if obj_type == "table":
                                    obj = tom.model.Tables[table]
                                elif obj_type == "measure":
                                    obj = tom.model.Tables[table].Measures[name]
                                elif obj_type == "column":
                                    obj = tom.model.Tables[table].Columns[name]
                                elif obj_type == "hierarchy":
                                    obj = tom.model.Tables[table].Hierarchies[name]
                                elif obj_type == "calc_item":
                                    # Calc items are in CalculationGroup.CalculationItems
                                    obj = tom.model.Tables[table].CalculationGroup.CalculationItems[name]
                                elif obj_type == "relationship":
                                    obj = tom.model.Relationships[table]
                                else:
                                    continue
                                tom.remove_object(obj)
                                deleted += 1
                                label = f"'{table}'" if obj_type == "table" else f"'{table}'[{name}]"
                                print(f"[OK] Deleted {obj_type}: {label}")
                            except Exception as e:
                                label = f"'{table}'" if obj_type == "table" else f"'{table}'[{name}]"
                                print(f"[ERR] Failed to delete {label}: {e}")
                        if deleted > 0:
                            tom.model.SaveChanges()
                            print(f"\n[OK] {deleted} object(s) deleted and saved.")
                output = buf.getvalue()
                status_html.value = f'<pre style="font-size:11px; font-family:monospace; color:#333; white-space:pre-wrap; margin-top:8px;">{output}</pre>'
                _btn.description = "✓ Deleted"
                _btn.button_style = ""
            except Exception as e:
                status_html.value = f'<div style="color:#ff3b30; font-size:12px; margin-top:4px;">Error: {e}</div>'
                _btn.disabled = False
                _btn.description = "🗑 Confirm Delete"

        delete_btn.on_click(on_delete)
        return _w.VBox([header, items_list, warning, delete_btn, status_html], layout=_w.Layout(padding="8px"))

    _model_fixer_cbs["  🗑 Delete Selected"] = _crud_delete_selected

    def _crud_create_measure(**kw):
        """Interactive form to create a new measure."""
        import ipywidgets as _w
        ds = kw.get("report", "")
        ws = kw.get("workspace")
        if not ds:
            print("[!] No model loaded.")
            return None

        # Get table list from model_data (passed by on_run_action)
        tables = []
        md = kw.get("model_data", {})
        if md and "models" in md:
            for mname, mdata in md["models"].items():
                for tname in mdata.get("tables", {}):
                    tables.append(tname)

        if not tables:
            print("[!] No tables found. Load a model first.")
            return None

        header = _w.HTML(
            value=f'<div style="font-size:13px; font-weight:600; color:{icon_accent}; '
            f'font-family:-apple-system,BlinkMacSystemFont,sans-serif; margin-bottom:4px;">'
            f'📐 Create Measure in \'{ds}\'</div>'
        )

        tbl_dd = _w.Dropdown(options=sorted(tables), description="Table:", layout=_w.Layout(width="350px"))
        name_input = _w.Text(value="", placeholder="Measure name", description="Name:", layout=_w.Layout(width="350px"))
        dax_input = _w.Textarea(value="", placeholder="DAX expression, e.g. SUM('Table'[Column])", description="DAX:", layout=_w.Layout(width="500px", height="80px"))
        fmt_input = _w.Text(value="", placeholder='e.g. #,0 or 0.0%', description="Format:", layout=_w.Layout(width="350px"))
        folder_input = _w.Text(value="", placeholder="Display folder (optional)", description="Folder:", layout=_w.Layout(width="350px"))

        create_btn = _w.Button(description="Create Measure", button_style="success", layout=_w.Layout(width="180px", margin="8px 0 0 0"))
        status_html = _w.HTML(value="")

        def on_create(_btn):
            mname = name_input.value.strip()
            dax = dax_input.value.strip()
            if not mname or not dax:
                status_html.value = '<div style="color:#ff3b30; font-size:12px;">Name and DAX expression are required.</div>'
                return
            _btn.disabled = True
            _btn.description = "Creating…"
            try:
                from sempy_labs.tom import connect_semantic_model
                with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
                    tom.add_measure(
                        table_name=tbl_dd.value,
                        measure_name=mname,
                        expression=dax,
                        format_string=fmt_input.value.strip() or None,
                        display_folder=folder_input.value.strip() or None,
                    )
                    tom.model.SaveChanges()
                status_html.value = f'<div style="color:#34c759; font-size:12px; margin-top:4px;">\u2713 Measure [{mname}] created in \'{tbl_dd.value}\'.</div>'
                _btn.description = "✓ Created"
                _btn.button_style = ""
            except Exception as e:
                status_html.value = f'<div style="color:#ff3b30; font-size:12px; margin-top:4px;">Error: {e}</div>'
                _btn.disabled = False
                _btn.description = "Create Measure"

        create_btn.on_click(on_create)
        return _w.VBox([header, tbl_dd, name_input, dax_input, fmt_input, folder_input, create_btn, status_html], layout=_w.Layout(padding="8px"))

    _model_fixer_cbs["  📐 Create Measure"] = _crud_create_measure

    def _crud_create_calc_column(**kw):
        """Interactive form to create a calculated column."""
        import ipywidgets as _w
        ds = kw.get("report", "")
        ws = kw.get("workspace")
        if not ds:
            print("[!] No model loaded.")
            return None

        tables = []
        md = kw.get("model_data", {})
        if md and "models" in md:
            for mname, mdata in md["models"].items():
                for tname in mdata.get("tables", {}):
                    tables.append(tname)
        if not tables:
            print("[!] No tables found.")
            return None

        header = _w.HTML(
            value=f'<div style="font-size:13px; font-weight:600; color:{icon_accent}; '
            f'font-family:-apple-system,BlinkMacSystemFont,sans-serif; margin-bottom:4px;">'
            f'📊 Create Calculated Column in \'{ds}\'</div>'
        )

        tbl_dd = _w.Dropdown(options=sorted(tables), description="Table:", layout=_w.Layout(width="350px"))
        name_input = _w.Text(value="", placeholder="Column name", description="Name:", layout=_w.Layout(width="350px"))
        dax_input = _w.Textarea(value="", placeholder="DAX expression, e.g. [Column1] * [Column2]", description="DAX:", layout=_w.Layout(width="500px", height="80px"))
        dtype_dd = _w.Dropdown(options=["String", "Int64", "Double", "DateTime", "Boolean", "Decimal"], value="String", description="Type:", layout=_w.Layout(width="350px"))
        fmt_input = _w.Text(value="", placeholder='e.g. 0 or yyyy-MM-dd', description="Format:", layout=_w.Layout(width="350px"))
        folder_input = _w.Text(value="", placeholder="Display folder (optional)", description="Folder:", layout=_w.Layout(width="350px"))

        create_btn = _w.Button(description="Create Column", button_style="success", layout=_w.Layout(width="180px", margin="8px 0 0 0"))
        refresh_hint = _w.HTML(
            value='<div style="font-size:11px; color:#666; margin-top:4px;">'
            'ℹ️ After creation, the model needs a <b>refresh/recalc</b> for the column values to populate.</div>'
        )
        status_html = _w.HTML(value="")

        def on_create(_btn):
            cname = name_input.value.strip()
            dax = dax_input.value.strip()
            if not cname or not dax:
                status_html.value = '<div style="color:#ff3b30; font-size:12px;">Name and DAX expression are required.</div>'
                return
            _btn.disabled = True
            _btn.description = "Creating…"
            try:
                from sempy_labs.tom import connect_semantic_model
                with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
                    tom.add_calculated_column(
                        table_name=tbl_dd.value,
                        column_name=cname,
                        expression=dax,
                        data_type=dtype_dd.value,
                        format_string=fmt_input.value.strip() or None,
                        display_folder=folder_input.value.strip() or None,
                    )
                    tom.model.SaveChanges()
                status_html.value = f'<div style="color:#34c759; font-size:12px; margin-top:4px;">\u2713 Column [{cname}] created in \'{tbl_dd.value}\'. Refresh the model to populate values.</div>'
                _btn.description = "✓ Created"
                _btn.button_style = ""
            except Exception as e:
                status_html.value = f'<div style="color:#ff3b30; font-size:12px; margin-top:4px;">Error: {e}</div>'
                _btn.disabled = False
                _btn.description = "Create Column"

        create_btn.on_click(on_create)
        return _w.VBox([header, tbl_dd, name_input, dax_input, dtype_dd, fmt_input, folder_input, create_btn, refresh_hint, status_html], layout=_w.Layout(padding="8px"))

    _model_fixer_cbs["  📊 Create Calculated Column"] = _crud_create_calc_column

    def _crud_create_calc_table(**kw):
        """Interactive form to create a calculated table."""
        import ipywidgets as _w
        ds = kw.get("report", "")
        ws = kw.get("workspace")
        if not ds:
            print("[!] No model loaded.")
            return None

        header = _w.HTML(
            value=f'<div style="font-size:13px; font-weight:600; color:{icon_accent}; '
            f'font-family:-apple-system,BlinkMacSystemFont,sans-serif; margin-bottom:4px;">'
            f'📋 Create Calculated Table in \'{ds}\'</div>'
        )

        name_input = _w.Text(value="", placeholder="Table name", description="Name:", layout=_w.Layout(width="350px"))
        dax_input = _w.Textarea(value="", placeholder='DAX expression, e.g. SELECTCOLUMNS(\'Table\', "Col", [Column])\nor CALENDAR(DATE(2020,1,1), DATE(2030,12,31))', description="DAX:", layout=_w.Layout(width="500px", height="100px"))
        hidden_cb = _w.Checkbox(value=False, description="Hidden", indent=False, layout=_w.Layout(width="150px"))

        create_btn = _w.Button(description="Create Table", button_style="success", layout=_w.Layout(width="180px", margin="8px 0 0 0"))
        refresh_hint = _w.HTML(
            value='<div style="font-size:11px; color:#666; margin-top:4px;">'
            'ℹ️ After creation, a <b>refresh/recalc</b> is needed to populate the table and generate columns. '
            'You can trigger this from the Fabric workspace or via semantic-link-labs.</div>'
        )
        status_html = _w.HTML(value="")

        def on_create(_btn):
            tname = name_input.value.strip()
            dax = dax_input.value.strip()
            if not tname or not dax:
                status_html.value = '<div style="color:#ff3b30; font-size:12px;">Name and DAX expression are required.</div>'
                return
            _btn.disabled = True
            _btn.description = "Creating…"
            try:
                from sempy_labs.tom import connect_semantic_model
                with connect_semantic_model(dataset=ds, readonly=False, workspace=ws) as tom:
                    tom.add_calculated_table(
                        name=tname,
                        expression=dax,
                        hidden=hidden_cb.value,
                    )
                    tom.model.SaveChanges()
                status_html.value = (
                    f'<div style="color:#34c759; font-size:12px; margin-top:4px;">'
                    f'\u2713 Calculated table \'{tname}\' created. '
                    f'Refresh the model to populate columns and data.</div>'
                )
                _btn.description = "✓ Created"
                _btn.button_style = ""
            except Exception as e:
                status_html.value = f'<div style="color:#ff3b30; font-size:12px; margin-top:4px;">Error: {e}</div>'
                _btn.disabled = False
                _btn.description = "Create Table"

        create_btn.on_click(on_create)
        return _w.VBox([header, name_input, dax_input, hidden_cb, create_btn, refresh_hint, status_html], layout=_w.Layout(padding="8px"))

    _model_fixer_cbs["  📋 Create Calculated Table"] = _crud_create_calc_table

    # -- Clone callbacks (for action dropdowns — reuse shared impl) --
    def _clone_report(**kw):
        """Clone the report (appends '_copy' to the name)."""
        rpt = kw.get("report", "")
        ws = kw.get("workspace")
        if not rpt:
            print("No report specified.")
            return
        cloned_name = f"{rpt}_copy"
        print(f"Cloning report '{rpt}' \u2192 '{cloned_name}'\u2026")
        from sempy_labs.report._report_functions import clone_report as _clone_rpt
        _clone_rpt(report=rpt, cloned_report=cloned_name, workspace=ws)
        print(f"\u2713 Report cloned as '{cloned_name}'.")

    # Clone Report removed from dropdown — available as top-level button

    def _clone_semantic_model(**kw):
        """Clone the semantic model via shared impl."""
        ds = kw.get("report", "")
        ws = kw.get("workspace")
        if not ds:
            print("No model specified.")
            return
        print(f"Cloning model '{ds}' \u2192 '{ds}_copy'\u2026")
        _clone_semantic_model_impl(ds, ws)
        print(f"\u2713 Semantic model cloned as '{ds}_copy'.")

    # Clone Model removed from dropdown — available as top-level button

    # -- Build tab panels (show/hide via layout.display) --
    tab_panels = [fix_all_content]
    _rpt_format_container = None

    if model_explorer_tab is not None:
        model_result = model_explorer_tab(
            workspace_input=workspace_input, report_input=report_input,
            fixer_callbacks=_model_fixer_cbs,
        )
        if isinstance(model_result, tuple):
            model_content, _model_load_fn = model_result
        else:
            model_content = model_result
        tab_panels.append(model_content)

    if report_explorer_tab is not None:
        def _navigate_to_model(obj_name, table_name, obj_type):
            """Switch to Model Explorer tab (callback from Report Explorer)."""
            model_tab_label = "\U0001F4C4 Model"
            if model_tab_label in _tab_options:
                tab_selector.value = model_tab_label

        rpt_result = report_explorer_tab(
            workspace_input=workspace_input, report_input=report_input,
            fixer_callbacks=_rpt_fixer_cbs,
            navigate_to_sm=_navigate_to_model if model_explorer_tab is not None else None,
        )
        if isinstance(rpt_result, tuple):
            rpt_content, _rpt_load_fn = rpt_result
        else:
            rpt_content = rpt_result
        tab_panels.append(rpt_content)
        # Extract format container for placement below the main UI
        _rpt_format_container = getattr(rpt_content, '_format_container', None)

    if _fixer_visible:
        tab_panels.append(fixer_content)

    # -- Lazy tab loading: extra tabs are built on first click --
    _lazy_builders = {}  # tab label -> builder function (removed after first build)
    _container_ref = [None]  # mutable ref set after container creation; used by Show Native to close UI

    def _make_lazy_specs():
        """Build (label, builder) list for all extra tabs."""
        specs = []
        if perspective_editor_tab is not None:
            specs.append(("\U0001F441 Perspectives", lambda: perspective_editor_tab(
                workspace_input=workspace_input, report_input=report_input
            )))
        specs.extend([
            ("\U0001F310 Translations", lambda: _translations_tab(
                workspace_input=workspace_input, report_input=report_input
            )),
            ("\U0001F4BE Memory Analyzer", lambda: _vertipaq_tab(
                workspace_input=workspace_input, report_input=report_input
            )),
            ("\U0001F4CB Model BPA", lambda: _bpa_tab(
                workspace_input=workspace_input, report_input=report_input, container_ref=_container_ref
            )),
            ("\U0001F4C4 Report BPA", lambda: _report_bpa_tab(
                workspace_input=workspace_input, report_input=report_input, container_ref=_container_ref
            )),
            ("\U0001F4D0 Delta Analyzer", lambda: _delta_analyzer_tab(
                workspace_input=workspace_input, report_input=report_input, container_ref=_container_ref
            )),
            ("\u270F\uFE0F Prototype", lambda: _prototype_tab(
                workspace_input=workspace_input, report_input=report_input
            )),
            ("\U0001F5FA Model Diagram", lambda: _diagram_tab(
                workspace_input=workspace_input, report_input=report_input
            )),
        ])
        return specs

    if all_tabs:
        for label, builder in _make_lazy_specs():
            placeholder = widgets.VBox(layout=widgets.Layout(display="none"))
            _lazy_builders[label] = builder
            tab_panels.append(placeholder)

    # About tab
    about_content = widgets.HTML(
        value=(
            f'<div style="font-family:-apple-system,BlinkMacSystemFont,sans-serif; padding:24px; max-width:600px;">'
            f'<div style="font-size:28px; font-weight:700; color:#FF9500; margin-bottom:4px;">Power BI Fixer</div>'
            f'<div style="font-size:14px; color:#666; margin-bottom:24px;">Version {__version__}</div>'
            # --- Author: Alexander Korn ---
            f'<div style="margin-bottom:20px; padding:16px; background:#fafafa; border-radius:8px; border:1px solid #e0e0e0;">'
            f'<div style="font-size:20px; font-weight:600; color:#333;">Alexander Korn</div>'
            f'<div style="font-size:13px; color:#888; margin-top:2px;">PBI Fixer UI, all Report & Semantic Model Fixers</div>'
            f'<div style="font-size:13px; margin-top:8px;">'
            f'<a href="https://www.linkedin.com/in/alexanderkorn/" target="_blank" style="color:#0A66C2; text-decoration:none; margin-right:12px;">LinkedIn</a>'
            f'<a href="https://github.com/KornAlexander" target="_blank" style="color:#333; text-decoration:none; margin-right:12px;">GitHub</a>'
            f'<a href="https://actionablereporting.com" target="_blank" style="color:#FF9500; text-decoration:none;">actionablereporting.com</a>'
            f'</div>'
            f'<div style="font-size:13px; color:#888; margin-top:4px;">Transform data into actionable insights</div>'
            f'</div>'
            # --- Credits: Michael Kovalsky ---
            f'<div style="margin-bottom:20px; padding:16px; background:#fafafa; border-radius:8px; border:1px solid #e0e0e0;">'
            f'<div style="font-size:16px; font-weight:600; margin-bottom:8px;">\U0001F3C6 Built on Semantic Link Labs</div>'
            f'<div style="font-size:13px; color:#555; line-height:1.8;">'
            f'The core library powering this tool was created by <b>Michael Kovalsky</b> '
            f'(<a href="https://github.com/m-kovalsky" target="_blank" style="color:#FF9500;">m-kovalsky</a>):<br>'
            f'\u2022 <b>TOM / connect_semantic_model</b> \u2014 .NET interop for semantic models<br>'
            f'\u2022 <b>Model BPA</b> \u2014 Best Practice Analyzer engine &amp; rules<br>'
            f'\u2022 <b>Report BPA</b> \u2014 Report-level Best Practice Analyzer<br>'
            f'\u2022 <b>ReportWrapper</b> \u2014 PBIR report definition read/write<br>'
            f'\u2022 <b>Vertipaq Analyzer</b> \u2014 storage &amp; memory analysis<br>'
            f'\u2022 <b>Perspective Editor</b> \u2014 original logic (UI adapted by A. Korn)<br>'
            f'\u2022 <b>DAX utilities, Direct Lake, model dependencies</b> &amp; more'
            f'</div>'
            f'<div style="font-size:13px; color:#888; margin-top:8px;">'
            f'Official repo: <a href="https://github.com/microsoft/semantic-link-labs" target="_blank" style="color:#FF9500;">microsoft/semantic-link-labs</a>'
            f'</div>'
            f'</div>'
            # --- Source ---
            f'<div style="margin-bottom:20px; padding:16px; background:#fafafa; border-radius:8px; border:1px solid #e0e0e0;">'
            f'<div style="font-size:16px; font-weight:600; margin-bottom:8px;">\U0001F4E6 Source</div>'
            f'<div style="font-size:13px;">'
            f'<a href="https://github.com/KornAlexander/pbi_fixer" target="_blank" style="color:#FF9500;">github.com/KornAlexander/pbi_fixer</a><br>'
            f'<a href="https://github.com/KornAlexander/semantic-link-labs" target="_blank" style="color:#FF9500;">github.com/KornAlexander/semantic-link-labs</a> (fork)'
            f'</div>'
            f'</div>'
            # --- Built with ---
            f'<div style="padding:16px; background:#fafafa; border-radius:8px; border:1px solid #e0e0e0;">'
            f'<div style="font-size:16px; font-weight:600; margin-bottom:8px;">\U0001F6E0\ufe0f Built with</div>'
            f'<div style="font-size:13px; color:#555; line-height:1.8;">'
            f'\u2022 <b>ipywidgets</b> \u2014 interactive UI in Fabric Notebooks<br>'
            f'\u2022 <b>powerbiclient</b> \u2014 live report preview embed<br>'
            f'\u2022 <b>SynapseML</b> \u2014 Azure AI Translator for auto-translations<br>'
            f'\u2022 <b>DAX Formatter</b> by SQLBI \u2014 '
            f'<a href="https://www.daxformatter.com/" target="_blank" style="color:#FF9500;">daxformatter.com</a> '
            f'(<a href="https://www.sqlbi.com/blog/marco/2014/02/24/how-to-pass-a-dax-query-to-dax-formatter/" target="_blank" style="color:#FF9500;">API docs</a>)<br>'
            f'\u2022 <b>Prep for AI</b> API discovery by <b>Lukasz Obst</b> '
            f'(<a href="https://github.com/lobst4r" target="_blank" style="color:#FF9500;">lobst4r</a>)'
            f'</div>'
            f'</div>'
            f'</div>'
        ),
        layout=widgets.Layout(padding="12px"),
    )
    tab_panels.append(about_content)

    _loading_html = (
        f'<div style="padding:40px; text-align:center; color:{gray_color}; font-size:13px; '
        f'font-family:-apple-system,BlinkMacSystemFont,sans-serif; font-style:italic;">'
        f'Loading\u2026</div>'
    )

    def _switch_tab(change=None):
        label = tab_selector.value
        idx = _tab_options.index(label)
        # Lazy-build on first click
        if label in _lazy_builders:
            panel = tab_panels[idx]
            panel.children = [widgets.HTML(value=_loading_html)]
            panel.layout.display = ""
            # Hide all others
            for i, p in enumerate(tab_panels):
                if i != idx:
                    p.layout.display = "none"
            try:
                content = _lazy_builders.pop(label)()
                panel.children = [content]
            except Exception as e:
                panel.children = [widgets.HTML(
                    value=f'<div style="padding:20px; color:#ff3b30; font-size:13px;">Failed to load: {e}</div>'
                )]
            return
        for i, p in enumerate(tab_panels):
            p.layout.display = "" if i == idx else "none"

    tab_selector.observe(_switch_tab, names="value")
    _switch_tab()  # set initial visibility

    # "Show All Tabs" button — dynamically adds extra tabs without restart
    _show_all_btn = widgets.Button(
        description="\u2795 Show All Tabs",
        layout=widgets.Layout(width="140px", height="34px", display="" if not all_tabs else "none"),
        button_style="",
    )

    def _on_show_all(_):
        _show_all_btn.disabled = True
        _show_all_btn.description = "Loading\u2026"
        # Insert extra tabs before About (last tab)
        about_idx = len(_tab_options) - 1  # About is always last
        for label, builder in _make_lazy_specs():
            if label in _tab_options:
                continue  # skip if somehow already present
            _tab_options.insert(about_idx, label)
            placeholder = widgets.VBox(layout=widgets.Layout(display="none"))
            _lazy_builders[label] = builder
            tab_panels.insert(about_idx, placeholder)
            about_idx += 1
        # Rebuild ToggleButtons options
        current = tab_selector.value
        tab_selector.unobserve(_switch_tab, names="value")
        tab_selector.options = list(_tab_options)
        tab_selector.value = current
        tab_selector.observe(_switch_tab, names="value")
        # Rebuild container children
        container.children = (
            [header, shared_inputs_box, download_row, tab_bar] + tab_panels + [version_footer]
        )
        _show_all_btn.layout.display = "none"

    _show_all_btn.on_click(_on_show_all)

    tab_bar = widgets.HBox(
        [tab_selector, _show_all_btn],
        layout=widgets.Layout(align_items="center", gap="12px", margin="0 0 12px 0"),
    )

    # Collect extra widgets to place below the main container
    _below_widgets = []
    if _rpt_format_container is not None:
        _below_widgets.append(_rpt_format_container)

    container = widgets.VBox(
        [header, shared_inputs_box, download_row, tab_bar] + tab_panels + [version_footer],
        layout=widgets.Layout(
            width="100%",
            padding="20px",
            border=f"1px solid {border_color}",
            border_radius="12px",
        ),
    )
    _container_ref[0] = container

    if _below_widgets:
        return widgets.VBox([container] + _below_widgets)
    return container


def pbi_fixer_v2(
    all_tabs: bool = False,
    workspace: Optional[str | UUID] = None,
    report: Optional[str | UUID] = None,
    page_name: Optional[str] = None,
    show_fixer_tab: bool = False,
):
    """
    Launches PBI Fixer with ``all_tabs`` as the first parameter for convenience.

    Parameters
    ----------
    all_tabs : bool, default=False
        If True, shows all tabs. If False (default), only Fix All, SM, Report, About.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    report : str | uuid.UUID, default=None
        Name(s) or ID(s) of the report(s). Supports comma-separated values.
    page_name : str, default=None
        The display name of the page.
    show_fixer_tab : bool, default=False
        If True, shows the legacy Fixer tab.

    Examples
    --------
    >>> pbi_fixer_v2()                              # Minimal tabs
    >>> pbi_fixer_v2(True)                           # All tabs
    >>> pbi_fixer_v2(True, "My Workspace", "Report") # All tabs, pre-filled
    """
    return pbi_fixer(
        workspace=workspace,
        report=report,
        page_name=page_name,
        show_fixer_tab=show_fixer_tab,
        all_tabs=all_tabs,
    )


# Sample usage (must be last line of notebook cell so Jupyter renders the returned widget):
# pbi_fixer_v2()
# pbi_fixer_v2(True)
# pbi_fixer_v2(True, "Your Workspace Name")
# pbi_fixer_v2(True, "Your Workspace Name", "My Report")
