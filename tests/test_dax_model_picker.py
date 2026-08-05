from pathlib import Path


SOURCE_PATH = (
    Path(__file__).parents[1]
    / "src"
    / "sempy_labs"
    / "semantic_model"
    / "_dax_perf.py"
)


def _source() -> str:
    return SOURCE_PATH.read_text(encoding="utf-8")


def test_dax_model_picker_source_has_valid_python_syntax():
    compile(_source(), str(SOURCE_PATH), "exec")


def test_no_dataset_uses_searchable_theme_aware_pickers():
    source = _source()
    picker_start = source.index("// ---------- Model picker (first screen")
    picker_end = source.index("// ---------- Query options + editor ----------", picker_start)
    picker_source = source[picker_start:picker_end]

    assert 'pickerTitle.textContent = "Connect to a semantic model"' in source
    assert 'pickerTop.className = "dtx-picker-top"' in source
    assert 'pickerReloadBtn.innerHTML = REFRESH_SVG + "Reload"' in source
    assert '"SEARCH_SELECT_CSS", _FALLBACK_SEARCH_SELECT_CSS' in source
    assert '"SEARCH_SELECT_JS", _FALLBACK_SEARCH_SELECT_JS' in source
    assert "function createSearchSelect(config)" in source
    assert '"Filter workspaces…"' in source
    assert '"Filter semantic models…"' in source
    assert 'wsPicker.picker.setOptions(' in source
    assert 'dsPicker.picker.setOptions(' in source
    assert 'document.createElement("select")' not in picker_source
    assert 'body.style.display = show ? "none" : ""' in source


def test_dax_performance_uses_the_shared_speedometer_icon():
    source = _source()
    ui_source = SOURCE_PATH.parents[1].joinpath("_ui_components.py").read_text(
        encoding="utf-8"
    )

    assert '"dax_performance": (' in ui_source
    assert '_UI_ICONS.get(' in source
    assert '"dax_performance", _FALLBACK_DAX_PERFORMANCE_ICON' in source
    assert 'toolIcon.className = "dtx-tool-icon"' in source
    assert "toolIcon.innerHTML = DAX_PERFORMANCE_SVG" in source
    assert "    background: var(--ui-bg-secondary);\n    color: var(--ui-accent);" in source
    assert ".dtx .dtx-tool-icon svg path:nth-of-type(2) {{ stroke: var(--ui-accent); }}" in source
    assert ".dtx .dtx-tool-icon svg circle {{ fill: var(--ui-accent); }}" in source


def test_connect_is_the_explicit_model_activation_action():
    source = _source()
    picker_start = source.index("// ---------- Model picker (first screen")
    picker_end = source.index("// ---------- Query options + editor ----------", picker_start)
    picker_source = source[picker_start:picker_end]

    assert 'pickerBtn.textContent = "Connect"' in picker_source
    assert 'model.set("select_dataset_trigger"' in picker_source
    assert 'pickerBtn.addEventListener("click"' in picker_source
    assert "connectingToModel = true;" in picker_source
    assert "modelViewVisible = true;" in picker_source
    assert "pickerOpen = false;" in picker_source
    assert "const show = pickerOpen || (!chosen && !connectingToModel);" in picker_source
    assert 'model.set("metadata_loading", true);' in picker_source


def test_connect_does_not_reopen_picker_for_transient_active_model_state():
    source = _source()
    active_listener_start = source.index('model.on("change:active_dataset_id"')
    listener_start = source.index('model.on("change:picker_loading"')
    active_listener = source[active_listener_start:listener_start]
    listener_end = source.index("    applyTheme();", listener_start)
    listener = source[listener_start:listener_end]
    activation_start = source.index("def _activate_selected_dataset()")
    activation_end = source.index("def _on_select_dataset", activation_start)
    activation = source[activation_start:activation_end]

    assert 'const activationError = String(model.get("error_message") || "").trim();' in listener
    assert "selected !== active && activationError" in listener
    assert 'if (model.get("dataset_chosen") === true) connectingToModel = false;' in active_listener
    error_assignment = activation.index(
        'widget.error_message = f"Failed to load semantic model: {exc}"'
    )
    loading_finished = activation.index("widget.picker_loading = False", error_assignment)
    assert error_assignment < loading_finished
    assert activation.index("widget.active_dataset_id =") < activation.index(
        "widget.dataset_chosen = True"
    )


def test_model_metadata_loads_before_main_screen_is_enabled():
    source = _source()
    collector_start = source.index("def _collect_model_metadata")
    collector_end = source.index("def _list_reports_for_capture", collector_start)
    collector_source = source[collector_start:collector_end]
    activation_start = source.index("def _activate_selected_dataset()")
    activation_end = source.index("def _on_select_dataset", activation_start)
    activation_source = source[activation_start:activation_end]

    assert "with connect_semantic_model(" in collector_source
    assert "return _build_model_tree(tom), _build_model_roles(tom)" in collector_source
    assert collector_source.count("return [], []") == 2
    assert activation_source.index("_collect_model_metadata(") < activation_source.index(
        "widget.dataset_chosen = True"
    )


def test_no_dataset_workspace_loading_uses_frontend_trigger_after_render():
    source = _source()
    initial_render_position = source.index("    renderBuilderZones();\n    renderBuildBtn();")
    frontend_trigger_position = source.index(
        'model.set("load_workspaces_trigger",', initial_render_position
    )
    display_position = source.index("    display(widget)", frontend_trigger_position)

    assert initial_render_position < frontend_trigger_position < display_position
    assert 'model.get("dataset_chosen") !== true' in source[
        initial_render_position:frontend_trigger_position
    ]
    assert "threading.Thread(target=_load_workspaces" not in source[display_position:]


def test_model_view_and_collapsed_query_builder_are_identifiable():
    source = _source()
    ui_source = SOURCE_PATH.parents[1].joinpath("_ui_components.py").read_text(
        encoding="utf-8"
    )

    assert 'sidebarTitle.textContent = "Model View"' in source
    assert 'modelViewShowBtn.innerHTML = LIST_TREE_SVG' in source
    assert 'header.appendChild(modelViewShowBtn)' in source
    assert 'sidebarMark.innerHTML = LIST_TREE_SVG' in source
    assert 'sidebarMark.title = "Model View"' in source
    assert ".dtx-sidebar.dtx-sidebar-collapsed .dtx-sidebar-mark" in source
    assert "    border-color: transparent;\n    border-radius: 0;\n    background: transparent;" in source[
        source.index(".dtx .dtx-sidebar.dtx-sidebar-collapsed .dtx-sidebar-mark") :
        source.index(".dtx .dtx-sidebar.dtx-sidebar-collapsed .dtx-sidebar-toggle")
    ]
    assert "const available = chosen || connectingToModel;" in source
    assert "renderModelViewChrome();" in source
    assert "    flex: 0 0 44px;\n    min-width: 44px;\n    max-width: 44px;" in source
    assert ".dtx .dtx-sidebar.dtx-sidebar-collapsed .dtx-sidebar-toggle {{" in source
    assert ".dtx .dtx-builder.dtx-builder-collapsed .dtx-builder-collapse svg {{" in source
    assert '"list_tree", _FALLBACK_LIST_TREE_ICON' in source
    assert '"list_tree": (' in ui_source
    assert 'builderShowBtn.innerHTML = BUILDER_SVG' in source
    assert 'builderMark.innerHTML = BUILDER_SVG' in source
    assert 'builderMark.title = "Query Builder"' in source
    assert ".dtx-builder.dtx-builder-collapsed .dtx-builder-mark" in source
    assert "    border-color: transparent;\n    border-radius: 0;\n    background: transparent;" in source[
        source.index(".dtx .dtx-builder.dtx-builder-collapsed .dtx-builder-mark") :
        source.index(".dtx .dtx-builder.dtx-builder-collapsed .dtx-builder-toggle")
    ]
    assert '"hammer", _FALLBACK_HAMMER_ICON' in source
    assert 'ICONS["builder"] = ICONS["hammer"]' in ui_source
    assert '<path d="m15 12-9.373 9.373a1 1 0 0 1-3.001-3L12 9"/>' in ui_source
    assert '<path d="m18 15 4-4"/>' in ui_source
    assert 'm21.5 11.5-1.914-1.914A2 2 0 0 1 19 8.172' in ui_source
    assert "m14.5 9.5-8.8 8.8" not in ui_source
    assert "    background: var(--ui-surface);\n    color: var(--ui-text-secondary);" in source
    assert "    background: var(--ui-accent-soft);\n    color: var(--ui-accent);" in source
    assert "#67e8eb" not in source
    assert "#08282d" not in source


def test_model_view_flattens_type_groups_and_preserves_display_folders():
    source = _source()
    renderer = source[
        source.index("function renderTableObjects") : source.index(
            "// Filter the model tree"
        )
    ]
    tree_render = source[
        source.index("function renderTree()") : source.index(
            "const main = document.createElement", source.index("function renderTree()")
        )
    ]

    measure_pos = renderer.index('table.measures || []')
    column_pos = renderer.index('table.columns || []')
    hierarchy_pos = renderer.index('table.hierarchies || []')
    calculation_item_pos = renderer.index('table.calculation_items || []')
    assert measure_pos < column_pos < hierarchy_pos < calculation_item_pos
    assert "renderFolderTree(parentEl, buildFolderTree(objects), build, 0)" in renderer
    assert 'it._kind === "measure"' in renderer
    assert 'it._kind === "column"' in renderer
    assert 'it._kind === "hierarchy"' in renderer
    assert "renderTableObjects(children, tbl)" in tree_render
    assert 'makeGroup("Columns"' not in source
    assert 'makeGroup("Measures"' not in source
    assert 'makeGroup("Hierarchies"' not in source
    assert 'makeGroup("Calculation items"' not in source


def test_model_view_uses_power_bi_typography_and_table_counts():
    source = _source()
    tree_render = source[
        source.index("function renderTree()") : source.index(
            "const main = document.createElement", source.index("function renderTree()")
        )
    ]

    assert 'font-family: "Segoe UI", SegoeUI, Arial, sans-serif;' in source
    assert ".dtx .dtx-tree-leaf .dtx-tree-label {{\n    font-size: 14px;" in source
    assert ".dtx .dtx-tree-counts {{" in source
    assert '`${(tbl.columns || []).length}c`' in tree_render
    assert '`${(tbl.measures || []).length}m`' in tree_render
    assert 'countParts.push(`${tbl.hierarchies.length}h`)' in tree_render
    assert 'countParts.join(" · ")' in tree_render


def test_report_query_capture_lists_related_reports_and_uses_camera_action():
    source = _source()
    ui_source = SOURCE_PATH.parents[1].joinpath("_ui_components.py").read_text(
        encoding="utf-8"
    )

    assert '"camera": (' in ui_source
    assert "def _list_reports_for_capture(" in source
    assert '(reports["Dataset Id"].astype(str) == str(dataset_id))' in source
    assert '(reports["Dataset Workspace Id"].astype(str) == str(workspace_id))' in source
    assert 'available_reports = traitlets.List([]).tag(sync=True)' in source
    assert 'reportCaptureBtn.innerHTML = CAMERA_SVG' in source
    assert 'reportCaptureBtn.setAttribute("aria-label", "Capture report queries")' in source
    assert 'let selectedReportIds = new Set();' in source
    assert 'model.set("capture_report_ids", [...selectedReportIds]);' in source
    assert "initial_reports = _list_reports_for_capture(dataset_id, workspace_id)" in source


def test_report_selector_closes_when_canvas_is_clicked():
    source = _source()
    report_ui_start = source.index('const reportCapture = document.createElement("div")')
    report_ui_end = source.index("const runBtn = document.createElement", report_ui_start)
    report_ui = source[report_ui_start:report_ui_end]

    assert "function hideReportMenuOnOutsidePointer(event)" in report_ui
    assert "reportCapture.contains(event.target)" in report_ui
    assert "reportMenuOpen = false;" in report_ui
    assert 'document.addEventListener("pointerdown", hideReportMenuOnOutsidePointer)' in source
    assert 'document.removeEventListener("pointerdown", hideReportMenuOnOutsidePointer)' in source


def test_report_selector_uses_structured_compact_controls():
    source = _source()
    report_label_start = source.index(".dtx .dtx-report-label {{")
    report_label_end = source.index("}}", report_label_start)
    report_label_css = source[report_label_start:report_label_end]

    assert 'reportLabel.textContent = "Reports";' in source
    assert "color: var(--ui-text-secondary);" in report_label_css
    assert 'reportSelectIcon.innerHTML = REPORT_FILE_SVG;' in source
    assert 'reportSelectChevron.innerHTML = CHEVRON_DOWN_SVG;' in source
    assert "reportSelectText.textContent = count === 0" in source
    assert 'clearSelection.textContent = "Clear selection";' in source
    assert "selectedReportIds.clear();" in source
    assert 'checkbox.className = "dtx-report-check"' in source
    assert 'checkbox.innerHTML = CHECK_SVG;' in source
    assert ".dtx .dtx-report-select-icon {{" in source
    assert ".dtx .dtx-report-select-chevron {{" in source
    assert ".dtx .dtx-report-check.dtx-checked {{" in source
    capture_button_css = source[
        source.index(".dtx .dtx-report-capture-btn {{") : source.index(
            ".dtx .dtx-report-capture-btn:hover", source.index(".dtx .dtx-report-capture-btn {{")
        )
    ]
    assert "width: 26px;" in capture_button_css
    assert "height: 26px;" in capture_button_css
    assert "min-width: 26px;" in capture_button_css
    assert ".dtx .dtx-report-capture-btn svg {{ width: 14px; height: 14px; }}" in source


def test_report_query_capture_cycles_pages_and_signals_completion():
    source = _source()
    capture_js = source[
        source.index('const reportCaptureFrame = document.createElement("iframe")') : source.index(
            "// ---------- Analyze", source.index('const reportCaptureFrame = document.createElement("iframe")')
        )
    ]

    assert "powerbi-client@2.23.1" in capture_js
    assert "new client.service.Service(" in capture_js
    assert 'document.createElement("iframe")' in capture_js
    assert "reportCaptureFrame.contentDocument" in capture_js
    assert 'let powerbi = context.captureWindow.powerbi;' in capture_js
    assert "context.captureDocument.head.appendChild(script);" in capture_js
    assert "document.head.appendChild(script);" not in capture_js
    assert "host: context.host" in capture_js
    assert 'window["powerbi-client"].models' not in capture_js
    assert "powerbi.min.js" in capture_js
    assert "await import(" not in capture_js
    assert "script.onload" in capture_js
    assert 'report.on("loaded"' in capture_js
    assert "report.getPages()" in capture_js
    assert "Promise.resolve(page.setActive())" in capture_js
    assert 'report.on("rendered"' in capture_js
    assert "window.setTimeout(activateNext, 1800)" in capture_js
    assert "window.setTimeout(activateNext, 30000)" in capture_js
    assert "window.setTimeout(finish, 120000)" in capture_js
    assert 'model.set("report_capture_finish_trigger"' in capture_js
    assert 'model.on("change:report_capture_payload"' in source


def test_trace_history_is_the_rightmost_results_tab():
    source = _source()
    tab_appends = source[
        source.index("seg.appendChild(segTrace)") : source.index(
            "viewToolbar.appendChild(seg)", source.index("seg.appendChild(segTrace)")
        )
    ]

    assert tab_appends.rstrip().endswith("seg.appendChild(segHistory);")


def test_report_query_capture_correlates_trace_rows_and_appends_history():
    source = _source()
    normalizer = source[
        source.index("def _captured_queries_from_df") : source.index(
            "def _run_dax_trace", source.index("def _captured_queries_from_df")
        )
    ]
    checkpoint_worker = source[
        source.index("def _checkpoint_report_capture") : source.index(
            "def _finish_report_capture", source.index("def _checkpoint_report_capture")
        )
    ]
    finish_worker = source[
        source.index("def _finish_report_capture") : source.index(
            "def _on_report_capture_start", source.index("def _finish_report_capture")
        )
    ]

    assert '"VertiPaqSEQueryEnd"' in normalizer
    assert 'str.contains("Internal", case=False, na=False)' in normalizer
    assert "storage_by_request.get(request_id, 0)" in normalizer
    assert 'df[df[event_col] == "QueryEnd"]' in normalizer
    assert "captured = _captured_queries_from_df(new_logs)" in checkpoint_worker
    assert '"method": "Report"' in checkpoint_worker
    assert '"report_name": report_name' in checkpoint_worker
    assert '"report_workspace_name": report_workspace_name' in checkpoint_worker
    assert "widget.trace_history = list(reversed(entries))" in finish_worker
    assert 'widget.view_mode = "history"' in finish_worker
    assert "widget.trace_rows" not in finish_worker
    assert 'model.set("report_capture_checkpoint_trigger"' in source
    assert 'model.get("report_capture_checkpoint_ack") === checkpointId' in source
    assert 'widget.observe(_on_report_capture_start, names="report_capture_start_trigger")' in source
    assert 'widget.observe(_on_report_capture_finish, names="report_capture_finish_trigger")' in source


def test_query_builder_footer_clear_can_be_undone():
    source = _source()

    undo_position = source.index("builderHeader.appendChild(builderUndoBtn)")
    collapse_position = source.index("builderHeader.appendChild(builderCollapseBtn)")
    assert undo_position < collapse_position
    assert "builderToggle" not in source
    assert "dtx-builder-clear-toggle" not in source
    assert "fields: structuredClone(builderFields)" in source
    assert "filters: structuredClone(builderFilters)" in source
    assert "orderBy: structuredClone(builderOrderBy)" in source
    assert "builderUndoBtn.addEventListener(\"click\", undoBuilderClear)" in source
    assert 'builderUndoBtn.style.display = clearedBuilderState ? "" : "none"' in source
    assert "builderFields = clearedBuilderState.fields" in source
    assert "builderFilters = clearedBuilderState.filters" in source
    assert "builderOrderBy = clearedBuilderState.orderBy" in source
    assert "clearBtn.addEventListener(\"click\", clearBuilder)" in source


def test_query_builder_clears_after_model_change():
    source = _source()
    reset_builder = source[
        source.index("function resetBuilderForModelChange") : source.index(
            "function undoBuilderClear"
        )
    ]
    active_model_listener = source[
        source.index('model.on("change:active_dataset_id"') : source.index(
            'model.on("change:picker_loading"'
        )
    ]

    assert "builderFields = [];" in reset_builder
    assert "builderFilters = [];" in reset_builder
    assert "builderOrderBy = [];" in reset_builder
    assert "clearedBuilderState = null;" in reset_builder
    assert "qbSeq = 0;" in reset_builder
    assert "renderBuilderZones();" in reset_builder
    assert "renderBuilderChrome();" in reset_builder
    assert "resetBuilderForModelChange();" in active_model_listener


def test_panel_header_actions_are_larger():
    source = _source()

    assert ".dtx .dtx-sidebar-toggle,\n.dtx .dtx-sidebar-refresh,\n.dtx .dtx-builder-toggle {{" in source
    assert "    width: 28px;\n    height: 28px;" in source
    assert ".dtx .dtx-sidebar-toggle svg,\n.dtx .dtx-sidebar-refresh svg,\n.dtx .dtx-builder-toggle svg {{" in source
    assert "    width: 16px;\n    height: 16px;" in source


def test_fullscreen_uses_viewport_height_for_panes_and_query_editor():
    source = _source()

    for selector in (".dtx.dtx-fullscreen", ".dtx:fullscreen"):
        assert f"{selector} .dtx-container {{{{" in source
        assert f"{selector} .dtx-body {{{{" in source
        assert "    flex: 1 1 0;\n    height: 0;\n    min-height: 0;\n    overflow: hidden;" in source
        assert f"{selector} .dtx-main {{{{" in source
        assert "    height: 100%;\n    min-height: 0;\n    max-height: 100%;\n    overflow-x: hidden;\n    overflow-y: auto;" in source
        assert f"{selector} .dtx-main > * {{{{ flex-shrink: 0; }}}}" in source
        assert f"{selector} .dtx-query {{{{ min-height: 300px; max-height: 60vh; }}}}" in source

    assert source.count("    height: 100vh;\n    min-height: 100vh;") >= 2
    assert source.count("    display: flex;\n    flex-direction: column;") >= 2
    assert source.count("    overflow: hidden;\n    background: var(--ui-bg);") >= 1


def test_fullscreen_container_has_no_later_shape_override():
    source = _source()

    assert source.count(".dtx.dtx-fullscreen .dtx-container {{") == 1
    assert source.count(".dtx:fullscreen .dtx-container {{") == 1
    assert ".dtx .dtx-perf-chip {{" in source
    assert "border-radius: 999px;" in source
    assert "border-radius: 999px;\n    background: var(--ui-surface" in source


def test_change_model_button_is_larger_and_beside_the_tool_name():
    source = _source()
    title_start = source.index('title.textContent = "DAX Perf Optimizer"')
    change_start = source.index('changeModelBtn.className = "dtx-change-btn"')
    title_row_end = source.index("titleRow.appendChild(changeModelBtn)", change_start)

    assert title_start < change_start < title_row_end
    assert '.dtx .dtx-change-btn {{' in source
    assert "    width: 32px;\n    height: 32px;" in source
    assert 'changeModelBtn.title = "Change model / workspace"' in source


def test_run_as_segmented_control_is_above_the_dax_query():
    source = _source()
    options_position = source.index('queryOptions.className = "dtx-query-options"')
    query_position = source.index('qTitle.textContent = "DAX Query"')

    assert options_position < query_position
    assert 'impLabel.textContent = "RUN AS"' in source
    assert '["none", "No impersonation", SHIELD_CHECK_SVG]' in source
    assert '["role", "Role", USERS_SVG]' in source
    assert '["user", "User", USER_SVG]' in source
    assert 'impSegment.setAttribute("role", "group")' in source
    assert "    height: 26px;\n    overflow: hidden;" in source
    assert "    height: 24px !important;\n    min-height: 24px !important;\n    max-height: 24px !important;\n    padding: 0 8px !important;" in source
    assert "    line-height: 1 !important;" in source
    assert ".dtx .dtx-imp-mode svg {{ width: 12px; height: 12px; }}" in source
    assert "    white-space: nowrap;" in source
    assert ".dtx .dtx-imp-segment {{\n    display: inline-flex;\n    flex: 0 0 auto;" in source
    assert "    width: 220px;" in source
    assert ".dtx .dtx-main {{" in source
    assert "    padding-left: 0;" in source


def test_timing_cards_are_below_query_and_hidden_until_execution():
    source = _source()
    query_append = source.index("main.appendChild(queryBlock)")
    cards_append = source.index("main.appendChild(cardsEl)")

    assert query_append < cards_append
    assert 'model.get("query_executed") === true' in source
    assert "query_executed = traitlets.Bool(False).tag(sync=True)" in source
    assert "widget.query_executed = True" in source
    assert "widget.query_executed = False" in source


def test_timing_cards_show_metric_specific_icons():
    source = _source()
    ui_source = SOURCE_PATH.parents[1].joinpath("_ui_components.py").read_text(
        encoding="utf-8"
    )
    cards = source[
        source.index("const cards = [") : source.index(
            "const toolbar = document.createElement", source.index("const cards = [")
        )
    ]

    for icon_name in ("cpu", "database", "zap"):
        assert f'"{icon_name}": (' in ui_source
    assert 'label: "Duration", icon: DAX_PERFORMANCE_SVG' in cards
    assert 'label: "FE Duration", icon: CPU_SVG' in cards
    assert 'label: "SE Duration", icon: DATABASE_SVG' in cards
    assert 'label: "CPU", icon: ZAP_SVG' in cards
    assert '${c.icon}<span>${escapeHtml(c.label)}</span>' in cards
    assert ".dtx .dtx-card-label svg {{" in source
    assert "    width: 16px;\n    height: 16px;" in source


def test_clear_cache_uses_a_toggle_switch():
    source = _source()
    query_wrap_append = source.index("queryBlock.appendChild(queryWrap)")
    cache_row_append = source.index("queryBlock.appendChild(queryCacheRow)")

    assert 'cacheSwitch.className = "dtx-cache-switch"' in source
    assert ".dtx-cache-label input:checked + .dtx-cache-switch" in source
    assert "toolbar.appendChild(cacheLabel)" not in source
    assert "queryOptions.appendChild(cacheLabel)" not in source
    assert "queryOptions.appendChild(impWrap)" in source
    assert "queryOptions.appendChild(reportCapture)" in source
    assert query_wrap_append < source.index("queryCacheRow.appendChild(cacheLabel)")
    assert query_wrap_append < cache_row_append
    assert ".dtx .dtx-query-cache-row {{" in source
    assert "    justify-content: flex-end;" in source
    assert 'cacheText.textContent = "Clear cache before run (cold-cache timings)"' in source


def test_eraser_button_clears_the_active_model_cache():
    source = _source()
    ui_source = SOURCE_PATH.parents[1].joinpath("_ui_components.py").read_text(
        encoding="utf-8"
    )

    run_position = source.index("toolbar.appendChild(runBtn)")
    cache_position = source.index("toolbar.appendChild(clearModelCacheBtn)")
    assert run_position < cache_position
    assert '"eraser", _FALLBACK_ERASER_ICON' in source
    assert '"eraser": (' in ui_source
    assert 'stroke="currentColor"' in ui_source[ui_source.index('"eraser": ('):]
    assert '<path d="M22 21H7"/><path d="m5 11 9 9"/>' in ui_source
    assert "const ERASER_SVG = `__DTX_ERASER__`;" in source
    assert "clearModelCacheBtn.innerHTML = ERASER_SVG" in source
    assert ".dtx .dtx-clear-model-cache-btn {{ color: var(--ui-text-secondary); }}" in source
    assert "    width: 17px;\n    height: 17px;\n    fill: none !important;\n    stroke: currentColor !important;" in source
    assert ".dtx .dtx-clear-model-cache-btn:hover:not(:disabled) {{ color: var(--ui-accent); }}" in source
    assert 'clearModelCacheBtn.setAttribute("aria-label", "Clear model cache")' in source
    assert 'cache_clear_trigger = traitlets.Int(0).tag(sync=True)' in source
    assert 'cache_clear_loading = traitlets.Bool(False).tag(sync=True)' in source
    assert 'widget.observe(_on_clear_model_cache, names="cache_clear_trigger")' in source
    assert '_clear_cache_fn(\n                dataset=dataset_id,' in source
    assert "renderRunBtn(); renderClearModelCacheBtn(); renderSubtitle();" in source


def test_run_button_matches_adjacent_toolbar_button_size():
    source = _source()
    run_button_css = source[
        source.index(".dtx .dtx-btn {{") : source.index(
            ".dtx .dtx-btn:hover", source.index(".dtx .dtx-btn {{")
        )
    ]

    assert "width: 26px;" in run_button_css
    assert "height: 26px;" in run_button_css
    assert "min-width: 26px;" in run_button_css
    assert ".dtx .dtx-fmt-btn {{" in source
    assert source.count("width: 26px;") >= 2
    assert source.count("height: 26px;") >= 2


def test_query_builder_filter_placeholder_names_supported_objects():
    source = _source()

    assert 'ph.textContent = "Drag columns and measures here to filter"' in source
    assert 'ph.textContent = "Drag fields here to filter"' not in source


def test_query_builder_filters_and_toolbar_actions_keep_stable_layouts():
    source = _source()

    assert ".dtx .dtx-builder-chip-filter {{\n    display: grid;" in source
    assert ".dtx .dtx-filter-chip-head {{" in source
    assert ".dtx .dtx-builder-chip-filter .dtx-chip-op {{" in source
    assert ".dtx .dtx-builder-chip-filter .dtx-chip-values {{" in source
    filter_chip = source[
        source.index("function makeFilterChip") : source.index(
            "// Keep the Order By pane"
        )
    ]
    assert 'head.className = "dtx-filter-chip-head"' in filter_chip
    assert filter_chip.index("head.appendChild(ic)") < filter_chip.index("chip.appendChild(opSel)")
    assert filter_chip.index("head.appendChild(label)") < filter_chip.index("chip.appendChild(opSel)")
    assert filter_chip.index("head.appendChild(makeChipRemove") < filter_chip.index("chip.appendChild(opSel)")
    assert filter_chip.index("chip.appendChild(opSel)") < filter_chip.index("chip.appendChild(valWrap)")
    assert "    flex: 0 0 30px;\n    width: 30px;\n    min-width: 30px;\n    max-width: 30px;" in source
    assert "    height: 30px;\n    min-height: 30px;\n    max-height: 30px;" in source
    assert 'buildBtn.innerHTML = BUILDER_SVG + "<span>Build</span>"' in source
    assert ".dtx .dtx-build-btn svg {{ width: 14px; height: 14px; }}" in source


def test_dax_formatter_icon_fits_inside_its_button():
    source = _source()

    assert 'fmtBtn.className = "dtx-fmt-btn dtx-daxformat-btn"' in source
    assert ".dtx .dtx-daxformat-btn svg {{ width: 18px; }}" in source


def test_running_query_shows_an_indeterminate_progress_bar():
    source = _source()

    assert 'runProgress.className = "dtx-run-progress"' in source
    assert 'runProgress.setAttribute("role", "progressbar")' in source
    assert 'runProgress.classList.toggle("dtx-active", running)' in source
    assert ".dtx .dtx-run-progress.dtx-active {{ display: block; }}" in source
    assert "animation: dtx-run-progress 1s ease-in-out infinite;" in source


def test_interactive_run_does_not_wait_for_optional_trace_events():
    source = _source()
    capture = source[
        source.index("def _execute_and_capture") : source.index(
            "def _compute_trace_stats"
        )
    ]
    persistent_run = source[
        source.index("def _run_query_persistent") : source.index(
            "def _backfill_query_plan"
        )
    ]
    worker = source[source.index("def _worker") : source.index("def _on_run")]

    assert "wait_for_optional_events: bool = True" in capture
    assert "30.0 if wait_for_optional_events else 0.0" in capture
    assert "_first_qe_poll = True" in capture
    assert "time.sleep(0.05)" in capture
    assert "wait_for_optional_events=False" in persistent_run
    assert "_backfill_query_plan(run_id, int(start_baseline), query)" in worker


def test_play_button_is_a_rounded_rectangle():
    source = _source()
    button_css = source[
        source.index(".dtx .dtx-btn {{") : source.index(
            ".dtx .dtx-query-wrap {{"
        )
    ]

    assert "    border-radius: 7px;" in button_css
    assert ".dtx .dtx-btn.dtx-btn-stop {{" in button_css
    assert "    border-radius: 50%;" in button_css


def test_trace_details_include_direct_query_and_optimizer_fields():
    source = _source()
    direct_query_schema = source[
        source.index('"DirectQueryEnd": [') : source.index('"DAXQueryPlan": [')
    ]
    trace_serializer = source[
        source.index("def _trace_rows_from_df") : source.index(
            "def _query_plan_rows_from_df"
        )
    ]
    trace_table = source[
        source.index("function renderTraceTable") : source.index(
            "function renderResultTable"
        )
    ]

    assert '"EventSubclass"' not in direct_query_schema
    assert '"DirectQueryEnd",' in trace_serializer
    detail_classes = trace_serializer[
        trace_serializer.index("detail_classes = {") : trace_serializer.index(
            "rows_df =", trace_serializer.index("detail_classes = {")
        )
    ]
    assert '"QueryEnd"' not in detail_classes
    assert 'if subclass_v == "VertiPaqScanInternal":' in trace_serializer
    assert "marshalling bytes" in trace_serializer
    assert '"rows": int(estimate.group(1)) if estimate else None' in trace_serializer
    assert '"kb": int(estimate.group(2)) / 1024 if estimate else None' in trace_serializer
    assert '"text": text_v' in trace_serializer
    assert 'colspan="7"' in trace_table
    for heading in ("Event", "Subclass", "Duration", "CPU", "Rows", "KB", "Text"):
        assert f">{heading}</th>" in trace_table


def test_trace_details_format_timings_and_sql_like_text():
    source = _source()
    trace_helpers = source[
        source.index("const TRACE_XMSQL_KEYWORDS") : source.index(
            "function render({ model, el })"
        )
    ]
    trace_table = source[
        source.index("function renderTraceTable") : source.index(
            "function renderResultTable"
        )
    ]

    assert '${escapeHtml(fmt(r.duration))} ms' in trace_table
    assert '${escapeHtml(fmt(r.cpu))} ms' in trace_table
    assert "function highlightXmSqlLine(line)" in trace_helpers
    assert "function highlightSqlLine(line)" in trace_helpers
    assert 'eventClass === "DirectQueryEnd"' in trace_helpers
    assert 'eventClass === "VertiPaqSEQueryEnd"' in trace_helpers
    assert "html += escapeHtml(line.slice(last, match.index));" in trace_helpers
    assert "Estimated size: rows = " in trace_helpers
    assert "  bytes = " in trace_helpers
    assert 'replace(/<\\/?ccon>/gi, "")' in trace_helpers
    assert 'const closingMarkerIndex = callbackTail.search(/<\\/ccon>/i)' in trace_helpers
    assert 'const callbackHighlight = callbackText.replace(/[\\s)]*$/, "")' in trace_helpers
    assert "const callbackSuffix = callbackText.slice(callbackHighlight.length)" in trace_helpers
    assert "line.slice(callbackEnd + closingMarkerLength)" in trace_helpers
    assert 'class="dtx-trace-callback"' in trace_helpers
    assert ".dtx .dtx-trace-callback {{" in source
    assert "background: var(--ui-warning-bg);" in source
    assert "color: var(--ui-warning-text);" in source
    assert "font-weight: 700;" in source
    assert "volume, marshalling bytes" not in trace_table
    assert "renderTraceText(r.text, r.event_class)" in trace_table


def test_trace_history_matches_optimizer_columns_and_metrics_dictionary():
    source = _source()
    history_table = source[
        source.index("function renderHistoryTable") : source.index(
            "function renderQueryPlanTable"
        )
    ]
    metrics_helper = source[
        source.index("def _execution_metrics_dict") : source.index(
            "def _result_payload_from_df"
        )
    ]
    history_export = source[
        source.index("def _build_history_excel") : source.index(
            "def _on_download_history"
        )
    ]

    headings = (
        "Run", "Total", "FE", "SE", "CPU", "Cache", "Execution metrics",
        "Method", "Query", "Report", "Workspace",
    )
    positions = [history_table.index(f">{heading}</th>") for heading in headings]
    assert positions == sorted(positions)
    assert 'const renderMetrics = (metrics) =>' in history_table
    assert 'typeof value === "number" && Number.isFinite(value)' in history_table
    assert 'class="dtx-hist-metric-number"' in history_table
    assert '.dtx .dtx-hist-metric-number {{ color: var(--ui-syntax-number); }}' in source
    assert 'class="dtx-hist-metrics"' in history_table
    assert '${escapeHtml(fmt(h.duration))} ms' in history_table
    assert '${escapeHtml(fmt(h.fe_duration))} ms' in history_table
    assert '${escapeHtml(fmt(h.se_duration))} ms' in history_table
    assert '${escapeHtml(fmt(h.cpu))} ms' in history_table
    assert 'const fmtRunTime = (value) =>' in history_table
    assert 'date.toLocaleTimeString("en-US", {' in history_table
    assert 'hour: "numeric", minute: "2-digit", second: "2-digit", hour12: true' in history_table
    assert 'const runTime = fmtRunTime(run)' in history_table
    assert 'str(row.get("label") or row.get("key") or "")' in metrics_helper
    assert '"directQueryTotalRows", "DirectQuery Total Rows"' in source
    assert '"Method",\n            "Query",\n            "Report",\n            "Workspace",' in history_export
    assert '"Method": entry.get("method", "Query")' in history_export
    assert 'const method = String(h.method || "Query")' in history_table
    assert 'const reportName = method === "Report"' in history_table
    assert history_table.index(">Method</th>") < history_table.index(">Query</th>")
    assert history_table.index(">Query</th>") < history_table.index(">Report</th>")
    assert history_table.index(">Report</th>") < history_table.index(">Workspace</th>")
    assert source.count('"method": "Query"') == 2


def test_output_tables_are_resizable_and_trace_history_is_sortable():
    source = _source()
    table_setup = source[
        source.index("const outputColumnWidths = new Map()") : source.index(
            "const chartControls", source.index("const outputColumnWidths = new Map()")
        )
    ]
    history_table = source[
        source.index("const historySortState") : source.index(
            "function renderQueryPlanTable", source.index("const historySortState")
        )
    ]

    assert "function installColumnResizers(table)" in table_setup
    assert 'handle.className = "dtx-column-resizer"' in table_setup
    assert 'handle.addEventListener("pointerdown"' in table_setup
    assert "outputColumnWidths.set(key, [...widths])" in table_setup
    assert "new MutationObserver(enhanceOutputTables)" in table_setup
    assert "outputTableObserver.disconnect();" in source
    assert ".dtx .dtx-column-resizer {{" in source
    assert 'const historySortState = { key: "", direction: "ascending" }' in history_table
    assert "function historySortValue(entry, key)" in history_table
    assert 'data-history-sort="query"' in history_table
    assert 'data-history-sort="workspace"' in history_table
    assert 'header.setAttribute("aria-sort", historySortState.direction)' in history_table
    assert "indexedHistory.sort((left, right) =>" in history_table


def test_workspace_monitoring_matches_tools_app_behavior():
    source = _source()
    ui_source = SOURCE_PATH.parents[1].joinpath("_ui_components.py").read_text(
        encoding="utf-8"
    )
    panel_start = source.index("// ---------- Workspace monitoring ----------")
    panel = source[panel_start : source.index("const chartControls", panel_start)]
    worker_start = source.index("def _load_workspace_monitoring()")
    worker = source[worker_start : source.index("widget.observe(_on_run", worker_start)]

    assert '"activity": (' in ui_source
    assert "header.appendChild(builderShowBtn);\n    header.appendChild(monitoringShowBtn);" in source
    assert 'monitoringShowBtn.innerHTML = ACTIVITY_SVG' in source
    assert '? "Hide workspace monitoring" : "Show workspace monitoring"' in panel
    assert '<span>Workspace monitoring</span>' in panel
    assert '· slowest recent queries</span>' in panel
    assert '["15m", "Last 15 min"]' in panel
    assert '["30d", "Last 30 days"]' in panel
    assert 'topInput.max = "200"' in panel
    assert 'model.set("workspace_monitoring_trigger"' in panel
    assert 'installColumnResizers(monitoringContent.querySelector("table"))' in panel
    assert 'model.set("dax_query", query)' in panel
    assert 'data-monitoring-sort="${index}"' in panel
    assert 'monitoringSearchInput.className = "dtx-monitoring-search"' in panel
    assert 'monitoringSearch = monitoringSearchInput.value' in panel
    assert 'row.some((value, index) =>' in panel
    assert 'class="dtx-monitoring-filter"' not in panel
    assert 'monitoringResizer.className = "dtx-monitoring-resizer"' in panel
    assert 'startHeight + startY - moveEvent.clientY' in panel
    assert 'monitoringContent.style.height = `${monitoringContentHeight}px`' in panel
    assert ".dtx .dtx-monitoring-content {{\n    min-height: 190px;\n    max-height: none;" in source
    assert 'monitoringSort.direction === "ascending"' in panel
    assert 'cleanDaxQuery(rawValue)' in panel
    assert 'durationms: "Duration (MS)"' in panel
    assert 'eventtext: "Query"' in panel
    assert 'date.toLocaleString()' in panel
    assert 'workspace_monitoring_request = traitlets.Dict({}).tag(sync=True)' in source
    assert 'workspace_monitoring_rows = traitlets.List([]).tag(sync=True)' in source
    assert '"SemanticModelLogs\\n"' in worker
    assert 'OperationName == "QueryEnd"' in worker
    assert 'EventText startswith "EVALUATE"' in worker
    assert 'EventText startswith "DEFINE"' in worker
    assert 'f\'| where ItemName == "{safe_dataset}"\\n\'' in worker
    assert 'f"| where Timestamp >= ago({time_range})\\n"' in worker
    assert 'f"| top {top_n} by DurationMs desc"' in worker
    assert "query_workspace_monitoring(" in worker
    assert 'dataset != str(widget.dataset_name or "")' in worker
    assert 'workspace != model_ctx["workspace_id"]' in worker
    assert 'monitoring_df["ReportName"]' in worker
    assert 'monitoring_df["ReportWorkspace"]' in worker
    assert 'names="workspace_monitoring_trigger"' in source
    assert "widget.workspace_monitoring_loaded = False" in source


def test_trace_history_backfills_late_execution_metrics():
    source = _source()
    update_helper = source[
        source.index("def _update_history_execution_metrics") : source.index(
            "def _backfill_query_plan"
        )
    ]
    backfill = source[
        source.index("def _backfill_query_plan") : source.index("def _worker")
    ]

    assert 'entry.get("run_id") == history_id' in update_helper
    assert 'entry["execution_metrics"] = metrics' in update_helper
    assert '_update_history_execution_metrics(run_id, metric_rows)' in backfill
    assert source.count('"execution_metrics": _execution_metrics_dict(metric_rows)') == 2
    assert source.count('"cache": "Cold"') == 3


def test_trace_history_queries_copy_and_clear_with_user_feedback():
    source = _source()
    history_table = source[
        source.index("function renderHistoryTable") : source.index(
            "function renderQueryPlanTable"
        )
    ]
    history_controls = source[
        source.index('const histDownloadBtn = document.createElement("button")') :
        source.index('const resultDownloadBtn = document.createElement("button")')
    ]

    assert 'data-history-index="${index}"' in history_table
    assert 'const q = cleanDaxQuery(h.dax_query)' in history_table
    assert 'const query = entry ? cleanDaxQuery(entry.dax_query) : ""' in history_table
    assert 'tabindex="0" role="button"' in history_table
    assert 'writeClipboard(query)' in history_table
    assert 'event.key === "Enter" || event.key === " "' in history_table
    assert 'showToast("Query copied to clipboard")' in history_table
    assert 'histClearBtn.innerHTML = TRASH_SVG' in history_controls
    assert 'class="dtx-confirm-dialog" role="dialog" aria-modal="true"' in history_controls
    assert 'class="dtx-confirm-clear">Clear history</button>' in history_controls
    assert 'histClearBtn.addEventListener("click", openClearHistoryDialog)' in history_controls
    assert 'event.key === "Escape"' in history_controls
    assert 'window.confirm(' not in source
    assert 'model.set("trace_history", [])' in history_controls
    assert 'showToast("Trace history cleared")' in history_controls
    assert 'toast.setAttribute("aria-live", "polite")' in source
    assert '.replace("__DTX_TRASH__", trash_icon)' in source
