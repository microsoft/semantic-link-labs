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


def test_model_metadata_loads_before_main_screen_is_enabled():
    source = _source()
    activation_start = source.index("def _activate_selected_dataset()")
    activation_end = source.index("def _on_select_dataset", activation_start)
    activation_source = source[activation_start:activation_end]

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


def test_query_builder_header_clear_can_be_undone():
    source = _source()

    clear_position = source.index("builderHeader.appendChild(builderToggle)")
    undo_position = source.index("builderHeader.appendChild(builderUndoBtn)")
    collapse_position = source.index("builderHeader.appendChild(builderCollapseBtn)")
    assert clear_position < undo_position < collapse_position
    assert 'builderToggle.title = "Clear query builder"' in source
    assert "builderToggle.addEventListener(\"click\", clearBuilder)" in source
    assert "fields: structuredClone(builderFields)" in source
    assert "filters: structuredClone(builderFilters)" in source
    assert "orderBy: structuredClone(builderOrderBy)" in source
    assert "builderUndoBtn.addEventListener(\"click\", undoBuilderClear)" in source
    assert 'builderUndoBtn.style.display = clearedBuilderState ? "" : "none"' in source
    assert "builderFields = clearedBuilderState.fields" in source
    assert "builderFilters = clearedBuilderState.filters" in source
    assert "builderOrderBy = clearedBuilderState.orderBy" in source
    assert "clearBtn.addEventListener(\"click\", clearBuilder)" in source
    assert 'builderToggle.title = "Hide query builder"' not in source


def test_panel_header_actions_are_larger_and_clear_requires_contents():
    source = _source()

    assert ".dtx .dtx-sidebar-toggle,\n.dtx .dtx-sidebar-refresh,\n.dtx .dtx-builder-toggle {{" in source
    assert "    width: 28px;\n    height: 28px;" in source
    assert ".dtx .dtx-sidebar-toggle svg,\n.dtx .dtx-sidebar-refresh svg,\n.dtx .dtx-builder-toggle svg {{" in source
    assert "    width: 16px;\n    height: 16px;" in source
    assert "builderToggle.style.display =\n            (builderFields.length || builderFilters.length) ? \"\" : \"none\";" in source


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

    assert 'cacheSwitch.className = "dtx-cache-switch"' in source
    assert ".dtx-cache-label input:checked + .dtx-cache-switch" in source
    assert "toolbar.appendChild(cacheLabel)" not in source
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


def test_query_builder_filter_placeholder_names_supported_objects():
    source = _source()

    assert 'ph.textContent = "Drag columns and measures here to filter"' in source
    assert 'ph.textContent = "Drag fields here to filter"' not in source


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
    assert "volume, marshalling bytes" not in trace_table
    assert "renderTraceText(r.text, r.event_class)" in trace_table
