from inspect import getsource, signature

from sempy_labs.semantic_model import _refresh_manager as refresh_manager_module


class _Response:
    def json(self):
        return {"status": "Completed"}


class _DotNetDate:
    def ToString(self, date_format):
        assert date_format == "o"
        return "2026-08-02T12:30:00.0000000Z"


def test_response_json_accepts_response_and_mapping():
    assert refresh_manager_module._response_json(_Response()) == {"status": "Completed"}
    assert refresh_manager_module._response_json({"status": "Failed"}) == {
        "status": "Failed"
    }


def test_refresh_detail_json_preserves_route_request_id():
    assert refresh_manager_module._refresh_detail_json(
        {"status": "Completed", "type": "Full"}, "refresh-id"
    ) == {
        "status": "Completed",
        "type": "Full",
        "requestId": "refresh-id",
    }


def test_format_refreshed_time_supports_dotnet_datetime():
    assert (
        refresh_manager_module._format_refreshed_time(_DotNetDate())
        == "2026-08-02T12:30:00.0000000Z"
    )


def test_refresh_trace_events_filter_and_map_partitions():
    source = getsource(refresh_manager_module._get_refresh_trace_events)
    assert '["ExecuteSql", "Process"]' in source
    assert 'left_on="Object ID"' in source
    assert 'right_on="PartitionID"' in source
    for field in [
        "objectName",
        "tableName",
        "partitionName",
        "eventSubclass",
        "startTime",
        "endTime",
        "durationMs",
        "cpuTimeMs",
    ]:
        assert field in source


def test_widget_sources_have_no_unresolved_templates():
    assert "__I_" not in refresh_manager_module._WIDGET_JS
    assert "__LIGHT_THEME__" not in refresh_manager_module._WIDGET_CSS
    assert "__DARK_THEME__" not in refresh_manager_module._WIDGET_CSS
    assert "export default { render };" in refresh_manager_module._WIDGET_JS


def test_refresh_manager_public_signature():
    parameters = signature(refresh_manager_module.refresh_manager).parameters
    assert list(parameters) == ["dataset", "workspace", "dark_mode"]
    assert parameters["dataset"].default is None
    assert parameters["workspace"].default is None
    assert parameters["dark_mode"].default is False


def test_widget_includes_optional_dataset_picker():
    assert "Connect to a semantic model" in refresh_manager_module._WIDGET_JS
    assert 'dispatch("connect"' in refresh_manager_module._WIDGET_JS
    assert 'model.get("connected")' in refresh_manager_module._WIDGET_JS
    assert ">Connect</button>" in refresh_manager_module._WIDGET_JS
    assert "slls-rm-picker-connect" in refresh_manager_module._WIDGET_JS
    assert "slls-rm-btn.primary.slls-rm-picker-connect" in (
        refresh_manager_module._WIDGET_CSS
    )
    assert "flex:none;width:auto;min-width:96px" in refresh_manager_module._WIDGET_CSS
    assert "data-picker-close" in refresh_manager_module._WIDGET_JS
    assert "data-picker-cancel" not in refresh_manager_module._WIDGET_JS


def test_picker_matches_lineage_view_design():
    widget_js = refresh_manager_module._WIDGET_JS
    widget_css = refresh_manager_module._WIDGET_CSS

    assert "createSearchSelect({" in widget_js
    assert widget_js.count("createSearchSelect({") == 2
    assert 'searchPlaceholder:"Filter workspaces\\u2026"' in widget_js
    assert 'searchPlaceholder:"Filter semantic models\\u2026"' in widget_js
    assert "slls-rm-combo" not in widget_js
    assert "slls-rm-combo" not in widget_css
    assert "Select a workspace and semantic model to begin." in widget_js
    assert ".slls-ss-btn" in widget_css
    assert ".slls-rm-picker-grid .slls-ss-btn { border-radius:999px;" in widget_css
    assert (
        ".slls-rm-picker { width:100%;border:1px solid var(--ui-border);"
        "border-radius:14px;background:var(--ui-surface);padding:16px; }" in widget_css
    )
    assert ".sl-reload-btn {" in widget_css
    reload_btn_rule = widget_css.split(".sl-reload-btn {", 1)[1].split("}", 1)[0]
    assert "width: 32px;" in reload_btn_rule
    assert "height: 32px;" in reload_btn_rule
    assert ".slls-rm-picker-grid { display:flex;align-items:flex-end;" in widget_css


def test_picker_lists_are_seeded_before_the_widget_is_displayed():
    """A trait set after ``display()`` races the comm handshake.

    In Fabric PySpark notebooks that update never reaches the browser, so the
    picker lists have to be part of the widget's initial state instead.
    """

    source = getsource(refresh_manager_module.refresh_manager)

    assert "_list_picker_workspaces(workspace_id, workspace_name)" in source
    assert "_list_picker_datasets(target_workspace_id)" in source
    seed_index = source.index("initial_workspaces = (")
    display_index = source.index("display(widget)")
    assert seed_index < display_index
    assert "else list_workspaces()" in source
    assert "{workspace_id: list_datasets(workspace_id)}" in source
    # The old post-display discovery must not come back.
    assert "load_initial_workspaces" not in source
    assert "load_initial_datasets" not in source
    assert "threading.Thread" not in source


def test_fullscreen_and_theme_buttons_use_neutral_icon_style():
    assert "slls-rm-header-action" not in refresh_manager_module._WIDGET_JS
    assert "slls-rm-header-action" not in refresh_manager_module._WIDGET_CSS
    # The header controls are the shared ones from sempy_labs._ui_components.
    assert 'class="sl-theme-btn" data-fullscreen' in (
        refresh_manager_module._WIDGET_JS
    )
    assert 'class="sl-theme-btn" data-theme' in refresh_manager_module._WIDGET_JS
    assert 'class="sl-change-btn" data-change-model' in (
        refresh_manager_module._WIDGET_JS
    )
    assert ".slls-rm-title-row .slls-rm-iconbtn" not in (
        refresh_manager_module._WIDGET_CSS
    )
    assert ".slls-rm-iconbtn { width:34px;height:34px" in (
        refresh_manager_module._WIDGET_CSS
    )
    assert "background:var(--ui-bg);color:var(--ui-text);display:inline-flex" in (
        refresh_manager_module._WIDGET_CSS
    )
    iconbtn_rule = refresh_manager_module._WIDGET_CSS.split(
        ".slls-rm-iconbtn {", 1
    )[1].split("}", 1)[0]
    assert "--ui-text-secondary" not in iconbtn_rule
    assert "data-picker-ws" in refresh_manager_module._WIDGET_JS
    assert "data-change-model" in refresh_manager_module._WIDGET_JS
    assert "data-fullscreen" in refresh_manager_module._WIDGET_JS
    assert 'fullscreenLabel=fsMode?"Exit full screen":"Full screen"' in (
        refresh_manager_module._WIDGET_JS
    )
    assert 'title="${fullscreenLabel}" aria-label="${fullscreenLabel}"' in (
        refresh_manager_module._WIDGET_JS
    )
    assert 'class="sl-reload-btn' in refresh_manager_module._WIDGET_JS
    assert "slls-rm-picker-reload" not in refresh_manager_module._WIDGET_JS
    assert 'title="Reload workspaces and semantic models"' in (
        refresh_manager_module._WIDGET_JS
    )
    assert "${I.refresh}Reload" not in refresh_manager_module._WIDGET_JS
    assert 'class="slls-rm-title-row"' in refresh_manager_module._WIDGET_JS


def test_widget_uses_shared_refresh_history_icon():
    assert (
        refresh_manager_module._UI_ICONS["history"] in refresh_manager_module._WIDGET_JS
    )


def test_widget_uses_shared_expand_and_collapse_icons():
    assert (
        refresh_manager_module._UI_ICONS["expand_rows"]
        in refresh_manager_module._WIDGET_JS
    )
    assert (
        refresh_manager_module._UI_ICONS["collapse_rows"]
        in refresh_manager_module._WIDGET_JS
    )
    assert 'expandAll.title=collapse?"Collapse all":"Expand all"' in (
        refresh_manager_module._WIDGET_JS
    )


def test_objects_to_refresh_tables_are_sorted_alphabetically():
    assert (
        '.sort((left,right)=>left.name.localeCompare(right.name,undefined,'
        '{sensitivity:"base"}))' in refresh_manager_module._WIDGET_JS
    )


def test_widget_has_independent_refresh_history_reload_button():
    assert "data-reload-history" in refresh_manager_module._WIDGET_JS
    assert 'closest("[data-reload-history]")' in refresh_manager_module._WIDGET_JS
    assert 'keepPosition();dispatch("load_history")' in (
        refresh_manager_module._WIDGET_JS
    )
    assert (
        'event.target.closest("[data-reload-history],[data-reload-schedule],'
        '[data-cancel-history]")' in refresh_manager_module._WIDGET_JS
    )


def test_refresh_history_can_cancel_active_refreshes():
    widget_js = refresh_manager_module._WIDGET_JS
    source = getsource(refresh_manager_module.refresh_manager)

    assert 'r.status==="Unknown"' in widget_js
    assert 'data-cancel-history="${esc(r.requestId)}"' in widget_js
    assert 'title="Cancel refresh" aria-label="Cancel refresh"' in widget_js
    assert '${I.close}</button>' in widget_js
    assert 'dispatch("cancel_refresh",{request_id:cancel.dataset.cancelHistory})' in widget_js
    cancel_handler = widget_js.split(
        'root.addEventListener("click",event=>{const cancel=', 1
    )[1].split("},true);", 1)[0]
    assert "event.stopPropagation()" in cancel_handler
    assert "from sempy_labs._refresh_semantic_model import cancel_dataset_refresh" in source
    assert "request_id=target_request_id" in source
    assert 'cancel_refresh(str(data.get("request_id") or "") or None)' in source


def test_completed_refresh_cancel_conflict_is_user_friendly():
    source = getsource(refresh_manager_module.refresh_manager)
    cancel_source = source.split("def cancel_refresh(", 1)[1].split(
        "def save_schedule(", 1
    )[0]

    assert 'status_code == 409 or "409 Conflict" in error_text' in cancel_source
    assert '"cannot be cancelled" in error_text' in cancel_source
    assert '"status": "Completed"' in cancel_source
    assert '"kind": "success"' in cancel_source
    assert "This refresh has already completed and no longer needs" in cancel_source
    assert "if not cancellation_conflict:\n                raise" in cancel_source
    assert "load_history()" in cancel_source


def test_cancel_refresh_uses_reloaded_history_status():
    source = getsource(refresh_manager_module.refresh_manager)
    cancel_source = source.split("def cancel_refresh(", 1)[1].split(
        "def save_schedule(", 1
    )[0]

    assert "refreshed_request = next(" in cancel_source
    assert 'refreshed_status = str(refreshed_request.get("status")' in cancel_source
    assert 'if refreshed_status == "Completed":' in cancel_source
    assert "This refresh completed before the cancellation could be" in cancel_source
    assert '"status": refreshed_status' in cancel_source


def test_widget_has_tools_style_schedule_controls():
    assert "data-reload-schedule" in refresh_manager_module._WIDGET_JS
    assert 'closest("[data-reload-schedule]")' in refresh_manager_module._WIDGET_JS
    assert 'model.get("schedule_loading")' in refresh_manager_module._WIDGET_JS
    assert "slls-rm-save-schedule" in refresh_manager_module._WIDGET_JS
    assert "Saving also enables the schedule." in refresh_manager_module._WIDGET_JS
    assert refresh_manager_module._UI_ICONS["save"] in refresh_manager_module._WIDGET_JS
    assert 'role="switch"' in refresh_manager_module._WIDGET_JS
    assert "data-time-index" in refresh_manager_module._WIDGET_JS
    assert "data-add-time" in refresh_manager_module._WIDGET_JS
    assert "data-remove-time" in refresh_manager_module._WIDGET_JS
    assert "length:48" in refresh_manager_module._WIDGET_JS
    assert "option===time||!d.times.includes(option)" in (
        refresh_manager_module._WIDGET_JS
    )
    assert "keepPosition()" in refresh_manager_module._WIDGET_JS
    assert "restorePosition()" in refresh_manager_module._WIDGET_JS
    assert "function draw(){if(!savedPosition)keepPosition();" in (
        refresh_manager_module._WIDGET_JS
    )
    assert 'root.querySelector(".slls-rm-body")' in (
        refresh_manager_module._WIDGET_JS
    )
    assert 'key:"body"' in refresh_manager_module._WIDGET_JS
    assert 'key==="body"?root.querySelector(".slls-rm-body"):node' in (
        refresh_manager_module._WIDGET_JS
    )
    assert "node.scrollHeight>node.clientHeight" in refresh_manager_module._WIDGET_JS
    assert "node.parentNode||(node.host||null)" in refresh_manager_module._WIDGET_JS
    assert "document.scrollingElement" in refresh_manager_module._WIDGET_JS
    assert "if(target)target.scrollTo(left,top)" in refresh_manager_module._WIDGET_JS
    assert "requestAnimationFrame(()=>{restore();requestAnimationFrame(restore);})" in (
        refresh_manager_module._WIDGET_JS
    )
    assert "let savedPosition=null,positionVersion=0" in (
        refresh_manager_module._WIDGET_JS
    )
    assert "positionVersion++;savedPosition=" in refresh_manager_module._WIDGET_JS
    assert "if(version!==positionVersion)return" in refresh_manager_module._WIDGET_JS
    assert "[50,150,300].forEach(delay=>setTimeout(restore,delay))" in (
        refresh_manager_module._WIDGET_JS
    )


def test_history_and_schedule_panel_headers_toggle_in_place():
    widget_js = refresh_manager_module._WIDGET_JS

    assert "const preservePanelPosition=" in widget_js
    assert 'event.target.closest(".slls-rm-panel")' in widget_js
    assert "if(panel)preservePanelPosition()" in widget_js
    assert "if(savedPosition===positions)restorePosition()" in widget_js
    assert '["mousedown","click","change"].forEach(type=>' in widget_js
    assert 'if(event.target.closest(".slls-rm-panel"))event.stopPropagation()' in (
        widget_js
    )
    assert 'event.target.closest(".slls-rm-panel-head")' in widget_js
    assert 'panelHead.querySelector("[data-panel]")' in widget_js
    assert 'event.target.closest("[data-reload-history],[data-reload-schedule]")' in (
        widget_js
    )
    assert 'panelHead.closest(".slls-rm-panel").classList.toggle("open",open)' in (
        widget_js
    )
    capture_handler = widget_js.split(
        'root.addEventListener("click",event=>{const panelHead=', 1
    )[1].split("},true);", 1)[0]
    assert "event.stopPropagation()" in capture_handler
    assert "draw()" not in capture_handler
    assert '{keepPosition();dispatch("load_history");}' in capture_handler
    assert '{keepPosition();dispatch("load_schedule");}' in capture_handler


def test_widget_uses_semantic_table_icons():
    assert (
        refresh_manager_module._UI_ICONS["calculated_table"]
        in refresh_manager_module._WIDGET_JS
    )
    assert (
        refresh_manager_module._UI_ICONS["calculation_group"]
        in refresh_manager_module._WIDGET_JS
    )
    assert (
        refresh_manager_module._UI_ICONS["field_parameter"]
        in refresh_manager_module._WIDGET_JS
    )
    assert 't.kind==="calculation_group"' in refresh_manager_module._WIDGET_JS
    assert 't.kind==="calculated_table"' in refresh_manager_module._WIDGET_JS
    assert 't.kind==="field_parameter"' in refresh_manager_module._WIDGET_JS


def test_semantic_table_icons_have_hover_labels():
    widget_js = refresh_manager_module._WIDGET_JS

    for label in ["Table", "Calculation Group", "Calculated Table", "Field Parameter"]:
        assert label in widget_js
    assert 'title="${tableKind}" aria-label="${tableKind}"' in widget_js
    assert "tom.is_field_parameter(table_name=str(table.Name))" in getsource(
        refresh_manager_module._get_refresh_objects_tom
    )


def test_refresh_detail_titles_use_neutral_text_color():
    assert (
        ".slls-rm-summary span,.slls-rm-detail-title { display:block;"
        "color:var(--ui-text-secondary)"
    ) in refresh_manager_module._WIDGET_CSS


def test_widget_has_tools_style_refresh_summary():
    for field in ["Refresh type", "Duration", "Table", "Partition", "Status"]:
        assert field in refresh_manager_module._WIDGET_JS
    assert "x.startTime" in refresh_manager_module._WIDGET_JS
    assert "x.endTime" in refresh_manager_module._WIDGET_JS
    assert "x.objects||[]" in refresh_manager_module._WIDGET_JS


def test_widget_has_refresh_policy_and_visualize_toggles():
    assert 'data-option="policy"' in refresh_manager_module._WIDGET_JS
    assert 'data-option="visualize"' in refresh_manager_module._WIDGET_JS
    assert 'aria-label="Toggle apply refresh policy"' in (
        refresh_manager_module._WIDGET_JS
    )
    assert 'aria-label="Toggle visualize refresh"' in refresh_manager_module._WIDGET_JS
    assert "visualize:s.visualize" in refresh_manager_module._WIDGET_JS
    assert "busy&&!s.visualize" in refresh_manager_module._WIDGET_JS


def test_refresh_option_values_use_compact_controls():
    assert "slls-rm-card slls-rm-options" in refresh_manager_module._WIDGET_JS
    assert (
        ".slls-rm-options .slls-rm-input,.slls-rm-options .slls-rm-select"
        in refresh_manager_module._WIDGET_CSS
    )
    assert "min-height:32px;height:32px" in refresh_manager_module._WIDGET_CSS
    assert "font-size:12px" in refresh_manager_module._WIDGET_CSS


def test_refresh_options_have_hover_descriptions():
    widget_js = refresh_manager_module._WIDGET_JS

    for option in ["type", "commit", "parallel", "retry", "policy"]:
        assert f'{option}:"' in widget_js
    assert "const annotateOptionDescriptions=" in widget_js
    assert 'control.closest(".slls-rm-field,.slls-rm-toggle-row")' in widget_js
    assert "field.title=description" in widget_js
    assert "new MutationObserver(()=>{annotateOptionDescriptions();" in widget_js


def test_visualized_refresh_suppresses_empty_trace_warning():
    source = getsource(refresh_manager_module._read_refresh_trace_events)
    assert "warnings.catch_warnings()" in source
    assert 'message="No trace logs have been recorded.*"' in source
    assert "trace.stop() if stop else trace.get_trace_logs()" in source


def test_widget_renders_visualized_refresh_timeline():
    assert 'model.get("gantt_events")' in refresh_manager_module._WIDGET_JS
    assert "Refresh timeline" in refresh_manager_module._WIDGET_JS
    assert "Execute SQL" in refresh_manager_module._WIDGET_JS
    assert "event.durationMs" in refresh_manager_module._WIDGET_JS
    assert "event.cpuTimeMs" in refresh_manager_module._WIDGET_JS


def test_visualized_refresh_timeline_filters_and_scrolls():
    widget_js = refresh_manager_module._WIDGET_JS
    widget_css = refresh_manager_module._WIDGET_CSS

    assert "data-gantt-search" in widget_js
    assert "Search tables and partitions..." in widget_js
    assert "event.tableName,event.partitionName,event.objectName" in widget_js
    assert "row.dataset.filter.includes(query)" in widget_js
    assert 'row.classList.toggle("filtered",!show)' in widget_js
    assert "function applyGanttFilter()" in widget_js
    assert 'model.on("change:gantt_events",updateGantt)' in widget_js
    assert "input.setSelectionRange(start,end)" in widget_js
    assert 'root.querySelector(".slls-rm-gantt-rows")?.scrollTop||0' in widget_js
    assert "if(rows)rows.scrollTop=scrollTop" in widget_js
    assert ".slls-rm-gantt-rows {" in widget_css
    assert "max-height:50vh;overflow-y:auto" in widget_css
    assert ".slls-rm-gantt-row.filtered { display:none; }" in widget_css


def test_visualized_refresh_updates_live_regions_without_full_redraw():
    widget_js = refresh_manager_module._WIDGET_JS

    assert "data-live-status" in widget_js
    assert "data-live-gantt" in widget_js
    assert 'else updateLive("[data-live-status]",status);' in widget_js
    assert 'model.on("change:gantt_events",updateGantt)' in widget_js
    assert 'updateLive("[data-live-gantt]",gantt)' in widget_js
    draw_observers = widget_js.split(
        'const updateLive=(selector,render)=>', 1
    )[1].split('model.on("change:refresh_status"', 1)[0]
    assert '"refresh_status"' not in draw_observers
    assert '"gantt_events"' not in draw_observers


def test_refresh_status_polling_accepts_active_202_responses():
    source = getsource(refresh_manager_module.refresh_manager)
    assert source.count("status_codes=[200, 202]") == 2


def test_refresh_details_include_tools_app_sections():
    for label in [
        "Current type",
        "Commit mode",
        "Initiated by",
        "Attempts",
        "Start",
        "End",
        "Duration",
    ]:
        assert label in refresh_manager_module._WIDGET_JS
    assert "d.refreshAttempts||[]" in refresh_manager_module._WIDGET_JS
    assert "d.objects||[]" in refresh_manager_module._WIDGET_JS
    assert "serviceExceptionJson" in refresh_manager_module._WIDGET_JS
    assert "No attempt details are available." in refresh_manager_module._WIDGET_JS
    assert "No object details are available." in refresh_manager_module._WIDGET_JS


def test_refresh_objects_are_read_from_dmvs(monkeypatch):
    import datetime

    queries = []
    rows = {
        "TMSCHEMA_TABLES": [
            {"TableID": 1, "TableName": "Sales"},
            {"TableID": 2, "TableName": "Calc"},
            {"TableID": 3, "TableName": "Parameter"},
            {"TableID": 4, "TableName": "Calculated"},
        ],
        "TMSCHEMA_PARTITIONS": [
            {
                "TableID": 1,
                "PartitionName": "Sales-2",
                "Mode": 5,
                "Type": 4,
                "RefreshedTime": datetime.datetime(2026, 8, 2, 12, 30),
                "QueryDefinition": "",
            },
            {
                "TableID": 1,
                "PartitionName": "Sales-1",
                "Mode": 0,
                "Type": 4,
                "RefreshedTime": datetime.datetime(1, 1, 1),
                "QueryDefinition": "",
            },
            {"TableID": 2, "PartitionName": "Calc", "Mode": 0, "Type": 7},
            {
                "TableID": 3,
                "PartitionName": "Parameter",
                "Mode": 0,
                "Type": 2,
                "QueryDefinition": "{(\"A\", NAMEOF('Sales'[Amount]), 0)}",
            },
            {
                "TableID": 4,
                "PartitionName": "Calculated",
                "Mode": 0,
                "Type": 2,
                "QueryDefinition": "CALENDARAUTO()",
            },
        ],
        "TMSCHEMA_EXTENDED_PROPERTIES": [
            {"ObjectID": 30, "ObjectType": 4, "Name": "ParameterMetadata"},
            {"ObjectID": 40, "ObjectType": 4, "Name": "Other"},
        ],
        "TMSCHEMA_COLUMNS": [
            {"ColumnID": 30, "TableID": 3},
            {"ColumnID": 40, "TableID": 4},
        ],
    }

    def query(dataset_id, workspace_id, dax):
        queries.append(dax)
        return next(value for key, value in rows.items() if key in dax)

    monkeypatch.setattr(refresh_manager_module, "_query_dmv", query)

    objects = refresh_manager_module._get_refresh_objects("ds", "ws")

    assert [table["name"] for table in objects] == [
        "Calc",
        "Calculated",
        "Parameter",
        "Sales",
    ]
    kinds = {table["name"]: table["kind"] for table in objects}
    assert kinds == {
        "Calc": "calculation_group",
        "Calculated": "calculated_table",
        "Parameter": "field_parameter",
        "Sales": "table",
    }
    sales = next(table for table in objects if table["name"] == "Sales")
    assert sales["partitions"] == [
        {"name": "Sales-1", "mode": "Import", "refreshedTime": None},
        {
            "name": "Sales-2",
            "mode": "DirectLake",
            "refreshedTime": "2026-08-02T12:30:00Z",
        },
    ]
    assert len(queries) == 4


def test_refresh_objects_fall_back_to_tom_when_dmvs_fail(monkeypatch):
    def fail(*_args):
        raise RuntimeError("DMV unavailable")

    monkeypatch.setattr(refresh_manager_module, "_query_dmv", fail)
    monkeypatch.setattr(
        refresh_manager_module,
        "_get_refresh_objects_tom",
        lambda dataset_id, workspace_id: [{"name": "T", "kind": "table"}],
    )

    assert refresh_manager_module._get_refresh_objects("ds", "ws") == [
        {"name": "T", "kind": "table"}
    ]


def _open_refresh_manager(monkeypatch, objects_by_dataset, dataset="ds1"):
    import IPython.display
    import sempy_labs._helper_functions as helpers

    shown = []
    loads = []

    def get_objects(dataset_id, workspace_id):
        loads.append((dataset_id, workspace_id))
        result = objects_by_dataset[dataset_id]
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(IPython.display, "display", shown.append)
    monkeypatch.setattr(
        helpers, "resolve_workspace_name_and_id", lambda workspace: ("WS 1", "ws1")
    )
    monkeypatch.setattr(
        helpers,
        "resolve_dataset_name_and_id",
        lambda dataset, workspace: ("Model 1", "ds1"),
    )
    monkeypatch.setattr(
        refresh_manager_module,
        "_list_picker_workspaces",
        lambda *_args: [{"id": "ws1", "name": "WS 1"}, {"id": "ws2", "name": "WS 2"}],
    )
    monkeypatch.setattr(
        refresh_manager_module,
        "_list_picker_datasets",
        lambda workspace_id: [{"id": f"{workspace_id}-ds", "name": "Model"}],
    )
    monkeypatch.setattr(refresh_manager_module, "_get_refresh_objects", get_objects)
    refresh_manager_module.refresh_manager(dataset=dataset)
    return shown[0], loads


def _dispatch(widget, action, **payload):
    widget.pending_action = {"action": action, **payload}
    widget.run = widget.run + 1


def test_reload_objects_signals_completion_even_when_unchanged(monkeypatch):
    objects = [{"name": "Sales", "kind": "table", "partitions": []}]
    widget, loads = _open_refresh_manager(monkeypatch, {"ds1": objects})

    _dispatch(widget, "load_objects")
    _dispatch(widget, "load_objects")

    assert loads == [("ds1", "ws1"), ("ds1", "ws1")]
    assert widget.objects == objects
    assert widget.objects_version == 2
    assert widget.objects_loading is False


def test_change_model_picker_loads_workspaces_and_models(monkeypatch):
    widget, _loads = _open_refresh_manager(monkeypatch, {"ds1": []})

    assert widget.workspaces == [{"id": "ws1", "name": "WS 1"}]
    assert widget.workspaces_loaded is False
    assert "ws1" not in widget.datasets

    _dispatch(widget, "open_picker", workspace_id="ws1")

    assert [w["id"] for w in widget.workspaces] == ["ws1", "ws2"]
    assert widget.workspaces_loaded is True
    assert widget.datasets["ws1"] == [{"id": "ws1-ds", "name": "Model"}]
    assert widget.picker_loading is False


def test_connect_shows_main_page_before_loading_objects(monkeypatch):
    new_objects = [{"name": "Orders", "kind": "table", "partitions": []}]
    widget, loads = _open_refresh_manager(
        monkeypatch, {"ds1": [], "ws2-ds": new_objects}
    )
    seen_at_load = []
    original_version = widget.objects_version

    def record(change):
        if change["name"] == "objects_loading" and change["new"]:
            seen_at_load.append(
                (widget.connect_done, widget.dataset_name, list(widget.objects))
            )

    widget.observe(record, names=["objects_loading"])

    _dispatch(
        widget,
        "connect",
        workspace_id="ws2",
        dataset_id="ws2-ds",
        workspace_name="WS 2",
        dataset_name="Model",
    )

    # The switch is signalled (and stale objects cleared) before the load.
    assert seen_at_load == [(1, "Model", [])]
    assert loads == [("ws2-ds", "ws2")]
    assert widget.connect_failed == 0
    assert (widget.workspace_id, widget.dataset_name) == ("ws2", "Model")
    assert widget.objects == new_objects
    assert widget.objects_version == original_version + 1

    _dispatch(widget, "load_objects")
    assert loads[-1] == ("ws2-ds", "ws2")


def test_connect_metadata_failure_is_reported_on_main_page(monkeypatch):
    widget, _loads = _open_refresh_manager(
        monkeypatch, {"ds1": [], "ws2-ds": RuntimeError("no access")}
    )

    _dispatch(widget, "connect", workspace_id="ws2", dataset_id="ws2-ds")

    assert widget.connect_done == 1
    assert widget.connect_failed == 0
    assert widget.objects == []
    assert widget.objects_loading is False
    assert "no access" in widget.refresh_status["message"]


def test_connect_without_selection_returns_to_picker(monkeypatch):
    widget, _loads = _open_refresh_manager(monkeypatch, {"ds1": []})

    _dispatch(widget, "connect", workspace_id="ws2", dataset_id="")

    assert widget.connect_done == 0
    assert widget.connect_failed == 1
    assert widget.picker_error == "Select a workspace and semantic model."
    assert (widget.workspace_id, widget.dataset_name) == ("ws1", "Model 1")


def test_refresh_running_state_ends_before_follow_up_reloads(monkeypatch):
    import sempy.fabric
    import sempy_labs._helper_functions as helpers

    busy_during_reload = []

    def history(workspace_id, dataset_id):
        busy_during_reload.append(widget.busy)
        return []

    monkeypatch.setattr(sempy.fabric, "refresh_dataset", lambda **_kwargs: "req-1")
    monkeypatch.setattr(
        helpers,
        "_base_api",
        lambda **_kwargs: {"status": "Completed", "type": "Full"},
    )
    monkeypatch.setattr(refresh_manager_module, "_get_refresh_history", history)
    widget, _loads = _open_refresh_manager(monkeypatch, {"ds1": []})

    _dispatch(widget, "start_refresh", refresh_type="Full")

    assert widget.refresh_status["status"] == "Completed"
    assert busy_during_reload == [False]
    assert widget.busy is False
    assert widget.refresh_id == ""


def test_refresh_button_requires_loaded_tables_and_partitions():
    widget_js = refresh_manager_module._WIDGET_JS

    assert (
        'const objectsReady=()=>!s.connecting&&!objectsBusy()&&(model.get("objects")||[]).length>0;'
        in widget_js
    )
    assert "busy||!objectsReady()||" in widget_js
    assert 'start.onclick=()=>objectsReady()&&dispatch("start_refresh"' in widget_js


def test_objects_reload_button_matches_expand_button_colors():
    widget_css = refresh_manager_module._WIDGET_CSS

    assert (
        ".slls-rm .slls-rm-card-head [data-reload-objects] { background:var(--ui-bg); }"
        in widget_css
    )
    assert (
        ".slls-rm .slls-rm-card-head [data-reload-objects]:hover:not(:disabled) "
        "{ border-color:var(--ui-border-strong);background:var(--ui-bg-secondary); }"
        in widget_css
    )
    assert ".slls-rm-iconbtn:hover { background:var(--ui-bg-secondary);" in widget_css
