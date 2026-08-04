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
    assert "Choose a semantic model" in refresh_manager_module._WIDGET_JS
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


def test_no_dataset_picker_renders_before_discovery():
    source = getsource(refresh_manager_module.refresh_manager)

    assert 'fabric.list_datasets(' in source
    assert 'workspace=target_workspace_id, mode="rest"' in source
    assert 'initial_workspaces = [{"id": workspace_id, "name": workspace_name}]' in source
    assert "initial_datasets = {}" in source
    display_index = source.index("display(widget)")
    discovery_index = source.index(
        "threading.Thread(target=load_initial_workspaces, daemon=True).start()"
    )
    assert display_index < discovery_index
    assert "threading.Thread(target=load_initial_datasets, daemon=True).start()" in source


def test_fullscreen_and_theme_buttons_use_neutral_icon_style():
    assert "slls-rm-header-action" not in refresh_manager_module._WIDGET_JS
    assert "slls-rm-header-action" not in refresh_manager_module._WIDGET_CSS
    assert 'class="slls-rm-iconbtn" data-fullscreen' in (
        refresh_manager_module._WIDGET_JS
    )
    assert 'class="slls-rm-iconbtn" data-theme' in refresh_manager_module._WIDGET_JS
    assert ".slls-rm-title-row .slls-rm-iconbtn" not in (
        refresh_manager_module._WIDGET_CSS
    )
    assert ".slls-rm-iconbtn { width:34px;height:34px" in (
        refresh_manager_module._WIDGET_CSS
    )
    assert "data-combo-input" in refresh_manager_module._WIDGET_JS
    assert "data-change-model" in refresh_manager_module._WIDGET_JS
    assert "data-fullscreen" in refresh_manager_module._WIDGET_JS
    assert 'event.key==="Tab"&&kind==="dataset"' in refresh_manager_module._WIDGET_JS
    assert 'if(connect&&!connect.disabled)connect.focus()' in (
        refresh_manager_module._WIDGET_JS
    )
    assert "choice=exact||(visible.length===1?visible[0]:null)" in (
        refresh_manager_module._WIDGET_JS
    )
    assert "if(choice&&!s.pickDs)" in refresh_manager_module._WIDGET_JS
    assert "onChoose(choice.dataset.id)" in refresh_manager_module._WIDGET_JS
    assert "s.pickDs=id;draw();requestAnimationFrame" in (
        refresh_manager_module._WIDGET_JS
    )
    assert 'class="slls-rm-iconbtn" data-picker-reload' in (
        refresh_manager_module._WIDGET_JS
    )
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
    assert "node.scrollTo(left,top)" in refresh_manager_module._WIDGET_JS
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
        refresh_manager_module._get_refresh_objects
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
    assert "new MutationObserver(annotateOptionDescriptions)" in widget_js


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
    assert 'model.on("change:refresh_status",()=>updateLive(' in widget_js
    assert 'model.on("change:gantt_events",()=>updateLive(' in widget_js
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
