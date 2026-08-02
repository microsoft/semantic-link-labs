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
    assert "data-combo-input" in refresh_manager_module._WIDGET_JS
    assert "data-change-model" in refresh_manager_module._WIDGET_JS
    assert "data-fullscreen" in refresh_manager_module._WIDGET_JS
    assert 'event.key==="Tab"&&kind==="dataset"' in refresh_manager_module._WIDGET_JS
    assert 'root.querySelector("[data-picker-connect]").focus()' in (
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


def test_widget_uses_semantic_table_icons():
    assert (
        refresh_manager_module._UI_ICONS["calculated_table"]
        in refresh_manager_module._WIDGET_JS
    )
    assert (
        refresh_manager_module._UI_ICONS["calculation_group"]
        in refresh_manager_module._WIDGET_JS
    )
    assert 't.kind==="calculation_group"' in refresh_manager_module._WIDGET_JS
    assert 't.kind==="calculated_table"' in refresh_manager_module._WIDGET_JS


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


def test_widget_renders_visualized_refresh_timeline():
    assert 'model.get("gantt_events")' in refresh_manager_module._WIDGET_JS
    assert "Refresh timeline" in refresh_manager_module._WIDGET_JS
    assert "Execute SQL" in refresh_manager_module._WIDGET_JS
    assert "event.durationMs" in refresh_manager_module._WIDGET_JS
    assert "event.cpuTimeMs" in refresh_manager_module._WIDGET_JS


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
