import ast
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, Optional
from uuid import UUID

import pandas as pd
import pytest

SOURCE_PATH = Path(__file__).parents[1] / "src" / "sempy_labs" / "_delta_analyzer.py"
UI_SOURCE_PATH = Path(__file__).parents[1] / "src" / "sempy_labs" / "_ui_components.py"


def _load_delta_analyzer(visualize):
    tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "delta_analyzer"
    )
    namespace = {
        "Dict": Dict,
        "Optional": Optional,
        "UUID": UUID,
        "pd": pd,
        "log": lambda function: function,
        "icons": SimpleNamespace(red_dot="error"),
        "_visualize_delta_analyzer": visualize,
    }
    exec(compile(ast.Module([function], []), str(SOURCE_PATH), "exec"), namespace)
    return namespace["delta_analyzer"]


def test_datasetless_delta_analyzer_opens_picker():
    calls = []
    delta_analyzer = _load_delta_analyzer(lambda **kwargs: calls.append(kwargs))

    result = delta_analyzer(table_name=None, visualize=True, dark_mode=True)

    assert result == {}
    assert len(calls) == 1
    assert calls[0]["initial_dataframes"] is None
    assert calls[0]["table_name"] is None
    assert calls[0]["dark_mode"] is True


def test_datasetless_nonvisual_delta_analyzer_requires_table():
    delta_analyzer = _load_delta_analyzer(lambda **kwargs: None)

    with pytest.raises(ValueError, match="table_name.*required"):
        delta_analyzer(table_name=None, visualize=False)


def test_delta_visualization_adapter_and_builder_are_defined():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    assert "def _visualize_delta_analyzer(" in source
    assert "def _list_delta_picker_workspaces()" in source
    assert "def _list_delta_picker_lakehouses(" in source
    assert "def _list_delta_picker_tables(" in source
    builder_start = source.index("def _build_delta_analyzer_html(")
    adapter_start = source.index("def _visualize_delta_analyzer(")
    builder = source[builder_start:adapter_start]
    assert "return full_html + theme_script" in builder
    assert "fullscreen_script" not in builder
    assert "display(HTML(full_html" not in builder


def test_delta_picker_has_usable_height_and_search_list_space():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    assert "min-height: min(680px, calc(100vh - 32px));" in source
    assert ".slls-da-panel { flex: 1 1 auto; min-height: 480px;" in source
    assert "max-height: min(360px, calc(100vh - 290px));" in source
    assert ".slls-da-picker:fullscreen .slls-da-panel" in source


def test_delta_anywidget_fullscreen_fallback_covers_viewport():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    fallback_start = source.index(".slls-da-picker.slls-da-fullscreen {")
    fallback_end = source.index("}", fallback_start)
    fallback = source[fallback_start:fallback_end]

    assert "position: fixed; inset: 0; z-index: 2147483000;" in fallback
    assert "width: 100vw; height: 100vh;" in fallback


def test_delta_picker_has_fullscreen_and_rightmost_theme_controls():
    source = SOURCE_PATH.read_text(encoding="utf-8")
    header_start = source.index('const shellHeader = document.createElement("div")')
    header_end = source.index("const panel = document.createElement", header_start)
    header = source[header_start:header_end]

    assert 'fullscreenBtn.className = "sl-theme-btn"' in header
    assert 'themeBtn.className = "sl-theme-btn"' in header
    assert (
        "header.append(titleIcon, titleWrap, spacer, fullscreenBtn, themeBtn)" in header
    )
    assert "sllsDaSetupFullscreen(" in header
    assert 'model.set("dark_mode"' in header


def test_delta_picker_dark_mode_paints_root_and_panel_backgrounds():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    assert ".slls-da-picker.slls-da-dark { __DARK_VARS__ }" in source
    assert "background: var(--ui-bg); border: 1px solid var(--ui-border);" in source
    assert ".slls-da-shell-header {" in source
    assert "background: var(--ui-bg);" in source
    assert ".slls-da-panel { flex: 1 1 auto; min-height: 480px;" in source


def test_delta_picker_analyze_immediately_shows_main_loading_shell():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    assert 'const loadingShell = document.createElement("div")' in source
    assert 'loadingShell.className = "slls-da-loading"' in source
    assert "content.append(loadingShell, resultsProgress, results)" in source
    assert (
        "const showLoading = !pickerOpen && (analysisRequested || analyzing);" in source
    )
    assert 'pickerBackdrop.style.display = pickerOpen ? "flex" : "none";' in source
    assert (
        'loadingShell.classList.toggle("slls-da-active", showLoading && !hasResults);'
        in source
    )
    assert (
        'pickerOpen = false; analysisRequested = true; renderState(); dispatch("run_analysis_trigger")'
        in source
    )


def test_delta_results_change_button_is_larger_and_beside_tool_name():
    delta_source = SOURCE_PATH.read_text(encoding="utf-8")
    ui_source = UI_SOURCE_PATH.read_text(encoding="utf-8")

    title_row_start = ui_source.index("parts.append('<div class=\"sl-title-row\">')")
    title_start = ui_source.index(
        'parts.append(f\'<div class="sl-title">', title_row_start
    )
    change_start = ui_source.index("if picker_btn_id:", title_start)
    title_row_end = ui_source.index('parts.append("</div>")  # title row', change_start)

    assert title_row_start < title_start < change_start < title_row_end
    assert "width: 32px;\n    height: 32px;" in ui_source
    assert 'title="Change table / workspace"' in ui_source
    assert "margin: 0 auto 24px;" in delta_source


def test_delta_analyzer_uses_vertipaq_delta_button_icon():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    assert 'title_icon=_UI_ICONS["delta_stats"]' in source
    assert '.replace("__DELTA_ICON__", _UI_ICONS["delta_stats"])' in source


def test_delta_results_fullscreen_is_owned_by_anywidget():
    source = SOURCE_PATH.read_text(encoding="utf-8")
    ui_source = UI_SOURCE_PATH.read_text(encoding="utf-8")

    assert "class DeltaAnalyzerWidget(anywidget.AnyWidget):" in source
    assert "_ui_display_html_widget" not in source
    assert "The Delta Analyzer visualization requires 'anywidget'." in source
    assert (
        'resultsFullscreenBtn.addEventListener("click", event => { '
        "event.preventDefault(); fullscreenBtn.click(); })" in source
    )
    assert source.count("sllsDaSetupFullscreen(root,") == 1
    assert '.slls-da-picker:fullscreen .slls-da-results > [class*="-root"]' in source
    enter_start = ui_source.index("function enterFullscreen()")
    request_start = ui_source.index("root.requestFullscreen()", enter_start)
    immediate_fallback = ui_source.index("cssFullscreen = true;", enter_start)
    assert immediate_fallback < request_start


def test_delta_analysis_shows_full_dashboard_layout_with_progress():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    assert 'loadingCards.className = "slls-da-loading-cards"' in source
    assert 'loadingTabs.className = "slls-da-loading-tabs"' in source
    assert 'loadingToolbar.className = "slls-da-loading-toolbar"' in source
    assert 'loadingTable.className = "slls-da-loading-table"' in source
    assert (
        "loadingShell.append(progress, loadingCards, loadingTabs, loadingToolbar, loadingTable)"
        in source
    )
    assert 'results.style.display = hasResults ? "" : "none";' in source
    assert (
        'resultsProgress.classList.toggle("slls-da-active", showLoading && hasResults);'
        in source
    )


def test_delta_change_picker_opens_as_modal_over_results():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    assert 'pickerBackdrop.className = "slls-da-picker-backdrop"' in source
    assert "const pickerModal = pickerOpen && hasResults;" in source
    assert 'pickerBackdrop.classList.toggle("slls-da-modal", pickerModal);' in source
    assert 'closePicker.className = "slls-da-close"' in source
    assert 'closePicker.setAttribute("aria-label", "Close table picker")' in source
    assert 'closePicker.addEventListener("click"' in source
    assert "slls-da-cancel" not in source
    assert ".slls-da-picker-backdrop.slls-da-modal {" in source
    assert 'content.style.display = pickerOpen && !pickerModal ? "none" : "";' in source


def test_delta_opening_subtitle_and_fullscreen_modal_height_are_compact():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    assert "Preparing analysis" not in source
    assert 'shellSubtitle.textContent = pickerOpen ? ""' in source
    assert (
        ".slls-da-picker:fullscreen .slls-da-picker-backdrop.slls-da-modal .slls-da-panel"
        in source
    )
    assert "min-height: 0; max-height: calc(100vh - 48px);" in source


def test_delta_sticky_headers_stay_above_scrolling_bar_values():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    table_start = source.index(".da-{uid}-table {{")
    table_end = source.index("}}", table_start)
    table_css = source[table_start:table_end]
    thead_start = source.index(".da-{uid}-table thead {{")
    thead_end = source.index("}}", thead_start)
    thead_css = source[thead_start:thead_end]
    header_start = source.index(".da-{uid}-table thead th {{")
    header_end = source.index("}}", header_start)
    header_css = source[header_start:header_end]
    bar_cell_start = source.index("tbody td.da-{uid}-bar-cell {{")
    bar_cell_end = source.index("}}", bar_cell_start)
    bar_cell_css = source[bar_cell_start:bar_cell_end]

    assert "border-collapse: separate;" in table_css
    assert "border-spacing: 0;" in table_css
    assert "position: sticky;" in thead_css
    assert "z-index: 10;" in thead_css
    assert "isolation: isolate;" in thead_css
    assert "position: relative;" in header_css
    assert "z-index: 0;" in bar_cell_css
    assert "isolation: isolate;" in bar_cell_css
