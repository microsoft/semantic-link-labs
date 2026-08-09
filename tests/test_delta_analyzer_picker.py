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
    assert "return full_html + theme_script + fullscreen_script" in builder
    assert "display(HTML(full_html" not in builder


def test_delta_picker_has_usable_height_and_search_list_space():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    assert "min-height: min(680px, calc(100vh - 32px));" in source
    assert ".slls-da-panel { flex: 1 1 auto; min-height: 480px;" in source
    assert "max-height: min(360px, calc(100vh - 290px));" in source
    assert ".slls-da-picker:fullscreen .slls-da-panel" in source


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
    assert "content.append(loadingShell, results)" in source
    assert (
        "const showLoading = !pickerOpen && (analysisRequested || analyzing);" in source
    )
    assert 'panel.style.display = pickerOpen ? "" : "none";' in source
    assert 'loadingShell.classList.toggle("slls-da-active", showLoading);' in source
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
