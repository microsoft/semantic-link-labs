import ast
from pathlib import Path

ROOT = Path(__file__).parents[1]
WRAPPER_PATH = ROOT / "src" / "sempy_labs" / "_vertipaq.py"
ANALYZER_PATH = ROOT / "src" / "sempy_labs" / "semantic_model" / "_vertipaq_analyzer.py"
UI_PATH = ROOT / "src" / "sempy_labs" / "_ui_components.py"


def _function(path: Path, name: str) -> ast.FunctionDef:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    )


def test_vertipaq_dataset_is_optional_when_visualized():
    for path in (WRAPPER_PATH, ANALYZER_PATH):
        function = _function(path, "vertipaq_analyzer")
        arguments = [argument.arg for argument in function.args.args]
        defaults = dict(
            zip(arguments[-len(function.args.defaults) :], function.args.defaults)
        )

        assert isinstance(defaults["dataset"], ast.Constant)
        assert defaults["dataset"].value is None
        assert isinstance(defaults["visualize"], ast.Constant)
        assert defaults["visualize"].value is True


def test_vertipaq_datasetless_call_bootstraps_existing_picker():
    source = ANALYZER_PATH.read_text(encoding="utf-8")

    assert "if dataset is None:" in source
    assert "_show_vertipaq_picker(" in source
    assert "read_stats_from_data=read_stats_from_data" in source
    assert "picker_initial=True" in source
    assert "picker_initial = traitlets.Bool(False).tag(sync=True)" in source
    assert 'const initialPicker = model.get("picker_initial") === true;' in source
    assert '" vpx-picker-only" if picker_initial else ""' in source


def test_initial_vertipaq_picker_is_inline_not_modal():
    source = ANALYZER_PATH.read_text(encoding="utf-8")
    initial_start = source.index(
        "        if picker_initial:", source.index("if can_pick:")
    )
    modal_start = source.index("        else:", initial_start)
    initial_markup = source[initial_start:modal_start]
    modal_markup = source[
        modal_start : source.index('    html_parts.append("</div>")', modal_start)
    ]

    assert '"vpx-picker-screen vpx-picker-dialog"' in source
    assert '"vpx-delta-dialog vpx-picker-dialog"' in modal_markup
    assert '"vpx-picker-panel"' in source
    assert ".vpx-{uid} .vpx-picker-screen {{" in source
    assert ".vpx-{uid}.vpx-picker-only .vpx-container > :not(.vpx-header)" in source
    assert "picker_dialog_icon" not in initial_markup
    assert "Connect to a semantic model" in initial_markup
    assert 'class="vpx-picker-fields"' in initial_markup
    assert 'class="vpx-picker-actions"' in initial_markup


def test_vertipaq_nonvisual_call_requires_dataset_and_skips_rendering():
    source = ANALYZER_PATH.read_text(encoding="utf-8")

    assert "The 'dataset' parameter is required when visualize=False." in source
    assert "if export is None and visualize:" in source
    assert "if export is None:\n        return final_dict" in source


def test_vertipaq_picker_uses_shared_searchable_selects():
    source = ANALYZER_PATH.read_text(encoding="utf-8")

    assert "SEARCH_SELECT_CSS as _UI_SEARCH_SELECT_CSS" in source
    assert "SEARCH_SELECT_JS as _UI_SEARCH_SELECT_JS" in source
    assert '_VPX_WIDGET_JS = _UI_SEARCH_SELECT_JS + "\\n"' in source
    assert "const wsPicker = createSearchSelect({" in source
    assert "const dsPicker = createSearchSelect({" in source
    assert '<select class="vpx-picker-select' not in source


def test_change_model_modal_reserves_height_for_searchable_lists():
    source = ANALYZER_PATH.read_text(encoding="utf-8")

    assert "height: min(720px, calc(100vh - 32px));" in source
    assert "max-height: calc(100vh - 32px);" in source
    assert ".vpx-{uid} .vpx-picker-modal > .vpx-delta-modal-body" in source
    assert "flex: 1 1 auto;" in source
    assert "min-height: 260px;" in source
    assert "max-height: min(360px, calc(100vh - 300px));" in source


def test_connect_reveals_main_ui_with_progress_in_shared_root():
    source = ANALYZER_PATH.read_text(encoding="utf-8")
    connect_start = source.index('connectBtn.addEventListener("click"')
    connect_end = source.index("        });", connect_start) + len("        });")
    connect_handler = source[connect_start:connect_end]

    assert "setAnalyzing(true);" in connect_handler
    assert 'root.classList.remove("vpx-picker-only")' in source
    assert 'picker.style.display = "none"' in source
    assert 'class="vpx-analysis-progress" role="progressbar"' in source
    assert ".vpx-{uid} .vpx-analysis-progress.vpx-active" in source
    assert 'root.setAttribute("aria-busy", on ? "true" : "false")' in source


def test_analysis_progress_stays_inside_fullscreen_root():
    source = ANALYZER_PATH.read_text(encoding="utf-8")
    root_append = source.index("html_parts.append(f'<div class=\"{root_classes}\">')")
    root_close = source.index('html_parts.append("</div>")  # root', root_append)
    root_markup = source[root_append:root_close]

    assert "vpx-analysis-progress" in root_markup
    assert ".vpx-{uid}.vpx-fs" in source
    assert "if (on) root.classList.remove" in source


def test_initial_picker_remains_visible_in_fullscreen():
    source = ANALYZER_PATH.read_text(encoding="utf-8")

    assert ".vpx-{uid}.vpx-picker-only.vpx-fs .vpx-container" in source
    assert ".vpx-{uid}.vpx-picker-only:fullscreen .vpx-container" in source
    assert ".vpx-{uid}.vpx-picker-only:-webkit-full-screen .vpx-container" in source
    assert ".vpx-{uid}.vpx-picker-only:fullscreen .vpx-picker-screen" in source
    assert "flex: 0 0 auto;\n        min-height: 0;" in source
    assert "flex: 1 1 auto;\n        min-height: 0;\n        overflow: auto;" in source


def test_shared_header_keeps_theme_button_rightmost():
    source = UI_PATH.read_text(encoding="utf-8")
    start = source.index("def render_header_html(")
    end = source.index("\ndef theme_toggle_script(", start)
    renderer = source[start:end]
    theme_append = renderer.index("# The theme button is appended last")

    assert renderer.count("if fullscreen_btn_id:") == 1
    assert renderer.index("if fullscreen_btn_id:") < theme_append
    assert "if fullscreen_btn_id:" not in renderer[theme_append:]
