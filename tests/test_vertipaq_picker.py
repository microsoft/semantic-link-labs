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

    assert '"vpx-picker-screen vpx-picker-dialog"' in source
    assert 'else "vpx-delta-dialog vpx-picker-dialog"' in source
    assert '"vpx-picker-panel"' in source
    assert ".vpx-{uid} .vpx-picker-screen {{" in source
    assert ".vpx-{uid}.vpx-picker-only .vpx-container > :not(.vpx-header)" in source


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


def test_shared_header_keeps_theme_button_rightmost():
    source = UI_PATH.read_text(encoding="utf-8")
    start = source.index("def render_header_html(")
    end = source.index("\ndef theme_toggle_script(", start)
    renderer = source[start:end]
    theme_append = renderer.index("# The theme button is appended last")

    assert renderer.count("if fullscreen_btn_id:") == 1
    assert renderer.index("if fullscreen_btn_id:") < theme_append
    assert "if fullscreen_btn_id:" not in renderer[theme_append:]
