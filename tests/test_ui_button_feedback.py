from pathlib import Path

from sempy_labs._ui_components import BUTTON_PRESS_CSS, scoped_button_press_css

ROOT = Path(__file__).parents[1]


def test_button_press_css_provides_immediate_enabled_control_feedback():
    assert "button:not(:disabled):active" in BUTTON_PRESS_CSS
    assert '[role="button"]:not([aria-disabled="true"]):active' in BUTTON_PRESS_CSS
    assert "transform: scale(0.96)" in BUTTON_PRESS_CSS
    assert "filter: brightness(0.9)" in BUTTON_PRESS_CSS
    assert "transform-origin: center" in BUTTON_PRESS_CSS
    assert "transition:" not in BUTTON_PRESS_CSS


def test_button_press_css_is_scoped_to_widget_root():
    css = scoped_button_press_css(".example-tool")

    assert ".example-tool button:not(:disabled)" in css
    assert '.example-tool [role="button"]:not([aria-disabled="true"]):active' in css


def test_all_interactive_tools_use_shared_button_press_feedback():
    integrations = {
        "src/sempy_labs/semantic_model/_bpa.py": (
            '_ui_scoped_button_press_css(".slls-bpa")'
        ),
        "src/sempy_labs/semantic_model/_direct_lake_manager.py": (
            '_ui_scoped_button_press_css(".slls-dle")'
        ),
        "src/sempy_labs/semantic_model/_direct_lake_migration.py": (
            '_ui_scoped_button_press_css(".slls-mdl")'
        ),
        "src/sempy_labs/semantic_model/_find_unused_objects.py": (
            '_ui_scoped_button_press_css(".fuo")'
        ),
        "src/sempy_labs/semantic_model/_lineage_view.py": (
            '_ui_scoped_button_press_css(".slls-lv")'
        ),
        "src/sempy_labs/semantic_model/_perspective_editor.py": (
            '_ui_scoped_button_press_css(".slls-pe")'
        ),
        "src/sempy_labs/semantic_model/_refresh_manager.py": (
            '_ui_scoped_button_press_css(".slls-rm")'
        ),
        "src/sempy_labs/_delta_analyzer.py": (
            "_ui_scoped_button_press_css(root_selector)"
        ),
        "src/sempy_labs/semantic_model/_vertipaq_analyzer.py": (
            "_ui_scoped_button_press_css(root_selector)"
        ),
        "src/sempy_labs/_copilot.py": (
            'scoped_button_press_css(f"#chat-wrapper-{session_id}")'
        ),
        "src/sempy_labs/_model_bpa.py": "scoped_button_press_css(f'#{root_id}')",
        "src/sempy_labs/report/_report_bpa.py": (
            "scoped_button_press_css(f'#{root_id}')"
        ),
    }

    for relative_path, expected in integrations.items():
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        assert expected in source, relative_path
