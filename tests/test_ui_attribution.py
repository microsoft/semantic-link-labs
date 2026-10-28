"""The "Powered by Semantic Link Labs" attribution must be identical across tools.

Its markup and its styling (tertiary text that turns to the accent color on
hover) are defined once in ``sempy_labs._ui_components``; tools render that
shared attribution rather than writing their own.
"""

from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1] / "src" / "sempy_labs"

TOOLS = (
    "_app.py",
    "_delta_analyzer.py",
    "lakehouse/_lakehouse_manager.py",
    "semantic_model/_bpa.py",
    "semantic_model/_dax_perf.py",
    "semantic_model/_direct_lake_manager.py",
    "semantic_model/_find_unused_objects.py",
    "semantic_model/_lineage_view.py",
    "semantic_model/_mini_model_manager.py",
    "semantic_model/_model_comparison.py",
    "semantic_model/_perspective_editor.py",
    "semantic_model/_refresh_manager.py",
    "semantic_model/_vertipaq_analyzer.py",
)


def test_attribution_link_turns_to_the_accent_color_on_hover():
    from sempy_labs._ui_components import ATTRIBUTION_CSS, scoped_attribution_css

    assert ".sl-attribution a:hover {\n    color: var(--ui-accent);" in ATTRIBUTION_CSS
    assert ".slls-x .sl-attribution a:hover {" in scoped_attribution_css(".slls-x")


def test_only_ui_components_writes_the_attribution():
    offenders = [
        str(path.relative_to(ROOT))
        for path in ROOT.rglob("*.py")
        if path.name != "_ui_components.py"
        and "Powered by" in path.read_text(encoding="utf-8")
    ]

    assert offenders == []


@pytest.mark.parametrize("tool", TOOLS)
def test_tools_use_the_shared_attribution(tool):
    source = (ROOT / tool).read_text(encoding="utf-8")

    assert "render_attribution_html" in source
    assert "scoped_attribution_css" in source
