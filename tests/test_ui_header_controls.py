"""The standard header controls must be identical across every interactive tool.

The light/dark, full-screen, change model/workspace and reload buttons are
defined once in ``sempy_labs._ui_components``; tools reference the shared
classes rather than styling their own.
"""

from pathlib import Path

import pytest


ROOT = Path(__file__).parents[1] / "src" / "sempy_labs"
UI_COMPONENTS = ROOT / "_ui_components.py"

# Tool module -> the shared control classes it is expected to reference.
TOOLS = {
    "semantic_model/_dax_perf.py": ("sl-theme-btn", "sl-change-btn", "sl-reload-btn"),
    "semantic_model/_bpa.py": ("sl-theme-btn", "sl-change-btn", "sl-reload-btn"),
    "semantic_model/_refresh_manager.py": (
        "sl-theme-btn",
        "sl-change-btn",
        "sl-reload-btn",
    ),
    "semantic_model/_vertipaq_analyzer.py": ("sl-change-btn", "sl-reload-btn"),
    "semantic_model/_perspective_editor.py": (
        "sl-theme-btn",
        "sl-change-btn",
        "sl-reload-btn",
    ),
    "semantic_model/_lineage_view.py": (
        "sl-theme-btn",
        "sl-change-btn",
        "sl-reload-btn",
    ),
    "semantic_model/_find_unused_objects.py": (
        "sl-theme-btn",
        "sl-change-btn",
        "sl-reload-btn",
    ),
    "_delta_analyzer.py": ("sl-theme-btn",),
}

CONTROL_CLASSES = ("sl-theme-btn", "sl-change-btn", "sl-reload-btn")


def _read(relative_path):
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_ui_components_defines_the_standard_controls():
    source = _read("_ui_components.py")

    assert ".sl-theme-btn {" in source
    assert ".sl-change-btn {" in source
    assert ".sl-reload-btn {" in source
    # Same footprint for every control, matching the DAX Perf Optimizer.
    assert source.count("    width: 32px;\n    height: 32px;") >= 3
    assert ".sl-theme-btn svg { display: block; width: 18px; height: 18px; }" in source
    assert ".sl-change-btn svg { display: block; width: 18px; height: 18px; }" in source
    assert ".sl-reload-btn svg { display: block; width: 14px; height: 14px; }" in source
    assert ".sl-reload-btn.sl-spinning svg { animation: sl-spin" in source


@pytest.mark.parametrize("module", sorted(TOOLS))
def test_tools_do_not_restyle_the_shared_controls(module):
    source = _read(module)

    for control in CONTROL_CLASSES:
        assert f".{control} {{" not in source, (
            f"{module} restyles .{control}; the shared definition in "
            "sempy_labs._ui_components is the single source of truth."
        )


@pytest.mark.parametrize("module,controls", sorted(TOOLS.items()))
def test_tools_use_the_shared_control_classes(module, controls):
    source = _read(module)

    for control in controls:
        assert control in source


@pytest.mark.parametrize(
    "module",
    [
        "semantic_model/_bpa.py",
        "semantic_model/_refresh_manager.py",
        "semantic_model/_perspective_editor.py",
        "semantic_model/_lineage_view.py",
        "semantic_model/_find_unused_objects.py",
    ],
)
def test_tools_inject_the_shared_header_css(module):
    source = _read(module)

    assert "scoped_header_css" in source


def test_pickers_default_to_the_current_workspace():
    # resolve_workspace_name_and_id(None) resolves to the current workspace, so
    # it must be called even when no workspace/dataset was supplied.
    for module in (
        "semantic_model/_dax_perf.py",
        "semantic_model/_vertipaq_analyzer.py",
        "semantic_model/_bpa.py",
        "semantic_model/_refresh_manager.py",
        "semantic_model/_perspective_editor.py",
        "semantic_model/_lineage_view.py",
        "semantic_model/_find_unused_objects.py",
    ):
        assert "resolve_workspace_name_and_id" in _read(module)

    dax = _read("semantic_model/_dax_perf.py")
    assert "elif workspace is not None:" not in dax
    assert "model picker pre-selects the current workspace" in dax

    vertipaq = _read("semantic_model/_vertipaq_analyzer.py")
    picker = vertipaq[vertipaq.index("def _show_vertipaq_picker") :]
    picker = picker[: picker.index("def visualize_vertipaq")]
    assert "if workspace is not None:" not in picker
    assert "Always resolve so the picker opens on the current workspace." in picker
