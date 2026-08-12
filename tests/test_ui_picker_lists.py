"""Tests for the shared workspace / semantic model picker lists.

Every interactive tool opens on a workspace + semantic model picker. The lists
behind those pickers are built by ``sempy_labs._ui_components`` so a single,
reliable code path serves all of them (the per-tool implementations used to
call ``fabric.list_workspaces()``, which is unreliable in Fabric PySpark
notebooks and left the pickers empty).
"""

import importlib.util
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

SRC = Path(__file__).resolve().parents[1] / "src" / "sempy_labs"

# The tools whose pickers must go through the shared helpers.
PICKER_TOOLS = [
    SRC / "_delta_analyzer.py",
    SRC / "semantic_model" / "_bpa.py",
    SRC / "semantic_model" / "_direct_lake_manager.py",
    SRC / "semantic_model" / "_direct_lake_migration.py",
    SRC / "semantic_model" / "_find_unused_objects.py",
    SRC / "semantic_model" / "_lineage_view.py",
    SRC / "semantic_model" / "_mini_model_manager.py",
    SRC / "semantic_model" / "_perspective_editor.py",
    SRC / "semantic_model" / "_refresh_manager.py",
    SRC / "semantic_model" / "_vertipaq_analyzer.py",
]


def _ui_components():
    """Load the shared module on its own.

    ``sempy_labs/__init__.py`` pulls in semantic link and the Azure SDK, neither
    of which the helpers need: they import lazily inside the functions. Loading
    the module directly keeps these tests runnable anywhere.
    """

    spec = importlib.util.spec_from_file_location(
        "_ui_components_under_test", SRC / "_ui_components.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def ui():
    return _ui_components()


# ---------------------------------------------------------------------------
# Source-level guards (no semantic link required)
# ---------------------------------------------------------------------------
def test_shared_picker_helpers_exist():
    source = (SRC / "_ui_components.py").read_text(encoding="utf-8")

    assert "def list_picker_workspaces(" in source
    assert "def list_picker_datasets(" in source
    assert "def list_picker_lakehouses(" in source
    # The Fabric REST endpoints are queried first; semantic link is the fallback.
    assert '_picker_items_from_api("/v1/workspaces")' in source
    assert 'f"/v1/workspaces/{workspace_id}/semanticModels"' in source
    assert 'f"/v1/workspaces/{workspace_id}/lakehouses"' in source
    assert 'client="fabric_sp"' in source


@pytest.mark.parametrize("path", PICKER_TOOLS, ids=lambda p: p.name)
def test_tools_do_not_build_picker_lists_themselves(path):
    """No tool may fall back to its own ``fabric.list_workspaces()`` picker."""

    source = path.read_text(encoding="utf-8")

    assert "fabric.list_workspaces()" not in source
    assert "list_picker_workspaces" in source


@pytest.mark.parametrize("path", PICKER_TOOLS, ids=lambda p: p.name)
def test_tools_seed_pickers_before_display(path):
    """Picker traits assigned after ``display()`` race the comm handshake.

    In Fabric PySpark notebooks that update never reaches the browser, so the
    lists must be passed as initial widget state instead.
    """

    source = path.read_text(encoding="utf-8")
    if "display(widget)" not in source:
        pytest.skip("does not display an anywidget")
    display_index = source.index("display(widget)")
    after_display = source[display_index:]

    assert "widget.workspaces =" not in after_display
    assert "widget.datasets =" not in after_display
    assert "widget.available_workspaces =" not in after_display


def test_refresh_manager_seeds_the_full_workspace_list():
    source = (SRC / "semantic_model" / "_refresh_manager.py").read_text(
        encoding="utf-8"
    )

    seed_index = source.index("initial_workspaces = (")
    display_index = source.index("display(widget)")
    assert seed_index < display_index
    assert "else list_workspaces()" in source
    assert "load_initial_workspaces" not in source


def test_delta_analyzer_reports_an_empty_workspace_list():
    """An empty list must not leave the picker showing "Loading workspaces…"."""

    source = (SRC / "_delta_analyzer.py").read_text(encoding="utf-8")

    assert (
        'ws.setEmptyLabel(loading ? "Loading workspaces…" : "No workspaces");' in source
    )


# ---------------------------------------------------------------------------
# Behavioural tests
# ---------------------------------------------------------------------------
@pytest.fixture
def fake_fabric(ui, monkeypatch):
    """Install a stub ``sempy.fabric`` so the fallback path can be exercised.

    Depends on ``ui`` so the real package is imported before it is shadowed.
    """

    fabric = types.ModuleType("sempy.fabric")
    sempy = types.ModuleType("sempy")
    sempy.fabric = fabric
    monkeypatch.setitem(sys.modules, "sempy", sempy)
    monkeypatch.setitem(sys.modules, "sempy.fabric", fabric)
    return fabric


def test_workspaces_prefer_the_rest_api(ui, monkeypatch):
    calls = []

    def fake_api(request):
        calls.append(request)
        return [{"id": "2", "name": "Zeta"}, {"id": "1", "name": "alpha"}]

    monkeypatch.setattr(ui, "_picker_items_from_api", fake_api)

    # Sorted case-insensitively so the picker reads naturally.
    assert ui.list_picker_workspaces() == [
        {"id": "1", "name": "alpha"},
        {"id": "2", "name": "Zeta"},
    ]
    assert calls == ["/v1/workspaces"]


def test_workspaces_fall_back_to_semantic_link(ui, monkeypatch, fake_fabric):

    def boom(request):
        raise RuntimeError("REST unavailable")

    monkeypatch.setattr(ui, "_picker_items_from_api", boom)
    fake_fabric.list_workspaces = lambda: pd.DataFrame(
        {"Id": ["b", "a"], "Name": ["Beta", "Alpha"]}
    )

    assert ui.list_picker_workspaces() == [
        {"id": "a", "name": "Alpha"},
        {"id": "b", "name": "Beta"},
    ]


def test_workspaces_fall_back_to_the_current_workspace(ui, monkeypatch, fake_fabric):

    def boom(*args, **kwargs):
        raise RuntimeError("nothing works here")

    monkeypatch.setattr(ui, "_picker_items_from_api", boom)
    fake_fabric.list_workspaces = boom

    # A usable picker beats an empty one when both lookups fail.
    assert ui.list_picker_workspaces("wid", "My Workspace") == [
        {"id": "wid", "name": "My Workspace"}
    ]
    assert ui.list_picker_workspaces() == []


def test_datasets_use_the_semantic_models_endpoint(ui, monkeypatch):
    calls = []

    def fake_api(request):
        calls.append(request)
        return [{"id": "d1", "name": "Model"}]

    monkeypatch.setattr(ui, "_picker_items_from_api", fake_api)

    assert ui.list_picker_datasets("ws1") == [{"id": "d1", "name": "Model"}]
    assert calls == ["/v1/workspaces/ws1/semanticModels"]
    # Without a workspace there is nothing to list.
    assert ui.list_picker_datasets("") == []


def test_datasets_fall_back_to_semantic_link(ui, monkeypatch, fake_fabric):

    def boom(request):
        raise RuntimeError("REST unavailable")

    monkeypatch.setattr(ui, "_picker_items_from_api", boom)
    fake_fabric.list_datasets = lambda workspace, mode: pd.DataFrame(
        {"Dataset Id": ["d1"], "Dataset Name": ["Sales"]}
    )

    assert ui.list_picker_datasets("ws1") == [{"id": "d1", "name": "Sales"}]


def test_api_items_flattens_pages_and_skips_entries_without_an_id(ui, monkeypatch):
    helpers = types.ModuleType("sempy_labs._helper_functions")
    helpers._base_api = lambda request, uses_pagination, client: [
        {"value": [{"id": "1", "displayName": "One"}, {"displayName": "None"}]},
        {"value": [{"id": "2", "displayName": "Two"}]},
    ]
    monkeypatch.setitem(sys.modules, "sempy_labs", types.ModuleType("sempy_labs"))
    monkeypatch.setitem(sys.modules, "sempy_labs._helper_functions", helpers)

    assert ui._picker_items_from_api("/v1/workspaces") == [
        {"id": "1", "name": "One"},
        {"id": "2", "name": "Two"},
    ]


def test_df_items_tolerates_column_spellings(ui):
    df = pd.DataFrame({"Dataset ID": ["d1"], "Name": ["Sales"]})

    assert ui._picker_items_from_df(
        df, ["Dataset Id", "Dataset ID", "Id"], ["Dataset Name", "Name"]
    ) == [{"id": "d1", "name": "Sales"}]
    # An unrecognised shape yields nothing rather than raising.
    assert ui._picker_items_from_df(pd.DataFrame({"X": [1]}), ["Id"], ["Name"]) == []


def test_perspective_icon_is_a_valid_standalone_svg(ui):
    """The tool icon must inherit the theme color like every other icon."""

    icon = ui.ICONS["perspective"]

    assert icon.startswith("<svg ") and icon.endswith("</svg>")
    assert 'viewBox="0 0 24 24"' in icon
    assert 'stroke="currentColor"' in icon
    # No hard-coded size on the <svg> itself, so it takes the header's box.
    opening_tag = icon[: icon.index(">") + 1]
    assert " width=" not in opening_tag and " height=" not in opening_tag
