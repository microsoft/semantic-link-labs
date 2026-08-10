import importlib.util
import sys
import types
from pathlib import Path

SOURCE_PATH = (
    Path(__file__).parents[1]
    / "src"
    / "sempy_labs"
    / "semantic_model"
    / "_model_comparison.py"
)


def _source() -> str:
    return SOURCE_PATH.read_text(encoding="utf-8")


def _module():
    """Load the module in isolation.

    ``sempy_labs/__init__`` pulls in sempy and pandas, which the diff engine
    does not need, so the package is registered as a bare namespace first.
    """
    if "mc_under_test" in sys.modules:
        return sys.modules["mc_under_test"]
    for name in ["sempy", "sempy._utils", "sempy._utils._log"]:
        sys.modules.setdefault(name, types.ModuleType(name))
    sys.modules["sempy._utils._log"].log = lambda f: f
    src = SOURCE_PATH.parents[2]
    for name, path in [
        ("sempy_labs", src / "sempy_labs"),
        ("sempy_labs.semantic_model", src / "sempy_labs" / "semantic_model"),
    ]:
        if name not in sys.modules or not hasattr(sys.modules[name], "__path__"):
            pkg = types.ModuleType(name)
            pkg.__path__ = [str(path)]
            sys.modules[name] = pkg
    spec = importlib.util.spec_from_file_location("mc_under_test", SOURCE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["mc_under_test"] = module
    spec.loader.exec_module(module)
    return module


def _pair():
    mc = _module()
    e = mc._entity
    current = [
        e(
            "t\x01Sales",
            "Table",
            "Sales",
            {"Hidden": "No", "Description": None},
            "Sales",
        ),
        e("t\x01Old", "Table", "Old", {"Hidden": "No", "Description": "gone"}, "Old"),
        e(
            "m\x01Sales\x01Qty",
            "Measure",
            "'Sales'[Qty]",
            {"Expression": "SUM(x)", "Hidden": "No", "Description": None},
            "Sales",
        ),
        e(
            "c\x01Sales\x01Amt",
            "Column",
            "'Sales'[Amt]",
            {"Data type": "Double"},
            "Sales",
        ),
    ]
    compared = [
        e(
            "t\x01Sales",
            "Table",
            "Sales",
            {"Hidden": "Yes", "Description": None},
            "Sales",
        ),
        e("t\x01New", "Table", "New", {"Hidden": "No", "Description": "fresh"}, "New"),
        e(
            "m\x01Sales\x01Qty",
            "Measure",
            "'Sales'[Qty]",
            {"Expression": "SUM(y)", "Hidden": "No", "Description": "d"},
            "Sales",
        ),
        e(
            "c\x01Sales\x01Amt",
            "Column",
            "'Sales'[Amt]",
            {"Data type": "Double"},
            "Sales",
        ),
    ]
    return mc, current, compared


def test_objects_are_classified_and_counted():
    mc, current, compared = _pair()

    entities, summary = mc._compute_diff(current, compared)

    assert summary == {
        "added": 1,
        "removed": 1,
        "modified": 2,
        "unchanged": 1,
        "total": 5,
    }
    status = {e["label"]: e["status"] for e in entities}
    assert status["New"] == "added"
    assert status["Old"] == "removed"
    assert status["Sales"] == "modified"
    assert status["'Sales'[Amt]"] == "unchanged"


def test_added_and_removed_carry_one_sided_properties():
    mc, current, compared = _pair()

    entities, _ = mc._compute_diff(current, compared)
    added = next(e for e in entities if e["status"] == "added")
    removed = next(e for e in entities if e["status"] == "removed")

    # Empty values are dropped so an absent property is not reported as a change.
    assert added["props"] == [
        {"name": "Hidden", "compared": "No"},
        {"name": "Description", "compared": "fresh"},
    ]
    assert all("current" in p and "compared" not in p for p in removed["props"])


def test_modified_carries_only_the_differing_properties():
    mc, current, compared = _pair()

    entities, _ = mc._compute_diff(current, compared)
    measure = next(e for e in entities if e["kind"] == "Measure")
    unchanged = next(e for e in entities if e["status"] == "unchanged")

    assert {p["name"] for p in measure["props"]} == {"Expression", "Description"}
    assert unchanged["props"] == []


def test_entities_sort_by_kind_then_table_then_label():
    mc, current, compared = _pair()

    entities, _ = mc._compute_diff(current, compared)

    assert [e["kind"] for e in entities] == [
        "Table",
        "Table",
        "Table",
        "Column",
        "Measure",
    ]
    tables = [e["label"] for e in entities if e["kind"] == "Table"]
    assert tables == sorted(tables)


def test_property_normalization():
    mc = _module()

    assert mc._str("  ") is None
    assert mc._str(None) is None
    assert mc._str("  x ") == "x"
    # Undefined and False both read as "No" to avoid noisy diffs.
    assert mc._yes_no(True) == "Yes"
    assert mc._yes_no(False) == "No"
    assert mc._yes_no(None) == "No"


def test_kinds_match_the_reference_tool():
    mc = _module()

    assert mc.DIFF_KINDS == [
        "Table",
        "Column",
        "Measure",
        "Hierarchy",
        "Calculation item",
        "Relationship",
        "Role",
        "Expression",
        "Function",
        "Perspective",
        "Perspective object",
        "Culture",
        "Translation",
    ]


def test_widget_uses_the_shared_ui_building_blocks():
    source = _source()

    # Icons, the searchable picker and the header controls are shared.
    assert "SEARCH_SELECT_CSS as _UI_SEARCH_SELECT_CSS" in source
    assert "SEARCH_SELECT_JS as _UI_SEARCH_SELECT_JS" in source
    assert '_WIDGET_JS = _UI_SEARCH_SELECT_JS + "\\n" + _WIDGET_JS' in source
    assert '_ui_scoped_header_css(".slls-mc")' in source
    assert '_ui_scoped_button_press_css(".slls-mc")' in source
    # Four searchable pickers (workspace + model, for each side) via one helper.
    assert "const picker = createSearchSelect(opts);" in source
    assert source.count("= mount(") == 4
    assert "<select" not in source
    assert 'class="sl-reload-btn' in source
    assert 'class="sl-theme-btn"' in source
    # The model/workspace picker is reopened with the shared swap control.
    assert 'class="sl-change-btn" data-act="change-model"' in source
    assert "${ICON.swap}" in source
    assert "Change models" not in source
    # Icons are substituted from _ui_components, not inlined in the widget JS.
    assert source.count('.replace("__ICON_') == 26
    assert '_UI_ICONS["git_compare"]' in source
    for token in (
        "--ui-bg: var(--slls-bg-solid)",
        "--ui-border-strong: var(--slls-border-strong)",
        "--ui-accent: var(--slls-accent)",
    ):
        assert token in source
    assert "Powered by <a href=" in source


def test_widget_renders_the_reference_layout():
    source = _source()

    # Current model chip is rose/danger, compared is emerald/success.
    assert ".slls-mc-chip.current { border-left-color: var(--slls-danger); }" in source
    assert (
        ".slls-mc-chip.compared { border-left-color: var(--slls-success); }" in source
    )
    assert 'modelChip("Current model"' in source
    assert 'modelChip("Compared model"' in source
    # Status filter chips default to everything except unchanged.
    assert 'let statusFilter = new Set(["added", "removed", "modified"]);' in source
    assert 'const STATUSES = ["added", "removed", "modified", "unchanged"];' in source
    # Side-by-side property grid with the two model names as column headers.
    assert "function sideCell(value, tone)" in source
    assert '`<div class="slls-mc-cell absent">Not present</div>`' in source
    assert "(none)" in source
    assert "grid-template-columns: minmax(112px, auto) 1fr 1fr" in source
    # Grouped by kind and collapsible.
    assert "function visibleGroups()" in source
    assert "collapsedKinds" in source
    assert "kinds = traitlets.List().tag(sync=True)" in source


def test_comparison_runs_through_the_backend_callback():
    source = _source()

    assert 'action == "compare"' in source
    assert 'action == "reload"' in source
    assert 'action == "list_workspaces"' in source
    assert 'action == "list_datasets"' in source
    assert 'widget.observe(_on_run, names=["run"])' in source
    assert "connect_semantic_model(" in source
    assert "readonly=True" in source
    # Displayed, never returned (returning it double-renders in Jupyter).
    assert source.rstrip().endswith("display(widget)")
