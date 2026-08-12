import ast
from pathlib import Path
from types import SimpleNamespace


SOURCE_PATH = (
    Path(__file__).parents[1]
    / "src"
    / "sempy_labs"
    / "semantic_model"
    / "_perspective_editor.py"
)


def _source() -> str:
    return SOURCE_PATH.read_text(encoding="utf-8")


def _load_function(name: str):
    module = ast.parse(_source())
    function = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    )
    namespace = {}
    exec(
        compile(ast.Module(body=[function], type_ignores=[]), SOURCE_PATH, "exec"),
        namespace,
    )
    return namespace[name]


def test_perspective_table_kind_uses_specialized_table_types():
    classify = _load_function("_perspective_table_kind")
    table = SimpleNamespace(
        Name="Table", CalculationGroup=None, DataCategory="", Columns=[]
    )
    tom = SimpleNamespace(
        is_field_parameter=lambda **_kwargs: False,
        is_calculated_table=lambda **_kwargs: False,
    )

    assert classify(tom, table) == "table"
    table.CalculationGroup = object()
    assert classify(tom, table) == "calculation_group"
    table.CalculationGroup = None
    tom.is_field_parameter = lambda **_kwargs: True
    tom.is_calculated_table = lambda **_kwargs: True
    assert classify(tom, table) == "field_parameter"
    tom.is_field_parameter = lambda **_kwargs: False
    assert classify(tom, table) == "calculated_table"
    tom.is_calculated_table = lambda **_kwargs: False
    table.DataCategory = "Time"
    table.Columns = [SimpleNamespace(IsKey=True, DataType="DateTime")]
    assert classify(tom, table) == "date_table"


def test_include_all_perspective_table_expands_all_members():
    members = _load_function("_perspective_table_members")
    metadata = {
        "Calc Group": {
            "columns": ["Name", "Ordinal"],
            "measures": [],
            "hierarchies": [],
        }
    }
    perspective_table = SimpleNamespace(
        Table=SimpleNamespace(Name="Calc Group"),
        IncludeAll=True,
        PerspectiveColumns=[],
        PerspectiveMeasures=[],
        PerspectiveHierarchies=[],
    )

    assert members(metadata, perspective_table) == {
        "columns": ["Name", "Ordinal"],
        "measures": [],
        "hierarchies": [],
    }


def test_perspective_editor_uses_table_kind_icons():
    source = _source()

    for kind in (
        "table",
        "calculated_table",
        "calculation_group",
        "date_table",
        "field_parameter",
    ):
        assert f"{kind}: `__SLLS_ICON_{kind.upper()}__`" in source
        assert f'_UI_ICONS["{kind}"]' in source
    assert 'tblIcon.innerHTML = ICON_SVG[data.kind] || ICON_SVG.table;' in source
    assert '"kind": _perspective_table_kind(tom, table)' in source


def test_perspective_editor_header_shows_a_tool_icon():
    source = _source()

    assert "perspective: `__SLLS_ICON_PERSPECTIVE__`," in source
    assert '.replace("__SLLS_ICON_PERSPECTIVE__", _UI_ICONS["perspective"])' in source
    assert 'titleIcon.className = "slls-pe-title-icon";' in source
    assert "titleIcon.innerHTML = ICON_SVG.perspective;" in source
    # Sits ahead of the title block in the header.
    assert source.index("header.appendChild(titleIcon);") < source.index(
        "header.appendChild(titleWrap);"
    )
    assert ".slls-pe-title-icon {" in source
    assert ".slls-pe-title-icon svg { display: block; width: 27px; height: 27px; }" in (
        source
    )


def test_expand_and_collapse_all_are_icon_buttons():
    source = _source()

    assert "expand: `__SLLS_ICON_EXPAND__`," in source
    assert "collapse: `__SLLS_ICON_COLLAPSE__`," in source
    # Reuses the shared chevron pair already used by the other tools.
    assert '.replace("__SLLS_ICON_EXPAND__", _UI_ICONS["expand_rows"])' in source
    assert '.replace("__SLLS_ICON_COLLAPSE__", _UI_ICONS["collapse_rows"])' in source
    assert 'expandAllBtn.className = "slls-pe-btn slls-pe-btn-icon";' in source
    assert "expandAllBtn.innerHTML = ICON_SVG.expand;" in source
    assert 'collapseAllBtn.className = "slls-pe-btn slls-pe-btn-icon";' in source
    assert "collapseAllBtn.innerHTML = ICON_SVG.collapse;" in source
    # The label moves to a tooltip so the buttons stay accessible.
    assert 'expandAllBtn.title = "Expand all";' in source
    assert 'collapseAllBtn.title = "Collapse all";' in source
    assert 'expandAllBtn.setAttribute("aria-label", expandAllBtn.title);' in source
    assert 'collapseAllBtn.setAttribute("aria-label", collapseAllBtn.title);' in source
    assert 'textContent = "Expand All"' not in source
    assert 'textContent = "Collapse All"' not in source
    assert ".slls-pe-btn-icon svg { display: block; }" in source


def test_perspective_editor_sorts_tables_alphabetically():
    source = _source()
    tree = source[source.index("function renderTree()") : source.index("function deepClone")]

    assert "Object.keys(md).sort((left, right) =>" in tree
    assert 'left.localeCompare(right, undefined, { sensitivity: "base" })' in tree


def test_calculation_group_columns_are_controlled_by_table_selection():
    source = _source()
    selection = source[
        source.index("function buildSelectionFromMembers") : source.index(
            "function isObjDirty"
        )
    ]
    tree = source[source.index("function renderTree()") : source.index("function deepClone")]
    payload = source[
        source.index("function buildSavePayload()") : source.index("function send(action)")
    ]

    assert 'md[tbl].kind === "calculation_group"' in selection
    assert "normalizeCalculationGroups &&" in selection
    assert 'sel[tbl].columns[n] = selected;' in selection
    assert 'const lockedColumn = data.kind === "calculation_group" && t === "columns";' in tree
    assert 'if (!lockedColumn) {' in tree
    assert "Calculation group columns are controlled by the calculation group selection." in tree
    assert 'out.push({ table: tbl, type: "table" });' in payload
    assert "buildSelectionFromMembers(cur, true)" in source
    assert "buildSelectionFromMembers(cur, false)" in source


def test_perspective_editor_uses_shared_searchable_model_picker():
    source = _source()

    assert "SEARCH_SELECT_CSS as _UI_SEARCH_SELECT_CSS" in source
    assert "SEARCH_SELECT_JS as _UI_SEARCH_SELECT_JS" in source
    assert '_WIDGET_CSS += "\\n" + _UI_SEARCH_SELECT_CSS' in source
    assert '_WIDGET_JS = _UI_SEARCH_SELECT_JS + "\\n" + _WIDGET_JS' in source
    assert source.count("createSearchSelect({") == 2
    assert 'searchPlaceholder: "Filter workspaces\\u2026"' in source
    assert 'searchPlaceholder: "Filter semantic models\\u2026"' in source
    assert "Connect to a semantic model" in source
    assert "Select a workspace and semantic model to begin." in source
    assert 'action: "list_workspaces"' in source
    assert 'action: "list_datasets"' in source
    assert 'action: "connect"' in source


def test_perspective_editor_header_can_change_active_model():
    source = _source()

    assert 'changeModelBtn.innerHTML = ICON_SVG.swap;' in source
    assert 'changeModelBtn.title = "Change model / workspace";' in source
    assert 'model.get("active_workspace_id")' in source
    assert 'model.get("active_dataset_id")' in source
    assert 'model.on("change:connect_done"' in source


def test_perspective_editor_dataset_is_optional_and_picker_state_is_synced():
    source = _source()
    module = ast.parse(source)
    function = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "perspective_editor"
    )

    assert function.args.args[0].arg == "dataset"
    assert isinstance(function.args.defaults[0], ast.Constant)
    assert function.args.defaults[0].value is None
    for trait in (
        "dataset_chosen",
        "active_workspace_id",
        "active_dataset_id",
        "selected_workspace_id",
        "selected_dataset_id",
        "available_workspaces",
        "available_datasets",
        "picker_loading",
        "connect_done",
    ):
        assert f"{trait} = traitlets." in source


def test_perspective_editor_connect_replaces_state_and_writes_active_model():
    source = _source()
    observer = source[source.index("def _on_run(change)") : source.index("widget.observe")]

    assert 'if action == "list_workspaces":' in observer
    assert 'if action == "list_datasets":' in observer
    assert 'if action == "connect":' in observer
    assert 'state = _collect_perspective_editor_state(' in observer
    assert 'widget.metadata = state["metadata"]' in observer
    assert 'widget.perspective_members = state["perspective_members"]' in observer
    assert 'widget.dataset_chosen = True' in observer
    assert 'widget.connect_done += 1' in observer
    assert 'dataset=model_ctx["dataset_id"]' in observer
    assert 'workspace=model_ctx["workspace_id"]' in observer
    assert "current_metadata = dict(widget.metadata or {})" in source


def test_perspective_editor_lists_measures_before_columns():
    source = _source()
    tree = source[
        source.index("function renderTree()") : source.index("function deepClone")
    ]

    # Only the loop feeding the child rows drives display order; the other
    # type loops are order-agnostic aggregations.
    render_loop = tree[: tree.index("for (const n of (data[t] || []))")]
    assert render_loop.rstrip().endswith(
        'for (const t of ["measures", "columns", "hierarchies"]) {'
    )
    summary = tree[tree.index("summary.textContent =") :]
    assert summary.index("measures") < summary.index("cols")


def test_fullscreen_tree_grows_into_the_unused_vertical_space():
    source = _source()
    fullscreen = source[
        source.index(".slls-pe.slls-pe-fs {") : source.index(".slls-pe-header {")
    ]

    assert "display: flex;" in fullscreen
    assert "flex-direction: column;" in fullscreen
    assert "flex: 1 1 auto;" in fullscreen
    assert "max-height: none;" in fullscreen
    # A fixed cap would leave dead space below the tree when full screen.
    assert "calc(100vh - 320px)" not in fullscreen


def test_connecting_shows_a_progress_bar_without_waiting_for_the_kernel():
    source = _source()

    assert ".slls-pe-picker-progress { display: none;" in source
    assert ".slls-pe-picker-progress.show { display: flex; }" in source
    assert ".slls-pe-picker-track::after {" in source
    assert "@keyframes slls-pe-progress {" in source

    # The flag is set on the click itself; waiting for picker_loading to come
    # back from the kernel would delay the bar by a full comm round trip.
    connect = source[
        source.index("pickerScreen.querySelector('[data-picker=\"connect\"]')") :
    ]
    connect = connect[: connect.index("// ============== Renderers")]
    assert "connecting = true;" in connect
    assert connect.index("connecting = true;") < connect.index("sendPicker({")
    assert "renderPicker();" in connect

    assert 'class="slls-pe-picker-progress${connecting ? " show" : ""}"' in source
    assert 'if (model.get("picker_loading") !== true) connecting = false;' in source
    assert "connecting = false;\n        pickerOpen = false;" in source