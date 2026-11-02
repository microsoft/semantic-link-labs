from pathlib import Path

SOURCE_PATH = (
    Path(__file__).parents[1]
    / "src"
    / "sempy_labs"
    / "semantic_model"
    / "_mini_model_manager.py"
)


def _source() -> str:
    return SOURCE_PATH.read_text(encoding="utf-8")


def test_broken_objects_are_labelled_with_a_type_icon():
    source = _source()
    start = source.index('const broken = model.get("broken_objects") || [];')
    block = source[start : source.index("masterPanel.innerHTML = html;", start)]

    assert 'const kind = b.name ? (b.type || "") : "table";' in block
    assert 'ICON_SVG[kind === "table" ? "table" : kind]' in block
    assert 'const kindLabel = TYPE_LABEL[kind] || "Object";' in block
    assert 'class="slls-mmm-broken-icon"' in block
    assert 'aria-label="${kindLabel}"' in block
    # The label itself must stay escaped now that it is embedded in richer markup.
    assert "${escapeHtml(label)}" in block

    assert 'columns: "Column",' in source
    assert 'measures: "Measure",' in source
    assert 'hierarchies: "Hierarchy",' in source
    assert 'table: "Table",' in source
    assert ".slls-mmm-broken-icon {" in source
    assert ".slls-mmm-broken-icon svg {" in source


def test_mini_model_manager_lists_measures_before_columns():
    source = _source()

    assert 'const TYPES = ["measures", "columns", "hierarchies"];' in source
    assert 'const TYPES = ["columns", "measures", "hierarchies"];' not in source
    summary = source[source.index("summary.textContent =") :]
    assert summary.index("measures") < summary.index("cols")


def test_connecting_shows_a_progress_bar_without_waiting_for_the_kernel():
    source = _source()

    assert ".slls-mmm-pickerprogress {" in source
    assert ".slls-mmm-pickerprogress.show { display: flex; }" in source
    assert 'class="slls-mmm-pickerprogress${connectingModel ? " show" : ""}"' in source

    # The flag is set on the click itself; waiting for the busy trait to come
    # back from the kernel would delay the bar by a full comm round trip.
    click = source[source.index("connectBtn.onclick = () => {") :]
    click = click[: click.index("\n        };")]
    assert "connectingModel = true;" in click
    assert click.index("connectingModel = true;") < click.index("send({")

    # Cleared on every path the kernel can take back to the browser: an error
    # status, the busy flag dropping, and a successful connect.
    observers = source[source.index("// ================= Model observers") :]
    for observer in ("change:status", "change:busy", "change:connect_done"):
        block = observers[observers.index(f'model.on("{observer}"') :]
        assert "connectingModel = false;" in block[: block.index("});")]


def test_connecting_disables_the_picker_actions():
    source = _source()
    picker = source[source.index("pickerPage.innerHTML =") :]
    picker = picker[: picker.index("const connectBtn")]

    assert '(!pickDs || isBusy() || connectingModel) ? "disabled" : ""' in picker
    assert 'data-picker="cancel" ${connectingModel ? "disabled" : ""}' in picker
