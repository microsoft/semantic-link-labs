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
