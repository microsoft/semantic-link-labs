from pathlib import Path

SOURCE_PATH = (
    Path(__file__).parents[1]
    / "src"
    / "sempy_labs"
    / "semantic_model"
    / "_find_unused_objects.py"
)


def test_find_unused_objects_normal_layout_is_roomier():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    root_start = source.index(".fuo {")
    root_end = source.index("}", root_start)
    root_css = source[root_start:root_end]

    assert "max-width: 760px;" in root_css
    assert ".fuo-tree { max-height: 520px;" in source
    assert ".fuo.fuo-fs .fuo-tree { max-height: calc(100vh - 220px); }" in source
