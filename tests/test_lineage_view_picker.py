from pathlib import Path

SOURCE_PATH = (
    Path(__file__).parents[1]
    / "src"
    / "sempy_labs"
    / "semantic_model"
    / "_lineage_view.py"
)


def _source() -> str:
    return SOURCE_PATH.read_text(encoding="utf-8")


def test_lineage_uses_shared_searchable_model_pickers():
    source = _source()
    connect_picker = source[
        source.index("function buildPickerScreen()") : source.index(
            "// ---------- Rebind modal ----------"
        )
    ]
    rebind_picker = source[
        source.index("function buildRebindModal()") : source.index(
            "// ---------- Node width resize ----------"
        )
    ]

    assert "SEARCH_SELECT_CSS as _UI_SEARCH_SELECT_CSS" in source
    assert "SEARCH_SELECT_JS as _UI_SEARCH_SELECT_JS" in source
    assert '_WIDGET_CSS += "\\n" + _UI_SEARCH_SELECT_CSS' in source
    assert '_WIDGET_JS = _UI_SEARCH_SELECT_JS + "\\n" + _WIDGET_JS' in source
    assert source.count("createSearchSelect({") == 4
    assert 'searchPlaceholder: "Filter workspaces\\u2026"' in connect_picker
    assert 'searchPlaceholder: "Filter semantic models\\u2026"' in connect_picker
    assert 'searchPlaceholder: "Filter workspaces\\u2026"' in rebind_picker
    assert 'searchPlaceholder: "Filter semantic models\\u2026"' in rebind_picker
    assert "<select" not in connect_picker
    assert "<select" not in rebind_picker
    assert "wsPicker.setDisabled(working())" in connect_picker
    assert "dsPicker.setDisabled(!pickWs || ds === null || working())" in connect_picker
    assert "dsPicker.setDisabled(ds === null || working())" in rebind_picker


def test_lineage_picker_maps_shared_theme_tokens_and_matches_dax_perf_layout():
    source = _source()

    for token in (
        "--ui-bg: var(--slls-bg-solid)",
        "--ui-bg-solid: var(--slls-bg-solid)",
        "--ui-border-strong: var(--slls-border-strong)",
        "--ui-text: var(--slls-text)",
        "--ui-accent: var(--slls-accent)",
    ):
        assert token in source
    assert ".slls-lv-picker-grid .slls-ss-btn" in source
    assert "border-radius: 999px" in source
    # The reload control comes from the shared UI components module.
    assert 'class="sl-reload-btn' in source
    assert "sl-spinning" in source
    assert ".slls-lv-picker-reload" not in source
    assert "Connect to a semantic model" in source
    assert "Select a workspace and semantic model to begin." in source


def test_lineage_picker_gray_surface_is_limited_to_picker_card():
    source = _source()

    assert (
        ".slls-lv-graphwrap.slls-lv-picker-wrap { "
        "background: var(--slls-bg-solid); }" in source
    )
    assert ".slls-lv-picker { width: 100%; background: var(--slls-surface);" in source
    assert (
        ".slls-lv-graphwrap { position: relative; flex: 1; min-width: 0; "
        "background: var(--slls-bg-secondary); }" in source
    )
