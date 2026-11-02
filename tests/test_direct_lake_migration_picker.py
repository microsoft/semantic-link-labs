from pathlib import Path

SOURCE_PATH = (
    Path(__file__).parents[1]
    / "src"
    / "sempy_labs"
    / "semantic_model"
    / "_direct_lake_migration.py"
)


def test_migration_picker_uses_shared_searchable_controls():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    assert "SEARCH_SELECT_CSS as _UI_SEARCH_SELECT_CSS" in source
    assert "SEARCH_SELECT_JS as _UI_SEARCH_SELECT_JS" in source
    assert '_UI_SEARCH_SELECT_JS\n    + "\\n"' in source
    assert "workspacePicker = createSearchSelect({" in source
    assert "datasetPicker = createSearchSelect({" in source
    assert 'searchPlaceholder: "Filter workspaces\\u2026"' in source
    assert 'searchPlaceholder: "Filter semantic models\\u2026"' in source
    assert 'data-r="pick-ws-mount"' in source
    assert 'data-r="pick-ds-mount"' in source
    assert 'data-r="pick-ws"' not in source
    assert 'data-r="pick-ds"' not in source


def test_migration_picker_matches_dax_panel_and_actions():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    assert ".slls-mdl-picker-panel { width: 100%; padding: 16px;" in source
    assert ".slls-mdl-picker-reload { display: inline-flex;" in source
    assert "width: 32px; height: 32px;" in source
    assert ".slls-mdl-picker-fields { display: flex; align-items: flex-end;" in source
    assert ".slls-mdl-picker-field .slls-ss-btn { border-radius: 999px;" in source
    assert 'title="Reload workspaces and semantic models"' in source
    assert ">Connect to a semantic model</h2>" in source


def test_migration_picker_preserves_existing_selection_actions():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    assert (
        'pickWs = option.value; pickDs = ""; ensureDatasets(pickWs); route();' in source
    )
    assert "pickDs = option.value; route();" in source
    assert 'runAction("connect", { workspace_id: pickWs, dataset_id: pickDs' in source
    assert 'data-r="pick-cancel"' in source
