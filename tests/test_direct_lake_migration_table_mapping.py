from pathlib import Path

SOURCE_PATH = (
    Path(__file__).parents[1]
    / "src"
    / "sempy_labs"
    / "semantic_model"
    / "_direct_lake_migration.py"
)


def test_table_mapping_panel_is_gated_on_schema_support():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    # The panel (and the overrides it produces) are only offered for a
    # Warehouse or a schema-enabled Lakehouse.
    assert (
        'return !(type === "Lakehouse" && model.get("lakehouse_schema_enabled") === false);'
        in source
    )
    assert 'if (!schemaSupported() || rows.length === 0) return "";' in source
    assert "return schemaSupported() ? tableMappings() : {};" in source
    assert "table_mappings: mappingsPayload()," in source


def test_table_mapping_panel_is_collapsible_and_editable():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    assert 'data-r="map-details"' in source
    assert 'data-r="map-reset"' in source
    assert 'data-map-kind="schema"' in source
    assert 'data-map-kind="table"' in source
    assert "let mapOpen = false;" in source
    assert "table_mappings = traitlets.Dict().tag(sync=True)" in source
    assert '"entityTables": entity_tables,' in source


def test_table_mapping_rows_follow_the_schema_box():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    # Typing in the schema box repopulates every non-overridden row and
    # refreshes the summary / Reset button without a re-render.
    assert (
        'model.set("schema", e.target.value); model.save_changes(); syncMappingSchemas();'
        in source
    )
    assert (
        'if (!((maps[row.name] || {}).schema || "").trim()) node.value = defSchema;'
        in source
    )
    assert 'const s = (m.schema || "").trim() || defSchema;' in source
    assert 'const tbl = (m.table || "").trim() || t.entity;' in source
    # A row only counts as customized when it deviates from the default, so
    # Reset becomes clickable exactly when something was actually changed.
    assert "return (s && s !== defSchema) || (tbl && tbl !== t.entity);" in source
    assert "if (btn) btn.disabled = overrides === 0;" in source
    assert "refreshMappingChrome();" in source
    # The schema field states that it applies to every table.
    assert "Applied to every table, unless a table is given its own schema" in source


def test_configure_requires_a_source_item():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    # Preview model stays disabled until a lakehouse/warehouse is picked.
    assert 'const noSource = !model.get("source_item_id");' in source
    assert 'const previewAttrs = (b || noSource ? " disabled" : "")' in source
    assert '+ (noSource ? ` title="Select a ${esc(srcLabel)} first"` : "");' in source
    # The empty option prompts for a selection instead of claiming there are none.
    assert "`Select a ${typeLower}\u2026`" in source
    assert '"Select a lakehouse\u2026"' in source
    assert "`No ${typeLower}s found`" in source
    # ...and that prompt can't be selected back.
    assert (
        'const attrs = lockPlaceholder ? ` disabled hidden${hasSelection ? "" : " selected"}` : "";'
        in source
    )
    assert 'model.get("source_item_id"), itemsPlaceholder, true)' in source
    assert 'model.get("backup_lakehouse_id"), backupPlaceholder, true)' in source
    assert (
        'data-r="src_item"${srcItems === undefined ? " disabled" : ""}>${srcOptions}'
        in source
    )


def test_table_mapping_is_applied_by_both_migration_paths():
    source = SOURCE_PATH.read_text(encoding="utf-8")

    assert source.count('table_mappings = data.get("table_mappings") or {}') == 2
    # In-place conversion: the entity partition is created with the mapping.
    assert (
        "tbl_schema, ent_name = _table_mapping(\n"
        "                    table_mappings, tname, schema, _default_entity_name(tname)\n"
        "                )" in source
    )
    assert "schema_name=tbl_schema or None," in source
    # New-model migration: the mapping is stamped onto the entity partition.
    assert "p.Source.SchemaName = tbl_schema" in source
    assert "p.Source.EntityName = ent_name" in source
