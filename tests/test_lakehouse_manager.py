"""The Lakehouse Manager rolls OneLake blobs up into lakehouse objects."""

import json
import sys
import types

import pytest

from sempy_labs import _app
from sempy_labs.lakehouse import _lakehouse_manager as lhm

LAKEHOUSE_ID = "11111111-2222-3333-4444-555555555555"


def _blob(name, size=100, deleted=False, created="2026-01-01T00:00:00Z"):
    return {
        "Blob Name": name,
        "Content Length": size,
        "Is Deleted": deleted,
        "Creation Time": created,
    }


def test_blob_paths_are_relative_to_the_lakehouse_root():
    assert lhm._blob_path(
        f"{LAKEHOUSE_ID}/Tables/sales/part.parquet", LAKEHOUSE_ID
    ) == ("Tables/sales/part.parquet")
    assert lhm._blob_path("/Files/raw/data.csv", LAKEHOUSE_ID) == "Files/raw/data.csv"
    assert lhm._blob_path(None, LAKEHOUSE_ID) == ""


def test_blobs_are_rolled_up_into_tables_folders_and_files():
    blobs = [
        _blob("Tables/sales/part-0.parquet", 100),
        _blob("Tables/sales/part-1.parquet", 150),
        _blob("Tables/sales/_delta_log/00.json", 50),
        _blob("Files/raw/data.csv", 300),
        _blob("Files/readme.txt", 20),
        # System folders are not part of the lakehouse's contents.
        _blob("TableMaintenance/job.json", 10),
    ]

    rows = {row["path"]: row for row in lhm._summarize_blobs(blobs, LAKEHOUSE_ID)}

    assert set(rows) == {"Tables/sales", "Files/raw", "Files/readme.txt"}
    assert rows["Tables/sales"]["type"] == "Table"
    assert rows["Tables/sales"]["files"] == 3
    assert rows["Tables/sales"]["size"] == 300
    assert rows["Files/raw"]["type"] == "Folder"
    assert rows["Files/readme.txt"]["type"] == "File"


def test_schema_enabled_lakehouses_key_tables_by_schema():
    blobs = [_blob("Tables/dbo/sales/part-0.parquet")]

    row = lhm._summarize_blobs(blobs, LAKEHOUSE_ID, schema_enabled=True)[0]

    assert row["path"] == "Tables/dbo/sales"
    assert row["schema"] == "dbo"
    assert row["name"] == "sales"


def test_schema_folders_are_never_mistaken_for_tables():
    # OneLake lists each schema's folder as a blob too, including empty schemas.
    blobs = [
        _blob("Tables/dbo"),
        _blob("Tables/dbo/sales/part-0.parquet"),
        _blob("Tables/empty"),
    ]

    rows = lhm._summarize_blobs(blobs, LAKEHOUSE_ID, schema_enabled=True)
    tree = lhm._build_tree(blobs, LAKEHOUSE_ID, schema_enabled=True)

    assert [(row["schema"], row["name"]) for row in rows] == [("dbo", "sales")]
    assert lhm._summarize_objects(rows)["tables"] == 1
    schemas = {node["name"]: node["type"] for node in tree[0]["children"]}
    assert schemas == {"dbo": "Schema", "empty": "Schema"}


def test_soft_deleted_objects_and_shortcuts_are_flagged():
    blobs = [
        _blob("Tables/dropped/part-0.parquet", deleted=True),
        _blob("Tables/kept/part-0.parquet"),
        _blob("Tables/kept/part-1.parquet", deleted=True),
        _blob("Tables/linked/part-0.parquet"),
    ]

    rows = {
        row["path"]: row
        for row in lhm._summarize_blobs(
            blobs, LAKEHOUSE_ID, shortcut_paths={"Tables/linked"}
        )
    }

    assert rows["Tables/dropped"]["deleted"] is True
    # Live files remain, so the table itself is not deleted - but its deleted
    # files are still recoverable.
    assert rows["Tables/kept"]["deleted"] is False
    assert rows["Tables/kept"]["deleted_files"] == 1
    assert rows["Tables/linked"]["shortcut"] is True
    assert rows["Tables/dropped"]["shortcut"] is False


def test_summary_counts_only_live_objects():
    objects = lhm._summarize_blobs(
        [
            _blob("Tables/sales/part-0.parquet", 1024),
            _blob("Tables/dropped/part-0.parquet", 512, deleted=True),
            _blob("Files/readme.txt", 1024),
        ],
        LAKEHOUSE_ID,
    )

    summary = lhm._summarize_objects(objects)

    assert summary["tables"] == 1
    assert summary["files"] == 1
    assert summary["deleted"] == 1
    assert summary["recoverable"] == 1
    assert summary["size_label"] == "2.0 KB"


def _source():
    from pathlib import Path

    return Path(lhm.__file__).read_text(encoding="utf-8")


def test_the_tree_groups_blobs_by_container_schema_and_table():
    tree = lhm._build_tree(
        [
            _blob("Tables/dbo/sales/part-0.parquet", 100),
            _blob("Tables/dbo/sales/part-1.parquet", 200),
            _blob("Files/raw/data.csv", 50),
        ],
        LAKEHOUSE_ID,
        schema_enabled=True,
    )

    tables, files = tree
    assert [node["name"] for node in tree] == ["Tables", "Files"]
    assert tables["type"] == "Container"
    assert tables["size"] == 300
    schema = tables["children"][0]
    assert (schema["name"], schema["type"]) == ("dbo", "Schema")
    table = schema["children"][0]
    assert (table["name"], table["type"], table["size"]) == ("sales", "Table", 300)
    assert [child["type"] for child in table["children"]] == ["File", "File"]
    assert files["children"][0]["type"] == "Folder"


def test_the_tree_marks_deleted_nodes_and_shortcuts():
    tree = lhm._build_tree(
        [
            _blob("Tables/dropped/part-0.parquet", deleted=True),
            _blob("Tables/linked/part-0.parquet"),
        ],
        LAKEHOUSE_ID,
        shortcut_paths={"Tables/linked"},
    )

    nodes = {node["name"]: node for node in tree[0]["children"]}

    assert nodes["dropped"]["deleted"] is True
    assert nodes["dropped"]["deleted_files"] == 1
    assert nodes["linked"]["shortcut"] is True


def test_the_tree_collapses_the_extra_children_of_a_large_table():
    blobs = [_blob(f"Tables/big/part-{i}.parquet") for i in range(5)]

    table = lhm._build_tree(blobs, LAKEHOUSE_ID, max_children=2)[0]["children"][0]

    assert [child["type"] for child in table["children"]] == ["File", "File", "More"]
    assert table["children"][-1]["name"] == "+3 more"


def test_widget_assets_are_fully_substituted():
    assert "__ICON_" not in lhm._LHM_JS
    assert "__ATTRIBUTION__" not in lhm._LHM_JS
    assert "__LIGHT_VARS__" not in lhm._LHM_CSS
    assert "__SEARCH_CSS__" not in lhm._LHM_CSS
    # The shared header controls and press feedback are used as-is.
    source = _source()
    assert '_ui_scoped_header_css(".slls-lhm")' in source
    assert '_ui_scoped_button_press_css(".slls-lhm")' in source
    assert "list_picker_workspaces" in source
    assert "fabric.list_workspaces()" not in source


def test_the_contents_can_be_shown_as_a_tree_or_a_table():
    js = lhm._LHM_JS

    assert 'let view = "Tree";' in js
    assert 'if (view === "Tree") renderTree(); else renderTable();' in js
    assert "function renderTree()" in js
    assert "function renderTable()" in js
    # A search opens the branches which matched inside.
    assert "(term && childMatch) || expanded.has(node.path)" in js


def test_the_table_view_lists_only_tables_with_a_schema_column():
    js = lhm._LHM_JS

    assert 'filter((row) => row.type === "Table")' in js
    assert 'const pillButtons = ["All", "Deleted"]' in js
    # Tables are not grouped by schema; the schema is a column instead.
    assert "slls-lhm-group" not in js
    assert '{ key: "schema", label: "Schema", text: true, schemaOnly: true }' in js
    # No status column; a shortcut shows the shortcut icon instead.
    assert '{ key: "status"' not in js and '"Status"' not in js
    assert "icon.innerHTML = row.shortcut ? `" in js
    # Columns sort across all tables and can be resized.
    assert (
        "return order * tableSort.dir || byText(a.schema, b.schema) || byText(a.name, b.name);"
        in js
    )
    assert "sllsInstallColumnResizers(table, {" in js
    assert "function sllsInstallColumnResizers(" in js
    assert ".slls-lhm .slls-column-resizer {" in lhm._LHM_CSS


def test_the_table_view_shows_size_bars_and_delta_table_details():
    js = lhm._LHM_JS

    for label in ["File Sizes", "Partitions", "Deletion Vectors"]:
        assert f'label: "{label}"' in js
    assert "tr.appendChild(sizeCell(row, largest));" in js
    assert "tr.appendChild(fileSizesCell(row));" in js
    assert '"< 1 MB"' in js and "__FILE_SIZE_BANDS__" not in js
    assert (
        ".slls-lhm-table .slls-lhm-bar { display: block; width: 100%; }" in lhm._LHM_CSS
    )


def test_tables_report_their_file_sizes_partitions_and_deletion_vectors():
    mb = 1024 * 1024
    blobs = [
        _blob("Tables/sales/year=2025/part-0.parquet", 200 * mb),
        _blob("Tables/sales/year=2026/part-1.parquet", 2 * mb),
        _blob("Tables/sales/year=2026/part-2.parquet", 500 * 1024),
        _blob("Tables/sales/deletion_vector_abc.bin", 1024),
        # Neither delta log checkpoints nor deleted files are data files.
        _blob("Tables/sales/_delta_log/00.checkpoint.parquet", 5 * mb),
        _blob("Tables/sales/year=2027/part-3.parquet", 9 * mb, deleted=True),
        _blob("Tables/plain/part-0.parquet", 4 * mb),
        _blob("Files/raw/data.parquet", 4 * mb),
    ]

    rows = {row["path"]: row for row in lhm._summarize_blobs(blobs, LAKEHOUSE_ID)}

    sales = rows["Tables/sales"]
    assert sales["partitions"] == 2
    assert sales["deletion_vectors"] is True
    assert sales["deletion_vector_files"] == 1
    stats = sales["file_sizes"]
    assert stats["count"] == 3
    assert (stats["min"], stats["max"]) == (500 * 1024, 200 * mb)
    assert stats["max_label"] == "200.0 MB"
    # < 1 MB, 1-16 MB, 16-64 MB, 64-128 MB, 128-256 MB, 256 MB-1 GB, >= 1 GB
    assert stats["bands"] == [1, 1, 0, 0, 1, 0, 0]

    plain = rows["Tables/plain"]
    assert plain["partitions"] == 0
    assert plain["deletion_vectors"] is False
    assert plain["file_sizes"]["count"] == 1
    assert rows["Files/raw"]["file_sizes"] is None


def test_schema_enabled_tables_report_their_partitions():
    blobs = [
        _blob("Tables/dbo/sales/region=eu/part-0.parquet"),
        _blob("Tables/dbo/sales/region=us/part-0.parquet"),
    ]

    row = lhm._summarize_blobs(blobs, LAKEHOUSE_ID, schema_enabled=True)[0]

    assert row["partitions"] == 2
    assert row["file_sizes"]["count"] == 2


def test_tree_objects_can_be_selected_and_acted_on_in_bulk():
    js = lhm._LHM_JS

    assert 'check.setAttribute("aria-label", "Select " + node.path);' in js
    # Containers are groupings, not objects; a schema is picked as a whole, and
    # a table only as a whole, never the files inside it.
    assert 'if (node.type === "Container" || node.type === "More") return false;' in js
    assert 'if (parent.type === "Table") return false;' in js
    assert 'act("recover", { paths: paths })' in js
    assert 'act("delete", { paths: paths })' in js
    # Deleting is confirmed first.
    assert 'confirmOverlay.style.display = "flex";' in js
    assert '"delete": _delete' in _source()


def test_selecting_a_folder_selects_its_contents_and_partial_folders_show_it():
    js = lhm._LHM_JS

    # A picked folder stands for its contents; a partly picked one is shown so.
    assert "check.checked = isChecked(node);" in js
    assert "check.indeterminate = !check.checked && partial.has(node.path);" in js
    assert "function setChecked(node, on)" in js
    # A fully picked branch becomes picked as a whole, unless it lists only
    # some of its objects (+N more).
    assert (
        "if (!children.every((child) => selectable(child) && selected.has(child.path))) break;"
        in js
    )


def test_an_actions_outcome_is_shown_temporarily():
    js = lhm._LHM_JS

    assert "const STATUS_VISIBLE_MS = 5000;" in js
    assert 'model.on("change:status_message", scheduleStatusHide);' in js
    assert (
        'const status = statusShown ? (model.get("status_message") || "") : "";' in js
    )
    # Errors are not hidden on a timer.
    assert 'const error = model.get("error_message") || "";' in js


def test_an_action_can_target_one_object_or_a_selection():
    assert lhm._action_paths({"path": "Tables/sales"}) == ["Tables/sales"]
    assert lhm._action_paths({"paths": ["Files/a", "/Files/b/"]}) == [
        "Files/a",
        "Files/b",
    ]
    assert lhm._action_paths({}) == []


def test_the_tree_reads_as_regular_text_and_files_use_a_file_icon():
    from sempy_labs._ui_components import ICONS

    assert "'Segoe UI'" in lhm._LHM_CSS
    assert "monospace" not in lhm._LHM_CSS
    assert ICONS["file"] in lhm._LHM_JS
    assert ICONS["source"] not in lhm._LHM_JS


def test_pickers_are_seeded_before_display():
    source = _source()
    after_display = source[source.index("display(widget)") :]

    assert "widget.available_workspaces =" not in after_display
    assert "widget.available_lakehouses =" not in after_display


def test_the_tool_is_offered_by_the_app_launcher():
    tool = next(t for t in _app._TOOLS if t["key"] == "lakehouse_manager")

    assert tool["function"] == "lakehouse_manager"
    assert tool["module"] == "sempy_labs.lakehouse._lakehouse_manager"
    assert "Lakehouse" in tool["tags"]
    assert callable(lhm.lakehouse_manager)


def test_the_last_modified_time_is_shown_rather_than_the_creation_time():
    blob = _blob("Files/readme.txt", created="2026-01-01T00:00:00Z")
    blob["Last Modified"] = "2026-03-01T00:00:00Z"

    row = lhm._summarize_blobs([blob], LAKEHOUSE_ID)[0]

    assert row["modified"] == "2026-03-01T00:00:00Z"


def test_paths_which_could_escape_the_lakehouse_are_refused():
    assert lhm._is_safe_object_path("Tables/sales")
    assert lhm._is_safe_object_path("Files/raw/data.csv")
    for path in [
        "",
        "Tables",
        "Other/x",
        "Tables/../../other-lakehouse/Files/x",
        "Files/./x",
        "Files//x",
        "Files/%2e%2e/x",
        "Files\\..\\x",
        "Files/x?comp=undelete",
        "Files/x#frag",
        "Files/x\n",
    ]:
        assert not lhm._is_safe_object_path(path), path


def test_only_loaded_objects_schemas_and_never_containers_can_be_targeted():
    blobs = [
        _blob("Tables/dbo/sales/part-0.parquet"),
        _blob("Files/raw/data.csv"),
    ]
    objects = lhm._summarize_blobs(blobs, LAKEHOUSE_ID, schema_enabled=True)
    tree = lhm._build_tree(blobs, LAKEHOUSE_ID, schema_enabled=True)

    allowed = lhm._actionable_paths(objects, tree)

    assert "Tables/dbo/sales" in allowed
    assert "Files/raw/data.csv" in allowed
    # A whole schema can be deleted or recovered.
    assert "Tables/dbo" in allowed
    assert "Tables" not in allowed
    assert "Files" not in allowed
    accepted, rejected = lhm._authorized_paths(
        ["Tables/dbo/sales", "Tables/dbo", "Tables", "Files/other"], allowed
    )
    assert accepted == ["Tables/dbo/sales", "Tables/dbo"]
    assert rejected == ["Tables", "Files/other"]


def test_the_table_view_selects_tables_for_bulk_maintenance():
    js = lhm._LHM_JS
    css = lhm._LHM_CSS

    # Shortcuts and deleted tables get no checkbox and are never maintained.
    assert 'return row.type === "Table" && !row.deleted && !row.shortcut;' in js
    assert "if (maintainable(row)) {" in js
    assert 'checkAll.title = "Select all tables shown";' in js
    # The toolbar runs maintenance on the selection; the rows no longer do.
    assert (
        'maintainSelected.addEventListener("click", () => openMaintenance(selectedTables(), true));'
        in js
    )
    assert "actionsCell.appendChild(rowActions(row, false));" in js
    assert "paths: maintenanceTargets.map((row) => row.path)," in js
    # The list scrolls itself, so its sticky headers stay in view.
    assert "max-height: min(640px, calc(100vh - 160px));" in css
    assert ".slls-lhm:fullscreen .slls-lhm-list { max-height: none; }" in css


def test_only_live_tables_which_are_not_shortcuts_can_be_maintained():
    blobs = [
        _blob("Tables/dbo/sales/part-0.parquet"),
        _blob("Tables/dbo/dropped/part-0.parquet", deleted=True),
        _blob("Tables/dbo/linked/part-0.parquet"),
        _blob("Files/raw/data.csv"),
    ]
    objects = lhm._summarize_blobs(
        blobs,
        LAKEHOUSE_ID,
        schema_enabled=True,
        shortcut_paths={"Tables/dbo/linked"},
    )

    assert lhm._maintainable_tables(objects) == {"Tables/dbo/sales": ("dbo", "sales")}


@pytest.fixture
def manager(monkeypatch):
    """A Lakehouse Manager widget wired to fake lakehouses and file system."""

    import IPython.display
    from sempy_labs import _helper_functions

    contents = {
        "lh-a": [
            _blob("Files/shared.csv"),
            _blob("Files/a-only.csv"),
            _blob("Tables/sales/part-0.parquet"),
            _blob("Tables/orders/part-0.parquet"),
        ],
        "lh-b": [_blob("Files/shared.csv")],
    }
    removed = []
    state = {"rm_result": True, "fail_read": False}

    def fake_objects(workspace_id, lakehouse_id):
        if state["fail_read"]:
            raise RuntimeError("listing failed")
        blobs = contents[lakehouse_id]
        return (
            lhm._summarize_blobs(blobs, lakehouse_id),
            lhm._build_tree(blobs, lakehouse_id),
            False,
        )

    def fake_rm(path, recurse=False):
        removed.append(path)
        return state["rm_result"]

    monkeypatch.setattr(
        _helper_functions,
        "resolve_workspace_name_and_id",
        lambda workspace=None: ("WS", "ws-1"),
    )
    monkeypatch.setattr(
        _helper_functions,
        "resolve_lakehouse_name_and_id",
        lambda lakehouse=None, workspace=None: ("A", "lh-a"),
    )
    monkeypatch.setattr(
        _helper_functions,
        "create_abfss_path_from_path",
        lambda lakehouse_id, workspace_id, path: f"{workspace_id}/{lakehouse_id}/{path}",
    )
    monkeypatch.setattr(lhm, "_ui_list_picker_workspaces", lambda *a: [])
    monkeypatch.setattr(lhm, "_ui_list_picker_lakehouses", lambda *a: [])
    monkeypatch.setattr(lhm, "_lakehouse_objects", fake_objects)
    monkeypatch.setitem(
        sys.modules,
        "notebookutils",
        types.SimpleNamespace(fs=types.SimpleNamespace(rm=fake_rm)),
    )
    shown = []
    monkeypatch.setattr(IPython.display, "display", shown.append)

    lhm.lakehouse_manager()
    widget = shown[0]

    def act(action, _drain=True, **data):
        widget.pending_action = dict(action=action, **data)
        widget.action_trigger += 1
        # Like the browser, ask for each next table of a batch.
        while _drain and widget.batch:
            widget.pending_action = {"action": "batch_next"}
            widget.action_trigger += 1

    act("open", workspace="ws-1", lakehouse="lh-a")
    return widget, act, removed, state


def test_actions_target_the_loaded_lakehouse_not_the_picker_selection(manager):
    widget, act, removed, _ = manager

    # Lakehouse B is picked, but the picker is closed without connecting.
    widget.selected_lakehouse_id = "lh-b"
    act("delete", paths=["Files/shared.csv"])

    assert removed == ["ws-1/lh-a/Files/shared.csv"]
    assert widget.loaded_lakehouse_id == "lh-a"


def test_paths_outside_the_loaded_objects_are_never_deleted(manager):
    widget, act, removed, _ = manager

    act("delete", paths=["Files/../../lh-b/Files/x", "Files/unknown.csv"])

    assert removed == []
    assert "not an object of the loaded lakehouse" in widget.error_message


def test_a_delete_reported_as_failed_is_not_counted(manager):
    widget, act, removed, state = manager
    state["rm_result"] = False

    act("delete", paths=["Files/a-only.csv"])

    assert removed == ["ws-1/lh-a/Files/a-only.csv"]
    assert widget.status_message == "Deleted 0 object(s)."
    # The refresh which follows the action keeps its error.
    assert "Could not delete Files/a-only.csv" in widget.error_message


def test_a_failed_refresh_keeps_the_action_error(manager):
    widget, act, _, state = manager
    state["rm_result"] = False
    state["fail_read"] = True

    act("delete", paths=["Files/a-only.csv"])

    assert "Could not delete Files/a-only.csv" in widget.error_message
    assert "Could not read the lakehouse: listing failed" in widget.error_message


def test_maintenance_runs_on_each_selected_table(manager, monkeypatch):
    from sempy_labs.lakehouse import _lakehouse

    widget, act, _, _ = manager
    calls = []
    monkeypatch.setattr(
        _lakehouse, "run_table_maintenance", lambda **kwargs: calls.append(kwargs)
    )

    act("maintenance", paths=["Tables/sales", "Tables/orders"], optimize=True)

    assert [call["table_name"] for call in calls] == ["sales", "orders"]
    assert all(call["lakehouse"] == "lh-a" for call in calls)
    assert all(call["schema"] is None for call in calls)
    assert widget.status_message == "Table maintenance finished for 2 of 2 tables."
    assert widget.error_message == ""


def test_maintenance_never_targets_a_path_which_is_not_a_loaded_table(
    manager, monkeypatch
):
    from sempy_labs.lakehouse import _lakehouse

    widget, act, _, _ = manager
    calls = []
    monkeypatch.setattr(
        _lakehouse, "run_table_maintenance", lambda **kwargs: calls.append(kwargs)
    )

    act("maintenance", paths=["Files/a-only.csv", "Tables/unknown"], optimize=True)

    assert calls == []
    assert "not a table of the loaded lakehouse" in widget.error_message


def test_a_failed_table_does_not_stop_the_others(manager, monkeypatch):
    from sempy_labs.lakehouse import _lakehouse

    widget, act, _, _ = manager
    calls = []

    def fake_maintenance(**kwargs):
        calls.append(kwargs["table_name"])
        if kwargs["table_name"] == "sales":
            raise RuntimeError("locked")

    monkeypatch.setattr(_lakehouse, "run_table_maintenance", fake_maintenance)

    act("maintenance", paths=["Tables/sales", "Tables/orders"], optimize=True)

    assert calls == ["sales", "orders"]
    assert widget.status_message == "Table maintenance finished for 1 of 2 tables."
    assert "Table maintenance failed for 'sales': locked" in widget.error_message


def test_the_table_view_collects_stats_for_the_selected_tables():
    js = lhm._LHM_JS

    assert "tableSelection.append(maintainSelected, statsSelected, " in js
    assert 'if (paths.length) clearTablesAfter = act("stats", { paths: paths });' in js
    assert lhm._UI_ICONS["bar_chart"] in js
    for label in [
        "Columns",
        "Row Groups",
        "Rows",
        "Z-Order",
        "Liquid Clustering",
        "Auto-compaction",
        "V-Order",
    ]:
        assert f'label: "{label}", ' in js, label
    # The stat columns appear once any table has (or is collecting) stats.
    assert "(showStats || !column.stats)" in js
    assert '"stats": _collect_stats' in _source()


def test_data_files_are_located_relative_to_their_table():
    table = "Tables/dbo/sales"
    uri = (
        "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/dbo/sales/"
        "region%3Deu%20west/part-0.parquet"
    )

    assert lhm._relative_data_file(uri, table) == "region=eu west/part-0.parquet"
    assert (
        lhm._relative_data_file("abfss://x/lh/Tables/other/part-0.parquet", table)
        is None
    )


def test_zorder_and_clustering_are_read_from_the_history():
    import pandas as pd

    history = pd.DataFrame(
        [
            {
                "version": 3,
                "operation": "OPTIMIZE",
                "operationParameters": {"zOrderBy": "[]"},
            },
            {"version": 4, "operation": "WRITE", "operationParameters": {}},
            {
                "version": 2,
                "operation": "OPTIMIZE",
                "operationParameters": {"zOrderBy": '["region","day"]'},
            },
            {
                "version": 0,
                "operation": "CREATE TABLE",
                "operationParameters": {"clusterBy": '["id"]'},
            },
        ]
    )

    assert lhm._history_zorder(history) == ["region", "day"]
    assert lhm._history_clustering(history) == ["id"]
    # A later CLUSTER BY replaces the columns; CLUSTER BY NONE removes them.
    altered = [
        {
            "version": 5,
            "operation": "CLUSTER BY",
            "operationParameters": {"newClusteringColumns": "[]"},
        },
        {
            "version": 0,
            "operation": "CREATE TABLE",
            "operationParameters": {"clusterBy": '["id"]'},
        },
    ]
    assert lhm._history_clustering(altered) == []
    assert lhm._history_zorder([]) == []
    assert lhm._parse_column_list('[["a", "b"], "c"]') == ["a.b", "c"]
    assert lhm._parse_column_list("x, y") == ["x", "y"]


def test_row_groups_and_vorder_are_read_from_the_parquet_footers(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    plain = pa.table({"id": list(range(10))})
    vordered = plain.replace_schema_metadata(
        {"com.microsoft.parquet.vorder.enabled": "true"}
    )
    (tmp_path / "region=eu").mkdir()
    pq.write_table(plain, tmp_path / "region=eu" / "part-0.parquet", row_group_size=4)
    pq.write_table(vordered, tmp_path / "part-1.parquet")

    assert lhm._footer_stats(str(tmp_path), ["region=eu/part-0.parquet"]) == (
        3,
        10,
        False,
        0,
    )
    assert lhm._footer_stats(
        str(tmp_path), ["region=eu/part-0.parquet", "part-1.parquet", "missing.parquet"]
    ) == (4, 20, True, 1)


def test_table_stats_combine_the_delta_metadata_footers_and_history(
    monkeypatch, tmp_path
):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from sempy_labs import _helper_functions

    table_dir = tmp_path / "Tables" / "dbo" / "sales"
    table_dir.mkdir(parents=True)
    pq.write_table(pa.table({"id": [1, 2, 3]}), table_dir / "part-0.parquet")
    seen = {}

    def fake_details(path, table_rel):
        seen["path"], seen["table_rel"] = path, table_rel
        return {
            "columns": 4,
            "rows": 2,
            "files": ["part-0.parquet"],
            "properties": {"delta.autoOptimize.autoCompact": "true"},
            "clustering": ["id"],
        }

    monkeypatch.setattr(_helper_functions, "_pure_python_notebook", lambda: False)
    monkeypatch.setattr(
        _helper_functions,
        "create_abfss_path",
        lambda lakehouse_id, workspace_id, table, schema=None: (
            f"abfss://{workspace_id}@onelake/{lakehouse_id}/Tables/{schema}/{table}"
        ),
    )
    monkeypatch.setattr(lhm, "_delta_details_spark", fake_details)
    monkeypatch.setattr(
        _helper_functions,
        "_read_delta_table_history",
        lambda path: [
            {
                "version": 1,
                "operation": "OPTIMIZE",
                "operationParameters": {"zOrderBy": '["id"]'},
            }
        ],
    )

    stats = lhm._table_stats("ws-1", "lh-a", "dbo", "sales", str(tmp_path))

    assert seen["table_rel"] == "Tables/dbo/sales"
    assert seen["path"].endswith("/lh-a/Tables/dbo/sales")
    assert stats == {
        "columns": 4,
        # Spark's count, which leaves out rows removed by deletion vectors.
        "rows": 2,
        "row_groups": 1,
        "z_order": ["id"],
        "liquid_clustering": ["id"],
        "auto_compaction": True,
        "v_order": False,
        "unread_files": 0,
    }


def test_stats_are_collected_for_each_selected_table(manager, monkeypatch):
    from sempy_labs import _helper_functions

    widget, act, _, _ = manager
    calls = []

    def fake_stats(workspace_id, lakehouse_id, schema, table, local_root):
        calls.append((workspace_id, lakehouse_id, table, local_root))
        if table == "orders":
            raise RuntimeError("unreadable")
        return {"columns": 3, "rows": 10}

    monkeypatch.setattr(lhm, "_table_stats", fake_stats)
    monkeypatch.setattr(
        _helper_functions, "_mount", lambda lakehouse=None, workspace=None: "/mnt/lh"
    )

    act("stats", paths=["Tables/sales", "Tables/orders", "Files/a-only.csv"])

    assert calls == [
        ("ws-1", "lh-a", "sales", "/mnt/lh"),
        ("ws-1", "lh-a", "orders", "/mnt/lh"),
    ]
    assert widget.table_stats["Tables/sales"] == {"columns": 3, "rows": 10}
    assert widget.table_stats["Tables/orders"] == {"error": "unreadable"}
    assert widget.stats_pending == []
    assert widget.status_message == "Collected the stats for 1 of 2 tables."
    assert "not a table of the loaded lakehouse" in widget.error_message
    assert (
        "Could not collect the stats for 'orders': unreadable" in widget.error_message
    )


def test_maintenance_drops_the_stale_stats_of_its_tables(manager, monkeypatch):
    from sempy_labs.lakehouse import _lakehouse

    widget, act, _, _ = manager
    widget.table_stats = {"Tables/sales": {"rows": 1}, "Tables/orders": {"rows": 2}}
    monkeypatch.setattr(_lakehouse, "run_table_maintenance", lambda **kwargs: None)

    act("maintenance", paths=["Tables/sales"], optimize=True)

    assert widget.table_stats == {"Tables/orders": {"rows": 2}}


class _PanicException(BaseException):
    """Like the Rust panics native readers raise: not an Exception."""


def test_a_table_which_panics_fails_alone_and_later_runs_still_work(
    manager, monkeypatch
):
    from sempy_labs import _helper_functions

    widget, act, _, _ = manager

    def fake_stats(workspace_id, lakehouse_id, schema, table, local_root):
        if table == "orders":
            raise _PanicException("reader panicked")
        return {"rows": len(table)}

    monkeypatch.setattr(lhm, "_table_stats", fake_stats)
    monkeypatch.setattr(
        _helper_functions, "_mount", lambda lakehouse=None, workspace=None: "/mnt"
    )

    act("stats", paths=["Tables/orders"])
    assert widget.table_stats["Tables/orders"] == {"error": "reader panicked"}
    assert (
        "Could not collect the stats for 'orders': reader panicked"
        in widget.error_message
    )
    assert (widget.busy, widget.stats_pending) == (False, [])

    act("stats", paths=["Tables/sales"])
    assert widget.table_stats["Tables/sales"] == {"rows": 5}
    assert widget.status_message == "Collected the stats for 'sales'."


def test_an_action_which_fails_unexpectedly_is_reported(manager, monkeypatch):
    widget, act, _, _ = manager

    def broken(**kwargs):
        raise _PanicException("boom")

    monkeypatch.setattr(lhm, "_authorized_paths", lambda *a: broken())

    act("stats", paths=["Tables/sales"])

    assert widget.error_message == "The action failed: boom"
    assert widget.busy is False


def _schema(*names, mapping=False):
    fields = []
    for index, name in enumerate(names):
        metadata = (
            {"delta.columnMapping.physicalName": f"col-{index}"} if mapping else {}
        )
        fields.append(
            {"name": name, "type": "long", "nullable": True, "metadata": metadata}
        )
    return json.dumps({"type": "struct", "fields": fields})


def _commit(log_dir, version, *actions):
    log_dir.mkdir(parents=True, exist_ok=True)
    with open(log_dir / f"{version:020d}.json", "w") as handle:
        for action in actions:
            handle.write(json.dumps(action) + "\n")


def _add(path, records=None, dv=0):
    action = {"path": path, "size": 10, "dataChange": True}
    if records is not None:
        action["stats"] = json.dumps({"numRecords": records})
    if dv:
        action["deletionVector"] = {
            "storageType": "u",
            "pathOrInlineDv": "x",
            "sizeInBytes": 1,
            "cardinality": dv,
        }
    return {"add": action}


def test_the_delta_log_is_replayed_without_spark(tmp_path):
    log = tmp_path / "_delta_log"
    _commit(
        log,
        0,
        {"protocol": {"minReaderVersion": 3, "minWriterVersion": 7}},
        {
            "metaData": {
                "schemaString": _schema("id", "region", mapping=True),
                "configuration": {"delta.autoOptimize.autoCompact": "true"},
            }
        },
        {
            "domainMetadata": {
                "domain": "delta.clustering",
                "removed": False,
                "configuration": json.dumps({"clusteringColumns": [["col-1"]]}),
            }
        },
        _add("part-0.parquet", 10),
        _add("region%3Deu/part-1.parquet", 5),
    )
    _commit(
        log,
        1,
        {
            "commitInfo": {
                "operation": "OPTIMIZE",
                "operationParameters": {"zOrderBy": '["id"]'},
            }
        },
        {"remove": {"path": "part-0.parquet"}},
        _add("part-2.parquet", 10, dv=3),
    )
    _commit(log, 2, {"commitInfo": {"operation": "WRITE", "operationParameters": {}}})

    details = lhm._delta_log_details(str(tmp_path))

    assert details["columns"] == 2
    # 5 + 10 rows, less the 3 a deletion vector marks deleted.
    assert details["rows"] == 12
    assert sorted(details["files"]) == ["part-2.parquet", "region=eu/part-1.parquet"]
    assert details["properties"] == {"delta.autoOptimize.autoCompact": "true"}
    # Clustering columns are stored by their physical (column mapping) names.
    assert details["clustering"] == ["region"]
    assert details["z_order"] == ["id"]


def test_the_delta_log_is_read_from_its_latest_checkpoint(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    log = tmp_path / "_delta_log"
    # Commits before the checkpoint have been cleaned up.
    add_type = pa.struct(
        [("path", pa.string()), ("size", pa.int64()), ("stats", pa.string())]
    )
    meta_type = pa.struct(
        [
            ("schemaString", pa.string()),
            ("configuration", pa.map_(pa.string(), pa.string())),
        ]
    )
    checkpoint = pa.table(
        {
            "add": pa.array(
                [
                    {
                        "path": "a.parquet",
                        "size": 1,
                        "stats": json.dumps({"numRecords": 7}),
                    },
                    None,
                    None,
                ],
                type=add_type,
            ),
            "metaData": pa.array(
                [
                    None,
                    {
                        "schemaString": _schema("x", "y", "z"),
                        "configuration": [("delta.parquet.vorder.enabled", "true")],
                    },
                    None,
                ],
                type=meta_type,
            ),
        }
    )
    log.mkdir()
    pq.write_table(
        checkpoint, log / f"{10:020d}.checkpoint.0000000001.0000000002.parquet"
    )
    pq.write_table(
        checkpoint.slice(0, 0),
        log / f"{10:020d}.checkpoint.0000000002.0000000002.parquet",
    )
    _commit(log, 11, _add("b.parquet", 3))

    details = lhm._delta_log_details(str(tmp_path))

    assert details["columns"] == 3
    assert details["rows"] == 10
    assert sorted(details["files"]) == ["a.parquet", "b.parquet"]
    assert details["properties"] == {"delta.parquet.vorder.enabled": "true"}
    assert details["clustering"] == []


def test_a_delta_log_which_cannot_be_replayed_asks_for_pyspark(tmp_path):
    log = tmp_path / "_delta_log"
    # Commit 0 is gone and there is no classic checkpoint to start from.
    _commit(log, 1, _add("a.parquet", 1))

    with pytest.raises(ValueError, match="PySpark notebook"):
        lhm._delta_log_details(str(tmp_path))


def test_python_notebooks_read_table_stats_from_the_delta_log(monkeypatch, tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from sempy_labs import _helper_functions

    table_dir = tmp_path / "Tables" / "sales"
    pq.write_table(
        pa.table({"id": [1, 2, 3, 4]}), _mkfile(table_dir / "part-0.parquet")
    )
    _commit(
        table_dir / "_delta_log",
        0,
        {"metaData": {"schemaString": _schema("id"), "configuration": {}}},
        _add("part-0.parquet"),
    )
    monkeypatch.setattr(_helper_functions, "_pure_python_notebook", lambda: True)

    stats = lhm._table_stats("ws-1", "lh-a", "", "sales", str(tmp_path))

    # Without file stats in the log, the rows come from the parquet footers.
    assert (stats["columns"], stats["rows"], stats["row_groups"]) == (1, 4, 1)


def _mkfile(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def test_the_table_selection_is_cleared_once_its_operation_finishes(
    manager, monkeypatch
):
    from sempy_labs.lakehouse import _lakehouse

    widget, act, _, _ = manager
    monkeypatch.setattr(_lakehouse, "run_table_maintenance", lambda **kwargs: None)

    act("maintenance", paths=["Tables/sales"], optimize=True)

    # Python reports which action finished; the browser then clears the ticks
    # of the maintenance or stats run it started from the selection.
    assert widget.action_done == widget.action_trigger
    js = lhm._LHM_JS
    assert 'if (paths.length) clearTablesAfter = act("stats", { paths: paths });' in js
    assert "if (fromSelection) clearTablesAfter = trigger;" in js
    assert 'model.on("change:action_done", () => {' in js


def test_bar_columns_start_at_their_set_width():
    # The column resizers take a column's starting width from data-column-width.
    assert "th.dataset.columnWidth = String(column.width);" in lhm._LHM_JS
    assert '{ key: "size", label: "Size", num: true, width: 160 }' in lhm._LHM_JS


def test_table_settings_are_changed_with_spark_sql_on_the_tables_path():
    path = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/dbo/sales"

    assert lhm._auto_compaction_sql(path, True) == (
        f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES "
        "('delta.autoOptimize.autoCompact' = 'true')"
    )
    assert lhm._auto_compaction_sql(path, False).endswith("= 'false')")
    assert lhm._clustering_sql(path, ["region", "odd`name"]) == (
        f"ALTER TABLE delta.`{path}` CLUSTER BY (`region`, `odd``name`)"
    )
    assert (
        lhm._clustering_sql(path, []) == f"ALTER TABLE delta.`{path}` CLUSTER BY NONE"
    )


def test_the_table_view_offers_auto_compaction_and_clustering():
    js = lhm._LHM_JS

    assert (
        "tableSelection.append(maintainSelected, statsSelected, autoOnSelected, "
        "autoOffSelected, tableSelectedText);" in js
    )
    assert 'act("auto_compaction", { paths: paths, enable: enable });' in js
    assert 'clusterRequest = act("cluster_columns", { path: row.path });' in js
    assert 'act("clustering", payload);' in js
    assert (
        'if (column.key !== "liquid_clustering" || !maintainable(row) || pending) return cell;'
        in js
    )
    assert "const MAX_CLUSTERING_COLUMNS = 4;" in js
    for icon in ("collapse_diagonal", "expand_diagonal", "pencil"):
        assert lhm._UI_ICONS[icon] in js
    source = _source()
    for handler in (
        '"auto_compaction": _auto_compaction',
        '"cluster_columns": _cluster_columns',
        '"clustering": _set_clustering',
    ):
        assert handler in source


@pytest.fixture
def spark_sql(manager, monkeypatch):
    """The manager, with Spark SQL statements recorded rather than run."""

    from sempy_labs import _helper_functions

    widget, act, _, _ = manager
    statements = []
    monkeypatch.setattr(_helper_functions, "_run_spark_sql_query", statements.append)
    monkeypatch.setattr(
        _helper_functions,
        "create_abfss_path",
        lambda lakehouse_id, workspace_id, table, schema=None: f"abfss://{workspace_id}/{lakehouse_id}/Tables/{table}",
    )
    monkeypatch.setattr(
        lhm,
        "_table_columns_spark",
        lambda path: {
            "columns": [
                {"name": "id", "type": "bigint", "clusterable": True},
                {"name": "region", "type": "string", "clusterable": True},
                {"name": "tags", "type": "array<string>", "clusterable": False},
            ],
            "clustering": ["id"],
        },
    )
    return widget, act, statements


def test_auto_compaction_is_turned_on_for_the_selected_tables(spark_sql):
    widget, act, statements = spark_sql
    widget.table_stats = {"Tables/sales": {"rows": 1, "auto_compaction": False}}

    act(
        "auto_compaction",
        paths=["Tables/sales", "Tables/orders", "Files/a-only.csv"],
        enable=True,
    )

    assert statements == [
        lhm._auto_compaction_sql("abfss://ws-1/lh-a/Tables/sales", True),
        lhm._auto_compaction_sql("abfss://ws-1/lh-a/Tables/orders", True),
    ]
    # Collected stats reflect the change.
    assert widget.table_stats["Tables/sales"]["auto_compaction"] is True
    assert widget.status_message == "Turned auto-compaction on for 2 of 2 tables."
    assert "not a table of the loaded lakehouse" in widget.error_message
    assert widget.action_done == widget.action_trigger


def test_the_clustering_dialog_reads_the_tables_columns(spark_sql):
    widget, act, _ = spark_sql

    act("cluster_columns", path="Tables/sales")

    assert widget.cluster_info["path"] == "Tables/sales"
    assert widget.cluster_info["clustering"] == ["id"]
    assert [c["name"] for c in widget.cluster_info["columns"]] == [
        "id",
        "region",
        "tags",
    ]


def test_liquid_clustering_is_changed_and_removed(spark_sql):
    widget, act, statements = spark_sql
    widget.table_stats = {"Tables/sales": {"rows": 1, "liquid_clustering": ["id"]}}

    act("clustering", path="Tables/sales", columns=["region", "id"])
    assert statements[-1] == lhm._clustering_sql(
        "abfss://ws-1/lh-a/Tables/sales", ["region", "id"]
    )
    assert widget.table_stats["Tables/sales"]["liquid_clustering"] == ["region", "id"]
    assert widget.status_message.startswith("Clustered 'sales' by region, id.")

    act("clustering", path="Tables/sales", columns=[])
    assert statements[-1].endswith("CLUSTER BY NONE")
    assert widget.status_message == "Removed liquid clustering from 'sales'."


def test_only_the_tables_clusterable_columns_are_accepted(spark_sql):
    widget, act, statements = spark_sql

    act("clustering", path="Tables/sales", columns=["tags"])
    assert statements == []
    assert "not a column of the table which can be clustered" in widget.error_message

    act("clustering", path="Tables/sales", columns=["a", "b", "c", "d", "e"])
    assert statements == []
    assert "Choose up to 4 different clustering columns." == widget.error_message


def test_table_settings_require_a_pyspark_notebook(spark_sql):
    widget, act, statements = spark_sql
    widget.spark_available = False

    act("auto_compaction", paths=["Tables/sales"], enable=False)
    assert "requires a PySpark notebook" in widget.error_message

    act("cluster_columns", path="Tables/sales")
    assert "requires a PySpark notebook" in widget.cluster_info["error"]

    act("clustering", path="Tables/sales", columns=["id"])
    assert statements == []


def test_the_checkbox_schema_and_name_columns_are_frozen():
    js = lhm._LHM_JS
    css = lhm._LHM_CSS

    assert "const frozen = showSchema ? 3 : 2;" in js
    # Their offsets follow the widths of the columns before them, also on resize.
    assert "onWidthsChanged: (resized, widths) => {" in js
    assert 'tr.cells[i].style.left = left + "px";' in js
    assert ".slls-lhm-table td.slls-lhm-frozen {" in css
    # Opaque, so the cells scrolling under them do not show through.
    assert (
        "background: linear-gradient(var(--ui-surface-2), var(--ui-surface-2)), var(--ui-bg);"
        in css
    )


def _typed(name, kind, size=100):
    blob = _blob(name, size)
    blob["Resource Type"] = kind
    return blob


def test_files_beside_the_tables_are_not_tables():
    blobs = [
        _typed("Tables/sales", "directory", 0),
        _typed("Tables/sales/part-0.parquet", "file"),
        _typed("Tables/empty_table", "directory", 0),
        _typed("Tables/schema.json.gz", "file"),
    ]

    rows = {row["path"]: row for row in lhm._summarize_blobs(blobs, LAKEHOUSE_ID)}

    assert rows["Tables/sales"]["type"] == "Table"
    # A table folder with nothing in it yet is still a table.
    assert rows["Tables/empty_table"]["type"] == "Table"
    assert rows["Tables/schema.json.gz"]["type"] == "File"
    assert rows["Tables/schema.json.gz"]["file_sizes"] is None
    assert set(lhm._maintainable_tables(rows.values())) == {
        "Tables/sales",
        "Tables/empty_table",
    }
    assert lhm._summarize_objects(list(rows.values()))["tables"] == 2

    tables = lhm._build_tree(blobs, LAKEHOUSE_ID)[0]["children"]
    assert {node["name"]: node["type"] for node in tables} == {
        "sales": "Table",
        "empty_table": "Table",
        "schema.json.gz": "File",
    }


def test_files_beside_a_schemas_tables_are_not_tables():
    blobs = [
        _typed("Tables/dbo", "directory", 0),
        _typed("Tables/dbo/schema.json.gz", "file"),
        _typed("Tables/dbo/sales/part-0.parquet", "file"),
    ]

    rows = {
        row["path"]: row
        for row in lhm._summarize_blobs(blobs, LAKEHOUSE_ID, schema_enabled=True)
    }

    assert rows["Tables/dbo/sales"]["type"] == "Table"
    assert rows["Tables/dbo/schema.json.gz"]["type"] == "File"
    schema = lhm._build_tree(blobs, LAKEHOUSE_ID, schema_enabled=True)[0]["children"][0]
    assert {node["name"]: node["type"] for node in schema["children"]} == {
        "sales": "Table",
        "schema.json.gz": "File",
    }


def test_the_table_view_lists_only_tables():
    assert 'filter((row) => row.type === "Table")' in lhm._LHM_JS


def test_stats_are_only_collected_for_the_selected_tables():
    import inspect

    js = lhm._LHM_JS
    assert "collect_stats" not in inspect.signature(lhm.lakehouse_manager).parameters
    assert "pumpStats" not in js and "auto_stats" not in js
    assert "background: true" not in js


def test_the_mount_is_reused_across_stats_runs(manager, monkeypatch):
    from sempy_labs import _helper_functions

    widget, act, _, _ = manager
    mounts = []

    def fake_mount(lakehouse=None, workspace=None):
        mounts.append(lakehouse)
        return "/mnt/lh"

    monkeypatch.setattr(lhm, "_table_stats", lambda *args: {"rows": 1})
    monkeypatch.setattr(_helper_functions, "_mount", fake_mount)

    act("stats", paths=["Tables/sales"])
    act("stats", paths=["Tables/orders"])

    assert set(widget.table_stats) == {"Tables/sales", "Tables/orders"}
    assert mounts == ["lh-a"]


def test_bulk_table_actions_can_be_cancelled_between_tables():
    js = lhm._LHM_JS

    assert "body.append(cards, toolbar, batchBar, listWrap, note);" in js
    assert 'act(cancelRequested ? "batch_cancel" : "batch_next");' in js
    # The next table is only asked for once the kernel is idle.
    assert (
        'if ((model.get("action_done") || 0) < (model.get("action_trigger") || 0)) return;'
        in js
    )
    source = _source()
    assert '"batch_next": _next_in_batch' in source
    assert '"batch_cancel": _cancel_batch' in source


def test_cancelling_stats_stops_after_the_table_in_progress(manager, monkeypatch):
    from sempy_labs import _helper_functions

    widget, act, _, _ = manager
    monkeypatch.setattr(
        lhm, "_table_stats", lambda w, l, s, table, r: {"rows": len(table)}
    )
    monkeypatch.setattr(
        _helper_functions, "_mount", lambda lakehouse=None, workspace=None: "/mnt"
    )

    act("stats", paths=["Tables/sales", "Tables/orders"], _drain=False)

    # One table per request: the first is done, the next is waiting.
    assert widget.batch == {
        "title": "Collecting stats",
        "total": 2,
        "done": 1,
        "current": "",
    }
    assert widget.busy is True
    assert widget.stats_pending == ["Tables/orders"]
    assert set(widget.table_stats) == {"Tables/sales"}

    act("batch_cancel")

    assert widget.batch == {}
    assert widget.busy is False
    assert widget.stats_pending == []
    assert set(widget.table_stats) == {"Tables/sales"}
    assert widget.status_message == "Stopped collecting stats after 1 of 2 tables."


def test_cancelling_table_maintenance_and_auto_compaction(spark_sql, monkeypatch):
    from sempy_labs.lakehouse import _lakehouse

    widget, act, statements = spark_sql
    maintained = []
    monkeypatch.setattr(
        _lakehouse,
        "run_table_maintenance",
        lambda **kwargs: maintained.append(kwargs["table_name"]),
    )

    act(
        "maintenance",
        paths=["Tables/sales", "Tables/orders"],
        optimize=True,
        _drain=False,
    )
    assert widget.batch["title"] == "Running table maintenance"
    act("batch_cancel")
    assert maintained == ["sales"]
    assert widget.status_message == "Cancelled table maintenance after 1 of 2 tables."

    act(
        "auto_compaction",
        paths=["Tables/sales", "Tables/orders"],
        enable=True,
        _drain=False,
    )
    act("batch_cancel")
    assert len(statements) == 1
    assert widget.status_message == (
        "Cancelled turning auto-compaction on after 1 of 2 tables."
    )
    assert (widget.batch, widget.busy) == ({}, False)


def test_a_maintenance_job_which_did_not_succeed_is_a_failure(manager, monkeypatch):
    import pandas as pd
    from sempy_labs.lakehouse import _lakehouse

    widget, act, _, _ = manager
    monkeypatch.setattr(
        _lakehouse,
        "run_table_maintenance",
        lambda **kwargs: pd.DataFrame([{"Status": "Failed"}]),
    )

    act("maintenance", paths=["Tables/sales"], optimize=True)

    assert widget.status_message == ""
    assert (
        "Table maintenance failed for 'sales': the maintenance job ended as 'Failed'."
        in widget.error_message
    )
