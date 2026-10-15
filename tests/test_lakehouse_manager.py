"""The Lakehouse Manager rolls OneLake blobs up into lakehouse objects."""

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


def test_tree_objects_can_be_selected_and_acted_on_in_bulk():
    js = lhm._LHM_JS

    assert 'check.setAttribute("aria-label", "Select " + node.path);' in js
    # Containers and schemas are groupings, not objects.
    assert (
        'return node.type !== "Container" && node.type !== "Schema" && node.type !== "More";'
        in js
    )
    assert 'act("recover", { paths: paths })' in js
    assert 'act("delete", { paths: paths })' in js
    # Deleting is confirmed first.
    assert 'confirmOverlay.style.display = "flex";' in js
    assert '"delete": _delete' in _source()


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


def test_only_loaded_objects_and_never_groupings_can_be_targeted():
    blobs = [
        _blob("Tables/dbo/sales/part-0.parquet"),
        _blob("Files/raw/data.csv"),
    ]
    objects = lhm._summarize_blobs(blobs, LAKEHOUSE_ID, schema_enabled=True)
    tree = lhm._build_tree(blobs, LAKEHOUSE_ID, schema_enabled=True)

    allowed = lhm._actionable_paths(objects, tree)

    assert "Tables/dbo/sales" in allowed
    assert "Files/raw/data.csv" in allowed
    assert "Tables/dbo" not in allowed
    assert "Tables" not in allowed
    accepted, rejected = lhm._authorized_paths(
        ["Tables/dbo/sales", "Tables/dbo", "Files/other"], allowed
    )
    assert accepted == ["Tables/dbo/sales"]
    assert rejected == ["Tables/dbo", "Files/other"]


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

    def act(action, **data):
        widget.pending_action = dict(action=action, **data)
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


def test_maintenance_runs_only_on_a_table_of_the_loaded_lakehouse(manager, monkeypatch):
    from sempy_labs.lakehouse import _lakehouse

    widget, act, _, _ = manager
    calls = []
    monkeypatch.setattr(
        _lakehouse, "run_table_maintenance", lambda **kwargs: calls.append(kwargs)
    )

    act("maintenance", path="Tables/other", table="other", optimize=True)
    act("maintenance", path="Tables/sales", table="../x", optimize=True)
    assert calls == []
    assert "not a table of the loaded lakehouse" in widget.error_message

    act("maintenance", path="Tables/sales", table="sales", optimize=True)
    assert len(calls) == 1
    assert calls[0]["table_name"] == "sales"
    assert calls[0]["lakehouse"] == "lh-a"
    assert widget.error_message == ""
