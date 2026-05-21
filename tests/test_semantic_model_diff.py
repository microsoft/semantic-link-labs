"""Tests for the pure-Python diff/patch helpers used by
``sempy_labs.semantic_model.semantic_model_diff``.

These tests focus on the deterministic, network-free pieces of the
implementation (diff computation and patch application). The interactive
widget itself is only exercised at import time.
"""

from sempy_labs.semantic_model._semantic_model_diff import (
    _compute_bim_diff,
    _compute_tmdl_diff,
    _patch_bim,
    _patch_tmdl,
    _diff_counts,
)


def _ids(diff):
    return [it["id"] for c in diff["categories"] for it in c["items"]]


def test_bim_diff_detects_added_removed_modified():
    src = {
        "model": {
            "tables": [
                {
                    "name": "Sales",
                    "columns": [{"name": "Amount", "dataType": "decimal"}],
                    "measures": [
                        {"name": "Rev", "expression": "SUM(Sales[Amount])"}
                    ],
                },
                {"name": "OnlyInSource"},
            ]
        }
    }
    tgt = {
        "model": {
            "tables": [
                {
                    "name": "Sales",
                    "columns": [{"name": "Amount", "dataType": "int64"}],
                    "measures": [
                        {"name": "Rev", "expression": "SUM(Sales[Amount])"},
                        {"name": "OldMeas", "expression": "1"},
                    ],
                },
                {"name": "OnlyInTarget"},
            ]
        }
    }

    diff, _ = _compute_bim_diff(src, tgt)
    counts = _diff_counts(diff)

    assert counts["added"] == 1     # OnlyInSource
    assert counts["removed"] == 2   # OnlyInTarget + OldMeas
    assert counts["modified"] == 1  # Sales.Amount column type changed

    keys = {c["key"] for c in diff["categories"]}
    assert "tables" in keys
    assert "columns" in keys
    assert "measures" in keys


def test_bim_patch_s2t_applies_only_selected():
    src = {
        "model": {
            "tables": [
                {
                    "name": "Sales",
                    "measures": [{"name": "Rev", "expression": "A"}],
                }
            ],
            "relationships": [
                {"name": "r1", "fromTable": "A", "toTable": "B"}
            ],
        }
    }
    tgt = {
        "model": {
            "tables": [
                {
                    "name": "Sales",
                    "measures": [{"name": "Rev", "expression": "B"}],
                }
            ],
            "relationships": [],
        }
    }
    diff, idx = _compute_bim_diff(src, tgt)
    ids = _ids(diff)
    measure_id = next(i for i in ids if "Rev" in i)

    patched = _patch_bim(src, tgt, idx, [measure_id], "s2t")

    # Selected measure updated on the target side
    assert patched["model"]["tables"][0]["measures"][0]["expression"] == "A"
    # Relationship NOT applied because its id was not selected
    assert patched["model"].get("relationships", []) == []


def test_bim_patch_t2s_reverses_direction():
    src = {"model": {"tables": [
        {"name": "T", "measures": [{"name": "M", "expression": "A"}]}
    ]}}
    tgt = {"model": {"tables": [
        {"name": "T", "measures": [{"name": "M", "expression": "B"}]}
    ]}}
    diff, idx = _compute_bim_diff(src, tgt)
    ids = _ids(diff)

    patched = _patch_bim(src, tgt, idx, ids, "t2s")
    # In t2s, the destination is source, so source's M should now be "B"
    assert patched["model"]["tables"][0]["measures"][0]["expression"] == "B"


def test_bim_patch_handles_full_table_add_remove():
    src = {"model": {"tables": [
        {"name": "A", "columns": [{"name": "c1"}]},
        {"name": "B"},
    ]}}
    tgt = {"model": {"tables": [
        {"name": "B"},
        {"name": "C"},
    ]}}
    diff, idx = _compute_bim_diff(src, tgt)

    # Apply all source-side changes to target -> target should equal source
    patched = _patch_bim(src, tgt, idx, _ids(diff), "s2t")
    names = sorted(t["name"] for t in patched["model"]["tables"])
    assert names == ["A", "B"]


def test_tmdl_diff_and_patch_per_file():
    sf = [
        {
            "file_name": "definition/tables/A.tmdl",
            "content": "table A\n  column X int",
        },
        {"file_name": "definition/tables/B.tmdl", "content": "table B"},
    ]
    tf = [
        {
            "file_name": "definition/tables/A.tmdl",
            "content": "table A\n  column X string",
        },
        {"file_name": "definition/tables/C.tmdl", "content": "table C"},
    ]

    diff, idx = _compute_tmdl_diff(sf, tf)
    kinds = {it["name"]: it["kind"]
             for c in diff["categories"] for it in c["items"]}
    assert kinds["A.tmdl"] == "modified"
    assert kinds["B.tmdl"] == "added"
    assert kinds["C.tmdl"] == "removed"

    ids = _ids(diff)
    patched = _patch_tmdl(sf, tf, idx, ids, "s2t")
    names = sorted(f["file_name"] for f in patched)
    # After applying all source changes to target: C is removed,
    # B is added, A is updated.
    assert names == [
        "definition/tables/A.tmdl",
        "definition/tables/B.tmdl",
    ]
    a = next(f for f in patched
             if f["file_name"].endswith("A.tmdl"))["content"]
    assert "column X int" in a
