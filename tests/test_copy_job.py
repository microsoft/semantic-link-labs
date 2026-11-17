import base64
import json
from types import SimpleNamespace

import pandas as pd
import pytest

import sempy_labs.copy_job._items as copy_job_items

WORKSPACE_ID = "cfafbeb1-8037-4d0c-896e-a46fb27ff229"
ITEM_ID = "5b218778-e7a5-4d73-8187-f10824047715"


def _b64(content: str) -> str:
    return base64.b64encode(content.encode("utf-8")).decode("utf-8")


@pytest.fixture
def api_calls(monkeypatch):
    """Patches name resolution and records every _base_api call."""
    calls = []
    responses = {}

    def fake_base_api(**kwargs):
        calls.append(kwargs)
        return responses.get("result")

    monkeypatch.setattr(copy_job_items, "_base_api", fake_base_api)
    monkeypatch.setattr(copy_job_items, "resolve_workspace_id", lambda w: WORKSPACE_ID)
    monkeypatch.setattr(
        copy_job_items,
        "resolve_workspace_name_and_id",
        lambda w: ("My Workspace", WORKSPACE_ID),
    )
    monkeypatch.setattr(copy_job_items, "resolve_item_id", lambda **kw: ITEM_ID)
    monkeypatch.setattr(
        copy_job_items,
        "resolve_item_name_and_id",
        lambda **kw: ("My Copy Job", ITEM_ID),
    )
    return SimpleNamespace(calls=calls, responses=responses)


def test_list_copy_jobs_parses_response(api_calls):
    api_calls.responses["result"] = [
        {
            "value": [
                {
                    "id": ITEM_ID,
                    "displayName": "CopyJob 1",
                    "description": "desc",
                    "folderId": "f1",
                    "sensitivityLabel": {"id": "label1"},
                },
                {"id": "id2", "displayName": "CopyJob 2"},
            ]
        }
    ]

    df = copy_job_items.list_copy_jobs()

    assert list(df["Copy Job Name"]) == ["CopyJob 1", "CopyJob 2"]
    assert df.loc[0, "Sensitivity Label Id"] == "label1"
    assert pd.isna(df.loc[1, "Sensitivity Label Id"])
    assert api_calls.calls[0]["request"] == (
        f"/v1/workspaces/{WORKSPACE_ID}/copyJobs?recursive=true"
    )
    assert api_calls.calls[0]["uses_pagination"] is True


def test_list_copy_jobs_empty_has_columns(api_calls):
    api_calls.responses["result"] = [{"value": []}]

    df = copy_job_items.list_copy_jobs()

    assert df.empty
    assert "Copy Job Id" in df.columns


def test_list_copy_jobs_root_folder_id_and_non_recursive(api_calls):
    api_calls.responses["result"] = []
    folder_id = "11111111-2222-4333-8444-555555555555"

    copy_job_items.list_copy_jobs(recursive=False, root_folder=folder_id)

    assert api_calls.calls[0]["request"] == (
        f"/v1/workspaces/{WORKSPACE_ID}/copyJobs?recursive=false&rootFolderId={folder_id}"
    )


def test_get_copy_job_returns_single_row(api_calls):
    api_calls.responses["result"] = SimpleNamespace(
        json=lambda: {"id": ITEM_ID, "displayName": "CopyJob 1"}
    )

    df = copy_job_items.get_copy_job("CopyJob 1")

    assert len(df) == 1
    assert df.loc[0, "Copy Job Id"] == ITEM_ID
    assert api_calls.calls[0]["request"] == (
        f"/v1/workspaces/{WORKSPACE_ID}/copyJobs/{ITEM_ID}"
    )


def test_create_copy_job_encodes_decoded_payloads(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        copy_job_items, "create_item", lambda **kwargs: captured.update(kwargs)
    )
    content = {"properties": {"jobMode": "Batch"}, "activities": []}

    copy_job_items.create_copy_job(
        name="CopyJob 1",
        definition={"parts": [{"path": "copyjob-content.json", "payload": content}]},
    )

    part = captured["definition"]["parts"][0]
    assert captured["type"] == "CopyJob"
    assert part["payloadType"] == "InlineBase64"
    assert json.loads(base64.b64decode(part["payload"])) == content


def test_create_copy_job_without_definition(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        copy_job_items, "create_item", lambda **kwargs: captured.update(kwargs)
    )

    copy_job_items.create_copy_job(name="CopyJob 1", description="desc")

    assert captured["definition"] is None
    assert captured["description"] == "desc"


def test_update_copy_job_sends_patch(api_calls):
    copy_job_items.update_copy_job("CopyJob 1", name="New Name")

    call = api_calls.calls[0]
    assert call["method"] == "patch"
    assert call["payload"] == {"displayName": "New Name"}


def test_update_copy_job_requires_a_change(api_calls):
    with pytest.raises(ValueError, match="'name' or 'description'"):
        copy_job_items.update_copy_job("CopyJob 1")
    assert api_calls.calls == []


@pytest.mark.parametrize(
    "hard_delete,suffix", [(False, ""), (True, "?hardDelete=true")]
)
def test_delete_copy_job(api_calls, hard_delete, suffix):
    copy_job_items.delete_copy_job("CopyJob 1", hard_delete=hard_delete)

    call = api_calls.calls[0]
    assert call["method"] == "delete"
    assert call["request"] == (
        f"/v1/workspaces/{WORKSPACE_ID}/copyJobs/{ITEM_ID}{suffix}"
    )


def test_get_copy_job_definition_decodes_parts(api_calls):
    content = {"properties": {"jobMode": "CDC"}, "activities": []}
    api_calls.responses["result"] = {
        "definition": {
            "parts": [
                {
                    "path": "copyjob-content.json",
                    "payload": _b64(json.dumps(content)),
                    "payloadType": "InlineBase64",
                }
            ]
        }
    }

    result = copy_job_items.get_copy_job_definition("CopyJob 1")

    part = result["definition"]["parts"][0]
    assert part["payload"] == content
    assert part["payloadType"] == "InlineBase64"
    assert api_calls.calls[0]["request"].endswith(f"/copyJobs/{ITEM_ID}/getDefinition")
    assert api_calls.calls[0]["lro_return_json"] is True


def test_get_copy_job_definition_dataframe(api_calls):
    raw = _b64(json.dumps({"a": 1}))
    api_calls.responses["result"] = {
        "definition": {
            "parts": [
                {"path": ".platform", "payload": raw, "payloadType": "InlineBase64"}
            ]
        }
    }

    df = copy_job_items.get_copy_job_definition(
        "CopyJob 1", decode=False, return_dataframe=True
    )

    assert list(df.columns) == ["Path", "Payload", "Payload Type"]
    assert df.loc[0, "Payload"] == raw


def test_update_copy_job_definition_round_trip(api_calls):
    content = {"properties": {"jobMode": "CDC"}, "activities": []}
    already_encoded = _b64("platform")

    copy_job_items.update_copy_job_definition(
        "CopyJob 1",
        definition={
            "definition": {
                "parts": [
                    {"path": "copyjob-content.json", "payload": content},
                    {
                        "path": ".platform",
                        "payload": already_encoded,
                        "payloadType": "InlineBase64",
                    },
                ]
            }
        },
        update_metadata=True,
    )

    call = api_calls.calls[0]
    parts = call["payload"]["definition"]["parts"]
    assert call["request"].endswith("/updateDefinition?updateMetadata=True")
    assert json.loads(base64.b64decode(parts[0]["payload"])) == content
    assert parts[1]["payload"] == already_encoded


def test_update_copy_job_definition_requires_parts(api_calls):
    with pytest.raises(ValueError, match="non-empty list"):
        copy_job_items.update_copy_job_definition("CopyJob 1", definition={})
    assert api_calls.calls == []


def test_reset_copy_job_all_entities(api_calls):
    copy_job_items.reset_copy_job("CopyJob 1")

    call = api_calls.calls[0]
    assert call["request"].endswith(f"/copyJobs/{ITEM_ID}/resetCopyJob")
    assert call["payload"] == {"resetAllCopyJobEntities": True}


def test_reset_copy_job_selected_entities(api_calls):
    copy_job_items.reset_copy_job("CopyJob 1", copy_job_entities=["e1", "e2"])

    assert api_calls.calls[0]["payload"] == {
        "resetAllCopyJobEntities": False,
        "copyJobEntitiesToReset": [
            {"copyJobEntityId": "e1"},
            {"copyJobEntityId": "e2"},
        ],
    }
