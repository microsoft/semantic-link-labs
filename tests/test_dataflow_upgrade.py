import pandas as pd
import pytest

import sempy_labs.dataflow._upgrade as upgrade

WS_ID = "afced311-0e76-4b74-9d59-6cdcbab44653"
GEN1_A = "6b1f3d2e-8a4c-4b6e-9f1a-2c3d4e5f6a7b"
GEN1_B = "7c2a4e3f-9b5d-4c7f-a02b-3d4e5f6a7b8c"
GEN2_CICD = "8d3b5f40-ac6e-4d80-b13c-4e5f6a7b8c9d"


@pytest.fixture
def patched(monkeypatch):
    calls = []
    monkeypatch.setattr(
        upgrade,
        "resolve_workspace_name_and_id",
        lambda workspace: ("Test Workspace", WS_ID),
    )
    monkeypatch.setattr(upgrade, "resolve_workspace_id", lambda workspace: WS_ID)
    monkeypatch.setattr(
        upgrade,
        "list_dataflows",
        lambda workspace: pd.DataFrame(
            {
                "Dataflow Id": [GEN1_A, GEN1_B, GEN2_CICD],
                "Dataflow Name": ["Sales", "Marketing", "Native"],
                "Generation": ["Gen1", "Gen1", "Gen2 CI/CD"],
            }
        ),
    )

    def fake_base_api(**kwargs):
        calls.append(kwargs)
        if kwargs["request"].endswith("gen1UpgradeReadinessResults"):
            return [
                {
                    "value": [
                        {
                            "id": GEN1_A,
                            "displayName": "Sales",
                            "status": "ReadyToMigrate",
                        },
                        {
                            "id": GEN1_B,
                            "displayName": "Marketing",
                            "status": "NeedsAttention",
                            "reasons": [
                                {
                                    "status": "NeedsAttention",
                                    "code": "ExceedsGen2QueryLimit",
                                    "message": "Too many queries.",
                                }
                            ],
                        },
                    ]
                }
            ]
        return {
            "summary": {"totalCount": 2, "succeededCount": 1, "failedCount": 1},
            "migrations": [
                {
                    "dataflowId": GEN1_A,
                    "dataflowName": "Sales",
                    "targetDataflowName": "Sales",
                    "workspaceId": WS_ID,
                    "workspaceName": "Test Workspace",
                    "assessmentStatus": "ReadyToMigrate",
                    "migrationStatus": "Succeeded",
                    "migrationEndDateTime": "2026-08-19T10:15:42Z",
                },
                {
                    "dataflowId": GEN1_B,
                    "dataflowName": "Marketing",
                    "targetDataflowName": "Marketing",
                    "workspaceId": WS_ID,
                    "workspaceName": "Test Workspace",
                    "assessmentStatus": "NeedsAttention",
                    "requiredActions": [
                        {
                            "status": "NeedsAttention",
                            "code": "HasUnresolvedConnections",
                            "message": "Rebind connections.",
                        }
                    ],
                    "migrationStatus": "Failed",
                    "migrationError": {
                        "errorCode": "DataflowRequiresChanges",
                        "message": "Connections could not be resolved.",
                        "requestId": "0f8fad5b-d9cb-469f-a165-70867728950e",
                        "isRetriable": False,
                    },
                },
            ],
        }

    monkeypatch.setattr(upgrade, "_base_api", fake_base_api)
    return calls


def test_list_dataflow_upgrade_readiness(patched):
    df = upgrade.list_dataflow_upgrade_readiness()

    assert patched[0]["request"] == (
        f"/v1/workspaces/{WS_ID}/dataflows/gen1UpgradeReadinessResults"
    )
    assert patched[0]["uses_pagination"] is True
    assert list(df["Dataflow Id"]) == [GEN1_A, GEN1_B]
    assert list(df["Status"]) == ["ReadyToMigrate", "NeedsAttention"]
    assert df["Reason Codes"].iloc[0] is None
    assert df["Reason Codes"].iloc[1] == ["ExceedsGen2QueryLimit"]


def test_upgrade_dataflow_resolves_names_and_skips_non_gen1(patched, capsys):
    df = upgrade.upgrade_dataflow(dataflow=["Sales", GEN1_B, "Native", GEN1_A])

    call = patched[0]
    assert call["request"] == f"/v1/workspaces/{WS_ID}/dataflows/gen1Upgrade"
    assert call["method"] == "post"
    assert call["lro_return_json"] is True
    assert call["payload"] == {"dataflows": [{"id": GEN1_A}, {"id": GEN1_B}]}

    assert list(df["Migration Status"]) == ["Succeeded", "Failed"]
    assert df["Error Code"].iloc[1] == "DataflowRequiresChanges"
    assert df["Required Actions"].iloc[0] is None
    assert pd.api.types.is_datetime64_any_dtype(df["Migration End Date Time"])

    out = capsys.readouterr().out
    assert "Native" in out and "skipped" in out
    assert "1 of 2" in out


def test_upgrade_dataflow_single_and_deprecated_params(patched, capsys):
    upgrade.upgrade_dataflow(dataflow="Sales", new_dataflow_name="New")

    assert patched[0]["payload"] == {"dataflows": [{"id": GEN1_A}]}
    assert "deprecated" in capsys.readouterr().out


def test_upgrade_dataflow_only_non_gen1_makes_no_call(patched):
    df = upgrade.upgrade_dataflow(dataflow=GEN2_CICD)

    assert patched == []
    assert df.empty


def test_upgrade_dataflow_missing_raises(patched):
    with pytest.raises(ValueError, match="does not exist"):
        upgrade.upgrade_dataflow(dataflow="Missing")
