from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from sempy_labs.surge_protection import _surge_protection as sp

CAPACITY_ID = "cfafbeb1-8037-4d0c-896e-a46fb27ff229"
WORKSPACE_ID = "a1f2c3d4-e5f6-4890-abcd-1234567890ef"
OTHER_WORKSPACE_ID = "c3d4e5f6-a7b8-4012-8def-3456789012ab"
BASE = f"/v1/capacities/{CAPACITY_ID}/surgeProtection"


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


@pytest.fixture
def api(monkeypatch):
    """Patch capacity resolution and _base_api, recording each call."""
    monkeypatch.setattr(sp, "resolve_capacity_id", lambda capacity=None: CAPACITY_ID)
    mock = MagicMock()
    monkeypatch.setattr(sp, "_base_api", mock)
    return mock


def _paged(values):
    return [{"value": values}]


def test_get_capacity_surge_protection_disabled(api):
    api.return_value = _Response({"state": "Disabled"})

    df = sp.get_capacity_surge_protection()

    assert api.call_args.kwargs["request"] == BASE
    assert df.iloc[0]["State"] == "Disabled"
    assert pd.isna(df.iloc[0]["Rejection Threshold"])


def test_update_capacity_surge_protection_payload(api):
    api.return_value = _Response(
        {"state": "Enabled", "rejectionThreshold": 80, "recoveryThreshold": 60}
    )

    df = sp.update_capacity_surge_protection(
        enabled=True, rejection_threshold=80, recovery_threshold=60
    )

    kwargs = api.call_args.kwargs
    assert kwargs["method"] == "patch"
    assert kwargs["payload"] == {
        "state": "Enabled",
        "rejectionThreshold": 80,
        "recoveryThreshold": 60,
    }
    assert df.iloc[0]["Rejection Threshold"] == 80


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"rejection_threshold": 10},
        {"recovery_threshold": 5},
        {"rejection_threshold": 50, "recovery_threshold": 50},
    ],
)
def test_update_capacity_surge_protection_validation(api, kwargs):
    with pytest.raises(ValueError):
        sp.update_capacity_surge_protection(**kwargs)
    api.assert_not_called()


def test_list_actor_auto_detection_rules(api):
    api.return_value = _paged(
        [
            {
                "id": "r1",
                "actorsSubjectToRule": {"type": "AllWorkspaces"},
                "capacityUnitsConsumptionThresholdPercentage": 25,
                "blockDuration": {"type": "Hours", "hours": 24},
            },
            {
                "id": "r2",
                "actorsSubjectToRule": {"type": "AllWorkspaces"},
                "capacityUnitsConsumptionThresholdPercentage": 50,
                "blockDuration": {"type": "Indefinite"},
            },
        ]
    )

    df = sp.list_actor_auto_detection_rules()

    assert api.call_args.kwargs["request"] == f"{BASE}/actorAutoDetectionRules"
    assert api.call_args.kwargs["uses_pagination"] is True
    assert list(df["Block Duration Type"]) == ["Hours", "Indefinite"]
    assert df.iloc[0]["Block Duration Hours"] == 24
    assert pd.isna(df.iloc[1]["Block Duration Hours"])


def test_list_actor_auto_detection_rules_empty(api):
    api.return_value = _paged([])

    df = sp.list_actor_auto_detection_rules()

    assert df.empty
    assert "Actor Auto Detection Rule Id" in df.columns


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({}, {"type": "Hours", "hours": 24}),
        ({"block_indefinitely": True}, {"type": "Indefinite"}),
    ],
)
def test_create_actor_auto_detection_rule_payload(api, kwargs, expected):
    api.return_value = _Response({"id": "r1", "blockDuration": expected})

    sp.create_actor_auto_detection_rule(
        capacity_units_consumption_threshold_percentage=25, **kwargs
    )

    call = api.call_args.kwargs
    assert call["method"] == "post"
    assert call["status_codes"] == 201
    assert call["payload"] == {
        "actorsSubjectToRule": {"type": "AllWorkspaces"},
        "capacityUnitsConsumptionThresholdPercentage": 25,
        "blockDuration": expected,
    }


def test_create_actor_auto_detection_rule_invalid_threshold(api):
    with pytest.raises(ValueError):
        sp.create_actor_auto_detection_rule(
            capacity_units_consumption_threshold_percentage=0
        )
    api.assert_not_called()


def test_update_actor_auto_detection_rule_only_threshold(api):
    api.return_value = _Response({"id": "r1"})

    sp.update_actor_auto_detection_rule(
        "r1", capacity_units_consumption_threshold_percentage=15
    )

    call = api.call_args.kwargs
    assert call["request"] == f"{BASE}/actorAutoDetectionRules/r1"
    assert call["method"] == "patch"
    assert call["payload"] == {"capacityUnitsConsumptionThresholdPercentage": 15}


@pytest.mark.parametrize(
    "kwargs",
    [{}, {"block_duration_hours": 5, "block_indefinitely": True}],
)
def test_update_actor_auto_detection_rule_validation(api, kwargs):
    with pytest.raises(ValueError):
        sp.update_actor_auto_detection_rule("r1", **kwargs)
    api.assert_not_called()


def test_delete_actor_auto_detection_rule(api):
    sp.delete_actor_auto_detection_rule("r1")

    call = api.call_args.kwargs
    assert call["request"] == f"{BASE}/actorAutoDetectionRules/r1"
    assert call["method"] == "delete"


@pytest.mark.parametrize(
    "expire_after,expected",
    [
        ("2026-07-15T18:00:00Z", "2026-07-15T18:00:00Z"),
        (datetime(2026, 7, 15, 18, 0, 0), "2026-07-15T18:00:00Z"),
        (
            datetime(2026, 7, 15, 20, 0, 0, tzinfo=timezone(timedelta(hours=2))),
            "2026-07-15T18:00:00Z",
        ),
    ],
)
def test_expiration_policy_formats_utc(expire_after, expected):
    assert sp._expiration_policy(expire_after) == {
        "policyType": "ExpireAfterDate",
        "expireAfterDateTime": expected,
    }


def test_expiration_policy_never_expire():
    assert sp._expiration_policy(None) == {"policyType": "NeverExpire"}


def test_expiration_policy_invalid_string():
    with pytest.raises(ValueError):
        sp._expiration_policy("not a date")


def test_create_blocked_actor_payload(api):
    api.return_value = _Response(
        {
            "id": "b1",
            "actorDetails": {"actorType": "Workspace", "workspaceId": WORKSPACE_ID},
            "expirationPolicy": {"policyType": "NeverExpire"},
        }
    )

    df = sp.create_blocked_actor(WORKSPACE_ID)

    call = api.call_args.kwargs
    assert call["request"] == f"{BASE}/blockedActors"
    assert call["status_codes"] == [200, 201]
    assert call["payload"] == {
        "actorDetails": {"actorType": "Workspace", "workspaceId": WORKSPACE_ID},
        "expirationPolicy": {"policyType": "NeverExpire"},
    }
    assert df.iloc[0]["Blocked Actor Id"] == "b1"


def test_create_blocked_actor_resolves_workspace_name(api, monkeypatch):
    monkeypatch.setattr(sp, "resolve_workspace_id", lambda w: WORKSPACE_ID)
    api.return_value = _Response({"id": "b1"})

    sp.create_blocked_actor("My Workspace")

    payload = api.call_args.kwargs["payload"]
    assert payload["actorDetails"]["workspaceId"] == WORKSPACE_ID


def _blocked_actors():
    return _paged(
        [
            {
                "id": "b0",
                "actorDetails": {
                    "actorType": "Workspace",
                    "workspaceId": OTHER_WORKSPACE_ID,
                },
                "expirationPolicy": {"policyType": "NeverExpire"},
            },
            {
                "id": "b1",
                "actorDetails": {"actorType": "Workspace", "workspaceId": WORKSPACE_ID},
                "expirationPolicy": {
                    "policyType": "ExpireAfterDate",
                    "expireAfterDateTime": "2026-07-15T18:00:00Z",
                },
            },
        ]
    )


def test_list_blocked_actors(api):
    api.return_value = _blocked_actors()

    df = sp.list_blocked_actors()

    assert list(df["Blocked Actor Id"]) == ["b0", "b1"]
    assert pd.isna(df.iloc[0]["Expire After Date Time"])
    assert df.iloc[1]["Expire After Date Time"] == pd.Timestamp("2026-07-15T18:00:00Z")


def test_update_blocked_actor_resolves_actor_id(api):
    api.side_effect = [_blocked_actors(), _Response({"id": "b1"})]

    sp.update_blocked_actor(WORKSPACE_ID, expire_after="2026-08-01T00:00:00Z")

    call = api.call_args.kwargs
    assert call["request"] == f"{BASE}/blockedActors/b1"
    assert call["method"] == "patch"
    assert call["payload"] == {
        "expirationPolicy": {
            "policyType": "ExpireAfterDate",
            "expireAfterDateTime": "2026-08-01T00:00:00Z",
        }
    }


def test_delete_blocked_actor_not_found(api):
    api.return_value = _paged([])

    with pytest.raises(ValueError, match="not a blocked actor"):
        sp.delete_blocked_actor(WORKSPACE_ID)


def test_get_blocked_actor(api):
    api.side_effect = [
        _blocked_actors(),
        _Response(_blocked_actors()[0]["value"][1]),
    ]

    df = sp.get_blocked_actor(WORKSPACE_ID)

    assert api.call_args.kwargs["request"] == f"{BASE}/blockedActors/b1"
    assert df.iloc[0]["Workspace Id"] == WORKSPACE_ID


def _mission_critical_actors():
    return _paged(
        [
            {
                "id": "m1",
                "actorDetails": {"actorType": "Workspace", "workspaceId": WORKSPACE_ID},
            }
        ]
    )


def test_list_mission_critical_actors(api):
    api.return_value = _mission_critical_actors()

    df = sp.list_mission_critical_actors()

    assert api.call_args.kwargs["request"] == f"{BASE}/missionCriticalActors"
    assert df.iloc[0]["Mission Critical Actor Id"] == "m1"


def test_create_mission_critical_actor_payload(api):
    api.return_value = _Response({"id": "m1"})

    sp.create_mission_critical_actor(WORKSPACE_ID)

    call = api.call_args.kwargs
    assert call["method"] == "post"
    assert call["status_codes"] == [200, 201]
    assert call["payload"] == {
        "actorDetails": {"actorType": "Workspace", "workspaceId": WORKSPACE_ID}
    }


def test_delete_mission_critical_actor(api):
    api.side_effect = [_mission_critical_actors(), None]

    sp.delete_mission_critical_actor(WORKSPACE_ID)

    call = api.call_args.kwargs
    assert call["request"] == f"{BASE}/missionCriticalActors/m1"
    assert call["method"] == "delete"


def test_get_mission_critical_actor(api):
    api.side_effect = [
        _mission_critical_actors(),
        _Response(_mission_critical_actors()[0]["value"][0]),
    ]

    df = sp.get_mission_critical_actor(WORKSPACE_ID)

    assert api.call_args.kwargs["request"] == f"{BASE}/missionCriticalActors/m1"
    assert df.iloc[0]["Workspace Id"] == WORKSPACE_ID
