from types import SimpleNamespace

import pytest

import sempy_labs.gateway._members as members
import sempy_labs.gateway._role_assignments as role_assignments
import sempy_labs.gateway._status as status

GATEWAY_ID = "8e41c4dd-a382-4937-9bf3-695ab881f7c2"
MEMBER_ID = "f921ee6b-8feb-4595-8aa7-3ed34338e8b6"
PRINCIPAL_ID = "6a002b3d-e4ec-43df-8c08-e8eb7547d9dd"


def _mock_base_api(monkeypatch, module, response=None):
    calls = []

    def fake_base_api(**kwargs):
        calls.append(kwargs)
        return response if response is not None else SimpleNamespace()

    monkeypatch.setattr(module, "_base_api", fake_base_api)
    return calls


def test_add_gateway_role_assignment_normalizes_case(monkeypatch):
    calls = _mock_base_api(monkeypatch, role_assignments)

    role_assignments.add_gateway_role_assignment(
        GATEWAY_ID, PRINCIPAL_ID, principal_type="serviceprincipal", role="admin"
    )

    assert calls[0]["request"] == f"/v1/gateways/{GATEWAY_ID}/roleAssignments"
    assert calls[0]["method"] == "post"
    assert calls[0]["status_codes"] == 201
    assert calls[0]["payload"] == {
        "principal": {"id": PRINCIPAL_ID, "type": "ServicePrincipal"},
        "role": "Admin",
    }


@pytest.mark.parametrize("principal_type, role", [("App", "Admin"), ("User", "Viewer")])
def test_add_gateway_role_assignment_rejects_invalid_options(
    monkeypatch, principal_type, role
):
    calls = _mock_base_api(monkeypatch, role_assignments)

    with pytest.raises(ValueError):
        role_assignments.add_gateway_role_assignment(
            GATEWAY_ID, PRINCIPAL_ID, principal_type=principal_type, role=role
        )

    assert calls == []


def test_update_gateway_role_assignment(monkeypatch):
    calls = _mock_base_api(monkeypatch, role_assignments)

    role_assignments.update_gateway_role_assignment(
        GATEWAY_ID, PRINCIPAL_ID, role="connectioncreatorwithresharing"
    )

    assert (
        calls[0]["request"]
        == f"/v1/gateways/{GATEWAY_ID}/roleAssignments/{PRINCIPAL_ID}"
    )
    assert calls[0]["method"] == "patch"
    assert calls[0]["payload"] == {"role": "ConnectionCreatorWithResharing"}


def test_update_gateway_member_sends_only_provided_settings(monkeypatch):
    calls = _mock_base_api(monkeypatch, members)
    monkeypatch.setattr(
        members, "resolve_gateway_member_id", lambda **_kwargs: MEMBER_ID
    )

    members.update_gateway_member(GATEWAY_ID, "Member 1", enabled=False)

    assert calls[0]["request"] == f"/v1/gateways/{GATEWAY_ID}/members/{MEMBER_ID}"
    assert calls[0]["method"] == "patch"
    assert calls[0]["payload"] == {"enabled": False}


@pytest.mark.parametrize("kwargs", [{}, {"name": "x" * 201}])
def test_update_gateway_member_rejects_invalid_input(monkeypatch, kwargs):
    calls = _mock_base_api(monkeypatch, members)

    with pytest.raises(ValueError):
        members.update_gateway_member(GATEWAY_ID, MEMBER_ID, **kwargs)

    assert calls == []


def test_check_gateway_status(monkeypatch):
    result = {"status": "Online", "gatewayVersion": "1.2.3"}
    calls = _mock_base_api(monkeypatch, status, response=result)

    assert status.check_gateway_status(GATEWAY_ID) == result
    assert calls[0]["request"] == f"/v1/gateways/{GATEWAY_ID}/checkStatus"
    assert calls[0]["method"] == "post"
    assert calls[0]["lro_return_json"] is True


def test_check_gateway_member_status(monkeypatch):
    result = {"status": "Offline", "errorDetails": []}
    calls = _mock_base_api(monkeypatch, status, response=result)
    monkeypatch.setattr(
        status, "resolve_gateway_member_id", lambda **_kwargs: MEMBER_ID
    )

    assert status.check_gateway_member_status(GATEWAY_ID, "Member 1") == result
    assert (
        calls[0]["request"]
        == f"/v1/gateways/{GATEWAY_ID}/members/{MEMBER_ID}/checkStatus"
    )
    assert calls[0]["lro_return_json"] is True
