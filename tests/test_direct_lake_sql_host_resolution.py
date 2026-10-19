import base64
import uuid
from unittest.mock import patch

import pytest

from sempy_labs.tom._model import TOMWrapper


def _encode_segment(workspace_id: uuid.UUID, *, little_endian: bool = True) -> str:
    """Encodes a workspace ID the same way Fabric encodes it into a SQL
    Analytics Endpoint connection string hostname segment (uppercase, unpadded
    Base32 of the GUID's bytes)."""

    raw = workspace_id.bytes_le if little_endian else workspace_id.bytes
    return base64.b32encode(raw).decode().rstrip("=").lower()


def test_workspace_ids_from_sql_server_two_segment_host():
    """A hostname with a cluster segment and a workspace segment (separated by
    a hyphen) decodes the second label into little-endian and big-endian
    candidate workspace IDs."""

    workspace_id = uuid.uuid4()
    cluster_segment = _encode_segment(uuid.uuid4())
    workspace_segment = _encode_segment(workspace_id)
    server = (
        f"{cluster_segment}-{workspace_segment}.datawarehouse.pbidedicated.windows.net"
    )

    candidates = TOMWrapper._workspace_ids_from_sql_server(server)

    assert str(workspace_id) in candidates
    assert len(candidates) == 2


def test_workspace_ids_from_sql_server_real_format_hostname():
    """Validates decoding against a real-format hostname (two hyphenated,
    26-character Base32 segments) without asserting a specific workspace ID,
    since the source GUIDs for this example are not publicly known."""

    server = (
        "5lcsgl3vll3edero2m4sge7gdu-nya26urqtgsejoagwutwdoogl4"
        ".datawarehouse.pbidedicated.windows.net"
    )

    candidates = TOMWrapper._workspace_ids_from_sql_server(server)

    assert len(candidates) == 2
    for candidate in candidates:
        # Each candidate must be a syntactically valid UUID string.
        assert str(uuid.UUID(candidate)) == candidate


def test_workspace_ids_from_sql_server_single_segment_host():
    """A hostname with only a single 26-char Base32 segment (no cluster
    prefix) does not carry enough information to isolate the workspace
    segment, so no candidates are returned."""

    server = f"{_encode_segment(uuid.uuid4())}.datawarehouse.fabric.microsoft.com"

    assert TOMWrapper._workspace_ids_from_sql_server(server) == []


def test_workspace_ids_from_sql_server_no_valid_segments():
    """A hostname with no 26-character Base32-looking labels yields no
    candidates."""

    server = "my-custom-host.database.windows.net"

    assert TOMWrapper._workspace_ids_from_sql_server(server) == []


def _make_item(item_id, display_name, item_type="Lakehouse"):
    return {"id": item_id, "displayName": display_name, "type": item_type}


def _paginated_items_response(items):
    return [{"value": items}]


def test_resolve_item_id_by_name_single_match():
    """Resolves the item ID when exactly one Lakehouse/Warehouse in a
    candidate workspace matches the display name."""

    tom = object.__new__(TOMWrapper)
    workspace_id = str(uuid.uuid4())
    item_id = str(uuid.uuid4())

    with patch(
        "sempy_labs.tom._model._base_api",
        return_value=_paginated_items_response(
            [_make_item(item_id, "LH_Admin"), _make_item(str(uuid.uuid4()), "Other")]
        ),
    ):
        resolved_id = tom._resolve_item_id_by_name("LH_Admin", [workspace_id], {})

    assert resolved_id == item_id


def test_resolve_item_id_by_name_ambiguous_raises():
    """Raises a ValueError when the display name matches more than one
    Lakehouse/Warehouse."""

    tom = object.__new__(TOMWrapper)
    workspace_id = str(uuid.uuid4())

    with patch(
        "sempy_labs.tom._model._base_api",
        return_value=_paginated_items_response(
            [
                _make_item(str(uuid.uuid4()), "LH_Admin"),
                _make_item(str(uuid.uuid4()), "LH_Admin", item_type="Warehouse"),
            ]
        ),
    ):
        with pytest.raises(ValueError, match="ambiguous"):
            tom._resolve_item_id_by_name("LH_Admin", [workspace_id], {})


def test_resolve_item_id_by_name_no_match_raises():
    """Raises a ValueError when the display name matches no
    Lakehouse/Warehouse in any candidate workspace."""

    tom = object.__new__(TOMWrapper)
    workspace_id = str(uuid.uuid4())

    with patch(
        "sempy_labs.tom._model._base_api",
        return_value=_paginated_items_response([_make_item(str(uuid.uuid4()), "Other")]),
    ):
        with pytest.raises(ValueError, match="could not be resolved"):
            tom._resolve_item_id_by_name("LH_Admin", [workspace_id], {})


def test_resolve_item_id_by_name_caches_workspace_items():
    """Each candidate workspace's items are fetched only once even if the
    same workspace ID is looked up for multiple expressions."""

    tom = object.__new__(TOMWrapper)
    workspace_id = str(uuid.uuid4())
    item_id = str(uuid.uuid4())
    items_cache = {}

    with patch(
        "sempy_labs.tom._model._base_api",
        return_value=_paginated_items_response([_make_item(item_id, "LH_Admin")]),
    ) as mock_api:
        tom._resolve_item_id_by_name("LH_Admin", [workspace_id], items_cache)
        tom._resolve_item_id_by_name("LH_Admin", [workspace_id], items_cache)

    assert mock_api.call_count == 1
