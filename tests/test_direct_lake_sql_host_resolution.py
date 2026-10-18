import base64
import uuid

from sempy_labs.tom._model import TOMWrapper


def _encode_segment(workspace_id: uuid.UUID) -> str:
    """Encodes a workspace ID the same way Fabric encodes it into a SQL
    Analytics Endpoint connection string hostname segment (lowercase, unpadded
    Base32 of the GUID's little-endian bytes)."""

    return base64.b32encode(workspace_id.bytes_le).decode().rstrip("=").lower()


def test_resolve_workspace_ids_same_workspace_single_segment_host():
    """A hostname with a single 26-char Base32 segment (no cluster prefix)
    decodes to that one workspace ID."""

    workspace_id = uuid.uuid4()
    segment = _encode_segment(workspace_id)
    server = f"{segment}.datawarehouse.fabric.microsoft.com"

    candidates = TOMWrapper._resolve_workspace_ids_from_sql_host(server)

    assert candidates == [str(workspace_id)]


def test_resolve_workspace_ids_cross_workspace_two_segment_host():
    """A hostname with a cluster segment and a workspace segment (separated by
    a hyphen) decodes both labels, including the actual source workspace."""

    workspace_id = uuid.uuid4()
    cluster_segment = _encode_segment(uuid.uuid4())
    workspace_segment = _encode_segment(workspace_id)
    server = (
        f"{cluster_segment}-{workspace_segment}.datawarehouse.pbidedicated.windows.net"
    )

    candidates = TOMWrapper._resolve_workspace_ids_from_sql_host(server)

    assert str(workspace_id) in candidates
    assert len(candidates) == 2


def test_resolve_workspace_ids_real_format_hostname():
    """Validates decoding against a real-format hostname (two hyphenated,
    26-character Base32 segments) without asserting a specific workspace ID,
    since the source GUIDs for this example are not publicly known."""

    server = (
        "5lcsgl3vll3edero2m4sge7gdu-nya26urqtgsejoagwutwdoogl4"
        ".datawarehouse.pbidedicated.windows.net"
    )

    candidates = TOMWrapper._resolve_workspace_ids_from_sql_host(server)

    assert len(candidates) == 2
    for candidate in candidates:
        # Each candidate must be a syntactically valid UUID string.
        assert str(uuid.UUID(candidate)) == candidate


def test_resolve_workspace_ids_no_valid_segments():
    """A hostname with no 26-character Base32-looking labels yields no
    candidates."""

    server = "my-custom-host.database.windows.net"

    candidates = TOMWrapper._resolve_workspace_ids_from_sql_host(server)

    assert candidates == []
