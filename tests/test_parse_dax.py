"""Interactive test for parse_dax().dump().

Usage:
    # Run interactively (prompts for DAX input; end with Ctrl-D / EOF):
    pytest -s tests/test_parse_dax.py -k interactive

    # Or pass DAX via env var:
    DAX_EXPR="EVALUATE { 1 + 2 }" pytest -s tests/test_parse_dax.py -k env

    # Or run the file directly:
    python tests/test_parse_dax.py "EVALUATE { 1 + 2 }"
    python tests/test_parse_dax.py   # then type DAX, end with Ctrl-D
"""

import os
import sys

import pytest

from sempy_labs.dax._parser import parse_dax


def _read_stdin_dax() -> str:
    print("Enter DAX statement (end with Ctrl-D / EOF):", file=sys.stderr)
    return sys.stdin.read()


def _dump(text: str) -> None:
    text = text.strip()
    assert text, "No DAX expression provided."
    print("\n--- Input DAX ---")
    print(text)
    print("\n--- parse_dax().dump() ---")
    parse_dax(text).dump()


@pytest.mark.skipif(
    not sys.stdin.isatty(),
    reason="Interactive test; run with `pytest -s` from a terminal.",
)
def test_parse_dax_interactive():
    _dump(_read_stdin_dax())


@pytest.mark.skipif(
    "DAX_EXPR" not in os.environ,
    reason="Set the DAX_EXPR environment variable to run this test.",
)
def test_parse_dax_env():
    _dump(os.environ["DAX_EXPR"])


if __name__ == "__main__":
    dax = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else _read_stdin_dax()
    _dump(dax)
