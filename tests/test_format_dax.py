"""Interactive visual test for ``sempy_labs.dax.format_dax``.

Usage:
    # Run interactively (prompts for DAX input; end with Ctrl-D / EOF):
    pytest -s tests/test_format_dax.py -k interactive

    # Or pass DAX via env var:
    DAX_EXPR="EVALUATE { 1 + 2 }" pytest -s tests/test_format_dax.py -k env

    # Or run the file directly:
    python tests/test_format_dax.py "EVALUATE { 1 + 2 }"
    python tests/test_format_dax.py path/to/expression.dax
    python tests/test_format_dax.py            # then type DAX, end with Ctrl-D

Prints the DAX to the terminal using ANSI 24-bit colors (the same palette
``format_dax`` uses for HTML) and also writes an HTML preview file next
to this script so you can open it in a browser.
"""

from __future__ import annotations

import os
import sys

import pytest

# Make ``src/`` importable when running this file directly without
# installing the package.
_HERE = os.path.dirname(os.path.abspath(__file__))
_SRC = os.path.join(os.path.dirname(_HERE), "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from sempy_labs.dax._format import (  # noqa: E402
    _COLORS,
    _classify_tokens,
    _render_html,
)


def _hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    h = hex_color.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _ansi(hex_color: str) -> str:
    if not hex_color or hex_color == "inherit":
        return ""
    r, g, b = _hex_to_rgb(hex_color)
    return f"\x1b[38;2;{r};{g};{b}m"


_RESET = "\x1b[0m"


def render_ansi(dax_expression: str) -> str:
    """Render ``dax_expression`` as an ANSI-colored string for terminal output."""

    classified = _classify_tokens(dax_expression)
    parts: list[str] = []
    cursor = 0

    for token, kind in classified:
        if token.position > cursor:
            parts.append(dax_expression[cursor:token.position])

        color = _COLORS.get(kind or "default", "inherit")
        prefix = _ansi(color)
        if prefix:
            parts.append(f"{prefix}{token.text}{_RESET}")
        else:
            parts.append(token.text)

        cursor = token.position + len(token.text)

    if cursor < len(dax_expression):
        parts.append(dax_expression[cursor:])

    return "".join(parts)


def _read_stdin_dax() -> str:
    print("Enter DAX statement (end with Ctrl-D / EOF):", file=sys.stderr)
    return sys.stdin.read()


def _show(text: str, *, write_html: bool = True) -> None:
    text = text.strip()
    assert text, "No DAX expression provided."

    print("\n--- Input DAX ---")
    print(text)

    print("\n--- format_dax (ANSI terminal preview) ---")
    print(render_ansi(text))

    if write_html:
        out_path = os.path.join(_HERE, "format_dax_preview.html")
        html = _render_html(text)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(
                "<!doctype html><html><head><meta charset='utf-8'>"
                "<title>format_dax preview</title>"
                "<style>body{font-family:-apple-system,BlinkMacSystemFont,"
                "sans-serif;padding:24px;}h2{font-weight:500;}</style>"
                "</head><body>"
                "<h2 style='color:#f5f5f7;background:#1c1c1e;padding:8px;'>"
                "Dark background</h2>"
                f"<div style='background:#1c1c1e;padding:8px;border-radius:8px'>"
                f"{html}</div>"
                "<h2 style='color:#1d1d1f;background:#fff;padding:8px;'>"
                "Light background</h2>"
                f"<div style='background:#ffffff;color:#1d1d1f;padding:8px;"
                f"border-radius:8px'>{html}</div>"
                "</body></html>"
            )
        print(f"\nHTML preview written to: {out_path}")


@pytest.mark.skipif(
    not sys.stdin.isatty(),
    reason="Interactive test; run with `pytest -s` from a terminal.",
)
def test_format_dax_interactive():
    _show(_read_stdin_dax())


@pytest.mark.skipif(
    "DAX_EXPR" not in os.environ,
    reason="Set the DAX_EXPR environment variable to run this test.",
)
def test_format_dax_env():
    _show(os.environ["DAX_EXPR"])


if __name__ == "__main__":
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        # If the single argument is a path to an existing file, read it;
        # otherwise treat the joined arguments as the DAX expression itself.
        if len(sys.argv) == 2 and os.path.isfile(arg):
            with open(arg, "r", encoding="utf-8") as f:
                dax = f.read()
        else:
            dax = " ".join(sys.argv[1:])
    else:
        dax = _read_stdin_dax()

    _show(dax)
