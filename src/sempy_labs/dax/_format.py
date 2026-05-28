import html as _html
from typing import Optional

from sempy._utils._log import log

from ._expressions import Var
from ._parser import Parser
from ._tokenizer import tokenize
from ._tokens import TokenType


# Soft, Apple-inspired palette tuned to read well on both light and dark
# backgrounds (loosely based on Apple's "vivid" system colors, lightened
# slightly so they don't burn on a light background).
_COLORS = {
    "function": "#5E9EFF",  # soft blue   - functions & keywords
    "keyword": "#5E9EFF",
    "variable": "#5AC8B8",  # soft teal   - VAR-defined names
    "number": "#FF9F45",  # soft orange - numeric literals
    "virtual_column": "#FF7A8A",  # soft pink   - ADDCOLUMNS/SELECTCOLUMNS cols
    "string": "#9BB87A",  # muted green - string literals (default)
    "operator": "#A6A6A6",  # neutral gray
    "punctuation": "#A6A6A6",
    "default": "inherit",
}


def _classify_tokens(dax_expression: str):
    """Tokenize the DAX text and attach a semantic 'kind' to each token.

    Returns a list of ``(token, kind)`` tuples in source order. ``kind`` is
    one of the keys in ``_COLORS`` or ``None`` for tokens that should be
    rendered with the default color.
    """

    tokens = list(tokenize(dax_expression))

    # Best-effort parse to pick up VAR names and virtual columns. If parsing
    # fails (e.g. partial input), fall back to a structural-only highlight.
    var_names: set[str] = set()
    virtual_columns: set[str] = set()
    try:
        parser = Parser(dax_expression)
        tree = parser.parse()
        virtual_columns = set(parser.virtual_columns)
        for node in tree.walk():
            if isinstance(node, Var):
                name = node.args.get("this")
                if isinstance(name, str):
                    var_names.add(name)
    except Exception:
        pass

    classified = []

    for i, token in enumerate(tokens):
        kind: Optional[str] = None
        tt = token.token_type

        if tt == TokenType.EOF:
            continue

        if tt in (TokenType.VAR, TokenType.RETURN):
            kind = "keyword"
        elif tt == TokenType.IDENTIFIER:
            # Function call if the next non-EOF token is '('.
            next_token = tokens[i + 1] if i + 1 < len(tokens) else None
            if next_token is not None and next_token.token_type == TokenType.LPAREN:
                kind = "function"
            elif token.text in var_names:
                kind = "variable"
            elif token.text.upper() in Parser.KEYWORDS:
                kind = "keyword"
            else:
                kind = None
        elif tt == TokenType.NUMBER:
            kind = "number"
        elif tt == TokenType.COLUMN:
            inner = token.text[1:-1]
            if inner in virtual_columns:
                kind = "virtual_column"
            else:
                kind = None
        elif tt == TokenType.STRING:
            kind = "string"
        elif tt == TokenType.OPERATOR:
            kind = "operator"
        elif tt in (TokenType.LPAREN, TokenType.RPAREN, TokenType.COMMA):
            kind = "punctuation"

        classified.append((token, kind))

    return classified


def _render_html(dax_expression: str) -> str:
    """Render the DAX expression as an HTML ``<pre>`` block with inline
    color spans."""

    classified = _classify_tokens(dax_expression)

    parts = []
    cursor = 0

    for token, kind in classified:
        # Emit any whitespace / unmatched chars between tokens verbatim so
        # the original formatting (indentation, line breaks) is preserved.
        if token.position > cursor:
            parts.append(_html.escape(dax_expression[cursor : token.position]))

        text = _html.escape(token.text)
        color = _COLORS.get(kind or "default", "inherit")

        if color == "inherit":
            parts.append(text)
        else:
            parts.append(f'<span style="color:{color}">{text}</span>')

        cursor = token.position + len(token.text)

    # Trailing whitespace after the last token.
    if cursor < len(dax_expression):
        parts.append(_html.escape(dax_expression[cursor:]))

    body = "".join(parts)

    return (
        '<pre style="'
        "font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;"
        "font-size:13px;line-height:1.5;"
        "padding:12px 14px;border-radius:8px;"
        "background:transparent;"
        "color:inherit;"
        "white-space:pre;overflow-x:auto;"
        '">'
        f"{body}"
        "</pre>"
    )


@log
def format_dax(dax_expression: str, display: bool = True):
    """
    Color-codes a DAX expression for display in a notebook.

    Uses the DAX parser/tokenizer to classify tokens and renders them with
    a soft, Apple-inspired palette that reads well on both light and dark
    backgrounds:

    * Functions and keywords - soft blue
    * VAR-declared variables - soft teal
    * Numeric literals - soft orange
    * Virtual columns (introduced by ``ADDCOLUMNS`` / ``SELECTCOLUMNS``) - soft pink

    Parameters
    ----------
    dax_expression : str
        The DAX expression to color-code.
    display : bool, default=True
        If True, displays the formatted DAX in the current notebook and
        returns ``None``. If False, returns an ``IPython.display.HTML``
        object instead.

    Returns
    -------
    IPython.display.HTML | None
        The HTML object when ``display=False``; otherwise ``None``.
    """

    from IPython.display import HTML, display as _display

    rendered = HTML(_render_html(dax_expression))

    if display:
        _display(rendered)
        return None

    return rendered