import re
from dataclasses import dataclass
from ._tokens import TokenType


@dataclass
class Token:
    token_type: TokenType
    text: str
    position: int


TOKEN_REGEX = [
    (TokenType.TABLE_COLUMN, r"'[^']+'\[[^\]]+\]"),
    # NEW
    (TokenType.QUOTED_IDENTIFIER, r"'[^']+'"),
    (TokenType.COLUMN, r"\[[^\]]+\]"),
    (TokenType.STRING, r'"([^"]|"")*"'),
    (TokenType.NUMBER, r"\d+(\.\d+)?"),
    (TokenType.IDENTIFIER, r"[A-Za-z_][A-Za-z0-9_]*"),
    (TokenType.OPERATOR, r"<=|>=|<>|&&|\|\||[-+*/=<>&]"),
    (TokenType.LPAREN, r"\("),
    (TokenType.RPAREN, r"\)"),
    (TokenType.COMMA, r","),
]

MASTER_REGEX = re.compile(
    "|".join(f"(?P<{t.name}>{r})" for t, r in TOKEN_REGEX),
    re.IGNORECASE,
)


def tokenize(sql):

    position = 0

    while position < len(sql):

        if sql[position].isspace():
            position += 1
            continue

        match = MASTER_REGEX.match(sql, position)

        if not match:
            raise SyntaxError(f"Unexpected character: {sql[position]}")

        group = match.lastgroup
        token_type = TokenType[group]
        text = match.group()

        if token_type == TokenType.IDENTIFIER:
            upper = text.upper()
            if upper == "VAR":
                token_type = TokenType.VAR
            elif upper == "RETURN":
                token_type = TokenType.RETURN

        yield Token(token_type, text, position)

        position = match.end()

    yield Token(TokenType.EOF, "", position)
