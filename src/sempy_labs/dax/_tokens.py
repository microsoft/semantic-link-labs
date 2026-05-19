from enum import Enum, auto


class TokenType(Enum):

    IDENTIFIER = auto()
    QUOTED_IDENTIFIER = auto()

    NUMBER = auto()
    STRING = auto()

    COLUMN = auto()
    TABLE_COLUMN = auto()

    LPAREN = auto()
    RPAREN = auto()
    COMMA = auto()

    OPERATOR = auto()

    VAR = auto()
    RETURN = auto()

    EOF = auto()
