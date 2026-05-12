from ._expressions import *
from ._tokenizer import tokenize
from ._tokens import TokenType


class Parser:

    PRECEDENCE = {
        "||": 1,
        "&&": 2,
        "=": 5,
        "<>": 5,
        "<": 5,
        "<=": 5,
        ">": 5,
        ">=": 5,
        "&": 6,
        "+": 10,
        "-": 10,
        "*": 20,
        "/": 20,
    }

    def __init__(self, text):

        self.tokens = list(tokenize(text))
        self.index = 0

    @property
    def current(self):
        return self.tokens[self.index]

    def advance(self):
        self.index += 1

    def parse(self):
        return self.statement()

    def statement(self):

        # VAR ... [VAR ...]* RETURN <expr>
        if self.current.token_type == TokenType.VAR:

            variables = []

            while self.current.token_type == TokenType.VAR:

                self.advance()

                if self.current.token_type != TokenType.IDENTIFIER:
                    raise SyntaxError(
                        f"Expected variable name after VAR, got: {self.current}"
                    )

                name = self.current.text
                self.advance()

                if not (
                    self.current.token_type == TokenType.OPERATOR
                    and self.current.text == "="
                ):
                    raise SyntaxError(
                        f"Expected '=' after VAR name, got: {self.current}"
                    )
                self.advance()

                value = self.statement()

                variables.append(
                    Var(
                        args={
                            "this": name,
                            "expression": value,
                        }
                    )
                )

            if self.current.token_type != TokenType.RETURN:
                raise SyntaxError(
                    f"Expected RETURN after VAR block, got: {self.current}"
                )
            self.advance()

            ret_expr = self.expression()

            return Return(
                args={
                    "variables": variables,
                    "expression": ret_expr,
                }
            )

        return self.expression()

    def expression(self, precedence=0):

        left = self.primary()

        while True:

            token = self.current

            if token.token_type != TokenType.OPERATOR:
                break

            token_precedence = self.PRECEDENCE.get(
                token.text,
                0,
            )

            if token_precedence < precedence:
                break

            operator = token.text

            self.advance()

            right = self.expression(token_precedence + 1)

            left = Binary(
                args={
                    "this": left,
                    "expression": right,
                    "operator": operator,
                }
            )

        return left

    def primary(self):

        token = self.current

        # Unary +/-
        if token.token_type == TokenType.OPERATOR and token.text in ("-", "+"):
            self.advance()
            operand = self.primary()
            return Unary(
                args={
                    "this": operand,
                    "operator": token.text,
                }
            )

        if token.token_type == TokenType.IDENTIFIER:

            name = token.text

            self.advance()

            if self.current.token_type == TokenType.LPAREN:

                self.advance()

                args = []

                while self.current.token_type != TokenType.RPAREN:

                    # Empty argument (e.g. RANKX(..., , DESC, DENSE))
                    if self.current.token_type == TokenType.COMMA:
                        args.append(Literal(args={"this": None}))
                        self.advance()
                        continue

                    args.append(self.statement())

                    if self.current.token_type == TokenType.COMMA:
                        self.advance()
                        # Trailing comma immediately before RPAREN -> empty arg
                        if self.current.token_type == TokenType.RPAREN:
                            args.append(Literal(args={"this": None}))

                self.advance()

                return Function(
                    args={
                        "this": name,
                        "expressions": args,
                    }
                )

            return Literal(args={"this": name})

        elif token.token_type == TokenType.TABLE_COLUMN:

            text = token.text

            self.advance()

            table, column = text.split("[")

            table = table.strip("'")

            column = column[:-1]

            return Column(
                args={
                    "table": table,
                    "this": column,
                }
            )

        elif token.token_type == TokenType.QUOTED_IDENTIFIER:

            name = token.text.strip("'")

            self.advance()

            return Table(
                args={
                    "this": name,
                }
            )

        elif token.token_type == TokenType.COLUMN:

            name = token.text[1:-1]

            self.advance()

            return Measure(
                args={
                    "this": name,
                }
            )

        elif token.token_type == TokenType.NUMBER:

            self.advance()

            return Literal(
                args={
                    "this": token.text,
                }
            )

        elif token.token_type == TokenType.STRING:

            self.advance()

            return Literal(
                args={
                    "this": token.text,
                }
            )

        elif token.token_type == TokenType.LPAREN:

            self.advance()
            expr = self.statement()

            if self.current.token_type != TokenType.RPAREN:
                raise SyntaxError("Expected closing parenthesis")

            self.advance()

            return expr

        raise SyntaxError(f"Unexpected token: {token}")


def parse_dax(text: str):
    return Parser(text).parse()
