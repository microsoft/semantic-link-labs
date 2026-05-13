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

    # Bare-identifier DAX enum constants. These are arguments to specific
    # functions (e.g. DATESINPERIOD's interval, RANKX's order/ties) and must
    # not be treated as table references. Matched case-insensitively.
    KEYWORDS = {
        # Date intervals (DATESINPERIOD, DATEADD, PARALLELPERIOD, ...)
        "DAY",
        "WEEK",
        "MONTH",
        "QUARTER",
        "YEAR",
        # Sort order (RANKX, TOPN, ORDERBY, ...)
        "ASC",
        "DESC",
        # Rank ties (RANKX)
        "SKIP",
        "DENSE",
        "LAST",
        "FIRST",
        # Boolean / blank literals
        "TRUE",
        "FALSE",
        "BLANK",
        # Crossfilter direction (CROSSFILTER)
        "BOTH",
        "ONEWAY",
        "ONEWAY_LEFTFILTERSRIGHT",
        "ONEWAY_RIGHTFILTERSLEFT",
        "NONE",
        # PATH ordering (TREATAS / PATH-related)
        "FIRST",
        "LAST",
        # Match modes (LOOKUPVALUE-related)
        "ABSOLUTE",
        "RELATIVE",
        "INTEGER",
        "STRING",
        "DOUBLE",
        "BOOLEAN",
        "DATETIME",
        "VARIANT",
        "TEXT",
        "ALPHABETICAL",
        "DEFINE",
        "ORDER",
        "BY",
        "EVALUATE",
        "EVALUATEANDLOG",
        "OFFSET",
        "VAR",
        "RETURN",
        "PARTITIONBY",
        "RANK",
        "ROWNUMBER",
        "INDEX",
        "OFFSET",
        "WINDOW",
        "ORDERBY",
        "IN",
        "NOT",
        "AND",
        "OR",
        "ABS",
        "ACOS",
        "ACOT",
        "APPROXIMATEDISTINCTCOUNT",
        "AVERAGE",
        "AVERAGEA",
        "AVERAGEX",
        "COUNT",
        "COUNTA",
        "COUNTX",
        "COUNTAX",
        "COUNTBLANK",
        "COUNTROWS",
        "DISTINCTCOUNT",
        "DISTINCTCOUNTNOBLANK",
        "MAX",
        "MAXA",
        "MAXX",
        "MIN",
        "MINA",
        "MINX",
        "PRODUCT",
        "PRODUCTX",
        "SUM",
        "SUMX",
        "CALENDAR",
        "CALENDARAUTO",
        "DATE",
        "DATEDIFF",
        "DATEVALUE",
        "EDATE",
        "EOMONTH",
        "HOUR",
        "MINUTE",
        "NETWORKDAYS",
        "NOW",
        "SECOND",
        "TIME",
        "UTCNOW",
        "UTCTODAY",
        "WEEKDAY",
        "WEEKNUM",
        "YEARFRAC",
    }

    def __init__(self, text):

        self.tokens = list(tokenize(text))
        self.index = 0
        # Stack of sets of variable names currently in scope. Used to
        # distinguish references to VAR-declared names from table refs.
        self.scopes = []
        # Names of virtual columns introduced via ADDCOLUMNS / SELECTCOLUMNS
        # anywhere in the input. Bracketed references to these names are
        # emitted as VirtualColumn instead of Measure.
        self.virtual_columns = set()

    @property
    def current(self):
        return self.tokens[self.index]

    def advance(self):
        self.index += 1

    def _is_variable(self, name):
        return any(name in scope for scope in self.scopes)

    def parse(self):
        return self.statement()

    def statement(self):

        # VAR ... [VAR ...]* RETURN <expr>
        if self.current.token_type == TokenType.VAR:

            variables = []
            self.scopes.append(set())

            try:
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

                    # Make the new variable visible to subsequent VARs and to
                    # the RETURN expression. (DAX allows later VARs to
                    # reference earlier ones in the same block.)
                    self.scopes[-1].add(name)

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
            finally:
                self.scopes.pop()

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

                # Record virtual column names introduced by ADDCOLUMNS /
                # SELECTCOLUMNS so later bracketed references resolve to
                # VirtualColumn rather than Measure. Both have the shape:
                #   FN ( <table>, <name>, <expression> [, <name>, <expression>]... )
                if name.upper() in ("ADDCOLUMNS", "SELECTCOLUMNS"):
                    for i in range(1, len(args), 2):
                        arg = args[i]
                        if isinstance(arg, Literal):
                            value = arg.args.get("this")
                            if (
                                isinstance(value, str)
                                and len(value) >= 2
                                and value.startswith('"')
                                and value.endswith('"')
                            ):
                                self.virtual_columns.add(value[1:-1])

                return Function(
                    args={
                        "this": name,
                        "expressions": args,
                    }
                )

            # Bare identifier (not a function call): a reference to a VAR
            # in scope, a known DAX enum keyword, or otherwise a table
            # reference.
            if self._is_variable(name):
                return VariableReference(args={"this": name})

            if name.upper() in self.KEYWORDS:
                return Keyword(args={"this": name})

            return Table(args={"this": name})

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

            if name in self.virtual_columns:
                return VirtualColumn(args={"this": name})

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
