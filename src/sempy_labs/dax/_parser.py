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
        "WINDOW",
        "ORDERBY",
        "IN",
        "NOT",
        "AND",
        "OR",
        "ABS",
        "ACOS",
        "ACOSH",
        "ACOT",
        "ACOTH",
        "ASIN",
        "ASINH",
        "ATAN",
        "ATANH",
        "CEILING",
        "CONVERT",
        "COS",
        "COSH",
        "COT",
        "COTH",
        "CURRENCY",
        "DEGREES",
        "DIVIDE",
        "EVEN",
        "EXP",
        "FACT",
        "FLOOR",
        "GCD",
        "ISO.CEILING",
        "LCM",
        "LN",
        "LOG",
        "LOG10",
        "MOD",
        "MROUND",
        "ODD",
        "PI",
        "POWER",
        "QUOTIENT",
        "RADIANS",
        "RAND",
        "RANDBETWEEN",
        "ROUND",
        "ROUNDDOWN",
        "ROUNDUP",
        "SIGN",
        "SIN",
        "SINH",
        "SQRT",
        "SQRTPI",
        "TAN",
        "TANH",
        "TRUNC",
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
        "ALL",
        "ALLCROSSFILTERED",
        "ALLEXCEPT",
        "ALLNOBLANKROW",
        "ALLSELECTED",
        "CALCULATE",
        "CALCULATETABLE",
        "EARLIER",
        "EARLIEST",
        "FILTER",
        "FIRSTNONBLANK",
        "FIRSTNONBLANKVALUE",
        "KEEPFILTERS",
        "LASTNONBLANK",
        "LASTNONBLANKVALUE",
        "LOOKUP",
        "LOOKUPVALUE",
        "LOOKUPWITHTOTALS",
        "MATCHBY",
        "MOVINGAVERAGE",
        "RANGE",
        "REMOVEFILTERS",
        "RUNNINGSUM",
        "SELECTEDVALUE",
        "ACCRINT",
        "ACCRINTM",
        "AMORDEGRC",
        "AMORLINC",
        "COUPDAYBS",
        "COUPDAYS",
        "COUPDAYSNC",
        "COUPNCD",
        "COUPNUM",
        "COUPPCD",
        "CUMIPMT",
        "CUMPRINC",
        "DB",
        "DDB",
        "DISC",
        "DOLLARDE",
        "DOLLARFR",
        "DURATION",
        "EFFECT",
        "FV",
        "INTRATE",
        "IPMT",
        "ISPMT",
        "MDURATION",
        "NOMINAL",
        "NPER",
        "ODDFPRICE",
        "ODDFYIELD",
        "ODDLPRICE",
        "ODDLYIELD",
        "PDURATION",
        "PMT",
        "PPMT",
        "PRICE",
        "PRICEDISC",
        "PRICEMAT",
        "PV",
        "RATE",
        "RECEIVED",
        "RRI",
        "SLN",
        "SYD",
        "TBILLEQ",
        "TBILLPRICE",
        "TBILLYIELD",
        "VDB",
        "XIRR",
        "XNPV",
        "YIELD",
        "YIELDDISC",
        "YIELDMAT",
        "COLUMNSTATISTICS",
        "CONTAINS",
        "CONTAINSROW",
        "CONTAINSSTRING",
        "CONTAINSSTRINGEXACT",
        "CUSTOMDATA",
        "HASONEFILTER",
        "HASONEVALUE",
        "ISAFTER",
        "ISBLANK",
        "ISBOOLEAN",
        "ISCROSSFILTERED",
        "ISCURRENCY",
        "ISDATETIME",
        "ISDECIMAL",
        "ISDOUBLE",
        "ISEMPTY",
        "ISERROR",
        "ISEVEN",
        "ISFILTERED",
        "ISINSCOPE",
        "ISINT64",
        "ISLOGICAL",
        "ISNONTEXT",
        "ISNUMBER",
        "ISNUMERIC",
        "ISODD",
        "ISONORAFTER",
        "ISSELECTEDMEASURE",
        "ISSTRING",
        "ISSUBTOTAL",
        "ISTEXT",
        "NAMEOF",
        "NONVISUAL",
        "SELECTEDMEASURE",
        "SELECTEDMEASUREFORMATSTRING",
        "SELECTEDMEASURENAME",
        "TABLEOF",
        "USERCULTURE",
        "USERNAME",
        "USEROBJECTID",
        "USERPRINCIPALNAME",
        "BITAND",
        "BITLSHIFT",
        "BITOR",
        "BITRSHIFT",
        "BITXOR",
        "COALESCE",
        "IF",
        "IF.EAGER",
        "IFERROR",
        "SWITCH",
        "ERROR",
        "EXTERNALMEASURE",
        "TOCSV",
        "TOJSON",
        "PATH",
        "PATHCONTAINS",
        "PATHITEM",
        "PATHITEMREVERSE",
        "PATHLENGTH",
        "CROSSFILTER",
        "RELATED",
        "RELATEDTABLE",
        "USERELATIONSHIP",
        "BETA.DIST",
        "BETA.INV",
        "CHISQ.DIST",
        "CHISQ.DIST.RT",
        "CHISQ.INV",
        "CHISQ.INV.RT",
        "COMBIN",
        "COMBINA",
        "CONFIDENCE.NORM",
        "CONFIDENCE.T",
        "EXPON.DIST",
        "GEOMEAN",
        "GEOMEANX",
        "LINEST",
        "LINESTX",
        "MEDIAN",
        "MEDIANX",
        "NORM.DIST",
        "NORM.INV",
        "NORM.S.DIST",
        "NORM.S.INV",
        "PERCENTILE.EXC",
        "PERCENTILE.INC",
        "PERCENTILEX.EXC",
        "PERCENTILEX.INC",
        "PERMUT",
        "POISSON.DIST",
        "RANK.EX",
        "RANKX",
        "SAMPLE",
        "SAMPLECARTESIANPOINTSBYCOVER",
        "STDEV.S",
        "STDEV.P",
        "STDEVX.S",
        "STDEVX.P",
        "T.DIST",
        "T.DIST.2T",
        "T.DIST.RT",
        "T.INV",
        "T.INV.2T",
        "VAR.S",
        "VAR.P",
        "VARX.S",
        "VARX.P",
        "ADDCOLUMNS",
        "ADDMISSINGITEMS",
        "CROSSJOIN",
        "CURRENTGROUP",
        "DATATABLE",
        "DETAILROWS",
        "DISTINCT",
        "EXCEPT",
        "FILTERS",
        "GENERATE",
        "GENERATEALL",
        "GENERATESERIES",
        "GROUPBY",
        "IGNORE",
        "INTERSECT",
        "NATURALINNERJOIN",
        "NATURALLEFTOUTERJOIN",
        "ROLLUP",
        "ROLLUPADDISSUBTOTAL",
        "ROLLUPGROUP",
        "ROLLUPISSUBTOTAL",
        "ROW",
        "SELECTCOLUMNS",
        "SUBSTITUTEWITHINDEX",
        "SUMMARIZE",
        "SUMMARIZECOLUMNS",
        "TOPN",
        "TREATAS",
        "UNION",
        "VALUES",
        "COMBINEVALUES",
        "CONCATENATE",
        "CONCATENATEX",
        "EXACT",
        "FIND",
        "FIXED",
        "FORMAT",
        "LEFT",
        "LEN",
        "LOWER",
        "MID",
        "REPLACE",
        "REPT",
        "RIGHT",
        "SEARCH",
        "SUBSTITUTE",
        "TRIM",
        "UNICHAR",
        "UNICODE",
        "UPPER",
        "VALUE",
        # TIME INTELLIGENCE
        "CLOSINGBALANCEWEEK",
        "CLOSINGBALANCEMONTH",
        "CLOSINGBALANCEQUARTER",
        "CLOSINGBALANCEYEAR",
        "DATEADD",
        "DATESBETWEEN",
        "DATESINPERIOD",
        "DATESWTD",
        "DATESMTD",
        "DATESQTD",
        "DATESYTD",
        "ENDOFWEEK",
        "ENDOFMONTH",
        "ENDOFQUARTER",
        "ENDOFYEAR",
        "FIRSTDATE",
        "LASTDATE",
        "NEXTDAY",
        "NEXTWEEK",
        "NEXTMONTH",
        "NEXTQUARTER",
        "NEXTYEAR",
        "OPENINGBALANCEWEEK",
        "OPENINGBALANCEMONTH",
        "OPENINGBALANCEQUARTER",
        "OPENINGBALANCEYEAR",
        "PARALLELPERIOD",
        "PREVIOUSDAY",
        "PREVIOUSWEEK",
        "PREVIOUSMONTH",
        "PREVIOUSYEAR",
        "SAMEPERIODLASTYEAR",
        "STARTOFWEEK",
        "STARTOFMONTH",
        "STARTOFQUARTER",
        "STARTOFYEAR",
        "TOTALWTD",
        "TOTALMTD",
        "TOTALQTD",
        "TOTALYTD",
        # DAX STATEMENTS
        "FUNCTION",
        "MEASURE",
    }

    def __init__(self, text, tom=None):

        self.tokens = list(tokenize(text))
        self.index = 0
        # Stack of sets of variable names currently in scope. Used to
        # distinguish references to VAR-declared names from table refs.
        self.scopes = []
        # Names of virtual columns introduced via ADDCOLUMNS / SELECTCOLUMNS
        # anywhere in the input. Bracketed references to these names are
        # emitted as VirtualColumn instead of Measure.
        self.virtual_columns = set()
        # Optional TOM model. When supplied, the parser uses it to
        # disambiguate bare bracketed references (`[Name]`) between
        # measures and columns. Without a model every `[Name]` defaults
        # to a Measure node (backward-compatible behavior).
        self._measure_names = set()
        self._column_names = set()
        if tom is not None:
            model = getattr(tom, "model", tom)
            for t in model.Tables:
                for m in t.Measures:
                    self._measure_names.add(m.Name)
                for c in t.Columns:
                    self._column_names.add(c.Name)

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

            # When a TOM model is supplied, a `Table[Name]` token where
            # `Name` resolves to a measure (not a column) is a
            # fully-qualified measure reference. Emit a Measure node with
            # the table preserved so downstream analyses can flag it.
            if self._measure_names and column in self._measure_names:
                if column not in self._column_names:
                    return Measure(
                        args={
                            "table": table,
                            "this": column,
                        }
                    )

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

            # When a TOM model is available, disambiguate bare bracketed
            # references: prefer measure resolution, then fall back to an
            # unqualified column reference (Column with table=None) if a
            # column of that name exists in the model. Without a model we
            # keep the legacy default of emitting a Measure node.
            if self._measure_names or self._column_names:
                if name in self._measure_names:
                    return Measure(args={"this": name})
                if name in self._column_names:
                    return Column(args={"table": None, "this": name})

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


def parse_dax(text: str, tom=None):
    """Parse a DAX expression and return its AST.

    Parameters
    ----------
    text : str
        The DAX expression.
    tom : TOMWrapper | Microsoft.AnalysisServices.Tabular.Model, optional
        When supplied, the parser uses the model to disambiguate bare
        bracketed references. A ``[Name]`` reference resolves to a
        ``Measure`` node if a measure with that name exists, otherwise to
        a ``Column`` node with ``table=None`` (an unqualified column) if a
        column with that name exists, otherwise it falls back to
        ``Measure`` (the default when no model is provided).
    """
    return Parser(text, tom=tom).parse()