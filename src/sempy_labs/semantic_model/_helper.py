from typing import Optional, Set, Tuple
import re
import sqlglot
from sqlglot import expressions as exp
from sqlglot.errors import ErrorLevel
import sempy_labs._icons as icons


def convert_column_data_type(str_type: str) -> str:

    TYPE_MAPPING = {
        "boolean": "Boolean",
        "tinyint": "Int64",
        "smallint": "Int64",
        "int": "Int64",
        "integer": "Int64",
        "bigint": "Int64",
        "long": "Int64",
        "short": "Int64",
        "float": "Double",
        "double": "Double",
        "decimal": "Decimal",
        "string": "String",
        "char": "String",
        "varchar": "String",
        "binary": "Binary",
        "date": "DateTime",
        "timestamp": "DateTime",
        "timestamp_ntz": "DateTime",
    }
    str_type = str_type.lower()
    if str_type in TYPE_MAPPING:
        return TYPE_MAPPING[str_type]
    if "decimal" in str_type:
        return "Decimal"
    if "char" in str_type or "string" in str_type:
        return "String"
    if "int" in str_type or "long" in str_type:
        return "Int64"
    if "float" in str_type or "double" in str_type:
        return "Double"
    else:
        print(f"Warning: Unrecognized data type '{str_type}'. Defaulting to 'String'.")
        return "String"


def convert_sql_to_dax(
    sql: str,
    column_map: dict[str, str],
    default_table: str = "summary",
    relationships: Optional[list[dict]] = None,
) -> str:
    """Translate a SQL aggregation expression into a DAX measure expression.

    Parsing is delegated to :mod:`sqlglot`; the resulting AST is walked to
    produce DAX. ``column_map`` maps SQL identifiers (either ``column`` or
    ``table.column``, case-insensitive) to their DAX form, e.g.
    ``'table'[column]`` for a column or ``[Measure Name]`` for a measure
    reference.

    ``BLANK()`` is returned when the SQL cannot be fully translated (it fails
    to parse, references a column which cannot be resolved, or uses a function
    with no DAX equivalent) since an incorrect expression is worse than none.
    """

    rel_from_to: Set[Tuple[str, str]] = set()
    for r in relationships or []:
        if not isinstance(r, dict):
            continue
        ft = r.get("fromTable")
        tt = r.get("toTable")
        if ft and tt:
            rel_from_to.add((ft, tt))

    if not sql or not sql.strip():
        return ""

    # sqlglot does not support backtick-quoted identifiers in the snowflake
    # dialect, but does in the spark dialect; spark also leaves DIV0 and
    # MEASURE as anonymous calls (instead of expanding them to CASE/IF), so
    # we use spark for parsing here.
    try:
        tree = sqlglot.parse_one(sql, dialect="spark")
    except Exception:
        return _unsupported_dax(sql, ["the expression could not be parsed"])

    converter = _SqlToDaxConverter(
        column_map=column_map,
        default_table=default_table,
        rel_from_to=rel_from_to,
    )
    dax = converter.emit(tree)
    if converter.unsupported or not dax.strip():
        return _unsupported_dax(sql, converter.unsupported)

    return dax


def _unsupported_dax(sql: str, reasons: list) -> str:
    """Warn about an untranslatable SQL expression and return ``BLANK()``."""

    detail = "; ".join(dict.fromkeys(reasons)) or "the expression is not supported"
    print(
        f"{icons.warning} The SQL expression '{' '.join(sql.split())}' could not be converted to DAX ({detail}). 'BLANK()' is used instead."
    )
    return "BLANK()"


_AGG_TO_ITER = {
    "SUM": "SUMX",
    "AVERAGE": "AVERAGEX",
    "MIN": "MINX",
    "MAX": "MAXX",
    "COUNT": "COUNTX",
    "RANK": "RANKX",
    "PRODUCT": "PRODUCTX",
}

# SQL function names (as emitted by sqlglot) which differ in DAX.
_FUNCTION_NAME_MAP = {
    "LENGTH": "LEN",
    "SUBSTRING": "MID",
    "SUBSTR": "MID",
    "CHAR_LENGTH": "LEN",
    "STDDEV": "STDEV.S",
    "STDDEV_SAMP": "STDEV.S",
    "STDDEV_POP": "STDEV.P",
    "VARIANCE": "VAR.S",
    "VARIANCE_SAMP": "VAR.S",
    "VARIANCE_POP": "VAR.P",
}

# DAX functions which may be emitted by the generic (name-preserving) path.
# Any other function name means the SQL has no DAX equivalent.
_DAX_FUNCTIONS = {
    "ABS",
    "ACOS",
    "ASIN",
    "ATAN",
    "AVERAGE",
    "BLANK",
    "CEILING",
    "CONCATENATE",
    "CONVERT",
    "COS",
    "COT",
    "COUNT",
    "COUNTROWS",
    "DATE",
    "DATEDIFF",
    "DATEVALUE",
    "DAY",
    "DEGREES",
    "DISTINCTCOUNT",
    "DIVIDE",
    "EDATE",
    "EOMONTH",
    "EVEN",
    "EXACT",
    "EXP",
    "FIND",
    "FIXED",
    "FLOOR",
    "FORMAT",
    "HOUR",
    "IF",
    "INT",
    "ISBLANK",
    "LEFT",
    "LEN",
    "LN",
    "LOG",
    "LOG10",
    "LOWER",
    "MAX",
    "MEDIAN",
    "MID",
    "MIN",
    "MINUTE",
    "MOD",
    "MONTH",
    "NOW",
    "ODD",
    "PATHITEM",
    "PI",
    "POWER",
    "QUARTER",
    "RADIANS",
    "RAND",
    "REPT",
    "RIGHT",
    "ROUND",
    "ROUNDDOWN",
    "ROUNDUP",
    "SEARCH",
    "SECOND",
    "SIGN",
    "SIN",
    "SQRT",
    "STDEV.P",
    "STDEV.S",
    "SUBSTITUTE",
    "SUM",
    "TAN",
    "TIME",
    "TIMEVALUE",
    "TODAY",
    "TRIM",
    "TRUNC",
    "UPPER",
    "VALUE",
    "VAR.P",
    "VAR.S",
    "WEEKDAY",
    "WEEKNUM",
    "YEAR",
    "YEARFRAC",
}

# Date format tokens, longest first so that e.g. 'YYYY' is matched before 'YY'.
_DATE_FORMAT_TOKENS = [
    ("YYYY", "yyyy"),
    ("MONTH", "MMMM"),
    ("HH24", "HH"),
    ("HH12", "hh"),
    ("MON", "MMM"),
    ("DAY", "dddd"),
    ("MMMM", "MMMM"),
    ("YY", "yy"),
    ("MM", "MM"),
    ("DD", "dd"),
    ("DY", "ddd"),
    ("HH", "hh"),
    ("MI", "mm"),
    ("SS", "ss"),
    ("AM", "AM/PM"),
    ("PM", "AM/PM"),
]


def _convert_date_format(fmt: str) -> str:
    """Convert a SQL date format string (e.g. ``YYYY-MM``) to the DAX equivalent."""

    if not fmt:
        return ""
    upper = fmt.upper()
    result: list[str] = []
    i = 0
    while i < len(upper):
        for token, dax_token in _DATE_FORMAT_TOKENS:
            if upper.startswith(token, i):
                result.append(dax_token)
                i += len(token)
                break
        else:
            result.append(fmt[i])
            i += 1
    return "".join(result)


class _SqlToDaxConverter:
    """AST walker that emits DAX from a sqlglot expression tree."""

    def __init__(
        self,
        column_map: dict,
        default_table: str,
        rel_from_to: Set[Tuple[str, str]],
    ) -> None:
        self.column_map = column_map or {}
        # Build a case-insensitive lookup. ``column_map`` keys may be wrapped
        # in backticks (e.g. ```Energy Source```) or appear as
        # ``table.`column with spaces```. Normalize by also registering
        # variants with all backticks stripped so the resolver can match
        # identifiers parsed by sqlglot (whose ``Column.name`` strips the
        # quotes).
        self._column_map_lower: dict = {}
        for k, v in self.column_map.items():
            self._column_map_lower.setdefault(k.lower(), v)
            stripped = k.replace("`", "")
            if stripped and stripped != k:
                self._column_map_lower.setdefault(stripped.lower(), v)
        self.default_table = default_table
        self.rel_from_to = rel_from_to
        # Reasons why the SQL has no faithful DAX equivalent; when non-empty
        # the caller returns BLANK() instead of the emitted expression.
        self.unsupported: list = []
        self._var_index = 0
        # When non-empty, columns belonging to a table other than the
        # current iterator table are wrapped in ``RELATED(...)``.
        self._iter_table_stack: list = []

    # ---------- column resolution ----------

    def lookup_column(self, table: Optional[str], name: str) -> Optional[str]:
        """Resolve a SQL column reference to its DAX form via ``column_map``.

        Lookup is case-insensitive. ``table.column`` is preferred over a
        bare ``column`` lookup.
        """
        if table:
            key = f"{table}.{name}".lower()
            if key in self._column_map_lower:
                return self._column_map_lower[key]
        if name and name.lower() in self._column_map_lower:
            return self._column_map_lower[name.lower()]
        return None

    @staticmethod
    def _table_of_dax_ref(dax: str) -> Optional[str]:
        """Extract the table name from a DAX ``'table'[column]`` reference."""
        if not dax or not dax.startswith("'"):
            return None
        end = dax.find("'", 1)
        if end <= 1:
            return None
        return dax[1:end]

    # ---------- main dispatch ----------

    def emit(self, node) -> str:  # noqa: C901
        if node is None:
            return ""

        # --- atoms / literals ---
        if isinstance(node, exp.Paren):
            return f"({self.emit(node.this)})"
        if isinstance(node, exp.Literal):
            if node.is_string:
                return f'"{node.this}"'
            return str(node.this)
        if isinstance(node, exp.Boolean):
            return "TRUE()" if node.this else "FALSE()"
        if isinstance(node, exp.Null):
            return "BLANK()"
        if isinstance(node, exp.Star):
            return "*"
        if isinstance(node, exp.Identifier):
            return node.name
        if isinstance(node, exp.Column):
            return self._emit_column(node)

        # --- unary ---
        if isinstance(node, exp.Neg):
            return f"-{self.emit(node.this)}"
        if isinstance(node, exp.Not):
            return f"NOT({self.emit(node.this)})"

        # --- binary arithmetic ---
        if isinstance(node, exp.Div):
            return self._emit_div(node)
        if isinstance(node, exp.Add):
            return f"{self.emit(node.this)} + {self.emit(node.expression)}"
        if isinstance(node, exp.Sub):
            return f"{self.emit(node.this)} - {self.emit(node.expression)}"
        if isinstance(node, exp.Mul):
            return f"{self.emit(node.this)} * {self.emit(node.expression)}"
        if isinstance(node, exp.Mod):
            return f"MOD({self.emit(node.this)}, {self.emit(node.expression)})"
        if isinstance(node, exp.DPipe):
            return f"{self.emit(node.this)} & {self.emit(node.expression)}"

        # --- comparisons / logical ---
        if isinstance(node, exp.EQ):
            return f"{self.emit(node.this)} = {self.emit(node.expression)}"
        if isinstance(node, exp.NEQ):
            return f"{self.emit(node.this)} <> {self.emit(node.expression)}"
        if isinstance(node, exp.GT):
            return f"{self.emit(node.this)} > {self.emit(node.expression)}"
        if isinstance(node, exp.GTE):
            return f"{self.emit(node.this)} >= {self.emit(node.expression)}"
        if isinstance(node, exp.LT):
            return f"{self.emit(node.this)} < {self.emit(node.expression)}"
        if isinstance(node, exp.LTE):
            return f"{self.emit(node.this)} <= {self.emit(node.expression)}"
        if isinstance(node, exp.And):
            return f"{self.emit(node.this)} && {self.emit(node.expression)}"
        if isinstance(node, exp.Or):
            return f"{self.emit(node.this)} || {self.emit(node.expression)}"
        if isinstance(node, exp.Is):
            right = node.expression
            if isinstance(right, exp.Null):
                return f"ISBLANK({self.emit(node.this)})"
            return f"{self.emit(node.this)} = {self.emit(right)}"
        if isinstance(node, exp.In):
            items = node.args.get("expressions") or []
            rendered = ", ".join(self.emit(e) for e in items)
            return f"{self.emit(node.this)} IN {{{rendered}}}"
        if isinstance(node, exp.Between):
            return (
                f"({self.emit(node.this)} >= {self.emit(node.args.get('low'))} "
                f"&& {self.emit(node.this)} <= {self.emit(node.args.get('high'))})"
            )

        # --- aggregations ---
        if isinstance(node, exp.Sum):
            return self._emit_agg("SUM", node.this)
        if isinstance(node, exp.Avg):
            return self._emit_agg("AVERAGE", node.this)
        if isinstance(node, exp.Min):
            return self._emit_agg("MIN", node.this)
        if isinstance(node, exp.Max):
            return self._emit_agg("MAX", node.this)
        if isinstance(node, exp.Count):
            return self._emit_count(node)

        # --- structural ---
        if isinstance(node, exp.Distinct):
            inner = (node.expressions or [None])[0]
            return self.emit(inner)
        if isinstance(node, exp.Case):
            return self._emit_case(node)
        if isinstance(node, exp.If):
            return self._emit_if(node)
        if isinstance(node, exp.Window):
            return self._emit_window(node)
        if isinstance(node, exp.Filter):
            return self._emit_filter(node)
        if isinstance(node, exp.Nullif):
            return self._emit_nullif(node.this, node.expression)
        if isinstance(node, exp.Coalesce):
            args = [self.emit(node.this)] + [
                self.emit(e) for e in (node.expressions or [])
            ]
            return f"COALESCE({', '.join(args)})"
        if isinstance(node, exp.Cast):
            # DAX has no general CAST; emit the value untouched.
            return self.emit(node.this)
        if isinstance(node, exp.Anonymous):
            return self._emit_anonymous(node)
        if isinstance(node, exp.Func):
            return self._emit_function(node)

        # Fallback: defer to sqlglot.
        self.unsupported.append(f"'{type(node).__name__}' has no DAX equivalent")
        return node.sql(dialect="spark", unsupported_level=ErrorLevel.IGNORE)

    # ---------- columns ----------

    def _emit_column(self, node: "exp.Column") -> str:
        tbl = node.table or None
        name = node.name
        dax = self.lookup_column(tbl, name)
        if dax is None:
            # Unresolved — emit as bare or qualified identifier.
            self.unsupported.append(
                f"the '{f'{tbl}.{name}' if tbl else name}' column could not be resolved"
            )
            return f"{tbl}.{name}" if tbl else name

        # Apply RELATED wrapping if we are emitting inside an iterator on a
        # different table.
        iter_table = self._iter_table_stack[-1] if self._iter_table_stack else None
        if iter_table and dax.startswith("'"):
            ref_table = self._table_of_dax_ref(dax)
            if ref_table and ref_table != iter_table:
                return f"RELATED({dax})"
        return dax

    # ---------- DIV / NULLIF ----------

    def _emit_div(self, node: "exp.Div") -> str:
        num = self.emit(node.this)
        denom_node = node.expression
        # Unwrap NULLIF(<x>, 0) on the denominator since DIVIDE is
        # already divide-by-zero-safe.
        if (
            isinstance(denom_node, exp.Nullif)
            and isinstance(denom_node.expression, exp.Literal)
            and not denom_node.expression.is_string
            and str(denom_node.expression.this) == "0"
        ):
            denom = self.emit(denom_node.this)
        elif (
            isinstance(denom_node, exp.Anonymous)
            and denom_node.name.upper() == "NULLIF"
        ):
            args = denom_node.expressions or []
            if (
                len(args) == 2
                and isinstance(args[1], exp.Literal)
                and str(args[1].this) == "0"
            ):
                denom = self.emit(args[0])
            else:
                denom = self.emit(denom_node)
        else:
            denom = self.emit(denom_node)
        return f"DIVIDE({num}, {denom})"

    # ---------- COUNT family ----------

    def _emit_count(self, node: "exp.Count") -> str:
        arg = node.this
        if isinstance(arg, exp.Distinct):
            inner = (arg.expressions or [None])[0]
            if isinstance(inner, exp.Case):
                rewritten = self._count_distinct_case(inner)
                if rewritten is not None:
                    return rewritten
            if inner is None:
                return f"COUNTROWS('{self.default_table}')"
            inner_dax = self.emit(inner)
            if self._is_column_reference(inner_dax):
                return f"DISTINCTCOUNT({inner_dax})"
            # DISTINCTCOUNT only accepts a column, so materialize the values.
            table = self._select_iter_table(inner)
            return (
                f"COUNTROWS(DISTINCT(SELECTCOLUMNS('{table}', \"Value\", {inner_dax})))"
            )
        if arg is None or isinstance(arg, exp.Star):
            return f"COUNTROWS('{self.default_table}')"
        arg_dax = self.emit(arg)
        if self._is_column_reference(arg_dax):
            return f"COUNT({arg_dax})"
        return f"COUNTX('{self._select_iter_table(arg)}', {arg_dax})"

    @staticmethod
    def _is_column_reference(dax: str) -> bool:
        """Return True if ``dax`` is a plain ``'table'[column]`` reference."""

        return re.fullmatch(r"'[^']+'\[[^\]]+\]", dax or "") is not None

    def _emit_nullif(self, value_node, other_node) -> str:
        """Emit ``NULLIF(<value>, <other>)`` without evaluating ``<value>`` twice."""

        value = self.emit(value_node)
        other = self.emit(other_node)
        if self._is_column_reference(value) or isinstance(value_node, exp.Literal):
            return f"IF({value} = {other}, BLANK(), {value})"
        self._var_index += 1
        name = f"__value{self._var_index}"
        # Parenthesized so the VAR block stays valid when nested in another expression.
        return f"(VAR {name} = {value} RETURN IF({name} = {other}, BLANK(), {name}))"

    def _count_distinct_case(self, case_node: "exp.Case") -> Optional[str]:
        """Translate ``COUNT(DISTINCT CASE WHEN cond THEN col END)``."""
        ifs = case_node.args.get("ifs") or []
        default = case_node.args.get("default")
        if len(ifs) == 1 and default is None:
            first = ifs[0]
            cond = self.emit(first.this)
            then = self.emit(first.args.get("true"))
            if self._is_column_reference(then):
                return f"CALCULATE(DISTINCTCOUNT({then}), {cond})"
        return None

    # ---------- CASE / IF ----------

    def _emit_case(self, node: "exp.Case") -> str:
        ifs = node.args.get("ifs") or []
        default = node.args.get("default")
        result = self.emit(default) if default is not None else "BLANK()"
        for i in reversed(ifs):
            cond = self.emit(i.this)
            then = self.emit(i.args.get("true"))
            result = f"IF({cond}, {then}, {result})"
        return result

    def _emit_if(self, node: "exp.If") -> str:
        cond = self.emit(node.this)
        then = self.emit(node.args.get("true"))
        else_ = node.args.get("false")
        if else_ is not None:
            return f"IF({cond}, {then}, {self.emit(else_)})"
        return f"IF({cond}, {then})"

    # ---------- FILTER (WHERE ...) ----------

    def _emit_filter(self, node: "exp.Filter") -> str:
        agg_dax = self.emit(node.this)
        where = node.expression
        cond_node = where.this if isinstance(where, exp.Where) else where
        return f"CALCULATE({agg_dax}, {self.emit(cond_node)})"

    # ---------- WINDOW ----------

    def _emit_window(self, node: "exp.Window") -> str:
        inner = node.this
        order = node.args.get("order")
        spec = node.args.get("spec")
        partition = node.args.get("partition_by")

        # Detect ROWS BETWEEN N PRECEDING AND CURRENT ROW
        n_preceding: Optional[int] = None
        if spec is not None:
            kind = spec.args.get("kind")
            start = spec.args.get("start")
            start_side = spec.args.get("start_side")
            end = spec.args.get("end")
            if (
                kind
                and str(kind).upper() == "ROWS"
                and start_side
                and str(start_side).upper() == "PRECEDING"
                and end
                and "CURRENT" in str(end).upper()
                and isinstance(start, exp.Literal)
                and not start.is_string
            ):
                try:
                    n_preceding = int(start.this)
                except (TypeError, ValueError):
                    n_preceding = None

        # Order column → DAX
        order_col_dax: Optional[str] = None
        if order is not None:
            ord_exprs = order.args.get("expressions") or []
            if ord_exprs:
                first = ord_exprs[0]
                col = first.this if isinstance(first, exp.Ordered) else first
                order_col_dax = self.emit(col)

        # If the windowed function is itself an aggregate of an aggregate
        # (a common Snowflake/BigQuery pattern: ``SUM(SUM(x)) OVER (...)``)
        # strip the redundant outer aggregate.
        body_node = inner
        if isinstance(inner, (exp.Sum, exp.Avg, exp.Min, exp.Max, exp.Count)):
            child = inner.this
            if isinstance(child, exp.Distinct) and child.expressions:
                child = child.expressions[0]
            if isinstance(child, (exp.Sum, exp.Avg, exp.Min, exp.Max, exp.Count)):
                body_node = child

        body_dax = self.emit(body_node)

        if n_preceding is not None and order_col_dax:
            return (
                f"CALCULATE({body_dax}, "
                f"DATESINPERIOD({order_col_dax}, MAX({order_col_dax}), "
                f"-{n_preceding}, DAY))"
            )
        if not spec and not order and not partition:
            return self._emit_unbounded_window(body_node, body_dax)
        # Unsupported window shape — fall back to the body without the window.
        return body_dax

    def _emit_unbounded_window(self, body_node, body_dax: str) -> str:
        """Emit DAX for ``<agg>(...) OVER ()`` — no partition, order, or frame.

        For a simple aggregate (``SUM`` / ``AVG`` / ``MIN`` / ``MAX``) the
        result is the iterator form over ``ALL(<table>)``::

            MAX(cutoff) OVER ()
              ->  MAXX(ALL('table'), 'table'[cutoff])

        ``COUNT(*)`` becomes ``COUNTROWS(ALL(<table>))``, and
        ``COUNT(DISTINCT col)`` becomes
        ``CALCULATE(DISTINCTCOUNT(col), ALL(<table>))``. Anything else
        falls back to ``CALCULATE(<body>, ALL('default_table'))``.
        """

        agg_map = {
            exp.Sum: "SUM",
            exp.Avg: "AVERAGE",
            exp.Min: "MIN",
            exp.Max: "MAX",
        }

        if isinstance(body_node, tuple(agg_map.keys())):
            func = agg_map[type(body_node)]
            arg = body_node.this
            while isinstance(arg, exp.Paren):
                arg = arg.this
            iter_table = self._select_iter_table(arg)
            self._iter_table_stack.append(iter_table)
            try:
                arg_dax = self.emit(arg)
            finally:
                self._iter_table_stack.pop()
            return f"{_AGG_TO_ITER[func]}(ALL('{iter_table}'), {arg_dax})"

        if isinstance(body_node, exp.Count):
            arg = body_node.this
            if arg is None or isinstance(arg, exp.Star):
                return f"COUNTROWS(ALL('{self.default_table}'))"
            if isinstance(arg, exp.Distinct):
                inner_col = (arg.expressions or [None])[0]
                if inner_col is not None:
                    iter_table = self._select_iter_table(inner_col)
                    return (
                        f"CALCULATE(DISTINCTCOUNT({self.emit(inner_col)}), "
                        f"ALL('{iter_table}'))"
                    )
            iter_table = self._select_iter_table(arg)
            self._iter_table_stack.append(iter_table)
            try:
                arg_dax = self.emit(arg)
            finally:
                self._iter_table_stack.pop()
            return f"COUNTX(ALL('{iter_table}'), {arg_dax})"

        # Non-aggregate body — fall back to CALCULATE over ALL(default_table).
        return f"CALCULATE({body_dax}, ALL('{self.default_table}'))"

    def _select_iter_table(self, node) -> str:
        """Choose the iterator table for ``node`` (same logic as
        ``_emit_iterator``).

        Prefers the "many" side of a supplied relationship; otherwise falls
        back to ``default_table`` if it is among the referenced tables,
        then to the first referenced table, then to ``default_table``.
        """
        ref_tables: list = []
        for c in node.find_all(exp.Column):
            dax = self.lookup_column(c.table, c.name)
            tbl = self._table_of_dax_ref(dax) if dax else c.table
            if tbl and tbl not in ref_tables:
                ref_tables.append(tbl)

        if self.rel_from_to and len(ref_tables) > 1:
            for cand in ref_tables:
                for other in ref_tables:
                    if cand == other:
                        continue
                    if (cand, other) in self.rel_from_to:
                        return cand
        if self.default_table and self.default_table in ref_tables:
            return self.default_table
        if ref_tables:
            return ref_tables[0]
        return self.default_table

    # ---------- Anonymous functions (DIV0, MEASURE, etc.) ----------

    def _emit_anonymous(self, node: "exp.Anonymous") -> str:
        name = node.name.upper()
        args = node.expressions or []
        if name == "MEASURE" and args:
            first = args[0]
            measure_name = first.name if isinstance(first, exp.Column) else None
            if not measure_name and isinstance(first, exp.Literal):
                measure_name = str(first.this)
            if measure_name:
                # If the column_map has a measure mapping (e.g. for derived
                # metrics that share the same name), prefer it.
                resolved = self.lookup_column(None, measure_name)
                return resolved if resolved else f"[{measure_name}]"
        if name == "DIV0" and len(args) == 2:
            return f"DIVIDE({self.emit(args[0])}, {self.emit(args[1])})"
        if name == "NULLIF" and len(args) == 2:
            return self._emit_nullif(args[0], args[1])
        if name in ("IFF", "IIF") and len(args) == 3:
            return (
                f"IF({self.emit(args[0])}, {self.emit(args[1])}, {self.emit(args[2])})"
            )
        if name in ("TO_DATE", "TRY_TO_DATE", "DATE_FROM_PARTS") and args:
            return f"DATEVALUE({self.emit(args[0])})"
        if name in ("TO_VARCHAR", "TRY_TO_VARCHAR", "TO_CHAR", "TRY_TO_CHAR") and args:
            if len(args) > 1:
                fmt = _convert_date_format(self._literal_text(args[1]))
                return f'FORMAT({self.emit(args[0])}, "{fmt}")'
            return f"CONVERT({self.emit(args[0])}, STRING)"
        if (
            name
            in (
                "TO_NUMBER",
                "TRY_TO_NUMBER",
                "TO_DECIMAL",
                "TRY_TO_DECIMAL",
                "TO_DOUBLE",
                "TRY_TO_DOUBLE",
            )
            and args
        ):
            return f"VALUE({self.emit(args[0])})"
        if name == "ZEROIFNULL" and args:
            value = self.emit(args[0])
            return f"IF(ISBLANK({value}), 0, {value})"
        if name == "NULLIFZERO" and args:
            value = self.emit(args[0])
            return f"IF({value} = 0, BLANK(), {value})"
        rendered = ", ".join(self.emit(a) for a in args)
        return f"{self._validate_function_name(name)}({rendered})"

    # ---------- Scalar functions ----------

    def _emit_function(self, node) -> str:
        """Emit DAX for a scalar SQL function, translating where DAX differs."""

        # Wrappers added by parsing (e.g. CAST(x AS DATE)) carry no meaning in DAX.
        if type(node).__name__.startswith("TsOrDs"):
            return self.emit(node.this)
        if isinstance(node, exp.CurrentDate):
            return "TODAY()"
        if isinstance(node, exp.CurrentTimestamp):
            return "NOW()"
        if isinstance(node, exp.ToChar):
            value = self.emit(node.this)
            fmt = node.args.get("format")
            if fmt is None:
                return f"CONVERT({value}, STRING)"
            return f'FORMAT({value}, "{_convert_date_format(self._literal_text(fmt))}")'
        if isinstance(node, exp.Monthname):
            return f'FORMAT({self.emit(node.this)}, "MMM")'
        if isinstance(node, (exp.DateTrunc, exp.TimestampTrunc)):
            return self._emit_date_trunc(node)
        if isinstance(node, (exp.DateDiff, exp.TimestampDiff, exp.DatetimeDiff)):
            unit = self._unit_name(node.args.get("unit")) or "DAY"
            # sqlglot stores the end date in 'this' and the start date in 'expression'.
            return f"DATEDIFF({self.emit(node.expression)}, {self.emit(node.this)}, {unit})"
        if isinstance(node, (exp.DateAdd, exp.TimestampAdd, exp.DatetimeAdd)):
            return self._emit_date_add(node)
        if isinstance(node, (exp.DateSub, exp.DatetimeSub)):
            return self._emit_date_add(node, negate=True)
        if isinstance(node, exp.SplitPart):
            value = self.emit(node.this)
            delimiter = self.emit(node.args.get("delimiter"))
            index = self.emit(node.args.get("part_index"))
            return f'PATHITEM(SUBSTITUTE({value}, {delimiter}, "|"), {index})'
        if isinstance(node, exp.Pad):
            return self._emit_pad(node)
        if isinstance(node, (exp.Concat, exp.ConcatWs)):
            return self._emit_concat(node)
        if isinstance(node, exp.Substring):
            value = self.emit(node.this)
            start = self.emit(node.args.get("start")) or "1"
            length = node.args.get("length")
            length_dax = self.emit(length) if length is not None else f"LEN({value})"
            return f"MID({value}, {start}, {length_dax})"
        if isinstance(node, exp.Round):
            decimals = node.args.get("decimals")
            return f"ROUND({self.emit(node.this)}, {self.emit(decimals) if decimals is not None else 0})"
        if isinstance(node, exp.Ceil):
            return f"ROUNDUP({self.emit(node.this)}, 0)"
        if isinstance(node, exp.Floor):
            return f"ROUNDDOWN({self.emit(node.this)}, 0)"

        name = type(node).sql_names()[0].upper()
        name = _FUNCTION_NAME_MAP.get(name, name)
        rendered = ", ".join(self.emit(a) for a in self._function_args(node))
        return f"{self._validate_function_name(name)}({rendered})"

    def _validate_function_name(self, name: str) -> str:
        """Flag function names which are not valid DAX functions."""

        if name not in _DAX_FUNCTIONS:
            self.unsupported.append(f"the '{name}' function has no DAX equivalent")
        return name

    @staticmethod
    def _function_args(node) -> list:
        """Return the expression arguments of a function node, in SQL order."""

        args: list = []
        for key in type(node).arg_types:
            value = node.args.get(key)
            if value is None:
                continue
            for item in value if isinstance(value, list) else [value]:
                if isinstance(item, exp.Expression):
                    args.append(item)
        return args

    @staticmethod
    def _literal_text(node) -> str:
        """Return the text of a literal / identifier node."""

        if node is None:
            return ""
        if isinstance(node, (exp.Literal, exp.Var, exp.Identifier)):
            return str(node.this)
        return str(node)

    @classmethod
    def _unit_name(cls, node) -> Optional[str]:
        """Return the upper-cased date part of a date function's unit argument."""

        unit = cls._literal_text(node).upper().strip("'\"")
        return unit or None

    def _emit_date_trunc(self, node) -> str:
        unit = self._unit_name(node.args.get("unit")) or "DAY"
        value = self.emit(node.this)
        if unit.startswith("YEAR"):
            return f"DATE(YEAR({value}), 1, 1)"
        if unit.startswith("QUARTER"):
            return f"DATE(YEAR({value}), (QUARTER({value}) - 1) * 3 + 1, 1)"
        if unit.startswith("MONTH"):
            return f"DATE(YEAR({value}), MONTH({value}), 1)"
        if unit.startswith("WEEK"):
            # WEEKDAY(<date>, 3) is 0 for Monday, matching the ISO week start.
            return f"{value} - WEEKDAY({value}, 3)"
        if unit.startswith("DAY") or unit.startswith("DATE"):
            return f"DATE(YEAR({value}), MONTH({value}), DAY({value}))"
        return value

    def _emit_date_add(self, node, negate: bool = False) -> str:
        unit = self._unit_name(node.args.get("unit")) or "DAY"
        value = self.emit(node.this)
        amount = self.emit(node.expression)
        if negate:
            amount = f"-({amount})"
        if unit.startswith("YEAR"):
            return f"EDATE({value}, ({amount}) * 12)"
        if unit.startswith("QUARTER"):
            return f"EDATE({value}, ({amount}) * 3)"
        if unit.startswith("MONTH"):
            return f"EDATE({value}, {amount})"
        if unit.startswith("WEEK"):
            return f"{value} + ({amount}) * 7"
        return f"{value} + ({amount})"

    def _emit_pad(self, node: "exp.Pad") -> str:
        value = self.emit(node.this)
        length = self.emit(node.expression)
        fill = node.args.get("fill_pattern")
        fill_dax = self.emit(fill) if fill is not None else '" "'
        if node.args.get("is_left"):
            return f"RIGHT(REPT({fill_dax}, {length}) & {value}, {length})"
        return f"LEFT({value} & REPT({fill_dax}, {length}), {length})"

    def _emit_concat(self, node) -> str:
        parts = [self.emit(e) for e in (node.expressions or [])]
        if isinstance(node, exp.ConcatWs) and parts:
            separator, values = parts[0], parts[1:]
            return f" & {separator} & ".join(values)
        return " & ".join(parts)

    # ---------- Aggregation rewriting (distribute / iterator) ----------

    def _emit_agg(self, func: str, arg) -> str:
        """Emit ``SUM`` / ``AVERAGE`` / ``MIN`` / ``MAX`` over ``arg``.

        * Single column reference → scalar ``func(col)``.
        * Top-level additive expression → distribute the aggregation.
        * Anything else → iterator form (``SUMX`` etc.).
        """

        if arg is None:
            return f"{func}()"
        # Strip outer parens
        inner = arg
        while isinstance(inner, exp.Paren):
            inner = inner.this

        if isinstance(inner, exp.Column):
            return f"{func}({self._emit_agg_column(inner)})"

        terms = self._collect_additive(inner)
        if len(terms) > 1:
            return self._emit_distributed(func, terms)
        return self._emit_iterator(func, inner)

    def _emit_agg_column(self, col: "exp.Column") -> str:
        """Emit a column reference suitable as the sole argument to a scalar
        aggregation. Bare unresolved identifiers are qualified to
        ``default_table``."""
        dax = self.lookup_column(col.table, col.name)
        if dax is not None:
            return dax
        if self.default_table:
            return f"'{self.default_table}'[{col.name}]"
        return col.name

    def _collect_additive(self, node):
        """Split ``node`` on top-level ``+`` / ``-`` into ``(sign, sub-node)``."""
        terms: list = []

        def walk(n, sign):
            if isinstance(n, exp.Paren):
                walk(n.this, sign)
            elif isinstance(n, exp.Add):
                walk(n.this, sign)
                walk(n.expression, sign)
            elif isinstance(n, exp.Sub):
                walk(n.this, sign)
                walk(n.expression, "-" if sign == "+" else "+")
            else:
                terms.append((sign, n))

        walk(node, "+")
        return terms

    def _emit_distributed(self, func: str, terms: list) -> str:
        rendered: list = []
        for sign, term in terms:
            inner = term
            while isinstance(inner, exp.Paren):
                inner = inner.this
            if isinstance(inner, exp.Column):
                rendered.append((sign, f"{func}({self._emit_agg_column(inner)})"))
                continue
            nested = self._collect_additive(inner)
            if len(nested) > 1:
                rendered.append((sign, self._emit_distributed(func, nested)))
            else:
                rendered.append((sign, self._emit_iterator(func, inner)))

        pieces: list = []
        first_sign, first_text = rendered[0]
        if first_sign == "-":
            pieces.append("-")
        pieces.append(first_text)
        for sign, text in rendered[1:]:
            pieces.append(f" {sign} ")
            pieces.append(text)
        return "(" + "".join(pieces) + ")"

    def _emit_iterator(self, func: str, term) -> str:
        """Emit ``SUMX`` / ``AVERAGEX`` / ``MINX`` / ``MAXX`` over ``term``.

        Iterator-table selection prefers the "many" side of any supplied
        relationship; otherwise falls back to ``default_table`` if it is
        among the referenced tables, then to the first referenced table.
        Columns belonging to a different table than the iterator are
        wrapped in ``RELATED(...)`` (handled by ``_emit_column`` via the
        ``_iter_table_stack``).
        """

        # Discover referenced tables (after column_map resolution).
        ref_tables: list = []
        for c in term.find_all(exp.Column):
            dax = self.lookup_column(c.table, c.name)
            tbl = self._table_of_dax_ref(dax) if dax else c.table
            if tbl and tbl not in ref_tables:
                ref_tables.append(tbl)

        iter_table: Optional[str] = None
        if self.rel_from_to and len(ref_tables) > 1:
            for cand in ref_tables:
                for other in ref_tables:
                    if cand == other:
                        continue
                    if (cand, other) in self.rel_from_to:
                        iter_table = cand
                        break
                if iter_table:
                    break
        if iter_table is None:
            if self.default_table and self.default_table in ref_tables:
                iter_table = self.default_table
            elif ref_tables:
                iter_table = ref_tables[0]
            else:
                iter_table = self.default_table

        # Wrap iteration-time emission so foreign columns get RELATED().
        self._iter_table_stack.append(iter_table)
        try:
            term_dax = self.emit(term)
        finally:
            self._iter_table_stack.pop()

        return f"{_AGG_TO_ITER[func]}('{iter_table}', {term_dax})"


def convert_format_from_databricks(fmt: dict = None) -> str | None:
    """
    Convert Databricks metric view format dictionary
    into a Power BI format string.

    Returns
    -------
    str | None
    """

    if not fmt:
        return None

    # =========================
    # Currency symbol resolver
    # =========================
    def get_currency_symbol(code: str) -> str:
        symbols = {
            "USD": "$",
            "AUD": "$",
            "CAD": "$",
            "EUR": "€",
            "GBP": "£",
            "ILS": "₪",
            "JPY": "¥",
            "CNY": "¥",
            "INR": "₹",
            "KRW": "₩",
            "RUB": "₽",
            "TRY": "₺",
            "BRL": "R$",
            "MXN": "$",
            "ZAR": "R",
            "CHF": "CHF ",
            "SEK": "kr",
            "NOK": "kr",
            "DKK": "kr",
            "PLN": "zł",
            "CZK": "Kč",
            "HUF": "Ft",
            "AED": "د.إ",
            "SAR": "﷼",
            "DZD": "DZD ",
        }
        return symbols.get((code or "").upper(), f"{code.upper()} " if code else "")

    # =========================
    # Helpers
    # =========================
    def build_decimal_part(decimal_info: dict, abbreviation: str) -> str:
        if not decimal_info:
            return ""

        dtype = decimal_info.get("type")
        places = decimal_info.get("places", 0)

        # COMPACT → cap decimals
        if abbreviation == "COMPACT":
            max_places = min(places if places else 2, 2)
            return "." + ("#" * max_places) if max_places > 0 else ""

        if dtype == "ALL":
            return ".########"

        if places == 0:
            return ""

        if dtype == "EXACT":
            return "." + ("0" * places)

        if dtype == "MAX":
            return "." + ("#" * places)

        return ""

    def build_scientific(decimal_info: dict) -> str:
        if not decimal_info:
            return "0E+00"

        dtype = decimal_info.get("type")
        places = decimal_info.get("places", 2)

        if dtype == "EXACT":
            return f"0.{ '0'*places }E+00" if places > 0 else "0E+00"

        if dtype == "MAX":
            return f"0.{ '#'*places }E+00" if places > 0 else "0E+00"

        if dtype == "ALL":
            return "0.00E+00"  # controlled default

        return "0.00E+00"

    def apply_grouping(base: str, hide_group_separator: bool) -> str:
        if hide_group_separator:
            return base.replace("#,0", "0")
        return base

    def apply_compact(base: str, abbreviation: str) -> str:
        if abbreviation == "COMPACT":
            return base + ",,"
        return base

    # =========================
    # Validation
    # =========================
    if not fmt or not isinstance(fmt, dict):
        return None

    key = next(iter(fmt), None)
    if not key:
        return None

    props = fmt.get(key, {})

    decimal_info = props.get("decimal_places")
    abbreviation = props.get("abbreviation", "NONE")
    hide_group = props.get("hide_group_separator", False)

    # =========================
    # NUMBER PLAIN
    # =========================
    if key == "number_plain":
        if abbreviation == "SCIENTIFIC":
            return build_scientific(decimal_info)

        decimal_part = build_decimal_part(decimal_info, abbreviation)
        base = f"#,0{decimal_part}"
        base = apply_grouping(base, hide_group)
        base = apply_compact(base, abbreviation)
        return base

    # =========================
    # NUMBER CURRENCY
    # =========================
    if key == "number_currency":
        symbol = get_currency_symbol(props.get("currency_code"))

        if abbreviation == "SCIENTIFIC":
            return f"{symbol}{build_scientific(decimal_info)}"

        decimal_part = build_decimal_part(decimal_info, abbreviation)
        base = f"{symbol}#,0{decimal_part}"
        base = apply_grouping(base, hide_group)
        base = apply_compact(base, abbreviation)
        return base

    # =========================
    # NUMBER PERCENT
    # =========================
    if key == "number_percent":
        if abbreviation == "SCIENTIFIC":
            return build_scientific(decimal_info) + "%"

        decimal_part = build_decimal_part(decimal_info, abbreviation)
        return f"0{decimal_part}%"

    # =========================
    # NUMBER BYTES
    # =========================
    if key == "number_bytes":
        decimal_part = build_decimal_part(decimal_info, abbreviation)
        base = f"#,0{decimal_part}"
        base = apply_grouping(base, hide_group)
        return base

    # =========================
    # DATE
    # =========================
    if key == "date":
        return {
            "YEAR_MONTH_DAY": "yyyy-MM-dd",
            "MONTH_DAY_YEAR": "MM/dd/yyyy",
        }.get(props.get("date_format"))

    # =========================
    # DATE TIME
    # =========================
    if key == "date_time":
        date_part = {"YEAR_MONTH_DAY": "yyyy-MM-dd"}.get(
            props.get("date_format"), "yyyy-MM-dd"
        )

        return f"{date_part} HH:mm:ss"

    # =========================
    # FALLBACK
    # =========================
    return None
