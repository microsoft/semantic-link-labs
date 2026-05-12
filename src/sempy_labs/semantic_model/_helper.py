import re
from typing import Optional, Set, Tuple
import sqlglot
from sqlglot import expressions as exp


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
        return sql.strip()

    converter = _SqlToDaxConverter(
        column_map=column_map,
        default_table=default_table,
        rel_from_to=rel_from_to,
    )
    return converter.emit(tree)


_AGG_TO_ITER = {"SUM": "SUMX", "AVERAGE": "AVERAGEX", "MIN": "MINX", "MAX": "MAXX", "COUNT": "COUNTX", "RANK": "RANKX", "PRODUCT": "PRODUCTX"}


class _SqlToDaxConverter:
    """AST walker that emits DAX from a sqlglot expression tree."""

    def __init__(
        self,
        column_map: dict,
        default_table: str,
        rel_from_to: Set[Tuple[str, str]],
    ) -> None:
        self.column_map = column_map or {}
        self._column_map_lower = {k.lower(): v for k, v in self.column_map.items()}
        self.default_table = default_table
        self.rel_from_to = rel_from_to
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
            return "TRUE" if node.this else "FALSE"
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
            a = self.emit(node.this)
            b = self.emit(node.expression)
            return f"IF({a} = {b}, BLANK(), {a})"
        if isinstance(node, exp.Coalesce):
            args = [self.emit(node.this)] + [
                self.emit(e) for e in (node.expressions or [])
            ]
            result = args[-1]
            for a in reversed(args[:-1]):
                result = f"IF(ISBLANK({a}), {result}, {a})"
            return result
        if isinstance(node, exp.Cast):
            # DAX has no general CAST; emit the value untouched.
            return self.emit(node.this)
        if isinstance(node, exp.Anonymous):
            return self._emit_anonymous(node)

        # Fallback: defer to sqlglot.
        return node.sql(dialect="spark")

    # ---------- columns ----------

    def _emit_column(self, node: "exp.Column") -> str:
        tbl = node.table or None
        name = node.name
        dax = self.lookup_column(tbl, name)
        if dax is None:
            # Unresolved — emit as bare or qualified identifier.
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
            return f"DISTINCTCOUNT({self.emit(inner)})"
        if isinstance(arg, exp.Star):
            return f"COUNTROWS('{self.default_table}')"
        return f"COUNT({self.emit(arg)})"

    def _count_distinct_case(self, case_node: "exp.Case") -> Optional[str]:
        """Translate ``COUNT(DISTINCT CASE WHEN cond THEN col END)``."""
        ifs = case_node.args.get("ifs") or []
        default = case_node.args.get("default")
        if len(ifs) == 1 and default is None:
            first = ifs[0]
            cond = self.emit(first.this)
            then = self.emit(first.args.get("true"))
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
        if spec is None and order is None and partition is None:
            return f"CALCULATE({body_dax}, ALL('{self.default_table}'))"
        # Unsupported window shape — fall back to the body without the window.
        return body_dax

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
            a = self.emit(args[0])
            b = self.emit(args[1])
            return f"IF({a} = {b}, BLANK(), {a})"
        rendered = ", ".join(self.emit(a) for a in args)
        return f"{name}({rendered})"

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
