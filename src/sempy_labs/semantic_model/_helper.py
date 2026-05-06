import re


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
    sql: str, column_map: dict[str, str], default_table: str = "summary"
) -> str:
    dax = sql.strip()

    # =========================================================
    # 1. STRING PROTECTION (CRITICAL - MUST BE FIRST)
    # =========================================================
    def protect_strings(text):
        strings = {}

        def repl(m):
            key = f"__str{len(strings)}__"
            strings[key] = m.group(0)
            return key

        return re.sub(r"'[^']*'", repl, text), strings

    def restore_strings(text, strings):
        for k, v in strings.items():
            text = text.replace(k, '"' + v.strip("'") + '"')
        return text

    dax, strings = protect_strings(dax)

    # =========================================================
    # 2. MEASURE → COLUMN
    # =========================================================
    dax = re.sub(
        r"\bMEASURE\(`([^`]+)`\)",
        r"[\1]",
        dax,
        flags=re.IGNORECASE,
    )

    # =========================================================
    # 3. COUNT(*)
    # =========================================================
    dax = re.sub(
        r"\bCOUNT\s*\(\s*\*\s*\)",
        f"COUNTROWS('{default_table}')",
        dax,
        flags=re.IGNORECASE,
    )

    # =========================================================
    # 4. COUNT DISTINCT
    # =========================================================
    dax = re.sub(
        r"\bCOUNT\s*\(\s*DISTINCT\s+([^)]+)\)",
        r"DISTINCTCOUNT(\1)",
        dax,
        flags=re.IGNORECASE,
    )

    # =========================================================
    # 5. AGGREGATIONS
    # =========================================================
    for func in ["SUM", "AVG", "MAX", "MIN"]:
        dax = re.sub(
            rf"\b{func}\s*\(([^)]+)\)",
            lambda m: f"{'AVERAGE' if func=='AVG' else func}({m.group(1)})",
            dax,
            flags=re.IGNORECASE,
        )

    # =========================================================
    # 6. FILTER (WHERE → CALCULATE)
    # =========================================================
    def handle_filter(match):
        expr = match.group(1)
        condition = match.group(2)
        return f"CALCULATE({expr}, {condition})"

    dax = re.sub(
        r"(.+?)\s+FILTER\s*\(\s*WHERE\s+(.+?)\)",
        handle_filter,
        dax,
        flags=re.IGNORECASE,
    )

    # =========================================================
    # 7. IN (...) → IN {...}
    # =========================================================
    dax = re.sub(
        r"\bIN\s*\(([^)]+)\)",
        lambda m: f"IN {{{m.group(1)}}}",
        dax,
        flags=re.IGNORECASE,
    )

    # =========================================================
    # 8. SAFE DIVISION (CORE FIX)
    # =========================================================
    def handle_division(text):

        def repl(m):
            left = m.group(1).strip()
            right = m.group(2).strip()

            # NULLIF(x,0) → x
            right = re.sub(
                r"NULLIF\(([^,]+),\s*0\)",
                r"\1",
                right,
                flags=re.IGNORECASE,
            )

            return f"DIVIDE({left}, {right})"

        # only split top-level expressions
        pattern = r"(.+?)\s*/\s*(.+)"
        return re.sub(pattern, repl, text)

    dax = handle_division(dax)

    # =========================================================
    # 9. WINDOW FUNCTIONS
    # =========================================================
    dax = re.sub(
        r"(MAX\([^)]+\))\s+OVER\(\)",
        lambda m: f"CALCULATE({m.group(1)}, ALL('{default_table}'))",
        dax,
        flags=re.IGNORECASE,
    )

    # =========================================================
    # 10. RESTORE STRINGS (AFTER ALL LOGIC)
    # =========================================================
    dax = restore_strings(dax, strings)

    # =========================================================
    # 11. TABLE QUOTE FIXES
    # =========================================================
    dax = re.sub(r'COUNTROWS\("([^"]+)"\)', r"COUNTROWS('\1')", dax)
    dax = re.sub(r'ALL\("([^"]+)"\)', r"ALL('\1')", dax)

    # =========================================================
    # 12. COLUMN REPLACEMENT
    # =========================================================
    def replace_columns(text):
        refs = {}

        def protect(m):
            key = f"__col{len(refs)}__"
            refs[key] = m.group(0)
            return key

        text = re.sub(r"'[^']+'\[[^\]]+\]", protect, text)

        for col in sorted(column_map.keys(), key=len, reverse=True):
            pattern = rf"(?<!\[)(?<![A-Za-z0-9_`])`?{re.escape(col)}`?(?![A-Za-z0-9_])"
            text = re.sub(pattern, column_map[col], text)

        for k, v in refs.items():
            text = text.replace(k, v)

        return text

    dax = replace_columns(dax)

    return dax


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
