import pytest

from sempy_labs.semantic_model._convert import _unconverted_reason
from sempy_labs.semantic_model._helper import convert_sql_to_dax

COLUMN_MAP = {
    "DATE_DIM.D_DATE": "'DATE_DIM'[D_DATE]",
    "D_DATE": "'DATE_DIM'[D_DATE]",
    "DATE_DIM.D_YEAR": "'DATE_DIM'[D_YEAR]",
    "D_YEAR": "'DATE_DIM'[D_YEAR]",
    "CUSTOMER.C_EMAIL_ADDRESS": "'CUSTOMER'[C_EMAIL_ADDRESS]",
    "C_EMAIL_ADDRESS": "'CUSTOMER'[C_EMAIL_ADDRESS]",
    "C_FIRST_NAME": "'CUSTOMER'[C_FIRST_NAME]",
    "C_LAST_NAME": "'CUSTOMER'[C_LAST_NAME]",
    "C_BIRTH_MONTH": "'CUSTOMER'[C_BIRTH_MONTH]",
}


def _convert(sql: str, default_table: str = "DATE_DIM") -> str:
    return convert_sql_to_dax(sql, column_map=COLUMN_MAP, default_table=default_table)


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("TO_CHAR(D_DATE, 'YYYY-MM')", "FORMAT('DATE_DIM'[D_DATE], \"yyyy-MM\")"),
        ("TO_VARCHAR(D_YEAR)", "CONVERT('DATE_DIM'[D_YEAR], STRING)"),
        ("MONTHNAME(D_DATE)", "FORMAT('DATE_DIM'[D_DATE], \"MMM\")"),
        ("CURRENT_DATE()", "TODAY()"),
        (
            "DATE_TRUNC('MONTH', D_DATE)",
            "DATE(YEAR('DATE_DIM'[D_DATE]), MONTH('DATE_DIM'[D_DATE]), 1)",
        ),
        (
            "DATE_TRUNC('WEEK', D_DATE)",
            "'DATE_DIM'[D_DATE] - WEEKDAY('DATE_DIM'[D_DATE], 3)",
        ),
        ("DATEADD(YEAR, -1, D_DATE)", "EDATE('DATE_DIM'[D_DATE], (-1) * 12)"),
        ("DATEADD(DAY, -364, D_DATE)", "'DATE_DIM'[D_DATE] + (-364)"),
        ("DATEADD(dd, 1, D_DATE)", "'DATE_DIM'[D_DATE] + (1)"),
        ("DATE_ADD(D_DATE, 1)", "'DATE_DIM'[D_DATE] + (1)"),
        ("DATEADD(HOUR, 1, D_DATE)", "'DATE_DIM'[D_DATE] + (1) / 24"),
        ("DATEADD(MINUTE, 1, D_DATE)", "'DATE_DIM'[D_DATE] + (1) / 1440"),
        ("DATEADD(mi, -30, D_DATE)", "'DATE_DIM'[D_DATE] + (-30) / 1440"),
        ("DATEADD(SECOND, 1, D_DATE)", "'DATE_DIM'[D_DATE] + (1) / 86400"),
        ("DATEADD(mm, 2, D_DATE)", "EDATE('DATE_DIM'[D_DATE], 2)"),
        (
            "DATEDIFF(YEAR, D_DATE, CURRENT_DATE())",
            "DATEDIFF('DATE_DIM'[D_DATE], TODAY(), YEAR)",
        ),
        (
            "IFF(D_YEAR = 2024, TRUE, FALSE)",
            "IF('DATE_DIM'[D_YEAR] = 2024, TRUE(), FALSE())",
        ),
    ],
)
def test_date_functions(sql, expected):
    assert _convert(sql) == expected


def test_string_concatenation_uses_ampersand():
    dax = _convert("C_FIRST_NAME || ' ' || C_LAST_NAME", default_table="CUSTOMER")

    assert dax == "'CUSTOMER'[C_FIRST_NAME] & \" \" & 'CUSTOMER'[C_LAST_NAME]"


def test_split_part_uses_pathitem():
    dax = _convert("SPLIT_PART(C_EMAIL_ADDRESS, '@', 2)", default_table="CUSTOMER")

    assert dax == 'PATHITEM(SUBSTITUTE(\'CUSTOMER\'[C_EMAIL_ADDRESS], "@", "|"), 2)'


def test_lpad_pads_and_truncates_to_the_source_prefix():
    dax = _convert("LPAD(C_BIRTH_MONTH, 2, '0')", default_table="CUSTOMER")

    assert dax == (
        "IF(LEN('CUSTOMER'[C_BIRTH_MONTH]) >= 2, LEFT('CUSTOMER'[C_BIRTH_MONTH], 2), "
        "LEFT(REPT(\"0\", 2), 2 - LEN('CUSTOMER'[C_BIRTH_MONTH])) & 'CUSTOMER'[C_BIRTH_MONTH])"
    )


def test_lpad_on_an_expression_uses_a_variable():
    dax = _convert("LPAD(TRIM(C_LAST_NAME), 7, 'xy')", default_table="CUSTOMER")

    assert dax == (
        "(VAR __value1 = TRIM('CUSTOMER'[C_LAST_NAME]) RETURN "
        "IF(LEN(__value1) >= 7, LEFT(__value1, 7), "
        'LEFT(REPT("xy", 7), 7 - LEN(__value1)) & __value1))'
    )


def test_rpad_is_rewritten():
    dax = _convert("RPAD(C_LAST_NAME, 5, 'xy')", default_table="CUSTOMER")

    assert dax == "LEFT('CUSTOMER'[C_LAST_NAME] & REPT(\"xy\", 5), 5)"


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("CEIL(D_YEAR)", "-INT(-('DATE_DIM'[D_YEAR]))"),
        ("FLOOR(D_YEAR)", "INT('DATE_DIM'[D_YEAR])"),
        (
            "CEIL(D_YEAR, 2)",
            "IF('DATE_DIM'[D_YEAR] < 0, ROUNDDOWN('DATE_DIM'[D_YEAR], 2), ROUNDUP('DATE_DIM'[D_YEAR], 2))",
        ),
        (
            "FLOOR(D_YEAR, 2)",
            "IF('DATE_DIM'[D_YEAR] < 0, ROUNDUP('DATE_DIM'[D_YEAR], 2), ROUNDDOWN('DATE_DIM'[D_YEAR], 2))",
        ),
    ],
)
def test_ceil_and_floor_round_toward_infinity(sql, expected):
    assert _convert(sql) == expected


def _assert_unconverted(dax: str):
    assert dax.endswith("\nBLANK()")
    assert dax.startswith("// This SQL expression could not be converted to DAX")
    assert _unconverted_reason(dax) is not None


def test_unknown_function_returns_commented_blank():
    dax = _convert("REGEXP_COUNT(C_LAST_NAME, 'a')", default_table="CUSTOMER")

    _assert_unconverted(dax)
    assert "REGEXP_COUNT" in dax


def test_unresolved_column_returns_commented_blank():
    _assert_unconverted(_convert("UPPER(NOT_A_COLUMN)", default_table="CUSTOMER"))


def test_unparseable_sql_returns_commented_blank():
    _assert_unconverted(_convert("SELECT FROM WHERE (((", default_table="CUSTOMER"))


def test_unsupported_date_part_returns_commented_blank():
    dax = _convert("DATEADD(WEEKDAY, 1, D_DATE)")

    _assert_unconverted(dax)
    assert "WEEKDAY" in dax


def test_null_is_a_legitimate_blank():
    dax = _convert("NULL")

    assert dax == "BLANK()"
    assert _unconverted_reason(dax) is None


def test_bare_identifier_in_scalar_aggregate_is_still_converted():
    dax = _convert("SUM(UNMAPPED_AMOUNT)", default_table="CUSTOMER")

    assert dax == "SUM('CUSTOMER'[UNMAPPED_AMOUNT])"


def test_coalesce_uses_native_dax_function():
    dax = _convert("COALESCE(C_FIRST_NAME, C_LAST_NAME, '')", default_table="CUSTOMER")

    assert dax == "COALESCE('CUSTOMER'[C_FIRST_NAME], 'CUSTOMER'[C_LAST_NAME], \"\")"


def test_nullif_on_a_column_is_inlined():
    dax = _convert("NULLIF(C_LAST_NAME, '')", default_table="CUSTOMER")

    assert dax == "IF('CUSTOMER'[C_LAST_NAME] = \"\", BLANK(), 'CUSTOMER'[C_LAST_NAME])"


def test_nullif_on_an_expression_uses_a_variable():
    dax = _convert("NULLIF(TRIM(C_LAST_NAME), '')", default_table="CUSTOMER")

    assert dax == (
        "(VAR __value1 = TRIM('CUSTOMER'[C_LAST_NAME]) "
        'RETURN IF(__value1 = "", BLANK(), __value1))'
    )


def test_distinct_count_of_an_expression_materializes_values():
    dax = _convert(
        "COUNT(DISTINCT LOWER(C_LAST_NAME))",
        default_table="CUSTOMER",
    )

    assert dax == (
        "COUNTROWS(DISTINCT(SELECTCOLUMNS('CUSTOMER', \"Value\", "
        "LOWER('CUSTOMER'[C_LAST_NAME]))))"
    )


def test_count_of_an_expression_uses_countx():
    dax = _convert("COUNT(UPPER(C_LAST_NAME))", default_table="CUSTOMER")

    assert dax == "COUNTX('CUSTOMER', UPPER('CUSTOMER'[C_LAST_NAME]))"
