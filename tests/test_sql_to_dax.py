import pytest

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


def test_lpad_is_rewritten():
    dax = _convert("LPAD(C_BIRTH_MONTH, 2, '0')", default_table="CUSTOMER")

    assert dax == "RIGHT(REPT(\"0\", 2) & 'CUSTOMER'[C_BIRTH_MONTH], 2)"


def test_unknown_function_returns_blank():
    dax = _convert("REGEXP_COUNT(C_LAST_NAME, 'a')", default_table="CUSTOMER")

    assert dax == "BLANK()"


def test_unresolved_column_returns_blank():
    assert _convert("UPPER(NOT_A_COLUMN)", default_table="CUSTOMER") == "BLANK()"


def test_unparseable_sql_returns_blank():
    assert _convert("SELECT FROM WHERE (((", default_table="CUSTOMER") == "BLANK()"


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
