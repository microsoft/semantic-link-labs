"""Tests for `sempy_labs.dax._analysis.find_numeric_aggregation_columns`.

These tests exercise the parser-driven detector that
`TOMWrapper.find_non_numeric_aggregations` is built on. They do not
require a TOM connection.
"""

from sempy_labs.dax import find_numeric_aggregation_columns


def _findings(expr):
    return list(find_numeric_aggregation_columns(expr))


def test_sum_first_arg_column():
    # SUM(<column>) -- the column is the first arg
    assert _findings("SUM ( Sales[Quantity] )") == [
        ("SUM", "Sales", "Quantity"),
    ]


def test_average_first_arg_column():
    assert _findings("AVERAGE ( 'Sales'[Net Price] )") == [
        ("AVERAGE", "Sales", "Net Price"),
    ]


def test_sumx_scans_expression_arg():
    # SUMX(<table>, <expression>) -- column(s) live in the second arg
    found = _findings("SUMX ( Sales, Sales[Quantity] * Sales[Net Price] )")
    assert ("SUMX", "Sales", "Quantity") in found
    assert ("SUMX", "Sales", "Net Price") in found
    assert len(found) == 2


def test_averagex_scans_expression_arg():
    found = _findings(
        "AVERAGEX ( 'Date', 'Date'[Year] - 'Date'[StartYear] )"
    )
    assert ("AVERAGEX", "Date", "Year") in found
    assert ("AVERAGEX", "Date", "StartYear") in found


def test_nested_inside_calculate():
    # find_all walks the whole tree, so nesting under CALCULATE is fine
    found = _findings("CALCULATE ( SUM ( Sales[Amount] ) )")
    assert found == [("SUM", "Sales", "Amount")]


def test_non_aggregation_functions_ignored():
    # DIVIDE / IF aren't numeric aggregations -> no findings
    assert _findings("DIVIDE ( Sales[A], Sales[B] )") == []
    assert _findings("IF ( Sales[A] > 0, 1, 0 )") == []


def test_pure_measure_reference_yields_nothing():
    # [Total Sales] is a measure ref, not a column ref, so nothing to flag
    assert _findings("SUMX ( Sales, [Total Sales] )") == []


def test_unparseable_expression_returns_empty():
    # Should not raise -- just return nothing.
    assert _findings("THIS IS NOT VALID DAX ((") == []


def test_empty_or_none_expression():
    assert _findings("") == []
    assert _findings(None) == []


def test_multiple_aggregations_in_one_expression():
    found = _findings(
        "SUM ( Sales[Amount] ) + AVERAGE ( Sales[Discount] )"
    )
    assert ("SUM", "Sales", "Amount") in found
    assert ("AVERAGE", "Sales", "Discount") in found
    assert len(found) == 2
