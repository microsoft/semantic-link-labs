"""Static analysis helpers over DAX expressions.

These helpers operate purely on the parsed AST and do not require a TOM
connection, which makes them straightforward to unit-test.
"""

from typing import Iterator, List, Tuple, Union

from ._expressions import Column, Function
from ._parser import parse_dax


# Functions that require their target column(s) to be numeric.
# Value is the argument index that contains the column to aggregate:
#   0 -> the first arg is the column itself (e.g. SUM(<column>))
#   1 -> the second arg is an expression that should be scanned for any
#        Column references (e.g. SUMX(<table>, <expression>))
NUMERIC_AGGREGATIONS = {
    "SUM": 0,
    "AVERAGE": 0,
    "MIN": 0,
    "MAX": 0,
    "PRODUCT": 0,
    "SUMX": 1,
    "AVERAGEX": 1,
    "MINX": 1,
    "MAXX": 1,
    "PRODUCTX": 1,
}


def find_numeric_aggregation_columns(
    expression: str,
) -> Iterator[Tuple[str, str, str]]:
    """
    Walk a DAX expression and yield every column referenced by a numeric
    aggregation function.

    Yields
    ------
    (function_name, table_name, column_name)
        One tuple per column reference found inside a numeric-aggregation
        function call. The function name is upper-cased.

    Notes
    -----
    Silently yields nothing if the expression cannot be parsed.
    """

    if not expression:
        return

    try:
        tree = parse_dax(expression)
    except SyntaxError:
        return

    for fn in tree.find_all(Function):

        fn_name = fn.args.get("this", "").upper()

        if fn_name not in NUMERIC_AGGREGATIONS:
            continue

        idx = NUMERIC_AGGREGATIONS[fn_name]
        args = fn.args.get("expressions", [])

        if idx >= len(args):
            continue

        target = args[idx]

        # For SUM/AVERAGE/etc. the first arg IS the column; for the *X
        # variants the column lives somewhere inside the expression arg.
        if isinstance(target, Column):
            columns = [target]
        else:
            columns = list(target.find_all(Column))

        for col in columns:
            yield fn_name, col.args["table"], col.args["this"]


def uses_function(
    expression: str, function_name: Union[str, List[str]]
) -> bool:
    """
    Returns True if the DAX expression contains a call to any of the given
    function(s) (case-insensitive). Built on the DAX parser so it ignores
    occurrences inside string literals or comments that a regex would
    falsely match.

    Parameters
    ----------
    expression : str
        The DAX expression to analyze.
    function_name : str | list[str]
        A single DAX function name (e.g. ``"IFERROR"``) or a list of names
        (e.g. ``["IFERROR", "ERROR"]``). Matching is case-insensitive.

    Returns
    -------
    bool
        True if any of the named functions is called in ``expression``.
        False if the expression is empty or cannot be parsed.
    """

    if not expression:
        return False

    try:
        tree = parse_dax(expression)
    except SyntaxError:
        return False

    if isinstance(function_name, str):
        targets = {function_name.upper()}
    else:
        targets = {name.upper() for name in function_name}

    for fn in tree.find_all(Function):
        if fn.args.get("this", "").upper() in targets:
            return True

    return False


def find_non_numeric_aggregations(
    expression: str, tom
) -> Iterator[Tuple[str, str, str, str]]:
    """
    Yield every column reference inside a numeric-aggregation function
    (SUM, SUMX, AVERAGE, AVERAGEX, MIN, MINX, MAX, MAXX, PRODUCT, PRODUCTX)
    whose corresponding TOM column is **not** numeric (Int64/Decimal/Double).

    Parameters
    ----------
    expression : str
        The DAX expression to analyze.
    tom : TOMWrapper
        Used to resolve table/column data types. Columns that cannot be
        resolved against the model are silently skipped.

    Yields
    ------
    (function_name, table_name, column_name, data_type)
        One tuple per offending reference. ``data_type`` is the string
        form of the TOM ``DataType`` enum.

    Notes
    -----
    Designed to be called inline from a BPA rule lambda::

        lambda obj, tom: any(
            find_non_numeric_aggregations(obj.Expression, tom)
        )
    """

    import Microsoft.AnalysisServices.Tabular as TOM

    numeric_types = {
        TOM.DataType.Int64,
        TOM.DataType.Decimal,
        TOM.DataType.Double,
    }

    tables = tom.model.Tables

    for fn_name, table_name, column_name in find_numeric_aggregation_columns(
        expression
    ):
        table = next((t for t in tables if t.Name == table_name), None)
        if table is None:
            continue
        column = next(
            (c for c in table.Columns if c.Name == column_name), None
        )
        if column is None:
            continue
        if column.DataType in numeric_types:
            continue

        yield fn_name, table_name, column_name, str(column.DataType)
