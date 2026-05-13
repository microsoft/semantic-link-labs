"""Static analysis helpers over DAX expressions.

These helpers operate purely on the parsed AST and do not require a TOM
connection, which makes them straightforward to unit-test.
"""

from typing import Iterator, Tuple

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
