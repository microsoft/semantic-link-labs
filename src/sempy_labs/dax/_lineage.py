from ._expressions import *


def extract_lineage(tree):

    results = {
        "functions": set(),
        "columns": set(),
        "measures": set(),
    }

    for node in tree.walk():

        if isinstance(node, Function):
            results["functions"].add(node.args["this"])

        elif isinstance(node, Column):
            results["columns"].add(
                (
                    node.args["table"],
                    node.args["this"],
                )
            )

        elif isinstance(node, Measure):
            results["measures"].add(node.args["this"])

    return {k: sorted(v) for k, v in results.items()}