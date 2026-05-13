from collections import defaultdict
from itertools import combinations
from sempy_labs._sql import ConnectMirroredAzureDatabricksCatalog
from sempy._utils._log import log


@log
def infer_model_relationships(column_list, workspace):

    candidates = [c for c in column_list if c.get("name", "").endswith("ID")]

    def find_column_relationships(candidates):
        groups = defaultdict(list)

        for c in candidates:
            groups[(c["name"], c["pbiDataType"])].append(c)

        return [
            (c1, c2)
            for cols in groups.values()
            if len(cols) > 1
            for c1, c2 in combinations(cols, 2)
            if (c1["tableName"], c1["sourceSchema"], c1["sourceCatalog"])
            != (c2["tableName"], c2["sourceSchema"], c2["sourceCatalog"])
        ]

    # ✅ Cache results to avoid repeated DB hits
    cardinality_cache = {}

    def is_unique(source, schema, table, column, workspace):
        key = (source, schema, table, column)

        if key not in cardinality_cache:
            with ConnectMirroredAzureDatabricksCatalog(
                mirrored_azure_databricks_catalog=source, workspace=workspace
            ) as sql:
                df = sql.query(
                    f"""SELECT COUNT(DISTINCT {column}) AS ct_col,
                            COUNT({column}) AS ct_tbl
                        FROM {schema}.{table}"""
                )
            ct_col, ct_tbl = df.iloc[0]
            cardinality_cache[key] = ct_col == ct_tbl

        return cardinality_cache[key]

    # ✅ Build relationships
    relationships = []

    for left, right in find_column_relationships(candidates):

        left_key = (
            left["sourceCatalog"],
            left["sourceSchema"],
            left["tableName"],
            left["name"],
        )
        right_key = (
            right["sourceCatalog"],
            right["sourceSchema"],
            right["tableName"],
            right["name"],
        )

        # Check uniqueness (dimension side)
        if is_unique(*left_key, workspace=workspace):
            relationships.append(
                {
                    "fromTable": right["tableName"],
                    "fromColumn": right["name"],
                    "toTable": left["tableName"],
                    "toColumn": left["name"],
                }
            )
        elif is_unique(*right_key, workspace=workspace):
            relationships.append(
                {
                    "fromTable": left["tableName"],
                    "fromColumn": left["name"],
                    "toTable": right["tableName"],
                    "toColumn": right["name"],
                }
            )

    return relationships
