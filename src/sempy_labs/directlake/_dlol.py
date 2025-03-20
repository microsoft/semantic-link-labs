import re
from uuid import UUID
from typing import Optional


def _get_direct_lake_expressions(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> dict:

    from sempy_labs.tom import connect_semantic_model

    pattern_sql = r'Sql\.Database\s*\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)'
    pattern_no_sql = r'AzureDataLakeStorage\s*\{\s*"server".*?:\s*onelake\.dfs\.fabric\.microsoft\.com"\s*,\s*"path"\s*:\s*"/([\da-fA-F-]+)\s*/\s*([\da-fA-F-]+)\s*/"\s*\}'
    result = {}

    with connect_semantic_model(dataset=dataset, workspace=workspace) as tom:
        for e in tom.model.Expressions:
            expr_name = e.Name
            expr = e.Expression
            match_sql = re.search(pattern_sql, expr)
            match_no_sql = re.search(pattern_no_sql, expr)
            sql = True

            if match_sql:
                value_1, value_2 = match_sql.groups()
                result[expr_name] = [value_1, value_2, sql]
            elif match_no_sql:
                value_1, value_2 = match_no_sql.groups()
                sql = False
                result[expr_name] = [value_1, value_2, sql]

    return result
