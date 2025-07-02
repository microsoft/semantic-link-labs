from ._generate_shared_expression import generate_shared_expression
from .._helper_functions import (
    resolve_dataset_name_and_id,
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    resolve_lakehouse_name_and_id,
)
from sempy._utils._log import log
from ..tom import connect_semantic_model
from typing import Optional, List
import sempy_labs._icons as icons
from uuid import UUID
import re


@log
def _extract_expression_list(expression):
    """
    Finds the pattern for DL/SQL & DL/OL expressions in the semantic model.
    """

    pattern_sql = r'Sql\.Database\s*\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)'
    pattern_no_sql = (
        r'AzureStorage\.DataLake\(".*?/([0-9a-fA-F\-]{36})/([0-9a-fA-F\-]{36})"'
    )

    match_sql = re.search(pattern_sql, expression)
    match_no_sql = re.search(pattern_no_sql, expression)

    result = []
    if match_sql:
        value_1, value_2 = match_sql.groups()
        result = [value_1, value_2, True]
    elif match_no_sql:
        value_1, value_2 = match_no_sql.groups()
        result = [value_1, value_2, False]

    return result


@log
def _get_direct_lake_expressions(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> dict:
    """
    Extracts a dictionary of all Direct Lake expressions from a semantic model.
    """

    from sempy_labs.tom import connect_semantic_model

    result = {}

    with connect_semantic_model(dataset=dataset, workspace=workspace) as tom:
        for e in tom.model.Expressions:
            expr_name = e.Name
            expr = e.Expression

            list_values = _extract_expression_list(expr)
            if list_values:
                result[expr_name] = list_values

    return result


@log
def update_direct_lake_model_lakehouse_connection(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    lakehouse: Optional[str] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
):
    """
    Remaps a Direct Lake semantic model's SQL Endpoint connection to a new lakehouse.

    Parameters
    ----------
    dataset : str | UUID
        Name or ID of the semantic model.
    workspace : str | UUID, default=None
        The Fabric workspace name or ID in which the semantic model exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    lakehouse : str, default=None
        The Fabric lakehouse used by the Direct Lake semantic model.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str | UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    update_direct_lake_model_connection(
        dataset=dataset,
        workspace=workspace,
        source=lakehouse,
        source_type="Lakehouse",
        source_workspace=lakehouse_workspace,
    )


@log
def update_direct_lake_model_connection(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    source: Optional[str] = None,
    source_type: str = "Lakehouse",
    source_workspace: Optional[str | UUID] = None,
    use_sql_endpoint: bool = True,
    tables: Optional[str | List[str]] = None,
):
    """
    Remaps a Direct Lake semantic model's SQL Endpoint connection to a new lakehouse/warehouse.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the semantic model exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    source : str, default=None
        The name of the Fabric lakehouse/warehouse used by the Direct Lake semantic model.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    source_type : str, default="Lakehouse"
        The type of source for the Direct Lake semantic model. Valid options: "Lakehouse", "Warehouse".
    source_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse/warehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    use_sql_endpoint : bool, default=True
        If True, the SQL Endpoint will be used for the connection.
        If False, Direct Lake over OneLake will be used.
    tables : str | List[str], default=None
        The name(s) of the table(s) to update in the Direct Lake semantic model.
        If None, all tables will be updated (if there is only one expression).
        If multiple tables are specified, they must be provided as a list.
    """
    if use_sql_endpoint:
        icons.sll_tags.append("UpdateDLConnection_SQL")
    else:
        icons.sll_tags.append("UpdateDLConnection_DLOL")

    if isinstance(tables, str):
        tables = [tables]

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    source_type = source_type.capitalize()

    if source_type not in ["Lakehouse", "Warehouse"]:
        raise ValueError(
            f"{icons.red_dot} The 'source_type' must be either 'Lakehouse' or 'Warehouse'."
        )

    if source_workspace is None:
        source_workspace = workspace_name

    if source_type == "Lakehouse":
        (source_name, source_id) = resolve_lakehouse_name_and_id(
            lakehouse=source, workspace=source_workspace
        )
    else:
        (source_name, source_id) = resolve_item_name_and_id(
            item=source, type=source_type, workspace=source_workspace
        )

    shared_expression = generate_shared_expression(
        item_name=source_name,
        item_type=source_type,
        workspace=source_workspace,
        use_sql_endpoint=use_sql_endpoint,
    )

    expression_dict = _get_direct_lake_expressions(dataset=dataset, workspace=workspace)
    expressions = list(expression_dict.keys())

    with connect_semantic_model(
        dataset=dataset_id, readonly=False, workspace=workspace_id
    ) as tom:

        if not tom.is_direct_lake():
            raise ValueError(
                f"{icons.red_dot} The '{dataset_name}' semantic model within the '{workspace_name}' workspace is not in Direct Lake. This function is only applicable to Direct Lake semantic models."
            )

        # Update the single connection expression
        if len(expressions) > 1 and not tables:
            print(
                f"{icons.info} Multiple expressions found in the model. Please specify the tables to update using the 'tables parameter."
            )
            return
        elif len(expressions) == 1 and not tables:
            expr = expressions[0]
            tom.model.Expressions[expr].Expression = shared_expression

            print(
                f"{icons.green_dot} The expression in the '{dataset_name}' semantic model within the '{workspace_name}' workspace has been updated to point to the '{source}' {source_type.lower()} in the '{source_workspace}' workspace."
            )
        else:
            import sempy

            sempy.fabric._client._utils._init_analysis_services()
            import Microsoft.AnalysisServices.Tabular as TOM

            expr_list = _extract_expression_list(shared_expression)

            expr_name = next(
                (name for name, exp in expression_dict.items() if exp == expr_list),
                None,
            )

            # If the expression does not already exist, create it
            def generate_unique_name(existing_names):
                i = 1
                while True:
                    candidate = f"DatabaseQuery{i}"
                    if candidate not in existing_names:
                        return candidate
                    i += 1

            if not expr_name:
                expr_name = generate_unique_name(expressions)
                tom.add_expression(name=expr_name, expression=shared_expression)

            all_tables = [t.Name for t in tom.model.Tables]
            for t_name in tables:
                if t_name not in all_tables:
                    raise ValueError(
                        f"{icons.red_dot} The table '{t_name}' does not exist in the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
                    )
                p = next(p for p in tom.model.Tables[t_name].Partitions)
                if p.Mode != TOM.ModeType.DirectLake:
                    raise ValueError(
                        f"{icons.red_dot} The table '{t_name}' in the '{dataset_name}' semantic model within the '{workspace_name}' workspace is not in Direct Lake mode. This function is only applicable to Direct Lake tables."
                    )

                p.Source.ExpressionSource = tom.model.Expressions[expr_name]
