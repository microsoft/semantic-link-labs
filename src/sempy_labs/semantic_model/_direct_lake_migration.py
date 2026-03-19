import sempy
from typing import Optional, Literal
from uuid import UUID
from sempy_labs.tom import connect_semantic_model
from sempy_labs._helper_functions import (
    resolve_lakehouse_id,
    resolve_item_id,
    resolve_workspace_id,
)
from sempy_labs.directlake._generate_shared_expression import generate_shared_expression
from sempy_labs.lakehouse._schemas import is_schema_enabled
from sempy_labs._model_dependencies import get_model_dependencies

# class DirectLakeMigration:

#    def __init__(
#        self,
#        dataset,
#        workspace,
#    )
#        self.dataset = dataset
#        self.workspace = workspace

#    def identify_issues(self)


def migrate(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    source: Optional[str | UUID] = None,
    source_type: Literal["Lakehouse", "Warehouse"] = "Lakehouse",
    source_workspace: Optional[str | UUID] = None,
    source_schema: Optional[str] = None,
    use_sql_endpoint: bool = False,
):

    import Microsoft.AnalysisServices.Tabluar as TOM

    expression_name = "DatabaseQuery"
    sql_endpoint_sources = ['Lakehouse', 'Warehouse']
    dl_source_list = ['Lakehouse', 'Warehouse', 'MirroredDatabase', 'SQLDatabase', 'MirroredAzureDatabricksCatalog']

    # Get dependencies
    dep = get_model_dependencies(dataset=dataset, workspace=workspace)

    # SQL Endpoint only valid for Lakehouse and Warehouse sources
    if use_sql_endpoint and source_type not in sql_endpoint_sources:
        raise ValueError()

    # Resolve source
    source_workspace_id = resolve_workspace_id(source_workspace)
    if source_type == "Lakehouse":
        source_id = resolve_lakehouse_id(source, source_workspace_id)
        has_schema = is_schema_enabled(source_id, source_workspace_id)
        if has_schema and source_schema is None:
            raise ValueError(
                "Must enter a 'source_schema' if using a schema-enabled lakehouse."
            )
    else:
        source_id = resolve_item_id(
            item=source, type=source_type, workspace=source_workspace_id
        )

    # Have one for your review, Bo: https://github.com/microsoft/semantic-link-labs/pull/1114
    with connect_semantic_model(dataset=dataset, workspace=workspace) as tom:

        # Collect unsuppported objects and display it in the UI
        calc_tables = [
            t.Name for t in tom.model.Tables if tom.is_calculated_table(t.Name)
        ]
        calc_columns = [
            (c.Parent.Name, c.Name)
            for c in tom.model.all_columns()
            if tom.is_calculated_column(c.Parent.Name, c.Name)
        ]
        binary_columns = [
            (c.Parent.Name, c.Name)
            for c in tom.model.all_columns()
            if c.DataType == TOM.DataType.Binary
        ]
        non_matching_column_relationships = [
            (r.FromTable.Name, r.FromColumn.Name, r.ToTable.Name, r.ToColumn.Name)
            for r in tom.model.Relationships
            if r.FromColumn.DataType != r.ToColumn.DataType
        ]
        multi_partitioned_tables = [
            t.Name for t in tom.model.Tables if t.Partitions.Count > 1
        ]
        aggs_used = any(c for c in tom.all_columns() if c.AlternateOf is not None)

        # Collect depencency issues
        broken_measures = []
        broken_relationships = []
        broken_row_level_security = []

        # Create expression
        expr = generate_shared_expression(
            item_name=source_id,
            item_type=source_type,
            workspace=source_workspace_id,
            use_sql_endpoint=use_sql_endpoint,
        )

        # Initiate model changes
        tom.add_expression(name=expression_name, expression=expr)

        calc_table_list = []
        for t in tom.model.Tables:
            entity_name = t.Name.replace(" ", "_")
            if t.CalculationGroup is not None:
                pass
            elif tom.is_calculated_table(t.Name):
                if not tom.is_field_parameter(t.Name):
                    source_expression = next(p.Source.Expression for p in t.Partitions)
                    calc_table_list.append(
                        {"TableName": t.Name, "Expression": source_expression}
                    )
            else:
                # Create DL partition and remove old partition
                p = next(p for p in t.Partitions)
                tom.add_entity_partition(
                    table_name=t.Name,
                    entity_name=entity_name,
                    expression=None,
                    schema_name=source_schema,
                )
                tom.remove_object(object=p)

            # Collapse partitions
            if t.Partitions.Count > 1:
                first_partition = next(p for p in t.Partitions)
                for p in t.Partitions:
                    if p != first_partition:
                        tom.remove_object(p)

        for c in tom.all_columns():
            if tom.is_calculated_column(table_name=c.Parent.Name, column_name=c.Name):
                tom.remove_object(c)
            elif tom.DataType == TOM.DataType.Binary:
                tom.remove_object(c)
            elif c.AlternateOf is not None:
                # Remove Aggs
                c.AlternateOf = None

        for r in tom.model.Relationships:
            if r.FromColumn.DataType != r.ToColumn.DataType:
                tom.remove_object(r)

        # Create calc tables
