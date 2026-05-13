from uuid import UUID
from typing import Literal, Optional, List
import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    create_abfss_path,
    resolve_item_id,
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    retry,
    list_columns_from_path,
)
from sempy_labs.semantic_model._helper import (
    convert_column_data_type,
)
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs.directlake._generate_shared_expression import (
    generate_shared_expression,
)
from sempy_labs._generate_semantic_model import create_blank_semantic_model
from sempy_labs._refresh_semantic_model import refresh_semantic_model


@log
def generate_direct_lake_semantic_model(
    dataset: str,
    tables: List[str] | dict,
    source: str | UUID,
    source_type: Literal[
        "Lakehouse",
        "Warehouse",
        "SQLDatabase",
        "MirroredAzureDatabricksCatalog",
        "MirroredDatabase",
    ] = "Lakehouse",
    source_workspace: Optional[str | UUID] = None,
    use_sql_endpoint: bool = False,
    workspace: Optional[str | UUID] = None,
    refresh: bool = True,
):
    """
    Dynamically generates a Direct Lake semantic model based on tables in Fabric.

    Parameters
    ----------
    dataset : str
        Name of the semantic model to be created.
    tables : typing.List[str] | dict
        List or dictionary of tables to include in the semantic model. Table names should be schema-qualified if there are multiple tables with the same name across schemas (e.g. "schema1.tableA", "schema2.tableA", "schema1.tableB").

        Example 1:
            tables = ["dbo.sales", "dbo.geography"]
        Example 2:
            tables = {
                "Sales": "dbo.sales",
                "Geography": "dbo.geography",
            }
    source : str | uuid.UUID
        The source item name or ID from which to generate the semantic model. This can be a Lakehouse, Mirrored Azure Databricks Catalog, Warehouse, SQL Database, or Mirrored Database.
    source_type : typing.Literal["Lakehouse", "Warehouse", "SQLDatabase", "MirroredAzureDatabricksCatalog", "MirroredDatabase"], default = "Lakehouse"
        The type of the source item. Supported values are "Lakehouse", "Warehouse", "SQLDatabase", "MirroredAzureDatabricksCatalog", "MirroredDatabase".
    source_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the source item resides. This parameter is required if the source item is not in the same workspace as the semantic model will be created, and is ignored if the source item is in the same workspace. Defaults to None which resolves to the workspace of the attached lakehouse or if no lakehouse attached, resolves to the workspace of the notebook.
    use_sql_endpoint : bool, default=False
        If True, the generated expression will use the SQL endpoint. If False, the generated expression will use Direct Lake over OneLake.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the semantic model will reside.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    refresh: bool, default=True
        If True, refreshes the newly created semantic model after it is created.
    """

    # Convert to dictionary
    if isinstance(tables, str):
        tables = [tables]
    if isinstance(tables, list):
        if len(tables) != len({t.split(".", 1)[-1] for t in tables}):
            raise ValueError(
                f"{icons.red_dot} Duplicate table names are not allowed in the 'tables' parameter when considering schema-qualified names."
            )
        tables = {t.split(".", 1)[-1]: t for t in tables}

    if len(tables.keys()) != tables:
        raise ValueError(
            f"{icons.red_dot} Duplicate table names are not allowed in the 'tables' parameter."
        )

    supported_sources = [
        "Lakehouse",
        "MirroredAzureDatabricksCatalog",
        "Warehouse",
        "SQLDatabase",
        "MirroredDatabase",
    ]

    if source is None:
        raise ValueError(f"{icons.red_dot} The 'source' parameter must be provided.")

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    source_workspace_id = resolve_workspace_id(source_workspace)
    source_id = resolve_item_id(
        item=source, type=source_type, workspace=source_workspace_id
    )
    if source_type not in supported_sources:
        raise ValueError(
            f"{icons.red_dot} Unsupported source type '{source_type}'. Supported source types are: {supported_sources}."
        )

    expr = generate_shared_expression(
        item=source_id,
        item_type=source_type,
        workspace=source_workspace_id,
        use_sql_endpoint=use_sql_endpoint,
    )

    # Populate model map with table and column information
    model_map = {}
    for table_name, table_source_name in tables.items():
        # Handle schema + table
        if "." in table_source_name:
            schema_name, entity_name = table_source_name.split(".", 1)
        else:
            schema_name = None
            entity_name = table_source_name

        path = create_abfss_path(
            lakehouse_id=source_id,
            lakehouse_workspace_id=source_workspace_id,
            delta_table_name=entity_name,
            schema=schema_name,
        )

        df = list_columns_from_path(path=path)

        if df.empty:
            raise ValueError(
                f"{icons.red_dot} The table '{table_source_name}' does not exist in the source or has no columns."
            )

        # Initialize table entry
        model_map[table_name] = {
            "schema": schema_name,
            "entityName": entity_name,
            "columns": [],
        }

        # Add columns
        for _, row in df.iterrows():
            column_name = row["Column Name"]
            data_type = row["Data Type"]
            converted_data_type = convert_column_data_type(data_type)
            if converted_data_type is None:
                raise ValueError(
                    f"{icons.red_dot} The data type '{data_type}' of column '{column_name}' in table '{table_source_name}' is not supported."
                )
            if converted_data_type == "Binary":
                print(
                    f"{icons.warning} The column '{column_name}' in table '{table_source_name}' has data type 'Binary' which is not supported in Direct Lake semantic models. This column will be skipped."
                )
                continue
            model_map[table_name]["columns"].append(
                {"columnName": column_name, "dataType": converted_data_type}
            )
    if not model_map:
        raise ValueError(
            f"{icons.red_dot} No valid tables were provided given the source provided."
        )

    dfD = fabric.list_datasets(workspace=workspace_id, mode="rest")
    dfD_filt = dfD[dfD["Dataset Name"] == dataset]

    if dfD_filt.empty:
        dataset_id = create_blank_semantic_model(
            dataset=dataset,
            workspace=workspace_id,
        )
    else:
        raise ValueError(
            f"{icons.red_dot} A dataset with the name '{dataset}' already exists in the workspace '{workspace_name}'. Please choose a different name or delete the existing dataset."
        )

    # Imported lazily to avoid a circular import: sempy_labs.tom._model imports
    # from sempy_labs.semantic_model._helper, which triggers this module.
    from sempy_labs.tom import connect_semantic_model

    @retry(
        sleep_time=1,
        timeout_error_message=f"{icons.red_dot} Function timed out after 1 minute",
    )
    def dyn_connect():
        with connect_semantic_model(
            dataset=dataset_id, readonly=True, workspace=workspace_id
        ) as tom:

            tom.model

    dyn_connect()

    with connect_semantic_model(
        dataset=dataset_id, workspace=workspace_id, readonly=False
    ) as tom:

        expression_name = "DatabaseQuery" if use_sql_endpoint else f"DL_{source_type}"
        tom.add_expression(name=expression_name, expression=expr)

        for table_name, table_info in model_map.items():
            schema_name = table_info["schema"]
            entity_name = table_info["entityName"]
            tom.add_table(name=table_name)
            tom.add_entity_partition(
                table_name=table_name,
                entity_name=entity_name,
                expression=expression_name,
                schema_name=schema_name,
            )
            for column in table_info["columns"]:
                column_name = column["columnName"]
                data_type = column["dataType"]
                tom.add_data_column(
                    table_name=table_name,
                    column_name=column_name,
                    data_type=data_type,
                    source_column=column_name,
                )

    if refresh:
        refresh_semantic_model(dataset=dataset_id, workspace=workspace_id)

    return dataset_id
