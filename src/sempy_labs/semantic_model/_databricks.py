import pandas as pd
from typing import List, Optional
from uuid import UUID
import time
from sempy_labs._helper_functions import (
    _is_valid_uuid,
    resolve_workspace_name_and_id,
    resolve_item_id,
)
from sempy_labs.tom import connect_semantic_model
from sempy_labs.directlake._generate_shared_expression import (
    generate_shared_expression,
)
from sempy_labs._generate_semantic_model import create_blank_semantic_model
from sempy_labs._refresh_semantic_model import refresh_semantic_model
from sempy_labs.connection._databricks import (
    create_azure_databricks_workspace_connection,
)
from sempy_labs.connection._items import list_connections
from sempy_labs.mirrored_azure_databricks_catalog._list_objects import (
    list_databricks_columns,
)
from sempy_labs.mirrored_azure_databricks_catalog._items import (
    create_mirrored_azure_databricks_catalog,
    list_mirrored_azure_databricks_catalogs,
)
from sempy_labs.mirrored_azure_databricks_catalog._semantic_model import (
    create_expression_name,
    check_tables_format,
    convert_column_data_type,
    infer_model_relationships,
)
from sempy_labs.mirrored_azure_databricks_catalog._refresh_catalog_metadata import (
    refresh_catalog_metadata,
)
from sempy._utils._log import log


def generate_databricks_connection_name(
    name: str = None, dataframe: pd.DataFrame = None
) -> str:
    if name is not None:
        return name
    name = "AzureDatabricksWorkspaceConnection"
    if dataframe is None:
        raise ValueError("Dataframe must be provided if name is not specified.")
    connection_names = dataframe["Connection Name"].values.tolist()
    if name in connection_names:
        i = 1
        new_name = f"{name}_{i}"
        while new_name in connection_names:
            i += 1
            new_name = f"{name}_{i}"
        name = new_name
    return name


@log
def create_semantic_model_from_databricks(
    dataset: str | UUID,
    databricks_workspace: str,
    databricks_token: str,
    tables: List[str],
    databricks_connection_name: Optional[str] = None,
    infer_relationships: bool = True,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates or updates a semantic model based on tables in a Databricks workspace. The function will create a connection to the Databricks workspace, create mirrored catalogs for the specified tables, and generate a semantic model with entities and columns based on the tables' schemas. If infer_relationships is set to True, it will also attempt to infer relationships between tables based on column names and data types.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model. If the semantic model does not already exist, a new one will be created with this name. If it does exist, it will be updated with any new tables or columns.
    databricks_workspace : str
        The URL of the Databricks workspace (e.g. https://adb-12345678.9.azuredatabricks.net).
    databricks_token : str
        A Databricks personal access token with permissions to read the workspace's catalogs and tables.
    tables : typing.List[str]
        A list of tables to include in the semantic model, in the format "catalog.schema.table".
    databricks_connection_name : Optional[str], default=None
        The name of the Databricks connection to use. If not provided, a new connection will be created.
    infer_relationships : bool, default=True
        Whether to infer relationships between tables based on column names and data types.
    workspace : Optional[str | uuid.UUID], default=None
        The name or ID of the workspace where the semantic model will be created or updated.
    """
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    # Check if semantic model already exists
    item_id = resolve_item_id(
        item=dataset, type="SemanticModel", workspace=workspace_id, error_out=False
    )
    if not item_id and _is_valid_uuid(dataset):
        raise ValueError(
            f"The '{dataset}' semantic model does not already exist. If you want to create a new semantic model, please provide a name for the dataset instead of an id."
        )

    check_tables_format(tables)

    # Create databricks connection if it doesn't exist
    databricks_workspace = databricks_workspace.rstrip("/")
    df = list_connections()

    databricks_connection_name = generate_databricks_connection_name(
        name=databricks_connection_name, dataframe=df
    )
    df_filt = df[
        (df["Connection Type"] == "AzureDatabricksWorkspace")
        & (df["Connection Path"].str.rstrip("/") == databricks_workspace)
    ]
    if df_filt.empty:
        connection_id = create_azure_databricks_workspace_connection(
            name=databricks_connection_name,
            url=databricks_workspace,
            databricks_token=databricks_token,
            privacy_level=None,
        )
    else:
        connection_id = df_filt["Connection Id"].iloc[0]

    # Create mirrored catalog if it doesn't exist
    catalogs = list(set([catalog.split(".")[0] for catalog in tables]))

    df = list_mirrored_azure_databricks_catalogs(workspace=workspace_id)

    mirror_ids = {}
    for catalog in catalogs:
        df_filt = df[
            (df["Catalog Name"] == catalog)
            & (df["Databricks Workspace Connection Id"] == connection_id)
        ]
        if df_filt.empty:
            id = create_mirrored_azure_databricks_catalog(
                name=catalog,
                catalog_name=catalog,
                databricks_workspace_connection_id=connection_id,
                mirroring_mode="Full",
                workspace=workspace_id,
            )
            mirror_ids[catalog] = id
        else:
            mirror_ids[catalog] = df_filt["Mirrored Azure Databricks Catalog Id"].iloc[
                0
            ]

    # Refresh catalog metadata to ensure we have the latest schema information
    for catalog, catalog_id in mirror_ids.items():
        refresh_catalog_metadata(
            mirrored_azure_databricks_catalog=catalog_id,
            workspace=workspace_id,
        )

    if not item_id:
        create_blank_semantic_model(dataset=dataset, workspace=workspace_id)

    time.sleep(10)

    # Generate semantic model
    with connect_semantic_model(
        dataset=dataset, workspace=workspace_id, readonly=False
    ) as tom:

        column_list = []
        for t in tables:
            parts = t.split(".")
            catalog_name, schema_name, table_name = parts
            catalog_id = mirror_ids[catalog_name]

            # Generate the expression for the Mirrored Azure Databricks Catalog
            expr = generate_shared_expression(
                item=catalog_id,
                item_type="MirroredAzureDatabricksCatalog",
                workspace=workspace,
                use_sql_endpoint=False,
            )

            # Check if the expression already exists in the model
            expression_names = []
            found = False
            expression_name = None
            for e in tom.model.Expressions:
                expression_names.append(e.Name)
                if e.Expression == expr:
                    found = True
                    expression_name = e.Name

            # Add the expression if it does not exist
            if not found:
                expression_name = create_expression_name(
                    expression_names=expression_names
                )
                tom.add_expression(name=expression_name, expression=expr)

            # Determine the columns in the table
            df = list_databricks_columns(
                databricks_workspace=databricks_workspace,
                unity_catalog=catalog_name,
                schema=schema_name,
                databricks_token=databricks_token,
            )
            df_filt = df[
                (df["Table Name"] == table_name) & (df["Schema Name"] == schema_name)
            ]

            # Only add table if it is found and has columns
            if df_filt.empty:
                print(f"No columns found for table {t}. Skipping.")
                continue
            if table_name not in [t.Name for t in tom.model.Tables]:
                tom.add_table(name=table_name)
                tom.add_entity_partition(
                    table_name=table_name,
                    entity_name=table_name,
                    expression=expression_name,
                    schema_name=schema_name,
                )

                # Add the columns to the table
                for _, row in df_filt.iterrows():
                    column_name = row["Column Name"]
                    data_type = row["Data Type"]
                    data_type_converted = convert_column_data_type(data_type)
                    column_list.append(
                        {
                            "sourceCatalog": catalog_id,
                            "sourceSchema": schema_name,
                            "tableName": table_name,
                            "columnName": column_name,
                            "dataType": data_type_converted,
                        }
                    )
                    if column_name not in [
                        c.Name
                        for c in tom.all_columns()
                        if c.Name == column_name and c.Parent.Name == table_name
                    ]:
                        tom.add_data_column(
                            table_name=table_name,
                            column_name=column_name,
                            source_column=column_name,
                            data_type=data_type_converted,
                        )

        # Infer relationships
        if infer_relationships:
            relationships = infer_model_relationships(
                column_list=column_list, workspace=workspace
            )
            for r in relationships:
                tom.add_relationship(
                    from_table=r.get("fromTable"),
                    from_column=r.get("fromColumn"),
                    to_table=r.get("toTable"),
                    to_column=r.get("toColumn"),
                )

    time.sleep(5)
    refresh_semantic_model(dataset=dataset, workspace=workspace)
