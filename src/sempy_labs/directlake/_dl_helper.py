import sempy.fabric as fabric
import numpy as np
import pandas as pd
from typing import Optional, List, Literal
from uuid import UUID
import sempy_labs._icons as icons
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    resolve_dataset_name_and_id,
    resolve_workspace_id,
)


@log
def check_fallback_reason(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows the reason a table in a Direct Lake semantic model would fallback to DirectQuery.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        The tables in the semantic model and their fallback reason.
    """
    from sempy_labs.tom import connect_semantic_model

    workspace_id = resolve_workspace_id(workspace)
    dataset_name, dataset_id = resolve_dataset_name_and_id(
        dataset, workspace=workspace_id
    )

    with connect_semantic_model(
        dataset=dataset_id, workspace=workspace_id, readonly=True
    ) as tom:
        if not tom.is_direct_lake():
            raise ValueError(
                f"{icons.red_dot} The '{dataset_name}' semantic model is not in Direct Lake. This function is only applicable to Direct Lake semantic models."
            )

    df = fabric.evaluate_dax(
        dataset=dataset_id,
        workspace=workspace_id,
        dax_string="""
            SELECT [TableName] AS [Table Name],[FallbackReason] AS [FallbackReasonID]
            FROM $SYSTEM.TMSCHEMA_DELTA_TABLE_METADATA_STORAGES
            """,
    )

    value_mapping = {
        0: "No reason for fallback",
        1: "This table is not framed",
        2: "This object is a view in the lakehouse",
        3: "The table does not exist in the lakehouse",
        4: "Transient error",
        5: "Using OLS will result in fallback to DQ",
        6: "Using RLS will result in fallback to DQ",
    }

    # Create a new column based on the mapping
    df["Fallback Reason Detail"] = np.vectorize(value_mapping.get)(
        df["FallbackReasonID"]
    )

    return df


@log
def generate_direct_lake_semantic_model(
    dataset: str,
    tables: List[str],
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
    inherit_descriptions: bool = True,
    overwrite: bool = False,
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
    inherit_descriptions : bool, default=True
        If True, sets table/column descriptions based on the comments/descriptions in the source table. Only available for lakehouse sources.
    overwrite : bool, default=False
        If True, overwrites the existing semantic model if it already exists. If False, raises an error if the semantic model already exists.
    """
    from sempy_labs.semantic_model._generate import (
        generate_direct_lake_semantic_model as gen,
    )

    gen(
        dataset=dataset,
        tables=tables,
        source=source,
        source_type=source_type,
        source_workspace=source_workspace,
        use_sql_endpoint=use_sql_endpoint,
        workspace=workspace,
        refresh=refresh,
        inherit_descriptions=inherit_descriptions,
        overwrite=overwrite,
    )
