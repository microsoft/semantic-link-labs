import sempy
import pandas as pd
from ..lakehouse import get_lakehouse_columns
from ._dl_helper import get_direct_lake_source
from ..tom import connect_semantic_model
from .._helper_functions import (
    _convert_data_type,
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
)
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from uuid import UUID


@log
def direct_lake_schema_sync(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    add_to_model: bool = False,
    remove_from_model: bool = False,
) -> pd.DataFrame:
    """
    Shows/adds columns which exist in the lakehouse but do not exist in the semantic model (only for tables in the semantic model).

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    add_to_model : bool, default=False
        If set to True, columns which exist in the lakehouse but do not exist in the semantic model are added to the semantic model. No new tables are added.
    remove_from_model : bool, default=False
        If set to True, columns which exist in the semantic model but do not exist in the lakehouse are removed from the semantic model. No new tables are removed.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the status of columns in the semantic model and lakehouse (prior to adding/removing them from the model using this function).
    """

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    artifact_type, lakehouse_name, lakehouse_id, lakehouse_workspace_id = (
        get_direct_lake_source(dataset=dataset_id, workspace=workspace_id)
    )

    if artifact_type == "Warehouse":
        raise ValueError(
            f"{icons.red_dot} This function is only valid for Direct Lake semantic models which source from Fabric lakehouses (not warehouses)."
        )

    if artifact_type is None:
        raise ValueError(
            f"{icons.red_dot} This function only supports Direct Lake semantic models where the source lakehouse resides in the same workpace as the semantic model."
        )

    lc = get_lakehouse_columns(lakehouse_id, lakehouse_workspace_id)

    readonly = True
    if add_to_model or remove_from_model:
        readonly = False
    df = pd.DataFrame(
        columns=[
            "TableName",
            "ColumnName",
            "SourceTableName",
            "SourceColumnName",
            "Status",
        ]
    )

    with connect_semantic_model(
        dataset=dataset_id, readonly=readonly, workspace=workspace_id
    ) as tom:
        # Check if the columns in the semantic model exist in the lakehouse
        for c in tom.all_columns():
            column_name = c.Name
            table_name = c.Parent.Name
            partition_name = next(p.Name for p in c.Table.Partitions)
            p = c.Table.Partitions[partition_name]
            if p.SourceType == TOM.PartitionSourceType.Entity:
                entity_name = p.Source.EntityName
                source_column = c.SourceColumn
                lc_filt = lc[
                    (lc["Table Name"] == entity_name)
                    & (lc["Column Name"] == source_column)
                ]
                # Remove column from model if it doesn't exist in the lakehouse
                if lc_filt.empty:
                    new_data = {
                        "TableName": table_name,
                        "ColumnName": column_name,
                        "SourceTableName": entity_name,
                        "SourceColumnName": source_column,
                        "Status": "Not in lakehouse",
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )
                    if remove_from_model:
                        tom.remove_object(object=c)
                        print(
                            f"{icons.green_dot} The '{table_name}'[{column_name}] column has been removed from the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
                        )

        # Check if the lakehouse columns exist in the semantic model
        for i, r in lc.iterrows():
            lakeTName = r["Table Name"]
            lakeCName = r["Column Name"]
            dType = r["Data Type"]

            if any(
                p.Source.EntityName == lakeTName
                for p in tom.all_partitions()
                if p.SourceType == TOM.PartitionSourceType.Entity
            ):
                table_name = next(
                    t.Name
                    for t in tom.model.Tables
                    for p in t.Partitions
                    if p.SourceType == TOM.PartitionSourceType.Entity
                    and p.Source.EntityName == lakeTName
                )

                if not any(
                    c.SourceColumn == lakeCName and c.Parent.Name == table_name
                    for c in tom.all_columns()
                ):
                    new_data = {
                        "TableName": table_name,
                        "ColumnName": None,
                        "SourceTableName": lakeTName,
                        "SourceColumnName": lakeCName,
                        "Status": "Not in semantic model",
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

                    if add_to_model:
                        dt = _convert_data_type(dType)
                        tom.add_data_column(
                            table_name=table_name,
                            column_name=lakeCName,
                            source_column=lakeCName,
                            data_type=dt,
                        )
                        print(
                            f"{icons.green_dot} The '{lakeCName}' column in the '{lakeTName}' lakehouse table was added to the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
                        )

        return df
