import sempy
import sempy.fabric as fabric
from sempy_labs.lakehouse import get_lakehouse_columns
from sempy_labs.directlake._dl_helper import get_direct_lake_source
from sempy_labs.tom import connect_semantic_model
from sempy_labs._helper_functions import (
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
    **kwargs,
):
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
    """

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM

    if "lakehouse" in kwargs:
        print(
            "The 'lakehouse' parameter has been deprecated as it is no longer necessary. Please remove this parameter from the function going forward."
        )
        del kwargs["lakehouse"]
    if "lakehouse_workspace" in kwargs:
        print(
            "The 'lakehouse_workspace' parameter has been deprecated as it is no longer necessary. Please remove this parameter from the function going forward."
        )
        del kwargs["lakehouse_workspace"]

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    artifact_type, lakehouse_name, lakehouse_id, lakehouse_workspace_id = (
        get_direct_lake_source(dataset=dataset_id, workspace=workspace_id)
    )

    if artifact_type == "Warehouse":
        raise ValueError(
            f"{icons.red_dot} This function is only valid for Direct Lake semantic models which source from Fabric lakehouses (not warehouses)."
        )
    lakehouse_workspace = fabric.resolve_workspace_name(lakehouse_workspace_id)

    lc = get_lakehouse_columns(lakehouse_name, lakehouse_workspace)

    with connect_semantic_model(
        dataset=dataset_id, readonly=False, workspace=workspace_id
    ) as tom:

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
                    print(
                        f"{icons.yellow_dot} The '{lakeCName}' column exists in the '{lakeTName}' lakehouse table but not in the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
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
