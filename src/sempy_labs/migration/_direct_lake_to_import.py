import sempy
from uuid import UUID
import sempy_labs._icons as icons
from typing import Optional, Literal
from sempy._utils._log import log


@log
def migrate_direct_lake_to_import(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    mode: Literal["Import", "DirectQuery"] = "Import",
):
    """
    Migrates a semantic model or specific table(s) from a Direct Lake mode to import or DirectQuery mode. After running this function, you must go to the semantic model settings and update the cloud connection. Not doing so will result in an inablity to refresh/use the semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    mode : typing.Literal["Import", "DirectQuery"], default="Import"
        The mode to migrate to. Can be either "Import" or "DirectQuery".
    """

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM
    from sempy_labs.tom import connect_semantic_model

    modes = {
        "import": "Import",
        "directquery": "DirectQuery",
        "dq": "DirectQuery",
    }

    # Resolve mode
    mode = mode.lower()
    actual_mode = modes.get(mode)
    if actual_mode is None:
        raise ValueError(f"Invalid mode '{mode}'. Must be one of {list(modes.keys())}.")

    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=False
    ) as tom:

        if not tom.is_direct_lake():
            print(
                f"{icons.warning} The '{dataset}' semantic model within the '{workspace}' workspace is not in Direct Lake mode."
            )
            return

        sources = tom.get_direct_lake_sources()

        migrated_tables = []
        for t in tom.model.Tables:
            table_name = t.Name
            p = next(p for p in t.Partitions)
            if p.Mode == TOM.ModeType.DirectLake:
                source_table = p.Source.EntityName
                source_schema = p.Source.SchemaName or 'dbo'
                expr_name = p.Source.ExpressionSource.Name
                p.Name = f"{p.Name}_remove"
                expr = None

                s = next(s for s in sources if s.get('expressionName') == expr_name)
                if s.get('itemType') == 'Lakehouse':
                    expr = f"""let\n\tSource = Lakehouse.Contents(null),\n\tWorkspace = Source{{[workspaceId="{s.get('workspaceId')}"]}}[Data],\n\tArtifact = Workspace{{[lakehouseId="{s.get('itemId')}"]}}[Data],\n\tTable = Artifact{{[Name="{source_table}", ItemKind="Table", Schema="{source_schema}"]}}[Data]\nin\n\tTable"""
                elif s.get('itemType') == 'Warehouse':
                    expr = f"""let\n\tSource = Fabric.Warehouse(),\n\tWorkspace = Source{{[workspaceId="{s.get('workspaceId')}"]}}[Data],\n\tWarehouse = Workspace{{[warehouseId="{s.get('itemId')}"]}}[Data],\n\tTable = Warehouse{{[Schema="{source_schema}", Item="{source_table}"]}}[Data]\nin\n\tTable"""
                else:
                    print(f"{icons.warning} The source type '{s.get('itemType')}' is not supported for converting to Import/DirectQuery mode.")
                
                if expr:
                    tom.add_m_partition(
                        table_name=table_name,
                        partition_name=table_name,
                        expression=expr,
                        mode=actual_mode,
                    )           
                    
                    # Remove Direct Lake partition
                    tom.remove_object(object=p)
                    migrated_tables.append(table_name)

        if migrated_tables:
            tom.model.Model.DefaultMode = TOM.ModeType.Import
            print(
                f"{icons.green_dot} All tables which were in Direct Lake mode have been migrated to '{actual_mode}' mode."
            )