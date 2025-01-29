import sempy
from uuid import UUID
import sempy_labs._icons as icons


def migrate_direct_lake_to_import(dataset: str | UUID, workspace: str | UUID):
    """
    Migrates a semantic model from Direct Lake mode to import mode. After running this function, you must go to the semantic model settings and update the cloud connection. Not doing so will result in an inablity to refresh/use the semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM
    from sempy_labs.tom import connect_semantic_model

    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=False
    ) as tom:

        if not tom.is_direct_lake():
            print(
                f"{icons.warning} The '{dataset}' semantic model within the '{workspace}' workspace is not in Direct Lake mode."
            )
            return

        for t in tom.model.Tables:
            table_name = t.Name
            if t.Partitions.Count == 1 and all(
                p.Mode == TOM.ModeType.DirectLake for p in t.Partitions
            ):
                p = next(p for p in t.Partitions)
                partition_name = p.Name
                entity_name = p.Source.EntityName
                schema_name = p.Source.SchemaName or "dbo"
                # Rename Direct Lake partition
                t.Partitions[partition_name].Name = f"{partition_name}_remove"

                # Generate M expression for import partition
                expression = f"""let\n\tSource = DatabaseQuery,\n\tData = Source{{[Schema="{schema_name}",Item="{entity_name}"]}}[Data]\nin\n\tData"""

                # Generate M partition
                tom.add_m_partition(
                    table_name=table_name,
                    partition_name=partition_name,
                    expression=expression,
                    mode="Import",
                )
                # Remove Direct Lake partition
                tom.remove_object(object=p)

        tom.model.Model.DefaultMode = TOM.ModeType.Import

        # Check
        # for t in tom.model.Tables:
        #    if t.Partitions.Count == 1 and all(p.Mode == TOM.ModeType.Import for p in t.Partitions) and t.CalculationGroup is None:
        #        p = next(p for p in t.Partitions)
        #        print(p.Name)
        #        print(p.Source.Expression)
