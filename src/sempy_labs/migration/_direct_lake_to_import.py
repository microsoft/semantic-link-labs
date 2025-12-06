import sempy
from uuid import UUID
import sempy_labs._icons as icons
from typing import Optional
from sempy._utils._log import log


@log
def migrate_direct_lake_to_import(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    mode: str = "import",
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
    mode : str, default="import"
        The mode to migrate to. Can be either "import" or "directquery".
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

    # if isinstance(tables, str):
    #     tables = [tables]

    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=False
    ) as tom:

        if not tom.is_direct_lake():
            print(
                f"{icons.warning} The '{dataset}' semantic model within the '{workspace}' workspace is not in Direct Lake mode."
            )
            return

        # if tables is None:
        table_list = [t for t in tom.model.Tables]
        # else:
        #    table_list = [t for t in tom.model.Tables if t.Name in tables]
        # if not table_list:
        #    raise ValueError(f"{icons.red_dot} No tables found to migrate.")

        for t in table_list:
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
                    mode=actual_mode,
                )
                # Remove Direct Lake partition
                tom.remove_object(object=p)
                # if tables is not None:
                #    print(
                #        f"{icons.green_dot} The '{table_name}' table has been migrated to '{actual_mode}' mode."
                #    )

        tom.model.Model.DefaultMode = TOM.ModeType.Import
    # if tables is None:
    print(
        f"{icons.green_dot} All tables which were in Direct Lake mode have been migrated to '{actual_mode}' mode."
    )

    # Check
    # for t in tom.model.Tables:
    #    if t.Partitions.Count == 1 and all(p.Mode == TOM.ModeType.Import for p in t.Partitions) and t.CalculationGroup is None:
    #        p = next(p for p in t.Partitions)
    #        print(p.Name)
    #        print(p.Source.Expression)
