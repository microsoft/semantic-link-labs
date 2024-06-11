import sempy.fabric as fabric
from sempy_labs._tom import connect_semantic_model
from typing import List, Optional, Union


def update_direct_lake_partition_entity(
    dataset: str,
    table_name: Union[str, List[str]],
    entity_name: Union[str, List[str]],
    workspace: Optional[str] = None,
):
    """
    Remaps a table (or tables) in a Direct Lake semantic model to a table in a lakehouse.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    table_name : str, List[str]
        Name of the table(s) in the semantic model.
    entity_name : str, List[str]
        Name of the lakehouse table to be mapped to the semantic model table.
    workspace : str, default=None
        The Fabric workspace name in which the semantic model exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace = fabric.resolve_workspace_name(workspace)

    # Support both str & list types
    if isinstance(table_name, str):
        table_name = [table_name]
    if isinstance(entity_name, str):
        entity_name = [entity_name]

    if len(table_name) != len(entity_name):
        print(
            f"ERROR: The 'table_name' and 'entity_name' arrays must be of equal length."
        )
        return

    with connect_semantic_model(
        dataset=dataset, readonly=False, workspace=workspace
    ) as tom:

        if not tom.is_direct_lake():
            print(
                f"The '{dataset}' semantic model within the '{workspace}' workspace is not in Direct Lake mode."
            )
            return

        for tName in table_name:
            i = table_name.index(tName)
            eName = entity_name[i]
            try:
                tom.model.Tables[tName].Partitions[0].EntityName = eName
                print(
                    f"The '{tName}' table in the '{dataset}' semantic model has been updated to point to the '{eName}' table in the '{lakehouse}' lakehouse within the '{lakehouse_workspace}' workspace."
                )
            except:
                print(
                    f"ERROR: The '{tName}' table in the '{dataset}' semantic model has not been updated."
                )
