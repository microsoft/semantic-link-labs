import sempy
import sempy.fabric as fabric
from .TOM  import connect_semantic_model

def update_direct_lake_partition_entity(dataset: str, table_name: str | list, entity_name: str | list, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/m-kovalsky/fabric_cat_tools?tab=readme-ov-file#update_direct_lake_partition_entity

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    # Support both str & list types
    if isinstance(table_name, str):
        table_name = [table_name]
    if isinstance(entity_name, str):
        entity_name = [entity_name]
    
    if len(table_name) != len(entity_name):
        print(f"ERROR: The 'table_name' and 'entity_name' arrays must be of equal length.")
        return
    
    with connect_semantic_model(dataset=dataset, readonly=False, workspace=workspace) as tom:

        if not tom.is_direct_lake():
            print(f"The '{dataset}' semantic model within the '{workspace}' workspace is not in Direct Lake mode.")
            return

        for tName in table_name:
            i = table_name.index(tName)
            eName = entity_name[i]
            try:
                tom.model.Tables[tName].Partitions[0].EntityName = eName                 
                print(f"The '{tName}' table in the '{dataset}' semantic model has been updated to point to the '{eName}' table in the '{lakehouse}' lakehouse within the '{lakehouse_workspace}' workspace.")
            except:
                print(f"ERROR: The '{tName}' table in the '{dataset}' semantic model has not been updated.")


    

