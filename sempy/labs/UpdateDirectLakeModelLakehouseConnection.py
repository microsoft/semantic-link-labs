import sempy
import sempy.fabric as fabric
from .GetSharedExpression import get_shared_expression
from .HelperFunctions import resolve_lakehouse_name
from .TOM import connect_semantic_model

def update_direct_lake_model_lakehouse_connection(dataset: str, workspace: str | None = None, lakehouse: str | None = None, lakehouse_workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#update_direct_lake_model_lakehouse_connection

    """    

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    if lakehouse_workspace == None:
        lakehouse_workspace = workspace

    if lakehouse == None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, lakehouse_workspace)

    # Check if lakehouse is valid
    dfI = fabric.list_items(workspace = lakehouse_workspace, type = 'Lakehouse')
    dfI_filt = dfI[(dfI['Display Name'] == lakehouse)]

    if len(dfI_filt) == 0:
        print(f"The '{lakehouse}' lakehouse does not exist within the '{lakehouse_workspace}' workspace. Therefore it cannot be used to support the '{dataset}' semantic model within the '{workspace}' workspace.")

    dfP = fabric.list_partitions(dataset = dataset, workspace = workspace)
    dfP_filt = dfP[dfP['Mode'] == 'DirectLake']
    
    if len(dfP_filt) == 0:
        print(f"The '{dataset}' semantic model is not in Direct Lake. This function is only applicable to Direct Lake semantic models.")
    else:
        with connect_semantic_model(dataset=dataset, readonly=False, workspace=workspace) as tom:
        
            shEx = get_shared_expression(lakehouse,lakehouse_workspace)
            try:
                tom.model.Expressions['DatabaseQuery'].Expression = shEx
                print(f"The expression in the '{dataset}' semantic model has been updated to point to the '{lakehouse}' lakehouse in the '{lakehouse_workspace}' workspace.")
            except:
                print(f"ERROR: The expression in the '{dataset}' semantic model was not updated.")


    

