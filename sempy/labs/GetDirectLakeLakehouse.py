import sempy
import sempy.fabric as fabric
from .HelperFunctions import resolve_lakehouse_id
from .HelperFunctions import resolve_lakehouse_name
from .HelperFunctions import get_direct_lake_sql_endpoint

def get_direct_lake_lakehouse(dataset: str, workspace: str | None = None, lakehouse: str | None = None, lakehouse_workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_direct_lake_lakehouse

    """    

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    if lakehouse_workspace is None:
        lakehouse_workspace = workspace
    
    if lakehouse == None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, lakehouse_workspace)

    dfP = fabric.list_partitions(dataset = dataset, workspace = workspace)
    dfP_filt = dfP[dfP['Mode'] == 'DirectLake']

    if len(dfP_filt) == 0:
        print(f"ERROR: The '{dataset}' semantic model within the '{workspace}' workspace is not in Direct Lake mode.")
    else:
        sqlEndpointId = get_direct_lake_sql_endpoint(dataset, workspace)

        dfI = fabric.list_items(workspace = lakehouse_workspace, type = 'SQLEndpoint')
        dfI_filt = dfI[dfI['Id'] == sqlEndpointId]
        lakehouseName = dfI_filt['Display Name'].iloc[0]

        lakehouseId = resolve_lakehouse_id(lakehouseName, lakehouse_workspace)

        return lakehouseName, lakehouseId
    
   

