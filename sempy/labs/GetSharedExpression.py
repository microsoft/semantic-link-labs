import sempy
import sempy.fabric as fabric
from .HelperFunctions import resolve_lakehouse_name
from .ListFunctions import list_lakehouses

def get_shared_expression(lakehouse: str | None = None, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/m-kovalsky/fabric_cat_tools?tab=readme-ov-file#get_shared_expression

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)
    if lakehouse == None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id)

    dfL = list_lakehouses(workspace = workspace)
    lakeDetail = dfL[dfL['Lakehouse Name'] == lakehouse]

    sqlEPCS = lakeDetail['SQL Endpoint Connection String'].iloc[0]
    sqlepid = lakeDetail['SQL Endpoint ID'].iloc[0]
    provStatus = lakeDetail['SQL Endpoint Provisioning Status'].iloc[0]

    if provStatus == 'InProgress':
        print(f"The SQL Endpoint for the '{lakehouse}' lakehouse within the '{workspace}' workspace has not yet been provisioned. Please wait until it has been provisioned.")
        return
    
    sh = 'let\n\tdatabase = Sql.Database("' + sqlEPCS + '", "' + sqlepid + '")\nin\n\tdatabase'

    return sh