import sempy
import sempy.fabric as fabric
import pandas as pd
from .HelperFunctions import resolve_lakehouse_name, resolve_lakehouse_id
from typing import List, Optional, Union

green_dot = '\U0001F7E2'
yellow_dot = '\U0001F7E1'
red_dot = '\U0001F534'
in_progress = '⌛'

def create_shortcut_onelake(table_name: str, source_lakehouse: str, source_workspace: str, destination_lakehouse: str, destination_workspace: Optional[str] = None, shortcut_name: Optional[str] = None):

    """
    Creates a [shortcut](https://learn.microsoft.com/fabric/onelake/onelake-shortcuts) to a delta table in OneLake.

    Parameters
    ----------
    table_name : str
        The table name for which a shortcut will be created.
    source_lakehouse : str
        The Fabric lakehouse in which the table resides.
    source_workspace : str
        The name of the Fabric workspace in which the source lakehouse exists.
    destination_lakehouse : str
        The Fabric lakehouse in which the shortcut will be created.
    destination_workspace : str, default=None
        The name of the Fabric workspace in which the shortcut will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    shortcut_name : str, default=None 
        The name of the shortcut 'table' to be created. This defaults to the 'table_name' parameter value.       
    
    Returns
    -------
    
    """

    sourceWorkspaceId = fabric.resolve_workspace_id(source_workspace)
    sourceLakehouseId = resolve_lakehouse_id(source_lakehouse, source_workspace)

    if destination_workspace == None:
        destination_workspace = source_workspace
    
    destinationWorkspaceId = fabric.resolve_workspace_id(destination_workspace)
    destinationLakehouseId = resolve_lakehouse_id(destination_lakehouse, destination_workspace)

    if shortcut_name == None:
        shortcut_name = table_name
    
    client = fabric.FabricRestClient()
    tablePath = 'Tables/' + table_name

    request_body = {
    "path": 'Tables',
    "name": shortcut_name.replace(' ',''),
    "target": {
        "oneLake": {
        "workspaceId": sourceWorkspaceId,
        "itemId": sourceLakehouseId,
        "path": tablePath}
        }
    }

    try:
        response = client.post(f"/v1/workspaces/{destinationWorkspaceId}/items/{destinationLakehouseId}/shortcuts",json=request_body)
        if response.status_code == 201:
            print(f"{green_dot} The shortcut '{shortcut_name}' was created in the '{destination_lakehouse}' lakehouse within the '{destination_workspace} workspace. It is based on the '{table_name}' table in the '{source_lakehouse}' lakehouse within the '{source_workspace}' workspace.")
        else:
            print(response.status_code)
    except Exception as e:
        print(f"{red_dot} Failed to create a shortcut for the '{table_name}' table: {e}")

def create_shortcut(shortcut_name: str, location: str, subpath: str, source: str, connection_id: str, lakehouse: Optional[str] = None, workspace: Optional[str] = None):

    """
    Creates a [shortcut](https://learn.microsoft.com/fabric/onelake/onelake-shortcuts) to an ADLS Gen2 or Amazon S3 source.

    Parameters
    ----------
    shortcut_name : str
    location : str
    subpath : str
    source : str
    connection_id: str
    lakehouse : str
        The Fabric lakehouse in which the shortcut will be created.
    workspace : str, default=None
        The name of the Fabric workspace in which the shortcut will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.     
    
    Returns
    -------
    
    """

    source_titles = {
        'adlsGen2': 'ADLS Gen2',
        'amazonS3': 'Amazon S3'
    }

    sourceValues = list(source_titles.keys())

    if source not in sourceValues:
        print(f"{red_dot} The 'source' parameter must be one of these values: {sourceValues}.")
        return

    sourceTitle = source_titles[source]

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    if lakehouse == None:
        lakehouse_id = fabric.get_lakehouse_id()
    else:
        lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)
    
    client = fabric.FabricRestClient()
    shortcutActualName = shortcut_name.replace(' ','')

    request_body = {
    "path": 'Tables',
    "name": shortcutActualName,
    "target": {
        source: {
        "location": location,
        "subpath": subpath,
        "connectionId": connection_id}
        }
    }

    try:
        response = client.post(f"/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts",json=request_body)
        if response.status_code == 201:
            print(f"{green_dot} The shortcut '{shortcutActualName}' was created in the '{lakehouse}' lakehouse within the '{workspace} workspace. It is based on the '{subpath}' table in '{sourceTitle}'.")
        else:
            print(response.status_code)
    except:
        print(f"{red_dot} Failed to create a shortcut for the '{shortcut_name}' table.")

def list_shortcuts(lakehouse: Optional[str] = None, workspace: Optional[str] = None):

    """
    Shows all shortcuts which exist in a Fabric lakehouse.

    Parameters
    ----------
    lakehouse : str, default=None
        The Fabric lakehouse name.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str, default=None
        The name of the Fabric workspace in which lakehouse resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.     
    
    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing all the shortcuts which exist in the specified lakehouse.
    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)
    
    if lakehouse == None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, workspace)
    else:
        lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)

    df = pd.DataFrame(columns=['Shortcut Name', 'Shortcut Path', 'Source', 'Source Lakehouse Name', 'Source Workspace Name', 'Source Path', 'Source Connection ID', 'Source Location', 'Source SubPath'])

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts")
    if response.status_code == 200:
        for s in response.json()['value']:
            shortcutName = s['name']
            shortcutPath = s['path']
            source = list(s['target'].keys())[0]
            sourceLakehouseName, sourceWorkspaceName, sourcePath, connectionId, location, subpath = None, None, None, None, None, None
            if source == 'oneLake':
                sourceLakehouseId = s['target'][source]['itemId']
                sourcePath = s['target'][source]['path']
                sourceWorkspaceId = s['target'][source]['workspaceId']
                sourceWorkspaceName = fabric.resolve_workspace_name(sourceWorkspaceId)
                sourceLakehouseName = resolve_lakehouse_name(sourceLakehouseId, sourceWorkspaceName)
            else:
                connectionId = s['target'][source]['connectionId']
                location = s['target'][source]['location']
                subpath = s['target'][source]['subpath']

            new_data = {'Shortcut Name': shortcutName, 'Shortcut Path': shortcutPath, 'Source': source, 'Source Lakehouse Name': sourceLakehouseName, 'Source Workspace Name': sourceWorkspaceName, 'Source Path': sourcePath, 'Source Connection ID': connectionId, 'Source Location': location, 'Source SubPath': subpath}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True) 
    
    print(f"This function relies on an API which is not yet official as of May 21, 2024. Once the API becomes official this function will work as expected.")
    return df

def delete_shortcut(shortcut_name: str, lakehouse: Optional[str] = None, workspace: Optional[str] = None):

    """
    Deletes a shortcut.

    Parameters
    ----------
    shortcut_name : str
        The name of the shortcut.
    lakehouse : str, default=None
        The Fabric lakehouse name in which the shortcut resides.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str, default=None
        The name of the Fabric workspace in which lakehouse resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.     
    
    Returns
    -------
    
    """   

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)
    
    if lakehouse == None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, workspace)
    else:
        lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)

    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts/Tables/{shortcut_name}")
    
    if response.status_code == 200:
        print(f"{green_dot} The '{shortcut_name}' shortcut in the '{lakehouse}' within the '{workspace}' workspace has been deleted.")
    else:
        print(f"{red_dot} The '{shortcut_name}' has not been deleted.")