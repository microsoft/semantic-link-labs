import sempy
import sempy.fabric as fabric
import pandas as pd
from .HelperFunctions import resolve_dataset_id

green_dot = '\U0001F7E2'
yellow_dot = '\U0001F7E1'
red_dot = '\U0001F534'
in_progress = '⌛'

def qso_sync(dataset: str, workspace: str | None = None):

    #https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/trigger-query-scale-out-sync-in-group

    """
    
    

    """

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    dataset_id = resolve_dataset_id(dataset, workspace)

    client = fabric.PowerBIRestClient()
    response = client.post(f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/queryScaleOut/sync")

    if response.status_code == 200:
        print(f"{green_dot} QSO sync initiated for the '{dataset}' semantic model within the '{workspace}' workspace.")
    else:
        print(f"{red_dot} QSO sync failed for the '{dataset}' semantic model within the '{workspace}' workspace.")

def qso_sync_status(dataset: str, workspace: str | None = None):

    #https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/trigger-query-scale-out-sync-in-group#datasetqueryscaleoutsyncstatus

    df = pd.DataFrame(columns=['Scale Out Status', 'Sync Start Time', 'Sync End Time', 'Commit Version', 'Commit Timestamp', 'Target Sync Version', 'Target Sync Timestamp', 'Trigger Reason', 'Min Active Read Version', 'Min Active Read Timestamp'])
    dfRep = pd.DataFrame(columns=['Replica ID', 'Replica Type', 'Replica Version', 'Replica Timestamp'])


    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    dataset_id = resolve_dataset_id(dataset, workspace)

    client = fabric.PowerBIRestClient()
    response = client.get(f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/queryScaleOut/syncStatus")

    if response.status_code == 200:
        o = response.json()
        sos = o['scaleOutStatus']
        
        if sos == 'Enabled':
            new_data = {'Scale Out Status': o['scaleOutStatus'], 'Sync Start Time': o['syncStartTime'], 'Sync End Time': o['syncEndTime'], 'Commit Version': o['commitVersion'], 'Commit Timestamp': o['commitTimestamp'], 'Target Sync Version': o['targetSyncVersion'], 'Target Sync Timestamp': o['targetSyncTimestamp'], 'Trigger Reason': o['triggerReason'], 'Min Active Read Version': o['minActiveReadVersion'], 'Min Active Read Timestamp': o['minActiveReadTimestamp']}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

            for r in o['scaleOutReplicas']:
                new_data = {'Replica ID': r['replicaId'], 'Replica Type': r['replicaType'], 'Replica Version': str(r['replicaVersion']), 'Replica Timestamp': r['replicaTimestamp']}
                dfRep = pd.concat([dfRep, pd.DataFrame(new_data, index=[0])], ignore_index=True)

            df['Sync Start Time'] = pd.to_datetime(df['Sync Start Time'])
            df['Sync End Time'] = pd.to_datetime(df['Sync End Time'])
            df['Commit Timestamp'] = pd.to_datetime(df['Commit Timestamp'])
            df['Target Sync Timestamp'] = pd.to_datetime(df['Target Sync Timestamp'])
            df['Min Active Read Timestamp'] = pd.to_datetime(df['Min Active Read Timestamp'])
            dfRep['Replica Timestamp'] = pd.to_datetime(dfRep['Replica Timestamp'])
            df['Commit Version'] = df['Commit Version'].astype('int')
            df['Target Sync Version'] = df['Target Sync Version'].astype('int')
            df['Min Active Read Version'] = df['Min Active Read Version'].astype('int')            

            return df, dfRep
        else:
            print(f"{sos}\n\n")
            return df, dfRep
    else:
        return response.status_code

def disable_qso(dataset: str, workspace: str | None = None):

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    dataset_id = resolve_dataset_id(dataset, workspace)

    request_body = {
        "queryScaleOutSettings": {
            "maxReadOnlyReplicas": '0'
        }
    }

    client = fabric.PowerBIRestClient()
    response = client.patch(f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}", json = request_body)
    if response.status_code == 200:
        df = list_qso_settings(dataset = dataset, workspace = workspace)
        print(f"{green_dot} Query scale out has been disabled for the '{dataset}' semantic model within the '{workspace}' workspace.")
        return df
    else:
        return f"{red_dot} {response.status_code}"

def set_qso(dataset: str, auto_sync: bool = True, max_read_only_replicas: int = -1, workspace: str | None = None):

    #https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/update-dataset-in-group

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    dataset_id = resolve_dataset_id(dataset, workspace)

    if max_read_only_replicas == 0:
        disable_qso(dataset = dataset, workspace = workspace)
        return

    request_body = {
        "queryScaleOutSettings": {
            "autoSyncReadOnlyReplicas": auto_sync,
            "maxReadOnlyReplicas": str(max_read_only_replicas)
        }
    }

    ssm = set_semantic_model_storage_format(dataset = dataset, storage_format='Large', workspace=workspace)
    if ssm == 200:
        client = fabric.PowerBIRestClient()
        response = client.patch(f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}", json = request_body)
        if response.status_code == 200:
            df = list_qso_settings(dataset = dataset, workspace = workspace)
            print(f"{green_dot} Query scale out has been set on the '{dataset}' semantic model within the '{workspace}' workspace.")
            return df
        else:
            return f"{red_dot} {response.status_code}"
    else:
        print(f"{red_dot} Failed to set the '{dataset}' semantic model within the '{workspace}' workspace to large semantic model storage format. This is a prerequisite for enabling Query Scale Out.")
        print("https://learn.microsoft.com/power-bi/enterprise/service-premium-scale-out#prerequisites")
        return

    
def set_semantic_model_storage_format(dataset: str, storage_format: str, workspace: str | None = None):

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    dataset_id = resolve_dataset_id(dataset, workspace)

    storage_format = storage_format.capitalize()

    if storage_format == 'Abf':
        storage_format = 'Small'
    elif storage_format.startswith('Premium'):
        storage_format = 'Large'

    storageFormats = ['Small', 'Large']

    if storage_format == 'Large':
        request_body = {
            "targetStorageMode": "PremiumFiles"
        }
    elif storage_format == 'Small':
        request_body = {
            "targetStorageMode": "Abf"
        }
    else:
        print(f"{red_dot} Invalid storage format value. Valid options: {storageFormats}.")
        return

    client = fabric.PowerBIRestClient()
    response = client.patch(f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}", json = request_body)

    if response.status_code == 200:
        return print(f"{green_dot} Semantic model storage format set to '{storage_format}'.")
    else:
        return f"{red_dot} {response.status_code}"

def list_qso_settings(dataset: str | None = None, workspace: str | None = None):

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    if dataset is not None:
        dataset_id = resolve_dataset_id(dataset, workspace)

    workspace_id = fabric.get_workspace_id()
    df = pd.DataFrame(columns=['Dataset Id', 'Dataset Name', 'Storage Mode', 'QSO Auto Sync Enabled', 'QSO Max Read Only Replicas'])
    client = fabric.PowerBIRestClient()
    response = client.get(f"/v1.0/myorg/groups/{workspace_id}/datasets")
    for v in response.json()['value']:
        tsm = v['targetStorageMode']
        if tsm == 'Abf':
            sm = 'Small'
        else:
            sm = 'Large'
        new_data = {'Dataset Id': v['id'], 'Dataset Name': v['name'], 'Storage Mode': sm, 'QSO Auto Sync Enabled': v['queryScaleOutSettings']['autoSyncReadOnlyReplicas'], 'QSO Max Read Only Replicas': v['queryScaleOutSettings']['maxReadOnlyReplicas'] }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df['QSO Auto Sync Enabled'] = df['QSO Auto Sync Enabled'].astype('bool')
    df['QSO Max Read Only Replicas'] = df['QSO Max Read Only Replicas'].astype('int')
    
    if dataset is not None:
        df = df[df['Dataset Id'] == dataset_id]
    
    return df

def set_workspace_default_storage_format(storage_format: str, workspace: str | None = None):

    #https://learn.microsoft.com/en-us/rest/api/power-bi/groups/update-group#defaultdatasetstorageformat

    """


    """

    storageFormats = ['Small', 'Large']

    storage_format = storage_format.capitalize()

    if storage_format not in storageFormats:
        print(f"Invalid storage format. Please choose from these options: {storageFormats}.")

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    request_body = {
        "name": workspace,
        "defaultDatasetStorageFormat": storage_format
    }

    client = fabric.PowerBIRestClient()
    response = client.patch(f"/v1.0/myorg/groups/{workspace_id}", json = request_body)

    if response.status_code == 200:
        print(f"{green_dot} The default storage format for the '{workspace}' workspace has been updated to '{storage_format}.")
    else:
        print(f"{red_dot} {response.status_code}")