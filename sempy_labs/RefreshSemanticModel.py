import sempy
import sempy.fabric as fabric
import time
from .HelperFunctions import resolve_dataset_id
from sempy._utils._log import log

green_dot = '\U0001F7E2'
yellow_dot = '\U0001F7E1'
red_dot = '\U0001F534'
in_progress = '⌛'

@log
def refresh_semantic_model(dataset: str, tables: str | list | None = None, partitions: str | list | None = None, refresh_type: str | None = None, retry_count: int = 0, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#refresh_semantic_model

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    if refresh_type is None:
        refresh_type = 'full'

    if isinstance(tables, str):
        tables = [tables]
    if isinstance(partitions, str):
        partitions = [partitions]

    objects = []

    if tables is not None:
        objects = objects + [{"table": table} for table in tables]
    if partitions is not None:
        def extract_names(partition):
            parts = partition.split("[")
            table_name = parts[0].strip("'")
            partition_name = parts[1].strip("]")
            return {"table": table_name, "partition": partition_name}

        objects = objects + [extract_names(partition) for partition in partitions]

    refresh_type = refresh_type.lower().replace('only', 'Only').replace('values', 'Values')

    refreshTypes = ['full', 'automatic', 'dataOnly', 'calculate', 'clearValues', 'defragment']

    if refresh_type not in refreshTypes:
        print(f"{red_dot} Invalid refresh type. Refresh type must be one of these values: {refreshTypes}.")
        return
        
    if len(objects) == 0:
        requestID = fabric.refresh_dataset(dataset = dataset, workspace = workspace, refresh_type = refresh_type, retry_count = retry_count)
    else:
        requestID = fabric.refresh_dataset(dataset = dataset, workspace = workspace, refresh_type = refresh_type, retry_count = retry_count, objects = objects)
    print(f"{in_progress} Refresh of the '{dataset}' semantic model within the '{workspace}' workspace is in progress...")
    if len(objects) != 0:
        print(objects)

    while True:
        requestDetails = fabric.get_refresh_execution_details(dataset = dataset,refresh_request_id = requestID, workspace = workspace)
        status = requestDetails.status

        # Check if the refresh has completed
        if status == 'Completed':
            break
        elif status == 'Failed':
            print(f"{red_dot} The refresh of the '{dataset}' semantic model within the '{workspace}' workspace has failed.")
            return
        elif status == 'Cancelled':
            print(f"{yellow_dot} The refresh of the '{dataset}' semantic model within the '{workspace}' workspace has been cancelled.")
            return

        time.sleep(3)

    print(f"{green_dot} Refresh of the '{dataset}' semantic model within the '{workspace}' workspace is complete.")

def cancel_dataset_refresh(dataset, request_id = None, workspace = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#cancel_dataset_refresh

    """    

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)
    
    rr = fabric.list_refresh_requests(dataset = dataset, workspace = workspace)
    rr_filt = rr[rr['Status'] == 'Unknown']

    if request_id == None:
        if len(rr_filt) == 0:
            print(f"{red_dot} There are no active Enhanced API refreshes of the '{dataset}' semantic model within the '{workspace}' workspace.")
            return
        request_id = rr_filt['Request Id'].iloc[0]
    
    dataset_id = resolve_dataset_id(dataset = dataset, workspace = workspace)

    client = fabric.PowerBIRestClient()

    response = client.delete(f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes/{request_id}")    
    if response.status_code == 200:
        print(f"{green_dot} The '{request_id}' refresh request for the '{dataset}' semantic model within the '{workspace}' workspace has been cancelled.")
    else:
        print(response.status_code)

