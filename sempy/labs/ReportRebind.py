import sempy
import sempy.fabric as fabric
from .HelperFunctions import resolve_dataset_id, resolve_report_id

green_dot = '\U0001F7E2'
yellow_dot = '\U0001F7E1'
red_dot = '\U0001F534'
in_progress = '⌛'

def report_rebind(report: str, dataset: str, report_workspace: str | None = None, dataset_workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/m-kovalsky/fabric_cat_tools?tab=readme-ov-file#report_rebind

    """

    if report_workspace == None:
        report_workspace_id = fabric.get_workspace_id()
        report_workspace = fabric.resolve_workspace_name(report_workspace_id)
    else:
        report_workspace_id = fabric.resolve_workspace_id(report_workspace)
    if dataset_workspace == None:
        dataset_workspace = report_workspace 

    client = fabric.PowerBIRestClient()

    reportId = resolve_report_id(report = report, workspace = report_workspace)
    datasetId = resolve_dataset_id(dataset = dataset, workspace = dataset_workspace)

    # Prepare API
    request_body = {
        'datasetId': datasetId
    }

    response = client.post(f"/v1.0/myorg/groups/{report_workspace_id}/reports/{reportId}/Rebind",json=request_body)

    if response.status_code == 200:
        print(f"{green_dot} The '{report}' report has been successfully rebinded to the '{dataset}' semantic model.")
    else:
        print(f"{red_dot} The '{report}' report within the '{report_workspace}' workspace failed to rebind to the '{dataset}' semantic model within the '{dataset_workspace}' workspace.")

def report_rebind_all(dataset: str, new_dataset: str, dataset_workspace: str | None = None, new_dataset_workpace: str | None = None, report_workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/m-kovalsky/fabric_cat_tools?tab=readme-ov-file#report_rebind_all

    """    

    if dataset_workspace == None:
        dataset_workspace_id = fabric.get_workspace_id()
        dataset_workspace = fabric.resolve_workspace_name(dataset_workspace_id)
    else:
        dataset_workspace_id = fabric.resolve_workspace_id(dataset_workspace)    

    if new_dataset_workpace == None:
        new_dataset_workpace = dataset_workspace

    if report_workspace == None:
        report_workspace = dataset_workspace
    
    datasetId = resolve_dataset_id(dataset, dataset_workspace)

    dfRep = fabric.list_reports(workspace = report_workspace)
    dfRep_filt = dfRep[dfRep['Dataset Id'] == datasetId]

    for i, r in dfRep_filt.iterrows():
        rptName = r['Name']
        report_rebind(report = rptName, dataset = new_dataset, report_workspace = report_workspace, dataset_workspace = new_dataset_workpace)