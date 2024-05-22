import sempy
import sempy.fabric as fabric
import pandas as pd
import json, base64, time

def create_report_from_reportjson(report: str, dataset: str, report_json, theme_json = None, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/m-kovalsky/fabric_cat_tools?tab=readme-ov-file#create_report_from_reportjson

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    objectType = 'Report'

    dfI = fabric.list_items(workspace = workspace)
    dfI_model = dfI[(dfI['Display Name'] == dataset) & (dfI['Type'] == 'SemanticModel')]

    if len(dfI_model) == 0:
        print(f"ERROR: The '{dataset}' semantic model does not exist in the '{workspace}' workspace.")
        return
    
    datasetId = dfI_model['Id'].iloc[0]

    dfI_rpt = dfI[(dfI['Display Name'] == report) & (dfI['Type'] == 'Report')]

    if len(dfI_rpt) > 0:
        print(f"WARNING: '{report}' already exists as a report in the '{workspace}' workspace.")
        return

    client = fabric.FabricRestClient()
    defPBIR = {
    "version": "1.0",
    "datasetReference": {
        "byPath": None,
        "byConnection": {
            "connectionString": None,
            "pbiServiceModelId": None,
            "pbiModelVirtualServerName": "sobe_wowvirtualserver",
            "pbiModelDatabaseName": datasetId,
            "name": "EntityDataSource",
            "connectionType": "pbiServiceXmlaStyleLive"
        }
    }
}

    def conv_b64(file):
        
        loadJson = json.dumps(file)
        f = base64.b64encode(loadJson.encode('utf-8')).decode('utf-8')
        
        return f

    definitionPBIR = conv_b64(defPBIR)
    payloadReportJson = conv_b64(report_json)
    
    if theme_json == None:      
        request_body = {
                'displayName': report,
                'type': objectType,
                'definition': {
            "parts": [
                {
                    "path": "report.json",
                    "payload": payloadReportJson,
                    "payloadType": "InlineBase64"
                },
                {
                    "path": "definition.pbir",
                    "payload": definitionPBIR,
                    "payloadType": "InlineBase64"
                }
            ]

                }
            }
    else:
        payloadThemeJson = conv_b64(theme_json)
        themeID = theme_json['payload']['blob']['displayName']
        themePath = 'StaticResources/SharedResources/BaseThemes/' + themeID + '.json'
        request_body = {
                'displayName': report,
                'type': objectType,
                'definition': {
            "parts": [
                {
                    "path": "report.json",
                    "payload": payloadReportJson,
                    "payloadType": "InlineBase64"
                },
                {
                    "path": themePath,
                    "payload": payloadThemeJson,
                    "payloadType": "InlineBase64"
                },
                {
                    "path": "definition.pbir",
                    "payload": definitionPBIR,
                    "payloadType": "InlineBase64"
                }
            ]

                }
            }

    response = client.post(f"/v1/workspaces/{workspace_id}/items",json=request_body)

    if response.status_code == 201:
        print('Report creation succeeded')
        print(response.json())
    elif response.status_code == 202:
        operationId = response.headers['x-ms-operation-id']
        response = client.get(f"/v1/operations/{operationId}")
        response_body = json.loads(response.content) 
        while response_body['status'] != 'Succeeded':
            time.sleep(3)
            response = client.get(f"/v1/operations/{operationId}")
            response_body = json.loads(response.content)
        response = client.get(f"/v1/operations/{operationId}/result")
        print('Report creation succeeded')
        print(response.json())

def update_report_from_reportjson(report, report_json, workspace = None):

    """
    
    Documentation is available here: https://github.com/m-kovalsky/fabric_cat_tools?tab=readme-ov-file#update_report_from_reportjson

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    objectType = 'Report'

    dfR = fabric.list_reports(workspace = workspace)
    dfR_filt = dfR[(dfR['Name'] == report) & (dfR['Report Type'] == 'PowerBIReport')]

    if len(dfR_filt) == 0:
        print(f"The '{report}' report does not exist in the '{workspace}' workspace.")
        return
    
    reportId = dfR_filt['Id'].iloc[0]
    client = fabric.FabricRestClient()

    response = client.post(f"/v1/workspaces/{workspace_id}/items/{reportId}/getDefinition")
    df_items = pd.json_normalize(response.json()['definition']['parts'])
    df_items_filt = df_items[df_items['path'] == 'definition.pbir']
    rptDefFile = df_items_filt['payload'].iloc[0]
    #datasetId = dfR_filt['Dataset Id'].iloc[0]
    #datasetWorkspaceId = dfR_filt['Dataset Workspace Id'].iloc[0]


    #defPBIR = {
    #"version": "1.0",
    #"datasetReference": {
    #    "byPath": None,
    #    "byConnection": {
    #        "connectionString": None,
    #        "pbiServiceModelId": None,
    #        "pbiModelVirtualServerName": "sobe_wowvirtualserver",
    #        "pbiModelDatabaseName": datasetId,
    #        "name": "EntityDataSource",
    #        "connectionType": "pbiServiceXmlaStyleLive"
    #    }
    #}
#}

    def conv_b64(file):
        
        loadJson = json.dumps(file)
        f = base64.b64encode(loadJson.encode('utf-8')).decode('utf-8')
        
        return f

    #definitionPBIR = conv_b64(defPBIR)
    payloadReportJson = conv_b64(report_json)
        
    request_body = {
            'displayName': report,
            'type': objectType,
            'definition': {
        "parts": [
            {
                "path": "report.json",
                "payload": payloadReportJson,
                "payloadType": "InlineBase64"
            },
            {
                "path": "definition.pbir",
                "payload": rptDefFile,
                "payloadType": "InlineBase64"
            }
        ]

            }
        }

    response = client.post(f"/v1/workspaces/{workspace_id}/reports/{reportId}/updateDefinition",json=request_body)

    if response.status_code == 201:
        print(f"The '{report}' report has been successfully updated.")
        #print(response.json())
    elif response.status_code == 202:
        operationId = response.headers['x-ms-operation-id']
        response = client.get(f"/v1/operations/{operationId}")
        response_body = json.loads(response.content) 
        while response_body['status'] != 'Succeeded':
            time.sleep(3)
            response = client.get(f"/v1/operations/{operationId}")
            response_body = json.loads(response.content)
        response = client.get(f"/v1/operations/{operationId}/result")
        print(f"The '{report}' report has been successfully updated.")
        #print(response.json())