import sempy.fabric as fabric
import pandas as pd
import json
import base64
import time
from typing import Optional
from sempy_labs._helper_functions import resolve_workspace_name_and_id
import sempy_labs._icons as icons


def create_report_from_reportjson(
    report: str,
    dataset: str,
    report_json: dict,
    theme_json: Optional[dict] = None,
    workspace: Optional[str] = None,
):
    """
    Creates a report based on a report.json file (and an optional themes.json file).

    Parameters
    ----------
    report : str
        Name of the report.
    dataset : str
        Name of the semantic model to connect to the report.
    report_json : dict
        The report.json file to be used to create the report.
    theme_json : dict, default=None
        The theme.json file to be used for the theme of the report.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    objectType = "Report"

    dfI_m = fabric.list_items(workspace=workspace, type="SemanticModel")
    dfI_model = dfI_m[(dfI_m["Display Name"] == dataset)]

    if len(dfI_model) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{dataset}' semantic model does not exist in the '{workspace}' workspace."
        )

    datasetId = dfI_model["Id"].iloc[0]

    dfI_r = fabric.list_items(workspace=workspace, type="Report")
    dfI_rpt = dfI_r[(dfI_r["Display Name"] == report)]

    if len(dfI_rpt) > 0:
        print(
            f"{icons.yellow_dot} '{report}' already exists as a report in the '{workspace}' workspace."
        )
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
                "connectionType": "pbiServiceXmlaStyleLive",
            },
        },
    }

    def conv_b64(file):

        loadJson = json.dumps(file)
        f = base64.b64encode(loadJson.encode("utf-8")).decode("utf-8")

        return f

    definitionPBIR = conv_b64(defPBIR)
    payloadReportJson = conv_b64(report_json)

    if theme_json is None:
        request_body = {
            "displayName": report,
            "type": objectType,
            "definition": {
                "parts": [
                    {
                        "path": "report.json",
                        "payload": payloadReportJson,
                        "payloadType": "InlineBase64",
                    },
                    {
                        "path": "definition.pbir",
                        "payload": definitionPBIR,
                        "payloadType": "InlineBase64",
                    },
                ]
            },
        }
    else:
        payloadThemeJson = conv_b64(theme_json)
        themeID = theme_json["payload"]["blob"]["displayName"]
        themePath = "StaticResources/SharedResources/BaseThemes/" + themeID + ".json"
        request_body = {
            "displayName": report,
            "type": objectType,
            "definition": {
                "parts": [
                    {
                        "path": "report.json",
                        "payload": payloadReportJson,
                        "payloadType": "InlineBase64",
                    },
                    {
                        "path": themePath,
                        "payload": payloadThemeJson,
                        "payloadType": "InlineBase64",
                    },
                    {
                        "path": "definition.pbir",
                        "payload": definitionPBIR,
                        "payloadType": "InlineBase64",
                    },
                ]
            },
        }

    response = client.post(f"/v1/workspaces/{workspace_id}/items", json=request_body)

    if response.status_code == 201:
        print(f"{icons.green_dot} Report creation succeeded")
        print(response.json())
    elif response.status_code == 202:
        operationId = response.headers["x-ms-operation-id"]
        response = client.get(f"/v1/operations/{operationId}")
        response_body = json.loads(response.content)
        while response_body["status"] != "Succeeded":
            time.sleep(3)
            response = client.get(f"/v1/operations/{operationId}")
            response_body = json.loads(response.content)
        response = client.get(f"/v1/operations/{operationId}/result")
        print(f"{icons.green_dot} Report creation succeeded")
        print(response.json())


def update_report_from_reportjson(
    report: str, report_json: str, workspace: Optional[str] = None
):
    """
    Updates a report based on a report.json file.

    Parameters
    ----------
    report : str
        Name of the report.
    report_json : str
        The report.json file to be used to update the report.
    workspace : str, default=None
        The Fabric workspace name in which the report resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfR = fabric.list_reports(workspace=workspace)
    dfR_filt = dfR[(dfR["Name"] == report) & (dfR["Report Type"] == "PowerBIReport")]

    if len(dfR_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{report}' report does not exist in the '{workspace}' workspace."
        )

    reportId = dfR_filt["Id"].iloc[0]
    client = fabric.FabricRestClient()

    response = client.post(
        f"/v1/workspaces/{workspace_id}/items/{reportId}/getDefinition"
    )
    df_items = pd.json_normalize(response.json()["definition"]["parts"])
    df_items_filt = df_items[df_items["path"] == "definition.pbir"]
    rptDefFile = df_items_filt["payload"].iloc[0]
    # datasetId = dfR_filt['Dataset Id'].iloc[0]
    # datasetWorkspaceId = dfR_filt['Dataset Workspace Id'].iloc[0]

    # defPBIR = {
    # "version": "1.0",
    # "datasetReference": {
    #    "byPath": None,
    #    "byConnection": {
    #        "connectionString": None,
    #        "pbiServiceModelId": None,
    #        "pbiModelVirtualServerName": "sobe_wowvirtualserver",
    #        "pbiModelDatabaseName": datasetId,
    #        "name": "EntityDataSource",
    #        "connectionType": "pbiServiceXmlaStyleLive"
    #    }
    # }
    # }

    def conv_b64(file):

        loadJson = json.dumps(file)
        f = base64.b64encode(loadJson.encode("utf-8")).decode("utf-8")

        return f

    # definitionPBIR = conv_b64(defPBIR)
    payloadReportJson = conv_b64(report_json)

    request_body = {
        "displayName": report,
        "type": "Report",
        "definition": {
            "parts": [
                {
                    "path": "report.json",
                    "payload": payloadReportJson,
                    "payloadType": "InlineBase64",
                },
                {
                    "path": "definition.pbir",
                    "payload": rptDefFile,
                    "payloadType": "InlineBase64",
                },
            ]
        },
    }

    response = client.post(
        f"/v1/workspaces/{workspace_id}/reports/{reportId}/updateDefinition",
        json=request_body,
    )

    if response.status_code == 201:
        print(f"{icons.green_dot} The '{report}' report has been successfully updated.")
        # print(response.json())
    elif response.status_code == 202:
        operationId = response.headers["x-ms-operation-id"]
        response = client.get(f"/v1/operations/{operationId}")
        response_body = json.loads(response.content)
        while response_body["status"] != "Succeeded":
            time.sleep(3)
            response = client.get(f"/v1/operations/{operationId}")
            response_body = json.loads(response.content)
        response = client.get(f"/v1/operations/{operationId}/result")
        print(f"{icons.green_dot} The '{report}' report has been successfully updated.")
        # print(response.json())
