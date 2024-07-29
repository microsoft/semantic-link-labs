import sempy.fabric as fabric
import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _conv_b64,
    resolve_report_id,
)
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException


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

    dfI = fabric.list_items(workspace=workspace)

    dfI_model = dfI[(dfI["Display Name"] == dataset) & (dfI["Type"] == "SemanticModel")]

    if len(dfI_model) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{dataset}' semantic model does not exist in the '{workspace}' workspace."
        )

    datasetId = dfI_model["Id"].iloc[0]

    dfI_rpt = dfI[(dfI["Display Name"] == report) & (dfI["Type"] == "Report")]

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

    definitionPBIR = _conv_b64(defPBIR)
    payloadReportJson = _conv_b64(report_json)

    request_body = {
        "displayName": report,
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

    if theme_json is not None:
        theme_payload = _conv_b64(theme_json)
        theme_id = theme_json["payload"]["blob"]["displayName"]
        theme_path = f"StaticResources/SharedResources/BaseThemes/{theme_id}.json"

        part = {
            "path": theme_path,
            "payload": theme_payload,
            "payloadType": "InlineBase64",
        }
        request_body["definition"]["parts"].append(part)

    response = client.post(
        f"/v1/workspaces/{workspace_id}/reports", json=request_body, lro_wait=True
    )

    if response.status_code not in [200, 201]:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} Succesfully created the '{report}' report within the '{workspace}' workspace."
    )


def update_report_from_reportjson(
    report: str, report_json: dict, workspace: Optional[str] = None
):
    """
    Updates a report based on a report.json file.

    Parameters
    ----------
    report : str
        Name of the report.
    report_json : dict
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

    # Get the existing PBIR file
    response = client.post(
        f"/v1/workspaces/{workspace_id}/reports/{reportId}/getDefinition"
    )
    if response.status_code != 200:
        raise FabricHTTPException(response)
    df_items = pd.json_normalize(response.json()["definition"]["parts"])
    df_items_filt = df_items[df_items["path"] == "definition.pbir"]
    rptDefFile = df_items_filt["payload"].iloc[0]
    payloadReportJson = _conv_b64(report_json)

    request_body = {
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
        }
    }

    response = client.post(
        f"/v1/workspaces/{workspace_id}/reports/{reportId}/updateDefinition",
        json=request_body,
        lro_wait=True,
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{report}' report within the '{workspace}' workspace has been successfully updated."
    )


def get_report_definition(report: str, workspace: Optional[str] = None) -> pd.DataFrame:
    """
    Gets the collection of definition files of a report.

    Parameters
    ----------
    report : str
        Name of the report.
    workspace : str, default=None
        The Fabric workspace name in which the report resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        The collection of report definition files within a pandas dataframe.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    report_id = resolve_report_id(report=report, workspace=workspace)
    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/reports/{report_id}/getDefinition",
        lro_wait=True,
    )
    rdef = pd.json_normalize(response.json()["definition"]["parts"])

    return rdef


def create_model_bpa_report(
    report: Optional[str] = "BPAReport",
    dataset: Optional[str] = "BPAModel",
    dataset_workspace: Optional[str] = None,
):
    """
    Dynamically generates a Best Practice Analyzer report for analyzing semantic models.

    Parameters
    ----------
    report : str, default=None
        Name of the report.
        Defaults to 'BPAReport'.
    dataset : str, default='BPAModel'
        Name of the semantic model which feeds this report.
    dataset_workspace : str, default=None
        The Fabric workspace name in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    """

    import json
    import os

    # report_template_url = "https://raw.githubusercontent.com/m-kovalsky/Tabular/master/BPA/BPAReportTemplate.json"
    # response = requests.get(report_template_url)
    # if response.status_code != 200:
    #    raise FabricHTTPException(response)
    # report_json = json.loads(response.content)

    # Function to load JSON data

    dfI = fabric.list_items(workspace=dataset_workspace, type="SemanticModel")
    dfI_filt = dfI[dfI["Display Name"] == dataset]

    if len(dfI_filt) == 0:
        raise ValueError(
            f"The '{dataset}' semantic model does not exist within the '{dataset_workspace}' workspace."
        )

    dfR = fabric.list_reports(workspace=dataset_workspace)
    dfR_filt = dfR[dfR["Name"] == report]

    current_dir = os.path.dirname(__file__)
    json_file_path = os.path.join(current_dir, "_BPAReportTemplate.json")
    with open(json_file_path, "r") as file:
        report_json = json.load(file)

        if len(dfR_filt) > 0:
            update_report_from_reportjson(
                report=report, report_json=report_json, workspace=dataset_workspace
            )
        else:
            create_report_from_reportjson(
                report=report,
                dataset=dataset,
                report_json=report_json,
                workspace=dataset_workspace,
            )
