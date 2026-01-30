import sempy.fabric as fabric
import pandas as pd
import json
import os
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _conv_b64,
    resolve_dataset_name_and_id,
    resolve_item_name_and_id,
    _update_dataframe_datatypes,
    _base_api,
    resolve_item_id,
    _get_item_definition,
)
import sempy_labs._icons as icons
from sempy._utils._log import log
from uuid import UUID


@log
def create_report_from_reportjson(
    report: str,
    dataset: str | UUID,
    report_json: dict,
    theme_json: Optional[dict] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a report based on a report.json file (and an optional themes.json file).

    This is a wrapper function for the following API: `Items - Create Report <https://learn.microsoft.com/rest/api/fabric/report/items/create-report>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    report : str
        Name of the report.
    dataset : str | uuid.UUID
        Name or ID of the semantic model to connect to the report.
    report_json : dict
        The report.json file to be used to create the report.
    theme_json : dict, default=None
        The theme.json file to be used for the theme of the report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    dfI = fabric.list_items(workspace=workspace, type="Report")
    dfI_rpt = dfI[dfI["Display Name"] == report]

    if not dfI_rpt.empty:
        print(
            f"{icons.yellow_dot} '{report}' report already exists in the '{workspace_name}' workspace."
        )
        return

    defPBIR = {
        "version": "1.0",
        "datasetReference": {
            "byPath": None,
            "byConnection": {
                "connectionString": None,
                "pbiServiceModelId": None,
                "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                "pbiModelDatabaseName": dataset_id,
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

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/reports",
        method="post",
        payload=request_body,
        lro_return_status_code=True,
        status_codes=[201, 202],
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} Succesfully created the '{report}' report within the '{workspace_name}' workspace."
    )


@log
def update_report_from_reportjson(
    report: str | UUID, report_json: dict, workspace: Optional[str | UUID] = None
):
    """
    Updates a report based on a report.json file.

    This is a wrapper function for the following API: `Items - Update Report Definition <https://learn.microsoft.com/rest/api/fabric/report/items/update-report-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the report.
    report_json : dict
        The report.json file to be used to update the report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the report resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    report_id = resolve_item_id(item=report, type="Report", workspace=workspace)

    # Get the existing PBIR file
    df_items = get_report_definition(report=report, workspace=workspace_id)
    df_items_filt = df_items[df_items["path"] == "definition.pbir"]
    rptDefFile = df_items_filt["payload"].iloc[0]
    payloadReportJson = _conv_b64(report_json)

    payload = {
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

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/reports/{report_id}/updateDefinition",
        method="post",
        payload=payload,
        lro_return_status_code=True,
        status_codes=None,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{report}' report within the '{workspace_name}' workspace has been successfully updated."
    )


@log
def get_report_definition(
    report: str | UUID,
    workspace: Optional[str | UUID] = None,
    return_dataframe: bool = True,
) -> pd.DataFrame | dict:
    """
    Gets the collection of definition files of a report.

    This is a wrapper function for the following API: `Items - Get Report Definition <https://learn.microsoft.com/rest/api/fabric/report/items/get-report-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the report resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    return_dataframe : bool, default=True
        If True, returns a dataframe. If False, returns a json dictionary.

    Returns
    -------
    pandas.DataFrame
        The collection of report definition files within a pandas dataframe.
    """

    return _get_item_definition(
        item=report,
        type="Report",
        workspace=workspace,
        return_dataframe=return_dataframe,
    )


@log
def create_model_bpa_report(
    report: Optional[str] = icons.model_bpa_name,
    dataset: Optional[str] = icons.model_bpa_name,
    dataset_workspace: Optional[str | UUID] = None,
):
    """
    Dynamically generates a Best Practice Analyzer report for analyzing semantic models.

    Parameters
    ----------
    report : str, default='ModelBPA'
        Name of the report.
        Defaults to 'ModelBPA'.
    dataset : str, default='ModelBPA'
        Name of the semantic model which feeds this report.
        Defaults to 'ModelBPA'
    dataset_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    """
    (dataset_workspace_name, dataset_workspace_id) = resolve_workspace_name_and_id(
        dataset_workspace
    )

    dfI = fabric.list_items(workspace=dataset_workspace_id, type="SemanticModel")
    dfI_filt = dfI[dfI["Display Name"] == dataset]

    if len(dfI_filt) == 0:
        raise ValueError(
            f"The '{dataset}' semantic model does not exist within the '{dataset_workspace_name}' workspace."
        )

    dfR = fabric.list_reports(workspace=dataset_workspace_id)
    dfR_filt = dfR[dfR["Name"] == report]
    # dataset_id = resolve_dataset_id(dataset=dataset, workspace=dataset_workspace)

    current_dir = os.path.dirname(__file__)
    # directory_path = os.path.join(current_dir, "_bpareporttemplate")
    # len_dir_path = len(directory_path) + 1

    # request_body = {"displayName": report, "definition": {"parts": []}}

    # def get_all_file_paths(directory):
    #    file_paths = []

    #    for root, directories, files in os.walk(directory):
    #        for filename in files:
    #            full_path = os.path.join(root, filename)
    #            file_paths.append(full_path)

    #    return file_paths

    # all_files = get_all_file_paths(directory_path)

    # for file_path in all_files:
    #    fp = file_path[len_dir_path:]
    #    with open(file_path, "r") as file:
    #        json_file = json.load(file)
    #        if fp == 'definition.pbir':
    #            conn_string = f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{dataset_workspace};Initial Catalog={dataset};Integrated Security=ClaimsToken"
    #            json_file['datasetReference']['byConnection']['connectionString'] = conn_string
    #            json_file['datasetReference']['byConnection']['pbiModelDatabaseName'] = dataset_id
    #        part = {
    #            "path": fp,
    #            "payload": _conv_b64(json_file),
    #            "payloadType": "InlineBase64",
    #        }

    #    request_body["definition"]["parts"].append(part)

    # _create_report(
    #    report=report,
    #    request_body=request_body,
    #    dataset=dataset,
    #    report_workspace=dataset_workspace,
    #    dataset_workspace=dataset_workspace,
    # )

    json_file_path = os.path.join(current_dir, "_BPAReportTemplate.json")
    with open(json_file_path, "r") as file:
        report_json = json.load(file)

    if len(dfR_filt) > 0:
        update_report_from_reportjson(
            report=report, report_json=report_json, workspace=dataset_workspace_id
        )
    else:
        create_report_from_reportjson(
            report=report,
            dataset=dataset,
            report_json=report_json,
            workspace=dataset_workspace_id,
        )


def _create_report(
    report: str,
    request_body: dict,
    dataset: str,
    dataset_workspace: Optional[str] = None,
    report_workspace: Optional[str] = None,
    overwrite: bool = False,
):

    from sempy_labs.report import report_rebind

    (report_workspace_name, report_workspace_id) = resolve_workspace_name_and_id(
        workspace=report_workspace
    )

    dfR = fabric.list_reports(workspace=report_workspace)
    dfR_filt = dfR[dfR["Name"] == report]

    updated_report = False
    # Create report if it does not exist
    if dfR_filt.empty:
        _base_api(
            request=f"/v1/workspaces/{report_workspace_id}/reports",
            method="post",
            payload=request_body,
            lro_return_status_code=True,
            status_codes=[201, 202],
        )

        print(
            f"{icons.green_dot} The '{report}' report has been created within the '{report_workspace_name}'"
        )
        updated_report = True
    # Update the report if it exists
    elif not dfR_filt.empty and overwrite:
        report_id = dfR_filt["Id"].iloc[0]
        _base_api(
            request=f"/v1/workspaces/{report_workspace_id}/reports/{report_id}/updateDefinition",
            method="post",
            payload=request_body,
            lro_return_status_code=True,
            status_codes=None,
        )
        print(
            f"{icons.green_dot} The '{report}' report has been updated within the '{report_workspace_name}'"
        )
        updated_report = True
    else:
        raise ValueError(
            f"{icons.red_dot} The '{report}' report within the '{report_workspace_name}' workspace already exists and the 'overwrite' parameter was set to False."
        )

    # Rebind the report to the semantic model to make sure it is pointed at the correct semantic model
    if updated_report:
        report_rebind(
            report=report,
            dataset=dataset,
            report_workspace=report_workspace,
            dataset_workspace=dataset_workspace,
        )


def _get_report(
    report: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (report_name, report_id) = resolve_item_name_and_id(
        item=report, type="Report", workspace=workspace
    )

    response = _base_api(
        request=f"v1.0/myorg/groups/{workspace_id}/reports/{report_id}"
    )
    result = response.json()

    new_data = {
        "Id": result.get("id"),
        "Report Type": result.get("reportType"),
        "Name": result.get("name"),
        "Web Url": result.get("webUrl"),
        "Embed Url": result.get("embedUrl"),
        "Is From Pbix": result.get("isFromPbix"),
        "Is Owned By Me": result.get("isOwnedByMe"),
        "Dataset Id": result.get("datasetId"),
        "Dataset Workspace Id": result.get("datasetWorkspaceId"),
        "Users": result.get("users") if result.get("users") is not None else [],
        "Subscriptions": (
            result.get("subscriptions")
            if result.get("subscriptions") is not None
            else []
        ),
    }

    df = pd.DataFrame([new_data])

    column_map = {
        "Is From Pbix": "bool",
        "Is Owned By Me": "bool",
    }
    _update_dataframe_datatypes(dataframe=df, column_map=column_map)

    return df
