import sempy.fabric as fabric
import pandas as pd
import json
import os
import time
import copy
from anytree import Node, RenderTree
from powerbiclient import Report
from pyspark.sql.functions import col, flatten
from sempy_labs.report._generate_report import update_report_from_reportjson
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
from sempy_labs._helper_functions import (
    generate_embedded_filter,
    resolve_report_id,
    resolve_lakehouse_name,
    language_validate,
    resolve_workspace_name_and_id,
    lro,
    _decode_b64,
    resolve_dataset_id,
)
from typing import List, Optional, Union
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID


def get_report_json(
    report: str,
    workspace: Optional[str | UUID] = None,
    save_to_file_name: Optional[str] = None,
) -> dict:
    """
    Gets the report.json file content of a Power BI report.

    This is a wrapper function for the following API: `Items - Get Report Definition <https://learn.microsoft.com/rest/api/fabric/report/items/get-report-definition>`_.

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the report exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    save_to_file_name : str, default=None
        Specifying this parameter will save the report.json file to the lakehouse attached to the notebook with the file name of this parameter.

    Returns
    -------
    dict
        The report.json file for a given Power BI report.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    report_id = resolve_report_id(report=report, workspace=workspace_id)
    fmt = "PBIR-Legacy"

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/reports/{report_id}/getDefinition?format={fmt}"
    )

    result = lro(client, response).json()
    df_items = pd.json_normalize(result["definition"]["parts"])
    df_items_filt = df_items[df_items["path"] == "report.json"]
    payload = df_items_filt["payload"].iloc[0]
    report_file = _decode_b64(payload)
    report_json = json.loads(report_file)

    if save_to_file_name is not None:
        if not lakehouse_attached():
            raise ValueError(
                f"{icons.red_dot} In order to save the report.json file, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook."
            )

        lakehouse_id = fabric.get_lakehouse_id()
        lake_workspace = fabric.resolve_workspace_name()
        lakehouse = resolve_lakehouse_name(lakehouse_id, lake_workspace)
        folderPath = "/lakehouse/default/Files"
        fileExt = ".json"
        if not save_to_file_name.endswith(fileExt):
            save_to_file_name = f"{save_to_file_name}{fileExt}"
        filePath = os.path.join(folderPath, save_to_file_name)
        with open(filePath, "w") as json_file:
            json.dump(report_json, json_file, indent=4)
        print(
            f"{icons.green_dot} The report.json file for the '{report}' report has been saved to the '{lakehouse}' in this location: '{filePath}'.\n\n"
        )

    return report_json


def report_dependency_tree(workspace: Optional[str | UUID] = None):
    """
    Prints a dependency between reports and semantic models.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfR = fabric.list_reports(workspace=workspace_id)
    dfD = fabric.list_datasets(workspace=workspace_id)
    dfR = pd.merge(
        dfR,
        dfD[["Dataset ID", "Dataset Name"]],
        left_on="Dataset Id",
        right_on="Dataset ID",
        how="left",
    )
    dfR.rename(columns={"Name": "Report Name"}, inplace=True)
    dfR = dfR[["Report Name", "Dataset Name"]]

    report_icon = "\U0001F4F6"
    dataset_icon = "\U0001F9CA"
    workspace_icon = "\U0001F465"

    node_dict = {}
    rootNode = Node(workspace_name)
    node_dict[workspace_name] = rootNode
    rootNode.custom_property = f"{workspace_icon} "

    for _, r in dfR.iterrows():
        datasetName = r["Dataset Name"]
        reportName = r["Report Name"]
        parentNode = node_dict.get(datasetName)
        if parentNode is None:
            parentNode = Node(datasetName, parent=rootNode)
            node_dict[datasetName] = parentNode
        parentNode.custom_property = f"{dataset_icon} "

        child_node = Node(reportName, parent=parentNode)
        child_node.custom_property = f"{report_icon} "

    # Print the tree structure
    for pre, _, node in RenderTree(node_dict[workspace_name]):
        print(f"{pre}{node.custom_property}'{node.name}'")


@log
def export_report(
    report: str,
    export_format: str,
    file_name: Optional[str] = None,
    bookmark_name: Optional[str] = None,
    page_name: Optional[str] = None,
    visual_name: Optional[str] = None,
    report_filter: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Exports a Power BI report to a file in your lakehouse.

    This is a wrapper function for the following APIs: `Reports - Export To File In Group <https://learn.microsoft.com/rest/api/power-bi/reports/export-to-file-in-group>`_, `Reports - Get Export To File Status In Group <https://learn.microsoft.com/rest/api/power-bi/reports/get-export-to-file-status-in-group>`_, `Reports - Get File Of Export To File In Group <https://learn.microsoft.com/rest/api/power-bi/reports/get-file-of-export-to-file-in-group>`_.

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    export_format : str
        The format in which to export the report. For image formats, enter the file extension in this parameter, not 'IMAGE'.
        `Valid formats <https://learn.microsoft.com/rest/api/power-bi/reports/export-to-file-in-group#fileformat>`_
    file_name : str, default=None
        The name of the file to be saved within the lakehouse. Do not include the file extension. Defaults ot the reportName parameter value.
    bookmark_name : str, default=None
        The name (GUID) of a bookmark within the report.
    page_name : str, default=None
        The name (GUID) of the report page.
    visual_name : str, default=None
        The name (GUID) of a visual. If you specify this parameter you must also specify the page_name parameter.
    report_filter : str, default=None
        A report filter to be applied when exporting the report. Syntax is user-friendly. See above for examples.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if not lakehouse_attached():
        raise ValueError(
            f"{icons.red_dot} In order to run the 'export_report' function, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook."
        )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if isinstance(page_name, str):
        page_name = [page_name]
    if isinstance(visual_name, str):
        visual_name = [visual_name]

    if bookmark_name is not None and (page_name is not None or visual_name is not None):
        raise ValueError(
            f"{icons.red_dot} If the 'bookmark_name' parameter is set, the 'page_name' and 'visual_name' parameters must not be set."
        )

    if visual_name is not None and page_name is None:
        raise ValueError(
            f"{icons.red_dot} If the 'visual_name' parameter is set, the 'page_name' parameter must be set."
        )

    validFormats = {
        "ACCESSIBLEPDF": ".pdf",
        "CSV": ".csv",
        "DOCX": ".docx",
        "MHTML": ".mhtml",
        "PDF": ".pdf",
        "PNG": ".png",
        "PPTX": ".pptx",
        "XLSX": ".xlsx",
        "XML": ".xml",
        "BMP": ".bmp",
        "EMF": ".emf",
        "GIF": ".gif",
        "JPEG": ".jpeg",
        "TIFF": ".tiff",
    }

    export_format = export_format.upper()
    fileExt = validFormats.get(export_format)
    if fileExt is None:
        raise ValueError(
            f"{icons.red_dot} The '{export_format}' format is not a valid format for exporting Power BI reports. Please enter a valid format. Options: {validFormats}"
        )

    if file_name is None:
        file_name = f"{report}{fileExt}"
    else:
        file_name = f"{file_name}{fileExt}"

    folderPath = "/lakehouse/default/Files"
    filePath = os.path.join(folderPath, file_name)

    dfI = fabric.list_items(workspace=workspace_id)
    dfI_filt = dfI[
        (dfI["Type"].isin(["Report", "PaginatedReport"]))
        & (dfI["Display Name"] == report)
    ]

    if len(dfI_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{report}' report does not exist in the '{workspace_name}' workspace."
        )

    reportType = dfI_filt["Type"].iloc[0]

    # Limitations
    pbiOnly = ["PNG"]
    paginatedOnly = [
        "ACCESSIBLEPDF",
        "CSV",
        "DOCX",
        "BMP",
        "EMF",
        "GIF",
        "JPEG",
        "TIFF",
        "MHTML",
        "XLSX",
        "XML",
    ]

    if reportType == "Report" and export_format in paginatedOnly:
        raise ValueError(
            f"{icons.red_dot} The '{export_format}' format is only supported for paginated reports."
        )

    if reportType == "PaginatedReport" and export_format in pbiOnly:
        raise ValueError(
            f"{icons.red_dot} The '{export_format}' format is only supported for Power BI reports."
        )

    if reportType == "PaginatedReport" and (
        bookmark_name is not None or page_name is not None or visual_name is not None
    ):
        raise ValueError(
            f"{icons.red_dot} Export for paginated reports does not support bookmarks/pages/visuals. Those parameters must not be set for paginated reports."
        )

    reportId = dfI_filt["Id"].iloc[0]
    client = fabric.PowerBIRestClient()

    if (
        export_format in ["BMP", "EMF", "GIF", "JPEG", "TIFF"]
        and reportType == "PaginatedReport"
    ):
        request_body = {
            "format": "IMAGE",
            "paginatedReportConfiguration": {
                "formatSettings": {"OutputFormat": export_format.lower()}
            },
        }
    elif bookmark_name is None and page_name is None and visual_name is None:
        request_body = {"format": export_format}
    elif bookmark_name is not None:
        if reportType == "Report":
            request_body = {
                "format": export_format,
                "powerBIReportConfiguration": {
                    "defaultBookmark": {"name": bookmark_name}
                },
            }
    elif page_name is not None and visual_name is None:
        if reportType == "Report":
            request_body = {"format": export_format, "powerBIReportConfiguration": {}}

            request_body["powerBIReportConfiguration"]["pages"] = []
            dfPage = list_report_pages(report=report, workspace=workspace_id)

            for page in page_name:
                dfPage_filt = dfPage[dfPage["Page ID"] == page]
                if len(dfPage_filt) == 0:
                    raise ValueError(
                        f"{icons.red_dot} The '{page}' page does not exist in the '{report}' report within the '{workspace_name}' workspace."
                    )

                page_dict = {"pageName": page}
                request_body["powerBIReportConfiguration"]["pages"].append(page_dict)

    elif page_name is not None and visual_name is not None:
        if len(page_name) != len(visual_name):
            raise ValueError(
                f"{icons.red_dot} Each 'visual_name' must map to a single 'page_name'."
            )

        if reportType == "Report":
            request_body = {"format": export_format, "powerBIReportConfiguration": {}}

            request_body["powerBIReportConfiguration"]["pages"] = []
            dfVisual = list_report_visuals(report=report, workspace=workspace_id)
            a = 0
            for page in page_name:
                visual = visual_name[a]

                dfVisual_filt = dfVisual[
                    (dfVisual["Page ID"] == page) & (dfVisual["Visual ID"] == visual)
                ]
                if len(dfVisual_filt) == 0:
                    raise ValueError(
                        f"{icons.red_dot} The '{visual}' visual does not exist on the '{page}' in the '{report}' report within the '{workspace_name}' workspace."
                    )

                page_dict = {"pageName": page, "visualName": visual}
                request_body["powerBIReportConfiguration"]["pages"].append(page_dict)
                a += 1

    # Transform and add report filter if it is specified
    if report_filter is not None and reportType == "Report":
        reportFilter = generate_embedded_filter(filter=report_filter)
        report_level_filter = {"filter": reportFilter}

        if "powerBIReportConfiguration" not in request_body:
            request_body["powerBIReportConfiguration"] = {}
        request_body["powerBIReportConfiguration"]["reportLevelFilters"] = [
            report_level_filter
        ]

    base_url = f"/v1.0/myorg/groups/{workspace_id}/reports/{reportId}"
    response = client.post(f"{base_url}/ExportTo", json=request_body)

    if response.status_code == 202:
        response_body = json.loads(response.content)
        export_id = response_body["id"]
        response = client.get(f"{base_url}/exports/{export_id}")
        response_body = json.loads(response.content)
        while response_body["status"] not in ["Succeeded", "Failed"]:
            time.sleep(3)
            response = client.get(f"{base_url}/exports/{export_id}")
            response_body = json.loads(response.content)
        if response_body["status"] == "Failed":
            raise ValueError(
                f"{icons.red_dot} The export for the '{report}' report within the '{workspace_name}' workspace in the '{export_format}' format has failed."
            )
        else:
            response = client.get(f"{base_url}/exports/{export_id}/file")
            print(
                f"{icons.in_progress} Saving the '{export_format}' export for the '{report}' report within the '{workspace_name}' workspace to the lakehouse..."
            )
            with open(filePath, "wb") as export_file:
                export_file.write(response.content)
            print(
                f"{icons.green_dot} The '{export_format}' export for the '{report}' report within the '{workspace_name}' workspace has been saved to the following location: '{filePath}'."
            )


def clone_report(
    report: str,
    cloned_report: str,
    workspace: Optional[str | UUID] = None,
    target_workspace: Optional[str] = None,
    target_dataset: Optional[str] = None,
    target_dataset_workspace: Optional[str] = None,
):
    """
    Clones a Power BI report.

    This is a wrapper function for the following API: `Reports - Clone Report In Group <https://learn.microsoft.com/rest/api/power-bi/reports/clone-report-in-group>`_.

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    cloned_report : str
        Name of the new Power BI report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    target_workspace : str, default=None
        The name of the Fabric workspace to place the cloned report.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    target_dataset : str, default=None
        The name of the semantic model to be used by the cloned report.
        Defaults to None which resolves to the semantic model used by the initial report.
    target_dataset_workspace : str, default=None
        The workspace name in which the semantic model to be used by the report resides.
        Defaults to None which resolves to the semantic model used by the initial report.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfI = fabric.list_items(workspace=workspace_id, type="Report")
    dfI_filt = dfI[(dfI["Display Name"] == report)]

    if len(dfI_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{report}' report does not exist within the '{workspace_name}' workspace."
        )

    reportId = resolve_report_id(report, workspace_id)

    if target_workspace is None:
        target_workspace = workspace_name
        target_workspace_id = workspace_id
    else:
        target_workspace_id = fabric.resolve_workspace_id(target_workspace)

    if target_dataset is not None:
        if target_dataset_workspace is None:
            target_dataset_workspace = workspace_name
        target_dataset_id = resolve_dataset_id(target_dataset, target_dataset_workspace)

    if report == cloned_report and workspace_name == target_workspace:
        raise ValueError(
            f"{icons.warning} The 'report' and 'cloned_report' parameters have the same value of '{report}. The 'workspace' and 'target_workspace' have the same value of '{workspace_name}'. Either the 'cloned_report' or the 'target_workspace' must be different from the original report."
        )

    client = fabric.PowerBIRestClient()

    request_body = {"name": cloned_report}
    if target_dataset is not None:
        request_body["targetModelId"] = target_dataset_id
    if target_workspace != workspace_name:
        request_body["targetWorkspaceId"] = target_workspace_id

    response = client.post(
        f"/v1.0/myorg/groups/{workspace_id}/reports/{reportId}/Clone", json=request_body
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{report}' report has been successfully cloned as the '{cloned_report}' report within the '{target_workspace}' workspace."
    )


def launch_report(report: str, workspace: Optional[str | UUID] = None):
    """
    Shows a Power BI report within a Fabric notebook.

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        An embedded Power BI report within the notebook.
    """

    from sempy_labs import resolve_report_id

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    report_id = resolve_report_id(report, workspace_id)
    report = Report(group_id=workspace_id, report_id=report_id)

    return report


def list_report_pages(report: str, workspace: Optional[str | UUID] = None):
    """
    Shows the properties of all pages within a Power BI report.

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the pages within a Power BI report and their properties.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = pd.DataFrame(
        columns=["Page ID", "Page Name", "Hidden", "Width", "Height", "Visual Count"]
    )

    reportJson = get_report_json(report=report, workspace=workspace_id)

    for section in reportJson["sections"]:
        pageID = section.get("name")
        pageName = section.get("displayName")
        # pageFilters = section['filters']
        pageWidth = section.get("width")
        pageHeight = section.get("height")
        visualCount = len(section["visualContainers"])
        pageHidden = False
        pageConfig = section.get("config")
        pageConfigJson = json.loads(pageConfig)

        try:
            pageH = pageConfigJson["visibility"]
            if pageH == 1:
                pageHidden = True
        except Exception:
            pass

        new_data = {
            "Page ID": pageID,
            "Page Name": pageName,
            "Hidden": pageHidden,
            "Width": pageWidth,
            "Height": pageHeight,
            "Visual Count": visualCount,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df["Hidden"] = df["Hidden"].astype(bool)
    intCol = ["Width", "Height", "Visual Count"]
    df[intCol] = df[intCol].astype(int)

    return df


def list_report_visuals(report: str, workspace: Optional[str | UUID] = None):
    """
    Shows the properties of all visuals within a Power BI report.

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the visuals within a Power BI report and their properties.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    reportJson = get_report_json(report=report, workspace=workspace_id)

    df = pd.DataFrame(columns=["Page Name", "Page ID", "Visual ID", "Title"])

    for section in reportJson["sections"]:
        pageID = section["name"]
        pageName = section["displayName"]

        for visual in section["visualContainers"]:
            visualConfig = visual["config"]
            visualConfigJson = json.loads(visualConfig)
            visualID = visualConfigJson["name"]

            try:
                title = visualConfigJson["singleVisual"]["vcObjects"]["title"][0][
                    "properties"
                ]["text"]["expr"]["Literal"]["Value"]
                title = title[1:-1]
            except Exception:
                title = ""

            new_data = {
                "Page Name": pageName,
                "Page ID": pageID,
                "Visual ID": visualID,
                "Title": title,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_report_bookmarks(report: str, workspace: Optional[str | UUID] = None):
    """
    Shows the properties of all bookmarks within a Power BI report.

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the bookmarks within a Power BI report and their properties.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = pd.DataFrame(
        columns=[
            "Bookmark ID",
            "Bookmark Name",
            "Page ID",
            "Visual ID",
            "Visual Hidden",
        ]
    )

    reportJson = get_report_json(report=report, workspace=workspace_id)
    reportConfig = reportJson["config"]
    reportConfigJson = json.loads(reportConfig)

    try:
        for bookmark in reportConfigJson["bookmarks"]:
            bID = bookmark["name"]
            bName = bookmark["displayName"]
            rptPageId = bookmark["explorationState"]["activeSection"]

            for rptPg in bookmark["explorationState"]["sections"]:
                for vc in bookmark["explorationState"]["sections"][rptPg][
                    "visualContainers"
                ]:
                    vHidden = False
                    try:
                        hidden = bookmark["explorationState"]["sections"][rptPg][
                            "visualContainers"
                        ][vc]["singleVisual"]["display"]["mode"]
                        if hidden == "hidden":
                            vHidden = True
                    except Exception:
                        pass

            new_data = {
                "Bookmark ID": bID,
                "Bookmark Name": bName,
                "Page ID": rptPageId,
                "Visual ID": vc,
                "Visual Hidden": vHidden,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        listPages = list_report_pages(report=report, workspace=workspace_id)

        df = pd.merge(df, listPages[["Page ID", "Page Name"]], on="Page ID", how="left")
        df = df[
            [
                "Bookmark ID",
                "Bookmark Name",
                "Page ID",
                "Page Name",
                "Visual ID",
                "Visual Hidden",
            ]
        ]

        return df

    except Exception:
        print(
            f"The '{report}' report within the '{workspace_name}' workspace has no bookmarks."
        )


@log
def translate_report_titles(
    report: str,
    languages: Union[str, List[str]],
    workspace: Optional[str | UUID] = None,
):
    """
    Dynamically generates new Power BI reports which have report titles translated into the specified language(s).

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    languages : str, List[str]
        The language code(s) in which to translate the report titles.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """
    from synapse.ml.services import Translate
    from pyspark.sql import SparkSession

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if isinstance(languages, str):
        languages = [languages]

    for lang in languages:
        language_validate(lang)

    reportJson = get_report_json(report=report, workspace=workspace_id)
    dfV = list_report_visuals(report=report, workspace=workspace_id)
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(dfV)
    columnToTranslate = "Title"

    translate = (
        Translate()
        .setTextCol(columnToTranslate)
        .setToLanguage(languages)
        .setOutputCol("translation")
        .setConcurrency(5)
    )

    transDF = (
        translate.transform(df)
        .withColumn("translation", flatten(col("translation.translations")))
        .withColumn("translation", col("translation.text"))
        .select("Visual ID", columnToTranslate, "translation")
    )

    df_panda = transDF.toPandas()

    i = 0
    for lang in languages:
        # Clone report
        language = language_validate(lang)
        clonedReportName = f"{report}_{language}"

        dfRep = fabric.list_reports(workspace=workspace_id)
        dfRep_filt = dfRep[
            (dfRep["Name"] == clonedReportName)
            & (dfRep["Report Type"] == "PowerBIReport")
        ]

        if len(dfRep_filt) > 0:
            print(
                f"{icons.yellow_dot} The '{clonedReportName}' report already exists in the '{workspace_name} workspace."
            )
        else:
            clone_report(
                report=report, cloned_report=clonedReportName, workspace=workspace_id
            )
            print(
                f"{icons.green_dot} The '{clonedReportName}' report has been created via clone in the '{workspace_name} workspace."
            )

        rptJsonTr = copy.deepcopy(reportJson)

        # Update report json file
        for section in rptJsonTr["sections"]:
            for visual in section["visualContainers"]:
                visualConfig = visual["config"]
                visualConfigJson = json.loads(visualConfig)
                visualID = visualConfigJson["name"]

                df_filt = df_panda[
                    (df_panda["Visual ID"] == visualID) & (df_panda["Title"] != "")
                ]

                if len(df_filt) == 1:
                    tr = df_filt["translation"].str[i].iloc[0]
                    if len(tr) > 0:
                        prop = visualConfigJson["singleVisual"]["vcObjects"]["title"][
                            0
                        ]["properties"]["text"]["expr"]["Literal"]
                        prop["Value"] = f"'{tr}'"

                visual["config"] = json.dumps(visualConfigJson)

        i += 1

        # Post updated report json file to cloned report
        update_report_from_reportjson(
            report=clonedReportName, report_json=rptJsonTr, workspace=workspace_id
        )
        print(
            f"{icons.green_dot} The visual titles within the '{clonedReportName}' report within the '{workspace_name}' have been translated into '{language}' accordingly."
        )
