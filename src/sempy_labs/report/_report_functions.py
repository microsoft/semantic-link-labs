import sempy.fabric as fabric
import pandas as pd
import json
import os
import copy
from anytree import Node, RenderTree
from powerbiclient import Report
from pyspark.sql.functions import col, flatten
from sempy_labs.report._generate_report import update_report_from_reportjson
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
from sempy_labs._helper_functions import (
    resolve_report_id,
    language_validate,
    resolve_workspace_name_and_id,
    _decode_b64,
    resolve_dataset_id,
    _update_dataframe_datatypes,
    _base_api,
    _create_spark_session,
    _mount,
    resolve_workspace_id,
    resolve_item_name_and_id,
)
from typing import List, Optional, Union
from sempy._utils._log import log
import sempy_labs._icons as icons
from uuid import UUID


@log
def get_report_json(
    report: str,
    workspace: Optional[str | UUID] = None,
    save_to_file_name: Optional[str] = None,
) -> dict:
    """
    Gets the report.json file content of a Power BI report. This function only supports reports in the PBIR-Legacy format.

    This is a wrapper function for the following API: `Items - Get Report Definition <https://learn.microsoft.com/rest/api/fabric/report/items/get-report-definition>`_.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the Power BI report.
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
    (report_name, report_id) = resolve_item_name_and_id(
        item=report, type="Report", workspace=workspace_id
    )

    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/reports/{report_id}/getDefinition",
        method="post",
        lro_return_json=True,
        status_codes=None,
    )
    report_json = None
    for part in result.get("definition", {}).get("parts", {}):
        if part.get("path") == "report.json":
            payload = part.get("payload")
            report_file = _decode_b64(payload)
            report_json = json.loads(report_file)

    if not report_json:
        raise ValueError(
            f"{icons.red_dot} Unable to retrieve report.json for the '{report_name}' report within the '{workspace_name}' workspace. This function only supports reports in the PBIR-Legacy format."
        )

    if save_to_file_name is not None:
        if not lakehouse_attached():
            raise ValueError(
                f"{icons.red_dot} In order to save the report.json file, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook."
            )

        local_path = _mount()
        save_folder = f"{local_path}/Files"
        file_ext = ".json"
        if not save_to_file_name.endswith(file_ext):
            save_to_file_name = f"{save_to_file_name}{file_ext}"
        file_path = os.path.join(save_folder, save_to_file_name)
        with open(file_path, "w") as json_file:
            json.dump(report_json, json_file, indent=4)
        print(
            f"{icons.green_dot} The report.json file for the '{report}' report has been saved to the lakehouse attached to this notebook in this location: Files/'{save_to_file_name}'.\n\n"
        )

    return report_json


@log
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

    report_icon = "\U0001f4f6"
    dataset_icon = "\U0001f9ca"
    workspace_icon = "\U0001f465"

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
        target_workspace_id = resolve_workspace_id(workspace=target_workspace)

    if target_dataset is not None:
        if target_dataset_workspace is None:
            target_dataset_workspace = workspace_name
        target_dataset_id = resolve_dataset_id(target_dataset, target_dataset_workspace)

    if report == cloned_report and workspace_name == target_workspace:
        raise ValueError(
            f"{icons.warning} The 'report' and 'cloned_report' parameters have the same value of '{report}. The 'workspace' and 'target_workspace' have the same value of '{workspace_name}'. Either the 'cloned_report' or the 'target_workspace' must be different from the original report."
        )

    payload = {"name": cloned_report}
    if target_dataset is not None:
        payload["targetModelId"] = target_dataset_id
    if target_workspace != workspace_name:
        payload["targetWorkspaceId"] = target_workspace_id

    _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/reports/{reportId}/Clone",
        method="post",
        payload=payload,
    )
    print(
        f"{icons.green_dot} The '{report}' report has been successfully cloned as the '{cloned_report}' report within the '{target_workspace}' workspace."
    )


@log
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


@log
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

    column_map = {
        "Hidden": "bool",
        "Width": "int",
        "Height": "int",
        "Visual Count": "int",
    }

    _update_dataframe_datatypes(dataframe=df, column_map=column_map)

    return df


@log
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


@log
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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if isinstance(languages, str):
        languages = [languages]

    for lang in languages:
        language_validate(lang)

    reportJson = get_report_json(report=report, workspace=workspace_id)
    dfV = list_report_visuals(report=report, workspace=workspace_id)
    spark = _create_spark_session()
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
