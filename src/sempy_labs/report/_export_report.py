import sempy.fabric as fabric
import json
import os
import time
from sempy_labs._helper_functions import (
    generate_embedded_filter,
    resolve_workspace_name_and_id,
    _base_api,
    _mount,
)
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from uuid import UUID


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
    lakehouse: Optional[str | UUID] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
):
    """
    Exports a Power BI report to a file in your lakehouse.

    This is a wrapper function for the following APIs: `Reports - Export To File In Group <https://learn.microsoft.com/rest/api/power-bi/reports/export-to-file-in-group>`_, `Reports - Get Export To File Status In Group <https://learn.microsoft.com/rest/api/power-bi/reports/get-export-to-file-status-in-group>`_, `Reports - Get File Of Export To File In Group <https://learn.microsoft.com/rest/api/power-bi/reports/get-file-of-export-to-file-in-group>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID. This will be the lakehouse to which the export of the report is saved.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

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

    valid_formats = {
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
    file_ext = valid_formats.get(export_format)
    if file_ext is None:
        raise ValueError(
            f"{icons.red_dot} The '{export_format}' format is not a valid format for exporting Power BI reports. Please enter a valid format. Options: {valid_formats}"
        )

    if file_name is None:
        file_name = f"{report}{file_ext}"
    else:
        file_name = f"{file_name}{file_ext}"

    dfI = fabric.list_items(workspace=workspace)
    dfI_filt = dfI[
        (dfI["Type"].isin(["Report", "PaginatedReport"]))
        & (dfI["Display Name"] == report)
    ]

    if dfI_filt.empty:
        raise ValueError(
            f"{icons.red_dot} The '{report}' report does not exist in the '{workspace_name}' workspace."
        )

    report_type = dfI_filt["Type"].iloc[0]

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

    if report_type == "Report" and export_format in paginatedOnly:
        raise ValueError(
            f"{icons.red_dot} The '{export_format}' format is only supported for paginated reports."
        )

    if report_type == "PaginatedReport" and export_format in pbiOnly:
        raise ValueError(
            f"{icons.red_dot} The '{export_format}' format is only supported for Power BI reports."
        )

    if report_type == "PaginatedReport" and (
        bookmark_name is not None or page_name is not None or visual_name is not None
    ):
        raise ValueError(
            f"{icons.red_dot} Export for paginated reports does not support bookmarks/pages/visuals. Those parameters must not be set for paginated reports."
        )

    reportId = dfI_filt["Id"].iloc[0]

    if (
        export_format in ["BMP", "EMF", "GIF", "JPEG", "TIFF"]
        and report_type == "PaginatedReport"
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
        if report_type == "Report":
            request_body = {
                "format": export_format,
                "powerBIReportConfiguration": {
                    "defaultBookmark": {"name": bookmark_name}
                },
            }
    elif page_name is not None and visual_name is None:
        if report_type == "Report":
            request_body = {"format": export_format, "powerBIReportConfiguration": {}}

            request_body["powerBIReportConfiguration"]["pages"] = []
            for page in page_name:
                page_dict = {"pageName": page}
                request_body["powerBIReportConfiguration"]["pages"].append(page_dict)

    elif page_name is not None and visual_name is not None:
        if len(page_name) != len(visual_name):
            raise ValueError(
                f"{icons.red_dot} Each 'visual_name' must map to a single 'page_name'."
            )

        if report_type == "Report":
            request_body = {"format": export_format, "powerBIReportConfiguration": {}}

            request_body["powerBIReportConfiguration"]["pages"] = []
            a = 0
            for page in page_name:
                visual = visual_name[a]
                page_dict = {"pageName": page, "visualName": visual}
                request_body["powerBIReportConfiguration"]["pages"].append(page_dict)
                a += 1

    # Transform and add report filter if it is specified
    if report_filter is not None and report_type == "Report":
        reportFilter = generate_embedded_filter(filter=report_filter)
        report_level_filter = {"filter": reportFilter}

        if "powerBIReportConfiguration" not in request_body:
            request_body["powerBIReportConfiguration"] = {}
        request_body["powerBIReportConfiguration"]["reportLevelFilters"] = [
            report_level_filter
        ]

    base_url = f"/v1.0/myorg/groups/{workspace_id}/reports/{reportId}"
    response = _base_api(
        request=f"{base_url}/ExportTo",
        method="post",
        payload=request_body,
        status_codes=202,
        client="fabric_sp",
    )
    export_id = json.loads(response.content).get("id")

    get_status_url = f"{base_url}/exports/{export_id}"
    response = _base_api(
        request=get_status_url, status_codes=[200, 202], client="fabric_sp"
    )
    response_body = json.loads(response.content)
    while response_body["status"] not in ["Succeeded", "Failed"]:
        time.sleep(3)
        response = _base_api(
            request=get_status_url, status_codes=[200, 202], client="fabric_sp"
        )
        response_body = json.loads(response.content)
    if response_body["status"] == "Failed":
        raise ValueError(
            f"{icons.red_dot} The export for the '{report}' report within the '{workspace_name}' workspace in the '{export_format}' format has failed."
        )
    else:
        response = _base_api(request=f"{get_status_url}/file", client="fabric_sp")
        print(
            f"{icons.in_progress} Saving the '{export_format}' export for the '{report}' report within the '{workspace_name}' workspace to the lakehouse..."
        )

        local_path = _mount(lakehouse=lakehouse, workspace=lakehouse_workspace)
        folder_path = f"{local_path}/Files"
        file_path = os.path.join(folder_path, file_name)

        with open(file_path, "wb") as export_file:
            export_file.write(response.content)
        print(
            f"{icons.green_dot} The '{export_format}' export for the '{report}' report within the '{workspace_name}' workspace has been saved '{file_name}' in the '{lakehouse}' within the '{lakehouse_workspace}' workspace."
        )
