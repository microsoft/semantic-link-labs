import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    _base_api,
    _mount,
    resolve_workspace_name,
)
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
import sempy.fabric as fabric  # <--- FIXED: Added this missing import
from uuid import UUID
from sempy._utils._log import log

@log
def download_report(
    report: str | UUID,
    file_name: Optional[str] = None,
    download_type: str = "LiveConnect",
    workspace: Optional[str | UUID] = None,
):
    """
    Downloads the specified report from the specified workspace to a file in the attached lakehouse.
    Supports both Power BI Reports (.pbix) and Paginated Reports (.rdl).

    This is a wrapper function for the following API: `Reports - Export Report In Group <https://learn.microsoft.com/rest/api/power-bi/reports/export-report-in-group>`_.

    Service Principal Authentication is supported.

    Parameters
    ----------
    report: str | uuid.UUID
        Name or ID of the report.
    file_name : str, default=None
        Name of the file to be saved (without extension).
        Defaults to None which resolves to the name of the report.
    download_type : str, default="LiveConnect"
        The type of download (Only applies to Power BI .pbix reports). 
        Valid values are "LiveConnect" and "IncludeModel".
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if not lakehouse_attached():
        raise ValueError(
            f"{icons.red_dot} A lakehouse must be attached to the notebook."
        )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id()
    lakehouse_workspace = resolve_workspace_name()

    # --- NEW: Identify Report Type (PBIX vs RDL) ---
    dfI = fabric.list_items(workspace=workspace_id)
    
    # Filter by Name OR ID
    # Check if input is a valid UUID string
    if isinstance(report, UUID) or (isinstance(report, str) and len(report) == 36 and "-" in report):
         dfI_filt = dfI[dfI["Id"] == str(report)]
    else:
         dfI_filt = dfI[dfI["Display Name"] == report]

    # Filter for valid report types
    dfI_filt = dfI_filt[dfI_filt["Type"].isin(["Report", "PaginatedReport"])]

    if dfI_filt.empty:
        raise ValueError(
            f"{icons.red_dot} The report '{report}' was not found in workspace '{workspace_name}' or is not a supported type."
        )

    report_type = dfI_filt["Type"].iloc[0]
    report_id = dfI_filt["Id"].iloc[0]
    report_name = dfI_filt["Display Name"].iloc[0]

    # --- NEW: Branch Logic based on Type ---
    
    if report_type == "PaginatedReport":
        # RDL Logic
        extension = ".rdl"
        api_suffix = "" # RDLs do not support downloadType
        # Ignore download_type parameter validation for RDLs
    else:
        # PBIX Logic (Existing)
        extension = ".pbix"
        
        download_types = ["LiveConnect", "IncludeModel"]
        if download_type not in download_types:
            raise ValueError(
                f"{icons.red_dot} Invalid download_type parameter. Valid options: {download_types}."
            )
        api_suffix = f"?downloadType={download_type}"

    # Handle Filename
    if file_name is None:
        file_name = f"{report_name}{extension}"
    elif not file_name.endswith(extension):
        file_name = f"{file_name}{extension}"

    # --- Execute Download ---
    request_url = f"v1.0/myorg/groups/{workspace_id}/reports/{report_id}/Export{api_suffix}"
    
    response = _base_api(
        request=request_url,
        client="fabric_sp",
    )

    if response.status_code != 200:
         raise RuntimeError(
             f"{icons.red_dot} Download failed. Status: {response.status_code}, Msg: {response.text}"
         )

    # Save file to the attached lakehouse
    local_path = _mount()
    save_file = f"{local_path}/Files/{file_name}"
    
    with open(save_file, "wb") as file:
        file.write(response.content)

    print(
        f"{icons.green_dot} The '{report_name}' ({report_type}) has been saved as '{file_name}' in the '{lakehouse_name}' lakehouse."
    )
