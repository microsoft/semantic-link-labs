import sempy_labs._icons as icons
from typing import Optional, Literal
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    _base_api,
    _mount,
    resolve_workspace_name,
    _is_valid_uuid,
)
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
from uuid import UUID
from sempy._utils._log import log
import sempy.fabric as fabric


@log
def download_report(
    report: str | UUID,
    file_name: Optional[str] = None,
    download_type: Literal["LiveConnect", "IncludeModel"] = "LiveConnect",
    workspace: Optional[str | UUID] = None,
):
    """
    Downloads the specified report from the specified workspace to a Power BI .pbix file.

    This is a wrapper function for the following API: `Reports - Export Report In Group <https://learn.microsoft.com/rest/api/power-bi/reports/export-report-in-group>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    report: str | uuid.UUID
        Name or ID of the Power BI report or Paginated Report.
    file_name : str, default=None
        Name of the .pbix or .rdl file to be saved. Do not include the file extension.
        Defaults to None which resolves to the name of the report.
    download_type : typing.Literal["LiveConnect", "IncludeModel"], default="LiveConnect"
        The type of download. Valid values are "LiveConnect" and "IncludeModel". This parameter is ignored for Paginated Reports.
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

    dfI = fabric.list_items(workspace=workspace_id)
    dfI_filt = dfI[dfI["Type"].isin(["Report", "PaginatedReport"])]
    if _is_valid_uuid(report):
        df = dfI_filt[dfI_filt["Id"] == str(report)]
    else:
        df = dfI_filt[dfI_filt["Display Name"] == report]

    if df.empty:
        raise ValueError(
            f"{icons.red_dot} The report '{report}' was not found in the '{workspace_name}' workspace."
        )

    report_name = df["Display Name"].iloc[0]
    report_id = df["Id"].iloc[0]
    report_type = df["Type"].iloc[0]

    download_types = ["LiveConnect", "IncludeModel"]
    if download_type not in download_types and report_type != "PaginatedReport":
        raise ValueError(
            f"{icons.red_dot} Invalid download_type parameter. Valid options: {download_types}."
        )

    file_name = file_name or report_name

    base_url = f"v1.0/myorg/groups/{workspace_id}/reports/{report_id}/Export"
    extension = ".pbix"
    if report_type == "PaginatedReport":
        response = _base_api(
            request=base_url,
            client="fabric_sp",
        )
        extension = ".rdl"
    else:
        response = _base_api(
            request=f"{base_url}?downloadType={download_type}",
            client="fabric_sp",
        )

    # Save file to the attached lakehouse
    local_path = _mount()
    save_file = f"{local_path}/Files/{file_name}{extension}"
    with open(save_file, "wb") as file:
        file.write(response.content)

    print(
        f"{icons.green_dot} The '{report_name}' {report_type} within the '{workspace_name}' workspace has been exported as the '{file_name}' file in the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace}' workspace."
    )
