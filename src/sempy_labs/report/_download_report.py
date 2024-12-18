import sempy.fabric as fabric
import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_lakehouse_name,
)
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID


def download_report(
    report: str,
    file_name: Optional[str] = None,
    download_type: str = "LiveConnect",
    workspace: Optional[str | UUID] = None,
):
    """
    Downloads the specified report from the specified workspace to a Power BI .pbix file.

    This is a wrapper function for the following API: `Reports - Export Report In Group <https://learn.microsoft.com/rest/api/power-bi/reports/export-report-in-group>`.

    Parameters
    ----------
    report: str
        Name of the report.
    file_name : str, default=None
        Name of the .pbix file to be saved.
        Defaults to None which resolves to the name of the report.
    download_type : str, default="LiveConnect"
        The type of download. Valid values are "LiveConnect" and "IncludeModel".
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
    lakehouse_id = fabric.get_lakehouse_id()
    lakehouse_workspace = fabric.resolve_workspace_name()
    lakehouse_name = resolve_lakehouse_name(
        lakehouse_id=lakehouse_id, workspace=lakehouse_workspace
    )

    download_types = ["LiveConnect", "IncludeModel"]
    if download_type not in download_types:
        raise ValueError(
            f"{icons.red_dot} Invalid download_type parameter. Valid options: {download_types}."
        )

    file_name = file_name or report
    report_id = fabric.resolve_item_id(
        item_name=report, type="Report", workspace=workspace_id
    )

    client = fabric.PowerBIRestClient()
    response = client.get(
        f"/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/Export?downloadType={download_type}"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    # Save file to the attached lakehouse
    with open(f"/lakehouse/default/Files/{file_name}.pbix", "wb") as file:
        file.write(response.content)

    print(
        f"{icons.green_dot} The '{report}' report within the '{workspace_name}' workspace has been exported as the '{file_name}' file in the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace}' workspace."
    )
