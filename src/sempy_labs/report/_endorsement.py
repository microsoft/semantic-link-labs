import requests
from sempy_labs._helper_functions import (
    _get_url_prefix,
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
)
from typing import Optional, Literal
import sempy_labs._icons as icons
from sempy._utils._log import log
from uuid import UUID
from sempy.fabric.exceptions import FabricHTTPException


@log
def set_endorsement(
    report: str | UUID,
    endorsement: Literal["None", "Promoted"],
    workspace: Optional[str | UUID] = None,
):
    """
    Sets the endorsement status for a Power BI report.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the Power BI report.
    endorsement : Literal["None", "Promoted"]
        The endorsement status to set for the report.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (report_name, report_id) = resolve_dataset_name_and_id(report, workspace_id)

    import notebookutils

    token = notebookutils.credentials.getToken("pbi")
    headers = {"Authorization": f"Bearer {token}"}
    prefix = _get_url_prefix()
    endorsement = endorsement.strip().lower()

    endorsement_mapping = {
        "none": 0,
        "promoted": 1,
    }

    if endorsement not in endorsement_mapping:
        raise ValueError("Endorsement must be either 'None' or 'Promoted'.")

    stage = endorsement_mapping.get(endorsement)
    payload = {"stage": stage}

    response = requests.put(
        url=f"{prefix}/metadata/gallery/reports/{report_id}",
        headers=headers,
        json=payload,
    )

    if response.status_code != 200:
        raise FabricHTTPException(f"Failed to set endorsement: {response.text}")

    print(
        f"{icons.green_dot} The endorsement for the '{report_name}' report within the '{workspace_name}' workspace has been set to '{endorsement}'."
    )
