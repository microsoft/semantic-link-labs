from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    _base_api,
)
from typing import Optional, Literal
import sempy_labs._icons as icons
from sempy._utils._log import log
from uuid import UUID


@log
def set_endorsement(
    report: str | UUID,
    endorsement: Literal["None", "Promoted", "Certified", "Master data"],
    workspace: Optional[str | UUID] = None,
):
    """
    Sets the endorsement status for a Power BI report.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the Power BI report.
    endorsement : typing.Literal["None", "Promoted", "Certified", "Master data"]
        The endorsement status to set for the report.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (report_name, report_id) = resolve_item_name_and_id(
        item=report, type="Report", workspace=workspace_id
    )

    endorsement = endorsement.strip().lower()

    endorsement_mapping = {
        "none": 0,
        "promoted": 1,
        "certified": 2,
        "master data": 3,
    }

    if endorsement not in endorsement_mapping:
        raise ValueError(
            "Endorsement must be either 'None', 'Promoted', 'Certified', or 'Master data'."
        )
    stage = endorsement_mapping.get(endorsement)
    payload = {"stage": stage}

    _base_api(
        request=f"metadata/gallery/reports/{report_id}",
        client="internal",
        method="put",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The endorsement for the '{report_name}' report within the '{workspace_name}' workspace has been set to '{endorsement}'."
    )
