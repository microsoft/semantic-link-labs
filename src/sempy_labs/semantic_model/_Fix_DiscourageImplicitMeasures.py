# Fix / Set DiscourageImplicitMeasures on a Semantic Model
# Based on: https://github.com/KornAlexander/PBI-Tools/.../2. Check Discourage Implicit Measures.csx

from uuid import UUID
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_dataset_from_report,
)
from sempy_labs.tom import connect_semantic_model


@log
def fix_discourage_implicit_measures(
    report: str | UUID,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Checks the DiscourageImplicitMeasures property on the semantic model that
    backs the given report.  If the property is False, sets it to True (unless
    running in scan-only mode).

    Disabling implicit measures is generally recommended and is required for
    calculation groups to work correctly.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the Power BI report whose semantic model will be checked.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    scan_only : bool, default=False
        If True, only reports the current state without making any changes.

    Returns
    -------
    None
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)

    dataset_id, dataset_name, dataset_workspace_id, dataset_workspace_name = (
        resolve_dataset_from_report(report=report, workspace=workspace_id)
    )

    with connect_semantic_model(
        dataset=dataset_id,
        readonly=scan_only,
        workspace=dataset_workspace_id,
    ) as tom:

        if tom.model.DiscourageImplicitMeasures:
            print(
                f"{icons.green_dot} DiscourageImplicitMeasures is already set to "
                f"True on '{dataset_name}' — no action needed."
            )
            return

        # Property is False
        if scan_only:
            print(
                f"{icons.yellow_dot} DiscourageImplicitMeasures is False on "
                f"'{dataset_name}'. It would be set to True."
            )
            return

        # Fix mode
        print(
            f"{icons.in_progress} Setting DiscourageImplicitMeasures to True on "
            f"'{dataset_name}'..."
        )
        tom.model.DiscourageImplicitMeasures = True
        print(
            f"{icons.green_dot} DiscourageImplicitMeasures has been set to True "
            f"on '{dataset_name}'."
        )


# Sample usage:
# fix_discourage_implicit_measures(report="My Report")
# fix_discourage_implicit_measures(report="My Report", workspace="My Workspace")
# fix_discourage_implicit_measures(report="My Report", scan_only=True)
