# Remove Unused Custom Visuals from Power BI Reports
# Removes any custom visuals registered in the report that are not used by any visual.

from uuid import UUID
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs.report._reportwrapper import connect_report


@log
def fix_remove_unused_custom_visuals(
    report: str | UUID,
    page_name: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Removes custom visuals that are registered in the report but not used
    by any visual on any page.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the report.
    page_name : str, default=None
        Ignored (custom visuals are report-level). Kept for API consistency.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    scan_only : bool, default=False
        If True, only scans and reports unused custom visuals without removing them.

    Returns
    -------
    None
    """

    with connect_report(
        report=report, workspace=workspace, readonly=scan_only, show_diffs=False
    ) as rw:
        if rw.format != "PBIR":
            print(
                f"{icons.red_dot} Report '{rw._report_name}' is in '{rw.format}' format, not PBIR. "
                f"Run 'Upgrade to PBIR' first."
            )
            return

        dfCV = rw.list_custom_visuals()
        unused = dfCV[dfCV["Used in Report"] == False]

        if unused.empty:
            print(f"No unused custom visuals found. 0 changes needed.")
            return

        for _, row in unused.iterrows():
            display_name = row.get("Custom Visual Display Name", row.get("Custom Visual Name", ""))
            if scan_only:
                print(f"[SCAN] Unused custom visual: {display_name}")
            else:
                print(f"Removing unused custom visual: {display_name}")

        if not scan_only:
            rw.remove_unnecessary_custom_visuals()
            print(f"{icons.green_dot} Removed {len(unused)} unused custom visual(s).")
        else:
            print(f"{len(unused)} unused custom visual(s) found.")
