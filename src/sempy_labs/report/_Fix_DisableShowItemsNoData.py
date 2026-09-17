# Disable 'Show Items With No Data' in Power BI Reports
# Removes the showAll property from all visuals, which disables the
# "Show items with no data" setting that can cause performance issues.

from uuid import UUID
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs.report._reportwrapper import connect_report


@log
def fix_disable_show_items_no_data(
    report: str | UUID,
    page_name: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Disables the 'Show items with no data' property on all visuals in the
    report. This setting can lead to performance degradation, especially
    against large semantic models.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the report.
    page_name : str, default=None
        The display name of the page to apply changes to.
        Defaults to None which applies changes to all pages.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    scan_only : bool, default=False
        If True, only scans and reports which visuals have showAll enabled
        without making any changes.

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

        # List visuals and check which have "Show Items With No Data" enabled
        dfV = rw.list_visuals()
        if "Show Items With No Data" not in dfV.columns:
            print("No visuals with 'Show items with no data' found. 0 changes needed.")
            return

        affected = dfV[dfV["Show Items With No Data"] == True]

        if page_name:
            affected = affected[
                (affected["Page Display Name"] == page_name)
                | (affected["Page Name"] == page_name)
            ]

        if affected.empty:
            print("No visuals with 'Show items with no data' found. 0 changes needed.")
            return

        for _, row in affected.iterrows():
            page_disp = row.get("Page Display Name", "")
            visual_name = row.get("Visual Name", "")
            if scan_only:
                print(f"[SCAN] [{page_disp}] Visual '{visual_name}' has 'Show items with no data' enabled")
            else:
                print(f"[{page_disp}] Disabling 'Show items with no data' on '{visual_name}'")

        if not scan_only:
            rw.disable_show_items_with_no_data()
            print(f"{icons.green_dot} Disabled 'Show items with no data' on {len(affected)} visual(s).")
        else:
            print(f"{len(affected)} visual(s) with 'Show items with no data' enabled.")
