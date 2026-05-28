# Migrate Report-Level Measures to Semantic Model
# Moves report-level measures from the report into the underlying semantic model.

from uuid import UUID
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs.report._reportwrapper import connect_report


@log
def fix_migrate_report_level_measures(
    report: str | UUID,
    page_name: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Migrates report-level measures from the report into the semantic model.
    It is a best practice to keep measures defined in the semantic model,
    not in the report.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the report.
    page_name : str, default=None
        Ignored (report-level measures are report-wide). Kept for API consistency.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    scan_only : bool, default=False
        If True, only scans and reports which report-level measures exist
        without migrating them.

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

        dfRLM = rw.list_report_level_measures()

        if dfRLM.empty:
            print("No report-level measures found. 0 changes needed.")
            return

        for _, row in dfRLM.iterrows():
            measure_name = row.get("Measure Name", "")
            table_name = row.get("Table Name", "")
            if scan_only:
                print(f"[SCAN] Report-level measure: [{table_name}].[{measure_name}]")
            else:
                print(f"Migrating report-level measure: [{table_name}].[{measure_name}]")

    if not scan_only:
        # Re-open in write mode to actually migrate
        with connect_report(
            report=report, workspace=workspace, readonly=False, show_diffs=False
        ) as rw:
            rw.migrate_report_level_measures()
            print(f"{icons.green_dot} Migrated {len(dfRLM)} report-level measure(s) to the semantic model.")
    else:
        print(f"{len(dfRLM)} report-level measure(s) found.")
