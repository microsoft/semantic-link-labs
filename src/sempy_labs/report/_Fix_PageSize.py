# Fix Page Size in Power BI Reports
# Changes pages with the default 720×1280 size to 1080×1920 (Full HD).

from uuid import UUID
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs.report._reportwrapper import connect_report

# ---------------------------------------------------------------------------
# Default (old) and target (new) page dimensions
# ---------------------------------------------------------------------------
_OLD_HEIGHT = 720
_OLD_WIDTH = 1280
_NEW_HEIGHT = 1080
_NEW_WIDTH = 1920


@log
def fix_page_size(
    report: str | UUID,
    page_name: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Changes report pages that use the default 720×1280 size to 1080×1920
    (Full HD).  Pages that already use a non-default size are left untouched.

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
        If True, only scans and reports pages with default size without
        changing them.

    Returns
    -------
    None
    """

    with connect_report(report=report, workspace=workspace, readonly=scan_only, show_diffs=False) as rw:
        # Guard: report must be in PBIR format
        if rw.format != "PBIR":
            print(
                f"{icons.red_dot} Report '{rw._report_name}' is in '{rw.format}' format, not PBIR. "
                f"Run 'Upgrade to PBIR' first."
            )
            return

        paths_df = rw.list_paths()
        pages_found = 0
        pages_default = 0
        pages_fixed = 0

        page_id = rw.resolve_page_name(page_name) if page_name else None

        for file_path in paths_df["Path"]:
            if not file_path.endswith("/page.json"):
                continue

            if page_id and f"/{page_id}/" not in file_path:
                continue

            page = rw.get(file_path=file_path)
            pages_found += 1

            h = page.get("height")
            w = page.get("width")
            display = page.get("displayName", file_path)

            if h == _OLD_HEIGHT and w == _OLD_WIDTH:
                pages_default += 1

                if scan_only:
                    print(
                        f"{icons.yellow_dot} '{display}' — default size "
                        f"{_OLD_WIDTH}×{_OLD_HEIGHT} (would be changed to "
                        f"{_NEW_WIDTH}×{_NEW_HEIGHT})"
                    )
                    continue

                page["height"] = _NEW_HEIGHT
                page["width"] = _NEW_WIDTH
                rw.update(file_path=file_path, payload=page)
                pages_fixed += 1
                print(
                    f"{icons.green_dot} '{display}' — changed from "
                    f"{_OLD_WIDTH}×{_OLD_HEIGHT} to {_NEW_WIDTH}×{_NEW_HEIGHT}"
                )
            else:
                if scan_only:
                    print(
                        f"{icons.green_dot} '{display}' — custom size "
                        f"{w}×{h} — no action needed"
                    )

        if pages_found == 0:
            print(
                f"{icons.info} No pages found in the '{rw._report_name}' report."
            )
        elif scan_only:
            if pages_default == 0:
                print(
                    f"\n{icons.green_dot} Scanned {pages_found} page(s) — "
                    f"none use the default {_OLD_WIDTH}×{_OLD_HEIGHT} size."
                )
            else:
                print(
                    f"\n{icons.yellow_dot} Scanned {pages_found} page(s) — "
                    f"{pages_default} use the default size and would be resized."
                )
        elif pages_fixed == 0:
            print(
                f"{icons.info} Found {pages_found} page(s) — none use the "
                f"default {_OLD_WIDTH}×{_OLD_HEIGHT} size."
            )
        else:
            print(
                f"{icons.green_dot} Successfully resized {pages_fixed} of "
                f"{pages_found} page(s) to {_NEW_WIDTH}×{_NEW_HEIGHT}."
            )


# Sample usage:
# fix_page_size(report="My Report")
# fix_page_size(report="My Report", page_name="PageName")
# fix_page_size(report="My Report", workspace="My Workspace", scan_only=True)
