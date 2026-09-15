# Hide Visual-Level Filters in Power BI Reports
# Sets isHiddenInViewMode = true on every visual-level filter.
# If a visual has query fields but no filterConfig, one is created automatically.

from uuid import UUID
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs.report._reportwrapper import connect_report


def _extract_fields_from_query(visual: dict) -> list:
    """
    Walk visual.query.queryState and return a list of
    (field_dict, filter_type) tuples for every projected field.

    Column fields → "Categorical"
    Measure fields → "Advanced"
    Everything else → "Categorical"
    """
    fields = []
    query_state = visual.get("visual", {}).get("query", {}).get("queryState", {})

    for _role, role_def in query_state.items():
        for proj in role_def.get("projections", []):
            field = proj.get("field")
            if field is None:
                continue

            if "Measure" in field:
                fields.append((field, "Advanced"))
            else:
                # Column, Aggregation, HierarchyLevel, etc. → Categorical
                fields.append((field, "Categorical"))

    return fields


@log
def fix_hide_visual_filters(
    report: str | UUID,
    page_name: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Hides visual-level filters from the end-user filter pane by setting
    ``isHiddenInViewMode`` to ``True`` on every filter in every visual.

    For visuals that have query fields but no ``filterConfig``, one is
    constructed automatically from the visual's query projections.

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
        If True, only scans and reports which visuals have visible filters
        without making any changes.

    Returns
    -------
    None
    """

    with connect_report(
        report=report, workspace=workspace, readonly=scan_only, show_diffs=False
    ) as rw:
        # Guard: report must be in PBIR format
        if rw.format != "PBIR":
            print(
                f"{icons.red_dot} Report '{rw._report_name}' is in '{rw.format}' format, not PBIR. "
                f"Run 'Upgrade to PBIR' first."
            )
            return

        paths_df = rw.list_paths()
        visuals_checked = 0
        visuals_need_fixing = 0
        visuals_fixed = 0

        page_id = rw.resolve_page_name(page_name) if page_name else None

        for file_path in paths_df["Path"]:
            if not file_path.endswith("/visual.json"):
                continue

            if page_id and f"/{page_id}/" not in file_path:
                continue

            visual = rw.get(file_path=file_path)

            # Skip visuals without a query (text boxes, shapes, images, etc.)
            query_state = (
                visual.get("visual", {}).get("query", {}).get("queryState", {})
            )
            if not query_state:
                continue

            visuals_checked += 1

            filter_cfg = visual.get("filterConfig")
            filters = filter_cfg.get("filters", []) if filter_cfg else []

            if filters:
                # filterConfig exists — check if any filter is NOT hidden
                visible = [f for f in filters if not f.get("isHiddenInViewMode", False)]
                if not visible:
                    if scan_only:
                        print(
                            f"{icons.green_dot} {file_path} — all filters already hidden"
                        )
                    continue

                visuals_need_fixing += 1

                if scan_only:
                    print(
                        f"{icons.yellow_dot} {file_path} — "
                        f"{len(visible)} of {len(filters)} filter(s) visible"
                    )
                    continue

                # Set isHiddenInViewMode on each filter
                for f in filters:
                    f["isHiddenInViewMode"] = True

            else:
                # No filterConfig — build it from query fields
                extracted = _extract_fields_from_query(visual)
                if not extracted:
                    continue

                visuals_need_fixing += 1

                if scan_only:
                    print(
                        f"{icons.yellow_dot} {file_path} — no filterConfig; "
                        f"{len(extracted)} field(s) would be added as hidden filters"
                    )
                    continue

                new_filters = []
                for field_dict, filter_type in extracted:
                    new_filters.append(
                        {
                            "name": "",
                            "field": field_dict,
                            "type": filter_type,
                            "isHiddenInViewMode": True,
                        }
                    )

                visual["filterConfig"] = {"filters": new_filters}

            rw.update(file_path=file_path, payload=visual)
            visuals_fixed += 1
            print(f"{icons.green_dot} {file_path} — filters hidden")

        if visuals_checked == 0:
            print(
                f"{icons.info} No visuals with query fields found in the "
                f"'{rw._report_name}' report."
            )
        elif scan_only:
            if visuals_need_fixing == 0:
                print(
                    f"\n{icons.green_dot} Scanned {visuals_checked} visual(s) — "
                    f"all filters already hidden."
                )
            else:
                print(
                    f"\n{icons.yellow_dot} Scanned {visuals_checked} visual(s) — "
                    f"{visuals_need_fixing} need filter hiding."
                )
        elif visuals_fixed == 0:
            print(
                f"{icons.info} Found {visuals_checked} visual(s) — all filters "
                f"already hidden."
            )
        else:
            print(
                f"{icons.green_dot} Successfully hidden filters on "
                f"{visuals_fixed} of {visuals_checked} visual(s)."
            )


# Sample usage:
# fix_hide_visual_filters(report="My Report")
# fix_hide_visual_filters(report="My Report", page_name="PageName")
# fix_hide_visual_filters(report="My Report", workspace="My Workspace", scan_only=True)
