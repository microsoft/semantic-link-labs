# Fix: Migrate native Slicer visuals into the Slicer Bar custom visual
# %pip install semantic-link-labs

import json
import uuid
import copy
from uuid import UUID
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs.report._reportwrapper import connect_report


def _generate_slicerbar_item_id(item_type: str = "SLICER_BAR_SLICER") -> str:
    """Generate a unique item ID in the slicerbar format."""
    part1 = uuid.uuid4().hex[:8]
    part2 = uuid.uuid4().hex[:16]
    return f"{item_type}-{part1}-{part2}"


def _build_slicerbar_slicer_item(field_property: str) -> dict:
    """Build a default SLICER_BAR_SLICER item for a given field."""
    return {
        "itemId": _generate_slicerbar_item_id("SLICER_BAR_SLICER"),
        "type": "SLICER_BAR_SLICER",
        "filterDimension": field_property,
        "label": field_property,
        "isAutoCreated": True,
        "name": field_property,
        "tooltip": "",
        "withMultipleSelect": True,
        "onLoaddefaultSelection": [],
        "styling": {
            "alignItem": "center",
            "justifyContent": "center",
            "background": {
                "color": {"hover": None, "active": None, "deactivated": None, "default": None}
            },
            "height": "50",
            "font": {
                "color": {"hover": None, "active": None, "deactivated": None, "default": None},
                "fontFamily": None,
                "fontWeight": None,
                "fontWeightNumber": None,
                "fontStyle": None,
                "fontSize": None,
                "fontSizeNumber": None,
            },
            "iconSettings": {
                "iconName": "",
                "iconSize": "Medium",
                "iconPosition": "left",
                "iconColor": None,
            },
            "border": {
                "color": None,
                "style": None,
                "width": None,
                "radiusTopLeft": None,
                "radiusTopRight": None,
                "radiusBottomLeft": None,
                "radiusBottomRight": None,
            },
            "shadow": {
                "color": None,
                "xValue": None,
                "yValue": None,
                "blur": None,
                "spread": None,
            },
        },
    }


def _extract_slicer_field(slicer_visual: dict) -> dict | None:
    """Extract the field projection from a native slicer visual's query."""
    projections = (
        slicer_visual.get("visual", {})
        .get("query", {})
        .get("queryState", {})
        .get("Values", {})
        .get("projections", [])
    )
    if projections:
        return projections[0]
    return None


def _get_field_property(projection: dict) -> str | None:
    """Get the Property name from a field projection."""
    return projection.get("field", {}).get("Column", {}).get("Property")


def _get_field_entity(projection: dict) -> str | None:
    """Get the Entity name from a field projection."""
    return (
        projection.get("field", {})
        .get("Column", {})
        .get("Expression", {})
        .get("SourceRef", {})
        .get("Entity")
    )


@log
def fix_migrate_slicer_to_slicerbar(
    report: str | UUID,
    page_name: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Migrates native slicer visuals on a page into the existing Slicer Bar custom visual.

    For each native slicer found on the specified page, this function:
    1. Extracts the slicer's field (Entity + Property)
    2. Adds a new projection to the Slicer Bar's query
    3. Adds a new item entry to the Slicer Bar's saveObject configuration
    4. Deletes the original native slicer visual

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the report.
    page_name : str
        The display name of the page to apply changes to.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    scan_only : bool, default=False
        If True, only scans and reports slicers found without migrating them.

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

        page_id = rw.resolve_page_name(page_name)
        paths_df = rw.list_paths()

        # --- Step 1: Find the slicerbar visual on this page ---
        slicerbar_path = None
        slicerbar_visual = None

        for file_path in paths_df["Path"]:
            if not file_path.endswith("/visual.json"):
                continue
            if f"/{page_id}/" not in file_path:
                continue

            visual = rw.get(file_path=file_path)
            visual_type = visual.get("visual", {}).get("visualType", "")

            if visual_type.startswith("slicerbar"):
                slicerbar_path = file_path
                slicerbar_visual = visual
                break

        if slicerbar_visual is None:
            print(
                f"{icons.red_dot} No Slicer Bar visual found on page '{page_name}'. "
                f"Add a Slicer Bar custom visual to the page first."
            )
            return

        print(f"{icons.info} Found Slicer Bar visual: {slicerbar_visual.get('name')}")

        # --- Step 2: Find all native slicers on this page ---
        slicers = []  # list of (file_path, visual_dict)

        for file_path in paths_df["Path"]:
            if not file_path.endswith("/visual.json"):
                continue
            if f"/{page_id}/" not in file_path:
                continue

            visual = rw.get(file_path=file_path)
            if visual.get("visual", {}).get("visualType") == "slicer":
                slicers.append((file_path, visual))

        if not slicers:
            print(f"{icons.info} No native slicer visuals found on page '{page_name}'.")
            return

        # --- Step 3: Parse the slicerbar's saveObject ---
        save_object_str = (
            slicerbar_visual.get("visual", {})
            .get("objects", {})
            .get("internal", [{}])[0]
            .get("properties", {})
            .get("saveObject", {})
            .get("expr", {})
            .get("Literal", {})
            .get("Value")
        )

        if not save_object_str:
            print(f"{icons.red_dot} Could not read Slicer Bar saveObject configuration.")
            return

        # saveObject value is wrapped in single quotes
        save_object = json.loads(save_object_str.strip("'"))

        # Get existing category projections
        category_projections = (
            slicerbar_visual.get("visual", {})
            .get("query", {})
            .get("queryState", {})
            .get("category", {})
            .get("projections", [])
        )

        # Track existing filterDimensions to avoid duplicates
        existing_dimensions = {item.get("filterDimension") for item in save_object.get("items", [])}

        slicers_migrated = 0
        slicers_skipped = 0
        slicer_paths_to_delete = []

        for slicer_path, slicer_visual in slicers:
            projection = _extract_slicer_field(slicer_visual)
            if projection is None:
                print(f"{icons.yellow_dot} Slicer '{slicer_visual.get('name')}' has no field — skipping.")
                slicers_skipped += 1
                continue

            field_property = _get_field_property(projection)
            field_entity = _get_field_entity(projection)

            if not field_property or not field_entity:
                print(f"{icons.yellow_dot} Slicer '{slicer_visual.get('name')}' has incomplete field — skipping.")
                slicers_skipped += 1
                continue

            if field_property in existing_dimensions:
                if scan_only:
                    print(
                        f"{icons.yellow_dot} Slicer '{slicer_visual.get('name')}' "
                        f"({field_entity}.{field_property}) — already in Slicer Bar, would delete slicer only."
                    )
                else:
                    print(
                        f"{icons.yellow_dot} '{field_entity}.{field_property}' already in Slicer Bar — "
                        f"deleting duplicate slicer only."
                    )
                slicer_paths_to_delete.append(slicer_path)
                slicers_migrated += 1
                continue

            if scan_only:
                print(
                    f"{icons.yellow_dot} Slicer '{slicer_visual.get('name')}' "
                    f"({field_entity}.{field_property}) — would be migrated to Slicer Bar."
                )
                slicers_migrated += 1
                continue

            # --- Add projection to slicerbar query ---
            new_projection = {
                "field": {
                    "Column": {
                        "Expression": {"SourceRef": {"Entity": field_entity}},
                        "Property": field_property,
                    }
                },
                "queryRef": f"{field_entity}.{field_property}",
                "nativeQueryRef": field_property,
            }
            category_projections.append(new_projection)

            # --- Add item to saveObject ---
            new_item = _build_slicerbar_slicer_item(field_property)
            save_object["items"].append(new_item)
            existing_dimensions.add(field_property)

            slicer_paths_to_delete.append(slicer_path)
            slicers_migrated += 1
            print(
                f"{icons.green_dot} Migrated '{field_entity}.{field_property}' to Slicer Bar."
            )

        if scan_only:
            print(
                f"\n{icons.yellow_dot} Scan complete — {slicers_migrated} slicer(s) would be migrated, "
                f"{slicers_skipped} skipped."
            )
            return

        # --- Step 4: Write back the updated slicerbar ---
        # Update saveObject JSON string (re-wrap in single quotes)
        updated_save_object_str = f"'{json.dumps(save_object, separators=(',', ':'))}'"
        slicerbar_visual["visual"]["objects"]["internal"][0]["properties"]["saveObject"]["expr"]["Literal"]["Value"] = updated_save_object_str

        # Update projections
        slicerbar_visual["visual"]["query"]["queryState"]["category"]["projections"] = category_projections

        rw.update(file_path=slicerbar_path, payload=slicerbar_visual)

        # --- Step 5: Delete the native slicer visuals ---
        for slicer_path in slicer_paths_to_delete:
            rw.remove(file_path=slicer_path, verbose=False)
            print(f"{icons.green_dot} Deleted slicer visual: {slicer_path}")

        print(
            f"\n{icons.green_dot} Migration complete — {slicers_migrated} slicer(s) migrated, "
            f"{slicers_skipped} skipped."
        )
        

# Sample usage:
# fix_migrate_slicer_to_slicerbar(report="ReportName", page_name="Page 1")
# fix_migrate_slicer_to_slicerbar(report="ReportName", page_name="Page 1", scan_only=True)
# fix_migrate_slicer_to_slicerbar(report="ReportName", page_name="Page 1", workspace="Your Workspace")
