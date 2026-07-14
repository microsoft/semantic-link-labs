# Fix "Add data category for columns" — standalone BPA fixer.
# Sets DataCategory on columns based on naming conventions.

from typing import Optional
from uuid import UUID
import re


def fix_data_category(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Sets appropriate DataCategory on columns based on naming conventions.

    Mapping: City → City, Country → Country, State/Province → StateOrProvince,
    Postal/Zip → PostalCode, Continent → Continent, Latitude → Latitude,
    Longitude → Longitude, URL/Web → WebUrl, Image → ImageUrl.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    scan_only : bool, default=False
        If True, only reports what would be fixed without making changes.
    """
    from sempy_labs.tom import connect_semantic_model

    _CATEGORY_MAP = [
        (r"\bcity\b", "City"),
        (r"\bcountry\b", "Country"),
        (r"\bstate\b|\bprovince\b", "StateOrProvince"),
        (r"\bpostal\s*code\b|\bzip\s*code\b|\bzip\b|\bplz\b", "PostalCode"),
        (r"\bcontinent\b", "Continent"),
        (r"\blatitude\b|\blat\b", "Latitude"),
        (r"\blongitude\b|\blon\b|\blng\b", "Longitude"),
        (r"\burl\b|\bweb\s*url\b|\bwebsite\b|\blink\b", "WebUrl"),
        (r"\bimage\s*url\b|\bimage\b|\bthumbnail\b|\bphoto\b|\bpicture\b", "ImageUrl"),
        (r"\baddress\b", "Address"),
        (r"\bcounty\b", "County"),
    ]

    fixed = 0
    with connect_semantic_model(dataset=dataset, readonly=scan_only, workspace=workspace) as tom:
        for table in tom.model.Tables:
            for col in table.Columns:
                # Skip if already has a data category
                current = str(col.DataCategory) if col.DataCategory else ""
                if current and current != "Uncategorized":
                    continue
                name_lower = col.Name.lower()
                matched_cat = None
                for pattern, category in _CATEGORY_MAP:
                    if re.search(pattern, name_lower, re.IGNORECASE):
                        matched_cat = category
                        break
                if matched_cat is None:
                    continue
                if scan_only:
                    print(f"  Would fix: '{table.Name}'[{col.Name}] → DataCategory={matched_cat}")
                else:
                    col.DataCategory = matched_cat
                    print(f"  Fixed: '{table.Name}'[{col.Name}] → DataCategory={matched_cat}")
                fixed += 1
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} column(s).")
    return fixed
