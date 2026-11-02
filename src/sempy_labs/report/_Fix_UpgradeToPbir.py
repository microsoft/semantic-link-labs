# Upgrade Report to PBIR Format
# Converts PBIRLegacy reports to PBIR format using the Fabric REST API
# (getDefinition → updateDefinition round-trip).

from uuid import UUID
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    _base_api,
)
from sempy._utils._log import log
import sempy_labs._icons as icons
import time

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_POLL_TIME_LIMIT = 60  # seconds to poll for server-side format conversion
_TIME_BETWEEN_REQUESTS = 3  # seconds between status checks


# ---------------------------------------------------------------------------
# Helper: Poll the reports API until the format flips to PBIR
# ---------------------------------------------------------------------------
def _check_upgrade_status(
    url: str, report_id: str, workspace_name: str,
    start_time: float,
) -> bool:
    """
    Polls ``GET /v1.0/myorg/groups/{ws}/reports`` until the report
    shows ``format == "PBIR"`` or the time limit is exceeded.

    Returns True if PBIR was verified, False otherwise.
    """

    poll_count = 0
    verified_name = None

    while time.time() - start_time < _POLL_TIME_LIMIT:
        poll_count += 1
        elapsed = int(time.time() - start_time)
        print(
            f"{icons.in_progress} Poll #{poll_count} — "
            f"{elapsed}s / {_POLL_TIME_LIMIT}s elapsed..."
        )

        response = _base_api(request=url, client="fabric_sp")

        for rpt in response.json().get("value", []):
            if rpt.get("id") == report_id:
                if rpt.get("format") == "PBIR":
                    verified_name = rpt.get("name")
                break

        if verified_name:
            break

        time.sleep(_TIME_BETWEEN_REQUESTS)

    elapsed = int(time.time() - start_time)
    if verified_name:
        print(
            f"{icons.green_dot} The '{verified_name}' report in the "
            f"'{workspace_name}' workspace has been upgraded to PBIR format "
            f"({elapsed}s)."
        )
        return True
    else:
        print(
            f"{icons.warning} The report in the "
            f"'{workspace_name}' workspace could not be verified as PBIR "
            f"within {_POLL_TIME_LIMIT}s.  It may still be processing — "
            f"please check the workspace manually."
        )
        return False


# ---------------------------------------------------------------------------
# Main fixer function
# ---------------------------------------------------------------------------
@log
def fix_upgrade_to_pbir(
    report: str | UUID,
    page_name: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> bool:
    """
    Upgrades a report from PBIRLegacy format to PBIR format.

    Uses a pure REST approach: retrieves the report definition via the
    Fabric Items API (``getDefinition``) and pushes it back via
    ``updateDefinition``.  The server-side processing converts the
    definition to the workspace's current format (PBIR).

    In scan mode the function only reports the current format.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the report.
    page_name : str, default=None
        Unused — accepted for interface consistency with other report fixers.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    scan_only : bool, default=False
        If True, only reports the current format without upgrading.

    Returns
    -------
    bool
        True if the report is (or was upgraded to) PBIR format, False otherwise.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    rpt_name, rpt_id = resolve_item_name_and_id(
        item=report, type="Report", workspace=workspace_id
    )

    # Get report metadata to check current format
    reports_url = f"/v1.0/myorg/groups/{workspace_id}/reports"
    response = _base_api(request=reports_url, client="fabric_sp")

    rpt_format = None
    for rpt in response.json().get("value", []):
        if rpt.get("id") == str(rpt_id):
            rpt_format = rpt.get("format")
            break

    if rpt_format is None:
        print(
            f"{icons.red_dot} Could not find report '{rpt_name}' in the "
            f"'{workspace_name}' workspace."
        )
        return False

    # Already PBIR
    if rpt_format == "PBIR":
        print(
            f"{icons.green_dot} Report '{rpt_name}' is already in PBIR format "
            f"— no upgrade needed."
        )
        return True

    # Not PBIRLegacy — cannot upgrade
    if rpt_format != "PBIRLegacy":
        print(
            f"{icons.red_dot} Report '{rpt_name}' is in '{rpt_format}' format. "
            f"Only PBIRLegacy reports can be upgraded to PBIR."
        )
        return False

    # PBIRLegacy → eligible for upgrade
    if scan_only:
        # Quick check visual count via getDefinition
        try:
            result = _base_api(
                request=f"/v1/workspaces/{workspace_id}/reports/{rpt_id}/getDefinition",
                method="post",
                status_codes=None,
                lro_return_json=True,
                client="fabric_sp",
            )
            scan_parts = result.get("definition", {}).get("parts", [])
            visual_count = sum(1 for p in scan_parts if p.get("path", "").endswith("/visual.json"))
            if visual_count > 100:
                print(
                    f"{icons.yellow_dot} Report '{rpt_name}' is in PBIRLegacy format "
                    f"— eligible for upgrade to PBIR."
                )
                print(
                    f"{icons.warning} Report has {visual_count} visuals. "
                    f"PBIR conversion may fail for reports with more than 100 visuals."
                )
                return True
        except Exception:
            pass  # If getDefinition fails in scan, just report eligibility

        print(
            f"{icons.yellow_dot} Report '{rpt_name}' is in PBIRLegacy format "
            f"— eligible for upgrade to PBIR."
        )
        return True  # scan mode: report is eligible, not a failure

    # ------------------------------------------------------------------
    # Fix mode — perform the upgrade via REST round-trip
    # ------------------------------------------------------------------
    print(
        f"{icons.in_progress} Upgrading '{rpt_name}' from PBIRLegacy to PBIR..."
    )

    # Step 1: Get the current report definition (raw base64 parts)
    print(f"{icons.in_progress} Retrieving report definition...")
    try:
        result = _base_api(
            request=f"/v1/workspaces/{workspace_id}/reports/{rpt_id}/getDefinition",
            method="post",
            status_codes=None,
            lro_return_json=True,
            client="fabric_sp",
        )
    except Exception as e:
        print(
            f"{icons.red_dot} Failed to get report definition for "
            f"'{rpt_name}': {e}"
        )
        return False

    parts = result.get("definition", {}).get("parts", [])
    if not parts:
        print(
            f"{icons.red_dot} Report definition for '{rpt_name}' returned "
            f"no parts — cannot upgrade."
        )
        return False

    part_paths = [p.get("path") for p in parts]
    print(
        f"{icons.in_progress} Retrieved {len(parts)} definition "
        f"part(s): {', '.join(part_paths)}"
    )

    # Check visual count — conversion may fail with >100 visuals
    visual_count = sum(1 for p in part_paths if p and p.endswith("/visual.json"))
    if visual_count > 100:
        print(
            f"{icons.warning} Report '{rpt_name}' has {visual_count} visuals. "
            f"PBIR conversion may fail for reports with more than 100 visuals. "
            f"Proceeding anyway — check the result manually."
        )

    # Step 2: Push the definition back via updateDefinition
    # The server processes the definition and stores it in the
    # workspace's current format.  If the workspace has the
    # "enhanced report format" (PBIR) enabled this converts the report.
    print(f"{icons.in_progress} Pushing definition back via updateDefinition...")
    try:
        _base_api(
            request=f"/v1/workspaces/{workspace_id}/reports/{rpt_id}/updateDefinition",
            method="post",
            client="fabric_sp",
            payload={"definition": {"parts": parts}},
            lro_return_status_code=True,
            status_codes=None,
        )
    except Exception as e:
        print(
            f"{icons.red_dot} updateDefinition failed for '{rpt_name}': {e}"
        )
        return False

    print(f"{icons.in_progress} updateDefinition completed — checking format...")

    # Step 3: Poll for format change
    upgrade_start = time.time()
    return _check_upgrade_status(
        reports_url, str(rpt_id), workspace_name, start_time=upgrade_start
    )
