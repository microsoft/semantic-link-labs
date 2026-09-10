# Fix Visual Alignment — standalone report fixer.
# Aligns chart visuals on a page by snapping nearly-aligned visuals to a common position/size.

from typing import Optional
from uuid import UUID


# Chart visual types to check for alignment (actual data chart visuals only)
_CHART_TYPES = {
    "barChart", "clusteredBarChart", "stackedBarChart", "hundredPercentStackedBarChart",
    "columnChart", "clusteredColumnChart", "stackedColumnChart", "hundredPercentStackedColumnChart",
    "lineChart", "areaChart", "stackedAreaChart", "lineStackedColumnComboChart",
    "lineClusteredColumnComboChart", "ribbonChart", "waterfallChart", "funnel",
    "scatterChart", "pieChart", "donutChart",
}


def fix_visual_alignment(
    report: str,
    page_name: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
    tolerance_pct: float = 2.0,
):
    """
    Aligns chart visuals on report pages by snapping nearly-aligned visuals.

    Visuals within `tolerance_pct` of the page dimension are snapped to a common
    position (x or y) and resized to a common width/height.

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    page_name : str, default=None
        Specific page to fix. If None, processes all pages.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    scan_only : bool, default=False
        If True, only reports what would be fixed without making changes.
    tolerance_pct : float, default=2.0
        Percentage tolerance for alignment detection. Visuals within this % of
        the page width/height are considered "nearly aligned" and will be snapped.
    """
    from sempy_labs.report import connect_report
    import pandas as pd

    total_fixes = 0

    with connect_report(report=report, readonly=scan_only, workspace=workspace) as rw:
        pages_df = rw.list_pages()
        visuals_df = rw.list_visuals()

        target_pages = pages_df if page_name is None else pages_df[
            (pages_df["Page Display Name"] == page_name) | (pages_df["Page Name"] == page_name)
        ]

        for _, page_row in target_pages.iterrows():
            p_name = str(page_row.get("Page Name", ""))
            p_display = str(page_row.get("Page Display Name", ""))
            p_width = float(page_row.get("Width", 1280))
            p_height = float(page_row.get("Height", 720))

            tol_x = p_width * tolerance_pct / 100.0
            tol_y = p_height * tolerance_pct / 100.0

            # Get chart visuals on this page
            page_visuals = visuals_df[
                (visuals_df["Page Name"] == p_name) &
                (visuals_df["Type"].isin(_CHART_TYPES)) &
                (visuals_df["Hidden"] == False)
            ].copy()

            if len(page_visuals) < 2:
                continue

            # --- Step 1: Resize nearly-same-sized visuals ---
            # Group by approximate width (within tolerance)
            widths = page_visuals["Width"].values
            heights = page_visuals["Height"].values

            # Find groups of similar widths
            width_groups = _group_by_tolerance(page_visuals.index.tolist(), widths, tol_x)
            for group_indices in width_groups:
                if len(group_indices) < 2:
                    continue
                target_w = widths[group_indices[0]]
                for gi in group_indices[1:]:
                    if abs(widths[gi] - target_w) > 0 and abs(widths[gi] - target_w) <= tol_x:
                        row = page_visuals.iloc[gi]
                        if scan_only:
                            print(f"  [{p_display}] Would resize width: '{row.get('Title', row['Visual Name'])}' {widths[gi]:.0f} -> {target_w:.0f}")
                        else:
                            _update_visual_pos(rw, row["File Path"], width=target_w)
                        total_fixes += 1

            # Find groups of similar heights
            height_groups = _group_by_tolerance(page_visuals.index.tolist(), heights, tol_y)
            for group_indices in height_groups:
                if len(group_indices) < 2:
                    continue
                target_h = heights[group_indices[0]]
                for gi in group_indices[1:]:
                    if abs(heights[gi] - target_h) > 0 and abs(heights[gi] - target_h) <= tol_y:
                        row = page_visuals.iloc[gi]
                        if scan_only:
                            print(f"  [{p_display}] Would resize height: '{row.get('Title', row['Visual Name'])}' {heights[gi]:.0f} -> {target_h:.0f}")
                        else:
                            _update_visual_pos(rw, row["File Path"], height=target_h)
                        total_fixes += 1

            # --- Step 2: Align x-positions (vertical columns) ---
            x_vals = page_visuals["X"].values
            x_groups = _group_by_tolerance(page_visuals.index.tolist(), x_vals, tol_x)
            for group_indices in x_groups:
                if len(group_indices) < 2:
                    continue
                target_x = x_vals[group_indices[0]]
                for gi in group_indices[1:]:
                    if abs(x_vals[gi] - target_x) > 0 and abs(x_vals[gi] - target_x) <= tol_x:
                        row = page_visuals.iloc[gi]
                        if scan_only:
                            print(f"  [{p_display}] Would align X: '{row.get('Title', row['Visual Name'])}' {x_vals[gi]:.0f} -> {target_x:.0f}")
                        else:
                            _update_visual_pos(rw, row["File Path"], x=target_x)
                        total_fixes += 1

            # --- Step 3: Align y-positions (horizontal rows) ---
            y_vals = page_visuals["Y"].values
            y_groups = _group_by_tolerance(page_visuals.index.tolist(), y_vals, tol_y)
            for group_indices in y_groups:
                if len(group_indices) < 2:
                    continue
                target_y = y_vals[group_indices[0]]
                for gi in group_indices[1:]:
                    if abs(y_vals[gi] - target_y) > 0 and abs(y_vals[gi] - target_y) <= tol_y:
                        row = page_visuals.iloc[gi]
                        if scan_only:
                            print(f"  [{p_display}] Would align Y: '{row.get('Title', row['Visual Name'])}' {y_vals[gi]:.0f} -> {target_y:.0f}")
                        else:
                            _update_visual_pos(rw, row["File Path"], y=target_y)
                        total_fixes += 1

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {total_fixes} alignment issue(s).")
    return total_fixes


def _group_by_tolerance(indices, values, tolerance):
    """Group indices by value proximity within tolerance."""
    if len(values) == 0:
        return []
    sorted_pairs = sorted(zip(range(len(values)), values), key=lambda x: x[1])
    groups = []
    current_group = [sorted_pairs[0][0]]
    current_anchor = sorted_pairs[0][1]

    for i in range(1, len(sorted_pairs)):
        idx, val = sorted_pairs[i]
        if abs(val - current_anchor) <= tolerance:
            current_group.append(idx)
        else:
            groups.append(current_group)
            current_group = [idx]
            current_anchor = val
    groups.append(current_group)
    return groups


def _update_visual_pos(rw, file_path, x=None, y=None, width=None, height=None):
    """Update a visual's position/size in the report definition."""
    payload = rw.get(file_path=file_path)
    pos = payload.get("position", {})
    if x is not None:
        pos["x"] = x
    if y is not None:
        pos["y"] = y
    if width is not None:
        pos["width"] = width
    if height is not None:
        pos["height"] = height
    payload["position"] = pos
    rw.update(file_path=file_path, payload=payload)
