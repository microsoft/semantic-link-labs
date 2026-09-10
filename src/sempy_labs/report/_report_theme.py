# Design Theme Editor — standalone report module.
# Reads/modifies the Power BI report theme JSON.

from typing import Optional
from uuid import UUID


# Standard Power BI theme color keys
_COLOR_KEYS = [
    "dataColors",
    "background",
    "foreground",
    "tableAccent",
    "hyperlink",
    "maximum",
    "center",
    "minimum",
    "neutral",
    "bad",
    "good",
]


def get_report_theme(
    report: str,
    workspace: Optional[str | UUID] = None,
    theme_type: str = "baseTheme",
) -> dict:
    """
    Gets the current theme JSON of a report.

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    theme_type : str, default="baseTheme"
        The theme type: "baseTheme" or "customTheme".

    Returns
    -------
    dict
        The theme JSON.
    """
    from sempy_labs.report import connect_report

    with connect_report(report=report, readonly=True, workspace=workspace) as rw:
        try:
            theme = rw.get_theme(theme_type=theme_type)
        except Exception:
            # Try the other type
            alt = "customTheme" if theme_type == "baseTheme" else "baseTheme"
            theme = rw.get_theme(theme_type=alt)
    return theme


def set_report_theme(
    report: str,
    theme_json: dict,
    workspace: Optional[str | UUID] = None,
):
    """
    Sets a custom theme on a report.

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    theme_json : dict
        The theme JSON to apply.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    """
    from sempy_labs.report import connect_report

    with connect_report(report=report, readonly=False, workspace=workspace) as rw:
        rw.set_theme(theme_json=theme_json)
    print(f"\u2713 Theme applied to '{report}'.")


def update_theme_colors(
    report: str,
    workspace: Optional[str | UUID] = None,
    data_colors: Optional[list] = None,
    background: Optional[str] = None,
    foreground: Optional[str] = None,
    table_accent: Optional[str] = None,
    scan_only: bool = False,
):
    """
    Updates specific color properties in the report's theme.

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    data_colors : list of str, default=None
        List of hex color strings for the data palette (e.g. ["#FF9500", "#2563eb", ...]).
    background : str, default=None
        Hex color for the background.
    foreground : str, default=None
        Hex color for the foreground/text.
    table_accent : str, default=None
        Hex color for the table accent.
    scan_only : bool, default=False
        If True, only shows what would change.
    """
    from sempy_labs.report import connect_report

    with connect_report(report=report, readonly=scan_only, workspace=workspace) as rw:
        # Get current theme
        try:
            theme = rw.get_theme(theme_type="customTheme")
        except Exception:
            theme = rw.get_theme(theme_type="baseTheme")

        changes = 0

        if data_colors is not None:
            old = theme.get("dataColors", [])
            if scan_only:
                print(f"  Would update dataColors: {old[:3]}... -> {data_colors[:3]}...")
            else:
                theme["dataColors"] = data_colors
            changes += 1

        if background is not None:
            old = theme.get("background", "")
            if scan_only:
                print(f"  Would update background: {old} -> {background}")
            else:
                theme["background"] = background
            changes += 1

        if foreground is not None:
            old = theme.get("foreground", "")
            if scan_only:
                print(f"  Would update foreground: {old} -> {foreground}")
            else:
                theme["foreground"] = foreground
            changes += 1

        if table_accent is not None:
            old = theme.get("tableAccent", "")
            if scan_only:
                print(f"  Would update tableAccent: {old} -> {table_accent}")
            else:
                theme["tableAccent"] = table_accent
            changes += 1

        if not scan_only and changes > 0:
            # Ensure it has a name
            if "name" not in theme:
                theme["name"] = "PBI Fixer Custom Theme"
            rw.set_theme(theme_json=theme)
            print(f"\u2713 Updated {changes} theme property/properties on '{report}'.")
        elif scan_only:
            print(f"  Would update {changes} theme property/properties.")
        else:
            print("  No changes specified.")


def show_theme_summary(
    report: str,
    workspace: Optional[str | UUID] = None,
):
    """
    Prints a summary of the report's current theme colors.

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    """
    try:
        theme = get_report_theme(report, workspace, "customTheme")
        print("Theme type: customTheme")
    except Exception:
        theme = get_report_theme(report, workspace, "baseTheme")
        print("Theme type: baseTheme")

    print(f"  Name: {theme.get('name', '(unnamed)')}")

    colors = theme.get("dataColors", [])
    if colors:
        print(f"  Data Colors ({len(colors)}): {', '.join(colors[:8])}{'...' if len(colors) > 8 else ''}")

    for key in ["background", "foreground", "tableAccent", "hyperlink", "good", "bad", "neutral", "maximum", "center", "minimum"]:
        val = theme.get(key, "")
        if val:
            print(f"  {key}: {val}")

    # Font family
    ff = theme.get("textClasses", {})
    for cls_name, cls_val in ff.items():
        font = cls_val.get("fontFace", "")
        if font:
            print(f"  Font ({cls_name}): {font}")
