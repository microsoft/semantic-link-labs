"""
Shared UI building blocks used by interactive widgets in Semantic Link Labs
(e.g. ``perspective_editor`` and ``vertipaq_analyzer``).

This module centralizes:

- A library of monochrome SVG ``ICONS`` (use ``currentColor`` so they adapt to
  light/dark themes).
- Light/dark theme CSS variable blocks.
- Helpers to render a reusable widget header (title, dataset/workspace
  subtitle, and a light/dark theme toggle button) and the small bit of
  JavaScript that powers the theme toggle for static-HTML widgets.
- Immediate, consistent press feedback for buttons in every interactive tool.

The components here are intentionally framework-agnostic: they return plain
strings (HTML / CSS / JS) so they can be embedded in ``IPython.display.HTML``
output, an ``anywidget`` widget, or any other surface that renders raw HTML.
"""

from typing import Dict, List, Optional, Sequence, Tuple
import uuid

# ---------------------------------------------------------------------------
# Icons (monochrome SVGs that use currentColor)
# ---------------------------------------------------------------------------
ICONS: dict[str, str] = {
    # Tabular object icons --------------------------------------------------
    "table": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<rect x="2.5" y="3" width="11" height="10" rx="1.8"/>'
        '<path d="M2.5 6.75h11M8 6.75v6.25"/></svg>'
    ),
    "calculation_group": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<rect x="2.5" y="3" width="11" height="10" rx="1.8"/>'
        '<path d="M2.5 6.75h11M5.25 9.5h5.5M5.25 11.4h3.5"/></svg>'
    ),
    "column": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<rect x="6" y="2.5" width="4" height="11" rx="1.6"/></svg>'
    ),
    "column_chunk": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<rect x="6" y="2.5" width="4" height="11" rx="1.6"/>'
        '<path d="M6 6.25h4M6 9.75h4"/></svg>'
    ),
    "measure": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M12 3H4l4.5 5L4 13h8"/></svg>'
    ),
    "hierarchy": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<circle cx="8" cy="3.25" r="1.4"/>'
        '<circle cx="3.75" cy="12.75" r="1.4"/>'
        '<circle cx="12.25" cy="12.75" r="1.4"/>'
        '<path d="M8 4.65V8M8 8H3.75v3.35M8 8h4.25v3.35"/></svg>'
    ),
    "calculation_item": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<rect x="2.5" y="2.5" width="11" height="11" rx="2.4"/>'
        '<path d="M9.75 5.75H7.4q-.9 0-.9.95V11M5.6 8.2h2.6"/></svg>'
    ),
    "partition": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.4" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<rect x="3" y="1.5" width="10" height="4" rx="1"/>'
        '<rect x="3" y="6.5" width="10" height="4" rx="1"/>'
        '<rect x="3" y="11.5" width="10" height="3" rx="1"/></svg>'
    ),
    "relationship": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.4" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<circle cx="4" cy="8" r="2.5"/><circle cx="12" cy="8" r="2.5"/>'
        '<line x1="6.5" y1="8" x2="9.5" y2="8"/></svg>'
    ),
    "calculated_table": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<rect x="2.5" y="3" width="11" height="10" rx="1.8"/>'
        '<path d="M2.5 6.75h11"/>'
        '<path d="M9.7 8.1c-1.1-.4-1.9.1-2.1 1.1l-.7 3.6M6.3 9.9h3"/></svg>'
    ),
    "calculation_group": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<rect x="3" y="1.5" width="10" height="13" rx="2"/>'
        '<rect x="5" y="3.4" width="6" height="2.3" rx="0.6"/>'
        '<path d="M5.5 8.5h.01M8 8.5h.01M10.5 8.5h.01M5.5 10.7h.01M8 10.7h.01'
        'M10.5 10.7h.01M5.5 12.9h.01M8 12.9h.01M10.5 12.9h.01"/></svg>'
    ),
    "date_table": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<rect x="2.5" y="3.5" width="11" height="10" rx="1.8"/>'
        '<path d="M2.5 6.75h11"/>'
        '<path d="M5.5 2v2.6M10.5 2v2.6"/>'
        '<path d="M5.2 9.2h.01M8 9.2h.01M10.8 9.2h.01M5.2 11.4h.01'
        'M8 11.4h.01"/></svg>'
    ),
    "field_parameter": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.35" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<rect x="1.6" y="2" width="8.6" height="8.6" rx="1.2"/>'
        '<path d="M1.6 4.85h8.6M1.6 7.75h8.6"/>'
        '<path d="M4.5 2v8.6M7.4 2v8.6"/>'
        '<path d="M10.8 11.45c.1-1 .9-1.55 1.65-1.4.8.16 1.15.98.72 '
        '1.7-.27.45-.72.55-.72 1.25"/>'
        '<path d="M12.45 14.5h.01"/></svg>'
    ),
    "calculated_column": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<rect x="5" y="2.5" width="6" height="11" rx="1.8"/>'
        '<path stroke-width="1.3" d="M9.6 5.4c-.4-1.3-1.8-1.3-2.2.2l-1.2 5.8'
        'M6.3 8h2.8"/></svg>'
    ),
    # UI icons --------------------------------------------------------------
    "sun": (
        '<svg width="16" height="16" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<circle cx="8" cy="8" r="3"/>'
        '<path d="M8 1.5v1.5M8 13v1.5M1.5 8h1.5M13 8h1.5'
        "M3.3 3.3l1.05 1.05M11.65 11.65l1.05 1.05"
        'M3.3 12.7l1.05-1.05M11.65 4.35l1.05-1.05"/></svg>'
    ),
    "moon": (
        '<svg width="16" height="16" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M13.5 9.5A5.5 5.5 0 0 1 6.5 2.5a5.5 5.5 0 1 0 7 7z"/></svg>'
    ),
    "info": (
        '<svg width="16" height="16" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<circle cx="8" cy="8" r="6"/><path d="M8 7.25v4"/>'
        '<path d="M8 4.5h.01"/></svg>'
    ),
    "search": (
        '<svg viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">'
        '<path fill-rule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 '
        "0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 "
        '0 012 8z" clip-rule="evenodd"/></svg>'
    ),
    "dax_performance": (
        '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" '
        'stroke="currentColor" stroke-width="1.9" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M4.1 17.5a8.5 8.5 0 1 1 15.8 0"/>'
        '<path d="m12 14.5 4.1-4.8"/>'
        '<circle cx="12" cy="14.5" r="1" fill="currentColor" stroke="none"/>'
        '</svg>'
    ),
    "activity": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="M3 12h4l3-9 4 18 3-9h4"/></svg>'
    ),
    "cpu": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<rect width="16" height="16" x="4" y="4" rx="2"/>'
        '<rect width="6" height="6" x="9" y="9" rx="1"/>'
        '<path d="M15 2v2M15 20v2M2 15h2M2 9h2M20 15h2M20 9h2M9 2v2M9 20v2"/>'
        '</svg>'
    ),
    "database": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<ellipse cx="12" cy="5" rx="9" ry="3"/>'
        '<path d="M3 5v14c0 1.7 4 3 9 3s9-1.3 9-3V5"/>'
        '<path d="M3 12c0 1.7 4 3 9 3s9-1.3 9-3"/>'
        '</svg>'
    ),
    "vertipaq": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.3" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<ellipse cx="8" cy="3.3" rx="5.3" ry="1.7"/>'
        '<path d="M2.7 3.3v9.4c0 .94 2.37 1.7 5.3 1.7s5.3-.76 5.3-1.7V3.3"/>'
        '<circle cx="7" cy="7.4" r="3.1"/>'
        '<path d="M5.4 6.5a2.1 2.1 0 0 0-.2 2.3"/>'
        '<path d="M9.3 9.7 12.6 13"/></svg>'
    ),
    "zap": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="M4 14a1 1 0 0 1-.78-1.63l9-11a.5.5 0 0 1 .87.43L11.92 8h7.18a1 1 0 0 1 .78 1.63l-9 11a.5.5 0 0 1-.87-.43l1.17-6.2z"/>'
        '</svg>'
    ),
    "list_tree": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="M21 12h-8"/><path d="M21 6H8"/><path d="M21 18h-8"/>'
        '<path d="M3 6v4c0 1.1.9 2 2 2h3"/>'
        '<path d="M3 10v6c0 1.1.9 2 2 2h3"/></svg>'
    ),
    "git_branch": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<line x1="6" x2="6" y1="3" y2="15"/>'
        '<circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/>'
        '<path d="M18 9a9 9 0 0 1-9 9"/></svg>'
    ),
    "workflow": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<rect width="8" height="8" x="3" y="3" rx="2"/>'
        '<path d="M7 11v4a2 2 0 0 0 2 2h4"/>'
        '<rect width="8" height="8" x="13" y="13" rx="2"/></svg>'
    ),
    "plus": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.8" stroke-linecap="round" '
        'aria-hidden="true"><path d="M8 3.25v9.5M3.25 8h9.5"/></svg>'
    ),
    "caret_right": (
        "<svg width='8' height='10' viewBox='0 0 8 10' fill='currentColor'>"
        "<path d='M1 0l6 5-6 5V0z'/></svg>"
    ),
    "folder": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M2 4.25c0-.7.55-1.25 1.25-1.25h3l1.5 1.75h4.75c.7 0 '
        "1.25.55 1.25 1.25v6c0 .7-.55 1.25-1.25 1.25H3.25C2.55 13.25 2 "
        '12.7 2 12V4.25z"/></svg>'
    ),
    "level": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.8" stroke-linecap="round" '
        'aria-hidden="true">'
        '<path d="M2.5 4h11M5.5 8h8M8.5 12h5"/></svg>'
    ),
    "play": (
        '<svg viewBox="0 0 16 16" fill="currentColor" aria-hidden="true">'
        '<path d="M4 2.5v11l9-5.5z"/></svg>'
    ),
    "stop": (
        '<svg viewBox="0 0 16 16" fill="currentColor" aria-hidden="true">'
        '<rect x="4" y="4" width="8" height="8" rx="1.2"/></svg>'
    ),
    "eraser": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="m7 21-4.3-4.3c-1-1-1-2.5 0-3.4l9.6-9.6c1-1 2.5-1 3.4 0l4.6 4.6c1 1 1 2.5 0 3.4L11 21"/>'
        '<path d="M22 21H7"/><path d="m5 11 9 9"/></svg>'
    ),
    "trash": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="M3 6h18"/><path d="M8 6V4h8v2"/>'
        '<path d="M19 6l-1 14H6L5 6"/>'
        '<path d="M10 11v5M14 11v5"/></svg>'
    ),
    "camera": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="M14.5 4 16 7h3a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V9a2 2 0 0 1 2-2h3l1.5-3z"/>'
        '<circle cx="12" cy="13" r="3"/></svg>'
    ),
    "report_file": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>'
        '<path d="M14 2v6h6M8 17v-3M12 17v-6M16 17v-2"/></svg>'
    ),
    "chevron_down": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true"><path d="m6 9 6 6 6-6"/></svg>'
    ),
    "check": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true"><path d="m5 12 4 4L19 6"/></svg>'
    ),
    "refresh": (
        '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" '
        'stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="M13.5 8a5.5 5.5 0 1 1-1.61-3.89"/>'
        '<path d="M13.5 2.5v3h-3"/></svg>'
    ),
    "swap": (
        '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" '
        'stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="M2.5 5.5h9"/><path d="M9 3l2.5 2.5L9 8"/>'
        '<path d="M13.5 10.5h-9"/><path d="M7 8l-2.5 2.5L7 13"/></svg>'
    ),
    "sort_asc": (
        '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" '
        'stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="M5 12.5V3.5"/><path d="M2.5 6L5 3.5L7.5 6"/></svg>'
    ),
    "sort_desc": (
        '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" '
        'stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="M5 3.5v9"/><path d="M2.5 10L5 12.5L7.5 10"/></svg>'
    ),
    "panel_collapse": (
        '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" '
        'stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<rect x="2" y="3" width="12" height="10" rx="1.5"/>'
        '<path d="M6.5 3v10"/><path d="M10.5 6.5L8.5 8l2 1.5"/></svg>'
    ),
    "panel_expand": (
        '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" '
        'stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<rect x="2" y="3" width="12" height="10" rx="1.5"/>'
        '<path d="M6.5 3v10"/><path d="M8.5 6.5L10.5 8l-2 1.5"/></svg>'
    ),
    "hammer": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="m15 12-9.373 9.373a1 1 0 0 1-3.001-3L12 9"/>'
        '<path d="m18 15 4-4"/>'
        '<path d="m21.5 11.5-1.914-1.914A2 2 0 0 1 19 8.172v-.344a2 2 0 0 0-.586-1.414l-1.657-1.657A6 6 0 0 0 12.516 3H9l1.243 1.243A6 6 0 0 1 12 8.485V10l2 2h1.172a2 2 0 0 1 1.414.586L18.5 14.5"/></svg>'
    ),
    "shield_check": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true"><path d="M20 13c0 5-3.5 7.5-8 9-4.5-1.5-8-4-8-9V5l8-3 8 3v8z"/>'
        '<path d="m9 12 2 2 4-4"/></svg>'
    ),
    "users": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/>'
        '<circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.9M16 3.1a4 4 0 0 1 0 7.8"/></svg>'
    ),
    "user": (
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true"><path d="M20 21a8 8 0 0 0-16 0"/>'
        '<circle cx="12" cy="7" r="4"/></svg>'
    ),
    "close": (
        '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" '
        'stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="M4 4l8 8M12 4l-8 8"/></svg>'
    ),
    # A "maximize / full-screen" mark (four outward corner arrows) used by the
    # header button that expands the whole tool to fill the screen.
    "fullscreen": (
        '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" '
        'stroke="currentColor" stroke-width="2" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M3 9 V5 a2 2 0 0 1 2 -2 h4"/>'
        '<path d="M21 9 V5 a2 2 0 0 0 -2 -2 h-4"/>'
        '<path d="M3 15 v4 a2 2 0 0 0 2 2 h4"/>'
        '<path d="M21 15 v4 a2 2 0 0 1 -2 2 h-4"/></svg>'
    ),
    # An "exit full-screen" mark (four inward corner arrows).
    "fullscreen_exit": (
        '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" '
        'stroke="currentColor" stroke-width="2" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M9 3 v4 a2 2 0 0 1 -2 2 H3"/>'
        '<path d="M15 3 v4 a2 2 0 0 0 2 2 h4"/>'
        '<path d="M9 21 v-4 a2 2 0 0 0 -2 -2 H3"/>'
        '<path d="M15 21 v-4 a2 2 0 0 1 2 -2 h4"/></svg>'
    "back": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.8" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M10 3L5 8l5 5"/></svg>'
    ),
    "refresh": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="currentColor" '
        'aria-hidden="true">'
        '<path d="M8 2a6 6 0 0 1 5.196 3H11.5a.5.5 0 0 0 0 1h2.9A.6.6 0 0 0 '
        "15 5.4V2.5a.5.5 0 0 0-1 0v1.55A7 7 0 1 0 15 8a.5.5 0 0 0-1 0A6 6 0 "
        '1 1 8 2z"/></svg>'
    ),
    "history": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M2.5 3v3.5H6"/>'
        '<path d="M3 6.2A5.5 5.5 0 1 1 2.8 10"/>'
        '<path d="M8 4.5V8l2.5 1.5"/></svg>'
    ),
    "source": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<ellipse cx="8" cy="3.75" rx="5" ry="1.5"/>'
        '<path d="M3 3.75v8.5c0 .83 2.24 1.5 5 1.5s5-.67 5-1.5v-8.5"/>'
        '<path d="M3 8c0 .83 2.24 1.5 5 1.5s5-.67 5-1.5"/></svg>'
    ),
    "more": (
        '<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor" '
        'aria-hidden="true">'
        '<circle cx="3" cy="8" r="1.5"/>'
        '<circle cx="8" cy="8" r="1.5"/>'
        '<circle cx="13" cy="8" r="1.5"/></svg>'
    ),
    # Delta Analyzer badge: a delta (triangle) with mini bars — marks stats
    # sourced from the Delta Analyzer, distinct from the plain data-bars icon.
    "delta_stats": (
        '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" '
        'stroke="currentColor" stroke-width="2" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M12 4 20.5 20 3.5 20 Z"/>'
        '<line x1="9" y1="17.5" x2="9" y2="15"/>'
        '<line x1="12" y1="17.5" x2="12" y2="12.5"/>'
        '<line x1="15" y1="17.5" x2="15" y2="14.5"/></svg>'
    ),
    "sync": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M3 8a5 5 0 0 1 8.6-3.5"/>'
        '<path d="M11.6 2.5v2.5h-2.5"/>'
        '<path d="M13 8a5 5 0 0 1-8.6 3.5"/>'
        '<path d="M4.4 13.5V11h2.5"/></svg>'
    ),
    "pencil": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M11.5 2.5l2 2L5 13H3v-2z"/>'
        '<path d="M10 4l2 2"/></svg>'
    ),
    # Vertipaq Analyzer: a database cylinder examined by a magnifying glass,
    # mirroring the VertiPaq Analyzer mark (same drawing as the Tools app's
    # semantic model explorer sub-tool icon).
    "vertipaq": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.3" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<ellipse cx="8" cy="3.3" rx="5.3" ry="1.7"/>'
        '<path d="M2.7 3.3v9.4c0 .94 2.37 1.7 5.3 1.7s5.3-.76 5.3-1.7V3.3"/>'
        '<circle cx="7" cy="7.4" r="3.1"/>'
        '<path d="M5.4 6.5a2.1 2.1 0 0 0-.2 2.3"/>'
        '<path d="M9.3 9.7 12.6 13"/></svg>'
    ),
    "swap": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M3 5.5h9"/>'
        '<path d="M9.5 3l2.5 2.5L9.5 8"/>'
        '<path d="M13 10.5H4"/>'
        '<path d="M6.5 8L4 10.5 6.5 13"/></svg>'
    ),
    "link": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M6.5 9.5L9.5 6.5"/>'
        '<path d="M7 4.5l1-1a2.5 2.5 0 1 1 3.5 3.5l-1 1"/>'
        '<path d="M9 11.5l-1 1a2.5 2.5 0 1 1-3.5-3.5l1-1"/></svg>'
    ),
    "database": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<ellipse cx="8" cy="3.5" rx="5" ry="1.8"/>'
        '<path d="M3 3.5v9c0 1 2.24 1.8 5 1.8s5-.8 5-1.8v-9"/>'
        '<path d="M3 8c0 1 2.24 1.8 5 1.8s5-.8 5-1.8"/></svg>'
    ),
    "database_zap": (
        '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" '
        'stroke="currentColor" stroke-width="2" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<ellipse cx="12" cy="5" rx="9" ry="3"/>'
        '<path d="M3 5V19A9 3 0 0 0 15 21.84"/>'
        '<path d="M21 5V8"/>'
        '<path d="M21 12L18 17H22L19 22"/>'
        '<path d="M3 12A9 3 0 0 0 14.59 14.87"/></svg>'
    ),
    "report": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<rect x="3.5" y="2" width="9" height="12" rx="1.6"/>'
        '<path d="M6 11v-2M8 11V6.5M10 11V8.5"/></svg>'
    ),
    "check_circle": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<circle cx="8" cy="8" r="6"/>'
        '<path d="M5.5 8.2l1.8 1.8 3.2-3.6"/></svg>'
    ),
    "alert": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M8 2.5l5.5 9.5H2.5z"/>'
        '<path d="M8 6.5v2.5M8 11h.01"/></svg>'
    ),
    "external_link": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M9 3.5h3.5V7"/>'
        '<path d="M12.5 3.5L7.5 8.5"/>'
        '<path d="M11 9v3a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V6a1 1 0 0 1 1-1h3"/></svg>'
    ),
    "zoom_in": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<circle cx="7" cy="7" r="4.2"/>'
        '<path d="M10.2 10.2L13.5 13.5"/>'
        '<path d="M7 5.4v3.2M5.4 7h3.2"/></svg>'
    ),
    "zoom_out": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<circle cx="7" cy="7" r="4.2"/>'
        '<path d="M10.2 10.2L13.5 13.5"/>'
        '<path d="M5.4 7h3.2"/></svg>'
    ),
    "close": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.8" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M4 4l8 8M12 4l-8 8"/></svg>'
    ),
    "chevron_left": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.7" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M10 3.5L5.5 8l4.5 4.5"/></svg>'
    ),
    "chevron_right": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.7" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M6 3.5L10.5 8L6 12.5"/></svg>'
    ),
    "workflow": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<rect x="2" y="2.5" width="4.5" height="4.5" rx="1"/>'
        '<rect x="9.5" y="9" width="4.5" height="4.5" rx="1"/>'
        '<path d="M4.25 7v2.25a1.5 1.5 0 0 0 1.5 1.5h3.75"/></svg>'
    ),
    "expand_rows": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M5 6.5l3-3 3 3"/><path d="M5 9.5l3 3 3-3"/></svg>'
    ),
    "collapse_rows": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M5 4l3 3 3-3"/><path d="M5 12l3-3 3 3"/></svg>'
    ),
    "scan": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M2.5 5V3.5A1 1 0 0 1 3.5 2.5H5"/>'
        '<path d="M11 2.5h1.5a1 1 0 0 1 1 1V5"/>'
        '<path d="M13.5 11v1.5a1 1 0 0 1-1 1H11"/>'
        '<path d="M5 13.5H3.5a1 1 0 0 1-1-1V11"/>'
        '<circle cx="8" cy="8" r="2"/></svg>'
    ),
    "history": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M2.6 8a5.4 5.4 0 1 0 1.7-3.9"/>'
        '<path d="M2.5 2.8v2.6h2.6"/>'
        '<path d="M8 5.2V8l2 1.4"/></svg>'
    ),
    "scan_search": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M2.5 5V3.5A1 1 0 0 1 3.5 2.5H5"/>'
        '<path d="M11 2.5h1.5a1 1 0 0 1 1 1V5"/>'
        '<path d="M13.5 11v1.5a1 1 0 0 1-1 1H11"/>'
        '<path d="M5 13.5H3.5a1 1 0 0 1-1-1V11"/>'
        '<circle cx="7.2" cy="7.2" r="2.2"/>'
        '<path d="M8.9 8.9L11 11"/></svg>'
    ),
    "fullscreen": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M2.5 6V3.5a1 1 0 0 1 1-1H6"/>'
        '<path d="M10 2.5h2.5a1 1 0 0 1 1 1V6"/>'
        '<path d="M13.5 10v2.5a1 1 0 0 1-1 1H10"/>'
        '<path d="M6 13.5H3.5a1 1 0 0 1-1-1V10"/></svg>'
    ),
    "fullscreen_exit": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M6 2.5V5a1 1 0 0 1-1 1H2.5"/>'
        '<path d="M13.5 6H11a1 1 0 0 1-1-1V2.5"/>'
        '<path d="M10 13.5V11a1 1 0 0 1 1-1h2.5"/>'
        '<path d="M2.5 10H5a1 1 0 0 1 1 1v2.5"/></svg>'
    ),
    "save": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.4" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M3.25 2.5h7.1l3.15 3.15V12.5a1 1 0 0 1-1 1H3.5'
        'a1 1 0 0 1-1-1v-9a1 1 0 0 1 .75-.97z"/>'
        '<path d="M5 2.5h5v3H5z"/>'
        '<rect x="5" y="8.25" width="6" height="5.25" rx="0.5"/></svg>'
    ),
    "wrench": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M11.6 2.6a3 3 0 0 0-3.85 3.85l-4.9 4.9a1.25 1.25 0 0 0 '
        '1.77 1.77l4.9-4.9a3 3 0 0 0 3.85-3.85l-1.9 1.9-1.47-.3-.3-1.47z"/></svg>'
    ),
    "eye": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M1.5 8s2.4-4 6.5-4 6.5 4 6.5 4-2.4 4-6.5 4-6.5-4-6.5-4z"/>'
        '<circle cx="8" cy="8" r="1.9"/></svg>'
    ),
    "eye_off": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M6.3 3.7A6.9 6.9 0 0 1 8 3.5c4.1 0 6.5 4 6.5 4a12 12 0 0 1-2 2.5"/>'
        '<path d="M11.2 11.2A6.6 6.6 0 0 1 8 12c-4.1 0-6.5-4-6.5-4a12 12 0 0 1 '
        '3.2-3.4"/>'
        '<path d="M6.7 6.7a1.9 1.9 0 0 0 2.6 2.6"/>'
        '<path d="M2 2l12 12"/></svg>'
    ),
    "wand": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M9.6 4.3l2.1 2.1-7 7-2.1-2.1z"/>'
        '<path d="M11 2.9l.5 1.1 1.1.5-1.1.5-.5 1.1-.5-1.1L9.4 4.5l1.1-.5z"/>'
        '<path d="M13.4 8.1l.35.8.8.35-.8.35-.35.8-.35-.8-.8-.35.8-.35z"/>'
        '<path d="M4.3 2.2l.35.8.8.35-.8.35-.35.8-.35-.8-.8-.35.8-.35z"/></svg>'
    ),
    "undo": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M6 4L3 7l3 3"/>'
        '<path d="M3 7h6a3.5 3.5 0 0 1 0 7H5.5"/></svg>'
    ),
    "redo": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M10 4l3 3-3 3"/>'
        '<path d="M13 7H7a3.5 3.5 0 0 0 0 7h3.5"/></svg>'
    ),
    "error_circle": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<circle cx="8" cy="8" r="6"/>'
        '<path d="M6 6l4 4M10 6l-4 4"/></svg>'
    ),
    "check": (
        '<svg width="12" height="12" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="2.2" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M3.2 8.4l3.2 3.2 6.4-7"/></svg>'
    ),
    "reset": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M13.4 8a5.4 5.4 0 1 1-1.7-3.9"/>'
        '<path d="M13.5 2.7v3.1h-3.1"/>'
        '<circle cx="8" cy="8" r="1.15" fill="currentColor" stroke="none"/></svg>'
    ),
    "sliders": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<circle cx="4.5" cy="5" r="1.9"/><path d="M6.8 5h6.7"/>'
        '<circle cx="11.5" cy="11" r="1.9"/><path d="M9.2 11H2.5"/></svg>'
    ),
    "info": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<circle cx="8" cy="8" r="6"/>'
        '<path d="M8 7.4v3.2M8 5.2h.01"/></svg>'
    ),
    "shield_check": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M8 1.8l5 1.9v4.1c0 3-2.1 5.2-5 6.4-2.9-1.2-5-3.4-5-6.4V3.7z"/>'
        '<path d="M5.9 7.9l1.5 1.5 2.7-3"/></svg>'
    ),
    "activity": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M1.5 8h3l1.8-4.8 3 9.6L11.1 8h3.4"/></svg>'
    ),
    "code": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M5.4 4.5L2 8l3.4 3.5"/><path d="M10.6 4.5L14 8l-3.4 3.5"/>'
        '<path d="M9.2 3l-2.4 10"/></svg>'
    ),
    "settings": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.4" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<circle cx="8" cy="8" r="2.1"/>'
        '<path d="M12.9 9.8a1.1 1.1 0 0 0 .22 1.21l.04.04a1.33 1.33 0 1 1-1.88 1.88'
        "l-.04-.04a1.1 1.1 0 0 0-1.21-.22 1.1 1.1 0 0 0-.67 1v.11a1.33 1.33 0 1 1-2.66 0"
        "v-.06a1.1 1.1 0 0 0-.72-1 1.1 1.1 0 0 0-1.21.22l-.04.04a1.33 1.33 0 1 1-1.88-1.88"
        "l.04-.04a1.1 1.1 0 0 0 .22-1.21 1.1 1.1 0 0 0-1-.67h-.11a1.33 1.33 0 1 1 0-2.66"
        "h.06a1.1 1.1 0 0 0 1-.72 1.1 1.1 0 0 0-.22-1.21l-.04-.04a1.33 1.33 0 1 1 1.88-1.88"
        "l.04.04a1.1 1.1 0 0 0 1.21.22h.05a1.1 1.1 0 0 0 .67-1v-.11a1.33 1.33 0 1 1 2.66 0"
        "v.06a1.1 1.1 0 0 0 .67 1 1.1 1.1 0 0 0 1.21-.22l.04-.04a1.33 1.33 0 1 1 1.88 1.88"
        "l-.04.04a1.1 1.1 0 0 0-.22 1.21v.05a1.1 1.1 0 0 0 1 .67h.11a1.33 1.33 0 1 1 0 2.66"
        'h-.06a1.1 1.1 0 0 0-1 .67z"/></svg>'
    ),
    "text_type": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M3 4.2V3h10v1.2"/><path d="M8 3v10"/><path d="M6 13h4"/></svg>'
    ),
    "play": (
        '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M4.5 3.2l8 4.8-8 4.8z"/></svg>'
    ),
    "upload": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M2.5 10.5v2a1 1 0 0 0 1 1h9a1 1 0 0 0 1-1v-2"/>'
        '<path d="M5.2 5.2L8 2.4l2.8 2.8"/><path d="M8 2.6v7.6"/></svg>'
    ),
    "download": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M2.5 10.5v2a1 1 0 0 0 1 1h9a1 1 0 0 0 1-1v-2"/>'
        '<path d="M5.2 7.4L8 10.2l2.8-2.8"/><path d="M8 10v-7.6"/></svg>'
    ),
    "trash": (
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M2.5 4.5h11M6 2.5h4M4.5 4.5l.6 9h5.8l.6-9"/>'
        '<path d="M6.5 7v4M9.5 7v4"/></svg>'
    ),
}


# Shared table-column resizing for interactive widgets. Call
# ``sllsInstallColumnResizers(table, config)`` after rendering a table.
TABLE_COLUMN_RESIZE_JS: str = r"""
function sllsInstallColumnResizers(table, config) {
    const options = config || {};
    const headers = Array.from(table.querySelectorAll("thead tr:first-child th"));
    if (!headers.length || table.dataset.resizable === "true") return;
    table.dataset.resizable = "true";

    const minWidth = Number(options.minWidth) || 56;
    const widthsStore = options.widths || new Map();
    const key = typeof options.key === "function"
        ? options.key(table) : String(options.key || table.className || "table");
    const saved = widthsStore.get(key);
    const colgroup = document.createElement("colgroup");
    const widths = headers.map((header, index) =>
        saved?.[index] || Number(header.dataset.columnWidth)
            || Math.max(minWidth, Math.ceil(header.getBoundingClientRect().width))
    );

    function applyWidths() {
        widths.forEach((width, index) => {
            colgroup.children[index].style.width = `${width}px`;
        });
        table.style.width = `${widths.reduce((sum, width) => sum + width, 0)}px`;
        if (typeof options.onWidthsChanged === "function") {
            options.onWidthsChanged(table, [...widths]);
        }
    }

    function contentWidth(index) {
        const canvas = document.createElement("canvas");
        const context = canvas.getContext("2d");
        const cells = Array.from(table.rows)
            .map(row => row.cells[index])
            .filter(Boolean);
        let widest = minWidth;
        cells.forEach(cell => {
            const style = getComputedStyle(cell);
            context.font = style.font;
            const letterSpacing = Number.parseFloat(style.letterSpacing) || 0;
            const horizontalChrome =
                (Number.parseFloat(style.paddingLeft) || 0)
                + (Number.parseFloat(style.paddingRight) || 0)
                + (Number.parseFloat(style.borderLeftWidth) || 0)
                + (Number.parseFloat(style.borderRightWidth) || 0);
            const lines = String(cell.textContent || "").split(/\r?\n/);
            lines.forEach(line => {
                const renderedLine = style.textTransform === "uppercase"
                    ? line.toUpperCase() : line;
                const spacing = Math.max(0, renderedLine.length - 1) * letterSpacing;
                widest = Math.max(
                    widest,
                    Math.ceil(context.measureText(renderedLine).width + spacing + horizontalChrome + 2),
                );
            });
        });
        return widest;
    }

    widths.forEach(width => {
        const col = document.createElement("col");
        col.style.width = `${width}px`;
        colgroup.appendChild(col);
    });
    table.insertBefore(colgroup, table.firstChild);
    table.style.tableLayout = "fixed";
    applyWidths();

    headers.forEach((header, index) => {
        header.classList.add(options.resizableClass || "slls-resizable");
        const handle = document.createElement("span");
        handle.className = options.handleClass || "slls-column-resizer";
        handle.setAttribute("role", "separator");
        handle.setAttribute("aria-label", `Resize ${header.textContent.trim()} column`);
        handle.addEventListener("dblclick", event => {
            event.preventDefault();
            event.stopPropagation();
            widths[index] = contentWidth(index);
            applyWidths();
            widthsStore.set(key, [...widths]);
        });
        handle.addEventListener("pointerdown", event => {
            event.preventDefault();
            event.stopPropagation();
            const startX = event.clientX;
            const startWidth = widths[index];
            handle.classList.add(options.resizingClass || "slls-resizing");
            handle.setPointerCapture(event.pointerId);
            const onMove = moveEvent => {
                widths[index] = Math.max(
                    minWidth,
                    startWidth + moveEvent.clientX - startX,
                );
                applyWidths();
            };
            const onEnd = () => {
                handle.classList.remove(options.resizingClass || "slls-resizing");
                widthsStore.set(key, [...widths]);
                handle.removeEventListener("pointermove", onMove);
                handle.removeEventListener("pointerup", onEnd);
                handle.removeEventListener("pointercancel", onEnd);
            };
            handle.addEventListener("pointermove", onMove);
            handle.addEventListener("pointerup", onEnd);
            handle.addEventListener("pointercancel", onEnd);
        });
        header.appendChild(handle);
    });
}
"""

# Backward-compatible name used by older widget implementations.
ICONS["builder"] = ICONS["hammer"]


# ---------------------------------------------------------------------------
# Theme CSS variables (Light + Dark palettes)
# ---------------------------------------------------------------------------
LIGHT_THEME_VARS: str = """\
--ui-bg-solid: #ffffff;
--ui-bg: #ffffff;
--ui-bg-secondary: #f5f5f7;
--ui-bg-tertiary: #fbfbfd;
--ui-bg-hover: #f0f0f2;
--ui-surface: rgba(255, 255, 255, 0.85);
--ui-surface-2: rgba(0, 0, 0, 0.025);
--ui-border: rgba(0, 0, 0, 0.08);
--ui-border-strong: rgba(0, 0, 0, 0.14);
--ui-text: #1d1d1f;
--ui-text-secondary: #6e6e73;
--ui-text-tertiary: #86868b;
--ui-accent: #0071e3;
--ui-accent-hover: #0a6cdb;
--ui-accent-soft: rgba(0, 113, 227, 0.12);
--ui-on-accent: #ffffff;
--ui-danger: #dc2626;
--ui-danger-hover: #b91c1c;
--ui-danger-bg: rgba(220, 38, 38, 0.10);
--ui-danger-border: rgba(220, 38, 38, 0.35);
--ui-danger-text: #b91c1c;
--ui-warning-bg: #fef3c7;
--ui-warning-text: #92400e;
--ui-shadow-sm: 0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.06);
--ui-shadow-md: 0 4px 14px rgba(0,0,0,0.08), 0 2px 6px rgba(0,0,0,0.04);
--ui-shadow-lg: 0 12px 40px rgba(0,0,0,0.12), 0 4px 12px rgba(0,0,0,0.06);
"""

DARK_THEME_VARS: str = """\
--ui-bg-solid: #1e1e22;
--ui-bg: #1e1e22;
--ui-bg-secondary: #2a2a30;
--ui-bg-tertiary: #26262b;
--ui-bg-hover: #2c2c33;
--ui-surface: rgba(255, 255, 255, 0.04);
--ui-surface-2: rgba(255, 255, 255, 0.03);
--ui-border: rgba(255, 255, 255, 0.08);
--ui-border-strong: rgba(255, 255, 255, 0.16);
--ui-text: #f5f5f7;
--ui-text-secondary: #b8b8bf;
--ui-text-tertiary: #8e8e94;
--ui-accent: #0A84FF;
--ui-accent-hover: #1a8cff;
--ui-accent-soft: rgba(10, 132, 255, 0.18);
--ui-on-accent: #ffffff;
--ui-danger: #dc2626;
--ui-danger-hover: #b91c1c;
--ui-danger-bg: rgba(248, 113, 113, 0.12);
--ui-danger-border: rgba(248, 113, 113, 0.35);
--ui-danger-text: #fca5a5;
--ui-warning-bg: rgba(250, 204, 21, 0.22);
--ui-warning-text: #fde047;
--ui-shadow-sm: 0 1px 3px rgba(0,0,0,0.3), 0 1px 2px rgba(0,0,0,0.4);
--ui-shadow-md: 0 4px 14px rgba(0,0,0,0.4), 0 2px 6px rgba(0,0,0,0.3);
--ui-shadow-lg: 0 12px 40px rgba(0,0,0,0.5), 0 4px 12px rgba(0,0,0,0.3);
"""


# ---------------------------------------------------------------------------
# DAX / code syntax-highlight palette
# ---------------------------------------------------------------------------
# Theme-independent token colors used to colorize DAX (or similar code) in
# editors/highlighters. The values are tuned to read well on both the light
# and dark surfaces above, so the same block is injected once into a widget's
# base scope (it is not overridden in dark mode). Reference these via the
# ``--ui-syntax-*`` custom properties — never hard-code the hex values.
SYNTAX_HIGHLIGHT_VARS: str = """\
--ui-syntax-keyword: #5E9EFF;
--ui-syntax-function: #5E9EFF;
--ui-syntax-variable: #5AC8B8;
--ui-syntax-number: #FF9F45;
--ui-syntax-virtual-column: #FF7A8A;
--ui-syntax-string: #9BB87A;
--ui-syntax-comment: #6A9955;
--ui-syntax-operator: #A6A6A6;
--ui-syntax-punctuation: #A6A6A6;
"""

# Searchable single-select (the standard workspace / semantic model picker)
# ---------------------------------------------------------------------------
# Every tool which asks the user to pick a workspace, a semantic model or any
# other long list must use this control rather than a plain <select>, so that
# the list can always be filtered by typing.
SEARCH_SELECT_CSS: str = """\
.slls-ss { position: relative; display: flex; width: 100%; }
.slls-ss-btn {
    appearance: none; -webkit-appearance: none; width: 100%;
    background: var(--ui-bg); border: 1px solid var(--ui-border-strong);
    border-radius: 10px; padding: 10px 12px; font-size: 14px; font-family: inherit;
    color: var(--ui-text); cursor: pointer; display: inline-flex; align-items: center; gap: 8px;
    transition: border-color 120ms ease;
}
.slls-ss-btn:hover:not(:disabled) { border-color: var(--ui-text-tertiary); }
.slls-ss-btn:focus-visible { outline: none; border-color: var(--ui-accent); }
.slls-ss-btn:disabled { opacity: 0.55; cursor: not-allowed; }
.slls-ss-value { flex: 1 1 auto; min-width: 0; text-align: left; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-ss-value.slls-ss-placeholder { color: var(--ui-text-tertiary); }
.slls-ss-caret { display: inline-flex; flex-shrink: 0; color: var(--ui-text-tertiary); transform: rotate(90deg); transition: transform 140ms ease; }
.slls-ss-caret svg { display: block; width: 15px; height: 15px; }
.slls-ss.slls-ss-open .slls-ss-caret { transform: rotate(-90deg); }
.slls-ss-panel {
    display: none; position: absolute; top: calc(100% + 6px); left: 0; right: 0; z-index: 70;
    min-width: 240px; padding: 6px; background: var(--ui-bg-solid);
    border: 1px solid var(--ui-border); border-radius: 12px; box-shadow: var(--ui-shadow-lg);
}
.slls-ss.slls-ss-open .slls-ss-panel { display: block; }
.slls-ss-searchwrap { position: relative; display: flex; align-items: center; margin-bottom: 5px; }
.slls-ss-searchicon { position: absolute; left: 10px; display: inline-flex; color: var(--ui-text-tertiary); pointer-events: none; }
.slls-ss-searchicon svg { display: block; width: 15px; height: 15px; }
.slls-ss-search {
    width: 100%; appearance: none; background: var(--ui-bg-secondary);
    border: 1px solid transparent; border-radius: 8px; padding: 7px 10px 7px 32px;
    font-size: 13px; font-family: inherit; color: var(--ui-text);
}
.slls-ss-search::placeholder { color: var(--ui-text-tertiary); }
.slls-ss-search:focus { outline: none; border-color: var(--ui-accent); }
.slls-ss-list { max-height: 240px; overflow-y: auto; }
.slls-ss-opt {
    display: block; width: 100%; padding: 7px 10px; border: none; background: transparent;
    color: var(--ui-text); font-family: inherit; font-size: 13px; text-align: left;
    border-radius: 7px; cursor: pointer; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.slls-ss-opt:hover, .slls-ss-opt.slls-ss-active { background: var(--ui-surface-2); }
.slls-ss-opt.slls-ss-selected { color: var(--ui-accent); font-weight: 500; }
.slls-ss-empty { padding: 9px 10px; font-size: 12.5px; color: var(--ui-text-tertiary); }
"""

# JavaScript defining ``createSearchSelect(options)``. Embed this inside a
# widget's ESM module (or inside its ``render`` function) and call it to build a
# picker. ``options`` accepts ``placeholder``, ``searchPlaceholder``,
# ``ariaLabel``, ``emptyLabel`` and ``onChange``; the returned controller
# exposes ``el``, ``value``, ``label``, ``focus()``, ``setOptions(items, value)``,
# ``setEmptyLabel(text)`` and ``setDisabled(flag)``.
SEARCH_SELECT_JS: str = """\
const __sllsSsOpen = new Set();
document.addEventListener("click", () => { for (const close of __sllsSsOpen) close(); });

function createSearchSelect(config) {
    const cfg = config || {};
    const MAX_LIST_HEIGHT = 240, MIN_LIST_HEIGHT = 120;
    let placeholder = cfg.placeholder || "Select\\u2026";
    let emptyLabel = cfg.emptyLabel || "No items";
    const onChange = cfg.onChange || function () {};

    const wrap = document.createElement("div");
    wrap.className = "slls-ss";

    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "slls-ss-btn";
    btn.setAttribute("aria-haspopup", "listbox");
    if (cfg.ariaLabel) btn.setAttribute("aria-label", cfg.ariaLabel);
    const valueLabel = document.createElement("span");
    valueLabel.className = "slls-ss-value";
    btn.appendChild(valueLabel);
    const caret = document.createElement("span");
    caret.className = "slls-ss-caret";
    caret.innerHTML = `__SLLS_SS_CARET__`;
    btn.appendChild(caret);
    wrap.appendChild(btn);

    const panel = document.createElement("div");
    panel.className = "slls-ss-panel";
    const searchWrap = document.createElement("div");
    searchWrap.className = "slls-ss-searchwrap";
    const searchIcon = document.createElement("span");
    searchIcon.className = "slls-ss-searchicon";
    searchIcon.innerHTML = `__SLLS_SS_SEARCH__`;
    searchWrap.appendChild(searchIcon);
    const search = document.createElement("input");
    search.className = "slls-ss-search";
    search.type = "search";
    search.placeholder = cfg.searchPlaceholder || "Search\\u2026";
    search.setAttribute("aria-label", cfg.searchPlaceholder || "Search");
    searchWrap.appendChild(search);
    panel.appendChild(searchWrap);
    const list = document.createElement("div");
    list.className = "slls-ss-list";
    list.setAttribute("role", "listbox");
    panel.appendChild(list);
    wrap.appendChild(panel);

    let options = [];
    let value = "";
    let disabled = false;
    // Index into the currently filtered options, for keyboard navigation.
    let activeIndex = -1;
    let shown = [];

    function clearNode(node) { while (node.firstChild) node.removeChild(node.firstChild); }
    function close() {
        wrap.classList.remove("slls-ss-open");
        btn.setAttribute("aria-expanded", "false");
        activeIndex = -1;
    }
    function open() {
        for (const other of __sllsSsOpen) if (other !== close) other();
        wrap.classList.add("slls-ss-open");
        btn.setAttribute("aria-expanded", "true");
        search.value = "";
        // The list always drops downward and is capped to the room below the
        // control, so it scrolls instead of overflowing the widget.
        const rect = btn.getBoundingClientRect();
        const room = window.innerHeight - rect.bottom - 70;
        list.style.maxHeight = `${Math.max(MIN_LIST_HEIGHT, Math.min(MAX_LIST_HEIGHT, room))}px`;
        activeIndex = -1;
        renderList();
        setActive(shown.findIndex((o) => o.value === value));
        search.focus();
    }
    __sllsSsOpen.add(close);

    function selectedOption() { return options.find((o) => o.value === value) || null; }
    function renderValue() {
        const option = selectedOption();
        valueLabel.textContent = option ? option.label : (options.length === 0 ? emptyLabel : placeholder);
        valueLabel.classList.toggle("slls-ss-placeholder", !option);
        valueLabel.title = option ? option.label : "";
        btn.disabled = disabled || options.length === 0;
    }
    function setActive(index) {
        const rows = list.querySelectorAll(".slls-ss-opt");
        if (rows.length === 0) { activeIndex = -1; return; }
        activeIndex = Math.max(0, Math.min(index, rows.length - 1));
        rows.forEach((row, i) => row.classList.toggle("slls-ss-active", i === activeIndex));
        rows[activeIndex].scrollIntoView({ block: "nearest" });
    }
    function commit(option) {
        const changed = value !== option.value;
        value = option.value;
        renderValue();
        close();
        btn.focus();
        if (changed) onChange(option);
    }
    function renderList() {
        clearNode(list);
        const term = search.value.trim().toLowerCase();
        shown = term ? options.filter((o) => o.label.toLowerCase().includes(term)) : options;
        if (shown.length === 0) {
            const empty = document.createElement("div");
            empty.className = "slls-ss-empty";
            empty.textContent = options.length === 0 ? emptyLabel : "No matches";
            list.appendChild(empty);
            activeIndex = -1;
            return;
        }
        for (const option of shown) {
            const row = document.createElement("button");
            row.type = "button";
            row.tabIndex = -1;
            row.className = "slls-ss-opt" + (option.value === value ? " slls-ss-selected" : "");
            row.setAttribute("role", "option");
            row.setAttribute("aria-selected", String(option.value === value));
            row.textContent = option.label;
            row.title = option.label;
            row.addEventListener("click", () => commit(option));
            list.appendChild(row);
        }
        if (activeIndex >= 0) setActive(activeIndex);
    }

    btn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        if (wrap.classList.contains("slls-ss-open")) close(); else open();
    });
    btn.addEventListener("keydown", (ev) => {
        if (ev.key === "ArrowDown" || ev.key === "ArrowUp") {
            ev.preventDefault();
            if (!wrap.classList.contains("slls-ss-open")) open();
        }
    });
    panel.addEventListener("click", (ev) => ev.stopPropagation());
    search.addEventListener("input", () => { activeIndex = -1; renderList(); setActive(0); });
    search.addEventListener("keydown", (ev) => {
        if (ev.key === "ArrowDown") { ev.preventDefault(); setActive(activeIndex + 1); }
        else if (ev.key === "ArrowUp") { ev.preventDefault(); setActive(activeIndex <= 0 ? 0 : activeIndex - 1); }
        else if (ev.key === "Home") { ev.preventDefault(); setActive(0); }
        else if (ev.key === "End") { ev.preventDefault(); setActive(shown.length - 1); }
        else if (ev.key === "Enter") {
            ev.preventDefault();
            if (activeIndex >= 0 && shown[activeIndex]) commit(shown[activeIndex]);
        } else if (ev.key === "Escape" || ev.key === "Tab") {
            // Collapse first so Esc/Tab continue on to the surrounding UI rather
            // than stepping through the option list.
            ev.stopPropagation();
            ev.preventDefault();
            close();
            btn.focus();
        }
    });

    renderValue();

    return {
        el: wrap,
        get value() { return value; },
        get label() { const o = selectedOption(); return o ? o.label : ""; },
        focus() { btn.focus(); },
        setOptions(next, nextValue) {
            options = next || [];
            if (nextValue !== undefined) value = nextValue;
            if (!options.some((o) => o.value === value)) value = "";
            renderValue();
            renderList();
        },
        setEmptyLabel(text) { emptyLabel = text; renderValue(); renderList(); },
        setPlaceholder(text) { placeholder = text; renderValue(); },
        setDisabled(flag) { disabled = !!flag; if (disabled) close(); renderValue(); },
    };
}
"""

SEARCH_SELECT_JS = SEARCH_SELECT_JS.replace(
    "__SLLS_SS_CARET__", ICONS["caret_right"]
).replace("__SLLS_SS_SEARCH__", ICONS["search"])


# ---------------------------------------------------------------------------
# Reusable header (title + dataset/workspace subtitle + theme toggle)
# ---------------------------------------------------------------------------
HEADER_CSS: str = """\
.sl-header {
    display: flex;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
    width: 100%;
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display",
        "SF Pro Text", "Helvetica Neue", Helvetica, Arial, sans-serif;
    color: var(--ui-text);
}
.sl-header * { box-sizing: border-box; }
.sl-head-spacer { flex: 1 1 auto; }
.sl-title-icon {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 34px;
    height: 34px;
    border-radius: 9px;
    background: var(--ui-accent-soft);
    color: var(--ui-accent);
    flex-shrink: 0;
}
.sl-title-icon svg { display: block; width: 19px; height: 19px; }
.sl-titlewrap {
    display: flex;
    flex-direction: column;
    min-width: 0;
}
.sl-title {
    font-size: 22px;
    font-weight: 600;
    letter-spacing: -0.01em;
    line-height: 1.15;
    color: var(--ui-text);
}
.sl-subtitle {
    font-size: 12.5px;
    color: var(--ui-text-secondary);
    margin-top: 3px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 540px;
    font-weight: 400;
}
.sl-subtitle .sl-sep { color: var(--ui-text-tertiary); margin: 0 6px; }
.sl-subtitle b { color: var(--ui-text); font-weight: 500; }
.sl-theme-btn {
    appearance: none;
    -webkit-appearance: none;
    border: 1px solid var(--ui-border-strong);
    background: var(--ui-surface);
    color: var(--ui-text);
    width: 32px;
    height: 32px;
    padding: 0;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border-radius: 50%;
    font-size: 18px;
    line-height: 1;
    cursor: pointer;
    transition: background 120ms ease, border-color 120ms ease,
        transform 80ms ease;
    font-family: inherit;
    flex-shrink: 0;
}
.sl-theme-btn:hover {
    background: var(--ui-surface-2);
    border-color: var(--ui-text-tertiary);
}
.sl-theme-btn:active { transform: scale(0.95); }
.sl-theme-btn svg { display: block; width: 18px; height: 18px; }
.sl-change-btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 26px;
    height: 26px;
    padding: 0;
    margin-top: 6px;
    flex: 0 0 auto;
    align-self: flex-start;
    border-radius: 6px;
    border: 1px solid var(--ui-border);
    background: transparent;
    color: var(--ui-text-secondary);
    cursor: pointer;
    transition: border-color 120ms ease, color 120ms ease;
}
.sl-change-btn svg { display: block; width: 15px; height: 15px; }
.sl-change-btn:hover { border-color: var(--ui-accent); color: var(--ui-accent); }
"""


# ---------------------------------------------------------------------------
# Reusable button press feedback
# ---------------------------------------------------------------------------
BUTTON_PRESS_CSS: str = """\
button:not(:disabled),
[role="button"]:not([aria-disabled="true"]) {
    transform-origin: center;
    -webkit-tap-highlight-color: transparent;
}
button:not(:disabled):active,
[role="button"]:not([aria-disabled="true"]):active {
    transform: scale(0.96);
    filter: brightness(0.9);
}
"""


def _scope_css(root_selector: str, css: str) -> str:
    """Prefix every top-level rule in ``css`` with ``root_selector``.

    Used to raise selector specificity so the widget's styles win against
    notebook host styles.
    """
    import re

    def _prefix(match: "re.Match[str]") -> str:
        selectors = match.group(1)
        scoped = ", ".join(f"{root_selector} {s.strip()}" for s in selectors.split(","))
        return f"{scoped} {{"

    return re.sub(r"([^{}]+)\{", _prefix, css)


def scoped_header_css(root_selector: str) -> str:
    """Return :data:`HEADER_CSS` with every rule prefixed by ``root_selector``.

    This raises the specificity of the header rules so they win against
    notebook host styles (e.g. Jupyter's ``.jp-RenderedHTMLCommon button``
    rules that would otherwise override layout, border-radius, and colors
    on the theme toggle button).

    Parameters
    ----------
    root_selector : str
        A CSS selector for the widget's root container, e.g.
        ``".vpx-abc123"``. Each top-level rule in :data:`HEADER_CSS` is
        rewritten as ``{root_selector} <original-selector> { ... }``.

    Returns
    -------
    str
        The scoped CSS as a single string.
    """
    return _scope_css(root_selector, HEADER_CSS)


def scoped_button_press_css(root_selector: str) -> str:
    """Return immediate press-feedback styles scoped to a widget root.

    Every interactive Semantic Link Labs tool should include this CSS so a
    pointer or keyboard press is acknowledged before any slower JavaScript or
    Python action completes. The rules cover enabled native buttons and custom
    controls with ``role="button"``; disabled controls are intentionally
    excluded. Scoping prevents the styles from leaking into the notebook host
    or neighboring widget instances.

    Parameters
    ----------
    root_selector : str
        A CSS selector for the widget's root container, e.g.
        ``".slls-rm"`` or ``".vpx-abc123"``.

    Returns
    -------
    str
        :data:`BUTTON_PRESS_CSS` with every rule prefixed by
        ``root_selector``.
    """
    return _scope_css(root_selector, BUTTON_PRESS_CSS)


def _escape_html(value: str) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def render_header_html(
    title: str,
    dataset_name: Optional[str] = None,
    workspace_name: Optional[str] = None,
    theme_btn_id: Optional[str] = None,
    dark_mode: bool = False,
    fullscreen_btn_id: Optional[str] = None,
    picker_btn_id: Optional[str] = None,
    title_icon: Optional[str] = None,
    extra_buttons: Optional[List[Dict[str, str]]] = None,
) -> str:
    """Render the standard widget header as HTML.

    Parameters
    ----------
    title : str
        The header title (e.g. ``"Vertipaq Analyzer"``).
    dataset_name : str, default=None
        Optional dataset/semantic model name shown in the subtitle.
    workspace_name : str, default=None
        Optional workspace name shown in the subtitle.
    theme_btn_id : str, default=None
        If provided, includes a light/dark theme toggle button with this
        DOM id. Pair with :func:`theme_toggle_script` to wire up behavior.
        The theme button is always rendered as the rightmost control.
    dark_mode : bool, default=False
        Controls the initial icon shown on the theme toggle button.
    fullscreen_btn_id : str, default=None
        If provided, includes a full-screen toggle button with this DOM id
        (placed after the theme toggle). Pair with
        :func:`fullscreen_toggle_script` to wire up behavior.
    picker_btn_id : str, default=None
        If provided, includes a small "change" (swap) button with this DOM id
        in the title area. Used to reveal/toggle an interactive picker.
        If provided, includes a full-screen toggle button with this DOM id,
        placed immediately to the left of the theme toggle button. Pair with
        :func:`fullscreen_toggle_script` to wire up behavior.
    title_icon : str, default=None
        Optional SVG markup (e.g. an entry from :data:`ICONS`) rendered in an
        accent-colored badge to the left of the title.
    extra_buttons : list[dict[str, str]], default=None
        Optional extra icon buttons rendered immediately to the right of the
        title. Each dict accepts ``id``, ``icon`` (SVG markup), ``title`` and an
        optional ``cls`` appended to the button's classes.

    Returns
    -------
    str
        The header HTML fragment. The caller is responsible for including
        :data:`HEADER_CSS` (or otherwise providing the referenced CSS
        custom properties) on the page.
    """
    parts = ['<div class="sl-header">']
    if title_icon:
        parts.append(f'<span class="sl-title-icon">{title_icon}</span>')
    parts.append('<div class="sl-titlewrap">')
    parts.append(f'<div class="sl-title">{_escape_html(title)}</div>')

    if dataset_name or workspace_name:
        ds = _escape_html(dataset_name) if dataset_name else ""
        ws = _escape_html(workspace_name) if workspace_name else ""
        if ds and ws:
            sub = f"<b>{ds}</b><span class='sl-sep'>·</span>{ws}"
        else:
            sub = f"<b>{ds}</b>" if ds else ws
        parts.append(f'<div class="sl-subtitle">{sub}</div>')

    if picker_btn_id:
        parts.append(
            f'<button type="button" class="sl-change-btn" id="{picker_btn_id}" '
            f'title="Change table" aria-label="Change table">{ICONS["swap"]}</button>'
        )

    parts.append("</div>")  # titlewrap

    for btn in extra_buttons or []:
        cls = f"sl-theme-btn {btn.get('cls', '')}".strip()
        label = btn.get("title", "")
        parts.append(
            f'<button type="button" class="{cls}" id="{btn.get("id", "")}" '
            f'title="{label}" aria-label="{label}">{btn.get("icon", "")}</button>'
        )

    # Pushes the full-screen / theme buttons to the right edge.
    parts.append('<div class="sl-head-spacer"></div>')

    if fullscreen_btn_id:
        fs_icon = ICONS["fullscreen"]
        parts.append(
            f'<button type="button" class="sl-theme-btn" id="{fullscreen_btn_id}" '
            f'title="Toggle full screen" aria-label="Toggle full screen">'
            f"{fs_icon}</button>"
        )

    # The theme button is appended last so it is always the rightmost control.
    if theme_btn_id:
        icon = ICONS["sun"] if dark_mode else ICONS["moon"]
        label = "Switch to light mode" if dark_mode else "Switch to dark mode"
        parts.append(
            f'<button type="button" class="sl-theme-btn" id="{theme_btn_id}" '
            f'title="{label}" aria-label="{label}">{icon}</button>'
        )

    if fullscreen_btn_id:
        parts.append(
            f'<button type="button" class="sl-theme-btn" id="{fullscreen_btn_id}" '
            f'title="Full screen" aria-label="Full screen">'
            f'{ICONS["fullscreen"]}</button>'
        )

    parts.append("</div>")
    return "".join(parts)


def theme_toggle_script(
    btn_id: str,
    root_selector: str,
    dark_class: str = "sl-dark",
) -> str:
    """Return a small JS snippet that wires a theme toggle button.

    Clicking the button toggles ``dark_class`` on the element matched by
    ``root_selector`` and swaps the button icon between sun and moon.

    Parameters
    ----------
    btn_id : str
        The DOM id of the theme toggle button.
    root_selector : str
        A CSS selector for the root element whose ``dark_class`` should be
        toggled (e.g. ``".vpx-abc123"``).
    dark_class : str, default="sl-dark"
        The CSS class that activates the dark theme on the root element.

    Returns
    -------
    str
        A ``<script>`` block ready to be inserted into the rendered HTML.
    """
    sun = ICONS["sun"].replace("`", "\\`")
    moon = ICONS["moon"].replace("`", "\\`")
    return f"""
<script>
(function() {{
    var btn = document.getElementById({btn_id!r});
    if (!btn) return;
    var root = document.querySelector({root_selector!r});
    if (!root) return;
    var SUN = `{sun}`;
    var MOON = `{moon}`;
    function render() {{
        var isDark = root.classList.contains({dark_class!r});
        btn.innerHTML = isDark ? SUN : MOON;
        var label = isDark ? 'Switch to light mode' : 'Switch to dark mode';
        btn.title = label;
        btn.setAttribute('aria-label', label);
    }}
    btn.addEventListener('click', function() {{
        root.classList.toggle({dark_class!r});
        render();
    }});
    render();
}})();
</script>
"""


def fullscreen_toggle_script(
    btn_id: str,
    root_selector: str,
    fs_class: str = "sl-fs",
) -> str:
    """Return a small JS snippet that wires a full-screen toggle button.

    Clicking the button requests the native Fullscreen API on the widget root
    (so it truly fills the screen where the host permits it) and toggles
    ``fs_class`` on the root as a CSS-overlay fallback for hosts that reject the
    native request.

    The overlay class and a sized placeholder are applied first, and the native
    Fullscreen request is issued immediately afterwards within the same click
    handler. Requesting native fullscreen *before* mutating the DOM causes
    Chromium to abort the just-issued request (it then silently falls back to
    the iframe-bound CSS overlay), so the mutate-then-request ordering — which
    matches the working anywidget tools — is used instead. The widget is never
    re-parented (moving it across the DOM does disturb the user activation the
    Fullscreen API requires). A sized placeholder is inserted to reserve the
    widget's original footprint so an auto-height output iframe (VS Code /
    Fabric) does not collapse — which would otherwise clip the overlay fallback
    and make the UI appear to vanish.

    The button icon swaps between the enter/exit glyphs and pressing
    ``Escape`` exits.

    Parameters
    ----------
    btn_id : str
        The DOM id of the full-screen toggle button.
    root_selector : str
        A CSS selector for the root element to expand (e.g. ``".vpx-abc123"``).
    fs_class : str, default="sl-fs"
        The CSS class that activates the full-screen overlay on the root
        element. The caller must define what this class does in its own CSS
        (typically ``position: fixed; inset: 0``).

    Returns
    -------
    str
        A ``<script>`` block ready to be inserted into the rendered HTML.
    """
    fs = ICONS["fullscreen"].replace("`", "\\`")
    fsx = ICONS["fullscreen_exit"].replace("`", "\\`")
    return f"""
<script>
(function() {{
    var btn = document.getElementById({btn_id!r});
    if (!btn) return;
    var root = document.querySelector({root_selector!r});
    if (!root) return;
    var FS = `{fs}`;
    var FSX = `{fsx}`;
    var placeholder = null;
    function isOn() {{ return root.classList.contains({fs_class!r}); }}
    function render() {{
        btn.innerHTML = isOn() ? FSX : FS;
        var label = isOn() ? 'Exit full screen' : 'Toggle full screen';
        btn.title = label;
        btn.setAttribute('aria-label', label);
    }}
    function setFs(on) {{
        if (on === isOn()) return;
        if (on) {{
            // Reserve the widget's original footprint with a sized placeholder
            // so an auto-height output (VS Code / Fabric render static HTML in
            // an auto-sizing iframe) does not collapse to zero height when the
            // widget leaves normal flow — which would clip the overlay and make
            // the UI vanish. The widget is NOT re-parented: moving it in the
            // DOM disturbs the user activation that the native Fullscreen API
            // requires, which prevented true fullscreen.
            var rect = root.getBoundingClientRect();
            placeholder = document.createElement('div');
            placeholder.setAttribute('aria-hidden', 'true');
            placeholder.style.height = rect.height + 'px';
            placeholder.style.width = '100%';
            if (root.parentNode) root.parentNode.insertBefore(placeholder, root.nextSibling);
            root.classList.add({fs_class!r});
        }} else {{
            root.classList.remove({fs_class!r});
            if (placeholder && placeholder.parentNode) {{
                placeholder.parentNode.removeChild(placeholder);
            }}
            placeholder = null;
        }}
        render();
    }}
    function requestNative() {{
        // Request true (native) fullscreen within the click gesture so the
        // browser honors the user activation. This is called AFTER the overlay
        // class + placeholder have been applied (see the click handler): the
        // working anywidget tools (perspective_editor, lineage_view) also
        // mutate the DOM first and then request, whereas requesting *before*
        // the mutation causes Chromium to abort the just-issued request and
        // silently fall back to the (iframe-bound) CSS overlay. Notebook output
        // frames generally carry allow="fullscreen"; when the request is
        // rejected the CSS overlay applied by setFs() is the guaranteed
        // fallback.
        var req = root.requestFullscreen || root.webkitRequestFullscreen
            || root.mozRequestFullScreen || root.msRequestFullscreen;
        if (req) {{
            try {{ var pr = req.call(root); if (pr && pr.catch) pr.catch(function() {{}}); }}
            catch (e) {{ /* native fullscreen unavailable; overlay covers it */ }}
        }}
    }}
    function exitNative() {{
        var ex = document.exitFullscreen || document.webkitExitFullscreen
            || document.mozCancelFullScreen || document.msExitFullscreen;
        if (ex && (document.fullscreenElement || document.webkitFullscreenElement)) {{
            try {{ var pe = ex.call(document); if (pe && pe.catch) pe.catch(function() {{}}); }}
            catch (e) {{}}
        }}
    }}
    btn.addEventListener('click', function() {{
        if (isOn()) {{ setFs(false); exitNative(); }}
        else {{ setFs(true); requestNative(); }}
    }});
    // If the user leaves native fullscreen (Esc / F11), drop the overlay too.
    function onFsChange() {{
        var nativeOn = !!(document.fullscreenElement || document.webkitFullscreenElement);
        if (!nativeOn && isOn()) setFs(false);
    }}
    document.addEventListener('fullscreenchange', onFsChange);
    document.addEventListener('webkitfullscreenchange', onFsChange);
    document.addEventListener('keydown', function(e) {{ if (e.key === 'Escape' && isOn()) {{ exitNative(); setFs(false); }} }});
    render();
}})();
</script>
"""


# ---------------------------------------------------------------------------
# Full-screen toggle (expand a widget to fill the screen)
# ---------------------------------------------------------------------------
# Shared JavaScript body that wires a full-screen toggle button. It assumes
# the following identifiers are already in scope wherever it is embedded:
#   - ``root``            : the widget's root DOM element (the element that
#                           goes full screen).
#   - ``btn``             : the toggle <button> element.
#   - ``fullscreenClass`` : CSS class applied to ``root`` for the CSS-overlay
#                           fallback (see :func:`fullscreen_css`).
#   - ``enterSvg`` / ``exitSvg`` : button icon markup for the two states.
#
# This is a faithful port of the full-screen toggle in
# ``sempy_labs.semantic_model._test_dax.test`` — the reference implementation
# that behaves correctly across hosts. It uses the native Fullscreen API when
# available (and allowed by the host), otherwise falls back to a fixed-position
# CSS overlay (toggled via ``fullscreenClass``) that fills the viewport. For
# this to deliver real, edge-to-edge full screen, the widget must render in the
# notebook output *webview's* light DOM (i.e. as an ``anywidget`` or via
# :func:`display_html_widget`) rather than the nested, sandboxed ``srcdoc``
# iframe used for raw ``display(HTML)`` output — the webview permits the
# Fullscreen API, the sandbox iframe does not.
_FULLSCREEN_BODY: str = r"""
    var cssFullscreen = false;
    function isFullscreen() {
        return cssFullscreen || document.fullscreenElement === root;
    }
    function renderFullscreenBtn() {
        var on = isFullscreen();
        btn.innerHTML = on ? exitSvg : enterSvg;
        var label = on ? "Exit full screen" : "Full screen";
        btn.title = label;
        btn.setAttribute("aria-label", label);
        root.classList.toggle(fullscreenClass, cssFullscreen);
    }
    function enterFullscreen() {
        if (root.requestFullscreen) {
            root.requestFullscreen().then(function () {
                cssFullscreen = false;
                renderFullscreenBtn();
            }).catch(function () {
                cssFullscreen = true;
                renderFullscreenBtn();
            });
        } else {
            cssFullscreen = true;
            renderFullscreenBtn();
        }
    }
    function exitFullscreen() {
        if (document.fullscreenElement === root && document.exitFullscreen) {
            document.exitFullscreen().catch(function () {});
        }
        cssFullscreen = false;
        renderFullscreenBtn();
    }
    btn.addEventListener("click", function () {
        if (isFullscreen()) { exitFullscreen(); } else { enterFullscreen(); }
    });
    document.addEventListener("fullscreenchange", renderFullscreenBtn);
    renderFullscreenBtn();
"""


def fullscreen_css(
    root_selector: str,
    fullscreen_class: str,
    container_selector: Optional[str] = None,
    bg_var: str = "var(--ui-bg)",
) -> str:
    """Return the CSS that powers the full-screen state of a widget.

    The rules cover both full-screen mechanisms used by
    :data:`_FULLSCREEN_BODY` / :func:`fullscreen_toggle_script`: the native
    ``:fullscreen`` pseudo-class and the ``fullscreen_class`` CSS-overlay
    fallback (applied to ``root_selector``).

    Parameters
    ----------
    root_selector : str
        A CSS selector for the widget's root element (the element that goes
        full screen), e.g. ``".vpx-abc123"`` or ``".slls-pe"``.
    fullscreen_class : str
        The CSS class toggled on the root for the overlay fallback, e.g.
        ``"vpx-fullscreen"``.
    container_selector : str, default=None
        Optional selector (relative to the root) for an inner container that
        holds the visible "card" styling (border, radius, shadow). When the
        widget draws that styling on the root itself, leave this as ``None``.
    bg_var : str, default="var(--ui-bg)"
        The CSS background value to paint behind the widget while full screen.

    Returns
    -------
    str
        The CSS as a single string.
    """
    fs = f"{root_selector}.{fullscreen_class}"
    container_reset = (
        "border: none; border-radius: 0; box-shadow: none; min-height: 100vh;"
    )
    base = (
        "position: fixed; inset: 0; z-index: 99999; max-width: none; "
        f"margin: 0; background: {bg_var}; overflow: auto;"
    )
    parts: list = []
    if container_selector:
        parts.append(f"{fs} {{ {base} padding: 0; }}")
        parts.append(f"{fs} {container_selector} {{ {container_reset} }}")
        parts.append(
            f"{root_selector}:fullscreen {{ overflow: auto; background: {bg_var}; }}"
        )
        parts.append(
            f"{root_selector}:fullscreen {container_selector} {{ {container_reset} }}"
        )
    else:
        parts.append(f"{fs} {{ {base} {container_reset} }}")
        parts.append(f"{root_selector}:fullscreen {{ {base} {container_reset} }}")
    return "\n".join(parts)


def fullscreen_setup_js(func_name: str = "sllsSetupFullscreen") -> str:
    """Return a JS function definition that wires a full-screen toggle button.

    Intended for ``anywidget``-style widgets that build their DOM in
    JavaScript. Embed the returned source once at the top of the widget's
    ESM module, then call ``func_name(root, btn, fullscreenClass, enterSvg,
    exitSvg)`` after creating the toggle button.

    Parameters
    ----------
    func_name : str, default="sllsSetupFullscreen"
        The name of the generated JS function.

    Returns
    -------
    str
        The JS function definition (no ``<script>`` wrapper).
    """
    return (
        f"function {func_name}(root, btn, fullscreenClass, enterSvg, exitSvg) {{"
        + _FULLSCREEN_BODY
        + "}\n"
    )


def fullscreen_toggle_script(
    btn_id: str,
    root_selector: str,
    fullscreen_class: str,
) -> str:
    """Return a ``<script>`` block that wires a full-screen toggle button.

    Intended for static-HTML widgets (the *Vertipaq* style). The button — see
    :func:`render_header_html`'s ``fullscreen_btn_id`` parameter — toggles the
    widget between its normal size and a full-screen view.

    Parameters
    ----------
    btn_id : str
        The DOM id of the full-screen toggle button.
    root_selector : str
        A CSS selector for the root element that should go full screen
        (e.g. ``".vpx-abc123"``).
    fullscreen_class : str
        The CSS class toggled on the root for the overlay fallback (must match
        the class passed to :func:`fullscreen_css`).

    Returns
    -------
    str
        A ``<script>`` block ready to be inserted into the rendered HTML.
    """
    enter = ICONS["fullscreen"].replace("`", "\\`")
    exit_ = ICONS["fullscreen_exit"].replace("`", "\\`")
    return (
        "\n<script>\n(function() {\n"
        f"    var btn = document.getElementById({btn_id!r});\n"
        "    if (!btn) return;\n"
        f"    var root = document.querySelector({root_selector!r});\n"
        "    if (!root) return;\n"
        f"    var fullscreenClass = {fullscreen_class!r};\n"
        f"    var enterSvg = `{enter}`;\n"
        f"    var exitSvg = `{exit_}`;\n" + _FULLSCREEN_BODY + "\n})();\n</script>\n"
    )


# ---------------------------------------------------------------------------
# Rendering a self-contained HTML string as an anywidget
# ---------------------------------------------------------------------------
# ESM for a minimal anywidget that hosts a pre-built HTML string (styles +
# markup + <script> blocks). It injects the HTML into the widget's light DOM
# (which, in notebook hosts like VS Code, lives in the output *webview* rather
# than the nested, sandboxed ``srcdoc`` iframe used for raw ``display(HTML)``
# output). That matters for the full-screen toggle: the webview permits the
# native Fullscreen API, so ``root.requestFullscreen()`` succeeds and the tool
# expands edge-to-edge — exactly like the other anywidget-based tools — instead
# of being blocked and collapsing inside a content-sized sandbox iframe.
#
# Scripts injected via ``innerHTML`` do not execute, so each ``<script>`` is
# re-created as a fresh element (after the markup is attached to the document)
# so the browser runs it and its DOM lookups resolve.
_HTML_WIDGET_ESM: str = r"""
function render({ model, el }) {
    var html = model.get("html") || "";
    var holder = document.createElement("div");
    holder.innerHTML = html;
    el.appendChild(holder);
    var scripts = holder.querySelectorAll("script");
    for (var i = 0; i < scripts.length; i++) {
        var old = scripts[i];
        var s = document.createElement("script");
        for (var j = 0; j < old.attributes.length; j++) {
            s.setAttribute(old.attributes[j].name, old.attributes[j].value);
        }
        s.textContent = old.textContent;
        old.parentNode.replaceChild(s, old);
    }
}
export default { render };
"""


def display_html_widget(html: str, fallback: bool = True) -> None:
    """Display a self-contained HTML string via a lightweight anywidget.

    The HTML (styles + markup + inline ``<script>`` blocks) is rendered into the
    notebook output **webview's light DOM** instead of the nested, sandboxed
    ``srcdoc`` iframe that hosts raw ``IPython.display.HTML`` output. This gives
    script-driven widgets — e.g. the *Vertipaq* analyzer — the same full-screen
    behavior as the other anywidget-based tools (the native Fullscreen API is
    permitted in the webview but blocked in the sandbox iframe).

    Parameters
    ----------
    html : str
        A complete HTML fragment: any ``<style>``, the markup, and the inline
        ``<script>`` blocks that drive it. Scripts are re-executed after the
        markup is attached so their DOM lookups resolve.
    fallback : bool, default=True
        When ``True`` and the ``anywidget`` package is not installed, fall back
        to ``IPython.display.display(HTML(html))``. When ``False``, raise
        ``ImportError`` instead.
    """
    from IPython.display import display

    try:
        import anywidget
        import traitlets
    except ImportError:
        if fallback:
            from IPython.display import HTML

            display(HTML(html))
            return
        raise

    class _HtmlWidget(anywidget.AnyWidget):
        _esm = _HTML_WIDGET_ESM
        html = traitlets.Unicode("").tag(sync=True)

    display(_HtmlWidget(html=html))


# ---------------------------------------------------------------------------
# "Powered by Semantic Link Labs" attribution
# ---------------------------------------------------------------------------
ATTRIBUTION_CSS: str = """\
.sl-attribution {
    margin-top: 14px;
    margin-bottom: 8px;
    padding-right: 8px;
    text-align: right;
    font-size: 11.5px;
    line-height: 1.5;
    color: var(--ui-text-tertiary);
}
.sl-attribution a {
    color: var(--ui-text-tertiary);
    text-decoration: none;
    transition: color 120ms ease;
}
.sl-attribution a:hover {
    color: var(--ui-accent);
    text-decoration: none;
}
"""


def scoped_attribution_css(root_selector: str) -> str:
    """Return :data:`ATTRIBUTION_CSS` with every rule prefixed by ``root_selector``.

    Parameters
    ----------
    root_selector : str
        A CSS selector for the widget's root container.

    Returns
    -------
    str
        The scoped CSS as a single string.
    """
    return _scope_css(root_selector, ATTRIBUTION_CSS)


def render_attribution_html(
    extra_links: Optional[Sequence[Tuple[str, str]]] = None,
) -> str:
    """Render the standard "Powered by Semantic Link Labs" attribution.

    The rendered link uses the tertiary text color by default and animates
    to the accent color on hover (see :data:`ATTRIBUTION_CSS`).

    Parameters
    ----------
    extra_links : Sequence[tuple[str, str]], default=None
        Optional additional ``(label, url)`` pairs to append after the
        Semantic Link Labs link, separated by bullets (e.g.
        ``[("Vertipaq Analyzer", "https://www.sqlbi.com/tools/vertipaq-analyzer/")]``).

    Returns
    -------
    str
        The attribution HTML fragment. The caller is responsible for
        including :data:`ATTRIBUTION_CSS` (or a scoped variant) on the
        page.
    """
    parts = [
        'Powered by <a href="https://github.com/microsoft/semantic-link-labs" '
        'target="_blank" rel="noopener noreferrer">Semantic Link Labs</a>'
    ]
    if extra_links:
        for label, url in extra_links:
            parts.append(
                f'<a href="{_escape_html(url)}" target="_blank" '
                f'rel="noopener noreferrer">{_escape_html(label)}</a>'
            )
    body = " &bull; ".join(parts)
    return f'<div class="sl-attribution">{body}</div>'


# ---------------------------------------------------------------------------
# Reusable progress bar (in-place updating HTML progress indicator)
# ---------------------------------------------------------------------------
class ProgressBar:
    """A modern, theme-aware HTML/CSS progress bar for notebook output.

    Renders an in-place updating progress bar that matches the visual
    language of the interactive Semantic Link Labs widgets (shared theme
    tokens, typography, radii, and motion). Use it as a drop-in replacement
    for text-based progress indicators (e.g. ``tqdm``) inside long-running
    loops that run in a notebook cell.

    The bar is displayed as soon as the instance is created and updated in
    place via :meth:`update`. Call :meth:`close` when the work is done to
    stop the animation and show the final state.

    Parameters
    ----------
    total : int
        The total number of steps the loop will perform. If ``0`` (or less),
        the bar renders as complete.
    title : str, default="Processing…"
        The label shown above the bar.
    dark_mode : bool, default=False
        If True, renders the bar with the dark color palette. A CSS overlay
        keeps the bar consistent with the surrounding widgets.
    """

    def __init__(
        self,
        total: int,
        title: str = "Processing…",
        dark_mode: bool = False,
    ) -> None:
        self._total = max(int(total), 0)
        self._title = title
        self._dark_mode = dark_mode
        self._uid = uuid.uuid4().hex[:8]
        self._current = 0
        self._closed = False
        self._handle = self._display(self._render(0, ""))

    @staticmethod
    def _display(html: str):
        try:
            from IPython.display import display, HTML

            return display(HTML(html), display_id=True)
        except Exception:
            return None

    def _render(self, current: int, description: str) -> str:
        uid = self._uid
        if self._total > 0:
            pct = min(max(current / self._total * 100.0, 0.0), 100.0)
        else:
            pct = 100.0
        count_text = (
            f"{current:,} / {self._total:,} &middot; {pct:.0f}%"
            if self._total > 0
            else f"{pct:.0f}%"
        )
        desc = _escape_html(description) if description else "&nbsp;"
        root_cls = f"slpb-{uid}-root" + (" slpb-dark" if self._dark_mode else "")
        return f"""
<style>
    .slpb-{uid}-root {{
        {LIGHT_THEME_VARS}
        font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'SF Pro Text',
                     'Helvetica Neue', Helvetica, Arial, sans-serif;
        box-sizing: border-box;
        max-width: 1200px;
        margin: 12px auto;
        padding: 16px 18px;
        background: var(--ui-bg);
        border: 1px solid var(--ui-border);
        border-radius: 12px;
        box-shadow: var(--ui-shadow-sm);
        color: var(--ui-text);
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }}
    .slpb-{uid}-root.slpb-dark {{
        {DARK_THEME_VARS}
    }}
    .slpb-{uid}-root *, .slpb-{uid}-root *::before, .slpb-{uid}-root *::after {{
        box-sizing: border-box;
    }}
    .slpb-{uid}-head {{
        display: flex;
        align-items: baseline;
        justify-content: space-between;
        gap: 12px;
        margin-bottom: 10px;
    }}
    .slpb-{uid}-title {{
        font-size: 14px;
        font-weight: 600;
        letter-spacing: -0.01em;
        color: var(--ui-text);
    }}
    .slpb-{uid}-count {{
        font-size: 12px;
        font-weight: 500;
        color: var(--ui-text-secondary);
        font-variant-numeric: tabular-nums;
        white-space: nowrap;
    }}
    .slpb-{uid}-track {{
        position: relative;
        height: 8px;
        border-radius: 999px;
        background: var(--ui-bg-secondary);
        border: 1px solid var(--ui-border);
        overflow: hidden;
    }}
    .slpb-{uid}-fill {{
        position: absolute;
        left: 0;
        top: 0;
        bottom: 0;
        border-radius: 999px;
        background: linear-gradient(90deg, var(--ui-accent), var(--ui-accent-hover));
        transition: width 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }}
    .slpb-{uid}-fill::after {{
        content: '';
        position: absolute;
        inset: 0;
        border-radius: 999px;
        background-image: linear-gradient(90deg,
            rgba(255, 255, 255, 0) 0%,
            rgba(255, 255, 255, 0.28) 50%,
            rgba(255, 255, 255, 0) 100%);
        background-size: 200% 100%;
        animation: slpb-{uid}-shimmer 1.2s linear infinite;
    }}
    @keyframes slpb-{uid}-shimmer {{
        0% {{ background-position: 200% 0; }}
        100% {{ background-position: -200% 0; }}
    }}
    .slpb-{uid}-desc {{
        margin-top: 9px;
        font-size: 12px;
        color: var(--ui-text-tertiary);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }}
</style>
<div class="{root_cls}">
    <div class="slpb-{uid}-head">
        <span class="slpb-{uid}-title">{_escape_html(self._title)}</span>
        <span class="slpb-{uid}-count">{count_text}</span>
    </div>
    <div class="slpb-{uid}-track">
        <div class="slpb-{uid}-fill" style="width:{pct:.1f}%"></div>
    </div>
    <div class="slpb-{uid}-desc">{desc}</div>
</div>
"""

    def update(self, current: int, description: str = "") -> None:
        """Advance the bar to ``current`` and optionally update the caption.

        Parameters
        ----------
        current : int
            The number of completed steps.
        description : str, default=""
            Optional caption shown beneath the bar (e.g. the item currently
            being processed).
        """
        if self._closed:
            return
        self._current = current
        html = self._render(current, description)
        if self._handle is not None:
            self._handle.update(_HTML(html))

    def close(self, description: str = "") -> None:
        """Remove the progress bar from the output once the work is done.

        Parameters
        ----------
        description : str, default=""
            Unused; accepted for backward compatibility.
        """
        if self._closed:
            return
        self._closed = True
        if self._handle is not None:
            self._handle.update(_HTML(""))


def _HTML(html: str):
    from IPython.display import HTML

    return HTML(html)
