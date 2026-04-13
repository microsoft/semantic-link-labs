# Shared UI components for PBI Fixer explorer tabs.
# Provides theme constants, icon definitions, tree-building utilities,
# and reusable layout helpers for the Semantic Model and Report explorer tabs.

import ipywidgets as widgets

# ---------------------------------------------------------------------------
# Theme constants (shared across all tabs)
# ---------------------------------------------------------------------------
FONT_FAMILY = "-apple-system,BlinkMacSystemFont,sans-serif"
TEXT_COLOR = "inherit"
BORDER_COLOR = "#e0e0e0"
ICON_ACCENT = "#FF9500"
GRAY_COLOR = "#999"
SECTION_BG = "#fafafa"

# ---------------------------------------------------------------------------
# Icons for tree nodes (Unicode)
# ---------------------------------------------------------------------------
ICONS = {
    "table": "\U0001F4C1",        # 📁
    "column": "\U0001F4CF",        # 📏
    "measure": "\U0001F4D0",       # 📐
    "hierarchy": "\U0001F517",     # 🔗
    "calc_group": "\U0001F4CA",    # 📊
    "calc_item": "\u2022",         # •
    "model": "\U0001F4C4",         # 📄 (document = semantic model)
    "report": "\U0001F4CA",        # 📊 (chart = report)
    "page": "\U0001F4C4",         # 📄
    "visual": "\U0001F441",        # 👁
    "partition": "\U0001F4CE",     # 📎
    "folder": "\U0001F4C2",        # 📂
    "relationship": "\u2194",      # ↔
}

# Collapse/expand markers
EXPANDED = "\u25BC"   # ▼
COLLAPSED = "\u25B6"  # ▶

# Indentation string per level (using non-breaking spaces for Select widget)
_INDENT = "\u00A0\u00A0\u00A0\u00A0"  # 4 nbsp per level


def build_tree_items(items):
    """
    Build option list and reverse-lookup map for a widgets.Select tree.

    Parameters
    ----------
    items : list[tuple[int, str, str, str]]
        Each tuple is (indent_level, icon_key, display_label, object_key).
        ``icon_key`` must be a key in ``ICONS`` or a literal character.
        ``object_key`` is an opaque string used for reverse lookup.

    Returns
    -------
    options : list[str]
        Formatted strings for ``widgets.Select.options``.
    key_map : dict[str, str]
        Maps each formatted option string → its ``object_key``.
    """
    options = []
    key_map = {}
    seen = {}  # track duplicate display strings
    for indent, icon_key, label, obj_key in items:
        icon = ICONS.get(icon_key, icon_key)
        formatted = f"{_INDENT * indent}{icon} {label}"
        # Ensure uniqueness — append invisible counter for duplicates
        if formatted in key_map:
            count = seen.get(formatted, 1) + 1
            seen[formatted] = count
            # Use zero-width spaces to make string unique but visually identical
            formatted = formatted + ("\u200b" * count)
        options.append(formatted)
        key_map[formatted] = obj_key
    return options, key_map


def create_three_panel_layout(tree_widget, preview_widget, properties_widget):
    """
    Create the three-panel explorer layout:
    left = tree (fixed 280px), right = preview (top) + properties (bottom).

    Returns a single HBox widget.
    """
    right_panel = widgets.VBox(
        [preview_widget, properties_widget],
        layout=widgets.Layout(
            flex="1",
            gap="8px",
        ),
    )
    return widgets.HBox(
        [tree_widget, right_panel],
        layout=widgets.Layout(
            width="100%",
            gap="8px",
        ),
    )


def create_connection_bar(*children):
    """
    Wrap connection-bar children (inputs, buttons, status) in a styled HBox.
    """
    return widgets.HBox(
        list(children),
        layout=widgets.Layout(
            align_items="center",
            gap="8px",
            padding="8px 12px",
            margin="0 0 8px 0",
            border=f"1px solid {BORDER_COLOR}",
            border_radius="8px",
        ),
    )


def input_label(text):
    """Small styled label for connection bar inputs."""
    return widgets.HTML(
        value=f'<span style="font-size:13px; font-weight:500; color:{TEXT_COLOR}; '
        f"font-family:{FONT_FAMILY}; min-width:80px; display:inline-block;\">"
        f"{text}</span>"
    )


def status_html(msg="", color=GRAY_COLOR):
    """Returns a styled status HTML widget."""
    w = widgets.HTML(value="")
    if msg:
        set_status(w, msg, color)
    return w


def set_status(widget, msg, color):
    """Update a status HTML widget with a styled message."""
    widget.value = (
        f'<div style="padding:4px 8px; border-radius:6px; '
        f"background:{color}1a; color:{color}; font-size:13px; "
        f'font-family:{FONT_FAMILY};">{msg}</div>'
    )


def placeholder_panel(text, min_height="120px"):
    """Gray placeholder panel for 'coming soon' sections."""
    return widgets.HTML(
        value=f'<div style="padding:16px; color:{GRAY_COLOR}; font-size:13px; '
        f"font-family:{FONT_FAMILY}; text-align:center; "
        f'font-style:italic;">{text}</div>',
        layout=widgets.Layout(
            border=f"1px solid {BORDER_COLOR}",
            border_radius="8px",
            min_height=min_height,
            background_color=SECTION_BG,
        ),
    )


def panel_box(children, flex="1", min_height=None):
    """
    Consistent styled panel (used for preview and properties boxes).
    Ensures both panels have the same border, background, and radius.
    """
    layout_args = {
        "flex": flex,
        "border": f"1px solid {BORDER_COLOR}",
        "border_radius": "8px",
        "padding": "8px",
        "background_color": SECTION_BG,
    }
    if min_height:
        layout_args["min_height"] = min_height
    return widgets.VBox(children, layout=widgets.Layout(**layout_args))
