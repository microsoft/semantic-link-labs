"""
Shared UI building blocks used by interactive widgets in Semantic Link Labs
(e.g. ``perspective_editor`` and ``vertipaq_analyzer``).

This module centralizes:

- A library of monochrome SVG ``ICONS`` (use ``currentColor`` so they adapt to
  light/dark themes).
- Apple-inspired light/dark theme CSS variable blocks.
- Helpers to render a reusable widget header (title, dataset/workspace
  subtitle, and a light/dark theme toggle button) and the small bit of
  JavaScript that powers the theme toggle for static-HTML widgets.

The components here are intentionally framework-agnostic: they return plain
strings (HTML / CSS / JS) so they can be embedded in ``IPython.display.HTML``
output, an ``anywidget`` widget, or any other surface that renders raw HTML.
"""

from typing import Optional, Sequence, Tuple

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
    "search": (
        '<svg viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">'
        '<path fill-rule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 '
        "0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 "
        '0 012 8z" clip-rule="evenodd"/></svg>'
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
        'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" '
        'stroke-linejoin="round" aria-hidden="true">'
        '<path d="M4 3v7.5c0 .7.55 1.25 1.25 1.25H12"/>'
        '<path d="M9.5 9.5L12 11.75 9.5 14"/></svg>'
    ),
    "play": (
        '<svg viewBox="0 0 16 16" fill="currentColor" aria-hidden="true">'
        '<path d="M4 2.5v11l9-5.5z"/></svg>'
    ),
    "stop": (
        '<svg viewBox="0 0 16 16" fill="currentColor" aria-hidden="true">'
        '<rect x="4" y="4" width="8" height="8" rx="1.2"/></svg>'
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
    "builder": (
        '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" '
        'stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<rect x="2" y="2.5" width="12" height="11" rx="1.5"/>'
        '<path d="M2 6h12"/><path d="M4.5 9h4"/><path d="M4.5 11h2"/>'
        '<path d="M11.5 9.2v3.2M9.9 10.8h3.2"/></svg>'
    ),
    "close": (
        '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" '
        'stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" '
        'aria-hidden="true">'
        '<path d="M4 4l8 8M12 4l-8 8"/></svg>'
    ),
}


# ---------------------------------------------------------------------------
# Theme CSS variables (Apple-inspired light + dark palettes)
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
.sl-titlewrap {
    display: flex;
    flex-direction: column;
    margin-right: auto;
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
    dark_mode : bool, default=False
        Controls the initial icon shown on the theme toggle button.

    Returns
    -------
    str
        The header HTML fragment. The caller is responsible for including
        :data:`HEADER_CSS` (or otherwise providing the referenced CSS
        custom properties) on the page.
    """
    parts = ['<div class="sl-header">']
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

    parts.append("</div>")  # titlewrap

    if theme_btn_id:
        icon = ICONS["sun"] if dark_mode else ICONS["moon"]
        label = "Switch to light mode" if dark_mode else "Switch to dark mode"
        parts.append(
            f'<button type="button" class="sl-theme-btn" id="{theme_btn_id}" '
            f'title="{label}" aria-label="{label}">{icon}</button>'
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
