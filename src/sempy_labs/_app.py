"""An app-style launcher for the interactive tools in Semantic Link Labs.

The look and feel mirrors the Fabric Tools app: a Fluent palette, a splash
screen of tool cards filtered by Fabric item type, and a shell which keeps every
opened tool mounted so switching between tools (and back home) is instant.
"""

from contextlib import contextmanager
from typing import Dict, List, Optional
from sempy._utils._log import log
from sempy_labs._ui_components import (
    ICONS as _UI_ICONS,
    fullscreen_css as _ui_fullscreen_css,
    fullscreen_setup_js as _ui_fullscreen_setup_js,
    render_attribution_html as _ui_render_attribution_html,
    scoped_attribution_css as _ui_scoped_attribution_css,
    scoped_button_press_css as _ui_scoped_button_press_css,
)

# Every tool the launcher can open. ``module`` / ``function`` are imported
# lazily so opening the launcher does not import every tool in the library.
_TOOLS: tuple = (
    {
        "key": "dax_perf_optimizer",
        "name": "DAX Perf Optimizer",
        "description": "Profile and optimize slow DAX queries.",
        "tags": ("Semantic Model",),
        "icon": "dax_performance",
        "module": "sempy_labs.semantic_model._dax_perf",
        "function": "dax_perf_optimizer",
    },
    {
        "key": "vertipaq_analyzer",
        "name": "Vertipaq Analyzer",
        "description": "Analyze the memory footprint of a semantic model.",
        "tags": ("Semantic Model", "Direct Lake"),
        "icon": "vertipaq",
        "module": "sempy_labs.semantic_model._vertipaq_analyzer",
        "function": "vertipaq_analyzer",
    },
    {
        "key": "lineage_view",
        "name": "Lineage View",
        "description": "Explore how tables, columns and measures depend on each other.",
        "tags": ("Semantic Model",),
        "icon": "workflow",
        "module": "sempy_labs.semantic_model._lineage_view",
        "function": "lineage_view",
    },
    {
        "key": "mini_model_manager",
        "name": "Mini Model Manager",
        "description": "Create a smaller semantic model from a subset of a master model.",
        "tags": ("Semantic Model", "Direct Lake"),
        "icon": "mini_model",
        "module": "sempy_labs.semantic_model._mini_model_manager",
        "function": "mini_model_manager",
    },
    {
        "key": "find_unused_objects",
        "name": "Find Unused Objects",
        "description": "Find tables, columns and measures which are never used.",
        "tags": ("Semantic Model", "Report"),
        "icon": "scan_search",
        "module": "sempy_labs.semantic_model._find_unused_objects",
        "function": "find_unused_objects",
    },
    {
        "key": "perspective_editor",
        "name": "Perspective Editor",
        "description": "Create and manage perspectives in a semantic model.",
        "tags": ("Semantic Model",),
        "icon": "perspective",
        "module": "sempy_labs.semantic_model._perspective_editor",
        "function": "perspective_editor",
    },
    {
        "key": "bpa",
        "name": "Semantic Model BPA",
        "description": "Run Best Practice Analyzer rules against a semantic model.",
        "tags": ("Semantic Model",),
        "icon": "shield_check",
        "module": "sempy_labs.semantic_model._bpa",
        "function": "bpa",
    },
    {
        "key": "refresh_manager",
        "name": "Refresh Manager",
        "description": "Refresh a semantic model, its tables or individual partitions.",
        "tags": ("Semantic Model",),
        "icon": "sync",
        "module": "sempy_labs.semantic_model._refresh_manager",
        "function": "refresh_manager",
    },
    {
        "key": "migrate_to_direct_lake",
        "name": "Direct Lake Migration",
        "description": "Migrate an import or DirectQuery model to Direct Lake.",
        "tags": ("Semantic Model", "Direct Lake"),
        "icon": "database_zap",
        "module": "sempy_labs.semantic_model._direct_lake_migration",
        "function": "migrate_to_direct_lake",
    },
    {
        "key": "delta_analyzer",
        "name": "Delta Analyzer",
        "description": "Analyze the parquet files and row groups of a delta table.",
        "tags": ("Direct Lake", "Lakehouse"),
        "icon": "delta_stats",
        "module": "sempy_labs._delta_analyzer",
        "function": "delta_analyzer",
    },
    {
        "key": "model_comparison",
        "name": "Model Comparison",
        "description": "Compare the metadata of two semantic models side by side.",
        "tags": ("Semantic Model",),
        "icon": "git_compare",
        "module": "sempy_labs.semantic_model._model_comparison",
        "function": "model_comparison",
    },
)

_TOOLS_BY_KEY = {tool["key"]: tool for tool in _TOOLS}

# Fabric item types, in the order shown in the splash-screen filter.
_CATEGORY_ORDER = ("Semantic Model", "Direct Lake", "Report", "Lakehouse", "Admin")


def _tool_payload() -> List[dict]:
    """The tool catalog as sent to the frontend (icons resolved to SVG)."""

    return [
        {
            "key": tool["key"],
            "name": tool["name"],
            "description": tool["description"],
            "tags": list(tool["tags"]),
            "icon": _UI_ICONS[tool["icon"]],
        }
        for tool in _TOOLS
    ]


def _category_payload() -> List[str]:
    tags = {tag for tool in _TOOLS for tag in tool["tags"]}
    return ["All"] + [tag for tag in _CATEGORY_ORDER if tag in tags]


def _run_tool(tool: dict, dark_mode: bool) -> None:
    import importlib

    module = importlib.import_module(tool["module"])
    getattr(module, tool["function"])(dark_mode=dark_mode)


@contextmanager
def _capture_displayed_widgets(collected: list):
    """Intercept ``display`` so a tool's widget can be hosted by the launcher.

    The widget object is taken from the call rather than captured with an
    ``ipywidgets.Output``: notebook hosts (Fabric in particular) provide their
    own ``display`` which bypasses that capture, which would leave the tool
    rendered outside the launcher, and would make clearing the output wipe the
    whole cell. Anything which is not a widget is displayed as usual.
    """

    import builtins
    import ipywidgets
    import IPython.display as ipython_display

    passthrough = ipython_display.display
    targets = [(ipython_display, "display")]
    try:
        import IPython.core.display_functions as display_functions

        targets.append((display_functions, "display"))
    except ImportError:
        pass
    if hasattr(builtins, "display"):
        targets.append((builtins, "display"))
    originals = [(module, name, getattr(module, name)) for module, name in targets]

    def _display(*objects, **kwargs):
        widgets = [o for o in objects if isinstance(o, ipywidgets.Widget)]
        if not widgets:
            return passthrough(*objects, **kwargs)
        collected.extend(widgets)
        rest = [o for o in objects if not isinstance(o, ipywidgets.Widget)]
        return passthrough(*rest, **kwargs) if rest else None

    try:
        for module, name, _ in originals:
            setattr(module, name, _display)
        yield
    finally:
        for module, name, original in originals:
            setattr(module, name, original)


# The Fluent palette of the Fabric Tools app, mapped onto the --ui-* tokens the
# shared UI components read.
_LIGHT_VARS = """\
--ui-bg: #ffffff;
--ui-bg-solid: #ffffff;
--ui-bg-secondary: #fafafa;
--ui-bg-tertiary: #fafafa;
--ui-bg-hover: #f5f5f5;
--ui-surface: #ffffff;
--ui-surface-2: #f0f0f0;
--ui-border: #e0e0e0;
--ui-border-strong: #e0e0e0;
--ui-border-hover: rgba(15, 108, 189, 0.4);
--ui-text: #242424;
--ui-text-secondary: #616161;
--ui-text-tertiary: #616161;
--ui-accent: #0f6cbd;
--ui-accent-hover: #115ea3;
--ui-accent-soft: rgba(15, 108, 189, 0.1);
--ui-on-accent: #ffffff;
--ui-danger: #c50f1f;
--ui-danger-hover: #a80f1c;
--ui-danger-bg: rgba(197, 15, 31, 0.08);
--ui-danger-border: rgba(197, 15, 31, 0.35);
--ui-danger-text: #c50f1f;
--ui-shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.06);
--ui-shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -2px rgba(0, 0, 0, 0.1);
--ui-shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -4px rgba(0, 0, 0, 0.1);
"""

_DARK_VARS = """\
--ui-bg: #1e1e22;
--ui-bg-solid: #1e1e22;
--ui-bg-secondary: #1f1f1f;
--ui-bg-tertiary: #1f1f1f;
--ui-bg-hover: #3d3d3d;
--ui-surface: #292929;
--ui-surface-2: #141414;
--ui-border: #525252;
--ui-border-strong: #525252;
--ui-border-hover: rgba(71, 158, 245, 0.5);
--ui-text: #ffffff;
--ui-text-secondary: #adadad;
--ui-text-tertiary: #adadad;
--ui-accent: #115ea3;
--ui-accent-hover: #479ef5;
--ui-accent-soft: rgba(71, 158, 245, 0.16);
--ui-on-accent: #ffffff;
--ui-danger: #c50f1f;
--ui-danger-hover: #a80f1c;
--ui-danger-bg: rgba(197, 15, 31, 0.2);
--ui-danger-border: rgba(197, 15, 31, 0.45);
--ui-danger-text: #ff8a94;
--ui-shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.4);
--ui-shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.5), 0 2px 4px -2px rgba(0, 0, 0, 0.4);
--ui-shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.55), 0 4px 6px -4px rgba(0, 0, 0, 0.4);
"""

_FONT_STACK = (
    "'Segoe UI', 'Segoe UI Web (West European)', -apple-system, "
    "BlinkMacSystemFont, Roboto, 'Helvetica Neue', sans-serif"
)

_WIDGET_CSS = (
    """
.slls-app {
"""
    + _LIGHT_VARS
    + f"    font-family: {_FONT_STACK};"
    + """
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    width: 100%;
    background: var(--ui-bg);
    color: var(--ui-text);
    box-sizing: border-box;
    position: relative;
}
@media (prefers-color-scheme: dark) {
    .slls-app.slls-app-auto {
"""
    + _DARK_VARS
    + """
    }
}
.slls-app.slls-app-dark {
"""
    + _DARK_VARS
    + """
}
.slls-app * { box-sizing: border-box; }

/* ---------------- Shell ---------------- */
/* The launcher and every opened tool live in one container, so full screen and
   the Home button survive navigating between them. */
.slls-app-shell {
"""
    + _LIGHT_VARS
    + """
    width: 100%;
    background: var(--ui-bg);
}
@media (prefers-color-scheme: dark) {
    .slls-app-shell.slls-app-auto {
"""
    + _DARK_VARS
    + """
    }
}
.slls-app-shell.slls-app-dark {
"""
    + _DARK_VARS
    + """
}
.slls-app-shell, .slls-app-shell > * { box-sizing: border-box; }
.slls-app-tool { width: 100%; padding: 0 20px 24px; }

/* ---------------- Header ---------------- */
.slls-app-topbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 16px 20px;
    flex-wrap: wrap;
}
.slls-app-topbar-group {
    display: flex;
    align-items: center;
    gap: 12px;
    min-width: 0;
}
.slls-app-topbar-actions { display: flex; align-items: center; gap: 8px; }
.slls-app-brand {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 36px;
    height: 36px;
    flex: 0 0 auto;
    border-radius: 8px;
    background: var(--ui-accent);
    color: var(--ui-on-accent);
}
.slls-app-brand svg { display: block; width: 20px; height: 20px; }
.slls-app-brand-name {
    font-size: 16px;
    font-weight: 600;
    color: var(--ui-text);
}

/* ---------------- Buttons ---------------- */
.slls-app-btn {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    height: 28px;
    padding: 0 12px;
    flex: 0 0 auto;
    border: 1px solid var(--ui-border);
    border-radius: 6px;
    background: var(--ui-surface);
    color: var(--ui-text);
    font-family: inherit;
    font-size: 12px;
    font-weight: 500;
    cursor: pointer;
    transition: background 120ms ease, color 120ms ease, border-color 120ms ease;
}
.slls-app-btn svg { display: block; width: 16px; height: 16px; }
.slls-app-btn:hover { background: var(--ui-bg-hover); }
.slls-app-btn-icon {
    width: 28px;
    padding: 0;
    justify-content: center;
    color: var(--ui-text-secondary);
}
.slls-app-btn-icon:hover { color: var(--ui-text); }

/* Light / dark switcher, matching the app's segmented control. */
.slls-app-seg {
    display: inline-flex;
    align-items: center;
    gap: 2px;
    padding: 2px;
    border: 1px solid var(--ui-border);
    border-radius: 6px;
    background: var(--ui-surface);
}
.slls-app-seg-btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 28px;
    height: 28px;
    padding: 0;
    border: none;
    border-radius: 4px;
    background: transparent;
    color: var(--ui-text-secondary);
    cursor: pointer;
    transition: background 120ms ease, color 120ms ease;
}
.slls-app-seg-btn svg { display: block; width: 16px; height: 16px; }
.slls-app-seg-btn:hover { background: var(--ui-bg-hover); color: var(--ui-text); }
.slls-app-seg-btn.is-active { background: var(--ui-accent); color: var(--ui-on-accent); }

/* ---------------- Hero ---------------- */
.slls-app-main {
    width: 100%;
    max-width: 1024px;
    margin: 0 auto;
    padding: 24px 20px;
}
.slls-app-hero { max-width: 672px; margin-bottom: 32px; }
.slls-app-hero-title {
    margin: 0;
    font-size: 32px;
    line-height: 40px;
    font-weight: 600;
    color: var(--ui-text);
}
.slls-app-hero-sub {
    margin: 12px 0 0;
    font-size: 16px;
    line-height: 22px;
    color: var(--ui-text-secondary);
}

/* ---------------- Getting started ---------------- */
.slls-app-links {
    display: none;
    margin-bottom: 20px;
    padding: 16px;
    border: 1px solid var(--ui-border);
    border-radius: 12px;
    background: var(--ui-surface);
    box-shadow: var(--ui-shadow-sm);
    font-size: 12px;
    line-height: 20px;
    color: var(--ui-text-secondary);
}
.slls-app-links.show { display: block; }
.slls-app-links ul { margin: 8px 0 0; padding-left: 18px; }
.slls-app-links a { color: var(--ui-accent); text-decoration: none; }
.slls-app-links a:hover { text-decoration: underline; }

/* ---------------- Item-type filter ---------------- */
.slls-app-filters {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px;
    margin-bottom: 20px;
}
.slls-app-pill {
    border: 1px solid var(--ui-border);
    border-radius: 9999px;
    background: var(--ui-surface);
    color: var(--ui-text-secondary);
    font-family: inherit;
    font-size: 12px;
    font-weight: 500;
    padding: 4px 12px;
    cursor: pointer;
    transition: background 120ms ease, color 120ms ease, border-color 120ms ease;
}
.slls-app-pill:hover { background: var(--ui-bg-hover); color: var(--ui-text); }
.slls-app-pill.is-active {
    border-color: var(--ui-accent);
    background: var(--ui-accent);
    color: var(--ui-on-accent);
}

/* ---------------- Tool cards ---------------- */
.slls-app-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
    gap: 16px;
}
.slls-app-card {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 12px;
    padding: 16px;
    text-align: left;
    border: 1px solid var(--ui-border);
    border-radius: 12px;
    background: var(--ui-surface);
    color: var(--ui-text);
    font-family: inherit;
    box-shadow: var(--ui-shadow-sm);
    cursor: pointer;
    transition: transform 150ms ease, border-color 150ms ease, box-shadow 150ms ease;
}
.slls-app-card:hover {
    transform: translateY(-2px);
    border-color: var(--ui-border-hover);
    box-shadow: var(--ui-shadow-md);
}
.slls-app-card-icon {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 44px;
    height: 44px;
    border-radius: 8px;
    background: var(--ui-accent-soft);
    color: var(--ui-accent);
    transition: background 150ms ease, color 150ms ease;
}
.slls-app-card-icon svg { display: block; width: 20px; height: 20px; }
.slls-app-card:hover .slls-app-card-icon {
    background: var(--ui-accent);
    color: var(--ui-on-accent);
}
.slls-app-card-text { display: flex; flex-direction: column; gap: 4px; }
.slls-app-card-name { font-size: 16px; font-weight: 600; color: var(--ui-text); }
.slls-app-card-desc {
    font-size: 12px;
    line-height: 20px;
    color: var(--ui-text-secondary);
}
.slls-app-card-tags {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
    margin-top: auto;
    padding-top: 8px;
}
.slls-app-tag {
    border: 1px solid var(--ui-border);
    border-radius: 9999px;
    background: var(--ui-surface-2);
    color: var(--ui-text-secondary);
    font-size: 10px;
    font-weight: 500;
    padding: 2px 8px;
}
.slls-app-empty { font-size: 14px; color: var(--ui-text-secondary); }

/* ---------------- Status ---------------- */
.slls-app-banner {
    display: none;
    align-items: center;
    gap: 12px;
    margin-top: 20px;
    padding: 12px 16px;
    border: 1px solid var(--ui-border);
    border-radius: 8px;
    background: var(--ui-surface-2);
    font-size: 12px;
}
.slls-app-banner.show { display: flex; }
.slls-app-banner.is-error {
    border-color: var(--ui-danger-border);
    background: var(--ui-danger-bg);
    color: var(--ui-danger-text);
}
.slls-app-banner-text { flex: 1 1 auto; min-width: 0; }
.slls-app.slls-app-busy .slls-app-grid,
.slls-app.slls-app-busy .slls-app-filters { pointer-events: none; }

/* A tool is open: the launcher gets out of the way entirely — Back is moved
   into the tool's own header, next to its title. */
.slls-app.slls-app-tool-open .slls-app-main,
.slls-app.slls-app-tool-open .slls-app-topbar { display: none; }
/* Fallback bar for a tool with no header to host the Back button. */
.slls-app.slls-app-tool-open.slls-app-back-parked .slls-app-topbar {
    display: flex;
    padding: 8px 20px 0;
}
.slls-app.slls-app-tool-open .slls-app-brand,
.slls-app.slls-app-tool-open .slls-app-brand-name,
.slls-app.slls-app-tool-open .slls-app-topbar-actions { display: none; }

/* Styled to sit inside the open tool's header, so it inherits that tool's
   theme tokens. */
.slls-app-back {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 40px;
    height: 40px;
    flex: 0 0 auto;
    padding: 0;
    border: 1px solid var(--ui-border, rgba(128, 128, 128, 0.35));
    border-radius: 10px;
    background: transparent;
    color: inherit;
    font: inherit;
    cursor: pointer;
    transition: background 120ms ease, border-color 120ms ease, color 120ms ease;
}
.slls-app-back svg { display: block; width: 20px; height: 20px; }
.slls-app-back:hover {
    border-color: var(--ui-accent, currentColor);
    color: var(--ui-accent, inherit);
}
.slls-app:not(.slls-app-tool-open) .slls-app-back { display: none; }
"""
)

_WIDGET_JS = r"""
function render({ model, el }) {
    const root = document.createElement("div");
    root.className = "slls-app";
    // Full screen and the theme apply to the shell (launcher + open tools) so
    // that opening a tool never drops out of full screen.
    let shellEl = null;
    function shell() {
        if (!shellEl) shellEl = root.closest(".slls-app-shell");
        return shellEl || root;
    }
    function applyTheme() {
        const dm = model.get("dark_mode");
        for (const node of [root, shell()]) {
            node.classList.remove("slls-app-dark", "slls-app-auto");
            if (dm === true) node.classList.add("slls-app-dark");
            else if (dm === null || dm === undefined) node.classList.add("slls-app-auto");
        }
    }
    applyTheme();
    model.on("change:dark_mode", applyTheme);
    model.on("change:dark_mode", () => syncToolChrome());
    el.appendChild(root);

    let activeCategory = "All";

    // ---------------- Header ----------------
    const topbar = document.createElement("div");
    topbar.className = "slls-app-topbar";
    root.appendChild(topbar);

    const left = document.createElement("div");
    left.className = "slls-app-topbar-group";
    topbar.appendChild(left);

    const brand = document.createElement("span");
    brand.className = "slls-app-brand";
    brand.innerHTML = `__SLLS_ICON_SPARKLES__`;
    left.appendChild(brand);

    const brandName = document.createElement("span");
    brandName.className = "slls-app-brand-name";
    brandName.textContent = "Fabric Tools";
    left.appendChild(brandName);

    const backBtn = document.createElement("button");
    backBtn.type = "button";
    backBtn.className = "slls-app-back";
    backBtn.innerHTML = `__SLLS_ICON_ARROW_LEFT__`;
    backBtn.title = "Back to all tools";
    backBtn.setAttribute("aria-label", backBtn.title);
    backBtn.addEventListener("click", () => send({ action: "home" }));
    left.appendChild(backBtn);

    const actions = document.createElement("div");
    actions.className = "slls-app-topbar-actions";
    topbar.appendChild(actions);

    const linksBtn = document.createElement("button");
    linksBtn.type = "button";
    linksBtn.className = "slls-app-btn";
    linksBtn.innerHTML = `__SLLS_ICON_BOOK__<span>Getting started</span>`;
    actions.appendChild(linksBtn);

    const fsBtn = document.createElement("button");
    fsBtn.type = "button";
    fsBtn.className = "slls-app-btn slls-app-btn-icon";
    actions.appendChild(fsBtn);
    applyTheme();
    sllsSetupFullscreen(shell(), fsBtn, "slls-app-fs",
        `__SLLS_ICON_FULLSCREEN__`, `__SLLS_ICON_FULLSCREEN_EXIT__`);
    new MutationObserver(placeBack).observe(shell(),
        { childList: true, subtree: true });
    shell().addEventListener("click", interceptToolChrome, true);

    const themeGroup = document.createElement("div");
    themeGroup.className = "slls-app-seg";
    themeGroup.setAttribute("role", "group");
    themeGroup.setAttribute("aria-label", "Color theme");
    actions.appendChild(themeGroup);

    const themeButtons = [
        { dark: false, label: "Light mode", icon: `__SLLS_ICON_SUN__` },
        { dark: true, label: "Dark mode", icon: `__SLLS_ICON_MOON__` },
    ].map((option) => {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "slls-app-seg-btn";
        button.innerHTML = option.icon;
        button.title = option.label;
        button.setAttribute("aria-label", option.label);
        button.addEventListener("click", () => {
            model.set("dark_mode", option.dark);
            model.save_changes();
        });
        themeGroup.appendChild(button);
        return { button: button, dark: option.dark };
    });
    function renderTheme() {
        const isDark = model.get("dark_mode") === true;
        for (const entry of themeButtons) {
            const active = entry.dark === isDark;
            entry.button.classList.toggle("is-active", active);
            entry.button.setAttribute("aria-pressed", String(active));
        }
    }
    model.on("change:dark_mode", renderTheme);
    renderTheme();

    // ---------------- Hero ----------------
    const main = document.createElement("div");
    main.className = "slls-app-main";
    root.appendChild(main);

    const hero = document.createElement("div");
    hero.className = "slls-app-hero";
    main.appendChild(hero);

    const heroTitle = document.createElement("h1");
    heroTitle.className = "slls-app-hero-title";
    heroTitle.textContent = "Tools for Fabric devs & admins";
    hero.appendChild(heroTitle);

    const heroSub = document.createElement("p");
    heroSub.className = "slls-app-hero-sub";
    heroSub.textContent =
        "A growing toolbox for working with Microsoft Fabric. Pick a tool to get started.";
    hero.appendChild(heroSub);

    const links = document.createElement("div");
    links.className = "slls-app-links";
    links.innerHTML = `__SLLS_LINKS__`;
    main.appendChild(links);
    linksBtn.addEventListener("click", () => links.classList.toggle("show"));

    // ---------------- Item-type filter ----------------
    const filters = document.createElement("div");
    filters.className = "slls-app-filters";
    filters.setAttribute("role", "group");
    filters.setAttribute("aria-label", "Filter tools by item type");
    main.appendChild(filters);

    const grid = document.createElement("div");
    grid.className = "slls-app-grid";
    main.appendChild(grid);

    const empty = document.createElement("p");
    empty.className = "slls-app-empty";
    empty.textContent = "No tools match this item type yet.";
    empty.style.display = "none";
    main.appendChild(empty);

    // ---------------- Status ----------------
    const banner = document.createElement("div");
    banner.className = "slls-app-banner";
    const bannerText = document.createElement("div");
    bannerText.className = "slls-app-banner-text";
    banner.appendChild(bannerText);
    main.appendChild(banner);

    const attribution = document.createElement("div");
    attribution.innerHTML = `__SLLS_ATTRIBUTION__`;
    main.appendChild(attribution);

    // ---------------- Behavior ----------------
    function send(action) {
        model.set("pending_action", action);
        model.set("run", (model.get("run") || 0) + 1);
        model.save_changes();
    }

    function activeTool() {
        const key = model.get("active_tool") || "";
        return (model.get("tools") || []).find((tool) => tool.key === key) || null;
    }

    // The Back button is moved into the open tool's own header, so it sits next
    // to that tool's title rather than on a bar above it. Tools rebuild their
    // header when they re-render, so placement is re-applied on DOM changes.
    function activeToolNode() {
        for (const node of shell().querySelectorAll(".slls-app-tool")) {
            if (node.style.display !== "none") return node;
        }
        return null;
    }

    function toolHeader(node) {
        const candidates = [...node.querySelectorAll("header, [class*='head']")]
            .filter((el) =>
                !el.closest("[class*='modal'],[class*='dialog'],[class*='overlay'],[class*='popover']")
                && el.getClientRects().length > 0);
        const isHeader = (el) => el.tagName === "HEADER"
            || [...el.classList].some((name) => name.endsWith("-header"));
        // Some tools wrap their header in an outer element that matches too
        // (e.g. .vpx-header > .sl-header). The innermost match is the flex row
        // holding the title, so Back lands to the left of the tool's icon.
        const innermost = (list) => list.find((el) =>
            !list.some((other) => other !== el && el.contains(other))) || null;
        return innermost(candidates.filter(isHeader))
            || innermost(candidates.filter((el) =>
                [...el.classList].some((n) => n.endsWith("-head"))));
    }

    function park() {
        if (backBtn.parentElement !== left) left.appendChild(backBtn);
    }

    // ---- The open tool's own full-screen / theme buttons drive the app ----
    // Every tool ships those two controls in its header. While the tool is
    // hosted here they are re-pointed at the app, so full screen belongs to the
    // shell (launcher + tools) and the theme stays consistent across tools.
    const ctlLabel = (btn) =>
        (btn.getAttribute("aria-label") || btn.title || "").toLowerCase();
    const isThemeCtl = (label) => label.includes("light mode") || label.includes("dark mode");
    const isFullscreenCtl = (label) => label.includes("full screen") || label.includes("fullscreen");

    let syntheticClick = false;

    // Remembers the state each tool control was last driven to, so a pending
    // toggle (anywidget tools round-trip through the kernel) is not repeated.
    const driven = new WeakMap();
    function drive(btn, want, isOn, click) {
        if (!btn) return;
        if (isOn(btn) === want) { driven.set(btn, want); return; }
        if (driven.get(btn) === want) return;
        driven.set(btn, want);
        click(btn);
    }

    // Only the theme is mirrored onto a tool. A tool's own full screen is a
    // fixed, full-viewport overlay; stacking one inside the already full-screen
    // shell hides the tool, so the shell keeps sole ownership of full screen.
    function syncToolChrome() {
        const node = activeToolNode();
        if (!node) return;
        let theme = null;
        for (const btn of node.querySelectorAll("button")) {
            if (btn === backBtn) continue;
            if (isThemeCtl(ctlLabel(btn))) { theme = btn; break; }
        }
        drive(theme, model.get("dark_mode") === true,
            (btn) => ctlLabel(btn).includes("light mode"),
            (btn) => { syntheticClick = true; try { btn.click(); } finally { syntheticClick = false; } });
    }

    function interceptToolChrome(event) {
        if (syntheticClick || !event.isTrusted) return;
        const node = activeToolNode();
        const btn = event.target.closest && event.target.closest("button");
        if (!btn || btn === backBtn || !node || !node.contains(btn)) return;
        const label = ctlLabel(btn);
        if (isThemeCtl(label)) {
            model.set("dark_mode", !(model.get("dark_mode") === true));
            model.save_changes();
        } else if (isFullscreenCtl(label)) {
            fsBtn.click();
        } else {
            return;
        }
        // The app now owns the state and mirrors it back onto this button.
        event.preventDefault();
        event.stopPropagation();
    }

    let placeQueued = false;
    function placeBack() {
        if (placeQueued) return;
        placeQueued = true;
        setTimeout(() => {
            placeQueued = false;
            applyBack();
            syncToolChrome();
        });
    }

    function applyBack() {
        if (!activeTool()) {
            park();
            root.classList.remove("slls-app-back-parked");
            return;
        }
        const node = activeToolNode();
        if (!node || node.contains(backBtn)) return;
        const header = toolHeader(node);
        if (!header) {
            park();
            root.classList.add("slls-app-back-parked");
            return;
        }
        root.classList.remove("slls-app-back-parked");
        header.insertBefore(backBtn, header.firstChild);
    }

    function renderView() {
        root.classList.toggle("slls-app-tool-open", !!activeTool());
        links.classList.remove("show");
        placeBack();
    }

    function renderFilters() {
        filters.innerHTML = "";
        for (const category of (model.get("categories") || ["All"])) {
            const pill = document.createElement("button");
            pill.type = "button";
            pill.className = "slls-app-pill"
                + (category === activeCategory ? " is-active" : "");
            pill.textContent = category;
            pill.setAttribute("aria-pressed", String(category === activeCategory));
            pill.addEventListener("click", () => {
                activeCategory = category;
                renderFilters();
                renderGrid();
            });
            filters.appendChild(pill);
        }
    }

    function renderGrid() {
        grid.innerHTML = "";
        const tools = (model.get("tools") || []).filter((tool) =>
            activeCategory === "All" || (tool.tags || []).indexOf(activeCategory) >= 0);
        for (const tool of tools) {
            const card = document.createElement("button");
            card.type = "button";
            card.className = "slls-app-card";
            card.setAttribute("aria-label", `Open ${tool.name}`);

            const icon = document.createElement("span");
            icon.className = "slls-app-card-icon";
            icon.innerHTML = tool.icon || "";
            card.appendChild(icon);

            const text = document.createElement("span");
            text.className = "slls-app-card-text";
            const name = document.createElement("span");
            name.className = "slls-app-card-name";
            name.textContent = tool.name;
            text.appendChild(name);
            const desc = document.createElement("span");
            desc.className = "slls-app-card-desc";
            desc.textContent = tool.description || "";
            text.appendChild(desc);
            card.appendChild(text);

            const tags = document.createElement("span");
            tags.className = "slls-app-card-tags";
            for (const tag of (tool.tags || [])) {
                const chip = document.createElement("span");
                chip.className = "slls-app-tag";
                chip.textContent = tag;
                tags.appendChild(chip);
            }
            card.appendChild(tags);

            card.addEventListener("click", () => {
                root.classList.add("slls-app-busy");
                send({ action: "launch", tool: tool.key });
            });
            grid.appendChild(card);
        }
        empty.style.display = tools.length ? "none" : "";
    }

    function renderBanner() {
        const state = model.get("status") || {};
        const message = state.message || "";
        banner.className = "slls-app-banner"
            + (message ? " show" : "")
            + (state.kind === "error" ? " is-error" : "");
        bannerText.textContent = message;
    }

    model.on("change:status", () => {
        root.classList.remove("slls-app-busy");
        renderBanner();
    });
    model.on("change:active_tool", () => {
        root.classList.remove("slls-app-busy");
        renderView();
        renderBanner();
        shell().scrollTop = 0;
    });
    model.on("change:tools", renderGrid);
    model.on("change:categories", () => { renderFilters(); renderGrid(); });

    renderView();
    renderFilters();
    renderGrid();
    renderBanner();
}
export default { render };
"""

_LINKS_HTML = (
    "Each tool opens in place and stays loaded, so you can switch between them "
    "without losing your work. Every tool can also be called directly from "
    "Python."
    "<ul>"
    '<li><a href="https://semantic-link-labs.readthedocs.io/" target="_blank" '
    'rel="noopener noreferrer">Documentation</a></li>'
    '<li><a href="https://github.com/microsoft/semantic-link-labs/wiki" '
    'target="_blank" rel="noopener noreferrer">Wiki</a></li>'
    '<li><a href="https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples" '
    'target="_blank" rel="noopener noreferrer">Code examples</a></li>'
    "</ul>"
)

_WIDGET_CSS += _ui_scoped_attribution_css(".slls-app")
_WIDGET_CSS += _ui_scoped_button_press_css(".slls-app")
_WIDGET_CSS += "\n" + _ui_fullscreen_css(
    ".slls-app-shell", "slls-app-fs", bg_var="var(--ui-bg)"
)
# Fallback for hosts where the shell container cannot be resolved.
_WIDGET_CSS += "\n" + _ui_fullscreen_css(
    ".slls-app", "slls-app-fs", bg_var="var(--ui-bg)"
)

_WIDGET_JS = _ui_fullscreen_setup_js() + _WIDGET_JS
_WIDGET_JS = (
    _WIDGET_JS.replace("__SLLS_ICON_SUN__", _UI_ICONS["sun"])
    .replace("__SLLS_ICON_MOON__", _UI_ICONS["moon"])
    .replace("__SLLS_ICON_FULLSCREEN__", _UI_ICONS["fullscreen"])
    .replace("__SLLS_ICON_FULLSCREEN_EXIT__", _UI_ICONS["fullscreen_exit"])
    .replace("__SLLS_ICON_SPARKLES__", _UI_ICONS["sparkles"])
    .replace("__SLLS_ICON_BOOK__", _UI_ICONS["book"])
    .replace("__SLLS_ICON_ARROW_LEFT__", _UI_ICONS["arrow_left"])
    .replace("__SLLS_ATTRIBUTION__", _ui_render_attribution_html())
    .replace("__SLLS_LINKS__", _LINKS_HTML)
)


@log
def app(dark_mode: bool = False):
    """
    Displays an interactive launcher for the interactive tools in Semantic Link Labs.

    The launcher shows each tool as a card which can be filtered by Fabric item
    type. Selecting a tool opens it in place, exactly as if its function had
    been called with no arguments (so each tool opens on its own workspace /
    semantic model picker), and the launcher collapses to a header with a Home
    button. Every tool which has been opened stays loaded, so switching between
    tools, or back to the tool list, is instant and each tool resumes where it
    was left. Full screen is retained while navigating.

    Parameters
    ----------
    dark_mode : bool, default=False
        If True, renders the launcher with a dark color theme. If False,
        renders with a light color theme. The tools opened from the launcher
        inherit this setting.
    """

    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "The 'app' function requires the 'anywidget' package. "
            "Install it with: pip install anywidget"
        ) from e

    import ipywidgets
    from IPython.display import display

    class AppWidget(anywidget.AnyWidget):
        _esm = _WIDGET_JS
        _css = _WIDGET_CSS

        tools = traitlets.List().tag(sync=True)
        categories = traitlets.List().tag(sync=True)
        active_tool = traitlets.Unicode("").tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)

    widget = AppWidget(
        tools=_tool_payload(),
        categories=_category_payload(),
        active_tool="",
        status={},
        pending_action={},
        run=0,
        dark_mode=bool(dark_mode),
    )

    # One shell around the launcher and every opened tool, so full screen stays
    # on while navigating between them.
    shell = ipywidgets.VBox([widget])
    shell.add_class("slls-app-shell")
    # Tools stay mounted once opened and are only hidden, so returning to one
    # resumes it exactly where it was left.
    mounted: Dict[str, List] = {}

    def _show(key: str):
        for mounted_key, tool_widgets in mounted.items():
            for tool_widget in tool_widgets:
                tool_widget.layout.display = "" if mounted_key == key else "none"
        widget.active_tool = key if key in mounted else ""

    def _on_run(change):
        data = dict(widget.pending_action or {})
        action = data.get("action")

        if action == "home":
            widget.status = {}
            _show("")
            return
        if action != "launch":
            return

        tool = _TOOLS_BY_KEY.get(str(data.get("tool") or ""))
        if tool is None:
            widget.status = {"message": "Unknown tool.", "kind": "error"}
            return

        if tool["key"] in mounted:
            widget.status = {}
            _show(tool["key"])
            return

        captured: List = []
        error: Optional[Exception] = None
        try:
            with _capture_displayed_widgets(captured):
                _run_tool(tool, bool(widget.dark_mode))
        except Exception as e:
            error = e
        if error is None and not captured:
            error = RuntimeError("the tool did not produce a user interface")

        if error is not None:
            for new_widget in captured:
                new_widget.close()
            _show("")
            widget.status = {
                "message": f"Could not open {tool['name']}: {error}",
                "kind": "error",
            }
            return

        for new_widget in captured:
            new_widget.add_class("slls-app-tool")
        mounted[tool["key"]] = captured
        shell.children = (
            widget,
            *[w for tool_widgets in mounted.values() for w in tool_widgets],
        )
        widget.status = {}
        _show(tool["key"])

    widget.observe(_on_run, names=["run"])

    # The widget reference is kept alive by this closure so the observer keeps
    # firing; the widget is intentionally not returned to avoid a second render.
    display(shell)
