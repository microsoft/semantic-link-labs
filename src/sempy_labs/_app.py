"""An app-style launcher for the interactive tools in Semantic Link Labs.

A Fluent palette, a splash screen of tool cards filtered by Fabric item type,
and a shell which keeps every opened tool mounted so switching between tools
(and back home) is instant.
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
# ``does`` / ``when`` feed the "Getting started" guide.
_TOOLS: tuple = (
    {
        "key": "dax_perf_optimizer",
        "name": "DAX Perf Optimizer",
        "description": "Profile and optimize slow DAX queries.",
        "tags": ("Semantic Model",),
        "icon": "dax_performance",
        "module": "sempy_labs.semantic_model._dax_perf",
        "function": "dax_perf_optimizer",
        "does": (
            "Run a DAX query and capture server timings - formula engine vs "
            "storage engine.",
            "Scan report(s) and capture the DAX queries by cycling through each page.",
            "Assemble a query from columns, measures and filters with the "
            "Query Builder, without writing DAX.",
            "Use natural language to generate a DAX query.",
            "View top slowest queries via querying Workspace Monitoring.",
            "Identify and address performance bottlenecks in your DAX queries.",
            "Shows semantic model object lineage in a tree view or node graph.",
        ),
        "when": (
            "Find out why a visual or a query is slow.",
            "Check whether a model change removed a bottleneck.",
            "Identify and improve the slowest queries in your reports.",
            "Identify and improve the slowest queries to hit your semantic model (via Workspace Monitoring).",
        ),
    },
    {
        "key": "bpa",
        "name": "Semantic Model BPA",
        "description": "Run Best Practice Analyzer rules against a semantic model.",
        "tags": ("Semantic Model",),
        "icon": "shield_check",
        "module": "sempy_labs.semantic_model._bpa",
        "function": "bpa",
        "does": (
            "Run the Best Practice Analyzer rules against a semantic model.",
            "Group the violations by category, severity and object so the "
            "important ones surface first.",
            "Edit, disable or add rules, and re-run with your own rule set.",
        ),
        "when": (
            "Review a model before handing it over to report authors.",
            "Audit a model against your organization's modeling standards.",
            "Catch DAX, formatting and performance anti-patterns early.",
        ),
    },
    {
        "key": "vertipaq_analyzer",
        "name": "Vertipaq Analyzer",
        "description": "Analyze the memory footprint of a semantic model.",
        "tags": ("Semantic Model", "Direct Lake"),
        "icon": "vertipaq",
        "module": "sempy_labs.semantic_model._vertipaq_analyzer",
        "function": "vertipaq_analyzer",
        "does": (
            "Break the model size down by table, column, hierarchy and "
            "relationship.",
            "Show cardinality, encoding, compression and dictionary size for "
            "every column.",
            "Export the results, including as a .vpax file.",
        ),
        "when": (
            "Find the columns which are driving the model's size.",
            "Decide what to trim before a refresh or a Direct Lake migration.",
            "Compare the memory footprint before and after a model change.",
        ),
    },
    {
        "key": "refresh_manager",
        "name": "Refresh Manager",
        "description": "Refresh a semantic model, its tables or individual partitions.",
        "tags": ("Semantic Model",),
        "icon": "sync",
        "module": "sempy_labs.semantic_model._refresh_manager",
        "function": "refresh_manager",
        "does": (
            "Refresh a whole model, selected tables, or individual " "partitions.",
            "Pick the refresh type (full, automatic, calculate, data only and "
            "more).",
            "Follow the refresh live and review per-object results and " "errors.",
        ),
        "when": (
            "Reprocess one large table without refreshing the whole model.",
            "Debug a refresh which failed on a single partition.",
            "Recalculate the model after editing measures or relationships.",
        ),
    },
    {
        "key": "lineage_view",
        "name": "Lineage View",
        "description": "Explore which reports depend on a semantic model and determine if they have broken components.",
        "tags": ("Semantic Model",),
        "icon": "workflow",
        "module": "sempy_labs.semantic_model._lineage_view",
        "function": "lineage_view",
        "does": (
            "Shows a diagram of the reports which depend on the semantic model.",
            "Analyzes reports and Excel files for broken semantic model references.",
            "Fix broken references in reports and Excel files.",
            "Rebind report(s) to a new semantic model.",
        ),
        "when": (
            "Determine which reports or Excel files have broken semantic model references.",
            "Connect report(s) to a different semantic model.",
        ),
    },
    {
        "key": "find_unused_objects",
        "name": "Find Unused Objects",
        "description": "Find tables, columns and measures which are never used.",
        "tags": ("Semantic Model", "Report"),
        "icon": "scan_search",
        "module": "sempy_labs.semantic_model._find_unused_objects",
        "function": "find_unused_objects",
        "does": (
            "Scan the reports built on a model or Workspace Monitoring and match them against the "
            "model's objects.",
            "List the tables, columns, measures and hierarchies which no "
            "report uses.",
            "Account for indirect usage through measures, relationships, "
            "hierarchies and row-level security.",
        ),
        "when": (
            "Shrink a model by removing objects nobody consumes.",
            "Cut refresh time and memory on a bloated model.",
            "Clean up before a Direct Lake migration.",
        ),
    },
    {
        "key": "delta_analyzer",
        "name": "Delta Analyzer",
        "description": "Analyze the parquet files and row groups of a delta table.",
        "tags": ("Direct Lake", "Lakehouse"),
        "icon": "delta_stats",
        "module": "sempy_labs._delta_analyzer",
        "function": "delta_analyzer",
        "requires_spark": True,
        "does": (
            "Inspect the parquet files, row groups and column statistics "
            "behind a delta table.",
            "Summarize file counts, row group sizes, V-Order status and "
            "table history.",
            "Show which columns cost the most space on disk.",
        ),
        "when": (
            "Diagnose slow Direct Lake queries caused by too many small " "files.",
            "Decide whether a table needs OPTIMIZE or VACUUM.",
            "Check the effect of a write pattern on file layout.",
        ),
    },
    {
        "key": "migrate_to_direct_lake",
        "name": "Direct Lake Migration",
        "description": "Migrate an import or DirectQuery model to Direct Lake.",
        "tags": ("Semantic Model", "Direct Lake"),
        "icon": "database_zap",
        "module": "sempy_labs.semantic_model._direct_lake_migration",
        "function": "migrate_to_direct_lake",
        "does": (
            "Converts an import or DirectQuery model into a Direct Lake model.",
            "Identifies and summarizes unsupported objects which will not be migrated.",
        ),
        "when": (
            "Convert an existing import or DirectQuery model into a Direct Lake model.",
            "Prototype a Direct Lake version of a model without rebuilding "
            "it by hand.",
        ),
    },
    {
        "key": "mini_model_manager",
        "name": "Mini Model Manager",
        "description": "Create a smaller semantic model from a subset of a master model.",
        "tags": ("Semantic Model", "Direct Lake"),
        "icon": "mini_model",
        "module": "sempy_labs.semantic_model._mini_model_manager",
        "function": "mini_model_manager",
        "does": (
            "Clones a semantic model into a smaller semantic model.",
            "Keeps the cloned model consistent by pulling in the relationships and "
            "dependencies it needs.",
            "Cloned model is reduced by removing unneeded tables, columns, and measures.",
            "Cloned models in Direct Lake can also be shrunk using filters.",
        ),
        "when": (
            "Give a team a focused slice of a large enterprise model.",
            "Produce a lightweight copy of a model for testing or a demo.",
        ),
    },
    {
        "key": "perspective_editor",
        "name": "Perspective Editor",
        "description": "Create and manage perspectives in a semantic model.",
        "tags": ("Semantic Model",),
        "icon": "perspective",
        "module": "sempy_labs.semantic_model._perspective_editor",
        "function": "perspective_editor",
        "does": (
            "Create, rename and delete perspectives.",
            "Choose the tables, columns, measures and hierarchies each "
            "perspective exposes.",
            "Write the changes back to the semantic model.",
        ),
        "when": (
            "Tailor the field list a given audience sees.",
            "Fix perspectives which drifted as the model changed.",
        ),
    },
    {
        "key": "model_comparison",
        "name": "Model Comparison",
        "description": "Compare the metadata of two semantic models side by side.",
        "tags": ("Semantic Model",),
        "icon": "git_compare",
        "module": "sempy_labs.semantic_model._model_comparison",
        "function": "model_comparison",
        "does": (
            "Diff two semantic models object by object.",
            "Show what was added, removed or changed, down to the property " "level.",
            "Compare DAX expressions and partition sources side by side.",
        ),
        "when": (
            "Review what actually changed between development and " "production.",
            "Confirm a deployment pipeline moved what you expected.",
            "Track down a difference which only appears in one environment.",
        ),
    },
    {
        "key": "lakehouse_manager",
        "name": "Lakehouse Manager",
        "description": "Browse a lakehouse and recover soft-deleted tables, folders or files.",
        "tags": ("Lakehouse", "Direct Lake"),
        "icon": "warehouse",
        "module": "sempy_labs.lakehouse._lakehouse_manager",
        "function": "lakehouse_manager",
        "does": (
            "Browse a lakehouse's tables, folders and files with their size, "
            "file count and last modified time.",
            "Flag OneLake shortcuts and soft-deleted objects.",
            "Recover an object which was deleted within the last 7 days.",
            "Run table maintenance - optimize, V-Order and vacuum - on a " "table.",
        ),
        "when": (
            "Restore a table, folder or file someone deleted by accident.",
            "Audit what a lakehouse actually contains, and how big it is.",
            "Compact and V-Order a table feeding a Direct Lake model.",
        ),
    },
)

_TOOLS_BY_KEY = {tool["key"]: tool for tool in _TOOLS}

# Fabric item types, in the order shown in the splash-screen filter.
_CATEGORY_ORDER = ("Semantic Model", "Direct Lake", "Report", "Lakehouse", "Admin")

_SPARK_REQUIRED_NOTE = "Requires a PySpark notebook."


def _unavailable_reason(tool: dict) -> Optional[str]:
    """Why the tool cannot run in this notebook, or None when it can."""

    if not tool.get("requires_spark"):
        return None

    from sempy_labs._helper_functions import _pure_python_notebook

    return _SPARK_REQUIRED_NOTE if _pure_python_notebook() else None


def _tool_payload() -> List[dict]:
    """The tool catalog as sent to the frontend (icons resolved to SVG)."""

    return [
        {
            "key": tool["key"],
            "name": tool["name"],
            "description": tool["description"],
            "tags": list(tool["tags"]),
            "icon": _UI_ICONS[tool["icon"]],
            "does": list(tool["does"]),
            "when": list(tool["when"]),
            "unavailable": _unavailable_reason(tool) or "",
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


def _prefetch_tool_modules() -> None:
    """Import the tool modules off the click path.

    Importing a tool is most of the delay on its first open. Failures are
    ignored here; the import is repeated (and reported) when the tool is opened.
    """

    import importlib
    import threading

    def _load():
        for tool in _TOOLS:
            try:
                importlib.import_module(tool["module"])
            except Exception:
                pass

    threading.Thread(target=_load, daemon=True).start()


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


# The Fluent palette, mapped onto the --ui-* tokens the shared UI components
# read.
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
--ui-border-hover: rgba(72, 131, 247, 0.4);
--ui-text: #242424;
--ui-text-secondary: #616161;
--ui-text-tertiary: #616161;
--ui-accent: #4883f7;
--ui-accent-hover: #3a72e2;
--ui-accent-soft: rgba(72, 131, 247, 0.1);
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
--ui-bg: #0e1116;
--ui-bg-solid: #0e1116;
--ui-bg-secondary: #1f1f1f;
--ui-bg-tertiary: #1f1f1f;
--ui-bg-hover: #3d3d3d;
--ui-surface: #171b22;
--ui-surface-2: #141414;
--ui-border: #3f444c;
--ui-border-strong: #3f444c;
--ui-border-hover: rgba(72, 131, 247, 0.5);
--ui-text: #ffffff;
--ui-text-secondary: #adadad;
--ui-text-tertiary: #adadad;
--ui-accent: #4883f7;
--ui-accent-hover: #639af9;
--ui-accent-soft: rgba(72, 131, 247, 0.18);
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
    position: relative;
    width: 36px;
    height: 36px;
    flex: 0 0 auto;
    border-radius: 8px;
    /* Tint over the card surface, so the mark resolves to the same color as the
       tool icons even though the topbar behind it is darker. */
    background: linear-gradient(var(--ui-accent-soft), var(--ui-accent-soft)) var(--ui-surface);
    color: var(--ui-accent);
}
.slls-app-brand svg { display: block; width: 20px; height: 20px; }
/* Bubbles rise inside the flask while the mark is hovered. The overlay shares
   the mark's 24-unit viewBox, so the circles are placed in flask coordinates. */
.slls-app-brand-bubbles {
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    pointer-events: none;
    overflow: visible;
}
.slls-app-brand-bubble {
    fill: currentColor;
    opacity: 0;
    transform-box: fill-box;
    transform-origin: center;
}
.slls-app-brand:hover .slls-app-brand-bubble {
    animation: slls-app-bubble 1.6s ease-in-out infinite;
}
.slls-app-brand:hover .slls-app-brand-bubble:nth-child(2) {
    animation-duration: 1.9s;
    animation-delay: 0.35s;
}
.slls-app-brand:hover .slls-app-brand-bubble:nth-child(3) {
    animation-duration: 1.4s;
    animation-delay: 0.7s;
}
.slls-app-brand:hover .slls-app-brand-bubble:nth-child(4) {
    animation-duration: 2.1s;
    animation-delay: 1s;
}
@keyframes slls-app-bubble {
    0% { opacity: 0; transform: translateY(0) scale(0.35); }
    25% { opacity: 0.95; }
    70% { opacity: 0.7; }
    100% { opacity: 0; transform: translateY(-6.5px) scale(1.25); }
}
@media (prefers-reduced-motion: reduce) {
    .slls-app-brand:hover .slls-app-brand-bubble { animation: none; }
}
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
/* A modal over the launcher: an intro plus one section per tool. */
.slls-app-modal {
    display: none;
    position: fixed;
    inset: 0;
    z-index: 2147483000;
    align-items: center;
    justify-content: center;
    padding: 24px;
    background: rgba(0, 0, 0, 0.5);
}
.slls-app-modal.show { display: flex; }
.slls-app-dialog {
    display: flex;
    flex-direction: column;
    width: 100%;
    max-width: 900px;
    max-height: 85vh;
    border: 1px solid var(--ui-border);
    border-radius: 12px;
    background: var(--ui-bg);
    box-shadow: var(--ui-shadow-lg);
    overflow: hidden;
}
.slls-app-dialog-head {
    display: flex;
    align-items: center;
    gap: 16px;
    flex: 0 0 auto;
    padding: 20px 24px;
    border-bottom: 1px solid var(--ui-border);
}
.slls-app-dialog-icon {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 44px;
    height: 44px;
    flex: 0 0 auto;
    border-radius: 8px;
    background: var(--ui-accent-soft);
    color: var(--ui-accent);
}
.slls-app-dialog-icon svg { display: block; width: 22px; height: 22px; }
.slls-app-dialog-heading { flex: 1 1 auto; min-width: 0; }
.slls-app-dialog-title {
    font-size: 20px;
    font-weight: 600;
    color: var(--ui-text);
}
.slls-app-dialog-sub {
    margin-top: 2px;
    font-size: 13px;
    color: var(--ui-text-secondary);
}
.slls-app-dialog-close {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 36px;
    height: 36px;
    flex: 0 0 auto;
    padding: 0;
    border: 1px solid var(--ui-border);
    border-radius: 8px;
    background: var(--ui-surface);
    color: var(--ui-text-secondary);
    font: inherit;
    cursor: pointer;
    transition: background 120ms ease, color 120ms ease;
}
.slls-app-dialog-close svg { display: block; width: 18px; height: 18px; }
.slls-app-dialog-close:hover { background: var(--ui-bg-hover); color: var(--ui-text); }
.slls-app-dialog-body {
    flex: 1 1 auto;
    min-height: 0;
    overflow-y: auto;
    padding: 20px 24px 24px;
}
.slls-app-guide { display: flex; flex-direction: column; gap: 16px; }
.slls-app-guide-card {
    padding: 16px;
    border: 1px solid var(--ui-border);
    border-radius: 12px;
    background: var(--ui-surface);
}
.slls-app-guide-head { display: flex; align-items: flex-start; gap: 12px; }
.slls-app-guide-icon {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 40px;
    height: 40px;
    flex: 0 0 auto;
    border-radius: 8px;
    background: var(--ui-accent-soft);
    color: var(--ui-accent);
}
.slls-app-guide-icon svg { display: block; width: 20px; height: 20px; }
.slls-app-guide-title {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px;
}
.slls-app-guide-name { font-size: 16px; font-weight: 600; color: var(--ui-text); }
.slls-app-guide-desc {
    margin-top: 2px;
    font-size: 12px;
    line-height: 20px;
    color: var(--ui-text-secondary);
}
.slls-app-guide-cols {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    gap: 16px;
    margin-top: 12px;
}
.slls-app-guide-label {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--ui-text-tertiary);
}
.slls-app-guide-list {
    margin: 6px 0 0;
    padding-left: 18px;
    font-size: 12px;
    line-height: 20px;
    color: var(--ui-text);
}
.slls-app-guide-list li { margin-bottom: 4px; }
.slls-app-guide-note {
    display: flex;
    align-items: center;
    gap: 6px;
    margin-top: 10px;
    font-size: 11px;
    font-weight: 500;
    color: var(--ui-text-tertiary);
}
.slls-app-guide-note svg { display: block; width: 14px; height: 14px; flex: 0 0 auto; }
.slls-app-links {
    margin-bottom: 20px;
    padding: 16px;
    border: 1px solid var(--ui-border);
    border-radius: 12px;
    background: var(--ui-surface);
    font-size: 12px;
    line-height: 20px;
    color: var(--ui-text-secondary);
}
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
.slls-app-card-note {
    display: flex;
    align-items: center;
    gap: 4px;
    font-size: 11px;
    font-weight: 500;
    color: var(--ui-text-tertiary);
}
.slls-app-card-note svg { display: block; width: 14px; height: 14px; flex: 0 0 auto; }
/* Requirement not met in this notebook: shown, but inert. */
.slls-app-card.is-disabled {
    cursor: not-allowed;
    box-shadow: none;
    background: var(--ui-surface-2);
}
.slls-app-card.is-disabled:hover {
    transform: none;
    border-color: var(--ui-border);
    box-shadow: none;
}
.slls-app-card.is-disabled .slls-app-card-icon,
.slls-app-card.is-disabled .slls-app-card-text,
.slls-app-card.is-disabled .slls-app-card-tags { opacity: 0.5; }
.slls-app-card.is-disabled:hover .slls-app-card-icon {
    background: var(--ui-accent-soft);
    color: var(--ui-accent);
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

/* A tool is open: the launcher gets out of the way entirely — Back is moved
   into the tool's own header, next to its title. */
.slls-app.slls-app-tool-open .slls-app-main,
.slls-app.slls-app-tool-open .slls-app-modal,
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
    const FS_ENTER_SVG = `__SLLS_ICON_FULLSCREEN__`;
    const FS_EXIT_SVG = `__SLLS_ICON_FULLSCREEN_EXIT__`;
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
    brand.innerHTML = `__SLLS_ICON_BRAND____SLLS_BRAND_BUBBLES__`;
    left.appendChild(brand);

    const brandName = document.createElement("span");
    brandName.className = "slls-app-brand-name";
    brandName.textContent = "Semantic Link Labs";
    left.appendChild(brandName);

    const backBtn = document.createElement("button");
    backBtn.type = "button";
    backBtn.className = "slls-app-back";
    backBtn.innerHTML = `__SLLS_ICON_ARROW_LEFT__`;
    backBtn.title = "Back to all tools";
    backBtn.setAttribute("aria-label", backBtn.title);
    backBtn.addEventListener("click", () => {
        pendingKey = "";
        renderView();
        send({ action: "home" });
    });
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
    // ipywidgets can run render() before this view is inserted into the
    // .slls-app-shell box, so the shell is resolved on each toggle (passing
    // shell() now could make the launcher alone go full screen, without the
    // open tool), and the shell listeners are bound once the view is in it.
    sllsSetupFullscreen(shell, fsBtn, "slls-app-fs", FS_ENTER_SVG, FS_EXIT_SVG);
    let disposed = false;
    function bindShell() {
        if (disposed) return;
        if (!root.closest(".slls-app-shell") && !root.isConnected) {
            requestAnimationFrame(bindShell);
            return;
        }
        applyTheme();
        // Runs as a microtask, so the tool and its Back button land in one paint.
        new MutationObserver(() => { applyView(); placeBack(); }).observe(shell(),
            { childList: true, subtree: true });
        shell().addEventListener("click", interceptToolChrome, true);
        // Catch up on tools the shell gained before it was observed.
        setTimeout(() => { applyView(); placeBack(); });
    }
    bindShell();
    document.addEventListener("fullscreenchange", syncHostedTool);
    fsBtn.addEventListener("click", () => setTimeout(syncHostedTool));

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
        "A growing set of tools for working with Microsoft Fabric. Pick a tool to get started.";
    hero.appendChild(heroSub);

    const links = document.createElement("div");
    links.className = "slls-app-links";
    links.innerHTML = `__SLLS_LINKS__`;

    // ---------------- Getting started ----------------
    const modal = document.createElement("div");
    modal.className = "slls-app-modal";
    modal.setAttribute("role", "dialog");
    modal.setAttribute("aria-modal", "true");
    modal.setAttribute("aria-label", "Getting started");
    modal.innerHTML = `
        <div class="slls-app-dialog">
            <div class="slls-app-dialog-head">
                <span class="slls-app-dialog-icon">__SLLS_ICON_BOOK__</span>
                <div class="slls-app-dialog-heading">
                    <div class="slls-app-dialog-title">Getting started</div>
                    <div class="slls-app-dialog-sub">What each tool does and when to use it</div>
                </div>
                <button type="button" class="slls-app-dialog-close" title="Close"
                    aria-label="Close">__SLLS_ICON_CLOSE__</button>
            </div>
            <div class="slls-app-dialog-body"><div class="slls-app-guide"></div></div>
        </div>`;
    root.appendChild(modal);

    const dialogBody = modal.querySelector(".slls-app-dialog-body");
    dialogBody.insertBefore(links, dialogBody.firstChild);
    const guide = modal.querySelector(".slls-app-guide");
    const closeGuideBtn = modal.querySelector(".slls-app-dialog-close");

    function renderGuide() {
        guide.innerHTML = "";
        for (const tool of model.get("tools") || []) {
            const card = document.createElement("div");
            card.className = "slls-app-guide-card";

            const head = document.createElement("div");
            head.className = "slls-app-guide-head";
            const icon = document.createElement("span");
            icon.className = "slls-app-guide-icon";
            icon.innerHTML = tool.icon || "";
            head.appendChild(icon);

            const text = document.createElement("div");
            const title = document.createElement("div");
            title.className = "slls-app-guide-title";
            const name = document.createElement("span");
            name.className = "slls-app-guide-name";
            name.textContent = tool.name;
            title.appendChild(name);
            for (const tag of tool.tags || []) {
                const pill = document.createElement("span");
                pill.className = "slls-app-tag";
                pill.textContent = tag;
                title.appendChild(pill);
            }
            text.appendChild(title);
            const desc = document.createElement("div");
            desc.className = "slls-app-guide-desc";
            desc.textContent = tool.description || "";
            text.appendChild(desc);
            head.appendChild(text);
            card.appendChild(head);

            const cols = document.createElement("div");
            cols.className = "slls-app-guide-cols";
            const sections = [
                ["What it does", tool.does],
                ["When to use it", tool.when],
            ];
            for (const [label, items] of sections) {
                if (!(items || []).length) continue;
                const col = document.createElement("div");
                const heading = document.createElement("div");
                heading.className = "slls-app-guide-label";
                heading.textContent = label;
                col.appendChild(heading);
                const list = document.createElement("ul");
                list.className = "slls-app-guide-list";
                for (const item of items) {
                    const li = document.createElement("li");
                    li.textContent = item;
                    list.appendChild(li);
                }
                col.appendChild(list);
                cols.appendChild(col);
            }
            card.appendChild(cols);
            if (tool.unavailable) {
                const note = document.createElement("div");
                note.className = "slls-app-guide-note";
                note.innerHTML = `__SLLS_ICON_INFO__`;
                const noteText = document.createElement("span");
                noteText.textContent = tool.unavailable;
                note.appendChild(noteText);
                card.appendChild(note);
            }
            guide.appendChild(card);
        }
    }

    // While the guide is open the rest of the launcher is inert, so keyboard
    // and assistive-technology users cannot reach controls behind the modal.
    let guideReturnFocus = null;
    const inertWhileGuideOpen = [];
    function setBackgroundInert(on) {
        if (on) {
            for (const child of root.children) {
                if (child !== modal && !child.inert) {
                    child.inert = true;
                    inertWhileGuideOpen.push(child);
                }
            }
        } else {
            while (inertWhileGuideOpen.length) inertWhileGuideOpen.pop().inert = false;
        }
    }
    function guideFocusables() {
        const selector = 'a[href], button:not([disabled]), input:not([disabled]), '
            + 'select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
        return [...modal.querySelectorAll(selector)]
            .filter((el) => el.getClientRects().length > 0);
    }
    function setGuideOpen(open) {
        if (open === modal.classList.contains("show")) return;
        modal.classList.toggle("show", open);
        setBackgroundInert(open);
        if (open) {
            guideReturnFocus = document.activeElement;
            dialogBody.scrollTop = 0;
            closeGuideBtn.focus();
        } else {
            const target = guideReturnFocus && guideReturnFocus.isConnected
                ? guideReturnFocus : linksBtn;
            guideReturnFocus = null;
            target.focus();
        }
    }
    linksBtn.addEventListener("click", () =>
        setGuideOpen(!modal.classList.contains("show")));
    closeGuideBtn.addEventListener("click", () => setGuideOpen(false));
    modal.addEventListener("click", (event) => {
        if (event.target === modal) setGuideOpen(false);
    });
    // Keep Tab / Shift+Tab cycling within the dialog.
    modal.addEventListener("keydown", (event) => {
        if (event.key !== "Tab" || !modal.classList.contains("show")) return;
        const focusables = guideFocusables();
        if (!focusables.length) { event.preventDefault(); return; }
        const first = focusables[0];
        const last = focusables[focusables.length - 1];
        const active = document.activeElement;
        if (event.shiftKey && (active === first || !modal.contains(active))) {
            event.preventDefault(); last.focus();
        } else if (!event.shiftKey && (active === last || !modal.contains(active))) {
            event.preventDefault(); first.focus();
        }
    });
    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && modal.classList.contains("show")) {
            setGuideOpen(false);
        }
    });

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

    // Navigation is applied in the browser first so it feels immediate; the
    // kernel round trip (and, on first open, importing and starting the tool)
    // catches up afterwards.
    let pendingKey = null;
    function currentKey() {
        return pendingKey === null ? (model.get("active_tool") || "") : pendingKey;
    }

    function activeTool() {
        const key = currentKey();
        return (model.get("tools") || []).find((tool) => tool.key === key) || null;
    }

    function toolNode(key) {
        return key ? shell().querySelector(".slls-app-tool-" + key) : null;
    }

    function applyToolVisibility(key) {
        for (const node of shell().querySelectorAll(".slls-app-tool")) {
            node.style.display =
                key && node.classList.contains("slls-app-tool-" + key) ? "" : "none";
        }
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
    // Exact labels only: tools also ship panel-level expand controls whose
    // labels merely contain "full screen" (e.g. "Expand DAX editor to full
    // screen"), which must keep their own behavior and icon.
    const FS_TOGGLE_LABELS = ["full screen", "exit full screen", "toggle full screen"];
    const isFullscreenCtl = (label) => FS_TOGGLE_LABELS.indexOf(label.trim()) >= 0;

    // The tool's header toggle is the first such control in document order;
    // panel toggles deeper in the body can share its labels.
    function toolFullscreenBtn(node) {
        for (const btn of node.querySelectorAll("button")) {
            if (btn === backBtn) continue;
            if (isFullscreenCtl(ctlLabel(btn))) return btn;
        }
        return null;
    }

    let syntheticClick = false;

    function shellFullscreen() {
        return document.fullscreenElement === shell()
            || shell().classList.contains("slls-app-fs");
    }

    // Tools scope their full-height layout to their own :fullscreen rules, which
    // cannot match while the shell owns full screen. This marks the hosted tool
    // so those tools can key the same layout (without their own overlay) on it.
    function syncToolFullscreenClass() {
        const on = shellFullscreen();
        for (const node of shell().querySelectorAll(".slls-app-tool")) {
            for (const child of node.children) {
                child.classList.toggle("slls-app-fs-tool", on);
            }
        }
    }

    function syncHostedTool() {
        syncToolFullscreenClass();
        syncToolChrome();
    }

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

    // Only the theme is mirrored onto a tool by clicking. A tool's own full
    // screen is a fixed, full-viewport overlay; stacking one inside the already
    // full-screen shell hides the tool, so its toggle is only re-labelled to
    // show the shell's state.
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

        const fullscreen = toolFullscreenBtn(node);
        if (!fullscreen) return;
        const on = shellFullscreen();
        if (ctlLabel(fullscreen).includes("exit") === on) return;
        const text = on ? "Exit full screen" : "Full screen";
        fullscreen.innerHTML = on ? FS_EXIT_SVG : FS_ENTER_SVG;
        fullscreen.title = text;
        fullscreen.setAttribute("aria-label", text);
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
        } else if (btn === toolFullscreenBtn(node)) {
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
            applyView();
            applyBack();
            syncHostedTool();
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

    // A tool is revealed only once its own DOM has arrived, so it appears in
    // one piece rather than as an empty frame holding just the Back button.
    let lastOpenKey = null;
    function applyView() {
        if (pendingKey !== null && pendingKey === (model.get("active_tool") || "")) {
            pendingKey = null;
        }
        const key = currentKey();
        const open = !!activeTool() && !!toolNode(key);
        root.classList.toggle("slls-app-tool-open", open);
        applyToolVisibility(open ? key : "");
        const openKey = open ? key : "";
        if (openKey !== lastOpenKey) {
            lastOpenKey = openKey;
            // Same tick as the reveal, so the tool and its Back button paint together.
            applyBack();
        }
    }

    function renderView() {
        setGuideOpen(false);
        applyView();
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
            card.className = "slls-app-card"
                + (tool.unavailable ? " is-disabled" : "");
            card.disabled = !!tool.unavailable;
            card.setAttribute("aria-label", tool.unavailable
                ? `${tool.name} - ${tool.unavailable}`
                : `Open ${tool.name}`);
            if (tool.unavailable) card.title = tool.unavailable;

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

            if (tool.unavailable) {
                const note = document.createElement("span");
                note.className = "slls-app-card-note";
                note.innerHTML = `__SLLS_ICON_INFO__`;
                const noteText = document.createElement("span");
                noteText.textContent = tool.unavailable;
                note.appendChild(noteText);
                card.appendChild(note);
            }

            card.addEventListener("click", () => {
                if (tool.unavailable) return;
                pendingKey = tool.key;
                renderView();
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
        // A failed launch never changes active_tool, so drop the optimistic view.
        if ((model.get("status") || {}).kind === "error") pendingKey = null;
        renderView();
        renderBanner();
    });
    model.on("change:active_tool", () => {
        pendingKey = null;
        renderView();
        renderBanner();
        shell().scrollTop = 0;
    });
    model.on("change:tools", renderGrid);
    model.on("change:tools", renderGuide);
    model.on("change:categories", () => { renderFilters(); renderGrid(); });

    renderView();
    renderFilters();
    renderGrid();
    renderGuide();
    renderBanner();
    return () => { disposed = true; };
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

# Overlay on the brand mark: circles placed in the flask's own coordinates
# (24-unit viewBox), which bubble out of the mouth while the mark is hovered.
# The overlay does not clip, so above the rim a bubble is free of the neck's
# 3.2-unit width and can grow enough to read at a 20px mark.
_BRAND_BUBBLES_HTML = (
    '<svg class="slls-app-brand-bubbles" viewBox="0 0 24 24" width="20" '
    'height="20" aria-hidden="true">'
    '<circle class="slls-app-brand-bubble" cx="12" cy="3.4" r="1.3"/>'
    '<circle class="slls-app-brand-bubble" cx="10.6" cy="4.2" r="0.95"/>'
    '<circle class="slls-app-brand-bubble" cx="13.4" cy="4" r="0.8"/>'
    '<circle class="slls-app-brand-bubble" cx="11.3" cy="2.5" r="0.65"/>'
    "</svg>"
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

# A hosted tool renders as a centered card. While the shell is full screen it
# stretches to fill it instead, without the fixed overlay its own full-screen
# mode would use (stacking that inside the shell hides the tool).
_TOOL_FILL_CSS = (
    "max-width: none; width: 100%; margin: 0; border: none; "
    "border-radius: 0; box-shadow: none; min-height: 100vh;"
)
for _fs_shell in (".slls-app-shell.slls-app-fs", ".slls-app-shell:fullscreen"):
    _WIDGET_CSS += (
        f"\n{_fs_shell} .slls-app-tool {{ padding: 0; }}"
        f"\n{_fs_shell} .slls-app-tool > * {{ {_TOOL_FILL_CSS} }}"
    )

_WIDGET_JS = _ui_fullscreen_setup_js() + _WIDGET_JS
_WIDGET_JS = (
    _WIDGET_JS.replace("__SLLS_ICON_SUN__", _UI_ICONS["sun"])
    .replace("__SLLS_ICON_MOON__", _UI_ICONS["moon"])
    .replace("__SLLS_ICON_FULLSCREEN__", _UI_ICONS["fullscreen"])
    .replace("__SLLS_ICON_FULLSCREEN_EXIT__", _UI_ICONS["fullscreen_exit"])
    .replace("__SLLS_ICON_BRAND__", _UI_ICONS["semantic_link_labs"])
    .replace("__SLLS_BRAND_BUBBLES__", _BRAND_BUBBLES_HTML)
    .replace("__SLLS_ICON_BOOK__", _UI_ICONS["book"])
    .replace("__SLLS_ICON_INFO__", _UI_ICONS["info"])
    .replace("__SLLS_ICON_CLOSE__", _UI_ICONS["close"])
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

        reason = _unavailable_reason(tool)
        if reason is not None:
            widget.status = {
                "message": f"{tool['name']} is not available here: {reason}",
                "kind": "error",
            }
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
            new_widget.add_class(f"slls-app-tool-{tool['key']}")
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
    _prefetch_tool_modules()
