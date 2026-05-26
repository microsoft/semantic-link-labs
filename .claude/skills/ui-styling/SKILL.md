---
name: ui-styling
description: Guide for the visual style, structure, and shared building blocks used by Semantic Link Labs interactive UI tools (HTML widgets and anywidget-based widgets). Use this when adding a new interactive UI, modifying an existing one, or adding shared visual components.
---

# UI Styling for Interactive Tools

This skill describes the styling conventions, shared building blocks, and architectural patterns used by all interactive UI tools in Semantic Link Labs so that every tool has a consistent, elegant, Apple-inspired design.

## When to Use This Skill

Use this skill when you need to:

- Add a **new interactive UI** function to the library.
- Modify an existing interactive UI (e.g. `vertipaq_analyzer`, `delta_analyzer`, `perspective_editor`).
- Add new **shared visual components** (icons, theme variables, headers, etc.) that should be reused across tools.
- Decide between a **static-HTML** widget and an **anywidget**-based widget.

---

## The Two UI Patterns

Semantic Link Labs has exactly two supported patterns for interactive UI tools. Always pick one based on whether the UI needs to call back into Python after the initial render.

| Pattern | When to use | Reference implementations |
|---------|-------------|---------------------------|
| **Static-HTML widget** (the *Vertipaq style*) | The UI is fully driven by the data computed in Python *before* render. All interactivity (filtering, sorting, tab switching, theme toggle, column resizing, etc.) is done in **pure browser-side JavaScript**. No Python code runs after `display(HTML(...))`. | `sempy_labs.semantic_model._vertipaq_analyzer.vertipaq_analyzer`, `sempy_labs._delta_analyzer.delta_analyzer` |
| **anywidget widget** (the *Perspective Editor style*) | The UI must run Python code in response to user actions (e.g. write back to a semantic model, refresh data from a REST API, perform long-running operations). State is synced between the JS frontend and the Python backend via `traitlets`. | `sempy_labs.semantic_model._perspective_editor.perspective_editor` |

> **Rule of thumb:** if the only thing the user does is view, sort, filter, or switch between pre-computed data, use the **static-HTML** pattern. If they can *change* something that must persist (model edits, refresh triggers, server calls), use **anywidget**.

---

## Shared Building Blocks: `sempy_labs._ui_components`

`src/sempy_labs/_ui_components.py` is the single source of truth for everything visual that should be consistent across tools. **Both patterns must source their visual primitives from this module.** If you need a new shared visual component (a new icon, a new helper, a new themed control), add it here so every tool can pick it up.

### What lives there today

| Export | Purpose |
|--------|---------|
| `ICONS` | Dict of monochrome SVG icons. All use `stroke="currentColor"` / `fill="currentColor"` so they adapt to light and dark themes automatically. Keys include tabular-object icons (`table`, `column`, `column_chunk`, `measure`, `hierarchy`, `partition`, `relationship`) and UI icons (`sun`, `moon`, `search`, `plus`, `caret_right`). |
| `LIGHT_THEME_VARS`, `DARK_THEME_VARS` | CSS custom-property blocks defining the Apple-inspired light and dark palettes. Always reference colors via these `--ui-*` tokens, never hard-coded hex values. |
| `HEADER_CSS`, `scoped_header_css(root_selector)` | Standard widget header styles (title + dataset/workspace subtitle + theme toggle button). `scoped_header_css` prefixes every rule with the root selector so the styles win against notebook host CSS (e.g. Jupyter's `.jp-RenderedHTMLCommon button`). |
| `render_header_html(title, dataset_name, workspace_name, theme_btn_id, dark_mode)` | Renders the standard header markup. |
| `theme_toggle_script(btn_id, root_selector, dark_class)` | Returns a `<script>` block that wires the theme toggle button to flip a `dark_class` on the root element and swap the sun/moon icon. |
| `ATTRIBUTION_CSS`, `scoped_attribution_css(root_selector)`, `render_attribution_html(extra_links=None)` | "Powered by Semantic Link Labs" attribution shown at the bottom of every widget, with an optional list of extra `(label, url)` links. |

### When to extend `_ui_components`

Add a new export to `_ui_components.py` whenever the same visual element appears (or *should* appear) in more than one tool. Typical candidates: a new icon, a shared button style, a status-pill style, a confirmation-dialog component, a toast/notification helper. Do **not** copy-paste CSS or SVGs between widgets — promote them to `_ui_components` instead.

---

## Design Tokens (the visual language)

All interactive UIs share one visual language. Stick to these tokens — do not introduce one-off colors, fonts, radii, or shadows.

### Typography

- Font stack: `-apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "Helvetica Neue", Helvetica, Arial, sans-serif`.
- Enable font smoothing: `-webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale;`.
- Title (22px / 600 / `-0.01em` letter-spacing) and subtitle (≈12.5px / `--ui-text-secondary`) are produced by `render_header_html` — do not re-implement them.
- Use `font-variant-numeric: tabular-nums` for any numeric column or large numeric value.

### Color tokens (from `LIGHT_THEME_VARS` / `DARK_THEME_VARS`)

| Token | Use for |
|-------|---------|
| `--ui-bg`, `--ui-bg-secondary`, `--ui-bg-tertiary`, `--ui-bg-solid` | Surfaces, in increasing emphasis levels |
| `--ui-surface`, `--ui-surface-2` | Translucent overlays / hover backgrounds |
| `--ui-border`, `--ui-border-strong` | Subtle and strong borders |
| `--ui-text`, `--ui-text-secondary`, `--ui-text-tertiary` | Primary, secondary, tertiary text |
| `--ui-accent`, `--ui-accent-hover`, `--ui-accent-soft` | Brand accent (links, focus rings, active tab indicator, primary buttons, data bars) |
| `--ui-shadow-sm`, `--ui-shadow-md`, `--ui-shadow-lg` | Elevation |

If a tool needs its own derived names (e.g. `--vpx-text`), alias them to the `--ui-*` tokens inside the tool's scoped block — see `_vertipaq_analyzer.py` for the pattern. This keeps the palette centralized while letting tools use short, local names.

### Shape and motion

- Radii: `12px` (containers), `8px` (controls/cards) — exposed as `--vpx-radius` / `--vpx-radius-sm` style aliases when needed.
- Transitions: `0.25s cubic-bezier(0.4, 0, 0.2, 1)` for color/border/background; `120ms ease` for small button state changes; `80ms` for press scale.
- Subtle hover/active states only — no heavy animations.

---

## Anatomy of a Widget

Every interactive UI, regardless of pattern, should be composed of these regions in this order:

1. **Root container** with a per-instance unique class (e.g. `vpx-<uid>`) and an optional dark-mode modifier class (e.g. `vpx-dark`). The unique id (`uid = uuid.uuid4().hex[:8]`) keeps multiple widgets on the same page from colliding.
2. **Standard header** rendered via `render_header_html(...)` — title + dataset/workspace subtitle + theme toggle button.
3. **Summary cards** (optional) — a horizontal row of `(label, value)` cards summarizing the result set.
4. **Tab bar** (optional) — for multi-section views; active tab uses `--ui-accent` text + a 2px accent underline.
5. **Toolbar** (optional) — search box (using `ICONS["search"]`), row count, view toggles (e.g. data-bars on/off).
6. **Main content** — table, tree, form, etc.
7. **Attribution** rendered via `render_attribution_html(...)` at the bottom, with optional `extra_links` for upstream credit (e.g. SQLBI's Vertipaq Analyzer).

---

## Pattern A: Static-HTML Widget (Vertipaq / Delta Analyzer style)

Use this for **read-only / pre-computed** UIs. End-to-end recipe:

### 1. Compute data in Python, then render

Build the `pandas.DataFrame`(s) or dict of dataframes in Python. Pass them to a private `visualize_*` / `_render_*` helper that assembles HTML and JS strings and calls `IPython.display.display(HTML(...))`.

### 2. Generate a per-instance uid

```python
uid = uuid.uuid4().hex[:8]
root_selector = f".vpx-{uid}"            # used for scoping CSS + JS lookups
theme_btn_id = f"vpx-theme-{uid}"
```

Every CSS class, every DOM id, and every global JS function name **must** include `uid` so multiple instances on one notebook page never clash.

### 3. Import the shared building blocks

Pull from `sempy_labs._ui_components` — do not re-define icons, colors, headers, theme-toggle JS, or the attribution footer.

```python
from sempy_labs._ui_components import (
    ICONS as _UI_ICONS,
    LIGHT_THEME_VARS as _UI_LIGHT_VARS,
    DARK_THEME_VARS as _UI_DARK_VARS,
    scoped_header_css as _ui_scoped_header_css,
    scoped_attribution_css as _ui_scoped_attribution_css,
    render_header_html as _ui_render_header_html,
    render_attribution_html as _ui_render_attribution_html,
    theme_toggle_script as _ui_theme_toggle_script,
)
```

### 4. Scope all CSS under the root selector

Inline the light palette inside `.vpx-<uid> { ... }` and the dark palette inside `.vpx-<uid>.vpx-dark { ... }`, so dark mode is a class toggle on the root. Include `_ui_scoped_header_css(root_selector)` and `_ui_scoped_attribution_css(root_selector)` in your `<style>` block. Scoping is required so that high-specificity notebook host styles (Jupyter, VS Code, Fabric) do not override your widget.

### 5. Build the markup in order: header → cards → tabs → toolbar → content → attribution

Use `render_header_html(...)` for the header and `render_attribution_html(...)` for the footer. Re-use icons from `_UI_ICONS` (apply your own class via `.replace("<svg ", '<svg class="vpx-tab-icon" ', 1)` for sizing).

### 6. Wire all interactivity in inline JavaScript

Filter/sort/resize/tab-switch/bar-toggle logic lives in a `<script>` block whose function names are also uid-suffixed (e.g. `window.vpxSort_<uid>`). The theme toggle button is wired by appending `_ui_theme_toggle_script(btn_id=theme_btn_id, root_selector=root_selector, dark_class="vpx-dark")`.

### 7. Render

```python
display(HTML(styles + "\n".join(html_parts) + script + theme_script))
```

### Reference

- `src/sempy_labs/semantic_model/_vertipaq_analyzer.py` — `visualize_vertipaq` (full implementation: cards, tabs, toolbar, sortable/filterable/resizable table with optional data bars).
- `src/sempy_labs/_delta_analyzer.py` — same pattern applied to delta-table analysis output.

---

## Pattern B: anywidget Widget (Perspective Editor style)

Use this when the UI must call back into Python after render (e.g. to edit a semantic model, trigger a refresh, run an API call). The widget is implemented as a subclass of `anywidget.AnyWidget` with state synced via `traitlets`.

### 1. Guard the optional dependency

`anywidget` is **not** a hard dependency of the library — keep it that way. Import lazily inside the public function and raise a friendly `ImportError` if missing:

```python
try:
    import anywidget
    import traitlets
except ImportError as e:
    raise ImportError(
        "The '<my_tool>' function requires the 'anywidget' package. "
        "Install it with: pip install anywidget"
    ) from e
```

### 2. Subclass `anywidget.AnyWidget`

Define the widget with class attributes `_esm` (the JS module string — typically a top-level `function render({ model, el }) { ... }` block) and `_css` (the CSS string). Declare every piece of state that must cross the Python/JS boundary as a synced traitlet:

```python
class MyWidget(anywidget.AnyWidget):
    _esm = _WIDGET_JS
    _css = _WIDGET_CSS

    data = traitlets.Dict().tag(sync=True)
    selected = traitlets.Unicode("").tag(sync=True)
    status = traitlets.Dict().tag(sync=True)         # { "message": ..., "kind": "success"|"error" }
    pending_action = traitlets.Dict().tag(sync=True) # what JS asked Python to do
    run = traitlets.Int(0).tag(sync=True)            # bump to trigger the Python callback
    dataset_name = traitlets.Unicode("").tag(sync=True)
    workspace_name = traitlets.Unicode("").tag(sync=True)
    dark_mode = traitlets.Bool(False).tag(sync=True)
```

### 3. Use the "bump `run` + observe" callback pattern

JS triggers Python work by setting `pending_action` (a dict describing what to do) and then incrementing `run` and calling `model.save_changes()`. Python observes `run` and dispatches based on `pending_action["action"]`. On completion it writes results back to other traitlets (including a user-visible `status`) which the JS observer renders.

```python
def _on_run(change):
    data = dict(widget.pending_action or {})
    action = data.get("action")
    if not action:
        return
    try:
        ...  # do Python work, then update widget.status / other traitlets
    except Exception as e:
        widget.status = {"message": f"Error: {e}", "kind": "error"}

widget.observe(_on_run, names=["run"])
```

### 4. Display once, keep the reference alive

After `display(widget)`, keep the local `widget` reference inside the closure (Python's GC must not collect it, or observers stop firing). **Do not also `return widget`** — that causes Jupyter to render it a second time.

### 5. Visual conventions on the JS side

The frontend `render({ model, el })` function should:

- Create a root `<div>` with a stable namespace class (e.g. `slls-pe` for the perspective editor). Add `slls-pe-dark` when `dark_mode === true` and `slls-pe-auto` (which uses `@media (prefers-color-scheme: dark)`) when `dark_mode` is null/undefined.
- Build the header with the same title + dataset/workspace subtitle + sun/moon theme-toggle button shape used by the static widgets. The theme button toggles the `dark_mode` traitlet (`model.set("dark_mode", ...); model.save_changes();`) so the preference round-trips to Python.
- Use the **same color/typography/radius tokens** as `_ui_components` (light + dark palettes, Apple font stack, 12px / 8px radii). When you need an icon also used elsewhere, embed the SVG from `ICONS` (e.g. via a Python-side template-substitution placeholder like `__SLLS_ICON_TABLE__`) so there is one source of truth.
- Always render the "Powered by Semantic Link Labs" attribution at the bottom, matching `render_attribution_html`.

> If you need a new visual primitive in an anywidget tool (a new icon, a new button style, a new theme token), add it to `_ui_components` and substitute it into the `_WIDGET_JS`/`_WIDGET_CSS` strings — do not fork the design.

### Reference

- `src/sempy_labs/semantic_model/_perspective_editor.py` — `perspective_editor`. Full implementation showing widget class definition, traitlets, the `pending_action` + `run` callback pattern, dark-mode round-trip, and lazy-import guard.

---

## Public API Conventions for Interactive Tools

All interactive UI functions follow the same Python signature conventions as the rest of the library (see the [Add Function](../add-function/SKILL.md) skill), with these additions:

- Apply the `@log` decorator and write a numpydoc docstring.
- Accept a `dark_mode: bool = False` parameter so users can opt into dark on first render. Document it.
- Accept the standard `workspace: Optional[str | UUID] = None` parameter and resolve it via `resolve_workspace_name_and_id`.
- For functions that operate on a semantic model, accept `dataset: str | UUID` and use `connect_semantic_model` (read-only when the UI is view-only, read-write only when it actually needs to mutate the model).
- The function's job is to *display* the widget, not return it. Do not return the widget object (it causes double-rendering in Jupyter).

---

## Checklist for a New Interactive UI

- [ ] Picked the correct pattern: static-HTML if no Python callbacks are needed, anywidget if they are.
- [ ] All icons come from `sempy_labs._ui_components.ICONS` (no inlined one-off SVGs).
- [ ] All colors come from `LIGHT_THEME_VARS` / `DARK_THEME_VARS` (no hard-coded hex values).
- [ ] Standard header rendered via `render_header_html` (static) or built in JS using the same layout/tokens (anywidget).
- [ ] Standard "Powered by Semantic Link Labs" attribution rendered at the bottom.
- [ ] Theme toggle wired via `theme_toggle_script` (static) or via a synced `dark_mode` traitlet (anywidget).
- [ ] CSS is scoped under a per-instance `uid` (static) or under a stable namespace class (anywidget) so multiple instances on one page do not collide and notebook host styles do not bleed in.
- [ ] Apple-inspired font stack and antialiasing are applied to the root.
- [ ] Public function accepts `dark_mode: bool = False`, resolves workspace, has `@log` + numpydoc docstring, and calls `display(...)` (does not return the widget).
- [ ] Any genuinely reusable new component was promoted to `_ui_components` rather than duplicated.
- [ ] For anywidget tools: `anywidget` is imported lazily with a friendly `ImportError`.
