"""The app launcher must expose every tool it advertises."""

import importlib

from sempy_labs import _app


def test_every_tool_resolves_to_a_callable():
    for tool in _app._TOOLS:
        module = importlib.import_module(tool["module"])
        assert callable(getattr(module, tool["function"])), tool["key"]


def test_tool_payload_carries_an_svg_icon_per_tool():
    payload = _app._tool_payload()

    assert len(payload) == len(_app._TOOLS)
    for tool in payload:
        assert tool["icon"].startswith("<svg")
        assert tool["name"] and tool["description"] and tool["tags"]


def test_categories_start_with_all_and_cover_every_tag():
    categories = _app._category_payload()

    assert categories[0] == "All"
    tags = {tag for tool in _app._TOOLS for tag in tool["tags"]}
    assert tags == set(categories[1:])


def test_widget_assets_are_fully_substituted():
    assert "__SLLS_" not in _app._WIDGET_JS
    # The Fluent palette of the launcher, on the shared --ui-* tokens.
    assert "--ui-accent: #4883f7;" in _app._WIDGET_CSS
    assert "'Segoe UI'" in _app._WIDGET_CSS


def test_the_launcher_is_branded_as_semantic_link_labs():
    from sempy_labs._ui_components import ICONS

    assert 'brandName.textContent = "Semantic Link Labs";' in _app._WIDGET_JS
    assert "Fabric Tools" not in _app._WIDGET_JS
    # A lab flask holding an infinity symbol.
    assert ICONS["semantic_link_labs"] in _app._WIDGET_JS


def test_tools_open_inside_the_full_screen_shell():
    # Full screen applies to the shell (launcher + tools), so opening a tool
    # does not drop out of full screen, and Home returns to the tool list.
    assert 'root.closest(".slls-app-shell")' in _app._WIDGET_JS
    assert 'sllsSetupFullscreen(shell(), fsBtn, "slls-app-fs"' in _app._WIDGET_JS
    assert '{ action: "home" }' in _app._WIDGET_JS
    assert ".slls-app-shell.slls-app-fs {" in _app._WIDGET_CSS


def test_home_fullscreen_button_is_wired_before_first_click():
    setup = 'sllsSetupFullscreen(shell(), fsBtn, "slls-app-fs"'
    setup_index = _app._WIDGET_JS.index(setup)
    preceding_frame = _app._WIDGET_JS.rfind("requestAnimationFrame", 0, setup_index)

    assert preceding_frame == -1


def test_home_render_does_not_use_back_state_before_initialization():
    # The original bug: placeBack() ran synchronously while rendering, before
    # `placeQueued` existed, which threw and left the home screen empty. Calls
    # inside deferred callbacks are fine — they run after render() returns.
    import re

    js = _app._WIDGET_JS
    before = js[: js.index("let placeQueued = false;")]

    for match in re.finditer(r"placeBack\(\);", before):
        line = before[before.rfind("\n", 0, match.start()) + 1 : match.end()]
        assert "=>" in line, f"placeBack() runs during render: {line.strip()}"


def test_an_opened_tools_own_chrome_drives_the_app():
    # A tool's full-screen / theme buttons are re-pointed at the app, and the
    # app mirrors the theme back onto them. Programmatic clicks used by tools to
    # restore state after rendering must remain local to the tool.
    assert "function interceptToolChrome(event)" in _app._WIDGET_JS
    assert "if (syntheticClick || !event.isTrusted) return;" in _app._WIDGET_JS
    assert "fsBtn.click();" in _app._WIDGET_JS
    assert 'model.set("dark_mode", !(model.get("dark_mode")' in _app._WIDGET_JS


def test_the_shell_is_the_only_owner_of_full_screen():
    # A tool's own full screen is a fixed, full-viewport overlay. Driving it
    # while the shell is already full screen stacks a second overlay inside the
    # notebook's widget wrappers and hides the tool, so the app never does it.
    assert "driveFullscreen" not in _app._WIDGET_JS
    assert "clickWithoutNativeFullscreen" not in _app._WIDGET_JS


def test_hosted_tools_are_marked_while_the_shell_is_full_screen():
    # Tools scope their full-height layout to their own :fullscreen rules, which
    # cannot match when the shell is the element in full screen.
    assert "function syncToolFullscreenClass()" in _app._WIDGET_JS
    assert 'child.classList.toggle("slls-app-fs-tool", on);' in _app._WIDGET_JS
    assert (
        'document.addEventListener("fullscreenchange", syncHostedTool);'
        in _app._WIDGET_JS
    )


def test_the_open_tools_toggle_shows_the_shells_full_screen_state():
    # Re-labelled rather than clicked: clicking opens the tool's own overlay.
    js = _app._WIDGET_JS

    assert "const fullscreen = toolFullscreenBtn(node);" in js
    assert "fullscreen.innerHTML = on ? FS_EXIT_SVG : FS_ENTER_SVG;" in js
    assert 'fullscreen.setAttribute("aria-label", text);' in js


def test_only_the_header_toggle_is_treated_as_full_screen():
    # Tools also ship panel controls such as "Expand DAX editor to full screen"
    # and "Show the Vertipaq Analyzer full screen", which must keep their own
    # icon and behavior.
    js = _app._WIDGET_JS

    assert (
        'FS_TOGGLE_LABELS = ["full screen", "exit full screen", "toggle full screen"]'
        in js
    )
    assert "FS_TOGGLE_LABELS.indexOf(label.trim()) >= 0" in js
    assert "function toolFullscreenBtn(node)" in js
    assert "btn === toolFullscreenBtn(node)" in js


def test_every_tool_labels_its_full_screen_toggle_recognizably():
    import importlib
    from pathlib import Path

    recognized = ("Full screen", "Exit full screen", "Toggle full screen")
    shared_helpers = (
        "SetupFullscreen",
        "fullscreen_toggle_script",
        "fullscreen_setup_js",
    )

    for tool in _app._TOOLS:
        module = importlib.import_module(tool["module"])
        source = Path(module.__file__).read_text(encoding="utf-8")
        assert any(label in source for label in recognized) or any(
            helper in source for helper in shared_helpers
        ), tool["key"]


def test_opening_a_mounted_tool_is_applied_before_notifying_the_kernel():
    # An already-open tool is just a DOM toggle, so it needs no kernel wait.
    js = _app._WIDGET_JS
    start = js.index('card.addEventListener("click"')
    block = js[start : start + 200]

    assert "pendingKey = tool.key;" in block
    assert block.index("renderView();") < block.index('send({ action: "launch"')


def test_a_tool_is_revealed_only_once_its_own_dom_exists():
    # Otherwise the launcher hides first and the tool arrives in pieces, showing
    # an empty frame holding just the Back button.
    js = _app._WIDGET_JS

    assert "const open = !!activeTool() && !!toolNode(key);" in js
    assert 'root.classList.toggle("slls-app-tool-open", open);' in js
    assert "new MutationObserver(() => { applyView(); placeBack(); })" in js


def test_the_tool_and_its_back_button_are_revealed_together():
    js = _app._WIDGET_JS
    block = js[js.index("function applyView()") : js.index("function renderView()")]

    assert "if (openKey !== lastOpenKey)" in block
    assert "applyBack();" in block


def test_going_home_switches_the_view_before_notifying_the_kernel():
    js = _app._WIDGET_JS
    start = js.index('backBtn.addEventListener("click"')
    block = js[start : start + 200]

    assert 'pendingKey = "";' in block
    assert block.index("renderView();") < block.index('send({ action: "home" })')


def test_a_failed_launch_clears_the_optimistic_view():
    # A failed launch leaves active_tool unchanged, so it fires no change event.
    assert (
        '(model.get("status") || {}).kind === "error") pendingKey = null;'
        in _app._WIDGET_JS
    )


def test_mounted_tools_are_addressable_by_key():
    from inspect import getsource

    assert "function applyToolVisibility(key)" in _app._WIDGET_JS
    assert "slls-app-tool-{tool['key']}" in getsource(_app.app)


def test_tool_modules_are_imported_off_the_click_path():
    from inspect import getsource

    assert "daemon=True" in getsource(_app._prefetch_tool_modules)
    assert "_prefetch_tool_modules()" in getsource(_app.app)


def test_a_hosted_tool_fills_the_shell_while_full_screen():
    # Tools render as a centered card, which would leave the rest of the screen
    # empty once the shell goes full screen.
    for shell in (".slls-app-shell.slls-app-fs", ".slls-app-shell:fullscreen"):
        assert f"{shell} .slls-app-tool {{ padding: 0; }}" in _app._WIDGET_CSS
        assert f"{shell} .slls-app-tool > * {{" in _app._WIDGET_CSS
    assert "max-width: none; width: 100%;" in _app._WIDGET_CSS
    assert "min-height: 100vh;" in _app._WIDGET_CSS


def test_the_dark_theme_matches_the_tools_background():
    from sempy_labs._ui_components import DARK_THEME_VARS

    assert "--ui-bg: #0e1116;" in DARK_THEME_VARS
    assert "--ui-bg: #0e1116;" in _app._WIDGET_CSS


def test_displayed_widgets_are_captured_for_the_shell():
    import builtins

    import ipywidgets
    import IPython.display as ipython_display

    original = ipython_display.display
    tool_widget = ipywidgets.Button()
    passed_through = []

    def stub(*objects, **kwargs):
        passed_through.append(objects)

    builtins.display = stub
    try:
        collected = []
        with _app._capture_displayed_widgets(collected):
            ipython_display.display(tool_widget)
            # A host-provided (e.g. Fabric) display must be intercepted too.
            builtins.display(tool_widget)
            # Tools import display inside the function, at call time.
            from IPython.display import display as imported_at_call_time

            imported_at_call_time(tool_widget)

        assert collected == [tool_widget] * 3
        assert passed_through == []
        assert ipython_display.display is original
        assert builtins.display is stub
    finally:
        del builtins.display
