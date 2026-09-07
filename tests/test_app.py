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
    state_index = _app._WIDGET_JS.index("let placeQueued = false;")

    assert _app._WIDGET_JS.rfind("placeBack();", 0, state_index) == -1


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
    assert "shellFullscreen" not in _app._WIDGET_JS


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
