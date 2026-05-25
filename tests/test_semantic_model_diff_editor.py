"""Tests for :mod:`sempy_labs.semantic_model._diff_editor`.

These tests cover two layers of the interactive semantic-model diff editor:

* **Functionality** — the pure diff/apply helpers (``_split_lines``,
  ``_join_lines``, ``_compute_file_hunks``, ``_compute_diff``,
  ``_apply_hunks_to_lines``) that drive the comparison and cherry-pick logic
  exposed by the widget.
* **Design** — structural checks on the public ``semantic_model_diff_editor``
  function (signature, ``@log`` decoration, docstring, public export) and on
  the embedded CSS / JS that powers the UI (scoped class names, theming,
  required interaction surface).
"""

from __future__ import annotations

import importlib
import inspect

import pytest


# ---------------------------------------------------------------------------
# Module / public-surface fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def diff_editor_module():
    """Import the diff-editor module directly (bypasses the top-level package)."""
    return importlib.import_module("sempy_labs.semantic_model._diff_editor")


@pytest.fixture(scope="module")
def diff_editor_fn(diff_editor_module):
    """Resolve the public ``semantic_model_diff_editor`` entry point."""
    return diff_editor_module.semantic_model_diff_editor


# ---------------------------------------------------------------------------
# Functionality: pure helpers
# ---------------------------------------------------------------------------


class TestSplitJoinLines:
    """Tests for the line-splitting / line-joining helpers."""

    def test_split_lines_empty(self, diff_editor_module):
        """Empty / falsy input yields an empty list."""
        assert diff_editor_module._split_lines("") == []
        assert diff_editor_module._split_lines(None) == []

    def test_split_lines_preserves_blank_lines(self, diff_editor_module):
        """Blank lines in the middle of text are preserved."""
        text = "a\n\nb\n"
        assert diff_editor_module._split_lines(text) == ["a", "", "b"]

    def test_split_lines_drops_trailing_newline(self, diff_editor_module):
        """A single trailing newline is dropped (matches ``str.splitlines``)."""
        assert diff_editor_module._split_lines("a\nb\n") == ["a", "b"]

    def test_join_lines_empty(self, diff_editor_module):
        """Joining an empty list yields an empty string (no trailing newline)."""
        assert diff_editor_module._join_lines([]) == ""

    def test_join_lines_appends_trailing_newline(self, diff_editor_module):
        """Non-empty input is joined with ``\\n`` plus a trailing newline."""
        assert diff_editor_module._join_lines(["a", "b"]) == "a\nb\n"

    def test_split_join_roundtrip(self, diff_editor_module):
        """``split → join`` reconstructs the original text (modulo final newline)."""
        text = "line1\nline2\nline3\n"
        lines = diff_editor_module._split_lines(text)
        assert diff_editor_module._join_lines(lines) == text


# ---------------------------------------------------------------------------
# Functionality: hunk computation
# ---------------------------------------------------------------------------


class TestComputeFileHunks:
    """Tests for the per-file hunk computation."""

    def test_identical_text_yields_only_equal_hunks(self, diff_editor_module):
        """When source equals target, every opcode is ``equal`` and counts are 0."""
        result = diff_editor_module._compute_file_hunks("a\nb\nc", "a\nb\nc")
        assert result["additions"] == 0
        assert result["deletions"] == 0
        assert all(h["type"] == "equal" for h in result["hunks"])

    def test_pure_insertion(self, diff_editor_module):
        """Lines added in target produce an ``insert`` hunk and bump additions."""
        result = diff_editor_module._compute_file_hunks("a\nb", "a\nNEW\nb")
        insert_hunks = [h for h in result["hunks"] if h["type"] == "insert"]
        assert len(insert_hunks) == 1
        assert insert_hunks[0]["target_lines"] == ["NEW"]
        assert insert_hunks[0]["source_lines"] == []
        assert result["additions"] == 1
        assert result["deletions"] == 0

    def test_pure_deletion(self, diff_editor_module):
        """Lines removed from target produce a ``delete`` hunk and bump deletions."""
        result = diff_editor_module._compute_file_hunks("a\nGONE\nb", "a\nb")
        delete_hunks = [h for h in result["hunks"] if h["type"] == "delete"]
        assert len(delete_hunks) == 1
        assert delete_hunks[0]["source_lines"] == ["GONE"]
        assert delete_hunks[0]["target_lines"] == []
        assert result["additions"] == 0
        assert result["deletions"] == 1

    def test_replacement(self, diff_editor_module):
        """A modified line surfaces as a ``replace`` hunk with both sides populated."""
        result = diff_editor_module._compute_file_hunks(
            "line1\nline2\nline3", "line1\nLINE2 modified\nline3"
        )
        replace_hunks = [h for h in result["hunks"] if h["type"] == "replace"]
        assert len(replace_hunks) == 1
        assert replace_hunks[0]["source_lines"] == ["line2"]
        assert replace_hunks[0]["target_lines"] == ["LINE2 modified"]
        assert result["additions"] == 1
        assert result["deletions"] == 1

    def test_returns_full_line_arrays(self, diff_editor_module):
        """Returned ``source_lines``/``target_lines`` are the full split inputs."""
        result = diff_editor_module._compute_file_hunks("a\nb", "a\nc")
        assert result["source_lines"] == ["a", "b"]
        assert result["target_lines"] == ["a", "c"]


# ---------------------------------------------------------------------------
# Functionality: file-level diff classification
# ---------------------------------------------------------------------------


def _make_def(files: dict) -> dict:
    """Build a fake definition dict in the shape consumed by ``_compute_diff``."""
    return {
        "files": [{"path": p, "content": c} for p, c in files.items()]
    }


class TestComputeDiff:
    """Tests for the file-level diff classifier."""

    def test_unchanged_file(self, diff_editor_module):
        """Identical content on both sides ⇒ ``unchanged``."""
        src = _make_def({"model.bim": "X"})
        tgt = _make_def({"model.bim": "X"})
        diff = diff_editor_module._compute_diff(src, tgt, "TMSL")
        assert len(diff["files"]) == 1
        assert diff["files"][0]["status"] == "unchanged"
        assert diff["files"][0]["additions"] == 0
        assert diff["files"][0]["deletions"] == 0
        assert diff["format"] == "TMSL"

    def test_added_file(self, diff_editor_module):
        """File only present in target ⇒ ``added``."""
        src = _make_def({})
        tgt = _make_def({"tables/T1.tmdl": "a\nb\n"})
        diff = diff_editor_module._compute_diff(src, tgt, "TMDL")
        files = diff["files"]
        assert len(files) == 1
        f = files[0]
        assert f["status"] == "added"
        assert f["source_lines"] == []
        assert f["target_lines"] == ["a", "b"]
        assert f["additions"] == 2

    def test_removed_file(self, diff_editor_module):
        """File only present in source ⇒ ``removed``."""
        src = _make_def({"tables/T1.tmdl": "a\nb\n"})
        tgt = _make_def({})
        diff = diff_editor_module._compute_diff(src, tgt, "TMDL")
        files = diff["files"]
        assert len(files) == 1
        f = files[0]
        assert f["status"] == "removed"
        assert f["source_lines"] == ["a", "b"]
        assert f["target_lines"] == []
        assert f["deletions"] == 2

    def test_modified_file(self, diff_editor_module):
        """Same path, different content ⇒ ``modified`` with hunks populated."""
        src = _make_def({"tables/T1.tmdl": "a\nb\nc"})
        tgt = _make_def({"tables/T1.tmdl": "a\nB\nc"})
        diff = diff_editor_module._compute_diff(src, tgt, "TMDL")
        files = diff["files"]
        assert len(files) == 1
        f = files[0]
        assert f["status"] == "modified"
        assert f["additions"] >= 1
        assert f["deletions"] >= 1
        assert any(h["type"] != "equal" for h in f["hunks"])

    def test_files_are_sorted_changed_first(self, diff_editor_module):
        """Changed files sort before unchanged files; ties broken by path."""
        src = _make_def({
            "z_unchanged.tmdl": "same",
            "a_modified.tmdl": "old",
            "b_removed.tmdl": "gone",
        })
        tgt = _make_def({
            "z_unchanged.tmdl": "same",
            "a_modified.tmdl": "new",
            "c_added.tmdl": "fresh",
        })
        diff = diff_editor_module._compute_diff(src, tgt, "TMDL")
        statuses = [f["status"] for f in diff["files"]]
        # Modified first, then added, then removed, then unchanged.
        assert statuses == ["modified", "added", "removed", "unchanged"]

    def test_format_is_passed_through(self, diff_editor_module):
        """The ``format`` argument round-trips into the diff payload."""
        diff = diff_editor_module._compute_diff(_make_def({}), _make_def({}), "TMDL")
        assert diff["format"] == "TMDL"


# ---------------------------------------------------------------------------
# Functionality: hunk application
# ---------------------------------------------------------------------------


class TestApplyHunksToLines:
    """Tests for the cherry-pick ``_apply_hunks_to_lines`` logic."""

    @pytest.fixture
    def modified_file(self, diff_editor_module):
        """A small modified-file fixture covering equal/replace/insert/equal hunks."""
        src = "line1\nline2\nline3\nline4\nline5"
        tgt = "line1\nLINE2 modified\nline3\nNEW LINE\nline4\nline5"
        return diff_editor_module._compute_file_hunks(src, tgt)

    def _change_indices(self, file_diff):
        """Return the set of indices of hunks that are *not* ``equal``."""
        return {i for i, h in enumerate(file_diff["hunks"]) if h["type"] != "equal"}

    def test_select_none_target_preserves_target(self, diff_editor_module, modified_file):
        """Apply with no selection toward target ⇒ target stays as-is."""
        result = diff_editor_module._apply_hunks_to_lines(modified_file, set(), "target")
        assert result == modified_file["target_lines"]

    def test_select_none_source_preserves_source(self, diff_editor_module, modified_file):
        """Apply with no selection toward source ⇒ source stays as-is."""
        result = diff_editor_module._apply_hunks_to_lines(modified_file, set(), "source")
        assert result == modified_file["source_lines"]

    def test_select_all_target_yields_source(self, diff_editor_module, modified_file):
        """Apply *all* changes toward target ⇒ target becomes the source."""
        sel = self._change_indices(modified_file)
        result = diff_editor_module._apply_hunks_to_lines(modified_file, sel, "target")
        assert result == modified_file["source_lines"]

    def test_select_all_source_yields_target(self, diff_editor_module, modified_file):
        """Apply *all* changes toward source ⇒ source becomes the target."""
        sel = self._change_indices(modified_file)
        result = diff_editor_module._apply_hunks_to_lines(modified_file, sel, "source")
        assert result == modified_file["target_lines"]

    def test_partial_selection_target(self, diff_editor_module, modified_file):
        """Only selected hunks flip; unselected hunks keep the destination value."""
        # Pick the first non-equal hunk only.
        change_indices = sorted(self._change_indices(modified_file))
        partial = {change_indices[0]}
        result = diff_editor_module._apply_hunks_to_lines(modified_file, partial, "target")
        # Result mixes: source lines for the picked hunk, target lines for the rest.
        # The original source has 'line2' and we picked the replace hunk, so 'line2'
        # should appear; but the insert ('NEW LINE') should still be present since
        # we didn't select it.
        assert "line2" in result
        assert "NEW LINE" in result
        assert "LINE2 modified" not in result

    def test_equal_hunks_are_always_preserved(self, diff_editor_module, modified_file):
        """Lines inside ``equal`` hunks always survive regardless of selection."""
        result_a = diff_editor_module._apply_hunks_to_lines(modified_file, set(), "target")
        result_b = diff_editor_module._apply_hunks_to_lines(
            modified_file, self._change_indices(modified_file), "target"
        )
        # 'line1', 'line3', 'line4', 'line5' appear in equal hunks → both outputs.
        for ln in ("line1", "line3", "line4", "line5"):
            assert ln in result_a
            assert ln in result_b

    def test_empty_file_diff(self, diff_editor_module):
        """A file diff with no hunks yields an empty result."""
        assert diff_editor_module._apply_hunks_to_lines({}, set(), "target") == []
        assert diff_editor_module._apply_hunks_to_lines({"hunks": []}, set(), "source") == []


# ---------------------------------------------------------------------------
# Design / public-surface checks
# ---------------------------------------------------------------------------


class TestPublicSurface:
    """Sanity checks on the exported function's signature and metadata."""

    def test_function_is_callable(self, diff_editor_fn):
        """The entry point must be a callable."""
        assert callable(diff_editor_fn)

    def test_function_signature_has_expected_parameters(self, diff_editor_fn):
        """The public signature exposes the documented parameters with right defaults."""
        sig = inspect.signature(diff_editor_fn)
        params = sig.parameters
        for name in (
            "source_workspace",
            "source_dataset",
            "target_workspace",
            "target_dataset",
            "format",
            "dark_mode",
        ):
            assert name in params, f"missing parameter: {name}"
        assert params["source_workspace"].default is None
        assert params["source_dataset"].default is None
        assert params["target_workspace"].default is None
        assert params["target_dataset"].default is None
        assert params["format"].default == "TMDL"
        assert params["dark_mode"].default is False

    def test_function_has_comprehensive_docstring(self, diff_editor_fn):
        """The docstring must include the canonical numpydoc sections and SP note."""
        doc = diff_editor_fn.__doc__ or ""
        assert "Parameters" in doc
        assert "----------" in doc
        # Each documented parameter must be referenced in the docstring.
        for name in (
            "source_workspace",
            "source_dataset",
            "target_workspace",
            "target_dataset",
            "format",
            "dark_mode",
        ):
            assert name in doc, f"docstring is missing description of {name!r}"
        # Service-Principal authentication is supported and must be advertised.
        assert "Service Principal" in doc

    def test_function_is_exported_from_semantic_model_package(self):
        """``semantic_model_diff_editor`` is re-exported from ``sempy_labs.semantic_model``."""
        pkg = importlib.import_module("sempy_labs.semantic_model")
        assert hasattr(pkg, "semantic_model_diff_editor")
        assert "semantic_model_diff_editor" in getattr(pkg, "__all__", [])

    def test_function_raises_on_invalid_format(self, diff_editor_fn):
        """An unknown ``format`` value is rejected with a clear ``ValueError``."""
        with pytest.raises(ValueError, match="format"):
            diff_editor_fn(format="XML")

    def test_bim_alias_is_accepted(self, diff_editor_fn, monkeypatch):
        """``format='BIM'`` is accepted as an alias for ``'TMSL'`` (no ValueError).

        We intercept ``anywidget`` import to short-circuit the widget construction —
        we only care that the format-validation branch accepts the alias.
        """
        # Force the early ImportError path so we don't have to spin up a widget.
        import builtins
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "anywidget":
                raise ImportError("blocked for test")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        # Should reach the anywidget-import block (i.e. format validation passed)
        # and raise an ImportError rather than a ValueError.
        with pytest.raises(ImportError):
            diff_editor_fn(format="BIM")


# ---------------------------------------------------------------------------
# Design: CSS and JS content
# ---------------------------------------------------------------------------


class TestEmbeddedCss:
    """Structural checks on the embedded ``_WIDGET_CSS`` stylesheet."""

    def test_css_is_nonempty_string(self, diff_editor_module):
        """The CSS constant exists and is a non-trivial string."""
        css = diff_editor_module._WIDGET_CSS
        assert isinstance(css, str)
        assert len(css) > 500

    def test_css_uses_scoped_class_prefix(self, diff_editor_module):
        """All UI rules are scoped under ``.slls-de-`` to avoid host-page bleed."""
        css = diff_editor_module._WIDGET_CSS
        assert ".slls-de" in css
        # The root container class anchors every theme rule.
        assert ".slls-de {" in css or ".slls-de{" in css

    def test_css_supports_dark_and_light_themes(self, diff_editor_module):
        """Both dark and light theme branches are present."""
        css = diff_editor_module._WIDGET_CSS
        assert ".slls-de-dark" in css
        assert "prefers-color-scheme: dark" in css

    @pytest.mark.parametrize(
        "selector",
        [
            ".slls-de-pickers",       # source/target picker layout
            ".slls-de-stats",         # summary statistics row
            ".slls-de-files",         # file list container
            ".slls-de-file",          # per-file row
            ".slls-de-hunk",          # per-hunk container
            ".slls-de-diff",          # diff body
            ".slls-de-diff-row",      # unified-diff row
            ".slls-de-check",         # selection checkbox
            ".slls-de-badge",         # status badge
            ".slls-de-status",        # status banner
            ".slls-de-confirm",       # confirmation prompt
        ],
    )
    def test_css_defines_required_selectors(self, diff_editor_module, selector):
        """Each major UI region has a corresponding style rule."""
        assert selector in diff_editor_module._WIDGET_CSS

    @pytest.mark.parametrize(
        "color_token",
        [
            "--slls-success",   # additions (green)
            "--slls-danger",    # deletions (red)
            "--slls-warning",   # modified (orange)
            "--slls-accent",    # primary action (blue)
        ],
    )
    def test_css_defines_semantic_color_tokens(self, diff_editor_module, color_token):
        """Status colours are exposed as CSS custom properties for theming."""
        assert color_token in diff_editor_module._WIDGET_CSS


class TestEmbeddedJs:
    """Structural checks on the embedded ``_WIDGET_JS`` ESM."""

    def test_js_is_nonempty_string(self, diff_editor_module):
        """The JS constant exists and is a non-trivial string."""
        js = diff_editor_module._WIDGET_JS
        assert isinstance(js, str)
        assert len(js) > 500

    def test_js_is_anywidget_esm(self, diff_editor_module):
        """The ESM exports a default object with a ``render`` function."""
        js = diff_editor_module._WIDGET_JS
        assert "function render(" in js
        assert "export default" in js
        assert "render" in js

    @pytest.mark.parametrize(
        "traitlet_name",
        [
            "workspaces",
            "source_workspace_id",
            "target_workspace_id",
            "source_dataset_id",
            "target_dataset_id",
            "source_datasets",
            "target_datasets",
            "format",
            "diff",
            "status",
            "pending_action",
            "run",
            "dark_mode",
        ],
    )
    def test_js_references_each_traitlet(self, diff_editor_module, traitlet_name):
        """The JS reads/writes every traitlet that the Python widget syncs."""
        assert traitlet_name in diff_editor_module._WIDGET_JS

    @pytest.mark.parametrize(
        "action",
        ["load_datasets", "compare", "apply"],
    )
    def test_js_emits_each_pending_action(self, diff_editor_module, action):
        """Every server-side action is reachable from the JS side."""
        assert f'"{action}"' in diff_editor_module._WIDGET_JS \
            or f"'{action}'" in diff_editor_module._WIDGET_JS

    @pytest.mark.parametrize(
        "ui_element",
        [
            "Compare",         # primary action button label
            "Apply selected",  # apply buttons
            "Download patch",  # patch export
            "Filter files",    # filter placeholder
            "Select all",      # bulk selection
            "Expand all",      # bulk expand
        ],
    )
    def test_js_contains_expected_ui_labels(self, diff_editor_module, ui_element):
        """The user-facing labels for headline actions are present in the JS."""
        assert ui_element in diff_editor_module._WIDGET_JS

    def test_js_html_escape_helper_is_defined(self, diff_editor_module):
        """An HTML escape helper guards user-controlled strings rendered to the DOM."""
        js = diff_editor_module._WIDGET_JS
        assert "escapeHtml" in js
