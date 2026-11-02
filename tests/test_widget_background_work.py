"""Widget work must not be dispatched with a raw background thread.

Fabric PySpark notebooks route widget messages through the running cell, so
trait updates emitted from a worker thread never reach the browser and the
first sempy/TOM call made there blocks on the Spark gateway. Every interactive
tool therefore goes through ``sempy_labs._ui_components.run_widget_task``,
which only uses a thread where that is supported.
"""

from pathlib import Path

import pytest

SRC = Path(__file__).parents[1] / "src" / "sempy_labs"

WIDGET_MODULES = [
    "_delta_analyzer.py",
    "semantic_model/_bpa.py",
    "semantic_model/_dax_perf.py",
    "semantic_model/_direct_lake_migration.py",
    "semantic_model/_find_unused_objects.py",
    "semantic_model/_lineage_view.py",
    "semantic_model/_perspective_editor.py",
    "semantic_model/_refresh_manager.py",
    "semantic_model/_vertipaq_analyzer.py",
]


def test_run_widget_task_runs_on_the_kernel_thread():
    source = (SRC / "_ui_components.py").read_text(encoding="utf-8")
    helper = source[
        source.index("def run_widget_task(") : source.index(
            "# ---", source.index("def run_widget_task(")
        )
    ]

    assert "target(*args)" in helper
    # Nothing may be moved off the kernel thread: a Fabric PySpark notebook
    # drops trait updates emitted from a worker thread.
    assert "threading" not in helper
    assert "PySpark" in helper


@pytest.mark.parametrize("relative_path", WIDGET_MODULES)
def test_widgets_do_not_spawn_raw_threads(relative_path: str):
    source = (SRC / relative_path).read_text(encoding="utf-8")

    assert "threading.Thread(" not in source, (
        f"{relative_path} dispatches work with a raw thread; use "
        "run_widget_task so it also works in a PySpark notebook."
    )
