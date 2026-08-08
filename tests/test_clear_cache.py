import ast
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from uuid import UUID


SOURCE_PATH = (
    Path(__file__).parents[1] / "src" / "sempy_labs" / "_clear_cache.py"
)


def _source() -> str:
    return SOURCE_PATH.read_text(encoding="utf-8")


def _load_clear_cache():
    module = ast.parse(_source())
    clear_cache = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "clear_cache"
    )
    clear_cache.decorator_list = []
    namespace = {
        "Optional": Optional,
        "UUID": UUID,
        "fabric": SimpleNamespace(execute_xmla=lambda **_kwargs: None),
        "icons": SimpleNamespace(green_dot="success", red_dot="error"),
        "is_default_semantic_model": lambda **_kwargs: False,
        "resolve_workspace_name_and_id": lambda _workspace: ("Workspace", "ws-id"),
        "resolve_dataset_name_and_id": lambda _dataset, _workspace: (
            "Dataset",
            "dataset-id",
        ),
    }
    exec(
        compile(ast.Module(body=[clear_cache], type_ignores=[]), SOURCE_PATH, "exec"),
        namespace,
    )
    return namespace["clear_cache"]


def test_clear_cache_supports_suppressing_confirmation_output(capsys):
    clear_cache = _load_clear_cache()

    clear_cache("Dataset", verbose=False)

    assert capsys.readouterr().out == ""


def test_clear_cache_prints_confirmation_by_default(capsys):
    clear_cache = _load_clear_cache()

    clear_cache("Dataset")

    assert capsys.readouterr().out == (
        "success Cache cleared for the 'Dataset' semantic model within the "
        "'Workspace' workspace.\n"
    )