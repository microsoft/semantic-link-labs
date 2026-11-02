import ast
from pathlib import Path
from typing import List, Optional


SOURCE_PATH = (
    Path(__file__).parents[1] / "src" / "sempy_labs" / "_daxformatter.py"
)


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _Requests:
    def __init__(self):
        self.payload = None

    def post(self, url, json, headers):
        self.payload = json
        return _Response([{"formatted": value, "errors": None} for value in json["Dax"]])


def _load_format_dax(requests):
    tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_format_dax"
    )
    module = ast.Module(body=[function], type_ignores=[])
    namespace = {
        "List": List,
        "Optional": Optional,
        "log": lambda value: value,
        "requests": requests,
        "lib_name": "test",
        "lib_version": "0",
    }
    exec(compile(module, str(SOURCE_PATH), "exec"), namespace)
    return namespace["_format_dax"]


def test_format_dax_preserves_define_queries():
    requests = _Requests()
    format_dax = _load_format_dax(requests)
    queries = [
        "DEFINE VAR x = 1 EVALUATE {x}",
        "  DEFINE MEASURE 'Sales'[Amount] = 1 EVALUATE { [Amount] }",
    ]

    assert format_dax(queries) == [value.strip() for value in queries]
    assert requests.payload["Dax"] == queries


def test_format_dax_only_prefixes_model_expressions():
    requests = _Requests()
    format_dax = _load_format_dax(requests)

    assert format_dax(["EVALUATE {1}", "SUM('Sales'[Amount])"]) == [
        "EVALUATE {1}",
        "SUM('Sales'[Amount])",
    ]
    assert requests.payload["Dax"] == [
        "EVALUATE {1}",
        "x :=SUM('Sales'[Amount])",
    ]