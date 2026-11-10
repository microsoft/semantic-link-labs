import ast
from pathlib import Path
from typing import Optional

import pandas as pd

SOURCE_PATH = (
    Path(__file__).parents[1]
    / "src"
    / "sempy_labs"
    / "semantic_model"
    / "_cancel_query.py"
)

_HELPERS = {
    "_local_name",
    "_refresh_database_ids",
    "_is_refresh_command",
    "_is_blank",
    "_list_running_commands",
}
_CONSTANTS = {"_RUNNING_SESSION_STATUSES", "_REFRESH_ELEMENT_TAGS"}


def _load(sessions: Optional[pd.DataFrame] = None, commands=None) -> dict:
    module = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    body = [
        node
        for node in module.body
        if (isinstance(node, ast.FunctionDef) and node.name in _HELPERS)
        or (
            isinstance(node, ast.Assign)
            and any(getattr(t, "id", None) in _CONSTANTS for t in node.targets)
        )
    ]
    namespace = {
        "pd": pd,
        "Optional": Optional,
        "UUID": str,
        "list_semantic_model_sessions": lambda **_kwargs: sessions,
        "_discover_commands": lambda **_kwargs: commands,
    }
    exec(
        compile(ast.Module(body=body, type_ignores=[]), SOURCE_PATH, "exec"),
        namespace,
    )
    return namespace


REFRESH_XML_REORDERED = (
    '<Batch xmlns="http://schemas.microsoft.com/analysisservices/2003/engine" Transaction="true">'
    '<Refresh xmlns="http://schemas.microsoft.com/analysisservices/2014/engine">'
    "<DatabaseID>ABC-123</DatabaseID><Model><xs:schema "
    'xmlns:xs="http://www.w3.org/2001/XMLSchema" /></Model></Refresh></Batch>'
)
PROCESS_XML = (
    '<Batch xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">'
    "<Process><Object><DatabaseID>abc-123</DatabaseID></Object>"
    "<Type>ProcessFull</Type></Process></Batch>"
)
TMSL_REFRESH = '{"refresh": {"type": "full", "objects": [{"database": "Sales"}]}}'
TMSL_SEQUENCE = (
    '{"sequence": {"operations": [{"refresh": {"type": "full", '
    '"objects": [{"database": "Sales", "table": "T"}]}}]}}'
)
DAX_QUERY = 'EVALUATE ROW("x", 1)'
NON_REFRESH_XML = (
    '<Batch xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">'
    "<Refresh><DatabaseID>abc-123</DatabaseID></Refresh></Batch>"
)


def test_refresh_detection_uses_expanded_names_regardless_of_attributes():
    ns = _load()
    detected = ns["_is_refresh_command"](
        pd.Series(
            [
                REFRESH_XML_REORDERED,
                PROCESS_XML,
                TMSL_REFRESH,
                TMSL_SEQUENCE,
                DAX_QUERY,
                NON_REFRESH_XML,
                None,
                "<not xml",
            ]
        )
    )
    assert detected.tolist() == [True, True, True, True, False, False, False, False]


def test_refresh_database_ids_are_extracted():
    ns = _load()
    assert ns["_refresh_database_ids"](REFRESH_XML_REORDERED) == {"abc-123"}
    assert ns["_refresh_database_ids"](PROCESS_XML) == {"abc-123"}
    assert ns["_refresh_database_ids"](TMSL_SEQUENCE) == {"sales"}
    assert ns["_refresh_database_ids"](DAX_QUERY) is None


def _sessions():
    return pd.DataFrame(
        {
            "SESSION_SPID": [1, 2, 3, 4],
            "SESSION_USER_NAME": ["a", "b", "c", "d"],
            "SESSION_STATUS": [1, 0, 1, 2],
            "SESSION_LAST_COMMAND_START_TIME": [
                "2026-01-01T10:00:00",
                "2026-01-01T10:00:00",
                "2026-01-01T10:00:00",
                "2026-01-01T10:00:00",
            ],
            "SESSION_LAST_COMMAND_END_TIME": [
                "2026-01-01T09:00:00",  # previous command ended -> running
                "",  # idle
                "2026-01-01T10:00:05",  # last command finished -> not running
                None,  # never finished -> running
            ],
        }
    )


def _commands():
    return pd.DataFrame(
        {
            "SESSION_SPID": [1, 2, 3, 4, 99],
            "COMMAND_ELAPSED_TIME_MS": [120000, 120000, 120000, 120000, 120000],
            "COMMAND_TEXT": [DAX_QUERY] * 5,
        }
    )


def test_running_commands_do_not_require_command_end_time():
    ns = _load(sessions=_sessions(), commands=_commands())
    df = ns["_list_running_commands"](dataset="ds", workspace="ws")
    assert "COMMAND_END_TIME" not in df.columns
    assert sorted(df["SESSION_SPID"].tolist()) == [1, 4]


def test_running_commands_use_command_end_time_when_present():
    commands = _commands()
    commands["COMMAND_END_TIME"] = [None, None, None, "2026-01-01T10:00:01", None]
    ns = _load(sessions=_sessions(), commands=commands)
    df = ns["_list_running_commands"](dataset="ds", workspace="ws")
    assert df["SESSION_SPID"].tolist() == [1]


def test_running_commands_handles_empty_results():
    ns = _load(sessions=_sessions(), commands=pd.DataFrame())
    assert ns["_list_running_commands"](dataset="ds", workspace="ws").empty
