from ._copilot import (
    approved_for_copilot,
    set_endorsement,
    make_discoverable,
)
from ._cancel_query import (
    cancel_spid,
    list_semantic_model_sessions,
    list_semantic_model_commands,
    cancel_long_running_queries,
)

__all__ = [
    "approved_for_copilot",
    "set_endorsement",
    "make_discoverable",
    "cancel_spid",
    "list_semantic_model_sessions",
    "list_semantic_model_commands",
    "cancel_long_running_queries",
]
