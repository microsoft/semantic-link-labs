from sempy_labs.lakehouse._get_lakehouse_columns import get_lakehouse_columns
from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
from sempy_labs.lakehouse._lakehouse import (
    lakehouse_attached,
    optimize_lakehouse_tables,
)

from sempy_labs.lakehouse._shortcuts import (
    # create_shortcut,
    create_shortcut_onelake,
    delete_shortcut,
)

__all__ = [
    "get_lakehouse_columns",
    "get_lakehouse_tables",
    "lakehouse_attached",
    "optimize_lakehouse_tables",
    # create_shortcut,
    "create_shortcut_onelake",
    "delete_shortcut",
]
