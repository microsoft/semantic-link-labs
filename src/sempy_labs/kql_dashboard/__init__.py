from ._items import (
    create_kql_dashboard,
    delete_kql_dashboard,
    list_kql_dashboards,
)
from ._workspace_monitoring_dashboard import (
    create_workspace_monitoring_dashboard,
)

__all__ = [
    "create_kql_dashboard",
    "delete_kql_dashboard",
    "list_kql_dashboards",
    "create_workspace_monitoring_dashboard",
]
