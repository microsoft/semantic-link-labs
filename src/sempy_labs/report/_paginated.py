from typing import Optional
import pandas as pd
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    _base_api,
    resolve_item_id,
    _create_dataframe,
)
from sempy._utils._log import log


@log
def get_report_datasources(
    report: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns a list of data sources for the specified paginated report (RDL) from the specified workspace.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the Power BI report.
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace in which the report resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of data sources for the specified paginated report (RDL) from the specified workspace.
    """

    columns = {
        "Report Name": "str",
        "Report Id": "str",
        "Datasource Id": "str",
        "Datasource Type": "str",
        "Gateway Id": "str",
        "Server": "str",
        "Database": "str",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)
    report_id = resolve_item_id(
        item=report, type="PaginatedReport", workspace=workspace
    )

    response = _base_api(
        request=f"v1.0/myorg/groups/{workspace_id}/reports/{report_id}/datasources",
        client="fabric_sp",
    )

    rows = []
    for i in response.json().get("value", []):
        conn = i.get("connectionDetails", {})
        rows.append(
            {
                "Report Name": report,
                "Report Id": report_id,
                "Datasource Id": i.get("datasourceId"),
                "Datasource Type": i.get("datasourceType"),
                "Gateway Id": i.get("gatewayId"),
                "Server": conn.get("server") if conn else None,
                "Database": conn.get("database") if conn else None,
            }
        )
    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df
