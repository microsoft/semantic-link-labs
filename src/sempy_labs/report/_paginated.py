from typing import Optional
import pandas as pd
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    resolve_item_id,
    _create_dataframe,
)


def get_report_datasources(
    report: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns a list of data sources for the specified paginated report (RDL) from the specified workspace.

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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    report_id = resolve_item_id(
        item=report, type="PaginatedReport", workspace=workspace
    )

    response = _base_api(
        request=f"v1.0/myorg/groups/{workspace_id}/reports/{report_id}/datasources"
    )

    for i in response.json().get("value", []):
        conn = i.get("connectionDetails", {})
        new_data = {
            "Report Name": report,
            "Report Id": report_id,
            "Datasource Id": i.get("datasourceId"),
            "Datasource Type": i.get("datasourceType"),
            "Gateway Id": i.get("gatewayId"),
            "Server": conn.get("server") if conn else None,
            "Database": conn.get("database") if conn else None,
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df
