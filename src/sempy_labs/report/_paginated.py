import sempy.fabric as fabric
from typing import Optional
from sempy.fabric.exceptions import FabricHTTPException
import pandas as pd
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
)


def get_report_datasources(
    report: str,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns a list of data sources for the specified paginated report (RDL) from the specified workspace.

    Parameters
    ----------
    report : str | List[str]
        Name(s) of the Power BI report(s).
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace in which the report resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of data sources for the specified paginated report (RDL) from the specified workspace.
    """

    df = pd.DataFrame(
        columns=[
            "Report Name",
            "Report Id",
            "Datasource Id",
            "Datasource Type",
            "Gateway Id",
            "Server",
            "Database",
        ]
    )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    report_id = fabric.resolve_item_id(
        item_name=report, type="PaginatedReport", workspace=workspace_id
    )

    client = fabric.PowerBIRestClient()

    response = client.get(
        f"/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/datasources"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

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
