from typing import Optional, Literal
from uuid import UUID
import pandas as pd
from sempy._utils._log import log
import sempy_labs.sql_endpoint as sql


@log
def list_sql_endpoints(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the SQL endpoints within a workspace.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the SQL endpoints within a workspace.
    """

    return sql.list_sql_endpoints(workspace=workspace)


@log
def refresh_sql_endpoint_metadata(
    item: str | UUID,
    type: Literal["Lakehouse", "MirroredDatabase"],
    workspace: Optional[str | UUID] = None,
    timeout_unit: Literal["Seconds", "Minutes", "Hours", "Days"] = "Minutes",
    timeout_value: int = 15,
) -> pd.DataFrame:
    """
    Refreshes the metadata of a SQL endpoint.

    This is a wrapper function for the following API: `Items - Refresh Sql Endpoint Metadata <https://learn.microsoft.com/rest/api/fabric/sqlendpoint/items/refresh-sql-endpoint-metadata>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    item : str | uuid.UUID
        The name or ID of the item (Lakehouse or MirroredDatabase).
    type : typing.Literal['Lakehouse', 'MirroredDatabase']
        The type of the item. Must be 'Lakehouse' or 'MirroredDatabase'.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    timeout_unit : typing.Literal['Seconds', 'Minutes', 'Hours', 'Days'], default='Minutes'
        The unit of time for the request duration before timing out. Additional duration types may be added over time.
    timeout_value : int, default=15
        The number of time units in the request duration.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the status of the metadata refresh operation.
    """

    return sql.refresh_sql_endpoint_metadata(
        item=item,
        type=type,
        workspace=workspace,
        timeout_unit=timeout_unit,
        timeout_value=timeout_value,
    )
