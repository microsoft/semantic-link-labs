import sempy.fabric as fabric
from sempy_labs._helper_functions import resolve_lakehouse_name
from sempy_labs._list_functions import list_lakehouses
from typing import Optional
import sempy_labs._icons as icons


def get_shared_expression(
    lakehouse: Optional[str] = None, workspace: Optional[str] = None
) -> str:
    """
    Dynamically generates the M expression used by a Direct Lake model for a given lakehouse.

    Parameters
    ----------
    lakehouse : str, default=None
        The Fabric lakehouse used by the Direct Lake semantic model.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str, default=None
        The Fabric workspace used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        Shows the expression which can be used to connect a Direct Lake semantic model to its SQL Endpoint.
    """

    workspace = fabric.resolve_workspace_name(workspace)
    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id)

    dfL = list_lakehouses(workspace=workspace)
    lakeDetail = dfL[dfL["Lakehouse Name"] == lakehouse]

    sqlEPCS = lakeDetail["SQL Endpoint Connection String"].iloc[0]
    sqlepid = lakeDetail["SQL Endpoint ID"].iloc[0]
    provStatus = lakeDetail["SQL Endpoint Provisioning Status"].iloc[0]

    if provStatus == "InProgress":
        raise ValueError(
            f"{icons.red_dot} The SQL Endpoint for the '{lakehouse}' lakehouse within the '{workspace}' workspace has not yet been provisioned. Please wait until it has been provisioned."
        )

    sh = (
        'let\n\tdatabase = Sql.Database("'
        + sqlEPCS
        + '", "'
        + sqlepid
        + '")\nin\n\tdatabase'
    )

    return sh
