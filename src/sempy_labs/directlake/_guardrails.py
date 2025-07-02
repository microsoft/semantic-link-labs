import sempy.fabric as fabric
import pandas as pd
from typing import Optional
import sempy_labs._icons as icons
from uuid import UUID
from .._helper_functions import (
    resolve_workspace_name_and_id,
)
from sempy._utils._log import log


@log
def get_direct_lake_guardrails() -> pd.DataFrame:
    """
    Shows the guardrails for when Direct Lake semantic models will fallback to Direct Query
    based on Microsoft's `online documentation <https://learn.microsoft.com/power-bi/enterprise/directlake-overview>`_.

    Returns
    -------
    pandas.DataFrame
        A table showing the Direct Lake guardrails by SKU.
    """

    url = "https://learn.microsoft.com/power-bi/enterprise/directlake-overview"

    tables = pd.read_html(url)
    for df in tables:
        first_column_name = df.columns[0]
        if first_column_name.startswith("Fabric"):
            df[first_column_name] = df[first_column_name].str.split("/")
            df = df.explode(first_column_name, ignore_index=True)
            break

    return df


def get_sku_size(workspace: Optional[str | UUID] = None) -> str:
    """
    Shows the SKU size for a workspace.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The SKU size for a workspace.
    """
    from sempy_labs._capacities import list_capacities

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfW = fabric.list_workspaces(filter=f"id eq '{workspace_id}'")

    if len(dfW) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{workspace_name}' is not a valid workspace."
        )

    capacity_id = dfW["Capacity Id"].iloc[0]
    dfC = list_capacities()
    dfC_filt = dfC[dfC["Id"] == capacity_id]

    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{capacity_id}' Id is not a valid capacity Id."
        )

    return dfC_filt["Sku"].iloc[0]


def get_directlake_guardrails_for_sku(sku_size: str) -> pd.DataFrame:
    """
    Shows the guardrails for Direct Lake based on the SKU used by your workspace's capacity.
    * Use the result of the 'get_sku_size' function as an input for this function's sku_size parameter.*

    Parameters
    ----------
    sku_size : str
        Sku size of a workspace/capacity.

    Returns
    -------
    pandas.DataFrame
        A table showing the Direct Lake guardrails for the given SKU.
    """

    df = get_direct_lake_guardrails()
    col_name = df.columns[0]
    filtered_df = df[df[col_name] == sku_size]

    return filtered_df
