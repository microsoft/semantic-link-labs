import sempy.fabric as fabric
import pandas as pd
from typing import Optional
import sempy_labs._icons as icons


def get_direct_lake_guardrails() -> pd.DataFrame:
    """
    Shows the guardrails for when Direct Lake semantic models will fallback to Direct Query
     based on Microsoft's `online documentation <https://learn.microsoft.com/power-bi/enterprise/directlake-overview>`_.

    Parameters
    ----------

    Returns
    -------
    pandas.DataFrame
        A table showing the Direct Lake guardrails by SKU.
    """

    url = "https://learn.microsoft.com/power-bi/enterprise/directlake-overview"

    tables = pd.read_html(url)
    df = tables[0]
    df["Fabric SKUs"] = df["Fabric SKUs"].str.split("/")
    df = df.explode("Fabric SKUs", ignore_index=True)

    return df


def get_sku_size(workspace: Optional[str] = None) -> str:
    """
    Shows the SKU size for a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The SKU size for a workspace.
    """

    workspace = fabric.resolve_workspace_name(workspace)

    dfW = fabric.list_workspaces(filter=f"name eq '{workspace}'")

    if len(dfW) == 0:
        raise ValueError(f"{icons.red_dot} The '{workspace}' is not a valid workspace.")

    capacity_id = dfW["Capacity Id"].iloc[0]
    dfC = fabric.list_capacities()
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
        Sku size of a workspace/capacity

    Returns
    -------
    pandas.DataFrame
        A table showing the Direct Lake guardrails for the given SKU.
    """

    df = get_direct_lake_guardrails()
    filtered_df = df[df["Fabric SKUs"] == sku_size]

    return filtered_df
