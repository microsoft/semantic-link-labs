import sempy.fabric as fabric
import pandas as pd
from typing import Optional


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


def get_sku_size(workspace: Optional[str] = None):
    """
    Shows the SKU size for a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The SKU size for a workspace.
    """

    workspace = fabric.resolve_workspace_name(workspace)

    dfC = fabric.list_capacities()
    dfW = fabric.list_workspaces().sort_values(by="Name", ascending=True)
    dfC.rename(columns={"Id": "Capacity Id"}, inplace=True)
    dfCW = pd.merge(
        dfW,
        dfC[["Capacity Id", "Sku", "Region", "State"]],
        on="Capacity Id",
        how="inner",
    )
    sku_value = dfCW.loc[dfCW["Name"] == workspace, "Sku"].iloc[0]

    return sku_value


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
