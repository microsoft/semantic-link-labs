import sempy.fabric as fabric
import pandas as pd
from .._helper_functions import (
    format_dax_object_name,
    resolve_dataset_name_and_id,
    resolve_workspace_name_and_id,
)
from typing import Optional, Tuple
from sempy._utils._log import log
from uuid import UUID


@log
def show_unsupported_direct_lake_objects(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns a list of a semantic model's objects which are not supported by Direct Lake based on
    `official documentation <https://learn.microsoft.com/power-bi/enterprise/directlake-overview#known-issues-and-limitations>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame, pandas.DataFrame, pandas.DataFrame
        3 pandas dataframes showing objects in a semantic model which are not supported by Direct Lake.
    """

    pd.options.mode.chained_assignment = None

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    fabric.refresh_tom_cache(workspace=workspace)

    dfT = fabric.list_tables(dataset=dataset_id, workspace=workspace_id)
    dfC = fabric.list_columns(dataset=dataset_id, workspace=workspace_id)
    dfR = fabric.list_relationships(dataset=dataset_id, workspace=workspace_id)

    # Calc tables
    dfT_filt = dfT[dfT["Type"] == "Calculated Table"]
    dfT_filt.rename(columns={"Name": "Table Name"}, inplace=True)
    t = dfT_filt[["Table Name", "Type"]]

    # Calc columns
    dfC_filt = dfC[(dfC["Type"] == "Calculated") | (dfC["Data Type"] == "Binary")]
    c = dfC_filt[["Table Name", "Column Name", "Type", "Data Type", "Source"]]

    # Relationships
    dfC["Column Object"] = format_dax_object_name(dfC["Table Name"], dfC["Column Name"])
    dfR["From Object"] = format_dax_object_name(dfR["From Table"], dfR["From Column"])
    dfR["To Object"] = format_dax_object_name(dfR["To Table"], dfR["To Column"])
    merged_from = pd.merge(
        dfR, dfC, left_on="From Object", right_on="Column Object", how="left"
    )
    merged_to = pd.merge(
        dfR, dfC, left_on="To Object", right_on="Column Object", how="left"
    )

    dfR["From Column Data Type"] = merged_from["Data Type"]
    dfR["To Column Data Type"] = merged_to["Data Type"]

    dfR_filt = dfR[(dfR["From Column Data Type"] != dfR["To Column Data Type"])]
    r = dfR_filt[
        [
            "From Table",
            "From Column",
            "To Table",
            "To Column",
            "From Column Data Type",
            "To Column Data Type",
        ]
    ]

    # print('Calculated Tables are not supported...')
    # display(t)
    # print("Learn more about Direct Lake limitations here: https://learn.microsoft.com/power-bi/enterprise/directlake-overview#known-issues-and-limitations")
    # print('Calculated columns are not supported. Columns of binary data type are not supported.')
    # display(c)
    # print('Columns used for relationship cannot be of data type datetime and they also must be of the same data type.')
    # display(r)

    return t, c, r
