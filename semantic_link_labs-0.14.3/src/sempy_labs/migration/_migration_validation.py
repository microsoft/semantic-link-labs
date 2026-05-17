import pandas as pd
from typing import Optional
from .._list_functions import list_semantic_model_objects
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def migration_validation(
    dataset: str,
    new_dataset: str,
    workspace: Optional[str] = None,
    new_dataset_workspace: Optional[str] = None,
) -> pd.DataFrame:
    """
    Shows the objects in the original semantic model and whether then were migrated successfully or not.

    Parameters
    ----------
    dataset : str
        Name of the import/DirectQuery semantic model.
    new_dataset : str
        Name of the Direct Lake semantic model.
    workspace : str, default=None
        The Fabric workspace name in which the import/DirectQuery semantic model exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    new_dataset_workspace : str
        The Fabric workspace name in which the Direct Lake semantic model will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
       A pandas dataframe showing a list of objects and whether they were successfully migrated. Also shows the % of objects which were migrated successfully.
    """

    if dataset == new_dataset:
        raise ValueError(
            f"{icons.red_dot} The 'dataset' and 'new_dataset' parameters are both set to '{dataset}'. These parameters must be set to different values."
        )

    icons.sll_tags.append("DirectLakeMigration")

    dfA = list_semantic_model_objects(dataset=dataset, workspace=workspace)
    dfB = list_semantic_model_objects(
        dataset=new_dataset, workspace=new_dataset_workspace
    )

    def is_migrated(row):
        if row["Object Type"] == "Calculated Table":
            return (
                (dfB["Parent Name"] == row["Parent Name"])
                & (dfB["Object Name"] == row["Object Name"])
                & (dfB["Object Type"].isin(["Calculated Table", "Table"]))
            ).any()
        else:
            return (
                (dfB["Parent Name"] == row["Parent Name"])
                & (dfB["Object Name"] == row["Object Name"])
                & (dfB["Object Type"] == row["Object Type"])
            ).any()

    dfA["Migrated"] = dfA.apply(is_migrated, axis=1)

    denom = len(dfA)
    num = len(dfA[dfA["Migrated"]])
    print(f"{100 * round(num / denom,2)}% migrated")

    return dfA
