import sempy
import sempy.fabric as fabric
import pandas as pd
from sempy_labs._list_functions import list_tables, list_annotations
from typing import Optional
from sempy._utils._log import log


@log
def list_direct_lake_model_calc_tables(dataset: str, workspace: Optional[str] = None):
    """
    Shows the calculated tables and their respective DAX expression for a Direct Lake model (which has been migrated from import/DirectQuery.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the calculated tables which were migrated to Direct Lake and whose DAX expressions are stored as model annotations.
    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    df = pd.DataFrame(columns=["Table Name", "Source Expression"])

    dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
    dfP_filt = dfP[dfP["Mode"] == "DirectLake"]

    if len(dfP_filt) == 0:
        print(f"The '{dataset}' semantic model is not in Direct Lake mode.")
    else:
        dfA = list_annotations(dataset, workspace)
        dfT = list_tables(dataset, workspace)
        dfA_filt = dfA[
            (dfA["Object Type"] == "Model") & (dfA["Annotation Name"].isin(dfT["Name"]))
        ]

        for i, r in dfA_filt.iterrows():
            tName = r["Annotation Name"]
            se = r["Annotation Value"]

            new_data = {"Table Name": tName, "Source Expression": se}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        return df
