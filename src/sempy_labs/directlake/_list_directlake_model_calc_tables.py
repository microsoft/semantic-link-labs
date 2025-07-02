import sempy.fabric as fabric
import pandas as pd
from .._list_functions import list_tables
from ..tom import connect_semantic_model
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from uuid import UUID
from .._helper_functions import (
    resolve_dataset_name_and_id,
    resolve_workspace_name_and_id,
)


@log
def list_direct_lake_model_calc_tables(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows the calculated tables and their respective DAX expression for a Direct Lake model (which has been migrated from import/DirectQuery).

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
    pandas.DataFrame
        A pandas dataframe showing the calculated tables which were migrated to Direct Lake and whose DAX expressions are stored as model annotations.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    df = pd.DataFrame(columns=["Table Name", "Source Expression"])

    with connect_semantic_model(
        dataset=dataset_id, readonly=True, workspace=workspace_id
    ) as tom:

        is_direct_lake = tom.is_direct_lake()

        if not is_direct_lake:
            raise ValueError(
                f"{icons.red_dot} The '{dataset}' semantic model is not in Direct Lake mode."
            )
        else:
            fabric.refresh_tom_cache(workspace=workspace)
            dfA = fabric.list_annotations(dataset=dataset_id, workspace=workspace_id)
            dfT = list_tables(dataset_id, workspace_id)
            dfA_filt = dfA[
                (dfA["Object Type"] == "Model")
                & (dfA["Annotation Name"].isin(dfT["Name"]))
            ]

            for i, r in dfA_filt.iterrows():
                tName = r["Annotation Name"]
                se = r["Annotation Value"]

                new_data = {"Table Name": tName, "Source Expression": se}
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

            return df
