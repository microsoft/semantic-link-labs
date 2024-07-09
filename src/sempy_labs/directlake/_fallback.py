import sempy.fabric as fabric
import numpy as np
from typing import Optional
import sempy_labs._icons as icons


def check_fallback_reason(dataset: str, workspace: Optional[str] = None):
    """
    Shows the reason a table in a Direct Lake semantic model would fallback to DirectQuery.

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
        The tables in the semantic model and their fallback reason.
    """

    workspace = fabric.resolve_workspace_name(workspace)

    dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
    dfP_filt = dfP[dfP["Mode"] == "DirectLake"]

    if len(dfP_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{dataset}' semantic model is not in Direct Lake. This function is only applicable to Direct Lake semantic models."
        )
    else:
        df = fabric.evaluate_dax(
            dataset=dataset,
            workspace=workspace,
            dax_string="""
        SELECT [TableName] AS [Table Name],[FallbackReason] AS [FallbackReasonID]
        FROM $SYSTEM.TMSCHEMA_DELTA_TABLE_METADATA_STORAGES
        """,
        )

        value_mapping = {
            0: "No reason for fallback",
            1: "This table is not framed",
            2: "This object is a view in the lakehouse",
            3: "The table does not exist in the lakehouse",
            4: "Transient error",
            5: "Using OLS will result in fallback to DQ",
            6: "Using RLS will result in fallback to DQ",
        }

        # Create a new column based on the mapping
        df["Fallback Reason Detail"] = np.vectorize(value_mapping.get)(
            df["FallbackReasonID"]
        )

        return df
