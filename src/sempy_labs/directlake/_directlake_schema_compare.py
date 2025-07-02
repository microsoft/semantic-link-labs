import sempy.fabric as fabric
import pandas as pd
from .._helper_functions import (
    format_dax_object_name,
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
    resolve_workspace_name,
)
from IPython.display import display
from ..lakehouse import get_lakehouse_columns
from ..directlake._dl_helper import get_direct_lake_source
from typing import Optional
import sempy_labs._icons as icons
from sempy._utils._log import log
from uuid import UUID


@log
def direct_lake_schema_compare(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    **kwargs,
):
    """
    Checks that all the tables in a Direct Lake semantic model map to tables in their corresponding lakehouse and that the columns in each table exist.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if "lakehouse" in kwargs:
        print(
            "The 'lakehouse' parameter has been deprecated as it is no longer necessary. Please remove this parameter from the function going forward."
        )
        del kwargs["lakehouse"]
    if "lakehouse_workspace" in kwargs:
        print(
            "The 'lakehouse_workspace' parameter has been deprecated as it is no longer necessary. Please remove this parameter from the function going forward."
        )
        del kwargs["lakehouse_workspace"]

    fabric.refresh_tom_cache(workspace=workspace)

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    artifact_type, lakehouse_name, lakehouse_id, lakehouse_workspace_id = (
        get_direct_lake_source(dataset=dataset_id, workspace=workspace_id)
    )

    if artifact_type == "Warehouse":
        raise ValueError(
            f"{icons.red_dot} This function is only valid for Direct Lake semantic models which source from Fabric lakehouses (not warehouses)."
        )

    dfP = fabric.list_partitions(dataset=dataset_id, workspace=workspace_id)

    if not any(r["Mode"] == "DirectLake" for _, r in dfP.iterrows()):
        raise ValueError(
            f"{icons.red_dot} The '{dataset_name}' semantic model within the '{workspace_name}' workspace is not in Direct Lake mode."
        )

    if artifact_type is None:
        raise ValueError(
            f"{icons.red_dot} This function only supports Direct Lake semantic models where the source lakehouse resides in the same workpace as the semantic model."
        )

    lakehouse_workspace = resolve_workspace_name(workspace_id=lakehouse_workspace_id)
    dfT = fabric.list_tables(dataset=dataset_id, workspace=workspace_id)
    dfC = fabric.list_columns(dataset=dataset_id, workspace=workspace_id)
    lc = get_lakehouse_columns(lakehouse_name, lakehouse_workspace)

    dfT.rename(columns={"Type": "Table Type"}, inplace=True)
    dfP_filt = dfP[dfP["Mode"] == "DirectLake"]
    dfC = pd.merge(dfC, dfP[["Table Name", "Query"]], on="Table Name", how="inner")
    dfC = pd.merge(
        dfC,
        dfT[["Name", "Table Type"]],
        left_on="Table Name",
        right_on="Name",
        how="inner",
    )
    dfC["Full Column Name"] = format_dax_object_name(dfC["Query"], dfC["Source"])
    dfC_filt = dfC[dfC["Table Type"] == "Table"]
    # Schema compare
    missingtbls = dfP_filt[~dfP_filt["Query"].isin(lc["Table Name"])]
    missingtbls = missingtbls[["Table Name", "Query"]]
    missingtbls.rename(columns={"Query": "Source Table"}, inplace=True)
    missingcols = dfC_filt[~dfC_filt["Full Column Name"].isin(lc["Full Column Name"])]
    missingcols = missingcols[
        ["Table Name", "Column Name", "Type", "Data Type", "Source"]
    ]
    missingcols.rename(columns={"Source": "Source Column"}, inplace=True)

    if len(missingtbls) == 0:
        print(
            f"{icons.green_dot} All tables exist in the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace}' workspace."
        )
    else:
        print(
            f"{icons.yellow_dot} The following tables exist in the '{dataset_name}' semantic model within the '{workspace_name}' workspace"
            f" but do not exist in the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace}' workspace."
        )
        display(missingtbls)
    if len(missingcols) == 0:
        print(
            f"{icons.green_dot} All columns exist in the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace}' workspace."
        )
    else:
        print(
            f"{icons.yellow_dot} The following columns exist in the '{dataset_name}' semantic model within the '{workspace_name}' workspace "
            f"but do not exist in the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace}' workspace."
        )
        display(missingcols)
