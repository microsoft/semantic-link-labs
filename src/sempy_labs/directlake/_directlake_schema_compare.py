import sempy.fabric as fabric
import pandas as pd
from sempy_labs._helper_functions import (
    format_dax_object_name,
    resolve_lakehouse_name,
    get_direct_lake_sql_endpoint,
)
from IPython.display import display
from sempy_labs.lakehouse._get_lakehouse_columns import get_lakehouse_columns
from sempy_labs._list_functions import list_tables
from typing import Optional
import sempy_labs._icons as icons
from sempy._utils._log import log


@log
def direct_lake_schema_compare(
    dataset: str,
    workspace: Optional[str] = None,
    lakehouse: Optional[str] = None,
    lakehouse_workspace: Optional[str] = None,
):
    """
    Checks that all the tables in a Direct Lake semantic model map to tables in their corresponding lakehouse and that the columns in each table exist.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    lakehouse : str, default=None
        The Fabric lakehouse used by the Direct Lake semantic model.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str, default=None
        The Fabric workspace used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace = fabric.resolve_workspace_name(workspace)

    if lakehouse_workspace is None:
        lakehouse_workspace = workspace

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, lakehouse_workspace)

    dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
    sqlEndpointId = get_direct_lake_sql_endpoint(dataset, workspace)
    dfI = fabric.list_items(workspace=lakehouse_workspace, type="SQLEndpoint")
    dfI_filt = dfI[(dfI["Id"] == sqlEndpointId)]

    if len(dfI_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The SQL Endpoint in the '{dataset}' semantic model in the '{workspace} workspace does not point to the "
            f"'{lakehouse}' lakehouse in the '{lakehouse_workspace}' workspace as specified."
        )

    if not any(r["Mode"] == "DirectLake" for i, r in dfP.iterrows()):
        raise ValueError(
            f"{icons.red_dot} The '{dataset}' semantic model is not in Direct Lake mode."
        )

    dfT = list_tables(dataset, workspace)
    dfC = fabric.list_columns(dataset=dataset, workspace=workspace)
    lc = get_lakehouse_columns(lakehouse, lakehouse_workspace)

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
            f"{icons.green_dot} All tables exist in the '{lakehouse}' lakehouse within the '{lakehouse_workspace}' workspace."
        )
    else:
        print(
            f"{icons.yellow_dot} The following tables exist in the '{dataset}' semantic model within the '{workspace}' workspace"
            f" but do not exist in the '{lakehouse}' lakehouse within the '{lakehouse_workspace}' workspace."
        )
        display(missingtbls)
    if len(missingcols) == 0:
        print(
            f"{icons.green_dot} All columns exist in the '{lakehouse}' lakehouse within the '{lakehouse_workspace}' workspace."
        )
    else:
        print(
            f"{icons.yellow_dot} The following columns exist in the '{dataset}' semantic model within the '{workspace}' workspace "
            f"but do not exist in the '{lakehouse}' lakehouse within the '{lakehouse_workspace}' workspace."
        )
        display(missingcols)
