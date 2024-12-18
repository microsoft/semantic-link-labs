import sempy.fabric as fabric
import pandas as pd
from tqdm.auto import tqdm
import numpy as np
import time
from sempy_labs._helper_functions import (
    format_dax_object_name,
    resolve_dataset_name_and_id,
    resolve_workspace_name_and_id,
)
from sempy_labs._refresh_semantic_model import refresh_semantic_model
from sempy_labs._model_dependencies import get_measure_dependencies
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from uuid import UUID


@log
def warm_direct_lake_cache_perspective(
    dataset: str | UUID,
    perspective: str,
    add_dependencies: bool = False,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Warms the cache of a Direct Lake semantic model by running a simple DAX query against the columns in a perspective.

    Parameters
    ----------
    dataset : str | UUID
        Name or ID of the semantic model.
    perspective : str
        Name of the perspective which contains objects to be used for warming the cache.
    add_dependencies : bool, default=False
        Includes object dependencies in the cache warming process.
    workspace : str | UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Returns a pandas dataframe showing the columns that have been put into memory.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    dfP = fabric.list_partitions(dataset=dataset_id, workspace=workspace_id)
    if not any(r["Mode"] == "DirectLake" for _, r in dfP.iterrows()):
        raise ValueError(
            f"{icons.red_dot} The '{dataset_name}' semantic model in the '{workspace_name}' workspace is not in Direct Lake mode. This function is specifically for semantic models in Direct Lake mode."
        )

    dfPersp = fabric.list_perspectives(dataset=dataset_id, workspace=workspace_id)
    dfPersp["DAX Object Name"] = format_dax_object_name(
        dfPersp["Table Name"], dfPersp["Object Name"]
    )
    dfPersp_filt = dfPersp[dfPersp["Perspective Name"] == perspective]

    if len(dfPersp_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{perspective} perspective does not exist or contains no objects within the '{dataset}' semantic model in the '{workspace}' workspace."
        )

    dfPersp_c = dfPersp_filt[dfPersp_filt["Object Type"] == "Column"]

    column_values = dfPersp_c["DAX Object Name"].tolist()

    if add_dependencies:
        # Measure dependencies
        md = get_measure_dependencies(dataset_id, workspace_id)
        md["Referenced Full Object"] = format_dax_object_name(
            md["Referenced Table"], md["Referenced Object"]
        )
        dfPersp_m = dfPersp_filt[(dfPersp_filt["Object Type"] == "Measure")]
        md_filt = md[
            (md["Object Name"].isin(dfPersp_m["Object Name"].values))
            & (md["Referenced Object Type"] == "Column")
        ]
        measureDep = md_filt["Referenced Full Object"].unique()

        # Hierarchy dependencies
        dfPersp_h = dfPersp_filt[(dfPersp_filt["Object Type"] == "Hierarchy")]
        dfH = fabric.list_hierarchies(dataset=dataset_id, workspace=workspace_id)
        dfH["Hierarchy Object"] = format_dax_object_name(
            dfH["Table Name"], dfH["Hierarchy Name"]
        )
        dfH["Column Object"] = format_dax_object_name(
            dfH["Table Name"], dfH["Column Name"]
        )
        dfH_filt = dfH[
            dfH["Hierarchy Object"].isin(dfPersp_h["DAX Object Name"].values)
        ]
        hierarchyDep = dfH_filt["Column Object"].unique()

        # Relationship dependencies
        unique_table_names = dfPersp_filt["Table Name"].unique()
        dfR = fabric.list_relationships(dataset=dataset_id, workspace=workspace_id)
        dfR["From Object"] = format_dax_object_name(
            dfR["From Table"], dfR["From Column"]
        )
        dfR["To Object"] = format_dax_object_name(dfR["To Table"], dfR["To Column"])
        filtered_dfR = dfR[
            dfR["From Table"].isin(unique_table_names)
            & dfR["To Table"].isin(unique_table_names)
        ]

        fromObjects = filtered_dfR["From Object"].unique()
        toObjects = filtered_dfR["To Object"].unique()

        merged_list = np.concatenate(
            [column_values, measureDep, hierarchyDep, fromObjects, toObjects]
        )
        merged_list_unique = list(set(merged_list))

    else:
        merged_list_unique = column_values

    df = pd.DataFrame(merged_list_unique, columns=["DAX Object Name"])
    df[["Table Name", "Column Name"]] = df["DAX Object Name"].str.split(
        "[", expand=True
    )
    df["Table Name"] = df["Table Name"].str[1:-1]
    df["Column Name"] = df["Column Name"].str[0:-1]

    tbls = list(set(value.split("[")[0] for value in merged_list_unique))

    for tableName in (bar := tqdm(tbls)):
        filtered_list = [
            value for value in merged_list_unique if value.startswith(f"{tableName}[")
        ]
        bar.set_description(f"Warming the '{tableName}' table...")
        css = ",".join(map(str, filtered_list))
        dax = """EVALUATE TOPN(1,SUMMARIZECOLUMNS(""" + css + "))" ""
        fabric.evaluate_dax(dataset=dataset_id, dax_string=dax, workspace=workspace_id)

    print(f"{icons.green_dot} The following columns have been put into memory:")

    new_column_order = ["Table Name", "Column Name", "DAX Object Name"]
    df = df.reindex(columns=new_column_order)
    df = df[["Table Name", "Column Name"]].sort_values(
        by=["Table Name", "Column Name"], ascending=True
    )

    return df


@log
def warm_direct_lake_cache_isresident(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Performs a refresh on the semantic model and puts the columns which were in memory prior to the refresh back into memory.

    Parameters
    ----------
    dataset : str | UUID
        Name or ID of the semantic model.
    workspace : str | UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Returns a pandas dataframe showing the columns that have been put into memory.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    dfP = fabric.list_partitions(dataset=dataset_id, workspace=workspace_id)
    if not any(r["Mode"] == "DirectLake" for _, r in dfP.iterrows()):
        raise ValueError(
            f"{icons.red_dot} The '{dataset_name}' semantic model in the '{workspace_name}' workspace is not in Direct Lake mode. This function is specifically for semantic models in Direct Lake mode."
        )

    # Identify columns which are currently in memory (Is Resident = True)
    dfC = fabric.list_columns(dataset=dataset_id, workspace=workspace_id, extended=True)
    dfC["DAX Object Name"] = format_dax_object_name(
        dfC["Table Name"], dfC["Column Name"]
    )
    dfC_filtered = dfC[dfC["Is Resident"] == True]

    if len(dfC_filtered) == 0:
        raise ValueError(
            f"{icons.yellow_dot} At present, no columns are in memory in the '{dataset_name}' semantic model in the '{workspace_name}' workspace."
        )

    # Refresh/frame dataset
    refresh_semantic_model(
        dataset=dataset_id, refresh_type="full", workspace=workspace_id
    )
    time.sleep(2)

    # Run basic query to get columns into memory; completed one table at a time (so as not to overload the capacity)
    tbls = dfC_filtered["Table Name"].unique()
    for table_name in (bar := tqdm(tbls)):
        bar.set_description(f"Warming the '{table_name}' table...")
        css = ", ".join(
            dfC_filtered[dfC_filtered["Table Name"] == table_name]["DAX Object Name"]
            .astype(str)
            .tolist()
        )
        dax = f"""EVALUATE TOPN(1,SUMMARIZECOLUMNS({css}))"""
        fabric.evaluate_dax(dataset=dataset_id, dax_string=dax, workspace=workspace_id)

    print(
        f"{icons.green_dot} The following columns have been put into memory. Temperature indicates the column temperature prior to the semantic model refresh."
    )

    return dfC_filtered[
        ["Table Name", "Column Name", "Is Resident", "Temperature"]
    ].sort_values(by=["Table Name", "Column Name"], ascending=True)
