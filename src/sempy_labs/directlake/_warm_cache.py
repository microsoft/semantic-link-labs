import sempy.fabric as fabric
import pandas as pd
from tqdm.auto import tqdm
import numpy as np
import time
from .._helper_functions import (
    format_dax_object_name,
    resolve_dataset_name_and_id,
    resolve_workspace_name_and_id,
)
from .._refresh_semantic_model import refresh_semantic_model
from .._model_dependencies import get_measure_dependencies
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
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    perspective : str
        Name of the perspective which contains objects to be used for warming the cache.
    add_dependencies : bool, default=False
        Includes object dependencies in the cache warming process.
    workspace : str | uuid.UUID, default=None
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

    fabric.refresh_tom_cache(workspace=workspace)

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

    return _put_columns_into_memory(dataset=dataset, workspace=workspace, col_df=df)


@log
def warm_direct_lake_cache_isresident(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Performs a refresh on the semantic model and puts the columns which were in memory prior to the refresh back into memory.

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

    return _put_columns_into_memory(
        dataset=dataset, workspace=workspace, col_df=dfC_filtered
    )


@log
def _put_columns_into_memory(dataset, workspace, col_df, return_dataframe: bool = True):

    row_limit = 1000000

    dfT = fabric.list_tables(dataset=dataset, workspace=workspace, extended=True)
    col_df = col_df.copy()

    col_df["DAX Object"] = format_dax_object_name(
        col_df["Table Name"], col_df["Column Name"]
    )
    tbls = col_df["Table Name"].unique()

    for table_name in (bar := tqdm(tbls)):
        dfT_filt = dfT[dfT["Name"] == table_name]
        col_df_filt = col_df[col_df["Table Name"] == table_name]
        if not dfT_filt.empty:
            row_count = dfT_filt["Row Count"].iloc[0]
            bar.set_description(f"Warming the '{table_name}' table...")
            if pd.isna(row_count):
                pass
            elif row_count < row_limit:
                columns = col_df_filt["DAX Object"].tolist()
                css = ", ".join(columns)
                dax = f"EVALUATE TOPN(1, SELECTCOLUMNS('{table_name}', {css}))"
                fabric.evaluate_dax(
                    dataset=dataset, dax_string=dax, workspace=workspace
                )
            else:
                for _, r in col_df_filt.iterrows():
                    dax_object = r["DAX Object"]
                    dax = f"""EVALUATE TOPN(1, SELECTCOLUMNS('{table_name}', {dax_object}))"""
                    fabric.evaluate_dax(
                        dataset=dataset, dax_string=dax, workspace=workspace
                    )

    if return_dataframe:
        print(
            f"{icons.green_dot} The following columns have been put into memory. Temperature indicates the current column temperature."
        )

        dfC = fabric.list_columns(dataset=dataset, workspace=workspace, extended=True)
        dfC["DAX Object"] = format_dax_object_name(
            dfC["Table Name"], dfC["Column Name"]
        )
        dfC_filt = dfC[dfC["DAX Object"].isin(col_df["DAX Object"].values)]

        return (
            dfC_filt[["Table Name", "Column Name", "Is Resident", "Temperature"]]
            .sort_values(by=["Table Name", "Column Name"], ascending=True)
            .reset_index(drop=True)
        )
