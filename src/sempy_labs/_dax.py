import sempy.fabric as fabric
import pandas as pd
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    format_dax_object_name,
    resolve_dataset_name_and_id,
)
from sempy_labs._model_dependencies import get_model_calc_dependencies
from typing import Optional, List
from sempy._utils._log import log
from uuid import UUID
from sempy_labs.directlake._warm_cache import _put_columns_into_memory


@log
def evaluate_dax_impersonation(
    dataset: str | UUID,
    dax_query: str,
    user_name: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Runs a DAX query against a semantic model using the `REST API <https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries-in-group>`_.

    Compared to evaluate_dax this allows passing the user name for impersonation.
    Note that the REST API has significant limitations compared to the XMLA endpoint.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    dax_query : str
        The DAX query.
    user_name : str
        The user name (i.e. hello@goodbye.com).
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe holding the result of the DAX query.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    request_body = {
        "queries": [{"query": dax_query}],
        "impersonatedUserName": user_name,
    }

    client = fabric.PowerBIRestClient()
    response = client.post(
        f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries",
        json=request_body,
    )
    data = response.json()["results"][0]["tables"]
    column_names = data[0]["rows"][0].keys()
    data_rows = [row.values() for item in data for row in item["rows"]]
    df = pd.DataFrame(data_rows, columns=column_names)

    return df


@log
def get_dax_query_dependencies(
    dataset: str | UUID,
    dax_string: str | List[str],
    put_in_memory: bool = False,
    show_vertipaq_stats: bool = True,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Obtains the columns on which a DAX query depends, including model dependencies. Shows Vertipaq statistics (i.e. Total Size, Data Size, Dictionary Size, Hierarchy Size) for easy prioritizing.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    dax_string : str | List[str]
        The DAX query or list of DAX queries.
    put_in_memory : bool, default=False
        If True, ensures that the dependent columns are put into memory in order to give realistic Vertipaq stats (i.e. Total Size etc.).
    show_vertipaq_stats : bool, default=True
        If True, shows vertipaq stats (i.e. Total Size, Data Size, Dictionary Size, Hierarchy Size)
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the dependent columns of a given DAX query including model dependencies.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    if isinstance(dax_string, str):
        dax_string = [dax_string]

    final_df = pd.DataFrame(columns=["Object Type", "Table", "Object"])

    cd = get_model_calc_dependencies(dataset=dataset_id, workspace=workspace_id)

    for dax in dax_string:
        # Escape quotes in dax
        dax = dax.replace('"', '""')
        final_query = f"""
            EVALUATE
            VAR source_query = "{dax}"
            VAR all_dependencies = SELECTCOLUMNS(
                INFO.CALCDEPENDENCY("QUERY", source_query),
                    "Referenced Object Type",[REFERENCED_OBJECT_TYPE],
                    "Referenced Table", [REFERENCED_TABLE],
                    "Referenced Object", [REFERENCED_OBJECT]
                )             
            RETURN all_dependencies
            """
        dep = fabric.evaluate_dax(
            dataset=dataset_id, workspace=workspace_id, dax_string=final_query
        )

        # Clean up column names and values (remove outside square brackets, underscorees in object type)
        dep.columns = dep.columns.map(lambda x: x[1:-1])
        dep["Referenced Object Type"] = (
            dep["Referenced Object Type"].str.replace("_", " ").str.title()
        )

        # Dataframe df will contain the output of all dependencies of the objects used in the query
        df = dep.copy()

        for _, r in dep.iterrows():
            ot = r["Referenced Object Type"]
            object_name = r["Referenced Object"]
            table_name = r["Referenced Table"]
            cd_filt = cd[
                (cd["Object Type"] == ot)
                & (cd["Object Name"] == object_name)
                & (cd["Table Name"] == table_name)
            ]

            # Adds in the dependencies of each object used in the query (i.e. relationship etc.)
            if len(cd_filt) > 0:
                subset = cd_filt[
                    ["Referenced Object Type", "Referenced Table", "Referenced Object"]
                ]
                df = pd.concat([df, subset], ignore_index=True)

        df.columns = df.columns.map(lambda x: x.replace("Referenced ", ""))
        final_df = pd.concat([df, final_df], ignore_index=True)

    final_df = final_df[
        (final_df["Object Type"].isin(["Column", "Calc Column"]))
        & (~final_df["Object"].str.startswith("RowNumber-"))
    ]
    final_df = final_df.drop_duplicates().reset_index(drop=True)
    final_df = final_df.rename(columns={"Table": "Table Name", "Object": "Column Name"})
    final_df.drop(columns=["Object Type"], inplace=True)

    if not show_vertipaq_stats:
        return final_df

    # Get vertipaq stats, filter to just the objects in the df dataframe
    final_df["Full Object"] = format_dax_object_name(
        final_df["Table Name"], final_df["Column Name"]
    )
    dfC = fabric.list_columns(dataset=dataset_id, workspace=workspace_id, extended=True)
    dfC["Full Object"] = format_dax_object_name(dfC["Table Name"], dfC["Column Name"])

    dfC_filtered = dfC[dfC["Full Object"].isin(final_df["Full Object"].values)][
        [
            "Table Name",
            "Column Name",
            "Total Size",
            "Data Size",
            "Dictionary Size",
            "Hierarchy Size",
            "Is Resident",
            "Full Object",
        ]
    ].reset_index(drop=True)

    if put_in_memory:
        not_in_memory = dfC_filtered[dfC_filtered["Is Resident"] == False]

        if len(not_in_memory) > 0:
            _put_columns_into_memory(
                dataset=dataset,
                workspace=workspace,
                col_df=dfC_filtered,
                return_dataframe=False,
            )

            # Get column stats again
            dfC = fabric.list_columns(
                dataset=dataset_id, workspace=workspace_id, extended=True
            )
            dfC["Full Object"] = format_dax_object_name(
                dfC["Table Name"], dfC["Column Name"]
            )

            dfC_filtered = dfC[dfC["Full Object"].isin(final_df["Full Object"].values)][
                [
                    "Table Name",
                    "Column Name",
                    "Total Size",
                    "Data Size",
                    "Dictionary Size",
                    "Hierarchy Size",
                    "Is Resident",
                    "Full Object",
                ]
            ].reset_index(drop=True)

    dfC_filtered.drop(["Full Object"], axis=1, inplace=True)

    return dfC_filtered


@log
def get_dax_query_memory_size(
    dataset: str | UUID, dax_string: str, workspace: Optional[str | UUID] = None
) -> int:
    """
    Obtains the total size, in bytes, used by all columns that a DAX query depends on.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    dax_string : str
        The DAX query.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    int
        The total size, in bytes, used by all columns that the DAX query depends on.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    df = get_dax_query_dependencies(
        dataset=dataset_id,
        workspace=workspace_id,
        dax_string=dax_string,
        put_in_memory=True,
    )

    return df["Total Size"].sum()
