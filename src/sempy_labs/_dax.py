import sempy.fabric as fabric
import pandas as pd
from sempy_labs._helper_functions import (
    resolve_dataset_id,
    resolve_workspace_name_and_id,
    format_dax_object_name,
)
from sempy_labs._model_dependencies import get_model_calc_dependencies
from typing import Optional
from sempy._utils._log import log


@log
def evaluate_dax_impersonation(
    dataset: str,
    dax_query: str,
    user_name: Optional[str] = None,
    workspace: Optional[str] = None,
):
    """
    Runs a DAX query against a semantic model using the `REST API <https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries-in-group>`_.

    Compared to evaluate_dax this allows passing the user name for impersonation.
    Note that the REST API has significant limitations compared to the XMLA endpoint.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    dax_query : str
        The DAX query.
    user_name : str
        The user name (i.e. hello@goodbye.com).
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe holding the result of the DAX query.
    """

    # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries-in-group

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    dataset_id = resolve_dataset_id(dataset=dataset, workspace=workspace)

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
    dataset: str, dax_string: str, workspace: Optional[str] = None
):

    if workspace is None:
        workspace = fabric.resolve_workspace_name(workspace)

    # Escape quotes in dax
    dax_string = dax_string.replace('"', '""')
    final_query = f"""
        EVALUATE
        VAR source_query = "{dax_string}"
        VAR all_dependencies = SELECTCOLUMNS(
            INFO.CALCDEPENDENCY("QUERY", source_query), 
                "Referenced Object Type",[REFERENCED_OBJECT_TYPE], 
                "Referenced Table", [REFERENCED_TABLE],
                "Referenced Object", [REFERENCED_OBJECT]
            )             
        RETURN all_dependencies
        """
    dep = fabric.evaluate_dax(
        dataset=dataset, workspace=workspace, dax_string=final_query
    )

    # Clean up column names and values (remove outside square brackets, underscorees in object type)
    dep.columns = dep.columns.map(lambda x: x[1:-1])
    dep["Referenced Object Type"] = (
        dep["Referenced Object Type"].str.replace("_", " ").str.title()
    )
    dep

    # Dataframe df will contain the output of all dependencies of the objects used in the query
    df = dep.copy()

    cd = get_model_calc_dependencies(dataset=dataset, workspace=workspace)

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
    # Remove duplicates
    df = df.drop_duplicates().reset_index(drop=True)
    # Only show columns and remove the rownumber column
    df = df[
        (df["Object Type"].isin(["Column", "Calc Column"]))
        & (~df["Object"].str.startswith("RowNumber-"))
    ]

    # Get vertipaq stats, filter to just the objects in the df dataframe
    df["Full Object"] = format_dax_object_name(df["Table"], df["Object"])
    dfC = fabric.list_columns(dataset=dataset, workspace=workspace, extended=True)
    dfC["Full Object"] = format_dax_object_name(
        dfC["Table Name"], dfC["Column Name"]
    )
    dfC_filtered = dfC[dfC["Full Object"].isin(df["Full Object"].values)][
        [
            "Table Name",
            "Column Name",
            "Column Cardinality",
            "Total Size",
            "Data Size",
            "Dictionary Size",
            "Hierarchy Size",
        ]
    ].reset_index(drop=True)

    return dfC_filtered


@log
def get_dax_query_memory_size(
    dataset: str, dax_string: str, workspace: Optional[str] = None
) -> int:

    if workspace is None:
        workspace = fabric.resolve_workspace_name(workspace)

    df = get_dax_query_dependencies(
        dataset=dataset,
        workspace=workspace,
        dax_string=dax_string,
    )

    return df["Total Size"].sum()
