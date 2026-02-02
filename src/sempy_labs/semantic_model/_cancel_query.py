import sempy.fabric as fabric
import pandas as pd
from typing import Optional, List
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_item_id,
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
)
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def cancel_spid(dataset: str | UUID, spid: int, workspace: Optional[str | UUID] = None):
    """
    Cancels a given SPID (query) against a semantic model.
    See `here <https://learn.microsoft.com/analysis-services/xmla/xml-elements-commands/cancel-element-xmla?view=asallproducts-allversions>`_ for documentation.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    spid : int
        The SPID of the query to cancel. Can be found using the 'list_running_queries' function.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    xmla = f"""
            <Cancel xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">
                <SPID>{spid}</SPID>
            </Cancel>
            """
    fabric.execute_xmla(xmla_command=xmla, dataset=dataset, workspace=workspace)
    print(
        f"{icons.green_dot} The query with SPID '{spid}' has been cancelled in the '{dataset_name}' semantic model within the the '{workspace_name}' workspace."
    )


@log
def list_semantic_model_sessions(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    user_name: Optional[str | List[str]] = None,
    spid: Optional[int] = None,
) -> pd.DataFrame:
    """
    Lists the active sessions against a semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    user_name : str | List[str], default=None
        The user name or list of user names to filter by.
    spid : int, default=None
        The SPID of the query to filter by.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the active sessions against a semantic model.
    """

    if user_name and isinstance(user_name, str):
        user_name = [user_name]

    sessions = fabric.evaluate_dax(
        dataset=dataset,
        workspace=workspace,
        dax_string="""select * from $system.discover_sessions""",
    )
    if user_name:
        sessions = sessions[sessions["SESSION_USER_NAME"].isin(user_name)]
    if spid:
        sessions = sessions[sessions["SESSION_SPID"] == spid]
    return sessions


@log
def list_semantic_model_commands(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    min_elapsed_time_seconds: Optional[int] = None,
    spid: Optional[int] = None,
) -> pd.DataFrame:
    """
    Lists the active commands against a semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    min_elapsed_time_seconds : int, default=None
        The minimum elapsed time in seconds to filter by.
    spid : int, default=None
        The SPID of the query to filter by.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the active commands against a semantic model.
    """

    commands = fabric.evaluate_dax(
        dataset=dataset,
        workspace=workspace,
        dax_string="""select * from $system.discover_commands""",
    )

    if min_elapsed_time_seconds:
        commands = commands[
            commands["COMMAND_ELAPSED_TIME_MS"] >= min_elapsed_time_seconds * 1000
        ]
    if spid:
        commands = commands[commands["SESSION_SPID"] == spid]
    return commands


@log
def _cancel_refresh(dataset: str | UUID, workspace: Optional[str | UUID] = None):

    workspace_id = resolve_workspace_id(workspace)
    dataset_id = resolve_item_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )

    df = list_semantic_model_commands(dataset=dataset_id, workspace=workspace_id)
    # Find refresh operations
    df_filt = df[
        (
            df["COMMAND_TEXT"].str.contains(
                '<Batch Transaction="false" xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">'
            )
        )
        & (
            df["COMMAND_TEXT"].str.contains(
                '<Refresh xmlns="http://schemas.microsoft.com/analysisservices/2014/engine">'
            )
        )
        & (df["COMMAND_TEXT"].str.contains(f"<DatabaseID>{dataset_id}</DatabaseID>"))
    ]
    if df_filt:
        spids = df_filt["SESSION_SPID"].values.tolist()
        for spid in spids:
            cancel_spid(dataset=dataset_id, spid=spid, workspace=workspace_id)
    else:
        print(f"{icons.info} No refresh operations found to cancel.")


@log
def cancel_long_running_queries(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    min_elapsed_time_seconds: int = 60,
    skip_users: str | List[str] = None,
    include_refresh_operations: bool = False,
):
    """
    Cancels long-running queries against a semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    min_elapsed_time_seconds : int, default=60
        The minimum elapsed time in seconds which defines a long-running query.
    skip_users : str | List[str], default=None
        The user name or list of user names to skip when cancelling long-running queries.
    include_refresh_operations : bool, default=False
        If True, includes refresh operations when cancelling long-running queries.
    """

    workspace_id = resolve_workspace_id(workspace)
    dataset_id = resolve_item_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )

    df = list_semantic_model_sessions(dataset=dataset_id, workspace=workspace_id)
    # Filter out refresh operations unless specified to include
    if not include_refresh_operations:
        df = df[
            ~(
                (
                    df["COMMAND_TEXT"].str.contains(
                        '<Batch Transaction="false" xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">'
                    )
                )
                & (
                    df["COMMAND_TEXT"].str.contains(
                        '<Refresh xmlns="http://schemas.microsoft.com/analysisservices/2014/engine">'
                    )
                )
            )
        ]

    if skip_users and isinstance(skip_users, str):
        skip_users = [skip_users]
    if skip_users:
        df = df[~df["SESSION_USER_NAME"].isin(skip_users)]
    df = df[df["SESSION_ELAPSED_TIME_MS"] >= min_elapsed_time_seconds * 1000]
    spids = df["SESSION_SPID"].values.tolist()

    for spid in spids:
        cancel_spid(dataset=dataset_id, spid=spid, workspace=workspace_id)
