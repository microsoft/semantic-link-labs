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

_REFRESH_BATCH_TEXT = '<Batch Transaction="false" xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">'
_REFRESH_TEXT = (
    '<Refresh xmlns="http://schemas.microsoft.com/analysisservices/2014/engine">'
)
# SESSION_STATUS values (DISCOVER_SESSIONS): 0=Idle, 1=Active, 2=Blocked, 3=Cancelled.
_RUNNING_SESSION_STATUSES = [1, 2]


def _is_refresh_command(command_text: pd.Series) -> pd.Series:

    text = command_text.fillna("").astype(str)
    return text.str.contains(_REFRESH_BATCH_TEXT, regex=False) & text.str.contains(
        _REFRESH_TEXT, regex=False
    )


def _discover_commands(dataset: str | UUID, workspace: str | UUID) -> pd.DataFrame:

    return fabric.evaluate_dax(
        dataset=dataset,
        workspace=workspace,
        dax_string="""select * from $system.discover_commands""",
    )


def _list_running_commands(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Returns the currently executing commands against the semantic model, joined to their session metadata.

    DISCOVER_COMMANDS returns the currently executing *or last executed* command per session, so
    rows for completed commands (non-empty COMMAND_END_TIME) and idle/cancelled sessions are removed.
    """

    sessions = list_semantic_model_sessions(dataset=dataset, workspace=workspace)
    commands = _discover_commands(dataset=dataset, workspace=workspace)

    session_cols = [
        c
        for c in ["SESSION_SPID", "SESSION_USER_NAME", "SESSION_STATUS"]
        if c in sessions.columns
    ]
    df = commands.merge(sessions[session_cols], on="SESSION_SPID", how="inner")

    end_time = df["COMMAND_END_TIME"]
    df = df[end_time.isna() | (end_time.astype(str).str.strip() == "")]
    if "SESSION_STATUS" in df.columns:
        status = pd.to_numeric(df["SESSION_STATUS"], errors="coerce")
        df = df[status.isin(_RUNNING_SESSION_STATUSES)]

    return df


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
        The SPID of the query to cancel. Can be found using the 'list_semantic_model_commands' function.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """
    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    dataset_name, dataset_id = resolve_dataset_name_and_id(dataset, workspace_id)

    xmla = f"""
            <Cancel xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">
                <SPID>{spid}</SPID>
            </Cancel>
            """
    fabric.execute_xmla(xmla_command=xmla, dataset=dataset, workspace=workspace)
    print(
        f"{icons.green_dot} The query with SPID '{spid}' has been cancelled in the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
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

    Only sessions whose current database (SESSION_CURRENT_DATABASE) is the specified semantic model are returned.

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

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    dataset_name, dataset_id = resolve_dataset_name_and_id(dataset, workspace_id)

    sessions = fabric.evaluate_dax(
        dataset=dataset_id,
        workspace=workspace_id,
        dax_string="""select * from $system.discover_sessions""",
    )

    # DISCOVER_SESSIONS is not restricted to a database, so keep only sessions on this model.
    model_identifiers = {str(dataset_name).lower(), str(dataset_id).lower()}
    current_db = sessions["SESSION_CURRENT_DATABASE"].fillna("").astype(str)
    sessions = sessions[current_db.str.strip().str.lower().isin(model_identifiers)]
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

    Only commands belonging to sessions connected to the specified semantic model are returned.
    Note that each row shows the currently executing or last executed command of the session.

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

    workspace_id = resolve_workspace_id(workspace)
    dataset_name, dataset_id = resolve_dataset_name_and_id(dataset, workspace_id)

    sessions = list_semantic_model_sessions(dataset=dataset_id, workspace=workspace_id)
    commands = _discover_commands(dataset=dataset_id, workspace=workspace_id)
    commands = commands[commands["SESSION_SPID"].isin(sessions["SESSION_SPID"])]

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

    df = _list_running_commands(dataset=dataset_id, workspace=workspace_id)
    # Find refresh operations
    df_filt = df[
        _is_refresh_command(df["COMMAND_TEXT"])
        & (
            df["COMMAND_TEXT"]
            .fillna("")
            .astype(str)
            .str.contains(f"<DatabaseID>{dataset_id}</DatabaseID>", regex=False)
        )
    ]
    if not df_filt.empty:
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
    skip_users: Optional[str | List[str]] = None,
    include_refresh_operations: bool = False,
):
    """
    Cancels long-running queries against a semantic model.

    Only commands which are currently executing against the specified semantic model and have been running
    for at least the specified duration (based on the command's elapsed time, not the session's) are cancelled.

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

    df = _list_running_commands(dataset=dataset_id, workspace=workspace_id)
    # Filter out refresh operations unless specified to include
    if not include_refresh_operations:
        df = df[~_is_refresh_command(df["COMMAND_TEXT"])]

    if skip_users and isinstance(skip_users, str):
        skip_users = [skip_users]
    if skip_users:
        df = df[~df["SESSION_USER_NAME"].isin(skip_users)]
    elapsed_ms = pd.to_numeric(df["COMMAND_ELAPSED_TIME_MS"], errors="coerce")
    df = df[elapsed_ms >= min_elapsed_time_seconds * 1000]
    spids = df["SESSION_SPID"].drop_duplicates().tolist()

    if not spids:
        print(f"{icons.info} No long-running queries found to cancel.")
        return

    for spid in spids:
        cancel_spid(dataset=dataset_id, spid=spid, workspace=workspace_id)
