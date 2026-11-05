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

# SESSION_STATUS values (DISCOVER_SESSIONS): 0=Idle, 1=Active, 2=Blocked, 3=Cancelled.
_RUNNING_SESSION_STATUSES = [1, 2]
# Expanded (namespace-qualified) XML element names of refresh/processing commands.
_REFRESH_ELEMENT_TAGS = {
    "{http://schemas.microsoft.com/analysisservices/2014/engine}Refresh",
    "{http://schemas.microsoft.com/analysisservices/2003/engine}Process",
}


def _local_name(tag) -> str:
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


def _refresh_database_ids(text: str) -> Optional[set]:
    """
    Returns the (lower-cased) database identifiers targeted by a refresh command, or None if the
    command text is not a refresh. Supports XMLA (Refresh/Process elements, identified by their
    expanded names so attribute order or Batch attributes do not matter) and TMSL (JSON) refreshes.
    """
    import json
    import xml.etree.ElementTree as ET

    text = (text or "").strip()
    if not text:
        return None

    if text.startswith("{"):
        try:
            command = json.loads(text)
        except ValueError:
            return None
        if not isinstance(command, dict):
            return None
        refreshes = [command.get("refresh")]
        sequence = command.get("sequence")
        if isinstance(sequence, dict):
            refreshes += [
                op.get("refresh")
                for op in sequence.get("operations", [])
                if isinstance(op, dict)
            ]
        refreshes = [r for r in refreshes if isinstance(r, dict)]
        if not refreshes:
            return None
        return {
            str(obj["database"]).lower()
            for r in refreshes
            for obj in r.get("objects", [])
            if isinstance(obj, dict) and obj.get("database")
        }

    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return None
    refresh_elements = [e for e in root.iter() if e.tag in _REFRESH_ELEMENT_TAGS]
    if not refresh_elements:
        return None
    return {
        child.text.strip().lower()
        for element in refresh_elements
        for child in element.iter()
        if _local_name(child.tag) == "DatabaseID" and child.text
    }


def _is_refresh_command(command_text: pd.Series) -> pd.Series:

    return (
        command_text.fillna("")
        .astype(str)
        .map(lambda text: _refresh_database_ids(text) is not None)
        .astype(bool)
    )


def _is_blank(values: pd.Series) -> pd.Series:

    return values.isna() | (values.astype(str).str.strip().isin(["", "NaT", "None"]))


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

    DISCOVER_COMMANDS returns the currently executing *or last executed* command per session, so the
    running state is derived from the session metadata: the session must be Active or Blocked and its
    last command must not have ended (SESSION_LAST_COMMAND_END_TIME is empty or precedes
    SESSION_LAST_COMMAND_START_TIME). COMMAND_END_TIME is optional in the DISCOVER_COMMANDS rowset and
    is only used when the server returns it.
    """

    sessions = list_semantic_model_sessions(dataset=dataset, workspace=workspace)
    commands = _discover_commands(dataset=dataset, workspace=workspace)

    if (
        sessions.empty
        or commands.empty
        or "SESSION_SPID" not in sessions.columns
        or "SESSION_SPID" not in commands.columns
    ):
        return commands.iloc[0:0]

    session_cols = [
        c
        for c in [
            "SESSION_SPID",
            "SESSION_USER_NAME",
            "SESSION_STATUS",
            "SESSION_LAST_COMMAND_START_TIME",
            "SESSION_LAST_COMMAND_END_TIME",
        ]
        if c in sessions.columns and (c == "SESSION_SPID" or c not in commands.columns)
    ]
    df = commands.merge(sessions[session_cols], on="SESSION_SPID", how="inner")

    if "SESSION_STATUS" in df.columns:
        status = pd.to_numeric(df["SESSION_STATUS"], errors="coerce")
        df = df[status.isin(_RUNNING_SESSION_STATUSES)]

    if {"SESSION_LAST_COMMAND_START_TIME", "SESSION_LAST_COMMAND_END_TIME"}.issubset(
        df.columns
    ):
        start = pd.to_datetime(
            df["SESSION_LAST_COMMAND_START_TIME"], errors="coerce", utc=True
        )
        end = pd.to_datetime(
            df["SESSION_LAST_COMMAND_END_TIME"], errors="coerce", utc=True
        )
        df = df[_is_blank(df["SESSION_LAST_COMMAND_END_TIME"]) | (end < start)]

    if "COMMAND_END_TIME" in df.columns:
        df = df[_is_blank(df["COMMAND_END_TIME"])]

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
    dataset_name, dataset_id = resolve_dataset_name_and_id(dataset, workspace_id)
    model_identifiers = {str(dataset_name).lower(), str(dataset_id).lower()}

    def targets_model(text) -> bool:
        database_ids = _refresh_database_ids(text if isinstance(text, str) else "")
        # Sessions are already scoped to this model, so a refresh without an explicit database targets it.
        return database_ids is not None and (
            not database_ids or bool(database_ids & model_identifiers)
        )

    df = _list_running_commands(dataset=dataset_id, workspace=workspace_id)
    # Find refresh operations
    df_filt = (
        df[df["COMMAND_TEXT"].map(targets_model).astype(bool)] if not df.empty else df
    )
    if not df_filt.empty:
        spids = df_filt["SESSION_SPID"].drop_duplicates().tolist()
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
    if df.empty:
        print(f"{icons.info} No long-running queries found to cancel.")
        return

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
