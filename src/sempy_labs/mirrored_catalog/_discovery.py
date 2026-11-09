from typing import Optional, List
from urllib.parse import urlencode
import pandas as pd
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    resolve_workspace_id,
    _update_dataframe_datatypes,
)
from sempy_labs.connection._items import resolve_connection_id
from uuid import UUID
from sempy._utils._log import log


@log
def list_scopes(
    connection: str | UUID,
    parent: Optional[str | List[str]] = None,
    recursive: bool = False,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns a list of namespaces (scopes) from the source catalog. This is used to browse and select the scope when creating or updating a mirrored catalog.

    When listing from the root level, the virtual '(root)' entry returned by the API is omitted and its children are returned instead.
    When recursive is True, nested child namespaces are flattened into the dataframe.

    This is a wrapper function for the following API: `Discovery - List Scopes <https://learn.microsoft.com/rest/api/fabric/mirroredcatalog/discovery/list-scopes(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    connection : str | uuid.UUID
        The name or ID of the connection to the catalog.
    parent : str | List[str], default=None
        The parent namespace segments in hierarchical order (root-level segment first, leaf segment last).
        Defaults to None which returns the root namespaces.
    recursive : bool, default=False
        If True, returns nested child namespaces.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the namespaces (scopes) from the source catalog.
    """

    workspace_id = resolve_workspace_id(workspace)
    connection_id = resolve_connection_id(connection)

    if isinstance(parent, str):
        parent = [parent]

    params = [("beta", "True"), ("connectionId", str(connection_id))]
    params.extend(("parent", p) for p in (parent or []))
    if recursive:
        params.append(("recursive", "True"))

    columns = {
        "Scope Name": "str",
        "Fully Qualified Scope": "list",
        "Is Selectable": "bool",
        "Parent Scope": "list",
        "Level": "int",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/catalogmirroring/scopes?{urlencode(params)}",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []

    def _add_scope(scope: dict):
        fq_scope = scope.get("fullyQualifiedScope") or []
        # Skip the virtual '(root)' wrapper returned when no parent is specified
        if fq_scope:
            rows.append(
                {
                    "Scope Name": scope.get("name"),
                    "Fully Qualified Scope": fq_scope,
                    "Is Selectable": scope.get("isSelectable") == "Selectable",
                    "Parent Scope": fq_scope[:-1],
                    "Level": len(fq_scope),
                }
            )
        for child in scope.get("children") or []:
            _add_scope(child)

    for r in responses:
        for scope in r.get("value", []):
            _add_scope(scope)

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def list_tables(
    connection: str | UUID,
    scope: Optional[str | List[str]] = None,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns a list of tables within a specified namespace scope from the source catalog. This is used to browse tables when configuring partial mirroring with table-level selection.

    This is a wrapper function for the following API: `Discovery - List Tables <https://learn.microsoft.com/rest/api/fabric/mirroredcatalog/discovery/list-tables(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    connection : str | uuid.UUID
        The name or ID of the connection to the catalog.
    scope : str | List[str], default=None
        The selectable namespace scope segments in hierarchical order (root-level segment first).
        Defaults to None which resolves to the root namespace.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the tables within the specified namespace scope.
    """

    workspace_id = resolve_workspace_id(workspace)
    connection_id = resolve_connection_id(connection)

    if isinstance(scope, str):
        scope = [scope]

    params = [("beta", "True"), ("connectionId", str(connection_id))]
    # The API requires an empty 'scope=' parameter for the root namespace
    params.extend(("scope", s) for s in (scope or [""]))

    columns = {
        "Table Name": "str",
        "Schema Name": "str",
        "Fully Qualified Scope": "list",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/catalogmirroring/tables?{urlencode(params)}",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for table in r.get("value", []):
            fq_scope = table.get("fullyQualifiedScope") or []
            rows.append(
                {
                    "Table Name": table.get("name"),
                    "Schema Name": fq_scope[-1] if fq_scope else None,
                    "Fully Qualified Scope": fq_scope,
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df
