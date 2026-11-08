import re
from urllib.parse import unquote
from uuid import UUID
from typing import Iterator, List, Optional, Tuple
import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException
from sempy_labs._helper_functions import (
    _base_api,
    _is_valid_uuid,
    resolve_dataset_id,
    resolve_workspace_id,
)
import sempy_labs._icons as icons
from sempy._utils._log import log

# powerbi://api.powerbi.com/v1.0/myorg/<workspace> (XMLA endpoint) or
# pbiazure://api.powerbi.com (Power BI semantic model connection).
_PBI_SERVER_PATTERN = re.compile(
    r"^\s*(?:powerbi|pbiazure)://[^/]+(?:/v1\.0/[^/]+/(?P<workspace>[^/]+?))?/*\s*$",
    re.IGNORECASE,
)
_GUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE
)
# M strings escape double quotes by doubling them.
_AS_DATABASE_PATTERN = re.compile(
    r'AnalysisServices\.Database\s*\(\s*"((?:[^"]|"")*)"\s*,\s*"((?:[^"]|"")*)"',
    re.IGNORECASE,
)
_CONN_STRING_FIELD = r"{}\s*=\s*\"?([^;\"]*)"


def _parse_power_bi_source(
    server: Optional[str], database: Optional[str]
) -> Optional[Tuple[Optional[str], str]]:
    """
    Returns (workspace, database) when the source is a Power BI semantic model,
    otherwise None. The database is reduced to the model ID when the source
    uses the virtual server form (sobe_wowvirtualserver-<id>).
    """

    if not server or not database:
        return None
    match = _PBI_SERVER_PATTERN.match(server)
    if match is None:
        return None
    workspace = match.group("workspace")
    workspace = unquote(workspace).strip() if workspace else None
    database = database.strip()
    guid = _GUID_PATTERN.search(database)
    if guid is not None and not _is_valid_uuid(database):
        database = guid.group(0)

    return workspace or None, database


def _resolve_power_bi_source(
    server: str, database: str
) -> Optional[Tuple[str, Optional[str]]]:
    """
    Resolves a Power BI semantic model data source to (dataset_id, workspace_id).
    """

    parsed = _parse_power_bi_source(server, database)
    if parsed is None:
        return None
    workspace, database = parsed

    workspace_id = None
    if workspace is not None:
        try:
            workspace_id = str(resolve_workspace_id(workspace))
        except Exception:
            if not _is_valid_uuid(database):
                raise

    if _is_valid_uuid(database):
        return database.lower(), workspace_id

    if workspace_id is None:
        return None

    return (
        str(resolve_dataset_id(dataset=database, workspace=workspace_id)).lower(),
        workspace_id,
    )


def _sources_from_datasources(dataset_id: str) -> List[Tuple[str, str]]:

    response = _base_api(
        request=f"/v1.0/myorg/datasets/{dataset_id}/datasources", client="fabric_sp"
    )
    sources = []
    for ds in response.json().get("value", []):
        if str(ds.get("datasourceType", "")).lower() != "analysisservices":
            continue
        details = ds.get("connectionDetails") or {}
        server = details.get("server")
        database = details.get("database")
        conn_str = ds.get("connectionString") or ""
        if not server and conn_str:
            m = re.search(_CONN_STRING_FIELD.format("Data\\s*Source"), conn_str, re.I)
            server = m.group(1) if m else None
        if not database and conn_str:
            m = re.search(
                _CONN_STRING_FIELD.format("Initial\\s*Catalog"), conn_str, re.I
            )
            database = m.group(1) if m else None
        if server and database:
            sources.append((server, database))

    return sources


def _sources_from_expressions(
    dataset_id: str, workspace_id: str
) -> List[Tuple[str, str]]:

    df = fabric.list_expressions(dataset=dataset_id, workspace=workspace_id)
    sources = []
    for expression in df.get("Expression", []):
        for server, database in _AS_DATABASE_PATTERN.findall(str(expression or "")):
            sources.append((server.replace('""', '"'), database.replace('""', '"')))

    return sources


def _iter_power_bi_sources(
    dataset_id: str, workspace_id: Optional[str]
) -> Iterator[Tuple[str, str]]:
    """
    Yields the (server, database) pairs of the Power BI semantic models a
    semantic model connects to via DirectQuery. Uses the Get Datasources API
    and falls back to reading the model's M expressions (requires XMLA).
    """

    found = False
    try:
        for server, database in _sources_from_datasources(dataset_id):
            if _parse_power_bi_source(server, database) is not None:
                found = True
                yield server, database
    except Exception:
        pass

    if found or workspace_id is None:
        return

    try:
        for server, database in _sources_from_expressions(dataset_id, workspace_id):
            if _parse_power_bi_source(server, database) is not None:
                yield server, database
    except Exception:
        pass


def _get_upstream_dataset_ids(
    dataset_ids: List[str], workspace: Optional[str | UUID] = None
) -> List[str]:
    """
    Returns the IDs of all semantic models that the given semantic models
    (transitively) connect to via DirectQuery to Power BI semantic models.
    """

    workspace_id = None
    if workspace is not None:
        try:
            workspace_id = str(resolve_workspace_id(workspace))
        except Exception:
            workspace_id = None

    seen = {str(d).lower() for d in dataset_ids}
    queue = [(str(d), workspace_id) for d in dataset_ids]
    upstream = []
    while queue:
        dataset_id, ws_id = queue.pop(0)
        for server, database in _iter_power_bi_sources(dataset_id, ws_id):
            try:
                resolved = _resolve_power_bi_source(server, database)
            except Exception:
                print(
                    f"{icons.warning} Unable to resolve the upstream semantic model '{database}' ('{server}') used by the '{dataset_id}' semantic model."
                )
                continue
            if resolved is None:
                continue
            upstream_id, upstream_ws_id = resolved
            if upstream_id in seen:
                continue
            seen.add(upstream_id)
            upstream.append(upstream_id)
            queue.append((upstream_id, upstream_ws_id))

    return upstream


def _request_embed_token(
    dataset_ids: List[str],
    report_ids: List[str],
    xmla_permissions: Optional[str] = None,
) -> str:

    datasets = []
    for dataset_id in dataset_ids:
        dataset = {"id": dataset_id}
        if xmla_permissions is not None:
            dataset["xmlaPermissions"] = xmla_permissions
        datasets.append(dataset)

    payload = {
        "datasets": datasets,
        "reports": [{"id": report_id, "allowEdit": True} for report_id in report_ids],
    }

    response = _base_api(
        request="/v1.0/myorg/GenerateToken",
        method="post",
        client="fabric_sp",
        payload=payload,
    )
    return response.json().get("token")


_COMPOSITE_MODEL_HINT = (
    "If the report's semantic model uses DirectQuery to other Power BI semantic models (a composite model), "
    "every semantic model in the chain must be included in the embed token request with 'xmlaPermissions' set to 'ReadOnly' "
    "and the identity must have access to all of them. In this case an 'XMLA permissions are off' error does not "
    "necessarily mean that the XMLA endpoint is disabled on the capacity."
)


@log
def generate_embed_token(
    dataset_ids: List[UUID],
    report_ids: List[UUID],
    workspace: Optional[str | UUID] = None,
) -> str:
    """
    Generates an embed token for the specified reports and semantic models.

    This is a wrapper function for the following API: `Embed Token - Generate Token <https://learn.microsoft.com/rest/api/power-bi/embed-token/generate-token>`_.

    If the request fails because a semantic model uses DirectQuery to other Power BI semantic models
    (chained composite models), the upstream semantic models are discovered and the request is retried
    with all semantic models in the chain and 'xmlaPermissions' set to 'ReadOnly'.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataset_ids : typing.List[uuid.UUID]
        The semantic model IDs. Also accepts a single ID.
    report_ids : typing.List[uuid.UUID]
        The report IDs. Also accepts a single ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the semantic models reside. Only used to read the
        models' expressions when the upstream semantic models of a composite model cannot be found
        via the Get Datasources API.

    Returns
    -------
    str
        The embed token.
    """

    if not isinstance(dataset_ids, list):
        dataset_ids = [dataset_ids]
    if not isinstance(report_ids, list):
        report_ids = [report_ids]
    dataset_ids = [str(d) for d in dataset_ids]
    report_ids = [str(r) for r in report_ids]

    try:
        return _request_embed_token(dataset_ids, report_ids)
    except FabricHTTPException as e:
        if getattr(e, "status_code", None) != 400:
            raise
        upstream_ids = _get_upstream_dataset_ids(dataset_ids, workspace=workspace)
        if not upstream_ids:
            print(
                f"{icons.red_dot} Unable to generate an embed token and no upstream semantic models were found. {_COMPOSITE_MODEL_HINT}"
            )
            raise

    try:
        return _request_embed_token(
            dataset_ids + upstream_ids, report_ids, xmla_permissions="ReadOnly"
        )
    except FabricHTTPException:
        print(
            f"{icons.red_dot} Unable to generate an embed token including the upstream semantic models {upstream_ids}. {_COMPOSITE_MODEL_HINT}"
        )
        raise
