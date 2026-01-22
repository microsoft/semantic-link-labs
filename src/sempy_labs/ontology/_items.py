from os import PathLike
import pandas as pd
from typing import Optional
from uuid import UUID
import sempy.fabric as fabric
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    create_item,
    delete_item,
)
from sempy_labs.eventhouse import create_eventhouse
from sempy_labs._kql_databases import _resolve_cluster_uri


@log
def accellerate(name: str, workspace: Optional[str | UUID] = None):

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    eh_name = f"eh_{name}"
    lh_name = f"lh_{name}"
    ont_name = f"ont_{name}"

    # Create lakehouse
    lakehouse_id = fabric.create_lakehouse(display_name=lh_name, workspace=workspace_id, enable_schema=True)

    # Create eventhouse
    eventhouse_id = create_eventhouse(name=eh_name, definition={}, workspace=workspace_id)

    # Get the cluster URI of the eventhouse's KQL database
    cluster_uri = _resolve_cluster_uri(kql_database=eh_name, workspace=workspace_id)

    create_ontology(name=ont_name, definition={}, workspace=workspace_id)


@log
def create_ontology(name: str, definition: dict, workspace: Optional[str | UUID] = None, folder: Optional[str | PathLike] = None,) -> str:

    return create_item(name=name, type='Ontology', definition=definition, workspace=workspace, folder=folder)


@log
def list_ontologies(workspace: Optional[str | UUID] = None) -> pd.DataFrame:

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Ontology Name": "str",
        "Ontology Id": "str",
        "Description": "str",
    }

    df = _create_dataframe(columns=columns)

    responses = _base_api(request=f"/v1/workspaces/{workspace_id}/ontologies", uses_pagination=True, client="fabric_sp")

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Ontology Name": v.get("displayName"),
                    "Ontology Id": v.get("id"),
                    "Description": v.get("description"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def delete_ontology(ontology: str | UUID, workspace: Optional[str | UUID] = None) -> None:

    delete_item(item=ontology, type='Ontology', workspace=workspace)