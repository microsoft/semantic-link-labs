from typing import Optional
from uuid import UUID
import requests
from sempy._utils._log import log
import pandas as pd
from sempy_labs._helper_functions import (
    resolve_lakehouse_name_and_id,
    resolve_workspace_id,
    resolve_lakehouse_id,
    _create_dataframe,
    _update_dataframe_datatypes,
    _base_api,
    resolve_workspace_name_and_id,
)


def _get_headers():

    import notebookutils

    token = notebookutils.credentials.getToken("storage")
    headers = {"Authorization": f"Bearer {token}"}
    return headers


@log
def list_schemas(
    lakehouse: Optional[str | UUID] = None, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:

    columns = {
        "Schema Name": "str",
        #    "Created At": "datetime",
        #    "Updated At": "datetime"
    }
    df = _create_dataframe(columns=columns)
    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_lakehouse_id(lakehouse, workspace)
    response = requests.get(
        f"https://onelake.table.fabric.microsoft.com/delta/{workspace_id}/{item_id}/api/2.1/unity-catalog/schemas?catalog_name={item_id}",
        headers=_get_headers(),
    )

    rows = []
    for s in response.json().get("schemas", []):
        rows.append(
            {
                "Schema Name": s.get("name", {}),
                #       "Created At": s.get('created_at', {}),
                #       "Updated At": s.get('updated_at', {}),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(df, columns)

    return df


def list_tables(
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    schema: Optional[str] = None,
) -> pd.DataFrame:

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_lakehouse_name_and_id(lakehouse, workspace)

    response = _base_api(f"/v1/workspaces/{workspace_id}/lakehouses/{item_id})")
    default_schema = response.json().get("properties", {}).get("defaultSchema", None)
    schema_enabled = True if default_schema else False

    columns = {
        "Workspace Name": "str",
        "Lakehouse Name": "str",
        "Table Name": "str",
        "Schema Name": "str",
        "Format": "str",
        "Type": "str",
        "Location": "str",
    }
    df = _create_dataframe(columns=columns)

    rows = []
    if schema_enabled:
        schemas = list_schemas(lakehouse=lakehouse, workspace=workspace)
        # Loop through schemas
        for _, r in schemas.iterrows():
            schema_name = r["Schema Name"]
            response = requests.get(
                f"https://onelake.table.fabric.microsoft.com/delta/{workspace_id}/{item_id}/api/2.1/unity-catalog/tables?catalog_name={item_id}&schema_name={schema_name}",
                headers=_get_headers(),
            )
            # Loop through tables
            for t in response.json().get("tables", []):
                rows.append(
                    {
                        "Workspace Name": workspace_name,
                        "Lakehouse Name": item_name,
                        "Table Name": t.get("name", {}),
                        "Schema Name": schema_name,
                        "Format": t.get("data_source_format", {}),
                        "Type": None,
                        "Location": t.get("storage_location", {}),
                    }
                )
    else:
        responses = _base_api(
            request=f"v1/workspaces/{workspace_id}/lakehouses/{item_id}/tables",
            uses_pagination=True,
            client="fabric_sp",
        )
        for r in responses:
            for i in r.get("data", []):
                rows.append(
                    {
                        "Workspace Name": workspace_name,
                        "Lakehouse Name": item_name,
                        "Schema Name": None,
                        "Table Name": i.get("name"),
                        "Format": i.get("format"),
                        "Type": i.get("type"),
                        "Location": i.get("location"),
                    }
                )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df
