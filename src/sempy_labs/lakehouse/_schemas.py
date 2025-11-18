from typing import Optional, List
from uuid import UUID
from sempy._utils._log import log
import pandas as pd
from sempy_labs._helper_functions import (
    resolve_lakehouse_name_and_id,
    resolve_workspace_id,
    resolve_lakehouse_id,
    _create_dataframe,
    _base_api,
    resolve_workspace_name_and_id,
)
import sempy_labs._icons as icons


@log
def list_schemas(
    lakehouse: Optional[str | UUID] = None, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Lists the schemas within a Fabric lakehouse.

    Parameters
    ----------
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Shows the schemas within a lakehouse.
    """

    columns = {
        "Schema Name": "str",
    }
    df = _create_dataframe(columns=columns)
    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_lakehouse_id(lakehouse, workspace)
    response = _base_api(
        request=f"{workspace_id}/{item_id}/api/2.1/unity-catalog/schemas?catalog_name={item_id}",
        client="onelake",
    )

    rows = []
    for s in response.json().get("schemas", []):
        rows.append(
            {
                "Schema Name": s.get("name", None),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


def list_tables(
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    schema: Optional[str | List[str]] = None,
) -> pd.DataFrame:

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_lakehouse_name_and_id(lakehouse, workspace)

    response = _base_api(f"/v1/workspaces/{workspace_id}/lakehouses/{item_id}")
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
        if schema:
            if isinstance(schema, str):
                schema = [schema]
            schemas = schemas[schemas["Schema Name"].isin(schema)]

        # Loop through schemas
        for _, r in schemas.iterrows():
            schema_name = r["Schema Name"]
            response = _base_api(
                request=f"{workspace_id}/{item_id}/api/2.1/unity-catalog/tables?catalog_name={item_id}&schema_name={schema_name}",
                client="onelake",
            )
            # Loop through tables
            for t in response.json().get("tables", []):
                location = t.get("storage_location", {})
                location = f'abfss://{location.split(".microsoft.com/")[1]}'
                rows.append(
                    {
                        "Workspace Name": workspace_name,
                        "Lakehouse Name": item_name,
                        "Table Name": t.get("name", {}),
                        "Schema Name": schema_name,
                        "Format": t.get("data_source_format", {}).capitalize(),
                        "Type": "Managed",
                        "Location": location,
                    }
                )
    else:
        if schema:
            print(
                f"{icons.info} The schema parameter has been ignored as the '{item_name}' lakehouse within the '{workspace_name}' workspace has schemas disabled."
            )
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


def schema_exists(
    schema: str,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
) -> bool:
    """
    Indicates whether the specified schema exists within a Fabric lakehouse.

    Parameters
    ----------
    schema : str
        The name of the schema.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    bool
        Indicates whether the specified schema exists within the lakehouse.
    """

    df = list_schemas(lakehouse=lakehouse, workspace=workspace)
    return schema in df["Schema Name"].values

    # (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    # (item_name, item_id) = resolve_lakehouse_name_and_id(lakehouse, workspace)
    # response = _base_api(
    #    request=f"{workspace_id}/{item_id}/api/2.1/unity-catalog/schemas/{schema}",
    #    client="onelake",
    #    method="head",
    # )

    # response.json()
