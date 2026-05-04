import yaml
import pandas as pd
from typing import List, Optional
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _create_dataframe,
    _base_api,
)


def get_databricks_headers(databricks_token: str) -> dict:
    return {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json",
    }


@log
def list_databricks_columns(
    databricks_workspace: str,
    unity_catalog: str,
    schema: str,
    databricks_token: str,
    table_name: Optional[str] = None,
) -> pd.DataFrame:

    url = f"{databricks_workspace}/api/2.1/unity-catalog/tables?catalog_name={unity_catalog}&schema_name={schema}"
    response = _base_api(
        request=url,
        client="databricks",
        headers=get_databricks_headers(databricks_token),
    )

    columns = {
        "Catalog Name": "str",
        "Schema Name": "str",
        "Table Name": "str",
        "Column Name": "str",
        "Data Type": "str",
        "Owner": "str",
        "Storage Location": "str",
        "Comment": "str",
        "Data Source Format": "str",
        "Table Type": "str",
    }

    df = _create_dataframe(columns=columns)
    rows = []
    for t in response.json().get("tables", []):
        t_name = t.get("name")
        if table_name is None or table_name == t_name:
            for c in t.get("columns", []):
                rows.append(
                    {
                        "Catalog Name": t.get("catalog_name"),
                        "Schema Name": t.get("schema_name"),
                        "Table Name": table_name,
                        "Table Type": t.get("table_type"),
                        "Data Source Format": t.get("data_source_format"),
                        "Column Name": c.get("name"),
                        "Data Type": c.get("type_text"),
                        "Owner": t.get("owner"),
                        "Storage Location": t.get("storage_location"),
                        "Comment": t.get("comment"),
                    }
                )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def list_databricks_metric_views(
    databricks_workspace: str, unity_catalog: str, schema: str, databricks_token: str
) -> List[dict]:
    """
    Lists all metric views in a specified Unity Catalog and schema within an Azure Databricks workspace.

    Parameters
    ----------
    databricks_workspace : str
        The URL of the Azure Databricks workspace. Example: "https://dbc-12345x67-8xx9.cloud.databricks.com"
    unity_catalog : str
        The name of the Unity Catalog.
    schema : str
        The name of the schema within the Unity Catalog.
    databricks_token : str
        The personal access token for authenticating with the Azure Databricks REST API.

    Returns
    -------
    List[dict]
        A list of dictionaries, each containing details about a metric view, including its name, view definition, and columns.
    """

    response = _base_api(
        request=f"{databricks_workspace}/api/2.1/unity-catalog/tables?catalog_name={unity_catalog}&schema_name={schema}",
        client="databricks",
        headers=get_databricks_headers(databricks_token),
    )

    rows = []
    for t in response.json().get("tables"):
        name = t.get("name")
        table_type = t.get("table_type")
        view_definition = t.get("view_definition")
        if table_type == "METRIC_VIEW":
            yaml_dict = yaml.safe_load(view_definition)
            rows.append(
                {
                    "Name": name,
                    "View Definition": yaml_dict,
                    "Objects": t.get("columns", []),
                }
            )

    return rows


@log
def list_databricks_tables(
    databricks_workspace: str,
    unity_catalog: str,
    schema: str,
    databricks_token: str,
    table_name: Optional[str] = None,
) -> pd.DataFrame:
    """
    Lists all tables in a specified Unity Catalog and schema within a Databricks workspace.

    Parameters
    ----------
    databricks_workspace : str
        The URL of the Azure Databricks workspace. Example: "https://dbc-12345x67-8xx9.cloud.databricks.com"
    unity_catalog : str
        The name of the Unity Catalog.
    schema : str
        The name of the schema within the Unity Catalog.
    databricks_token : str
        The personal access token for authenticating with the Azure Databricks REST API.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing details about each table, including its name, catalog, schema, type, and data source format.

    """

    url = f"{databricks_workspace}/api/2.1/unity-catalog/tables?catalog_name={unity_catalog}&schema_name={schema}"
    if table_name:
        url = f"{databricks_workspace}/api/2.1/unity-catalog/tables/{unity_catalog}.{schema}.{table_name}"

    response = _base_api(
        request=url,
        client="databricks",
        headers=get_databricks_headers(databricks_token),
    )

    columns = {
        "Table Name": "str",
        "Catalog Name": "str",
        "Schema Name": "str",
        "Table Type": "str",
        "Data Source Format": "str",
    }

    df = _create_dataframe(columns=columns)

    rows = []

    data = response.json()

    # Normalize to a list
    tables = data.get("tables") if table_name is None else [data]

    for t in tables or []:
        rows.append(
            {
                "Table Name": t.get("name"),
                "Catalog Name": t.get("catalog_name"),
                "Schema Name": t.get("schema_name"),
                "Table Type": t.get("table_type"),
                "Data Source Format": t.get("data_source_format"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def list_permissions(object: str, databricks_workspace: str, databricks_token: str, return_dataframe: bool = True) -> pd.DataFrame | dict:

    """
    Lists the permissions associated with an object (i.e table, view, metric view) in a Databricks workspace.

    Parameters
    ----------
    object : str
        This can either be a catalog, schema, or table. If specifying a table, the format should be "catalog.schema.table". If specifying a schema, the format should be "catalog.schema". If specifying a catalog, just provide the catalog name.
    databricks_workspace : str
        The URL of the Azure Databricks workspace. Example: "https://dbc-12345x67-8xx9.cloud.databricks.com"
    databricks_token : str
        The personal access token for authenticating with the Azure Databricks REST API.
    return_dataframe : bool, default=True
        If True, returns the permissions as a pandas DataFrame. If False, returns the raw JSON response as a dictionary.

    Returns
    -------
    pandas.DataFrame | dict
        If return_dataframe is True, returns a DataFrame with columns for Principal, Privilege, Inherited From Type, and Inherited From Name.
        If return_dataframe is False, returns the raw JSON response from the API as a dictionary.
    """

    type = None
    parts = object.split(".")
    if len(parts) == 3:
        type = 'table'
    elif len(parts) == 2:
        type = 'schema'
    elif len(parts) == 1:
        type = 'catalog'
    else:
        raise ValueError("Invalid object format. Expected format: 'catalog.schema.table' or 'catalog.schema' or 'catalog'.")

    
    resp = _base_api(
        request=f"{databricks_workspace}/api/2.1/unity-catalog/effective-permissions/{type}/{object}",
        client="databricks",
        headers=get_databricks_headers(databricks_token),
    ).json()

    if not return_dataframe:
        return resp

    rows = []
    for p in resp.get('privilege_assignments', []):
        name = p.get('principal')
        prs = p.get('privileges')
        for priv in prs:
            rows.append({
                "Principal": name,
                "Privilege": priv.get('privilege'),
                "Inherited From Type": priv.get('inherited_from_type'),
                "Inherited From Name": priv.get('inherited_from_name'),
            })
    df = pd.DataFrame(rows)

    return df