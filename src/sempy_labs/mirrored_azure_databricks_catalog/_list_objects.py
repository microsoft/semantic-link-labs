import yaml
import pandas as pd
import re
from uuid import UUID
from typing import List, Optional
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _create_dataframe,
    _base_api,
    create_abfss_path,
    resolve_workspace_id,
    resolve_item_id,
    _pure_python_notebook,
    _get_delta_table,
)
import sempy_labs._icons as icons


def get_databricks_headers(databricks_token: str) -> dict:
    return {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json",
    }


@log
def list_databricks_columns(
    databricks_workspace: str, unity_catalog: str, schema: str, databricks_token: str
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
        table_type = t.get("table_type")
        if table_type == "MANAGED":
            for c in t.get("columns", []):
                rows.append(
                    {
                        "Catalog Name": t.get("catalog_name"),
                        "Schema Name": t.get("schema_name"),
                        "Table Name": t.get("name"),
                        "Column Name": c.get("name"),
                        "Data Type": c.get("type_text"),
                        "Owner": t.get("owner"),
                        "Storage Location": t.get("storage_location"),
                        "Comment": t.get("comment"),
                        "Data Source Format": t.get("data_source_format"),
                        "Table Type": table_type,
                    }
                )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def list_columns(mirrored_azure_databricks_catalog: str | UUID, schema: str, table: str, workspace: Optional[str | UUID] = None) -> pd.DataFrame:

    columns = {
        "Schema Name": "str",
        "Table Name": "str",
        "Column Name": "str",
        "Data Type": "str",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)
    catalog_id = resolve_item_id(item=mirrored_azure_databricks_catalog, type="MirroredAzureDatabricksCatalog", workspace=workspace_id)
    path = create_abfss_path(lakehouse_id=catalog_id, lakehouse_workspace_id=workspace_id, delta_table_name=table, schema=schema)

    rows = []
    if _pure_python_notebook():
        from deltalake import DeltaTable

        table_schema = DeltaTable(path).schema()

        for field in table_schema.fields:
            col_name = field.name
            match = re.search(r'"(.*?)"', str(field.type))
            if not match:
                raise ValueError(
                    f"{icons.red_dot} Could not find data type for column {col_name}."
                )
            data_type = match.group(1)
            rows.append({
                "Schema Name": schema,
                "Table Name": table,
                "Column Name": col_name,
                "Data Type": data_type,
            })
    else:
        delta_table = _get_delta_table(path=path)
        table_df = delta_table.toDF()

        for col_name, data_type in table_df.dtypes:
            rows.append({
                "Schema Name": schema,
                "Table Name": table,
                "Column Name": col_name,
                "Data Type": data_type,
            })

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
                    "Columns": t.get("columns", []),
                }
            )

    return rows
