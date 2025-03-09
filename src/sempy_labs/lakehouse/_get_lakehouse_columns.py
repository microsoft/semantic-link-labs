import pandas as pd
from sempy_labs._helper_functions import (
    format_dax_object_name,
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    _create_dataframe,
    _pure_python_notebook,
    _load_delta_table,
)
from typing import Optional
from sempy._utils._log import log
from uuid import UUID
import re


@log
def get_lakehouse_columns(
    lakehouse: Optional[str | UUID] = None, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows the tables and columns of a lakehouse and their respective properties.

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
        Shows the tables/columns within a lakehouse and their properties.
    """
    from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables

    columns = {
        "Workspace Name": "string",
        "Lakehouse Name": "string",
        "Table Name": "string",
        "Column Name": "string",
        "Full Column Name": "string",
        "Data Type": "string",
    }
    df = _create_dataframe(columns=columns)

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    tables = get_lakehouse_tables(
        lakehouse=lakehouse, workspace=workspace, extended=False, count_rows=False
    )
    tables_filt = tables[tables["Format"] == "delta"]

    df_list = []

    def extract_quoted_value(text):
        match = re.search(r'"(.*?)"', text)  # Find text within double quotes
        return (
            match.group(1) if match else text
        )  # Return extracted value or original text

    for _, r in tables_filt.iterrows():
        table_name = r["Table Name"]
        path = r["Location"]
        delta_table = _load_delta_table(path)

        if _pure_python_notebook():
            for field in delta_table.schema().fields:  # Get column schema
                col_name = field.name

                new_data = {
                    "Workspace Name": workspace_name,
                    "Lakehouse Name": lakehouse_name,
                    "Table Name": table_name,
                    "Column Name": col_name,
                    "Full Column Name": format_dax_object_name(table_name, col_name),
                    "Data Type": extract_quoted_value(
                        str(field.type)
                    ),  # Parse from 'PrimitiveType' to 'string'
                }
                df_list.append(new_data)
        else:
            for col_name, data_type in delta_table.toDF().dtypes:
                new_data = {
                    "Workspace Name": workspace_name,
                    "Lakehouse Name": lakehouse_name,
                    "Table Name": table_name,
                    "Column Name": col_name,
                    "Full Column Name": format_dax_object_name(table_name, col_name),
                    "Data Type": data_type,
                }
                df_list.append(new_data)

    df = pd.DataFrame(df_list)

    return df
