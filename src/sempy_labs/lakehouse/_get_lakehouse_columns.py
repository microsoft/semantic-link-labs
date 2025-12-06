import pandas as pd
import re
from sempy_labs._helper_functions import (
    format_dax_object_name,
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    _create_dataframe,
    _get_delta_table,
    _pure_python_notebook,
)
from typing import Optional
from sempy._utils._log import log
from uuid import UUID
import sempy_labs._icons as icons


@log
def get_lakehouse_columns(
    lakehouse: Optional[str | UUID] = None, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows the tables and columns of a lakehouse and their respective properties. This function can be executed in either a PySpark or pure Python notebook. Note that data types may show differently when using PySpark vs pure Python.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Shows the tables/columns within a lakehouse and their properties.
    """
    from ._get_lakehouse_tables import get_lakehouse_tables

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
        lakehouse=lakehouse_id, workspace=workspace_id, extended=False, count_rows=False
    )
    tables_filt = tables[tables["Format"] == "delta"]

    def add_column_metadata(table_name, col_name, data_type):
        new_rows.append(
            {
                "Workspace Name": workspace_name,
                "Lakehouse Name": lakehouse_name,
                "Table Name": table_name,
                "Column Name": col_name,
                "Full Column Name": format_dax_object_name(table_name, col_name),
                "Data Type": data_type,
            }
        )

    new_rows = []

    for _, r in tables_filt.iterrows():
        table_name = r["Table Name"]
        path = r["Location"]

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
                add_column_metadata(table_name, col_name, data_type)
        else:
            delta_table = _get_delta_table(path=path)
            table_df = delta_table.toDF()

            for col_name, data_type in table_df.dtypes:
                add_column_metadata(table_name, col_name, data_type)

    return pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
