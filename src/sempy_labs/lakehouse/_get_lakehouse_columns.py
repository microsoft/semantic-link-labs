import pandas as pd
from sempy_labs._helper_functions import (
    format_dax_object_name,
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    _create_dataframe,
    _create_spark_session,
)
from typing import Optional
from sempy._utils._log import log
from uuid import UUID


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
    lakehouse_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Shows the tables/columns within a lakehouse and their properties.
    """
    from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
    from delta import DeltaTable

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

    spark = _create_spark_session()

    tables = get_lakehouse_tables(
        lakehouse=lakehouse_id, workspace=workspace_id, extended=False, count_rows=False
    )
    tables_filt = tables[tables["Format"] == "delta"]

    for _, r in tables_filt.iterrows():
        table_name = r["Table Name"]
        path = r["Location"]
        delta_table = DeltaTable.forPath(spark, path)
        sparkdf = delta_table.toDF()

        for col_name, data_type in sparkdf.dtypes:
            full_column_name = format_dax_object_name(table_name, col_name)
            new_data = {
                "Workspace Name": workspace_name,
                "Lakehouse Name": lakehouse,
                "Table Name": table_name,
                "Column Name": col_name,
                "Full Column Name": full_column_name,
                "Data Type": data_type,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df
