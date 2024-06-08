import sempy.fabric as fabric
import pandas as pd
from pyspark.sql import SparkSession
from sempy_labs._helper_functions import (
    resolve_lakehouse_name,
    format_dax_object_name,
    resolve_lakehouse_id,
)
from typing import Optional
from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables


def get_lakehouse_columns(
    lakehouse: Optional[str] = None, workspace: Optional[str] = None
):
    """
    Shows the tables and columns of a lakehouse and their respective properties.

    Parameters
    ----------
    lakehouse : str, default=None
        The Fabric lakehouse.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str, default=None
        The Fabric workspace used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Shows the tables/columns within a lakehouse and their properties.
    """

    df = pd.DataFrame(
        columns=[
            "Workspace Name",
            "Lakehouse Name",
            "Table Name",
            "Column Name",
            "Full Column Name",
            "Data Type",
        ]
    )

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    if lakehouse == None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, workspace)
    else:
        lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)

    spark = SparkSession.builder.getOrCreate()

    tables = get_lakehouse_tables(
        lakehouse=lakehouse, workspace=workspace, extended=False, count_rows=False
    )
    tables_filt = tables[tables["Format"] == "delta"]

    for i, r in tables_filt.iterrows():
        tName = r["Table Name"]
        tPath = r["Location"]
        delta_table = DeltaTable.forPath(spark, tPath)
        sparkdf = delta_table.toDF()

        for cName, data_type in sparkdf.dtypes:
            tc = format_dax_object_name(tName, cName)
            new_data = {
                "Workspace Name": workspace,
                "Lakehouse Name": lakehouse,
                "Table Name": tName,
                "Column Name": cName,
                "Full Column Name": tc,
                "Data Type": data_type,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df
