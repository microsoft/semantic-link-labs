import pandas as pd
from typing import Optional, Union, List
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    resolve_item_name_and_id,
    create_abfss_path,
)
from tqdm.auto import tqdm


def optimize_delta_tables(
    tables: Optional[Union[str, List[str]]] = None,
    source: Optional[str | UUID] = None,
    source_type: str = "Lakehouse",
    workspace: Optional[str | UUID] = None,
):
    """
    Runs the `OPTIMIZE <https://docs.delta.io/latest/optimizations-oss.html>`_ function over the specified delta tables.

    Parameters
    ----------
    tables : str | List[str], default=None
        The table(s) to optimize.
        Defaults to None which resovles to optimizing all tables within the lakehouse.
    source : str | uuid.UUID, default=None
        The source location of the delta table (i.e. lakehouse).
        Defaults to None which resolves to the lakehouse attached to the notebook.
    source_type : str, default="Lakehouse"
        The source type (i.e. "Lakehouse", "SemanticModel")
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    from pyspark.sql import SparkSession
    from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
    from delta import DeltaTable

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if source is None:
        (item_name, item_id) = resolve_lakehouse_name_and_id()
    else:
        (item_name, item_id) = resolve_item_name_and_id(
            item=source, type=source_type, workspace=workspace_id
        )

    if isinstance(tables, str):
        tables = [tables]

    if source_type == "Lakehouse":
        dfL = get_lakehouse_tables(lakehouse=item_name, workspace=workspace_id)
        dfL_delta = dfL[dfL["Format"] == "delta"]

        if tables is not None:
            delta_tables = dfL_delta[dfL_delta["Table Name"].isin(tables)]
        else:
            delta_tables = dfL_delta.copy()
    else:
        data = []
        for t in tables:
            new_data = {
                "Table Name": t,
                "Location": create_abfss_path(workspace_id, item_id, t),
            }
            data.append(new_data)

        delta_tables = pd.DataFrame(data)

    spark = SparkSession.builder.getOrCreate()

    for _, r in (bar := tqdm(delta_tables.iterrows())):
        tableName = r["Table Name"]
        tablePath = r["Location"]
        bar.set_description(f"Optimizing the '{tableName}' table...")
        deltaTable = DeltaTable.forPath(spark, tablePath)
        deltaTable.optimize().executeCompaction()
