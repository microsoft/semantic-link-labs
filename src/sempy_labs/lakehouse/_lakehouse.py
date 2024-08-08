import sempy.fabric as fabric
from tqdm.auto import tqdm
from pyspark.sql import SparkSession
from sempy_labs._helper_functions import resolve_lakehouse_name
from typing import List, Optional, Union
import sempy_labs._icons as icons
from sempy._utils._log import log


def lakehouse_attached() -> bool:
    """
    Identifies if a lakehouse is attached to the notebook.

    Returns
    -------
    bool
        Returns True if a lakehouse is attached to the notebook.
    """

    spark = SparkSession.builder.getOrCreate()
    lakeId = spark.conf.get("trident.lakehouse.id")

    if len(lakeId) > 0:
        return True
    else:
        return False


@log
def optimize_lakehouse_tables(
    tables: Optional[Union[str, List[str]]] = None,
    lakehouse: Optional[str] = None,
    workspace: Optional[str] = None,
):
    """
    Runs the `OPTIMIZE <https://docs.delta.io/latest/optimizations-oss.html>`_ function over the specified lakehouse tables.

    Parameters
    ----------
    tables : str | List[str], default=None
        The table(s) to optimize.
        Defaults to None which resovles to optimizing all tables within the lakehouse.
    lakehouse : str, default=None
        The Fabric lakehouse.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str, default=None
        The Fabric workspace used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
    from delta import DeltaTable

    workspace = fabric.resolve_workspace_name(workspace)

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, workspace)

    lakeTables = get_lakehouse_tables(lakehouse=lakehouse, workspace=workspace)
    lakeTablesDelta = lakeTables[lakeTables["Format"] == "delta"]

    if isinstance(tables, str):
        tables = [tables]

    if tables is not None:
        tables_filt = lakeTablesDelta[lakeTablesDelta["Table Name"].isin(tables)]
    else:
        tables_filt = lakeTablesDelta.copy()

    tableCount = len(tables_filt)

    spark = SparkSession.builder.getOrCreate()

    i = 1
    for _, r in (bar := tqdm(tables_filt.iterrows())):
        tableName = r["Table Name"]
        tablePath = r["Location"]
        bar.set_description(f"Optimizing the '{tableName}' table...")
        deltaTable = DeltaTable.forPath(spark, tablePath)
        deltaTable.optimize().executeCompaction()
        print(
            f"{icons.green_dot} The '{tableName}' table has been optimized. ({str(i)}/{str(tableCount)})"
        )
        i += 1
