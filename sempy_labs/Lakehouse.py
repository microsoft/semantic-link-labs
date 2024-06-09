import sempy
import sempy.fabric as fabric
from tqdm.auto import tqdm
from pyspark.sql import SparkSession
from delta import DeltaTable
from .HelperFunctions import resolve_lakehouse_name
from typing import List, Optional, Union
from sempy._utils._log import log

def lakehouse_attached() -> bool:

    """
    Identifies if a lakehouse is attached to the notebook.

    Parameters
    ----------

    Returns
    -------
    bool
        Returns True if a lakehouse is attached to the notebook.
    """  

    spark = SparkSession.builder.getOrCreate()
    lakeId = spark.conf.get('trident.lakehouse.id')
    
    if len(lakeId) > 0:
        return True
    else:
        return False

@log
def optimize_lakehouse_tables(tables: Optional[Union[str, List[str]]] = None, lakehouse: Optional[str] = None, workspace: Optional[str] = None):

    """
    Runs the [OPTIMIZE](https://docs.delta.io/latest/optimizations-oss.html) function over the specified lakehouse tables.

    Parameters
    ----------
    tables : str | List[str] | None
        The table(s) to optimize. If no tables are specified, all tables in the lakehouse will be optimized.
    lakehouse : str, default=None
        The Fabric lakehouse.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str, default=None
        The Fabric workspace used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    
    """

    from .GetLakehouseTables import get_lakehouse_tables

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    
    if lakehouse == None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, workspace)

    lakeTables = get_lakehouse_tables(lakehouse = lakehouse, workspace = workspace)
    lakeTablesDelta = lakeTables[lakeTables['Format'] == 'delta']

    if isinstance(tables, str):
        tables = [tables]

    if tables is not None:
        tables_filt = lakeTablesDelta[lakeTablesDelta['Table Name'].isin(tables)]
    else:
        tables_filt = lakeTablesDelta.copy()

    tableCount = len(tables_filt)

    spark = SparkSession.builder.getOrCreate()

    i=1
    for index, r in (bar := tqdm(tables_filt.iterrows())):
        tableName = r['Table Name']
        tablePath = r['Location']
        bar.set_description(f"Optimizing the '{tableName}' table...")
        deltaTable = DeltaTable.forPath(spark, tablePath)
        deltaTable.optimize().executeCompaction()
        print(f"The '{tableName}' table has been optimized. ({str(i)}/{str(tableCount)})")
        i+=1
