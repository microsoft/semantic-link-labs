import sempy
import sempy.fabric as fabric
from tqdm.auto import tqdm
from pyspark.sql import SparkSession
from delta import DeltaTable
from .HelperFunctions import resolve_lakehouse_name

def lakehouse_attached():

    """
    
    Documentation is available here: https://github.com/m-kovalsky/fabric_cat_tools?tab=readme-ov-file#lakehouse_attached

    """

    spark = SparkSession.builder.getOrCreate()
    lakeId = spark.conf.get('trident.lakehouse.id')
    
    if len(lakeId) > 0:
        return True
    else:
        return False

def optimize_lakehouse_tables(tables: str | list | None = None, lakehouse: str | None = None, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/m-kovalsky/fabric_cat_tools?tab=readme-ov-file#optimize_lakehouse_tables

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
