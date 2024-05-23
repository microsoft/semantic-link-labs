import sempy
import sempy.fabric as fabric
import pandas as pd
from pyspark.sql import SparkSession
from delta import DeltaTable
from .HelperFunctions import resolve_lakehouse_name, format_dax_object_name, resolve_lakehouse_id
from .GetLakehouseTables import get_lakehouse_tables
from sempy._utils._log import log

def get_lakehouse_columns(lakehouse: str | None = None, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_lakehouse_columns

    """

    df = pd.DataFrame(columns=['Workspace Name', 'Lakehouse Name', 'Table Name', 'Column Name', 'Full Column Name', 'Data Type'])

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

    tables = get_lakehouse_tables(lakehouse = lakehouse, workspace = workspace, extended = False, count_rows = False)
    tables_filt = tables[tables['Format'] == 'delta']

    for i, r in tables_filt.iterrows():
        tName = r['Table Name']
        tPath = r['Location']
        delta_table = DeltaTable.forPath(spark, tPath)
        sparkdf = delta_table.toDF()

        for cName, data_type in sparkdf.dtypes:
            tc = format_dax_object_name(tName, cName)
            new_data = {'Workspace Name': workspace, 'Lakehouse Name': lakehouse, 'Table Name': tName, 'Column Name': cName, 'Full Column Name': tc, 'Data Type': data_type}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df