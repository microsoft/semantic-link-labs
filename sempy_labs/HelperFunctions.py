import sempy
import sempy.fabric as fabric
import re
from pyspark.sql import SparkSession

green_dot = '\U0001F7E2'
yellow_dot = '\U0001F7E1'
red_dot = '\U0001F534'
in_progress = '⌛'

def create_abfss_path(lakehouse_id: str, lakehouse_workspace_id, delta_table_name):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#create_abfss_path

    """

    return f"abfss://{lakehouse_workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{delta_table_name}"

def format_dax_object_name(a,b):
    
    return "'" + a + "'[" + b + "]"

def create_relationship_name(from_table, from_column, to_table, to_column):

    return format_dax_object_name(from_table, from_column) + ' -> ' + format_dax_object_name(to_table, to_column)

def resolve_report_id(report: str, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#resolve_report_id

    """
    
    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    objectType = 'Report'
    dfI = fabric.list_items(workspace = workspace)
    dfI_filt = dfI[(dfI['Display Name'] == report) & (dfI['Type'] == objectType)]
    obj = dfI_filt['Id'].iloc[0]

    return obj

def resolve_report_name(report_id, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#resolve_report_name

    """
    
    
    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    objectType = 'Report'
    dfI = fabric.list_items(workspace = workspace)
    dfI_filt = dfI[(dfI['Id'] == report_id) & (dfI['Type'] == objectType)]
    obj = dfI_filt['Display Name'].iloc[0]

    return obj

def resolve_dataset_id(dataset: str, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#resolve_dataset_id

    """
    
    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    objectType = 'SemanticModel'
    dfI = fabric.list_items(workspace = workspace)
    dfI_filt = dfI[(dfI['Display Name'] == dataset) & (dfI['Type'] == objectType)]
    obj = dfI_filt['Id'].iloc[0]

    return obj

def resolve_dataset_name(dataset_id, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#resolve_dataset_name

    """
    
    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    objectType = 'SemanticModel'
    dfI = fabric.list_items(workspace = workspace)
    dfI_filt = dfI[(dfI['Id'] == dataset_id) & (dfI['Type'] == objectType)]
    obj = dfI_filt['Display Name'].iloc[0]

    return obj

def resolve_lakehouse_name(lakehouse_id, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#resolve_lakehouse_name

    """
    
    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    objectType = 'Lakehouse'
    dfI = fabric.list_items(workspace = workspace)
    dfI_filt = dfI[(dfI['Id'] == lakehouse_id) & (dfI['Type'] == objectType)]

    if len(dfI_filt) == 0:
        print(f"The '{lakehouse_id}' Lakehouse Id does not exist within the '{workspace}' workspace.")
        return
    
    obj = dfI_filt['Display Name'].iloc[0]

    return obj

def resolve_lakehouse_id(lakehouse: str, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#resolve_lakehouse_id

    """
    
    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    dfI = fabric.list_items('Lakehouse', workspace = workspace)
    dfI_filt = dfI[(dfI['Display Name'] == lakehouse)]

    if len(dfI_filt) == 0:
        print(f"The '{lakehouse}' lakehouse does not exist within the '{workspace}' workspace.")
        return
    
    obj = dfI_filt['Id'].iloc[0]

    return obj

def get_direct_lake_sql_endpoint(dataset: str, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_direct_lake_sql_endpoint

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    dfP = fabric.list_partitions(dataset = dataset, workspace = workspace)
    dfP_filt = dfP[dfP['Mode'] == 'DirectLake']

    if len(dfP_filt) == 0:
        print(f"The '{dataset}' semantic model in the '{workspace}' workspace is not in Direct Lake mode.")
        return
    
    dfE = fabric.list_expressions(dataset = dataset, workspace = workspace)
    dfE_filt = dfE[dfE['Name']== 'DatabaseQuery']
    expr = dfE_filt['Expression'].iloc[0]

    matches = re.findall(r'"([^"]*)"', expr)
    sqlEndpointId = matches[1]
    
    return sqlEndpointId

def generate_embedded_filter(filter: str):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#generate_embedded_filter

    """

    pattern = r"'[^']+'\[[^\[]+\]"
    matches = re.findall(pattern, filter)
    for match in matches:
        matchReplace = match.replace("'",'').replace('[','/').replace(']','')\
        .replace(' ','_x0020_').replace('@','_00x40_').replace('+','_0x2B_').replace('{','_007B_').replace('}','_007D_')
        filter = filter.replace(match, matchReplace)
    
    pattern = r"\[[^\[]+\]"
    matches = re.findall(pattern, filter)
    for match in matches:
        matchReplace = match.replace("'",'').replace('[','/').replace(']','')\
        .replace(' ','_x0020_').replace('@','_00x40_').replace('+','_0x2B_').replace('{','_007B_').replace('}','_007D_')
        filter = filter.replace(match, matchReplace)

    revised_filter = filter.replace('<=','le').replace('>=','ge').replace('<>','ne').replace('!=','ne')\
            .replace('==','eq').replace('=','eq').replace('<','lt').replace('>','gt')\
            .replace(' && ',' and ').replace(' & ',' and ')\
            .replace(' || ',' or ').replace(' | ',' or ')\
            .replace('{','(').replace('}',')')
    
    return revised_filter

def save_as_delta_table(dataframe, delta_table_name: str, write_mode: str, lakehouse: str | None = None, workspace: str | None = None):

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id=lakehouse_id, workspace=workspace)
    else:
        lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)

    writeModes = ['append', 'overwrite']
    write_mode = write_mode.lower()

    if write_mode not in writeModes:
        print(f"{red_dot} Invalid 'write_type' parameter. Choose from one of the following values: {writeModes}.")
        return

    if ' ' in delta_table_name:
        print(f"{red_dot} Invalid 'delta_table_name'. Delta tables in the lakehouse cannot have spaces in their names.")
        return
    
    dataframe.columns = dataframe.columns.str.replace(' ', '_')

    spark = SparkSession.builder.getOrCreate()
    spark_df = spark.createDataFrame(dataframe)

    filePath = create_abfss_path(lakehouse_id = lakehouse_id, lakehouse_workspace_id = workspace_id, delta_table_name = delta_table_name)
    spark_df.write.mode(write_mode).format('delta').save(filePath)
    print(f"{green_dot} The dataframe has been saved as the '{delta_table_name}' table in the '{lakehouse}' lakehouse within the '{workspace}' workspace.")