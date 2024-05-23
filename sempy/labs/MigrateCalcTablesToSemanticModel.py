import sempy
import sempy.fabric as fabric
import re, datetime, time
from .GetLakehouseTables import get_lakehouse_tables
from .HelperFunctions import resolve_lakehouse_name
from .TOM import connect_semantic_model
from sempy._utils._log import log

green_dot = '\U0001F7E2'
yellow_dot = '\U0001F7E1'
red_dot = '\U0001F534'
in_progress = '⌛'

@log
def migrate_calc_tables_to_semantic_model(dataset: str, new_dataset: str, workspace: str | None = None, new_dataset_workspace: str | None = None, lakehouse: str | None = None, lakehouse_workspace: str | None = None ):
    
    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#migrate_calc_tables_to_semantic_model

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    if new_dataset_workspace == None:
        new_dataset_workspace = workspace

    if lakehouse_workspace == None:
        lakehouse_workspace = new_dataset_workspace
    if lakehouse == None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, lakehouse_workspace)

    # Get calc tables but not field parameters
    dfP = fabric.list_partitions(dataset = dataset, workspace = workspace)
    dfP_filt = dfP[(dfP['Source Type'] == 'Calculated')]
    dfP_filt = dfP_filt[~dfP_filt['Query'].str.contains('NAMEOF')]

    dfC = fabric.list_columns(dataset = dataset, workspace = workspace)
    lc = get_lakehouse_tables(lakehouse=lakehouse, workspace=lakehouse_workspace)
    # Get all calc table columns of calc tables not including field parameters
    dfC_filt = dfC[(dfC['Table Name'].isin(dfP_filt['Table Name']))]# & (dfC['Type'] == 'CalculatedTableColumn')]
    #dfA = list_annotations(new_dataset, new_dataset_workspace)
    #dfA_filt = dfA[(dfA['Object Type'] == 'Model') & ~ (dfA['Annotation Value'].str.contains('NAMEOF'))]

    if len(dfP_filt) == 0:
        print(f"{green_dot} The '{dataset}' semantic model has no calculated tables.")
        return
    
    start_time = datetime.datetime.now()
    timeout = datetime.timedelta(minutes=1)
    success = False

    while not success:
        try:
            with connect_semantic_model(dataset=new_dataset, readonly=False, workspace=new_dataset_workspace) as tom:
                success = True
                for tName in dfC_filt['Table Name'].unique():
                    if tName.lower() in lc['Table Name'].values:

                        try:
                            tom.model.Tables[tName]
                        except:
                            tom.add_table(name = tName)
                            tom.add_entity_partition(table_name=tName, entity_name=tName.replace(' ','_').lower())

                    columns_in_table = dfC_filt.loc[dfC_filt['Table Name'] == tName, 'Column Name'].unique()

                    for cName in columns_in_table:
                        scName = dfC.loc[(dfC['Table Name'] == tName) & (dfC['Column Name'] == cName), 'Source'].iloc[0]
                        cDataType = dfC.loc[(dfC['Table Name'] == tName) & (dfC['Column Name'] == cName), 'Data Type'].iloc[0]
                        cType = dfC.loc[(dfC['Table Name'] == tName) & (dfC['Column Name'] == cName), 'Type'].iloc[0]

                        #av = tom.get_annotation_value(object = tom.model, name = tName)

                        #if cType == 'CalculatedTableColumn':
                        #lakeColumn = scName.replace(' ','_')
                        #elif cType == 'Calculated':
                        pattern = r'\[([^]]+)\]'

                        matches = re.findall(pattern, scName)
                        lakeColumn = matches[0].replace(' ','')
                        try:
                            tom.model.Tables[tName].Columns[cName]
                        except:
                            tom.add_data_column(table_name = tName, column_name=cName, source_column=lakeColumn, data_type=cDataType)
                            print(f"{green_dot} The '{tName}'[{cName}] column has been added.")

                print(f"\n{green_dot} All viable calculated tables have been added to the model.")
        
        except Exception as e:
            if datetime.datetime.now() - start_time > timeout:
                break
            time.sleep(1)