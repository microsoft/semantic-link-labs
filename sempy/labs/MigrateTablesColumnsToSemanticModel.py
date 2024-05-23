import sempy
import sempy.fabric as fabric
import pandas as pd
import datetime, time
from .ListFunctions import list_tables
from .GetSharedExpression import get_shared_expression
from .HelperFunctions import resolve_lakehouse_name
from .Lakehouse import lakehouse_attached
from .TOM import connect_semantic_model
from sempy._utils._log import log

green_dot = '\U0001F7E2'
yellow_dot = '\U0001F7E1'
red_dot = '\U0001F534'
in_progress = '⌛'

@log
def migrate_tables_columns_to_semantic_model(dataset: str, new_dataset: str, workspace: str | None = None, new_dataset_workspace: str | None = None, lakehouse: str | None = None, lakehouse_workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#migrate_tables_columns_to_semantic_model

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

    # Check that lakehouse is attached to the notebook
    lakeAttach = lakehouse_attached()

    # Run if lakehouse is attached to the notebook or a lakehouse & lakehouse workspace are specified
    if lakeAttach or (lakehouse is not None and lakehouse_workspace is not None):
        shEx = get_shared_expression(lakehouse, lakehouse_workspace)

        dfC = fabric.list_columns(dataset = dataset, workspace = workspace)
        dfT = list_tables(dataset, workspace)
        dfT.rename(columns={'Type': 'Table Type'}, inplace=True)
        dfC = pd.merge(dfC, dfT[['Name', 'Table Type']], left_on = 'Table Name', right_on = 'Name', how='left')
        dfT_filt = dfT[dfT['Table Type'] == 'Table']
        dfC_filt = dfC[(dfC['Table Type'] == 'Table') & ~(dfC['Column Name'].str.startswith('RowNumber-')) & (dfC['Type'] != 'Calculated')]

        print(f"{in_progress} Updating '{new_dataset}' based on '{dataset}'...")
        start_time = datetime.datetime.now()
        timeout = datetime.timedelta(minutes=1)
        success = False

        while not success:
            try:
                with connect_semantic_model(dataset=new_dataset, readonly=False, workspace=new_dataset_workspace) as tom:
                    success = True
                    try:
                        tom.model.Expressions['DatabaseQuery']
                    except:
                        tom.add_expression('DatabaseQuery', expression = shEx)
                        print(f"{green_dot} The 'DatabaseQuery' expression has been added.")

                    for i, r in dfT_filt.iterrows():
                        tName = r['Name']
                        tDC = r['Data Category']
                        tHid = bool(r['Hidden'])
                        tDesc = r['Description']

                        try:
                            tom.model.Tables[tName]
                        except:
                            tom.add_table(name = tName, description=tDesc, data_category=tDC, hidden=tHid)
                            tom.add_entity_partition(table_name = tName, entity_name = tName.replace(' ','_'))
                            print(f"{green_dot} The '{tName}' table has been added.")
                    
                    for i, r in dfC_filt.iterrows():
                        tName = r['Table Name']
                        cName = r['Column Name']
                        scName = r['Source'].replace(' ','_')
                        cHid = bool(r['Hidden'])
                        cDataType = r['Data Type']

                        try:
                            tom.model.Tables[tName].Columns[cName]
                        except:
                            tom.add_data_column(table_name=tName, column_name=cName, source_column=scName, hidden=cHid, data_type=cDataType)
                            print(f"{green_dot} The '{tName}'[{cName}] column has been added.")

                    print(f"\n{green_dot} All regular tables and columns have been added to the '{new_dataset}' semantic model.")
            except Exception as e:
                if datetime.datetime.now() - start_time > timeout:
                    break
                time.sleep(1)
    else:
        print(f"{red_dot} Lakehouse not attached to notebook and lakehouse/lakehouse_workspace are not specified. Please add your lakehouse to this notebook or specify the lakehouse/lakehouse_workspace parameters.")
        print(f"To attach a lakehouse to a notebook, go to the the 'Explorer' window to the left, click 'Lakehouses' to add your lakehouse to this notebook")
        print(f"\nLearn more here: https://learn.microsoft.com/fabric/data-engineering/lakehouse-notebook-explore#add-or-remove-a-lakehouse")




        