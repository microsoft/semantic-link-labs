import sempy
import sempy.fabric as fabric
import pandas as pd
from .ListFunctions import list_tables, list_annotations

def list_direct_lake_model_calc_tables(dataset: str, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#list_direct_lake_model_calc_tables

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    df = pd.DataFrame(columns=['Table Name', 'Source Expression'])

    dfP = fabric.list_partitions(dataset = dataset, workspace = workspace)
    dfP_filt = dfP[dfP['Mode'] == 'DirectLake']

    if len(dfP_filt) == 0:
        print(f"The '{dataset}' semantic model is not in Direct Lake mode.")
    else:
        dfA = list_annotations(dataset, workspace)
        dfT = list_tables(dataset, workspace)
        dfA_filt = dfA[(dfA['Object Type'] == 'Model') & (dfA['Annotation Name'].isin(dfT['Name']))]

        for i,r in dfA_filt.iterrows():
            tName = r['Annotation Name']
            se = r['Annotation Value']

            new_data = {'Table Name': tName, 'Source Expression': se}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        return df