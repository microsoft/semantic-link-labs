import sempy
import sempy.fabric as fabric
import numpy as np

def check_fallback_reason(dataset: str, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/m-kovalsky/fabric_cat_tools?tab=readme-ov-file#check_fallback_reason

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    dfP = fabric.list_partitions(dataset = dataset, workspace = workspace)
    dfP_filt = dfP[dfP['Mode'] == 'DirectLake']
    
    if len(dfP_filt) == 0:
        print(f"The '{dataset}' semantic model is not in Direct Lake. This function is only applicable to Direct Lake semantic models.")
    else:
        df = fabric.evaluate_dax(dataset = dataset,workspace = workspace,
        dax_string = 
        """
        SELECT [TableName] AS [Table Name],[FallbackReason] AS [FallbackReasonID]
        FROM $SYSTEM.TMSCHEMA_DELTA_TABLE_METADATA_STORAGES
        """    
        )        

        value_mapping = {
            0: 'No reason for fallback',
            1: 'This table is not framed',
            2: 'This object is a view in the lakehouse',
            3: 'The table does not exist in the lakehouse',
            4: 'Transient error',
            5: 'Using OLS will result in fallback to DQ',
            6: 'Using RLS will result in fallback to DQ'
        }

        # Create a new column based on the mapping
        df['Fallback Reason Detail'] = np.vectorize(value_mapping.get)(df['FallbackReasonID'])
        
        return df