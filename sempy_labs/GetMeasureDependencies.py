import sempy
import sempy.fabric as fabric
import pandas as pd
from .HelperFunctions import format_dax_object_name
from typing import List, Optional, Union

def get_measure_dependencies(dataset: str, workspace: Optional[str] = None):

    """
    Shows all dependencies for all measures in a semantic model.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Shows all dependencies for all measures in the semantic model.
    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    dep = fabric.evaluate_dax(dataset = dataset, workspace = workspace, dax_string = 
        """
        SELECT 
         [TABLE] AS [Table Name]
        ,[OBJECT] AS [Object Name]
        ,[OBJECT_TYPE] AS [Object Type]
        ,[REFERENCED_TABLE] AS [Referenced Table]
        ,[REFERENCED_OBJECT] AS [Referenced Object]
        ,[REFERENCED_OBJECT_TYPE] AS [Referenced Object Type]
        FROM $SYSTEM.DISCOVER_CALC_DEPENDENCY
        WHERE [OBJECT_TYPE] = 'MEASURE'
        """)

    dep['Object Type'] = dep['Object Type'].str.capitalize()
    dep['Referenced Object Type'] = dep['Referenced Object Type'].str.capitalize()

    dep['Full Object Name'] =  format_dax_object_name(dep['Table Name'], dep['Object Name'])
    dep['Referenced Full Object Name'] = format_dax_object_name(dep['Referenced Table'], dep['Referenced Object'])
    dep['Parent Node'] = dep['Object Name']

    df = dep

    df['Done'] = df.apply(lambda row: False if row['Referenced Object Type'] == 'Measure' else True, axis=1)

    while(any(df['Done'] == False)):
        for i, r in df.iterrows():
            rObjFull = r['Referenced Full Object Name']
            rObj = r['Referenced Object']
            if r['Done'] == False:
                dep_filt = dep[dep['Full Object Name'] == rObjFull]

                for index, dependency in dep_filt.iterrows():
                    d = True
                    if dependency[5] == 'Measure':
                        d = False
                        df = pd.concat([df, pd.DataFrame([{'Table Name': r['Table Name'], 'Object Name': r['Object Name'], 'Object Type': r['Object Type']
                        , 'Referenced Object': dependency[4], 'Referenced Table': dependency[3], 'Referenced Object Type': dependency[5], 'Done': d, 'Full Object Name': r['Full Object Name'], 'Referenced Full Object Name': dependency[7],'Parent Node': rObj }])], ignore_index=True)
                    else:
                        df = pd.concat([df, pd.DataFrame([{'Table Name': r['Table Name'], 'Object Name': r['Object Name'], 'Object Type': r['Object Type']
                    , 'Referenced Object': dependency[5], 'Referenced Table': dependency[4], 'Referenced Object Type': dependency[6], 'Done': d, 'Full Object Name': r['Full Object Name'], 'Referenced Full Object Name': dependency[7],'Parent Node': rObj }])], ignore_index=True)

            df.loc[i, 'Done'] = True

    df = df.drop(['Done','Full Object Name','Referenced Full Object Name'], axis=1)

    return df

def get_model_calc_dependencies(dataset: str, workspace: Optional[str] = None):

    """
    Shows all dependencies for all objects in a semantic model.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Shows all dependencies for all objects in the semantic model.
    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    dep = fabric.evaluate_dax(dataset = dataset, workspace = workspace, dax_string =
        """
        SELECT
        [TABLE] AS [Table Name]
        ,[OBJECT] AS [Object Name]
        ,[OBJECT_TYPE] AS [Object Type]
        ,[EXPRESSION] AS [Expression]
        ,[REFERENCED_TABLE] AS [Referenced Table]
        ,[REFERENCED_OBJECT] AS [Referenced Object]
        ,[REFERENCED_OBJECT_TYPE] AS [Referenced Object Type]
        FROM $SYSTEM.DISCOVER_CALC_DEPENDENCY        
        """)

    dep['Object Type'] = dep['Object Type'].str.replace('_',' ').str.title()
    dep['Referenced Object Type'] = dep['Referenced Object Type'].str.replace('_',' ').str.title()

    dep['Full Object Name'] =  format_dax_object_name(dep['Table Name'], dep['Object Name'])
    dep['Referenced Full Object Name'] = format_dax_object_name(dep['Referenced Table'], dep['Referenced Object'])
    dep['Parent Node'] = dep['Object Name']

    df = dep

    objs = ['Measure','Calc Column', 'Calculation Item', 'Calc Table']

    df['Done'] = df.apply(lambda row: False if row['Referenced Object Type'] in objs else True, axis=1)

    while(any(df['Done'] == False)):
        for i, r in df.iterrows():
            rObjFull = r['Referenced Full Object Name']
            rObj = r['Referenced Object']
            if r['Done'] == False:
                dep_filt = dep[dep['Full Object Name'] == rObjFull]

                for index, dependency in dep_filt.iterrows():
                    d = True                    
                    if dependency[5] in objs:
                        d = False
                        df = pd.concat([df, pd.DataFrame([{'Table Name': r['Table Name'], 'Object Name': r['Object Name'], 'Object Type': r['Object Type']
                        , 'Referenced Object': dependency[4], 'Referenced Table': dependency[3], 'Referenced Object Type': dependency[5], 'Done': d, 'Full Object Name': r['Full Object Name'], 'Referenced Full Object Name': dependency[7],'Parent Node': rObj }])], ignore_index=True)
                    else:
                        df = pd.concat([df, pd.DataFrame([{'Table Name': r['Table Name'], 'Object Name': r['Object Name'], 'Object Type': r['Object Type']
                    , 'Referenced Object': dependency[5], 'Referenced Table': dependency[4], 'Referenced Object Type': dependency[6], 'Done': d, 'Full Object Name': r['Full Object Name'], 'Referenced Full Object Name': dependency[7],'Parent Node': rObj }])], ignore_index=True)

            df.loc[i, 'Done'] = True

    df = df.drop(['Done'], axis=1)

    return df