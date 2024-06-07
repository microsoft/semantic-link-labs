import sempy
import sempy.fabric as fabric
import pandas as pd
from .HelperFunctions import create_relationship_name
from .TOM import connect_semantic_model
from typing import List, Optional, Union


def list_semantic_model_objects(dataset: str, workspace: Optional[str] = None):

    """
    Shows a list of semantic model objects.

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
        A pandas dataframe showing a list of objects in the semantic model
    """

    if workspace is None:
        workspace = fabric.resolve_workspace_name()

    df = pd.DataFrame(columns=['Parent Name', 'Object Name', 'Object Type'])
    with connect_semantic_model(dataset=dataset, workspace = workspace, readonly=True) as tom:
        for t in tom.model.Tables:
            if t.CalculationGroup is not None:
                new_data = {'Parent Name': t.Parent.Name, 'Object Name': t.Name, 'Object Type': 'Calculation Group'}
                df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                for ci in t.CalculationGroup.CalculationItems:
                    new_data = {'Parent Name': t.Name, 'Object Name': ci.Name, 'Object Type': str(ci.ObjectType)}
                    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
            elif any(str(p.SourceType) == 'Calculated' for p in t.Partitions):
                new_data = {'Parent Name': t.Parent.Name, 'Object Name': t.Name, 'Object Type': 'Calculated Table'}
                df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
            else:
                new_data = {'Parent Name': t.Parent.Name, 'Object Name': t.Name, 'Object Type': str(t.ObjectType)}
                df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
            for c in t.Columns:
                if str(c.Type) != 'RowNumber':
                    if str(c.Type) == 'Calculated':
                        new_data = {'Parent Name': c.Parent.Name, 'Object Name': c.Name, 'Object Type': 'Calculated Column'}
                        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    else:
                        new_data = {'Parent Name': c.Parent.Name, 'Object Name': c.Name, 'Object Type': str(c.ObjectType)}
                        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
            for m in t.Measures:
                new_data = {'Parent Name': m.Parent.Name, 'Object Name': m.Name, 'Object Type': str(m.ObjectType)}
                df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
            for h in t.Hierarchies:
                new_data = {'Parent Name': h.Parent.Name, 'Object Name': h.Name, 'Object Type': str(h.ObjectType)}
                df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                for l in h.Levels:
                    new_data = {'Parent Name': l.Parent.Name, 'Object Name': l.Name, 'Object Type': str(l.ObjectType)}
                    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
            for p in t.Partitions:
                new_data = {'Parent Name': p.Parent.Name, 'Object Name': p.Name, 'Object Type': str(p.ObjectType)}
                df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        for r in tom.model.Relationships:
            rName = create_relationship_name(r.FromTable.Name, r.FromColumn.Name, r.ToTable.Name, r.ToColumn.Name)
            new_data = {'Parent Name': r.Parent.Name, 'Object Name': rName, 'Object Type': str(r.ObjectType)}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        for role in tom.model.Roles:
            new_data = {'Parent Name': role.Parent.Name, 'Object Name': role.Name, 'Object Type': str(role.ObjectType)}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
            for rls in role.TablePermissions:
                new_data = {'Parent Name': role.Name, 'Object Name': rls.Name, 'Object Type': str(rls.ObjectType)}
                df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        for tr in tom.model.Cultures:
            new_data = {'Parent Name': tr.Parent.Name, 'Object Name': tr.Name, 'Object Type': str(tr.ObjectType)}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        for per in tom.model.Perspectives:
            new_data = {'Parent Name': per.Parent.Name, 'Object Name': per.Name, 'Object Type': str(per.ObjectType)}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)    

    return df


def migration_validation(dataset: str, new_dataset: str, workspace: Optional[str] = None, new_dataset_workspace: Optional[str] = None):

    """
    Shows the objects in the original semantic model and whether then were migrated successfully or not.

    Parameters
    ----------
    dataset : str
        Name of the import/DirectQuery semantic model.
    new_dataset : str
        Name of the Direct Lake semantic model.
    workspace : str, default=None
        The Fabric workspace name in which the import/DirectQuery semantic model exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    new_dataset_workspace : str
        The Fabric workspace name in which the Direct Lake semantic model will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
       A pandas dataframe showing a list of objects and whether they were successfully migrated. Also shows the % of objects which were migrated successfully.
    """

    dfA = list_semantic_model_objects(dataset = dataset, workspace = workspace)
    dfB = list_semantic_model_objects(dataset = new_dataset, workspace = new_dataset_workspace)

    def is_migrated(row):
        if row['Object Type'] == 'Calculated Table':
            return ((dfB['Parent Name'] == row['Parent Name']) & 
                    (dfB['Object Name'] == row['Object Name']) & 
                    (dfB['Object Type'].isin(['Calculated Table', 'Table']))).any()
        else:
            return ((dfB['Parent Name'] == row['Parent Name']) & 
                    (dfB['Object Name'] == row['Object Name']) & 
                    (dfB['Object Type'] == row['Object Type'])).any()
    
    dfA['Migrated'] = dfA.apply(is_migrated, axis=1)

    denom = len(dfA)
    num = len(dfA[dfA['Migrated']])
    print(f"{100 * round(num / denom,2)}% migrated")
    
    return dfA