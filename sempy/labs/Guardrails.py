import sempy
import sempy.fabric as fabric
import pandas as pd

def get_direct_lake_guardrails():

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_direct_lake_guardrails

    """

    url = 'https://learn.microsoft.com/power-bi/enterprise/directlake-overview'

    tables = pd.read_html(url)
    df = tables[0]
    df['Fabric SKUs'] = df['Fabric SKUs'].str.split('/')
    df = df.explode('Fabric SKUs', ignore_index=True)
    
    return df

def get_sku_size(workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_sku_size

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    dfC = fabric.list_capacities()
    dfW = fabric.list_workspaces().sort_values(by='Name', ascending=True)
    dfC.rename(columns={'Id': 'Capacity Id'}, inplace=True)
    dfCW = pd.merge(dfW, dfC[['Capacity Id', 'Sku', 'Region', 'State']], on='Capacity Id', how='inner')
    sku_value = dfCW.loc[dfCW['Name'] == workspace, 'Sku'].iloc[0]
    
    return sku_value

def get_directlake_guardrails_for_sku(sku_size: str):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_directlake_guardrails_for_sku

    """

    df = get_direct_lake_guardrails()
    filtered_df = df[df['Fabric SKUs'] == sku_size]
    
    return filtered_df