import sempy
import sempy.fabric as fabric
import pandas as pd
import json, time
from pyspark.sql import SparkSession
from .GetDirectLakeLakehouse import get_direct_lake_lakehouse

def get_object_level_security(dataset: str, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_object_level_security

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    tom_server = fabric.create_tom_server(readonly=True, workspace=workspace)
    m = tom_server.Databases.GetByName(dataset).Model

    df = pd.DataFrame(columns=['Role Name', 'Object Type', 'Table Name', 'Object Name'])

    for r in m.Roles:
        for tp in r.TablePermissions:
            if len(tp.FilterExpression) == 0:
                columnCount = len(tp.ColumnPermissions)
                objectType = "Table"
                if columnCount == 0:
                    new_data = {'Role Name': r.Name, 'Object Type': objectType, 'Table Name': tp.Name, 'Object Name': tp.Name}
                    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                else:
                    objectType = "Column"
                    for cp in tp.ColumnPermissions:
                        new_data = {'Role Name': r.Name, 'Object Type': objectType, 'Table Name': tp.Name, 'Object Name': cp.Name}
                        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

def list_tables(dataset: str, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#list_tables

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    tom_server = fabric.create_tom_server(readonly=True, workspace=workspace)
    m = tom_server.Databases.GetByName(dataset).Model

    df = pd.DataFrame(columns=['Name', 'Type', 'Hidden', 'Data Category', 'Description', 'Refresh Policy', 'Source Expression'])

    for t in m.Tables:
        tableType = "Table"
        rPolicy = bool(t.RefreshPolicy)
        sourceExpression = None
        if str(t.CalculationGroup) != "None":
            tableType = "Calculation Group"
        else:
            for p in t.Partitions:
                if str(p.SourceType) == "Calculated":
                    tableType = "Calculated Table"

        if rPolicy:
            sourceExpression = t.RefreshPolicy.SourceExpression

        new_data = {'Name': t.Name, 'Type': tableType, 'Hidden': t.IsHidden, 'Data Category': t.DataCategory, 'Description': t.Description, 'Refresh Policy': rPolicy, 'Source Expression': sourceExpression}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

def list_annotations(dataset: str, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#list_annotations

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    tom_server = fabric.create_tom_server(readonly=True, workspace=workspace)
    m = tom_server.Databases.GetByName(dataset).Model

    df = pd.DataFrame(columns=['Object Name', 'Parent Object Name', 'Object Type', 'Annotation Name', 'Annotation Value'])

    mName = m.Name
    for a in m.Annotations:        
        objectType = 'Model'
        aName = a.Name
        aValue = a.Value
        new_data = {'Object Name': mName, 'Parent Object Name': "N/A", 'Object Type': objectType,'Annotation Name': aName, 'Annotation Value': aValue}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    for t in m.Tables:
        objectType = 'Table'
        tName = t.Name
        for ta in t.Annotations:
            taName = ta.Name
            taValue = ta.Value
            new_data = {'Object Name': tName, 'Parent Object Name': mName, 'Object Type': objectType,'Annotation Name': taName, 'Annotation Value': taValue}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        for p in t.Partitions:
            pName = p.Name
            objectType = 'Partition'
            for pa in p.Annotations:
                paName = paName
                paValue = paValue
                new_data = {'Object Name': pName, 'Parent Object Name': tName, 'Object Type': objectType,'Annotation Name': paName, 'Annotation Value': paValue}
                df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        for c in t.Columns:
            objectType = 'Column'
            cName = c.Name            
            for ca in c.Annotations:
                caName = ca.Name
                caValue = ca.Value
                new_data = {'Object Name': cName, 'Parent Object Name': tName, 'Object Type': objectType,'Annotation Name': caName, 'Annotation Value': caValue}
                df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        for ms in t.Measures:
            objectType = 'Measure'
            measName = ms.Name
            for ma in ms.Annotations:
                maName = ma.Name
                maValue = ma.Value
                new_data = {'Object Name': measName, 'Parent Object Name': tName, 'Object Type': objectType,'Annotation Name': maName, 'Annotation Value': maValue}
                df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        for h in t.Hierarchies:
            objectType = 'Hierarchy'
            hName = h.Name
            for ha in h.Annotations:
                haName = ha.Name
                haValue = ha.Value
                new_data = {'Object Name': hName, 'Parent Object Name': tName, 'Object Type': objectType,'Annotation Name': haName, 'Annotation Value': haValue}
                df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    for d in m.DataSources:
        dName = d.Name
        objectType = 'Data Source'
        for da in d.Annotations:
            daName = da.Name
            daValue = da.Value
            new_data = {'Object Name': dName, 'Parent Object Name': mName, 'Object Type': objectType,'Annotation Name': daName, 'Annotation Value': daValue}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    for r in m.Relationships:
        rName = r.Name
        objectType = 'Relationship'
        for ra in r.Annotations:
            raName = ra.Name
            raValue = ra.Value
            new_data = {'Object Name': rName, 'Parent Object Name': mName, 'Object Type': objectType,'Annotation Name': raName, 'Annotation Value': raValue}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    for cul in m.Cultures:
        culName = cul.Name
        objectType = 'Translation'
        for cula in cul.Annotations:
            culaName = cula.Name
            culaValue = cula.Value
            new_data = {'Object Name': culName, 'Parent Object Name': mName, 'Object Type': objectType,'Annotation Name': culaName, 'Annotation Value': culaValue}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    for e in m.Expressions:
        eName = e.Name
        objectType = 'Expression'
        for ea in e.Annotations:
            eaName = ea.Name
            eaValue = ea.Value
            new_data = {'Object Name': eName, 'Parent Object Name': mName, 'Object Type': objectType,'Annotation Name': eaName, 'Annotation Value': eaValue}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    for per in m.Perspectives:
        perName = per.Name
        objectType = 'Perspective'
        for pera in per.Annotations:
            peraName = pera.Name
            peraValue = pera.Value
            new_data = {'Object Name': perName, 'Parent Object Name': mName, 'Object Type': objectType,'Annotation Name': peraName, 'Annotation Value': peraValue}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    for rol in m.Roles:
        rolName = rol.Name
        objectType = 'Role'
        for rola in rol.Annotations:
            rolaName = rola.Name
            rolaValue = rola.Value
            new_data = {'Object Name': rolName, 'Parent Object Name': mName, 'Object Type': objectType,'Annotation Name': rolaName, 'Annotation Value': rolaValue}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

def list_columns(dataset: str, workspace: str | None = None, lakehouse: str | None = None, lakehouse_workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#list_columns

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    dfP = fabric.list_partitions(dataset = dataset, workspace = workspace)

    isDirectLake = any(r['Mode'] == 'DirectLake' for i, r in dfP.iterrows())

    dfC = fabric.list_columns(dataset = dataset, workspace = workspace)

    if isDirectLake:
        dfC['Column Cardinality'] = None
        sql_statements = []
        lakeID, lakeName = get_direct_lake_lakehouse(dataset = dataset, workspace = workspace, lakehouse = lakehouse, lakehouse_workspace = lakehouse_workspace)

        for table_name in dfC['Table Name'].unique():
            print(f"Gathering stats for table: '{table_name}'...")
            query = 'SELECT '
            
            columns_in_table = dfC.loc[dfC['Table Name'] == table_name, 'Column Name'].unique()
            
            # Loop through columns within those tables
            for column_name in columns_in_table:
                scName = dfC.loc[(dfC['Table Name'] == table_name) & (dfC['Column Name'] == column_name), 'Source'].iloc[0]
                lakeTName = dfC.loc[(dfC['Table Name'] == table_name) & (dfC['Column Name'] == column_name), 'Query'].iloc[0]

                # Build the query to be executed dynamically
                query = query + f"COUNT(DISTINCT({scName})) AS {scName}, "
            
            query = query[:-2]
            query = query + f" FROM {lakehouse}.{lakeTName}"
            sql_statements.append((table_name, query))

            spark = SparkSession.builder.getOrCreate()

        for o in sql_statements:
            tName = o[0]
            query = o[1]

            # Run the query
            df = spark.sql(query)
            
            for column in df.columns:
                x = df.collect()[0][column]
                for i, r in dfC.iterrows():
                    if r['Table Name'] == tName and r['Source'] == column:
                        dfC.at[i, 'Column Cardinality'] = x

        # Remove column added temporarily
        dfC.drop(columns=['Query'], inplace=True)

    return dfC

def list_dashboards(workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#list_dashboards

    """

    df = pd.DataFrame(columns=['Dashboard ID', 'Dashboard Name', 'Read Only', 'Web URL', 'Embed URL', 'Data Classification', 'Users', 'Subscriptions'])

    if workspace == 'None':
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resovle_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    client = fabric.PowerBIRestClient()
    response = client.get(f"/v1.0/myorg/groups/{workspace_id}/dashboards")

    for v in response.json()['value']:
        dashboardID = v['id']
        displayName = v['displayName']
        isReadOnly = v['isReadOnly']
        webURL = v['webUrl']
        embedURL = v['embedUrl']
        dataClass = v['dataClassification']
        users = v['users']
        subs = v['subscriptions']

        new_data = {'Dashboard ID': dashboardID, 'Dashboard Name': displayName, 'Read Only': isReadOnly, 'Web URL': webURL, 'Embed URL': embedURL, 'Data Classification': dataClass, 'Users': [users], 'Subscriptions': [subs]}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True) 

    df['Read Only'] = df['Read Only'].astype(bool)

    return df

def list_lakehouses(workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#list_lakehouses

    """

    df = pd.DataFrame(columns=['Lakehouse Name', 'Lakehouse ID', 'Description', 'OneLake Tables Path', 'OneLake Files Path', 'SQL Endpoint Connection String', 'SQL Endpoint ID', 'SQL Endpoint Provisioning Status'])

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/lakehouses/")
    
    for v in response.json()['value']:
        lakehouseId = v['id']
        lakehouseName = v['displayName']
        lakehouseDesc = v['description']
        prop = v['properties']
        oneLakeTP = prop['oneLakeTablesPath']
        oneLakeFP = prop['oneLakeFilesPath']
        sqlEPProp = prop['sqlEndpointProperties']
        sqlEPCS = sqlEPProp['connectionString']
        sqlepid = sqlEPProp['id']
        sqlepstatus = sqlEPProp['provisioningStatus']

        new_data = {'Lakehouse Name': lakehouseName, 'Lakehouse ID': lakehouseId, 'Description': lakehouseDesc, 'OneLake Tables Path': oneLakeTP, 'OneLake Files Path': oneLakeFP, 'SQL Endpoint Connection String': sqlEPCS, 'SQL Endpoint ID': sqlepid, 'SQL Endpoint Provisioning Status': sqlepstatus}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

def list_warehouses(workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#list_warehouses

    """

    df = pd.DataFrame(columns=['Warehouse Name', 'Warehouse ID', 'Description', 'Connection Info', 'Created Date', 'Last Updated Time'])

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/warehouses/")
    
    for v in response.json()['value']:
        warehouse_id = v['id']
        warehouse_name = v['displayName']
        desc = v['description']
        prop = v['properties']
        connInfo = prop['connectionInfo']
        createdDate = prop['createdDate']
        lastUpdate = prop['lastUpdatedTime']

        new_data = {'Warehouse Name': warehouse_name, 'Warehouse ID': warehouse_id, 'Description': desc, 'Connection Info': connInfo, 'Created Date': createdDate, 'Last Updated Time': lastUpdate}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

def list_sqlendpoints(workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#list_sqlendpoints

    """

    df = pd.DataFrame(columns=['SQL Endpoint ID', 'SQL Endpoint Name', 'Description'])

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/sqlEndpoints/")
    
    for v in response.json()['value']:
        sql_id = v['id']
        lake_name = v['displayName']
        desc = v['description']

        new_data = {'SQL Endpoint ID': sql_id, 'SQL Endpoint Name': lake_name, 'Description': desc}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

def list_mirroredwarehouses(workspace: str | None = None):

    df = pd.DataFrame(columns=['Mirrored Warehouse', 'Mirrored Warehouse ID', 'Description'])

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/mirroredWarehouses/")
    
    for v in response.json()['value']:
        mirr_id = v['id']
        dbname = v['displayName']
        desc = v['description']

        new_data = {'Mirrored Warehouse': dbname, 'Mirrored Warehouse ID': mirr_id, 'Description': desc}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

def list_kqldatabases(workspace: str | None = None):

    df = pd.DataFrame(columns=['KQL Database Name', 'KQL Database ID', 'Description', 'Parent Eventhouse Item ID', 'Query Service URI', 'Ingestion Service URI', 'Kusto Database Type'])

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/kqlDatabases/")
    
    for v in response.json()['value']:
        kql_id = v['id']
        kql_name = v['displayName']
        desc = v['description']
        prop = v['properties']
        eventId = prop['parentEventhouseItemId']
        qsURI = prop['queryServiceUri']
        isURI = prop['ingestionServiceUri']
        dbType = prop['kustoDatabaseType']

        new_data = {'KQL Database Name': kql_name, 'KQL Database ID': kql_id, 'Description': desc, 'Parent Eventhouse Item ID': eventId, 'Query Service URI': qsURI, 'Ingestion Service URI': isURI, 'Kusto Database Type': dbType}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

def list_kqlquerysets(workspace: str | None = None):

    df = pd.DataFrame(columns=['KQL Queryset Name', 'KQL Queryset ID', 'Description'])

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/kqlQuerysets/")
    
    for v in response.json()['value']:
        kql_id = v['id']
        kql_name = v['displayName']
        desc = v['description']

        new_data = {'KQL Queryset Name': kql_name, 'KQL Queryset ID': kql_id, 'Description': desc}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

def list_mlmodels(workspace: str | None = None):

    df = pd.DataFrame(columns=['ML Model Name', 'ML Model ID', 'Description'])

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/mlModels/")
    
    for v in response.json()['value']:
        model_id = v['id']
        modelName = v['displayName']
        desc = v['description']

        new_data = {'ML Model Name': modelName, 'ML Model ID': model_id, 'Description': desc}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

def list_eventstreams(workspace: str | None = None):

    df = pd.DataFrame(columns=['Eventstream Name', 'Eventstream ID', 'Description'])

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/eventstreams/")
    
    for v in response.json()['value']:
        model_id = v['id']
        modelName = v['displayName']
        desc = v['description']

        new_data = {'Eventstream Name': modelName, 'Eventstream ID': model_id, 'Description': desc}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

def list_datapipelines(workspace: str | None = None):

    df = pd.DataFrame(columns=['Data Pipeline Name', 'Data Pipeline ID', 'Description'])

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/dataPipelines/")
    
    for v in response.json()['value']:
        model_id = v['id']
        modelName = v['displayName']
        desc = v['description']

        new_data = {'Data Pipeline Name': modelName, 'Data Pipeline ID': model_id, 'Description': desc}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

def list_mlexperiments(workspace: str | None = None):

    df = pd.DataFrame(columns=['ML Experiment Name', 'ML Experiment ID', 'Description'])

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/mlExperiments/")
    
    for v in response.json()['value']:
        model_id = v['id']
        modelName = v['displayName']
        desc = v['description']

        new_data = {'ML Experiment Name': modelName, 'ML Experiment ID': model_id, 'Description': desc}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

def list_datamarts(workspace: str | None = None):

    df = pd.DataFrame(columns=['Datamart Name', 'Datamart ID', 'Description'])

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/datamarts/")
    
    for v in response.json()['value']:
        model_id = v['id']
        modelName = v['displayName']
        desc = v['description']

        new_data = {'Datamart Name': modelName, 'Datamart ID': model_id, 'Description': desc}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

def create_warehouse(warehouse: str, description: str | None = None, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#create_warehouse

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)    

    if description == None:
        request_body = {
        "displayName": warehouse
        }
    else:
        request_body = {
            "displayName": warehouse,
            "description": description
        }

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/warehouses/", json=request_body)

    if response.status_code == 201:
        print(f"The '{warehouse}' warehouse has been created within the '{workspace}' workspace.")
    elif response.status_code == 202:
        operationId = response.headers['x-ms-operation-id']
        response = client.get(f"/v1/operations/{operationId}")
        response_body = json.loads(response.content) 
        while response_body['status'] != 'Succeeded':
            time.sleep(3)
            response = client.get(f"/v1/operations/{operationId}")
            response_body = json.loads(response.content)
        response = client.get(f"/v1/operations/{operationId}/result")
        print(f"The '{warehouse}' warehouse has been created within the '{workspace}' workspace.")
    else:
        print(f"ERROR: Failed to create the '{warehouse}' warehouse within the '{workspace}' workspace.")

def update_item(item_type: str, current_name: str, new_name: str, description: str | None = None, workspace: str | None = None):

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#update_item

    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    itemTypes = {
    'DataPipeline': 'dataPipelines',
    'Eventstream': 'eventstreams',
    'KQLDatabase': 'kqlDatabases',
    'KQLQueryset': 'kqlQuerysets',
    'Lakehouse': 'lakehouses',
    'MLExperiment': 'mlExperiments',
    'MLModel': 'mlModels',
    'Notebook': 'notebooks',
    'Warehouse': 'warehouses',
    }

    item_type = item_type.replace(' ','').capitalize()

    if item_type not in itemTypes.keys():
        print(f"The '{item_type}' is not a valid item type. ")
        return
    
    itemType = itemTypes[item_type]

    dfI = fabric.list_items(workspace = workspace, type = item_type)
    dfI_filt = dfI[(dfI['Display Name'] == current_name)]

    if len(dfI_filt) == 0:
        print(f"The '{current_name}' {item_type} does not exist within the '{workspace}' workspace.")
        return
    
    itemId = dfI_filt['Id'].iloc[0]

    if description == None:
        request_body = {
        "displayName": new_name
        }
    else:
        request_body = {
            "displayName": new_name,
            "description": description
        }

    client = fabric.FabricRestClient()
    response = client.patch(f"/v1/workspaces/{workspace_id}/{itemType}/{itemId}", json=request_body)

    if response.status_code == 200:
        if description == None:
            print(f"The '{current_name}' {item_type} within the '{workspace}' workspace has been updated to be named '{new_name}'")
        else:
            print(f"The '{current_name}' {item_type} within the '{workspace}' workspace has been updated to be named '{new_name}' and have a description of '{description}'")
    else:
        print(f"ERROR: The '{current_name}' {item_type} within the '{workspace}' workspace was not updateds.")

def list_relationships(dataset: str, workspace: str | None = None, extended: bool = False):

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    
    dfR = fabric.list_relationships(dataset = dataset, workspace = workspace)

    if extended:
        # Used to map the Relationship IDs
        rel = fabric.evaluate_dax(
                dataset = dataset, workspace = workspace, dax_string = 
                """
                SELECT
                [ID] AS [RelationshipID]
                ,[Name] 
                FROM $SYSTEM.TMSCHEMA_RELATIONSHIPS
                """)

        # USED_SIZE shows the Relationship Size where TABLE_ID starts with R$
        cs = fabric.evaluate_dax(
                dataset = dataset, workspace = workspace, dax_string = 
                """
                SELECT 
                [TABLE_ID]
                ,[USED_SIZE]
                FROM $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMN_SEGMENTS
                """)

        def parse_value(text):
            ind = text.rfind('(') + 1
            output = text[ind:]
            output = output[:-1]
            return output

        cs['RelationshipID'] = cs['TABLE_ID'].apply(parse_value).astype('uint64')
        relcs = pd.merge(cs[['RelationshipID', 'TABLE_ID', 'USED_SIZE']], rel, on='RelationshipID', how='left')

        dfR['Used Size'] = None
        for i, r in dfR.iterrows():
            relName = r['Relationship Name']

            filtered_cs = relcs[(relcs['Name'] == relName) & (relcs['TABLE_ID'].str.startswith("R$"))]
            sumval = filtered_cs['USED_SIZE'].sum()
            dfR.at[i, 'Used Size'] = sumval

        dfR['Used Size'] = dfR['Used Size'].astype('int')
    
    return dfR

def list_dataflow_storage_accounts():

    """
    
    Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#list_dataflow_storage_accounts

    """

    df = pd.DataFrame(columns=['Dataflow Storage Account ID', 'Dataflow Storage Account Name', 'Enabled'])
    client = fabric.PowerBIRestClient()
    response = client.get(f"/v1.0/myorg/dataflowStorageAccounts")
    
    for v in response.json()['value']:
        dfsaId = v['id']
        dfsaName = v['name']
        isEnabled = v['isEnabled']
        
        new_data = {'Dataflow Storage Account ID': dfsaId, 'Dataflow Storage Account Name': dfsaName, 'Enabled': isEnabled}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df['Enabled'] = df['Enabled'].astype(bool)

    return df

def list_kpis(dataset, workspace = None):

    from .TOM import connect_semantic_model

    with connect_semantic_model(dataset = dataset, workspace = workspace, readonly = True) as tom:

        df = pd.DataFrame(columns=['Table Name', 'Measure Name', 'Target Expression', 'Target Format String', 'Target Description', 'Status Expression', 'Status Graphic', 'Status Description', 'Trend Expression', 'Trend Graphic', 'Trend Description'])

        for t in tom.model.Tables:
            for m in t.Measures:
                if m.KPI is not None:
                    new_data = {'Table Name': t.Name, 'Measure Name': m.Name, 'Target Expression': m.KPI.TargetExpression, 'Target Format String': m.KPI.TargetFormatString, 'Target Description': m.KPI.TargetDescription, 'Status Graphic': m.KPI.StatusGraphic, 'Status Expression': m.KPI.StatusExpression, 'Status Description': m.KPI.StatusDescription, 'Trend Expression': m.KPI.TrendExpression, 'Trend Graphic': m.KPI.TrendGraphic, 'Trend Description': m.KPI.TrendDescription}
                    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        return df
    
def list_workspace_role_assignments(workspace: str | None = None):

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    df = pd.DataFrame(columns=['User Name', 'User Email', 'Role Name', 'Type'])

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/roleAssignments")

    for i in response.json()['value']:
        user_name = i['principal']['displayName']
        role_name = i['role']
        user_email = i['principal']['userDetails']['userPrincipalName']
        user_type = i['principal']['type']

        new_data = {'User Name': user_name, 'Role Name': role_name, 'Type': user_type, 'User Email': user_email}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df