import sempy
import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id, 
    resolve_lakehouse_name, 
    create_relationship_name, 
    resolve_lakehouse_id)
import pandas as pd
import json, time
from pyspark.sql import SparkSession
from typing import Optional

def get_object_level_security(dataset: str, workspace: Optional[str] = None):
    """
    Shows the object level security for the semantic model.

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
        A pandas dataframe showing the object level security for the semantic model.
    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    tom_server = fabric.create_tom_server(readonly=True, workspace=workspace)
    m = tom_server.Databases.GetByName(dataset).Model

    df = pd.DataFrame(columns=["Role Name", "Object Type", "Table Name", "Object Name"])

    for r in m.Roles:
        for tp in r.TablePermissions:
            if len(tp.FilterExpression) == 0:
                columnCount = len(tp.ColumnPermissions)
                objectType = "Table"
                if columnCount == 0:
                    new_data = {
                        "Role Name": r.Name,
                        "Object Type": objectType,
                        "Table Name": tp.Name,
                        "Object Name": tp.Name,
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )
                else:
                    objectType = "Column"
                    for cp in tp.ColumnPermissions:
                        new_data = {
                            "Role Name": r.Name,
                            "Object Type": objectType,
                            "Table Name": tp.Name,
                            "Object Name": cp.Name,
                        }
                        df = pd.concat(
                            [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                        )

    return df


def list_tables(dataset: str, workspace: Optional[str] = None):
    """
    Shows a semantic model's tables and their properties.

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
        A pandas dataframe showing the semantic model's tables and their properties.
    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    tom_server = fabric.create_tom_server(readonly=True, workspace=workspace)
    m = tom_server.Databases.GetByName(dataset).Model

    df = pd.DataFrame(
        columns=[
            "Name",
            "Type",
            "Hidden",
            "Data Category",
            "Description",
            "Refresh Policy",
            "Source Expression",
        ]
    )

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

        new_data = {
            "Name": t.Name,
            "Type": tableType,
            "Hidden": t.IsHidden,
            "Data Category": t.DataCategory,
            "Description": t.Description,
            "Refresh Policy": rPolicy,
            "Source Expression": sourceExpression,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_annotations(dataset: str, workspace: Optional[str] = None):
    """
    Shows a semantic model's annotations and their properties.

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
        A pandas dataframe showing the semantic model's annotations and their properties.
    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    tom_server = fabric.create_tom_server(readonly=True, workspace=workspace)
    m = tom_server.Databases.GetByName(dataset).Model

    df = pd.DataFrame(
        columns=[
            "Object Name",
            "Parent Object Name",
            "Object Type",
            "Annotation Name",
            "Annotation Value",
        ]
    )

    mName = m.Name
    for a in m.Annotations:
        objectType = "Model"
        aName = a.Name
        aValue = a.Value
        new_data = {
            "Object Name": mName,
            "Parent Object Name": "N/A",
            "Object Type": objectType,
            "Annotation Name": aName,
            "Annotation Value": aValue,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    for t in m.Tables:
        objectType = "Table"
        tName = t.Name
        for ta in t.Annotations:
            taName = ta.Name
            taValue = ta.Value
            new_data = {
                "Object Name": tName,
                "Parent Object Name": mName,
                "Object Type": objectType,
                "Annotation Name": taName,
                "Annotation Value": taValue,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        for p in t.Partitions:
            pName = p.Name
            objectType = "Partition"
            for pa in p.Annotations:
                paName = pa.Name
                paValue = pa.Value
                new_data = {
                    "Object Name": pName,
                    "Parent Object Name": tName,
                    "Object Type": objectType,
                    "Annotation Name": paName,
                    "Annotation Value": paValue,
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
        for c in t.Columns:
            objectType = "Column"
            cName = c.Name
            for ca in c.Annotations:
                caName = ca.Name
                caValue = ca.Value
                new_data = {
                    "Object Name": cName,
                    "Parent Object Name": tName,
                    "Object Type": objectType,
                    "Annotation Name": caName,
                    "Annotation Value": caValue,
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
        for ms in t.Measures:
            objectType = "Measure"
            measName = ms.Name
            for ma in ms.Annotations:
                maName = ma.Name
                maValue = ma.Value
                new_data = {
                    "Object Name": measName,
                    "Parent Object Name": tName,
                    "Object Type": objectType,
                    "Annotation Name": maName,
                    "Annotation Value": maValue,
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
        for h in t.Hierarchies:
            objectType = "Hierarchy"
            hName = h.Name
            for ha in h.Annotations:
                haName = ha.Name
                haValue = ha.Value
                new_data = {
                    "Object Name": hName,
                    "Parent Object Name": tName,
                    "Object Type": objectType,
                    "Annotation Name": haName,
                    "Annotation Value": haValue,
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
    for d in m.DataSources:
        dName = d.Name
        objectType = "Data Source"
        for da in d.Annotations:
            daName = da.Name
            daValue = da.Value
            new_data = {
                "Object Name": dName,
                "Parent Object Name": mName,
                "Object Type": objectType,
                "Annotation Name": daName,
                "Annotation Value": daValue,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    for r in m.Relationships:
        rName = r.Name
        objectType = "Relationship"
        for ra in r.Annotations:
            raName = ra.Name
            raValue = ra.Value
            new_data = {
                "Object Name": rName,
                "Parent Object Name": mName,
                "Object Type": objectType,
                "Annotation Name": raName,
                "Annotation Value": raValue,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    for cul in m.Cultures:
        culName = cul.Name
        objectType = "Translation"
        for cula in cul.Annotations:
            culaName = cula.Name
            culaValue = cula.Value
            new_data = {
                "Object Name": culName,
                "Parent Object Name": mName,
                "Object Type": objectType,
                "Annotation Name": culaName,
                "Annotation Value": culaValue,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    for e in m.Expressions:
        eName = e.Name
        objectType = "Expression"
        for ea in e.Annotations:
            eaName = ea.Name
            eaValue = ea.Value
            new_data = {
                "Object Name": eName,
                "Parent Object Name": mName,
                "Object Type": objectType,
                "Annotation Name": eaName,
                "Annotation Value": eaValue,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    for per in m.Perspectives:
        perName = per.Name
        objectType = "Perspective"
        for pera in per.Annotations:
            peraName = pera.Name
            peraValue = pera.Value
            new_data = {
                "Object Name": perName,
                "Parent Object Name": mName,
                "Object Type": objectType,
                "Annotation Name": peraName,
                "Annotation Value": peraValue,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    for rol in m.Roles:
        rolName = rol.Name
        objectType = "Role"
        for rola in rol.Annotations:
            rolaName = rola.Name
            rolaValue = rola.Value
            new_data = {
                "Object Name": rolName,
                "Parent Object Name": mName,
                "Object Type": objectType,
                "Annotation Name": rolaName,
                "Annotation Value": rolaValue,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_columns(
    dataset: str,
    workspace: Optional[str] = None,
    lakehouse: Optional[str] = None,
    lakehouse_workspace: Optional[str] = None,
):
    """
    Shows a semantic model's columns and their properties.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    lakehouse : str, default=None
        The Fabric lakehouse (for Direct Lake semantic models).
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str, default=None
        The Fabric workspace used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the semantic model's columns and their properties.
    """
    from sempy_labs.directlake._get_directlake_lakehouse import (
        get_direct_lake_lakehouse,
    )

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)

    isDirectLake = any(r["Mode"] == "DirectLake" for i, r in dfP.iterrows())

    dfC = fabric.list_columns(dataset=dataset, workspace=workspace)

    if isDirectLake:
        dfC["Column Cardinality"] = None
        sql_statements = []
        (lakeID, lakeName) = get_direct_lake_lakehouse(
            dataset=dataset,
            workspace=workspace,
            lakehouse=lakehouse,
            lakehouse_workspace=lakehouse_workspace,
        )

        for table_name in dfC["Table Name"].unique():
            print(f"Gathering stats for table: '{table_name}'...")
            query = "SELECT "

            columns_in_table = dfC.loc[
                dfC["Table Name"] == table_name, "Column Name"
            ].unique()

            # Loop through columns within those tables
            for column_name in columns_in_table:
                scName = dfC.loc[
                    (dfC["Table Name"] == table_name)
                    & (dfC["Column Name"] == column_name),
                    "Source",
                ].iloc[0]
                lakeTName = dfC.loc[
                    (dfC["Table Name"] == table_name)
                    & (dfC["Column Name"] == column_name),
                    "Query",
                ].iloc[0]

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
                    if r["Table Name"] == tName and r["Source"] == column:
                        dfC.at[i, "Column Cardinality"] = x

        # Remove column added temporarily
        dfC.drop(columns=["Query"], inplace=True)

    return dfC


def list_dashboards(workspace: Optional[str] = None):
    """
    Shows a list of the dashboards within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the dashboards within a workspace.
    """

    df = pd.DataFrame(
        columns=[
            "Dashboard ID",
            "Dashboard Name",
            "Read Only",
            "Web URL",
            "Embed URL",
            "Data Classification",
            "Users",
            "Subscriptions",
        ]
    )

    if workspace == "None":
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resovle_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    client = fabric.PowerBIRestClient()
    response = client.get(f"/v1.0/myorg/groups/{workspace_id}/dashboards")

    for v in response.json()["value"]:
        dashboardID = v["id"]
        displayName = v["displayName"]
        isReadOnly = v["isReadOnly"]
        webURL = v["webUrl"]
        embedURL = v["embedUrl"]
        dataClass = v["dataClassification"]
        users = v["users"]
        subs = v["subscriptions"]

        new_data = {
            "Dashboard ID": dashboardID,
            "Dashboard Name": displayName,
            "Read Only": isReadOnly,
            "Web URL": webURL,
            "Embed URL": embedURL,
            "Data Classification": dataClass,
            "Users": [users],
            "Subscriptions": [subs],
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df["Read Only"] = df["Read Only"].astype(bool)

    return df


def list_lakehouses(workspace: Optional[str] = None):
    """
    Shows the lakehouses within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the lakehouses within a workspace.
    """

    df = pd.DataFrame(
        columns=[
            "Lakehouse Name",
            "Lakehouse ID",
            "Description",
            "OneLake Tables Path",
            "OneLake Files Path",
            "SQL Endpoint Connection String",
            "SQL Endpoint ID",
            "SQL Endpoint Provisioning Status",
        ]
    )

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/lakehouses/")

    for v in response.json()["value"]:
        lakehouseId = v["id"]
        lakehouseName = v["displayName"]
        lakehouseDesc = v["description"]
        prop = v["properties"]
        oneLakeTP = prop["oneLakeTablesPath"]
        oneLakeFP = prop["oneLakeFilesPath"]
        sqlEPProp = prop["sqlEndpointProperties"]
        sqlEPCS = sqlEPProp["connectionString"]
        sqlepid = sqlEPProp["id"]
        sqlepstatus = sqlEPProp["provisioningStatus"]

        new_data = {
            "Lakehouse Name": lakehouseName,
            "Lakehouse ID": lakehouseId,
            "Description": lakehouseDesc,
            "OneLake Tables Path": oneLakeTP,
            "OneLake Files Path": oneLakeFP,
            "SQL Endpoint Connection String": sqlEPCS,
            "SQL Endpoint ID": sqlepid,
            "SQL Endpoint Provisioning Status": sqlepstatus,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_warehouses(workspace: Optional[str] = None):
    """
    Shows the warehouses within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the warehouses within a workspace.
    """

    df = pd.DataFrame(
        columns=[
            "Warehouse Name",
            "Warehouse ID",
            "Description",
            "Connection Info",
            "Created Date",
            "Last Updated Time",
        ]
    )

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/warehouses/")

    for v in response.json()["value"]:
        warehouse_id = v["id"]
        warehouse_name = v["displayName"]
        desc = v["description"]
        prop = v["properties"]
        connInfo = prop["connectionInfo"]
        createdDate = prop["createdDate"]
        lastUpdate = prop["lastUpdatedTime"]

        new_data = {
            "Warehouse Name": warehouse_name,
            "Warehouse ID": warehouse_id,
            "Description": desc,
            "Connection Info": connInfo,
            "Created Date": createdDate,
            "Last Updated Time": lastUpdate,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_sqlendpoints(workspace: Optional[str] = None):
    """
    Shows the SQL Endpoints within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the SQL Endpoints within a workspace.
    """

    df = pd.DataFrame(columns=["SQL Endpoint ID", "SQL Endpoint Name", "Description"])

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/sqlEndpoints/")

    for v in response.json()["value"]:
        sql_id = v["id"]
        lake_name = v["displayName"]
        desc = v["description"]

        new_data = {
            "SQL Endpoint ID": sql_id,
            "SQL Endpoint Name": lake_name,
            "Description": desc,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_mirroredwarehouses(workspace: Optional[str] = None):
    """
    Shows the mirrored warehouses within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the mirrored warehouses within a workspace.
    """

    df = pd.DataFrame(
        columns=["Mirrored Warehouse", "Mirrored Warehouse ID", "Description"]
    )

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/mirroredWarehouses/")

    for v in response.json()["value"]:
        mirr_id = v["id"]
        dbname = v["displayName"]
        desc = v["description"]

        new_data = {
            "Mirrored Warehouse": dbname,
            "Mirrored Warehouse ID": mirr_id,
            "Description": desc,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_kqldatabases(workspace: Optional[str] = None):
    """
    Shows the KQL databases within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the KQL Databases within a workspace.
    """

    df = pd.DataFrame(
        columns=[
            "KQL Database Name",
            "KQL Database ID",
            "Description",
            "Parent Eventhouse Item ID",
            "Query Service URI",
            "Ingestion Service URI",
            "Kusto Database Type",
        ]
    )

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/kqlDatabases/")

    for v in response.json()["value"]:
        kql_id = v["id"]
        kql_name = v["displayName"]
        desc = v["description"]
        prop = v["properties"]
        eventId = prop["parentEventhouseItemId"]
        qsURI = prop["queryServiceUri"]
        isURI = prop["ingestionServiceUri"]
        dbType = prop["kustoDatabaseType"]

        new_data = {
            "KQL Database Name": kql_name,
            "KQL Database ID": kql_id,
            "Description": desc,
            "Parent Eventhouse Item ID": eventId,
            "Query Service URI": qsURI,
            "Ingestion Service URI": isURI,
            "Kusto Database Type": dbType,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_kqlquerysets(workspace: Optional[str] = None):
    """
    Shows the KQL Querysets within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the KQL Querysets within a workspace.
    """

    df = pd.DataFrame(columns=["KQL Queryset Name", "KQL Queryset ID", "Description"])

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/kqlQuerysets/")

    for v in response.json()["value"]:
        kql_id = v["id"]
        kql_name = v["displayName"]
        desc = v["description"]

        new_data = {
            "KQL Queryset Name": kql_name,
            "KQL Queryset ID": kql_id,
            "Description": desc,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_mlmodels(workspace: Optional[str] = None):
    """
    Shows the ML models within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the ML models within a workspace.
    """

    df = pd.DataFrame(columns=["ML Model Name", "ML Model ID", "Description"])

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/mlModels/")

    for v in response.json()["value"]:
        model_id = v["id"]
        modelName = v["displayName"]
        desc = v["description"]

        new_data = {
            "ML Model Name": modelName,
            "ML Model ID": model_id,
            "Description": desc,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_eventstreams(workspace: Optional[str] = None):
    """
    Shows the eventstreams within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the eventstreams within a workspace.
    """

    df = pd.DataFrame(columns=["Eventstream Name", "Eventstream ID", "Description"])

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/eventstreams/")

    for v in response.json()["value"]:
        model_id = v["id"]
        modelName = v["displayName"]
        desc = v["description"]

        new_data = {
            "Eventstream Name": modelName,
            "Eventstream ID": model_id,
            "Description": desc,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_datapipelines(workspace: Optional[str] = None):
    """
    Shows the data pipelines within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the data pipelines within a workspace.
    """

    df = pd.DataFrame(columns=["Data Pipeline Name", "Data Pipeline ID", "Description"])

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/dataPipelines/")

    for v in response.json()["value"]:
        model_id = v["id"]
        modelName = v["displayName"]
        desc = v["description"]

        new_data = {
            "Data Pipeline Name": modelName,
            "Data Pipeline ID": model_id,
            "Description": desc,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_mlexperiments(workspace: Optional[str] = None):
    """
    Shows the ML experiments within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the ML experiments within a workspace.
    """

    df = pd.DataFrame(columns=["ML Experiment Name", "ML Experiment ID", "Description"])

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/mlExperiments/")

    for v in response.json()["value"]:
        model_id = v["id"]
        modelName = v["displayName"]
        desc = v["description"]

        new_data = {
            "ML Experiment Name": modelName,
            "ML Experiment ID": model_id,
            "Description": desc,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_datamarts(workspace: Optional[str] = None):
    """
    Shows the datamarts within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the datamarts within a workspace.
    """

    df = pd.DataFrame(columns=["Datamart Name", "Datamart ID", "Description"])

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/datamarts/")

    for v in response.json()["value"]:
        model_id = v["id"]
        modelName = v["displayName"]
        desc = v["description"]

        new_data = {
            "Datamart Name": modelName,
            "Datamart ID": model_id,
            "Description": desc,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def create_warehouse(
    warehouse: str, description: Optional[str] = None, workspace: Optional[str] = None
):
    """
    Creates a Fabric warehouse.

    Parameters
    ----------
    warehouse: str
        Name of the warehouse.
    description : str, default=None
        A description of the warehouse.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------

    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    if description == None:
        request_body = {"displayName": warehouse}
    else:
        request_body = {"displayName": warehouse, "description": description}

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/warehouses/", json=request_body
    )

    if response.status_code == 201:
        print(
            f"The '{warehouse}' warehouse has been created within the '{workspace}' workspace."
        )
    elif response.status_code == 202:
        operationId = response.headers["x-ms-operation-id"]
        response = client.get(f"/v1/operations/{operationId}")
        response_body = json.loads(response.content)
        while response_body["status"] != "Succeeded":
            time.sleep(3)
            response = client.get(f"/v1/operations/{operationId}")
            response_body = json.loads(response.content)
        response = client.get(f"/v1/operations/{operationId}/result")
        print(
            f"The '{warehouse}' warehouse has been created within the '{workspace}' workspace."
        )
    else:
        print(
            f"ERROR: Failed to create the '{warehouse}' warehouse within the '{workspace}' workspace."
        )


def update_item(
    item_type: str,
    current_name: str,
    new_name: str,
    description: Optional[str] = None,
    workspace: Optional[str] = None,
):
    """
    Updates the name/description of a Fabric item.

    Parameters
    ----------
    item_type: str
        Type of item to update.
    current_name : str
        The current name of the item.
    new_name : str
        The new name of the item.
    description : str, default=None
        A description of the item.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    itemTypes = {
        "DataPipeline": "dataPipelines",
        "Eventstream": "eventstreams",
        "KQLDatabase": "kqlDatabases",
        "KQLQueryset": "kqlQuerysets",
        "Lakehouse": "lakehouses",
        "MLExperiment": "mlExperiments",
        "MLModel": "mlModels",
        "Notebook": "notebooks",
        "Warehouse": "warehouses",
    }

    item_type = item_type.replace(" ", "").capitalize()

    if item_type not in itemTypes.keys():
        print(f"The '{item_type}' is not a valid item type. ")
        return

    itemType = itemTypes[item_type]

    dfI = fabric.list_items(workspace=workspace, type=item_type)
    dfI_filt = dfI[(dfI["Display Name"] == current_name)]

    if len(dfI_filt) == 0:
        print(
            f"The '{current_name}' {item_type} does not exist within the '{workspace}' workspace."
        )
        return

    itemId = dfI_filt["Id"].iloc[0]

    if description == None:
        request_body = {"displayName": new_name}
    else:
        request_body = {"displayName": new_name, "description": description}

    client = fabric.FabricRestClient()
    response = client.patch(
        f"/v1/workspaces/{workspace_id}/{itemType}/{itemId}", json=request_body
    )

    if response.status_code == 200:
        if description == None:
            print(
                f"The '{current_name}' {item_type} within the '{workspace}' workspace has been updated to be named '{new_name}'"
            )
        else:
            print(
                f"The '{current_name}' {item_type} within the '{workspace}' workspace has been updated to be named '{new_name}' and have a description of '{description}'"
            )
    else:
        print(
            f"ERROR: The '{current_name}' {item_type} within the '{workspace}' workspace was not updateds."
        )


def list_relationships(
    dataset: str, workspace: Optional[str] = None, extended: Optional[bool] = False
):
    """
    Shows a semantic model's relationships and their properties.

    Parameters
    ----------
    dataset: str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    extended : bool, default=False
        Fetches extended column information.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the object level security for the semantic model.
    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    dfR = fabric.list_relationships(dataset=dataset, workspace=workspace)

    if extended:
        # Used to map the Relationship IDs
        rel = fabric.evaluate_dax(
            dataset=dataset,
            workspace=workspace,
            dax_string="""
                SELECT
                [ID] AS [RelationshipID]
                ,[Name] 
                FROM $SYSTEM.TMSCHEMA_RELATIONSHIPS
                """,
        )

        # USED_SIZE shows the Relationship Size where TABLE_ID starts with R$
        cs = fabric.evaluate_dax(
            dataset=dataset,
            workspace=workspace,
            dax_string="""
                SELECT 
                [TABLE_ID]
                ,[USED_SIZE]
                FROM $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMN_SEGMENTS
                """,
        )

        def parse_value(text):
            ind = text.rfind("(") + 1
            output = text[ind:]
            output = output[:-1]
            return output

        cs["RelationshipID"] = cs["TABLE_ID"].apply(parse_value).astype("uint64")
        relcs = pd.merge(
            cs[["RelationshipID", "TABLE_ID", "USED_SIZE"]],
            rel,
            on="RelationshipID",
            how="left",
        )

        dfR["Used Size"] = None
        for i, r in dfR.iterrows():
            relName = r["Relationship Name"]

            filtered_cs = relcs[
                (relcs["Name"] == relName) & (relcs["TABLE_ID"].str.startswith("R$"))
            ]
            sumval = filtered_cs["USED_SIZE"].sum()
            dfR.at[i, "Used Size"] = sumval

        dfR["Used Size"] = dfR["Used Size"].astype("int")

    return dfR


def list_dataflow_storage_accounts():
    """
    Shows the accessible dataflow storage accounts.

    Parameters
    ----------

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the accessible dataflow storage accounts.
    """

    df = pd.DataFrame(
        columns=[
            "Dataflow Storage Account ID",
            "Dataflow Storage Account Name",
            "Enabled",
        ]
    )
    client = fabric.PowerBIRestClient()
    response = client.get(f"/v1.0/myorg/dataflowStorageAccounts")

    for v in response.json()["value"]:
        dfsaId = v["id"]
        dfsaName = v["name"]
        isEnabled = v["isEnabled"]

        new_data = {
            "Dataflow Storage Account ID": dfsaId,
            "Dataflow Storage Account Name": dfsaName,
            "Enabled": isEnabled,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df["Enabled"] = df["Enabled"].astype(bool)

    return df


def list_kpis(dataset: str, workspace: Optional[str] = None):
    """
    Shows a semantic model's KPIs and their properties.

    Parameters
    ----------
    dataset: str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the KPIs for the semantic model.
    """

    from .tom import connect_semantic_model

    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=True
    ) as tom:

        df = pd.DataFrame(
            columns=[
                "Table Name",
                "Measure Name",
                "Target Expression",
                "Target Format String",
                "Target Description",
                "Status Expression",
                "Status Graphic",
                "Status Description",
                "Trend Expression",
                "Trend Graphic",
                "Trend Description",
            ]
        )

        for t in tom.model.Tables:
            for m in t.Measures:
                if m.KPI is not None:
                    new_data = {
                        "Table Name": t.Name,
                        "Measure Name": m.Name,
                        "Target Expression": m.KPI.TargetExpression,
                        "Target Format String": m.KPI.TargetFormatString,
                        "Target Description": m.KPI.TargetDescription,
                        "Status Graphic": m.KPI.StatusGraphic,
                        "Status Expression": m.KPI.StatusExpression,
                        "Status Description": m.KPI.StatusDescription,
                        "Trend Expression": m.KPI.TrendExpression,
                        "Trend Graphic": m.KPI.TrendGraphic,
                        "Trend Description": m.KPI.TrendDescription,
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

        return df


def list_workspace_role_assignments(workspace: Optional[str] = None):
    """
    Shows the members of a given workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the members of a given workspace and their roles.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = pd.DataFrame(columns=["User Name", "User Email", "Role Name", "Type"])

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/roleAssignments")

    for i in response.json()["value"]:
        user_name = i["principal"]["displayName"]
        role_name = i["role"]
        user_email = i["principal"]["userDetails"]["userPrincipalName"]
        user_type = i["principal"]["type"]

        new_data = {
            "User Name": user_name,
            "Role Name": role_name,
            "Type": user_type,
            "User Email": user_email,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

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
    from .tom import connect_semantic_model

    df = pd.DataFrame(columns=["Parent Name", "Object Name", "Object Type"])
    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=True
    ) as tom:
        for t in tom.model.Tables:
            if t.CalculationGroup is not None:
                new_data = {
                    "Parent Name": t.Parent.Name,
                    "Object Name": t.Name,
                    "Object Type": "Calculation Group",
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
                for ci in t.CalculationGroup.CalculationItems:
                    new_data = {
                        "Parent Name": t.Name,
                        "Object Name": ci.Name,
                        "Object Type": str(ci.ObjectType),
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )
            elif any(str(p.SourceType) == "Calculated" for p in t.Partitions):
                new_data = {
                    "Parent Name": t.Parent.Name,
                    "Object Name": t.Name,
                    "Object Type": "Calculated Table",
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
            else:
                new_data = {
                    "Parent Name": t.Parent.Name,
                    "Object Name": t.Name,
                    "Object Type": str(t.ObjectType),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
            for c in t.Columns:
                if str(c.Type) != "RowNumber":
                    if str(c.Type) == "Calculated":
                        new_data = {
                            "Parent Name": c.Parent.Name,
                            "Object Name": c.Name,
                            "Object Type": "Calculated Column",
                        }
                        df = pd.concat(
                            [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                        )
                    else:
                        new_data = {
                            "Parent Name": c.Parent.Name,
                            "Object Name": c.Name,
                            "Object Type": str(c.ObjectType),
                        }
                        df = pd.concat(
                            [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                        )
            for m in t.Measures:
                new_data = {
                    "Parent Name": m.Parent.Name,
                    "Object Name": m.Name,
                    "Object Type": str(m.ObjectType),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
            for h in t.Hierarchies:
                new_data = {
                    "Parent Name": h.Parent.Name,
                    "Object Name": h.Name,
                    "Object Type": str(h.ObjectType),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
                for l in h.Levels:
                    new_data = {
                        "Parent Name": l.Parent.Name,
                        "Object Name": l.Name,
                        "Object Type": str(l.ObjectType),
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )
            for p in t.Partitions:
                new_data = {
                    "Parent Name": p.Parent.Name,
                    "Object Name": p.Name,
                    "Object Type": str(p.ObjectType),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
        for r in tom.model.Relationships:
            rName = create_relationship_name(
                r.FromTable.Name, r.FromColumn.Name, r.ToTable.Name, r.ToColumn.Name
            )
            new_data = {
                "Parent Name": r.Parent.Name,
                "Object Name": rName,
                "Object Type": str(r.ObjectType),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        for role in tom.model.Roles:
            new_data = {
                "Parent Name": role.Parent.Name,
                "Object Name": role.Name,
                "Object Type": str(role.ObjectType),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
            for rls in role.TablePermissions:
                new_data = {
                    "Parent Name": role.Name,
                    "Object Name": rls.Name,
                    "Object Type": str(rls.ObjectType),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
        for tr in tom.model.Cultures:
            new_data = {
                "Parent Name": tr.Parent.Name,
                "Object Name": tr.Name,
                "Object Type": str(tr.ObjectType),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        for per in tom.model.Perspectives:
            new_data = {
                "Parent Name": per.Parent.Name,
                "Object Name": per.Name,
                "Object Type": str(per.ObjectType),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

def list_shortcuts(
    lakehouse: Optional[str] = None, workspace: Optional[str] = None
) -> pd.DataFrame:
    """
    Shows all shortcuts which exist in a Fabric lakehouse.

    Parameters
    ----------
    lakehouse : str, default=None
        The Fabric lakehouse name.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str, default=None
        The name of the Fabric workspace in which lakehouse resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing all the shortcuts which exist in the specified lakehouse.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    if lakehouse == None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, workspace)
    else:
        lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)

    df = pd.DataFrame(
        columns=[
            "Shortcut Name",
            "Shortcut Path",
            "Source",
            "Source Lakehouse Name",
            "Source Workspace Name",
            "Source Path",
            "Source Connection ID",
            "Source Location",
            "Source SubPath",
        ]
    )

    client = fabric.FabricRestClient()
    response = client.get(
        f"/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts"
    )
    if response.status_code == 200:
        for s in response.json()["value"]:
            shortcutName = s["name"]
            shortcutPath = s["path"]
            source = list(s["target"].keys())[0]
            (
                sourceLakehouseName,
                sourceWorkspaceName,
                sourcePath,
                connectionId,
                location,
                subpath,
            ) = (None, None, None, None, None, None)
            if source == "oneLake":
                sourceLakehouseId = s["target"][source]["itemId"]
                sourcePath = s["target"][source]["path"]
                sourceWorkspaceId = s["target"][source]["workspaceId"]
                sourceWorkspaceName = fabric.resolve_workspace_name(sourceWorkspaceId)
                sourceLakehouseName = resolve_lakehouse_name(
                    sourceLakehouseId, sourceWorkspaceName
                )
            else:
                connectionId = s["target"][source]["connectionId"]
                location = s["target"][source]["location"]
                subpath = s["target"][source]["subpath"]

            new_data = {
                "Shortcut Name": shortcutName,
                "Shortcut Path": shortcutPath,
                "Source": source,
                "Source Lakehouse Name": sourceLakehouseName,
                "Source Workspace Name": sourceWorkspaceName,
                "Source Path": sourcePath,
                "Source Connection ID": connectionId,
                "Source Location": location,
                "Source SubPath": subpath,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    print(
        f"This function relies on an API which is not yet official as of May 21, 2024. Once the API becomes official this function will work as expected."
    )
    return df