import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_lakehouse_name,
    create_relationship_name,
    resolve_lakehouse_id,
    resolve_dataset_id,
    lro,
    pagination,
    resolve_capacity_id,
    resolve_deployment_pipeline_id,
)
import pandas as pd
import base64
import requests
import time
import json
from pyspark.sql import SparkSession
from typing import Optional, List
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID


def get_object_level_security(
    dataset: str, workspace: Optional[str] = None
) -> pd.DataFrame:
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

    from sempy_labs.tom import connect_semantic_model

    workspace = fabric.resolve_workspace_name(workspace)

    df = pd.DataFrame(columns=["Role Name", "Object Type", "Table Name", "Object Name"])

    with connect_semantic_model(
        dataset=dataset, readonly=True, workspace=workspace
    ) as tom:

        for r in tom.model.Roles:
            for tp in r.TablePermissions:
                if len(tp.FilterExpression) == 0:
                    columnCount = 0
                    try:
                        columnCount = len(tp.ColumnPermissions)
                    except Exception:
                        pass
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
                                [df, pd.DataFrame(new_data, index=[0])],
                                ignore_index=True,
                            )

        return df


def list_tables(dataset: str, workspace: Optional[str] = None) -> pd.DataFrame:
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

    workspace = fabric.resolve_workspace_name(workspace)

    df = fabric.list_tables(
        dataset=dataset,
        workspace=workspace,
        additional_xmla_properties=["RefreshPolicy", "RefreshPolicy.SourceExpression"],
    )

    df["Refresh Policy"] = df["Refresh Policy"].notna()
    df.rename(
        columns={"Refresh Policy Source Expression": "Source Expression"}, inplace=True
    )

    return df


def list_annotations(dataset: str, workspace: Optional[str] = None) -> pd.DataFrame:
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

    from sempy_labs.tom import connect_semantic_model

    workspace = fabric.resolve_workspace_name(workspace)

    df = pd.DataFrame(
        columns=[
            "Object Name",
            "Parent Object Name",
            "Object Type",
            "Annotation Name",
            "Annotation Value",
        ]
    )

    with connect_semantic_model(
        dataset=dataset, readonly=True, workspace=workspace
    ) as tom:

        mName = tom.model.Name
        for a in tom.model.Annotations:
            objectType = "Model"
            aName = a.Name
            aValue = a.Value
            new_data = {
                "Object Name": mName,
                "Parent Object Name": None,
                "Object Type": objectType,
                "Annotation Name": aName,
                "Annotation Value": aValue,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        for t in tom.model.Tables:
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
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
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
        for d in tom.model.DataSources:
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
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
        for r in tom.model.Relationships:
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
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
        for cul in tom.model.Cultures:
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
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
        for e in tom.model.Expressions:
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
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
        for per in tom.model.Perspectives:
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
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
        for rol in tom.model.Roles:
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
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        return df


def list_columns(
    dataset: str,
    workspace: Optional[str] = None,
    lakehouse: Optional[str] = None,
    lakehouse_workspace: Optional[str] = None,
) -> pd.DataFrame:
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

    workspace = fabric.resolve_workspace_name(workspace)

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
                query = f"{query}COUNT(DISTINCT({scName})) AS {scName}, "

            query = query[:-2]
            query = f"{query} FROM {lakehouse}.{lakeTName}"
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


def list_dashboards(workspace: Optional[str] = None) -> pd.DataFrame:
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
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json()["value"]:
        new_data = {
            "Dashboard ID": v.get("id"),
            "Dashboard Name": v.get("displayName"),
            "Read Only": v.get("isReadOnly"),
            "Web URL": v.get("webUrl"),
            "Embed URL": v.get("embedUrl"),
            "Data Classification": v.get("dataClassification"),
            "Users": [v.get("users")],
            "Subscriptions": [v.get("subscriptions")],
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df["Read Only"] = df["Read Only"].astype(bool)

    return df


def list_lakehouses(workspace: Optional[str] = None) -> pd.DataFrame:
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
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json()["value"]:
        prop = v.get("properties", {})
        sqlEPProp = prop.get("sqlEndpointProperties", {})

        new_data = {
            "Lakehouse Name": v.get("displayName"),
            "Lakehouse ID": v.get("id"),
            "Description": v.get("description"),
            "OneLake Tables Path": prop.get("oneLakeTablesPath"),
            "OneLake Files Path": prop.get("oneLakeFilesPath"),
            "SQL Endpoint Connection String": sqlEPProp.get("connectionString"),
            "SQL Endpoint ID": sqlEPProp.get("id"),
            "SQL Endpoint Provisioning Status": sqlEPProp.get("provisioningStatus"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_warehouses(workspace: Optional[str] = None) -> pd.DataFrame:
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
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json()["value"]:
        prop = v.get("properties", {})

        new_data = {
            "Warehouse Name": v.get("displayName"),
            "Warehouse ID": v.get("id"),
            "Description": v.get("description"),
            "Connection Info": prop.get("connectionInfo"),
            "Created Date": prop.get("createdDate"),
            "Last Updated Time": prop.get("lastUpdatedTime"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_sqlendpoints(workspace: Optional[str] = None) -> pd.DataFrame:
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
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json()["value"]:

        new_data = {
            "SQL Endpoint ID": v.get("id"),
            "SQL Endpoint Name": v.get("displayName"),
            "Description": v.get("description"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_mirroredwarehouses(workspace: Optional[str] = None) -> pd.DataFrame:
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
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json()["value"]:

        new_data = {
            "Mirrored Warehouse": v.get("displayName"),
            "Mirrored Warehouse ID": v.get("id"),
            "Description": v.get("description"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_kqldatabases(workspace: Optional[str] = None) -> pd.DataFrame:
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
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json()["value"]:
        prop = v.get("properties", {})

        new_data = {
            "KQL Database Name": v.get("displayName"),
            "KQL Database ID": v.get("id"),
            "Description": v.get("description"),
            "Parent Eventhouse Item ID": prop.get("parentEventhouseItemId"),
            "Query Service URI": prop.get("queryServiceUri"),
            "Ingestion Service URI": prop.get("ingestionServiceUri"),
            "Kusto Database Type": prop.get("kustoDatabaseType"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_kqlquerysets(workspace: Optional[str] = None) -> pd.DataFrame:
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
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json()["value"]:

        new_data = {
            "KQL Queryset Name": v.get("displayName"),
            "KQL Queryset ID": v.get("id"),
            "Description": v.get("description"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_mlmodels(workspace: Optional[str] = None) -> pd.DataFrame:
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
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json()["value"]:
        model_id = v.get("id")
        modelName = v.get("displayName")
        desc = v.get("description")

        new_data = {
            "ML Model Name": modelName,
            "ML Model ID": model_id,
            "Description": desc,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_eventstreams(workspace: Optional[str] = None) -> pd.DataFrame:
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
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json()["value"]:
        model_id = v.get("id")
        modelName = v.get("displayName")
        desc = v.get("description")

        new_data = {
            "Eventstream Name": modelName,
            "Eventstream ID": model_id,
            "Description": desc,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_datapipelines(workspace: Optional[str] = None) -> pd.DataFrame:
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
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json()["value"]:
        model_id = v.get("id")
        modelName = v.get("displayName")
        desc = v.get("description")

        new_data = {
            "Data Pipeline Name": modelName,
            "Data Pipeline ID": model_id,
            "Description": desc,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_mlexperiments(workspace: Optional[str] = None) -> pd.DataFrame:
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
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json()["value"]:

        new_data = {
            "ML Experiment Name": v.get("displayName"),
            "ML Experiment ID": v.get("id"),
            "Description": v.get("description"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_datamarts(workspace: Optional[str] = None) -> pd.DataFrame:
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
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json()["value"]:

        new_data = {
            "Datamart Name": v.get("displayName"),
            "Datamart ID": v.get("id"),
            "Description": v.get("description"),
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

    request_body = {"displayName": warehouse}

    if description:
        request_body["description"] = description

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/warehouses/", json=request_body, lro_wait=True
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{warehouse}' warehouse has been created within the '{workspace}' workspace."
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
        raise ValueError(
            f"{icons.red_dot} The '{item_type}' is not a valid item type. "
        )

    itemType = itemTypes[item_type]

    dfI = fabric.list_items(workspace=workspace, type=item_type)
    dfI_filt = dfI[(dfI["Display Name"] == current_name)]

    if len(dfI_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{current_name}' {item_type} does not exist within the '{workspace}' workspace."
        )

    itemId = dfI_filt["Id"].iloc[0]

    request_body = {"displayName": new_name}
    if description:
        request_body["description"] = description

    client = fabric.FabricRestClient()
    response = client.patch(
        f"/v1/workspaces/{workspace_id}/{itemType}/{itemId}", json=request_body
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    if description is None:
        print(
            f"{icons.green_dot} The '{current_name}' {item_type} within the '{workspace}' workspace has been updated to be named '{new_name}'"
        )
    else:
        print(
            f"{icons.green_dot} The '{current_name}' {item_type} within the '{workspace}' workspace has been updated to be named '{new_name}' and have a description of '{description}'"
        )


def list_relationships(
    dataset: str, workspace: Optional[str] = None, extended: Optional[bool] = False
) -> pd.DataFrame:
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

    workspace = fabric.resolve_workspace_name(workspace)

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


def list_dataflow_storage_accounts() -> pd.DataFrame:
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
    response = client.get("/v1.0/myorg/dataflowStorageAccounts")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json()["value"]:

        new_data = {
            "Dataflow Storage Account ID": v.get("id"),
            "Dataflow Storage Account Name": v.get("name"),
            "Enabled": v.get("isEnabled"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df["Enabled"] = df["Enabled"].astype(bool)

    return df


def list_kpis(dataset: str, workspace: Optional[str] = None) -> pd.DataFrame:
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

    from sempy_labs.tom import connect_semantic_model

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


def list_workspace_role_assignments(workspace: Optional[str] = None) -> pd.DataFrame:
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
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for i in response.json()["value"]:
        user_name = i.get("principal", {}).get("displayName")
        role_name = i.get("role")
        user_email = (
            i.get("principal", {}).get("userDetails", {}).get("userPrincipalName")
        )
        user_type = i.get("principal", {}).get("type")

        new_data = {
            "User Name": user_name,
            "Role Name": role_name,
            "Type": user_type,
            "User Email": user_email,
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_semantic_model_objects(
    dataset: str, workspace: Optional[str] = None
) -> pd.DataFrame:
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
    from sempy_labs.tom import connect_semantic_model

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
                for lev in h.Levels:
                    new_data = {
                        "Parent Name": lev.Parent.Name,
                        "Object Name": lev.Name,
                        "Object Type": str(lev.ObjectType),
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

    if lakehouse is None:
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
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for s in response.json()["value"]:
        shortcutName = s.get("name")
        shortcutPath = s.get("path")
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
            sourceLakehouseId = s.get("target", {}).get(source, {}).get("itemId")
            sourcePath = s.get("target", {}).get(source, {}).get("path")
            sourceWorkspaceId = s.get("target", {}).get(source, {}).get("workspaceId")
            sourceWorkspaceName = fabric.resolve_workspace_name(sourceWorkspaceId)
            sourceLakehouseName = resolve_lakehouse_name(
                sourceLakehouseId, sourceWorkspaceName
            )
        else:
            connectionId = s.get("target", {}).get(source, {}).get("connectionId")
            location = s.get("target", {}).get(source, {}).get("location")
            subpath = s.get("target", {}).get(source, {}).get("subpath")

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

    return df


def list_custom_pools(workspace: Optional[str] = None) -> pd.DataFrame:
    """
    Lists all `custom pools <https://learn.microsoft.com/fabric/data-engineering/create-custom-spark-pools>`_ within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The name of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing all the custom pools within the Fabric workspace.
    """

    # https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/list-workspace-custom-pools
    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = pd.DataFrame(
        columns=[
            "Custom Pool ID",
            "Custom Pool Name",
            "Type",
            "Node Family",
            "Node Size",
            "Auto Scale Enabled",
            "Auto Scale Min Node Count",
            "Auto Scale Max Node Count",
            "Dynamic Executor Allocation Enabled",
            "Dynamic Executor Allocation Min Executors",
            "Dynamic Executor Allocation Max Executors",
        ]
    )

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/spark/pools")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for i in response.json()["value"]:

        aScale = i.get("autoScale", {})
        d = i.get("dynamicExecutorAllocation", {})

        new_data = {
            "Custom Pool ID": i.get("id"),
            "Custom Pool Name": i.get("name"),
            "Type": i.get("type"),
            "Node Family": i.get("nodeFamily"),
            "Node Size": i.get("nodeSize"),
            "Auto Scale Enabled": aScale.get("enabled"),
            "Auto Scale Min Node Count": aScale.get("minNodeCount"),
            "Auto Scale Max Node Count": aScale.get("maxNodeCount"),
            "Dynamic Executor Allocation Enabled": d.get("enabled"),
            "Dynamic Executor Allocation Min Executors": d.get("minExecutors"),
            "Dynamic Executor Allocation Max Executors": d.get("maxExecutors"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    bool_cols = ["Auto Scale Enabled", "Dynamic Executor Allocation Enabled"]
    int_cols = [
        "Auto Scale Min Node Count",
        "Auto Scale Max Node Count",
        "Dynamic Executor Allocation Enabled",
        "Dynamic Executor Allocation Min Executors",
        "Dynamic Executor Allocation Max Executors",
    ]

    df[bool_cols] = df[bool_cols].astype(bool)
    df[int_cols] = df[int_cols].astype(int)

    return df


def create_custom_pool(
    pool_name: str,
    node_size: str,
    min_node_count: int,
    max_node_count: int,
    min_executors: int,
    max_executors: int,
    node_family: Optional[str] = "MemoryOptimized",
    auto_scale_enabled: Optional[bool] = True,
    dynamic_executor_allocation_enabled: Optional[bool] = True,
    workspace: Optional[str] = None,
):
    """
    Creates a `custom pool <https://learn.microsoft.com/fabric/data-engineering/create-custom-spark-pools>`_ within a workspace.

    Parameters
    ----------
    pool_name : str
        The custom pool name.
    node_size : str
        The `node size <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#nodesize>`_.
    min_node_count : int
        The `minimum node count <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties>`_.
    max_node_count : int
        The `maximum node count <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties>`_.
    min_executors : int
        The `minimum executors <https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties>`_.
    max_executors : int
        The `maximum executors <https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties>`_.
    node_family : str, default='MemoryOptimized'
        The `node family <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#nodefamily>`_.
    auto_scale_enabled : bool, default=True
        The status of `auto scale <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties>`_.
    dynamic_executor_allocation_enabled : bool, default=True
        The status of the `dynamic executor allocation <https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties>`_.
    workspace : str, default=None
        The name of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool
    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    request_body = {
        "name": pool_name,
        "nodeFamily": node_family,
        "nodeSize": node_size,
        "autoScale": {
            "enabled": auto_scale_enabled,
            "minNodeCount": min_node_count,
            "maxNodeCount": max_node_count,
        },
        "dynamicExecutorAllocation": {
            "enabled": dynamic_executor_allocation_enabled,
            "minExecutors": min_executors,
            "maxExecutors": max_executors,
        },
    }

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/spark/pools", json=request_body, lro_wait=True
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{pool_name}' spark pool has been created within the '{workspace}' workspace."
    )


def update_custom_pool(
    pool_name: str,
    node_size: Optional[str] = None,
    min_node_count: Optional[int] = None,
    max_node_count: Optional[int] = None,
    min_executors: Optional[int] = None,
    max_executors: Optional[int] = None,
    node_family: Optional[str] = None,
    auto_scale_enabled: Optional[bool] = None,
    dynamic_executor_allocation_enabled: Optional[bool] = None,
    workspace: Optional[str] = None,
):
    """
    Updates the properties of a `custom pool <https://learn.microsoft.com/fabric/data-engineering/create-custom-spark-pools>`_ within a workspace.

    Parameters
    ----------
    pool_name : str
        The custom pool name.
    node_size : str, default=None
        The `node size <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#nodesize>`_.
        Defaults to None which keeps the existing property setting.
    min_node_count : int, default=None
        The `minimum node count <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties>`_.
        Defaults to None which keeps the existing property setting.
    max_node_count : int, default=None
        The `maximum node count <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties>`_.
        Defaults to None which keeps the existing property setting.
    min_executors : int, default=None
        The `minimum executors <https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties>`_.
        Defaults to None which keeps the existing property setting.
    max_executors : int, default=None
        The `maximum executors <https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties>`_.
        Defaults to None which keeps the existing property setting.
    node_family : str, default=None
        The `node family <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#nodefamily>`_.
        Defaults to None which keeps the existing property setting.
    auto_scale_enabled : bool, default=None
        The status of `auto scale <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties>`_.
        Defaults to None which keeps the existing property setting.
    dynamic_executor_allocation_enabled : bool, default=None
        The status of the `dynamic executor allocation <https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties>`_.
        Defaults to None which keeps the existing property setting.
    workspace : str, default=None
        The name of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/update-workspace-custom-pool?tabs=HTTP
    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = list_custom_pools(workspace=workspace)
    df_pool = df[df["Custom Pool Name"] == pool_name]

    if len(df_pool) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{pool_name}' custom pool does not exist within the '{workspace}'. Please choose a valid custom pool."
        )

    if node_family is None:
        node_family = df_pool["Node Family"].iloc[0]
    if node_size is None:
        node_size = df_pool["Node Size"].iloc[0]
    if auto_scale_enabled is None:
        auto_scale_enabled = bool(df_pool["Auto Scale Enabled"].iloc[0])
    if min_node_count is None:
        min_node_count = int(df_pool["Min Node Count"].iloc[0])
    if max_node_count is None:
        max_node_count = int(df_pool["Max Node Count"].iloc[0])
    if dynamic_executor_allocation_enabled is None:
        dynamic_executor_allocation_enabled = bool(
            df_pool["Dynami Executor Allocation Enabled"].iloc[0]
        )
    if min_executors is None:
        min_executors = int(df_pool["Min Executors"].iloc[0])
    if max_executors is None:
        max_executors = int(df_pool["Max Executors"].iloc[0])

    request_body = {
        "name": pool_name,
        "nodeFamily": node_family,
        "nodeSize": node_size,
        "autoScale": {
            "enabled": auto_scale_enabled,
            "minNodeCount": min_node_count,
            "maxNodeCount": max_node_count,
        },
        "dynamicExecutorAllocation": {
            "enabled": dynamic_executor_allocation_enabled,
            "minExecutors": min_executors,
            "maxExecutors": max_executors,
        },
    }

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/spark/pools", json=request_body
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{pool_name}' spark pool within the '{workspace}' workspace has been updated."
    )


def delete_custom_pool(pool_name: str, workspace: Optional[str] = None):
    """
    Deletes a `custom pool <https://learn.microsoft.com/fabric/data-engineering/create-custom-spark-pools>`_ within a workspace.

    Parameters
    ----------
    pool_name : str
        The custom pool name.
    workspace : str, default=None
        The name of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfL = list_custom_pools(workspace=workspace)
    dfL_filt = dfL[dfL["Custom Pool Name"] == pool_name]

    if len(dfL_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{pool_name}' custom pool does not exist within the '{workspace}' workspace."
        )
    poolId = dfL_filt["Custom Pool ID"].iloc[0]

    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/workspaces/{workspace_id}/spark/pools/{poolId}")

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{pool_name}' spark pool has been deleted from the '{workspace}' workspace."
    )


def assign_workspace_to_capacity(capacity_name: str, workspace: Optional[str] = None):
    """
    Assigns a workspace to a capacity.

    Parameters
    ----------
    capacity_name : str
        The name of the capacity.
    workspace : str, default=None
        The name of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfC = fabric.list_capacities()
    dfC_filt = dfC[dfC["Display Name"] == capacity_name]
    capacity_id = dfC_filt["Id"].iloc[0]

    request_body = {"capacityId": capacity_id}

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/assignToCapacity",
        json=request_body,
        lro_wait=True,
    )

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{workspace}' workspace has been assigned to the '{capacity_name}' capacity."
    )


def unassign_workspace_from_capacity(workspace: Optional[str] = None):
    """
    Unassigns a workspace from its assigned capacity.

    Parameters
    ----------
    workspace : str, default=None
        The name of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/unassign-from-capacity?tabs=HTTP
    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/unassignFromCapacity", lro_wait=True
    )

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{workspace}' workspace has been unassigned from its capacity."
    )


def get_spark_settings(workspace: Optional[str] = None) -> pd.DataFrame:
    """
    Shows the spark settings for a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The name of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the spark settings for a workspace.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/spark/workspace-settings/get-spark-settings?tabs=HTTP
    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = pd.DataFrame(
        columns=[
            "Automatic Log Enabled",
            "High Concurrency Enabled",
            "Customize Compute Enabled",
            "Default Pool Name",
            "Default Pool Type",
            "Max Node Count",
            "Max Executors",
            "Environment Name",
            "Runtime Version",
        ]
    )

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/spark/settings")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    i = response.json()
    p = i.get("pool")
    dp = i.get("pool", {}).get("defaultPool", {})
    sp = i.get("pool", {}).get("starterPool", {})
    e = i.get("environment", {})

    new_data = {
        "Automatic Log Enabled": i.get("automaticLog").get("enabled"),
        "High Concurrency Enabled": i.get("highConcurrency").get(
            "notebookInteractiveRunEnabled"
        ),
        "Customize Compute Enabled": p.get("customizeComputeEnabled"),
        "Default Pool Name": dp.get("name"),
        "Default Pool Type": dp.get("type"),
        "Max Node Count": sp.get("maxNodeCount"),
        "Max Node Executors": sp.get("maxExecutors"),
        "Environment Name": e.get("name"),
        "Runtime Version": e.get("runtimeVersion"),
    }
    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    bool_cols = [
        "Automatic Log Enabled",
        "High Concurrency Enabled",
        "Customize Compute Enabled",
    ]
    int_cols = ["Max Node Count", "Max Executors"]

    df[bool_cols] = df[bool_cols].astype(bool)
    df[int_cols] = df[int_cols].astype(int)

    return df


def update_spark_settings(
    automatic_log_enabled: Optional[bool] = None,
    high_concurrency_enabled: Optional[bool] = None,
    customize_compute_enabled: Optional[bool] = None,
    default_pool_name: Optional[str] = None,
    max_node_count: Optional[int] = None,
    max_executors: Optional[int] = None,
    environment_name: Optional[str] = None,
    runtime_version: Optional[str] = None,
    workspace: Optional[str] = None,
):
    """
    Updates the spark settings for a workspace.

    Parameters
    ----------
    automatic_log_enabled : bool, default=None
        The status of the `automatic log <https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#automaticlogproperties>`_.
        Defaults to None which keeps the existing property setting.
    high_concurrency_enabled : bool, default=None
        The status of the `high concurrency <https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#highconcurrencyproperties>`_ for notebook interactive run.
        Defaults to None which keeps the existing property setting.
    customize_compute_enabled : bool, default=None
        `Customize compute <https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#poolproperties>`_ configurations for items.
        Defaults to None which keeps the existing property setting.
    default_pool_name : str, default=None
        `Default pool <https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#poolproperties>`_ for workspace.
        Defaults to None which keeps the existing property setting.
    max_node_count : int, default=None
        The `maximum node count <https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#starterpoolproperties>`_.
        Defaults to None which keeps the existing property setting.
    max_executors : int, default=None
        The `maximum executors <https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#starterpoolproperties>`_.
        Defaults to None which keeps the existing property setting.
    environment_name : str, default=None
        The name of the `default environment <https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#environmentproperties>`_. Empty string indicated there is no workspace default environment
        Defaults to None which keeps the existing property setting.
    runtime_version : str, default=None
        The `runtime version <https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#environmentproperties>`_.
        Defaults to None which keeps the existing property setting.
    workspace : str, default=None
        The name of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP
    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfS = get_spark_settings(workspace=workspace)

    if automatic_log_enabled is None:
        automatic_log_enabled = bool(dfS["Automatic Log Enabled"].iloc[0])
    if high_concurrency_enabled is None:
        high_concurrency_enabled = bool(dfS["High Concurrency Enabled"].iloc[0])
    if customize_compute_enabled is None:
        customize_compute_enabled = bool(dfS["Customize Compute Enabled"].iloc[0])
    if default_pool_name is None:
        default_pool_name = dfS["Default Pool Name"].iloc[0]
    if max_node_count is None:
        max_node_count = int(dfS["Max Node Count"].iloc[0])
    if max_executors is None:
        max_executors = int(dfS["Max Executors"].iloc[0])
    if environment_name is None:
        environment_name = dfS["Environment Name"].iloc[0]
    if runtime_version is None:
        runtime_version = dfS["Runtime Version"].iloc[0]

    request_body = {
        "automaticLog": {"enabled": automatic_log_enabled},
        "highConcurrency": {"notebookInteractiveRunEnabled": high_concurrency_enabled},
        "pool": {
            "customizeComputeEnabled": customize_compute_enabled,
            "defaultPool": {"name": default_pool_name, "type": "Workspace"},
            "starterPool": {
                "maxNodeCount": max_node_count,
                "maxExecutors": max_executors,
            },
        },
        "environment": {"name": environment_name, "runtimeVersion": runtime_version},
    }

    client = fabric.FabricRestClient()
    response = client.patch(
        f"/v1/workspaces/{workspace_id}/spark/settings", json=request_body
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The spark settings within the '{workspace}' workspace have been updated accordingly."
    )


def add_user_to_workspace(
    email_address: str, role_name: str, workspace: Optional[str] = None
):
    """
    Adds a user to a workspace.

    Parameters
    ----------
    email_address : str
        The email address of the user.
    role_name : str
        The `role <https://learn.microsoft.com/rest/api/power-bi/groups/add-group-user#groupuseraccessright>`_ of the user within the workspace.
    workspace : str, default=None
        The name of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    role_names = ["Admin", "Member", "Viewer", "Contributor"]
    role_name = role_name.capitalize()
    if role_name not in role_names:
        raise ValueError(
            f"{icons.red_dot} Invalid role. The 'role_name' parameter must be one of the following: {role_names}."
        )
    plural = "n" if role_name == "Admin" else ""

    client = fabric.PowerBIRestClient()

    request_body = {"emailAddress": email_address, "groupUserAccessRight": role_name}

    response = client.post(
        f"/v1.0/myorg/groups/{workspace_id}/users", json=request_body
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{email_address}' user has been added as a{plural} '{role_name}' within the '{workspace}' workspace."
    )


def delete_user_from_workspace(email_address: str, workspace: Optional[str] = None):
    """
    Removes a user from a workspace.

    Parameters
    ----------
    email_address : str
        The email address of the user.
    workspace : str, default=None
        The name of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.PowerBIRestClient()
    response = client.delete(f"/v1.0/myorg/groups/{workspace_id}/users/{email_address}")

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{email_address}' user has been removed from accessing the '{workspace}' workspace."
    )


def update_workspace_user(
    email_address: str, role_name: str, workspace: Optional[str] = None
):
    """
    Updates a user's role within a workspace.

    Parameters
    ----------
    email_address : str
        The email address of the user.
    role_name : str
        The `role <https://learn.microsoft.com/rest/api/power-bi/groups/add-group-user#groupuseraccessright>`_ of the user within the workspace.
    workspace : str, default=None
        The name of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    role_names = ["Admin", "Member", "Viewer", "Contributor"]
    role_name = role_name.capitalize()
    if role_name not in role_names:
        raise ValueError(
            f"{icons.red_dot} Invalid role. The 'role_name' parameter must be one of the following: {role_names}."
        )

    request_body = {"emailAddress": email_address, "groupUserAccessRight": role_name}

    client = fabric.PowerBIRestClient()
    response = client.put(f"/v1.0/myorg/groups/{workspace_id}/users", json=request_body)

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{email_address}' user has been updated to a '{role_name}' within the '{workspace}' workspace."
    )


def list_workspace_users(workspace: Optional[str] = None) -> pd.DataFrame:
    """
    A list of all the users of a workspace and their roles.

    Parameters
    ----------
    workspace : str, default=None
        The name of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe the users of a workspace and their properties.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = pd.DataFrame(columns=["User Name", "Email Address", "Role", "Type", "User ID"])
    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/roleAssignments")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json()["value"]:
        p = v.get("principal", {})

        new_data = {
            "User Name": p.get("displayName"),
            "User ID": p.get("id"),
            "Type": p.get("type"),
            "Role": v.get("role"),
            "Email Address": p.get("userDetails", {}).get("userPrincipalName"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def assign_workspace_to_dataflow_storage(
    dataflow_storage_account: str, workspace: Optional[str] = None
):
    """
    Assigns a dataflow storage account to a workspace.

    Parameters
    ----------
    dataflow_storage_account : str
        The name of the dataflow storage account.
    workspace : str, default=None
        The name of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = list_dataflow_storage_accounts()
    df_filt = df[df["Dataflow Storage Account Name"] == dataflow_storage_account]
    dataflow_storage_id = df_filt["Dataflow Storage Account ID"].iloc[0]

    client = fabric.PowerBIRestClient()

    request_body = {"dataflowStorageId": dataflow_storage_id}

    response = client.post(
        f"/v1.0/myorg/groups/{workspace_id}/AssignToDataflowStorage", json=request_body
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{dataflow_storage_account}' dataflow storage account has been assigned to the '{workspace}' workspacce."
    )


def list_capacities() -> pd.DataFrame:
    """
    Shows the capacities and their properties.

    Parameters
    ----------

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the capacities and their properties
    """

    df = pd.DataFrame(
        columns=["Id", "Display Name", "Sku", "Region", "State", "Admins"]
    )

    client = fabric.PowerBIRestClient()
    response = client.get("/v1.0/myorg/capacities")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for i in response.json()["value"]:
        new_data = {
            "Id": i.get("id").lower(),
            "Display Name": i.get("displayName"),
            "Sku": i.get("sku"),
            "Region": i.get("region"),
            "State": i.get("state"),
            "Admins": [i.get("admins", [])],
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def get_notebook_definition(
    notebook_name: str, workspace: Optional[str] = None, decode: Optional[bool] = True
):
    """
    Obtains the notebook definition.

    Parameters
    ----------
    notebook_name : str
        The name of the notebook.
    workspace : str, default=None
        The name of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    decode : bool, default=True
        If True, decodes the notebook definition file into .ipynb format.
        If False, obtains the notebook definition file in base64 format.

    Returns
    -------
    ipynb
        The notebook definition.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfI = fabric.list_items(workspace=workspace, type="Notebook")
    dfI_filt = dfI[dfI["Display Name"] == notebook_name]

    if len(dfI_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{notebook_name}' notebook does not exist within the '{workspace}' workspace."
        )

    notebook_id = dfI_filt["Id"].iloc[0]
    client = fabric.FabricRestClient()
    response = client.post(
        f"v1/workspaces/{workspace_id}/notebooks/{notebook_id}/getDefinition",
    )
    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)
    if response.status_code == 200:
        result = response.json()
    if response.status_code == 202:
        operationId = response.headers["x-ms-operation-id"]
        response = client.get(f"/v1/operations/{operationId}")
        response_body = json.loads(response.content)
        while response_body["status"] != "Succeeded":
            time.sleep(1)
            response = client.get(f"/v1/operations/{operationId}")
            response_body = json.loads(response.content)
        response = client.get(f"/v1/operations/{operationId}/result")
        result = response.json()

    df_items = pd.json_normalize(result["definition"]["parts"])
    df_items_filt = df_items[df_items["path"] == "notebook-content.py"]
    payload = df_items_filt["payload"].iloc[0]

    if decode:
        result = base64.b64decode(payload).decode("utf-8")
    else:
        result = payload

    return result


def import_notebook_from_web(
    notebook_name: str,
    url: str,
    description: Optional[str] = None,
    workspace: Optional[str] = None,
):
    """
    Creates a new notebook within a workspace based on a Jupyter notebook hosted in the web.

    Parameters
    ----------
    notebook_name : str
        The name of the notebook to be created.
    url : str
        The url of the Jupyter Notebook (.ipynb)
    description : str, default=None
        The description of the notebook.
        Defaults to None which does not place a description.
    workspace : str, default=None
        The name of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    client = fabric.FabricRestClient()
    dfI = fabric.list_items(workspace=workspace, type="Notebook")
    dfI_filt = dfI[dfI["Display Name"] == notebook_name]
    if len(dfI_filt) > 0:
        raise ValueError(
            f"{icons.red_dot} The '{notebook_name}' already exists within the '{workspace}' workspace."
        )

    # Fix links to go to the raw github file
    starting_text = "https://github.com/"
    starting_text_len = len(starting_text)
    if url.startswith(starting_text):
        url = f"https://raw.githubusercontent.com/{url[starting_text_len:]}".replace(
            "/blob/", "/"
        )

    response = requests.get(url)
    if response.status_code != 200:
        raise FabricHTTPException(response)
    file_content = response.content
    notebook_payload = base64.b64encode(file_content)

    request_body = {
        "displayName": notebook_name,
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.py",
                    "payload": notebook_payload,
                    "payloadType": "InlineBase64",
                }
            ],
        },
    }
    if description is not None:
        request_body["description"] = description

    response = client.post(f"v1/workspaces/{workspace_id}/notebooks", json=request_body)
    if response.status_code not in [201, 202]:
        raise FabricHTTPException(response)
    if response.status_code == 202:
        operationId = response.headers["x-ms-operation-id"]
        response = client.get(f"/v1/operations/{operationId}")
        response_body = json.loads(response.content)
        while response_body["status"] not in ["Succeeded", "Failed"]:
            time.sleep(1)
            response = client.get(f"/v1/operations/{operationId}")
            response_body = json.loads(response.content)
        if response_body["status"] != "Succeeded":
            raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{notebook_name}' notebook was created within the '{workspace}' workspace."
    )


def list_reports_using_semantic_model(
    dataset: str, workspace: Optional[str] = None
) -> pd.DataFrame:
    """
    Shows a list of all the reports (in all workspaces) which use a given semantic model.

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
        A pandas dataframe showing the reports which use a given semantic model.
    """

    df = pd.DataFrame(
        columns=[
            "Report Name",
            "Report Id",
            "Report Workspace Name",
            "Report Workspace Id",
        ]
    )

    workspace = fabric.resolve_workspace_name(workspace)
    dataset_id = resolve_dataset_id(dataset, workspace)
    client = fabric.PowerBIRestClient()
    response = client.get(
        f"metadata/relations/downstream/dataset/{dataset_id}?apiVersion=3"
    )

    response_json = response.json()

    for i in response_json.get("artifacts", []):
        object_workspace_id = i.get("workspace", {}).get("objectId")
        object_type = i.get("typeName")

        if object_type == "Report":
            new_data = {
                "Report Name": i.get("displayName"),
                "Report Id": i.get("objectId"),
                "Report Workspace Name": fabric.resolve_workspace_name(
                    object_workspace_id
                ),
                "Report Workspace Id": object_workspace_id,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_dataset_gateways(
    dataset: str, workspace: Optional[str] = None
) -> pd.DataFrame:
    """
    Shows a list of gateways that the specified dataset from the specified workspace can be bound to.

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
        A pandas dataframe showing a list of gateways that the specified dataset from the specified workspace can be bound to.
    """

    # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/discover-gateways-in-group

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    dataset_id = resolve_dataset_id(dataset, workspace)

    df = pd.DataFrame(
        columns=["Gateway Id", "Gateway Name", "Type", "Exponent", "Modulus"]
    )
    client = fabric.PowerBIRestClient()
    response = client.get(
        f"v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/Default.DiscoverGateways"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    for i in response.json()["value"]:
        new_data = {
            "Gateway Id": i.get("id"),
            "Gateway Name": i.get("name"),
            "Type": i.get("type"),
            "Exponent": i.get("publicKey", {}).get("exponent"),
            "Modulus": i.get("publicKey", {}).get("modulus"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def bind_dataset_to_gateway(
    dataset: str,
    gateway: str,
    workspace: Optional[str] = None,
    datasource_object_ids: Optional[str | List[str]] = None,
):
    """
    Binds a semantic model to a gateway.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    gateway : str
        Name of the gateway.
    datasource_object_ids : str | List[str], default=None
        The unique identifiers for the data sources in the gateway.
        Defaults to None as it is not required.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/bind-to-gateway-in-group

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    dataset_id = resolve_dataset_id(dataset, workspace)
    if isinstance(datasource_object_ids, str):
        datasource_object_ids = [datasource_object_ids]

    dfG = fabric.list_gateways()
    dfG_filt = dfG[dfG["Gateway Name"] == gateway]
    if len(dfG_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{gateway}' gateway is not a valid gateway."
        )
    gateway_id = dfG_filt["Gateway Id"].iloc[0]

    request_body = {"gatewayObjectId": gateway_id}
    if datasource_object_ids is not None:
        request_body["datasourceObjectIds"] = datasource_object_ids

    client = fabric.PowerBIRestClient()
    response = client.post(
        f"v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/Default.BindToGateway",
        json=request_body,
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{gateway}' gateway has been binded to the '{dataset}' semantic model within the '{workspace}' workspace."
    )


def connect_workspace_to_git(
    organization_name: str,
    project_name: str,
    repository_name: str,
    branch_name: str,
    directory_name: str,
    git_provider_type: str = "AzureDevOps",
    workspace: Optional[str] = None,
):

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/git/connect?tabs=HTTP

    workspace, workspace_id = resolve_workspace_name_and_id(workspace)

    request_body = {
        "gitProviderDetails": {
            "organizationName": organization_name,
            "projectName": project_name,
            "gitProviderType": git_provider_type,
            "repositoryName": repository_name,
            "branchName": branch_name,
            "directoryName": directory_name,
        }
    }

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/git/connect", json=request_body
    )
    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{workspace}' workspace has been connected to the '{project_name}' Git project within the '{repository_name}' repository."
    )


def disconnect_workspace_from_git(workspace: Optional[str] = None):

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/git/disconnect?tabs=HTTP

    workspace, workspace_id = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/git/disconnect")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{workspace}' workspace has been disconnected from Git."
    )


def get_git_status(workspace: Optional[str] = None) -> pd.DataFrame:

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/git/get-status?tabs=HTTP

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)

    df = pd.DataFrame(
        columns=[
            "Workspace Head",
            "Remote Commit Hash",
            "Object ID",
            "Logical ID",
            "Item Type",
            "Item Name",
            "Workspace Change",
            "Remote Change",
            "Conflict Type",
        ]
    )

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/git/status", lro_wait=True)

    if response not in [200, 202]:
        raise FabricHTTPException(response)

    result = lro(client, response).json()

    for v in result.get("value", []):
        changes = v.get("changes", [])
        item_metadata = changes.get("itemMetadata", {})
        item_identifier = item_metadata.get("itemIdentifier", {})

        new_data = {
            "Workspace Head": v.get("workspaceHead"),
            "Remote Commit Hash": v.get("remoteCommitHash"),
            "Object ID": item_identifier.get("objectId"),
            "Logical ID": item_identifier.get("logicalId"),
            "Item Type": item_metadata.get("itemType"),
            "Item Name": item_metadata.get("displayName"),
            "Remote Change": changes.get("remoteChange"),
            "Workspace Change": changes.get("workspaceChange"),
            "Conflict Type": changes.get("conflictType"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def get_git_connection(workspace: Optional[str] = None) -> pd.DataFrame:

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/git/get-status?tabs=HTTP

    workspace, workspace_id = resolve_workspace_name_and_id(workspace)

    df = pd.DataFrame(
        columns=[
            "Organization Name",
            "Project Name",
            "Git Provider Type",
            "Repository Name",
            "Branch Name",
            "Directory Name",
            "Workspace Head",
            "Last Sync Time",
            "Git Connection State",
        ]
    )

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/git/connection")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("value", []):
        provider_details = v.get("gitProviderDetails", {})
        sync_details = v.get("gitSyncDetails", {})
        new_data = {
            "Organization Name": provider_details.get("organizationName"),
            "Project Name": provider_details.get("projectName"),
            "Git Provider Type": provider_details.get("gitProviderType"),
            "Repository Name": provider_details.get("repositoryName"),
            "Branch Name": provider_details.get("branchName"),
            "Directory Name": provider_details.get("directoryName"),
            "Workspace Head": sync_details.get("head"),
            "Last Sync Time": sync_details.get("lastSyncTime"),
            "Git Conneciton State": v.get("gitConnectionState"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def initialize_git_connection(workspace: Optional[str] = None):

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/git/initialize-connection?tabs=HTTP

    workspace, workspace_id = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/git/initializeConnection")

    if response not in [200, 202]:
        raise FabricHTTPException(response)

    lro(client, response)

    print(f"The '{workspace}' workspace git connection has been initialized.")


def commit_to_git(
    comment: str, item_ids: str | List[str] = None, workspace: Optional[str] = None
):
    """
    Commits all or a selection of items within a workspace to Git.

    Parameters
    ----------
    comment : str
        The Git commit comment.
    item_ids : str | List[str], default=None
        A list of item Ids to commit to Git.
        Defaults to None which commits all items to Git.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/git/commit-to-git?tabs=HTTP

    workspace, workspace_id = resolve_workspace_name_and_id(workspace)

    gs = get_git_status(workspace=workspace)
    workspace_head = gs["Workspace Head"].iloc[0]

    if item_ids is None:
        commit_mode = "All"
    else:
        commit_mode = "Selective"

    if isinstance(item_ids, str):
        item_ids = [item_ids]

    request_body = {
        "mode": commit_mode,
        "workspaceHead": workspace_head,
        "comment": comment,
    }

    if item_ids is not None:
        request_body["items"] = [{"objectId": item_id} for item_id in item_ids]

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/git/commitToGit",
        json=request_body,
    )

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)

    lro(client, response)

    if commit_mode == "All":
        print(
            f"{icons.green_dot} All items within the '{workspace}' workspace have been committed to Git."
        )
    else:
        print(
            f"{icons.green_dot} The {item_ids} items ithin the '{workspace}' workspace have been committed to Git."
        )


def update_from_git(
    workspace_head: str,
    remote_commit_hash: str,
    conflict_resolution_policy: str,
    allow_override: Optional[bool] = False,
    workspace: Optional[str] = None,
):

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/git/update-from-git?tabs=HTTP

    workspace, workspace_id = resolve_workspace_name_and_id(workspace)

    conflict_resolution_policies = ["PreferWorkspace", "PreferRemote"]
    if "remote" in conflict_resolution_policies.lower():
        conflict_resolution_policies = "PreferRemote"
    elif "workspace" in conflict_resolution_policies.lower():
        conflict_resolution_policies = "PreferWorkspace"

    if conflict_resolution_policy not in conflict_resolution_policies:
        raise ValueError(
            f"{icons.red_dot} Invalid conflict resolution policy. Valid options: {conflict_resolution_policies}."
        )

    request_body = {
        "workspaceHead": workspace_head,
        "remoteCommitHash": remote_commit_hash,
        "conflictResolution": {
            "conflictResolutionType": "Workspace",
            "conflictResolutionPolicy": conflict_resolution_policy,
        },
        "options": {"allowOverrideItems": allow_override},
    }

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/git/updateFromGit",
        json=request_body,
    )

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)

    lro(client, response)

    print(
        f"{icons.green_dot} The '{workspace}' workspace has been updated with commits pushed to the connected branch."
    )


def provision_workspace_identity(workspace: Optional[str] = None):

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/provision-identity?tabs=HTTP

    workspace, workspace_id = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/provisionIdentity")

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)

    lro(client, response)

    print(
        f"{icons.green_dot} A workspace identity has been provisioned for the '{workspace}' workspace."
    )


def deprovision_workspace_identity(workspace: Optional[str] = None):

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/deprovision-identity?tabs=HTTP

    workspace, workspace_id = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/deprovisionIdentity")

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)

    lro(client, response)

    print(
        f"{icons.green_dot} The workspace identity has been deprovisioned from the '{workspace}' workspace."
    )


def list_deployment_pipelines() -> pd.DataFrame:

    df = pd.DataFrame(
        columns=["Deployment Pipeline ID", "Deployment Pipeline Name", "Description"]
    )

    client = fabric.FabricRestClient()
    response = client.get("/v1/deploymentPipelines")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Deployment Pipeline ID": v.get("id"),
                "Deployment Pipeline Name": v.get("displayName"),
                "Description": v.get("description"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_deployment_pipeline_stages(deployment_pipeline: str) -> pd.DataFrame:

    from sempy_labs._helper_functions import resolve_deployment_pipeline_id

    df = pd.DataFrame(
        columns=[
            "Deployment Pipeline Stage ID",
            "Deployment Pipeline Stage Name",
            "Order",
            "Description",
            "Workspace ID",
            "Workspace Name",
            "Public",
        ]
    )

    deployment_pipeline_id = resolve_deployment_pipeline_id(
        deployment_pipeline=deployment_pipeline
    )
    client = fabric.FabricRestClient()
    response = client.get(f"/v1/deploymentPipelines/{deployment_pipeline_id}/stages")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Deployment Pipeline Stage ID": v["id"],
                "Deployment Pipeline Stage Name": v["displayName"],
                "Description": v["description"],
                "Order": v["order"],
                "Workspace ID": v["workspaceId"],
                "Workspace Name": v["workspaceName"],
                "Public": v["isPublic"],
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df["Order"] = df["Order"].astype(int)
    df["Public"] = df["Public"].astype(bool)

    return df


def list_deployment_pipeline_stage_items(
    deployment_pipeline: str, stage_name: str
) -> pd.DataFrame:

    df = pd.DataFrame(
        columns=[
            "Deployment Pipeline Stage Item ID",
            "Deployment Pipeline Stage Item Name",
            "Item Type",
            "Source Item ID",
            "Target Item ID",
            "Last Deployment Time",
        ]
    )

    deployment_pipeline_id = resolve_deployment_pipeline_id(
        deployment_pipeline=deployment_pipeline
    )
    dfPS = list_deployment_pipeline_stages(deployment_pipeline=deployment_pipeline)
    dfPS_filt = dfPS[dfPS["Deployment Pipeline Stage Name"] == stage_name]

    if len(dfPS_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{stage_name}' stage does not exist within the '{deployment_pipeline}' deployment pipeline."
        )
    stage_id = dfPS_filt["Deployment Pipeline Stage ID"].iloc[0]

    client = fabric.FabricRestClient()
    response = client.get(
        f"/v1/deploymentPipelines/{deployment_pipeline_id}/stages/{stage_id}/items"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Deployment Pipeline Stage Item ID": v.get("itemId"),
                "Deployment Pipeline Stage Item Name": v.get("itemDisplayName"),
                "Item Type": v.get("itemType"),
                "Source Item ID": v.get("sourceItemId"),
                "Target Item ID": v.get("targetItemId"),
                "Last Deployment Time": v.get("lastDeploymentTime"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df["Last Deployment Time"] = pd.to_datetime(df["Last Deployment Time"])

    return df


def refresh_user_permissions():

    client = fabric.PowerBIRestClient()
    response = client.post("/v1.0/myorg/RefreshUserPermissions")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} User permissions have been refreshed.")


def list_workloads(capacity_name: str) -> pd.DataFrame:

    # https://learn.microsoft.com/en-us/rest/api/power-bi/capacities/get-workloads

    df = pd.DataFrame(
        columns=["Workload Name", "State", "Max Memory Percentage Set By User"]
    )

    capacity_id = resolve_capacity_id(capacity_name=capacity_name)

    client = fabric.PowerBIRestClient()
    response = client.get(f"/v1.0/myorg/capacities/{capacity_id}/Workloads")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("value", []):
        new_data = {
            "Workload Name": v.get("name"),
            "State": v.get("state"),
            "Max Memory Percentage Set By User": v.get("maxMemoryPercentageSetByUser"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    int_cols = ["Max Memory Percentage Set By User"]
    df[int_cols] = df[int_cols].astype(int)

    return df


def patch_workload(
    capacity_name: str,
    workload_name: str,
    state: Optional[bool] = None,
    max_memory_percentage: Optional[int] = None,
):

    # https://learn.microsoft.com/en-us/rest/api/power-bi/capacities/patch-workload

    capacity_id = resolve_capacity_id(capacity_name=capacity_name)

    states = ["Disabled", "Enabled", "Unsupported"]
    state = state.capitalize()
    if state is not None and state not in states:
        raise ValueError(
            f"{icons.red_dot} Invalid 'state' parameter. Please choose from these options: {states}."
        )
    if max_memory_percentage is not None and (
        max_memory_percentage < 0 or max_memory_percentage > 100
    ):
        raise ValueError(
            f"{icons.red_dot} Invalid max memory percentage. Must be a value between 0-100."
        )

    payload = {}
    if state is not None:
        payload["state"] = state
    if max_memory_percentage is not None:
        payload["maxMemoryPercentageSetByUser"] = max_memory_percentage

    client = fabric.PowerBIRestClient()
    response = client.patch(
        f"/v1.0/myorg/capacities/{capacity_id}/Workloads/{workload_name}", json=payload
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"'The '{workload_name}' workload within the '{capacity_name}' capacity has been updated."
    )


def create_external_data_share(
    item_id: UUID,
    paths: str | List[str],
    recipient: str,
    workspace: Optional[str] = None,
):

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/external-data-shares/create-external-data-share?tabs=HTTP

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfI = fabric.list_items(workspace=workspace)
    dfI_filt = dfI[dfI["Id"] == item_id]
    if len(dfI_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{item_id}' item Id does not exist within the '{workspace}' workspace."
        )
    item_name = dfI_filt["Display Name"].iloc[0]
    item_type = dfI_filt["Type"].iloc[0]

    if isinstance(paths, str):
        paths = [paths]

    payload = {"paths": [paths], "recipient": {"userPrincipalName": recipient}}

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/items/{item_id}/externalDataShares",
        json=payload,
    )

    if response.status_code != 201:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} An external data share was created for the '{item_name}' {item_type} within the '{workspace}' workspace for the {paths} paths."
    )


def revoke_external_data_share(
    external_data_share_id: UUID, item_id: UUID, workspace: Optional[str] = None
):

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/external-data-shares/revoke-external-data-share?tabs=HTTP

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfI = fabric.list_items(workspace=workspace)
    dfI_filt = dfI[dfI["Id"] == item_id]
    if len(dfI_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{item_id}' item Id does not exist within the '{workspace}' workspace."
        )
    item_name = dfI_filt["Display Name"].iloc[0]
    item_type = dfI_filt["Type"].iloc[0]

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/items/{item_id}/externalDataShares/{external_data_share_id}/revoke"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{external_data_share_id}' external data share for the '{item_name}' {item_type} within the '{workspace}' workspace has been revoked."
    )


def list_external_data_shares_in_item(
    item_id: UUID, workspace: Optional[str] = None
) -> pd.DataFrame:

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/external-data-shares/list-external-data-shares-in-item?tabs=HTTP

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfI = fabric.list_items(workspace=workspace)
    client = fabric.FabricRestClient()
    response = client.get(
        f"/v1/workspaces/{workspace_id}/items/{item_id}/externalDataShares"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(
        columns=[
            "External Data Share Id",
            "Paths",
            "Creator Principal Id",
            "Creater Principal Type",
            "Recipient UPN",
            "Status",
            "Expiration Time UTC",
            "Workspace Id",
            "Item Id",
            "Item Name",
            "Item Type",
            "Invitation URL",
        ]
    )

    responses = pagination(client, response)
    dfs = []

    for r in responses:
        for i in r.get("value", []):
            item_id = i.get("itemId")
            dfI_filt = dfI[dfI["Id"] == item_id]
            item_name, item_type = None, None
            if len(dfI_filt) > 0:
                item_name = dfI_filt["Display Name"].iloc[0]
                item_type = dfI_filt["Type"].iloc[0]

            new_data = {
                "External Data Share Id": i.get("id"),
                "Paths": [i.get("paths")],
                "Creator Principal Id": i.get("creatorPrincipal", {}).get("id"),
                "Creator Principal Type": i.get("creatorPrincipal", {}).get("type"),
                "Recipient UPN": i.get("recipient", {}).get("userPrincipalName"),
                "Status": i.get("status"),
                "Expiration Time UTC": i.get("expriationTimeUtc"),
                "Workspace Id": i.get("workspaceId"),
                "Item Id": item_id,
                "Item Name": item_name,
                "Item Type": item_type,
                "Invitation URL": i.get("invitationUrl"),
            }
            dfs.append(pd.DataFrame(new_data, index=[0]))
    df = pd.concat(dfs, ignore_index=True)

    return df


def list_item_schedules(
    item_id: UUID, job_type: str, workspace: Optional[str]
) -> pd.DataFrame:

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/list-item-schedules?tabs=HTTP

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(
        f"/v1/workspaces/{workspace_id}/items/{item_id}/jobs/{job_type}/schedules"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(
        columns=[
            "Job Schedule Id",
            "Enabled",
            "Created Date Time",
            "Start Date Time",
            "End Date Time",
            "Local Time Zone Id",
            "Type",
            "Weekdays",
            "Times",
            "Interval",
            "Owner Id",
            "Owner Type",
        ]
    )

    for i in response.json().get("value", []):
        config = i.get("configuration", {})
        own = i.get("owner", {})
        new_data = {
            "Job Schedule Id": i.get("id"),
            "Enabled": i.get("enabled"),
            "Created Date Time": i.get("creatdDateTime"),
            "Start Date Time": config.get("startDateTime"),
            "End Date Time": config.get("endDateTime"),
            "Local Time Zone Id": config.get("localTimeZoneId"),
            "Type": config.get("type"),
            "Weekdays": [config.get("weekdays", [])],
            "Times": [config.get("times", [])],
            "Interval": config.get("interval"),
            "Owner Id": own.get("id"),
            "Owner Type": own.get("type"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    bool_cols = ["Enabled"]
    int_cols = ["Interval"]
    df[bool_cols] = df[bool_cols].astype(bool)
    df[int_cols] = df[int_cols].astype(int)

    return df


def create_item_schedule(
    item_id: UUID,
    job_type: str,
    type: str,
    enabled: bool,
    start_date_time,
    end_date_time,
    local_time_zone: str,
    interval: Optional[int] = None,
    times: str | List[str] = None,
    weekdays: str | List[str] = None,
    workspace: Optional[str] = None,
):

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/create-item-schedule?tabs=HTTP#weeklyscheduleconfig

    # times format: "2024-04-30T23:59:00"

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    if isinstance(times, str):
        times = [times]
    if isinstance(weekdays, str):
        weekdays = [weekdays]

    type_list = ["Cron", "Daily", "Weekly"]
    type = type.capitalize()

    if type not in type_list:
        raise ValueError(
            f"{icons.red_dot} Invalid 'type' parameter. Valid options: {type_list}."
        )

    payload = {
        "enabled": enabled,
        "configuration": {
            "startDateTime": start_date_time,
            "endDateTime": end_date_time,
            "localTimeZoneId": local_time_zone,
            "type": type,
        },
    }

    if type == "Cron":
        if interval is None:
            raise ValueError(
                f"{icons.red_dot} Cron job schedules must have an interval."
            )
        payload["configuration"]["interval"] = interval
    if type == "Weekly":
        if weekdays is None:
            raise ValueError(f"{icons.red_dot}")
        payload["configuration"]["weekdays"] = weekdays
    if type in ["Weekly", "Daily"]:
        if times is None:
            raise ValueError(f"{icons.red_dot}")
        payload["configuration"]["times"] = times

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/items/{item_id}/jobs/{job_type}/schedules",
        json=payload,
    )

    if response.status_code != 201:
        raise FabricHTTPException(response)
