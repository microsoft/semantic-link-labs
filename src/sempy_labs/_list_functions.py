import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    create_relationship_name,
    resolve_lakehouse_id,
    resolve_dataset_id,
    _decode_b64,
    pagination,
    lro,
    resolve_item_type,
)
import pandas as pd
import base64
import requests
from pyspark.sql import SparkSession
from typing import Optional
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException


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


def list_tables(
    dataset: str, workspace: Optional[str] = None, extended: Optional[bool] = False
) -> pd.DataFrame:
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
    extended : bool, default=False
        Adds additional columns including Vertipaq statistics.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the semantic model's tables and their properties.
    """

    from sempy_labs.tom import connect_semantic_model

    workspace = fabric.resolve_workspace_name(workspace)

    df = pd.DataFrame(
        columns=[
            "Name",
            "Description",
            "Hidden",
            "Data Category",
            "Type",
            "Refresh Policy",
            "Source Expression",
        ]
    )

    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=True
    ) as tom:
        if extended:
            dict_df = fabric.evaluate_dax(
                dataset=dataset,
                workspace=workspace,
                dax_string="""
                EVALUATE SELECTCOLUMNS(FILTER(INFO.STORAGETABLECOLUMNS(), [COLUMN_TYPE] = "BASIC_DATA"),[DIMENSION_NAME],[DICTIONARY_SIZE])
                """,
            )
            dict_sum = dict_df.groupby("[DIMENSION_NAME]")["[DICTIONARY_SIZE]"].sum()
            data = fabric.evaluate_dax(
                dataset=dataset,
                workspace=workspace,
                dax_string="""EVALUATE SELECTCOLUMNS(INFO.STORAGETABLECOLUMNSEGMENTS(),[TABLE_ID],[DIMENSION_NAME],[USED_SIZE])""",
            )
            data_sum = (
                data[
                    ~data["[TABLE_ID]"].str.startswith("R$")
                    & ~data["[TABLE_ID]"].str.startswith("U$")
                    & ~data["[TABLE_ID]"].str.startswith("H$")
                ]
                .groupby("[DIMENSION_NAME]")["[USED_SIZE]"]
                .sum()
            )
            hier_sum = (
                data[data["[TABLE_ID]"].str.startswith("H$")]
                .groupby("[DIMENSION_NAME]")["[USED_SIZE]"]
                .sum()
            )
            rel_sum = (
                data[data["[TABLE_ID]"].str.startswith("R$")]
                .groupby("[DIMENSION_NAME]")["[USED_SIZE]"]
                .sum()
            )
            uh_sum = (
                data[data["[TABLE_ID]"].str.startswith("U$")]
                .groupby("[DIMENSION_NAME]")["[USED_SIZE]"]
                .sum()
            )
            rc = fabric.evaluate_dax(
                dataset=dataset,
                workspace=workspace,
                dax_string="""
                SELECT [DIMENSION_NAME],[DIMENSION_CARDINALITY] FROM $SYSTEM.MDSCHEMA_DIMENSIONS
            """,
            )

        rows = []
        for t in tom.model.Tables:
            t_name = t.Name
            t_type = (
                "Calculation Group"
                if t.CalculationGroup
                else (
                    "Calculated Table"
                    if tom.is_calculated_table(table_name=t.Name)
                    else "Table"
                )
            )
            ref = bool(t.RefreshPolicy)
            ref_se = t.RefreshPolicy.SourceExpression if ref else None

            new_data = {
                "Name": t_name,
                "Description": t.Description,
                "Hidden": t.IsHidden,
                "Data Category": t.DataCategory,
                "Type": t_type,
                "Refresh Policy": ref,
                "Source Expression": ref_se,
            }

            if extended:
                dict_size = dict_sum.get(t_name, 0)
                data_size = data_sum.get(t_name, 0)
                h_size = hier_sum.get(t_name, 0)
                r_size = rel_sum.get(t_name, 0)
                u_size = uh_sum.get(t_name, 0)
                total_size = data_size + dict_size + h_size + r_size + u_size

                new_data.update(
                    {
                        "Row Count": (
                            rc[rc["DIMENSION_NAME"] == t_name][
                                "DIMENSION_CARDINALITY"
                            ].iloc[0]
                            if not rc.empty
                            else 0
                        ),
                        "Total Size": total_size,
                        "Dictionary Size": dict_size,
                        "Data Size": data_size,
                        "Hierarchy Size": h_size,
                        "Relationship Size": r_size,
                        "User Hierarchy Size": u_size,
                    }
                )

            rows.append(new_data)

        int_cols = [
            "Row Count",
            "Total Size",
            "Dictionary Size",
            "Data Size",
            "Hierarchy Size",
            "Relationship Size",
            "User Hierarchy Size",
        ]
        df[int_cols] = df[int_cols].astype(int)

        df = pd.DataFrame(rows)

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

    for v in response.json().get("value", []):
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
    response = client.get(f"/v1/workspaces/{workspace_id}/lakehouses")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
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
    response = client.get(f"/v1/workspaces/{workspace_id}/warehouses")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
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
    response = client.get(f"/v1/workspaces/{workspace_id}/sqlEndpoints")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):

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
    response = client.get(f"/v1/workspaces/{workspace_id}/mirroredWarehouses")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):

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
    response = client.get(f"/v1/workspaces/{workspace_id}/kqlDatabases")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
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
    response = client.get(f"/v1/workspaces/{workspace_id}/kqlQuerysets")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):

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
    response = client.get(f"/v1/workspaces/{workspace_id}/mlModels")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
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
    response = client.get(f"/v1/workspaces/{workspace_id}/eventstreams")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Eventstream Name": v.get("displayName"),
                "Eventstream ID": v.get("id"),
                "Description": v.get("description"),
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
    response = client.get(f"/v1/workspaces/{workspace_id}/dataPipelines")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Data Pipeline Name": v.get("displayName"),
                "Data Pipeline ID": v.get("id"),
                "Description": v.get("description"),
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
    response = client.get(f"/v1/workspaces/{workspace_id}/mlExperiments")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
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
    response = client.get(f"/v1/workspaces/{workspace_id}/datamarts")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in response.get("value", []):
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
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    request_body = {"displayName": warehouse}

    if description:
        request_body["description"] = description

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/warehouses/", json=request_body
    )

    lro(client, response, status_codes=[201, 202])

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

    responses = pagination(client, response)

    for r in responses:
        for i in r.get("value", []):
            principal = i.get("principal", {})
            new_data = {
                "User Name": principal.get("displayName"),
                "Role Name": i.get("role"),
                "Type": principal.get("type"),
                "User Email": principal.get("userDetails", {}).get("userPrincipalName"),
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
    Shows all shortcuts which exist in a Fabric lakehouse and their properties.

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
    else:
        lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)

    client = fabric.FabricRestClient()

    df = pd.DataFrame(
        columns=[
            "Shortcut Name",
            "Shortcut Path",
            "Source Type",
            "Source Workspace Id",
            "Source Workspace Name",
            "Source Item Id",
            "Source Item Name",
            "Source Item Type",
            "OneLake Path",
            "Connection Id",
            "Location",
            "Bucket",
            "SubPath",
        ]
    )

    response = client.get(
        f"/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for i in r.get("value", []):
            tgt = i.get("target", {})
            s3_compat = tgt.get("s3Compatible", {})
            gcs = tgt.get("googleCloudStorage", {})
            eds = tgt.get("externalDataShare", {})
            connection_id = (
                s3_compat.get("connectionId")
                or gcs.get("connectionId")
                or eds.get("connectionId")
                or None
            )
            location = s3_compat.get("location") or gcs.get("location") or None
            sub_path = s3_compat.get("subpath") or gcs.get("subpath") or None
            source_workspace_id = tgt.get("oneLake", {}).get("workspaceId")
            source_item_id = tgt.get("oneLake", {}).get("itemId")
            source_workspace_name = (
                fabric.resolve_workspace_name(source_workspace_id)
                if source_workspace_id is not None
                else None
            )

            new_data = {
                "Shortcut Name": i.get("name"),
                "Shortcut Path": i.get("path"),
                "Source Type": tgt.get("type"),
                "Source Workspace Id": source_workspace_id,
                "Source Workspace Name": source_workspace_name,
                "Source Item Id": source_item_id,
                "Source Item Name": (
                    fabric.resolve_item_name(
                        source_item_id, workspace=source_workspace_name
                    )
                    if source_item_id is not None
                    else None
                ),
                "Source Item Type": (
                    resolve_item_type(source_item_id, workspace=source_workspace_name)
                    if source_item_id is not None
                    else None
                ),
                "OneLake Path": tgt.get("oneLake", {}).get("path"),
                "Connection Id": connection_id,
                "Location": location,
                "Bucket": s3_compat.get("bucket"),
                "SubPath": sub_path,
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
        The `maximum node count <https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties>`_.
    min_executors : int
        The `minimum executors <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties>`_.
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
        f"/v1/workspaces/{workspace_id}/spark/pools", json=request_body
    )

    if response.status_code != 201:
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
        The `maximum node count <https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties>`_.
        Defaults to None which keeps the existing property setting.
    min_executors : int, default=None
        The `minimum executors <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties>`_.
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
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfC = fabric.list_capacities()
    dfC_filt = dfC[dfC["Display Name"] == capacity_name]

    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{capacity_name}' capacity does not exist."
        )

    capacity_id = dfC_filt["Id"].iloc[0]

    request_body = {"capacityId": capacity_id}

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/assignToCapacity",
        json=request_body,
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
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/unassign-from-capacity?tabs=HTTP
    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/unassignFromCapacity")

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
        The `maximum node count <https://learn.microsoft.com/en-us/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#starterpoolproperties>`_.
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
    email_address: str,
    role_name: str,
    principal_type: Optional[str] = "User",
    workspace: Optional[str] = None,
):
    """
    Adds a user to a workspace.

    Parameters
    ----------
    email_address : str
        The email address of the user.
    role_name : str
        The `role <https://learn.microsoft.com/rest/api/power-bi/groups/add-group-user#groupuseraccessright>`_ of the user within the workspace.
    principal_type : str, default='User'
        The `principal type <https://learn.microsoft.com/rest/api/power-bi/groups/add-group-user#principaltype>`_.
    workspace : str, default=None
        The name of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    role_names = ["Admin", "Member", "Viewer", "Contributor"]
    role_name = role_name.capitalize()
    if role_name not in role_names:
        raise ValueError(
            f"{icons.red_dot} Invalid role. The 'role_name' parameter must be one of the following: {role_names}."
        )
    plural = "n" if role_name == "Admin" else ""
    principal_types = ["App", "Group", "None", "User"]
    principal_type = principal_type.capitalize()
    if principal_type not in principal_types:
        raise ValueError(
            f"{icons.red_dot} Invalid princpal type. Valid options: {principal_types}."
        )

    client = fabric.PowerBIRestClient()

    request_body = {
        "emailAddress": email_address,
        "groupUserAccessRight": role_name,
        "principalType": principal_type,
        "identifier": email_address,
    }

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
    email_address: str,
    role_name: str,
    principal_type: Optional[str] = "User",
    workspace: Optional[str] = None,
):
    """
    Updates a user's role within a workspace.

    Parameters
    ----------
    email_address : str
        The email address of the user.
    role_name : str
        The `role <https://learn.microsoft.com/rest/api/power-bi/groups/add-group-user#groupuseraccessright>`_ of the user within the workspace.
    principal_type : str, default='User'
        The `principal type <https://learn.microsoft.com/rest/api/power-bi/groups/add-group-user#principaltype>`_.
    workspace : str, default=None
        The name of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    role_names = ["Admin", "Member", "Viewer", "Contributor"]
    role_name = role_name.capitalize()
    if role_name not in role_names:
        raise ValueError(
            f"{icons.red_dot} Invalid role. The 'role_name' parameter must be one of the following: {role_names}."
        )
    principal_types = ["App", "Group", "None", "User"]
    principal_type = principal_type.capitalize()
    if principal_type not in principal_types:
        raise ValueError(
            f"{icons.red_dot} Invalid princpal type. Valid options: {principal_types}."
        )

    request_body = {
        "emailAddress": email_address,
        "groupUserAccessRight": role_name,
        "principalType": principal_type,
        "identifier": email_address,
    }

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

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
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

    for i in response.json().get("value", []):
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

    result = lro(client, response).json()
    df_items = pd.json_normalize(result["definition"]["parts"])
    df_items_filt = df_items[df_items["path"] == "notebook-content.py"]
    payload = df_items_filt["payload"].iloc[0]

    if decode:
        result = _decode_b64(payload)
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

    lro(client, response, status_codes=[201, 202])

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
