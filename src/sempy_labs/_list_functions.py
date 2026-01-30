import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    create_relationship_name,
    format_dax_object_name,
    resolve_dataset_name_and_id,
    _update_dataframe_datatypes,
    _base_api,
    _create_dataframe,
    _run_spark_sql_query,
)
from sempy._utils._log import log
import pandas as pd
from typing import Optional
import sempy_labs._icons as icons
from uuid import UUID
import json
from collections import defaultdict


@log
def get_object_level_security(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows the object level security for the semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the object level security for the semantic model.
    """

    from sempy_labs.tom import connect_semantic_model

    columns = {
        "Role Name": "string",
        "Object Type": "string",
        "Table Name": "string",
        "Object Name": "string",
        "Metadata Permission": "string",
    }
    df = _create_dataframe(columns=columns)

    rows = []

    with connect_semantic_model(
        dataset=dataset, readonly=True, workspace=workspace
    ) as tom:

        for r in tom.model.Roles:
            for tp in r.TablePermissions:
                for cp in tp.ColumnPermissions:
                    rows.append(
                        {
                            "Role Name": r.Name,
                            "Object Type": "Column",
                            "Table Name": tp.Name,
                            "Object Name": cp.Name,
                            "Metadata Permission": cp.Permission,
                        }
                    )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
    return df


@log
def list_tables(
    dataset: str | UUID, workspace: Optional[str | UUID] = None, extended: bool = False
) -> pd.DataFrame:
    """
    Shows a semantic model's tables and their properties.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    columns = {
        "Name": "string",
        "Description": "string",
        "Hidden": "bool",
        "Data Category": "string",
        "Type": "string",
        "Refresh Policy": "bool",
        "Source Expression": "string",
    }

    df = _create_dataframe(columns=columns)

    with connect_semantic_model(
        dataset=dataset_id, workspace=workspace_id, readonly=True
    ) as tom:
        if extended:
            dict_df = fabric.evaluate_dax(
                dataset=dataset_id,
                workspace=workspace_id,
                dax_string="""
                EVALUATE SELECTCOLUMNS(FILTER(INFO.STORAGETABLECOLUMNS(), [COLUMN_TYPE] = "BASIC_DATA"),[DIMENSION_NAME],[DICTIONARY_SIZE])
                """,
            )
            dict_sum = dict_df.groupby("[DIMENSION_NAME]")["[DICTIONARY_SIZE]"].sum()
            data = fabric.evaluate_dax(
                dataset=dataset_id,
                workspace=workspace_id,
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
                dataset=dataset_id,
                workspace=workspace_id,
                dax_string="""
                SELECT [DIMENSION_NAME],[ROWS_COUNT] FROM $SYSTEM.DISCOVER_STORAGE_TABLES
                WHERE RIGHT ( LEFT ( TABLE_ID, 2 ), 1 ) <> '$'
            """,
            )

            model_size = (
                dict_sum.sum()
                + data_sum.sum()
                + hier_sum.sum()
                + rel_sum.sum()
                + uh_sum.sum()
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
                            rc[rc["DIMENSION_NAME"] == t_name]["ROWS_COUNT"].iloc[0]
                            if not rc.empty
                            else 0
                        ),
                        "Total Size": total_size,
                        "Dictionary Size": dict_size,
                        "Data Size": data_size,
                        "Hierarchy Size": h_size,
                        "Relationship Size": r_size,
                        "User Hierarchy Size": u_size,
                        "Partitions": int(len(t.Partitions)),
                        "Columns": sum(
                            1 for c in t.Columns if str(c.Type) != "RowNumber"
                        ),
                        "% DB": (
                            round((total_size / model_size) * 100, 2)
                            if model_size not in (0, None, float("nan"))
                            else 0.0
                        ),
                    }
                )

            rows.append(new_data)

        df = pd.DataFrame(rows)

        if extended:
            column_map = {
                "Row Count": "int",
                "Total Size": "int",
                "Dictionary Size": "int",
                "Data Size": "int",
                "Hierarchy Size": "int",
                "Relationship Size": "int",
                "User Hierarchy Size": "int",
                "Partitions": "int",
                "Columns": "int",
                "% DB": "float",
            }

            _update_dataframe_datatypes(dataframe=df, column_map=column_map)

    return df


@log
def list_annotations(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a semantic model's annotations and their properties.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the semantic model's annotations and their properties.
    """

    from sempy_labs.tom import connect_semantic_model

    workspace_id = resolve_workspace_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    columns = {
        "Object Name": "string",
        "Parent Object Name": "string",
        "Object Type": "string",
        "Annotation Name": "string",
        "Annotation Value": "string",
    }
    df = _create_dataframe(columns=columns)

    rows = []
    with connect_semantic_model(
        dataset=dataset_id, readonly=True, workspace=workspace_id
    ) as tom:

        mName = tom.model.Name
        for a in tom.model.Annotations:
            objectType = "Model"
            aName = a.Name
            aValue = a.Value
            rows.append(
                {
                    "Object Name": mName,
                    "Parent Object Name": None,
                    "Object Type": objectType,
                    "Annotation Name": aName,
                    "Annotation Value": aValue,
                }
            )
        for t in tom.model.Tables:
            objectType = "Table"
            tName = t.Name
            for ta in t.Annotations:
                taName = ta.Name
                taValue = ta.Value
                rows.append(
                    {
                        "Object Name": tName,
                        "Parent Object Name": mName,
                        "Object Type": objectType,
                        "Annotation Name": taName,
                        "Annotation Value": taValue,
                    }
                )
            for p in t.Partitions:
                pName = p.Name
                objectType = "Partition"
                for pa in p.Annotations:
                    paName = pa.Name
                    paValue = pa.Value
                    rows.append(
                        {
                            "Object Name": pName,
                            "Parent Object Name": tName,
                            "Object Type": objectType,
                            "Annotation Name": paName,
                            "Annotation Value": paValue,
                        }
                    )
            for c in t.Columns:
                objectType = "Column"
                cName = c.Name
                for ca in c.Annotations:
                    caName = ca.Name
                    caValue = ca.Value
                    rows.append(
                        {
                            "Object Name": cName,
                            "Parent Object Name": tName,
                            "Object Type": objectType,
                            "Annotation Name": caName,
                            "Annotation Value": caValue,
                        }
                    )
            for ms in t.Measures:
                objectType = "Measure"
                measName = ms.Name
                for ma in ms.Annotations:
                    maName = ma.Name
                    maValue = ma.Value
                    rows.append(
                        {
                            "Object Name": measName,
                            "Parent Object Name": tName,
                            "Object Type": objectType,
                            "Annotation Name": maName,
                            "Annotation Value": maValue,
                        }
                    )
            for h in t.Hierarchies:
                objectType = "Hierarchy"
                hName = h.Name
                for ha in h.Annotations:
                    haName = ha.Name
                    haValue = ha.Value
                    rows.append(
                        {
                            "Object Name": hName,
                            "Parent Object Name": tName,
                            "Object Type": objectType,
                            "Annotation Name": haName,
                            "Annotation Value": haValue,
                        }
                    )
        for d in tom.model.DataSources:
            dName = d.Name
            objectType = "Data Source"
            for da in d.Annotations:
                daName = da.Name
                daValue = da.Value
                rows.append(
                    {
                        "Object Name": dName,
                        "Parent Object Name": mName,
                        "Object Type": objectType,
                        "Annotation Name": daName,
                        "Annotation Value": daValue,
                    }
                )
        for r in tom.model.Relationships:
            rName = r.Name
            objectType = "Relationship"
            for ra in r.Annotations:
                raName = ra.Name
                raValue = ra.Value
                rows.append(
                    {
                        "Object Name": rName,
                        "Parent Object Name": mName,
                        "Object Type": objectType,
                        "Annotation Name": raName,
                        "Annotation Value": raValue,
                    }
                )
        for cul in tom.model.Cultures:
            culName = cul.Name
            objectType = "Translation"
            for cula in cul.Annotations:
                culaName = cula.Name
                culaValue = cula.Value
                rows.append(
                    {
                        "Object Name": culName,
                        "Parent Object Name": mName,
                        "Object Type": objectType,
                        "Annotation Name": culaName,
                        "Annotation Value": culaValue,
                    }
                )
        for e in tom.model.Expressions:
            eName = e.Name
            objectType = "Expression"
            for ea in e.Annotations:
                eaName = ea.Name
                eaValue = ea.Value
                rows.append(
                    {
                        "Object Name": eName,
                        "Parent Object Name": mName,
                        "Object Type": objectType,
                        "Annotation Name": eaName,
                        "Annotation Value": eaValue,
                    }
                )
        for per in tom.model.Perspectives:
            perName = per.Name
            objectType = "Perspective"
            for pera in per.Annotations:
                peraName = pera.Name
                peraValue = pera.Value
                rows.append(
                    {
                        "Object Name": perName,
                        "Parent Object Name": mName,
                        "Object Type": objectType,
                        "Annotation Name": peraName,
                        "Annotation Value": peraValue,
                    }
                )
        for rol in tom.model.Roles:
            rolName = rol.Name
            objectType = "Role"
            for rola in rol.Annotations:
                rolaName = rola.Name
                rolaValue = rola.Value
                rows.append(
                    {
                        "Object Name": rolName,
                        "Parent Object Name": mName,
                        "Object Type": objectType,
                        "Annotation Name": rolaName,
                        "Annotation Value": rolaValue,
                    }
                )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def list_columns(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    lakehouse: Optional[str] = None,
    lakehouse_workspace: Optional[str] = None,
) -> pd.DataFrame:
    """
    Shows a semantic model's columns and their properties.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
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

    workspace_id = resolve_workspace_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    fabric.refresh_tom_cache(workspace=workspace)

    dfP = fabric.list_partitions(dataset=dataset_id, workspace=workspace_id)

    isDirectLake = any(r["Mode"] == "DirectLake" for i, r in dfP.iterrows())

    dfC = fabric.list_columns(dataset=dataset_id, workspace=workspace_id)

    if isDirectLake:
        dfC["Column Cardinality"] = None
        sql_statements = []
        (lakeID, lakeName) = get_direct_lake_lakehouse(
            dataset=dataset_id,
            workspace=workspace_id,
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

        for o in sql_statements:
            tName = o[0]
            query = o[1]

            # Run the query
            df = _run_spark_sql_query(query)

            for column in df.columns:
                x = df.collect()[0][column]
                for i, r in dfC.iterrows():
                    if r["Table Name"] == tName and r["Source"] == column:
                        dfC.at[i, "Column Cardinality"] = x

        # Remove column added temporarily
        dfC.drop(columns=["Query"], inplace=True)

    return dfC


@log
def list_lakehouses(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the lakehouses within a workspace.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the lakehouses within a workspace.
    """

    columns = {
        "Lakehouse Name": "string",
        "Lakehouse ID": "string",
        "Description": "string",
        "OneLake Tables Path": "string",
        "OneLake Files Path": "string",
        "SQL Endpoint Connection String": "string",
        "SQL Endpoint ID": "string",
        "SQL Endpoint Provisioning Status": "string",
        "Schema Enabled": "bool",
        "Default Schema": "string",
        "Sensitivity Label Id": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/lakehouses",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            prop = v.get("properties", {})
            sqlEPProp = prop.get("sqlEndpointProperties", {})
            default_schema = prop.get("defaultSchema", None)

            rows.append(
                {
                    "Lakehouse Name": v.get("displayName"),
                    "Lakehouse ID": v.get("id"),
                    "Description": v.get("description"),
                    "OneLake Tables Path": prop.get("oneLakeTablesPath"),
                    "OneLake Files Path": prop.get("oneLakeFilesPath"),
                    "SQL Endpoint Connection String": sqlEPProp.get("connectionString"),
                    "SQL Endpoint ID": sqlEPProp.get("id"),
                    "SQL Endpoint Provisioning Status": sqlEPProp.get(
                        "provisioningStatus"
                    ),
                    "Schema Enabled": True if default_schema else False,
                    "Default Schema": default_schema,
                    "Sensitivity Label Id": v.get("sensitivityLabel", {}).get(
                        "sensitivityLabelId"
                    ),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def list_datamarts(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the datamarts within a workspace.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the datamarts within a workspace.
    """

    columns = {
        "Datamart Name": "string",
        "Datamart ID": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/datamarts", uses_pagination=True
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Datamart Name": v.get("displayName"),
                    "Datamart ID": v.get("id"),
                    "Description": v.get("description"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def update_item(
    item_type: str,
    current_name: str,
    new_name: str,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
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
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_type = item_type.replace(" ", "").capitalize()

    if item_type not in icons.itemTypes.keys():
        raise ValueError(
            f"{icons.red_dot} The '{item_type}' is not a valid item type. "
        )

    itemType = icons.itemTypes[item_type]

    dfI = fabric.list_items(workspace=workspace_id, type=item_type)
    dfI_filt = dfI[(dfI["Display Name"] == current_name)]

    if len(dfI_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{current_name}' {item_type} does not exist within the '{workspace_name}' workspace."
        )

    itemId = dfI_filt["Id"].iloc[0]

    payload = {"displayName": new_name}
    if description:
        payload["description"] = description

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/{itemType}/{itemId}",
        payload=payload,
        method="patch",
    )
    if description is None:
        print(
            f"{icons.green_dot} The '{current_name}' {item_type} within the '{workspace_name}' workspace has been updated to be named '{new_name}'"
        )
    else:
        print(
            f"{icons.green_dot} The '{current_name}' {item_type} within the '{workspace_name}' workspace has been updated to be named '{new_name}' and have a description of '{description}'"
        )


@log
def list_user_defined_functions(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a list of the user-defined functions within a semantic model.

    Parameters
    ----------
    dataset: str | uuid.UUID
        Name or UUID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the user-defined functions within a semantic model.
    """

    from sempy_labs.tom import connect_semantic_model

    columns = {
        "Function Name": "string",
        "Expression": "string",
        "Lineage Tag": "string",
    }
    df = _create_dataframe(columns=columns)
    rows = []
    with connect_semantic_model(dataset=dataset, workspace=workspace) as tom:
        for f in tom.model.Functions:
            rows.append(
                {
                    "Function Name": f.Name,
                    "Expression": f.Expression,
                    "Lineage Tag": f.LineageTag,
                }
            )

    if rows:
        df = pd.DataFrame(rows)

    return df


@log
def list_relationships(
    dataset: str | UUID, workspace: Optional[str | UUID] = None, extended: bool = False
) -> pd.DataFrame:
    """
    Shows a semantic model's relationships and their properties.

    Parameters
    ----------
    dataset: str | uuid.UUID
        Name or UUID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    extended : bool, default=False
        Fetches extended column information.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the object level security for the semantic model.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    fabric.refresh_tom_cache(workspace=workspace)

    dfR = fabric.list_relationships(dataset=dataset_id, workspace=workspace_id)
    dfR["From Object"] = format_dax_object_name(dfR["From Table"], dfR["From Column"])
    dfR["To Object"] = format_dax_object_name(dfR["To Table"], dfR["To Column"])

    if extended:
        # Used to map the Relationship IDs
        rel = fabric.evaluate_dax(
            dataset=dataset_id,
            workspace=workspace_id,
            dax_string="""
                SELECT
                [ID] AS [RelationshipID]
                ,[Name]
                FROM $SYSTEM.TMSCHEMA_RELATIONSHIPS
                """,
        )

        # USED_SIZE shows the Relationship Size where TABLE_ID starts with R$
        cs = fabric.evaluate_dax(
            dataset=dataset_id,
            workspace=workspace_id,
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

        column_map = {
            "Used Size": "int",
        }

        _update_dataframe_datatypes(dataframe=dfR, column_map=column_map)

    return dfR


@log
def list_kpis(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a semantic model's KPIs and their properties.

    Parameters
    ----------
    dataset: str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the KPIs for the semantic model.
    """

    from sempy_labs.tom import connect_semantic_model

    workspace_id = resolve_workspace_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    columns = {
        "Table Name": "string",
        "Measure Name": "string",
        "Target Expression": "string",
        "Target Format String": "string",
        "Target Description": "string",
        "Status Expression": "string",
        "Status Graphic": "string",
        "Status Description": "string",
        "Trend Expression": "string",
        "Trend Graphic": "string",
        "Trend Description": "string",
    }
    df = _create_dataframe(columns=columns)

    rows = []
    with connect_semantic_model(
        dataset=dataset_id, workspace=workspace_id, readonly=True
    ) as tom:

        for t in tom.model.Tables:
            for m in t.Measures:
                if m.KPI is not None:
                    rows.append(
                        {
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
                    )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def list_semantic_model_objects(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a list of semantic model objects.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.


    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of objects in the semantic model
    """
    from sempy_labs.tom import connect_semantic_model

    columns = {
        "Parent Name": "string",
        "Object Name": "string",
        "Object Type": "string",
    }
    df = _create_dataframe(columns=columns)

    rows = []
    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=True
    ) as tom:
        for t in tom.model.Tables:
            if t.CalculationGroup is not None:
                rows.append(
                    {
                        "Parent Name": t.Parent.Name,
                        "Object Name": t.Name,
                        "Object Type": "Calculation Group",
                    }
                )

                for ci in t.CalculationGroup.CalculationItems:
                    rows.append(
                        {
                            "Parent Name": t.Name,
                            "Object Name": ci.Name,
                            "Object Type": str(ci.ObjectType),
                        }
                    )
            elif any(str(p.SourceType) == "Calculated" for p in t.Partitions):
                rows.append(
                    {
                        "Parent Name": t.Parent.Name,
                        "Object Name": t.Name,
                        "Object Type": "Calculated Table",
                    }
                )
            else:
                rows.append(
                    {
                        "Parent Name": t.Parent.Name,
                        "Object Name": t.Name,
                        "Object Type": str(t.ObjectType),
                    }
                )
            for c in t.Columns:
                if str(c.Type) != "RowNumber":
                    if str(c.Type) == "Calculated":
                        rows.append(
                            {
                                "Parent Name": c.Parent.Name,
                                "Object Name": c.Name,
                                "Object Type": "Calculated Column",
                            }
                        )
                    else:
                        rows.append(
                            {
                                "Parent Name": c.Parent.Name,
                                "Object Name": c.Name,
                                "Object Type": str(c.ObjectType),
                            }
                        )
            for m in t.Measures:
                rows.append(
                    {
                        "Parent Name": m.Parent.Name,
                        "Object Name": m.Name,
                        "Object Type": str(m.ObjectType),
                    }
                )
            for h in t.Hierarchies:
                rows.append(
                    {
                        "Parent Name": h.Parent.Name,
                        "Object Name": h.Name,
                        "Object Type": str(h.ObjectType),
                    }
                )
                for lev in h.Levels:
                    rows.append(
                        {
                            "Parent Name": lev.Parent.Name,
                            "Object Name": lev.Name,
                            "Object Type": str(lev.ObjectType),
                        }
                    )
            for p in t.Partitions:
                rows.append(
                    {
                        "Parent Name": p.Parent.Name,
                        "Object Name": p.Name,
                        "Object Type": str(p.ObjectType),
                    }
                )
        for r in tom.model.Relationships:
            rName = create_relationship_name(
                r.FromTable.Name, r.FromColumn.Name, r.ToTable.Name, r.ToColumn.Name
            )
            rows.append(
                {
                    "Parent Name": r.Parent.Name,
                    "Object Name": rName,
                    "Object Type": str(r.ObjectType),
                }
            )
        for role in tom.model.Roles:
            rows.append(
                {
                    "Parent Name": role.Parent.Name,
                    "Object Name": role.Name,
                    "Object Type": str(role.ObjectType),
                }
            )
            for rls in role.TablePermissions:
                rows.append(
                    {
                        "Parent Name": role.Name,
                        "Object Name": rls.Name,
                        "Object Type": str(rls.ObjectType),
                    }
                )
        for tr in tom.model.Cultures:
            rows.append(
                {
                    "Parent Name": tr.Parent.Name,
                    "Object Name": tr.Name,
                    "Object Type": str(tr.ObjectType),
                }
            )
        for per in tom.model.Perspectives:
            rows.append(
                {
                    "Parent Name": per.Parent.Name,
                    "Object Name": per.Name,
                    "Object Type": str(per.ObjectType),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def list_shortcuts(
    lakehouse: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    path: Optional[str] = None,
) -> pd.DataFrame:
    """
    Shows all shortcuts which exist in a Fabric lakehouse and their properties.

    *** NOTE: This function has been moved to the lakehouse subpackage. Please repoint your code to use that location. ***

    Parameters
    ----------
    lakehouse : str, default=None
        The Fabric lakehouse name.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace in which lakehouse resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    path: str, default=None
        The path within lakehouse where to look for shortcuts. If provied, must start with either "Files" or "Tables". Examples: Tables/FolderName/SubFolderName; Files/FolderName/SubFolderName.
        Defaults to None which will retun all shortcuts on the given lakehouse

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing all the shortcuts which exist in the specified lakehouse.
    """

    from sempy_labs.lakehouse._shortcuts import list_shortcuts

    print(
        f"{icons.warning} This function has been moved to the lakehouse subpackage. Please repoint your code to use that location."
    )

    return list_shortcuts(lakehouse=lakehouse, workspace=workspace, path=path)


@log
def list_reports_using_semantic_model(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a list of all the reports which use a given semantic model. This is limited to the reports which are in the same workspace as the semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the reports which use a given semantic model.
    """

    # df = pd.DataFrame(
    #    columns=[
    #        "Report Name",
    #        "Report Id",
    #        "Report Workspace Name",
    #        "Report Workspace Id",
    #    ]
    # )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    dfR = fabric.list_reports(workspace=workspace_id)
    dfR_filt = dfR[
        (dfR["Dataset Id"] == dataset_id)
        & (dfR["Dataset Workspace Id"] == workspace_id)
    ][["Name", "Id"]]
    dfR_filt.rename(columns={"Name": "Report Name", "Id": "Report Id"}, inplace=True)
    dfR_filt["Report Workspace Name"] = workspace_name
    dfR_filt["Report Workspace Id"] = workspace_id

    return dfR_filt

    # response = _base_api(request=f"metadata/relations/downstream/dataset/{dataset_id}?apiVersion=3")

    # response_json = response.json()

    # for i in response_json.get("artifacts", []):
    #    object_workspace_id = i.get("workspace", {}).get("objectId")
    #    object_type = i.get("typeName")

    #    if object_type == "Report":
    #        new_data = {
    #            "Report Name": i.get("displayName"),
    #            "Report Id": i.get("objectId"),
    #            "Report Workspace Name": fabric.resolve_workspace_name(
    #                object_workspace_id
    #            ),
    #            "Report Workspace Id": object_workspace_id,
    #        }
    #        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)


@log
def list_report_semantic_model_objects(
    dataset: str | UUID, workspace: Optional[str | UUID] = None, extended: bool = False
) -> pd.DataFrame:
    """
    Shows a list of semantic model objects (i.e. columns, measures, hierarchies) used in all reports which feed data from
    a given semantic model.

    Note: As with all functions which rely on the ReportWrapper, this function requires the report(s) to be in the 'PBIR' format.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    extended: bool, default=False
        If True, adds an extra column called 'Valid Semantic Model Object' which identifies whether the semantic model object used
        in the report exists in the semantic model which feeds data to the report.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of semantic model objects (i.e. columns, measures, hierarchies) used in all reports which feed data from
    a given semantic model.
    """

    from sempy_labs.report import ReportWrapper
    from sempy_labs.tom import connect_semantic_model

    columns = {
        "Report Name": "string",
        "Report Workspace Name": "string",
        "Table Name": "string",
        "Object Name": "string",
        "Object Type": "string",
        "Report Source": "string",
        "Report Source Object": "string",
    }
    dfRO = _create_dataframe(columns=columns)

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    # Collect all reports which use the semantic model
    dfR = list_reports_using_semantic_model(dataset=dataset_id, workspace=workspace_id)

    if len(dfR) == 0:
        return dfRO

    for _, r in dfR.iterrows():
        report_name = r["Report Name"]
        report_workspace = r["Report Workspace Name"]

        rpt = ReportWrapper(report=report_name, workspace=report_workspace)
        # Collect all semantic model objects used in the report
        dfRSO = rpt.list_semantic_model_objects()
        dfRSO["Report Name"] = report_name
        dfRSO["Report Workspace Name"] = report_workspace
        colName = "Report Name"
        dfRSO.insert(0, colName, dfRSO.pop(colName))
        colName = "Report Workspace Name"
        dfRSO.insert(1, colName, dfRSO.pop(colName))

        dfRO = pd.concat([dfRO, dfRSO], ignore_index=True)

    # Collect all semantic model objects
    if extended:
        with connect_semantic_model(
            dataset=dataset_id, readonly=True, workspace=workspace_id
        ) as tom:
            for index, row in dfRO.iterrows():
                object_type = row["Object Type"]
                if object_type == "Measure":
                    dfRO.at[index, "Valid Semantic Model Object"] = any(
                        o.Name == row["Object Name"] for o in tom.all_measures()
                    )
                elif object_type == "Column":
                    dfRO.at[index, "Valid Semantic Model Object"] = any(
                        format_dax_object_name(c.Parent.Name, c.Name)
                        == format_dax_object_name(row["Table Name"], row["Object Name"])
                        for c in tom.all_columns()
                    )
                elif object_type == "Hierarchy":
                    dfRO.at[index, "Valid Semantic Model Object"] = any(
                        format_dax_object_name(h.Parent.Name, h.Name)
                        == format_dax_object_name(row["Table Name"], row["Object Name"])
                        for h in tom.all_hierarchies()
                    )

    return dfRO


@log
def list_semantic_model_object_report_usage(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    include_dependencies: bool = False,
    extended: bool = False,
) -> pd.DataFrame:
    """
    Shows a list of semantic model objects and how many times they are referenced in all reports which rely on this semantic model.

    Requirement: Reports must be in the PBIR format.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    include_dependencies : bool, default=False
        If True, includes measure dependencies.
    extended: bool, default=False
        If True, adds columns 'Total Size', 'Data Size', 'Dictionary Size', 'Hierarchy Size' based on Vertipaq statistics.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of semantic model objects and how many times they are referenced in all reports which rely on this semantic model. By default, the dataframe
        is sorted descending by 'Report Usage Count'.
    """

    from sempy_labs._model_dependencies import get_model_calc_dependencies
    from sempy_labs._helper_functions import format_dax_object_name

    workspace_id = resolve_workspace_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    fabric.refresh_tom_cache(workspace=workspace)

    dfR = list_report_semantic_model_objects(dataset=dataset_id, workspace=workspace_id)
    usage_column_name = "Report Usage Count"

    if not include_dependencies:
        final_df = (
            dfR.groupby(["Table Name", "Object Name", "Object Type"])
            .size()
            .reset_index(name=usage_column_name)
        )
    else:
        df = pd.DataFrame(columns=["Table Name", "Object Name", "Object Type"])
        dep = get_model_calc_dependencies(dataset=dataset_id, workspace=workspace_id)

        for i, r in dfR.iterrows():
            object_type = r["Object Type"]
            table_name = r["Table Name"]
            object_name = r["Object Name"]
            new_data = {
                "Table Name": table_name,
                "Object Name": object_name,
                "Object Type": object_type,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
            df["Object Type"] = df["Object Type"].replace(
                "Attribute Hierarchy", "Column"
            )
            if object_type in ["Measure", "Calc Column", "Calc Table", "Hierarchy"]:
                df_filt = dep[dep["Object Name"] == object_name][
                    ["Referenced Table", "Referenced Object", "Referenced Object Type"]
                ]
                df_filt.rename(
                    columns={
                        "Referenced Table": "Table Name",
                        "Referenced Object": "Object Name",
                        "Referenced Object Type": "Object Type",
                    },
                    inplace=True,
                )

                df = pd.concat([df, df_filt], ignore_index=True)

        final_df = (
            df.groupby(["Table Name", "Object Name", "Object Type"])
            .size()
            .reset_index(name=usage_column_name)
        )

    if extended:
        final_df["Object"] = format_dax_object_name(
            final_df["Table Name"], final_df["Object Name"]
        )
        dfC = fabric.list_columns(
            dataset=dataset_id, workspace=workspace_id, extended=True
        )
        dfC["Object"] = format_dax_object_name(dfC["Table Name"], dfC["Column Name"])
        final_df = pd.merge(
            final_df,
            dfC[
                [
                    "Object",
                    "Total Size",
                    "Data Size",
                    "Dictionary Size",
                    "Hierarchy Size",
                ]
            ],
            on="Object",
            how="left",
        )

        ext_int_cols = ["Total Size", "Data Size", "Dictionary Size", "Hierarchy Size"]
        final_df[ext_int_cols] = final_df[ext_int_cols].fillna(0).astype(int)
        final_df.drop("Object", axis=1, inplace=True)

    int_cols = [usage_column_name]
    final_df[int_cols] = final_df[int_cols].astype(int)

    final_df = final_df[final_df["Object Type"] != "Table"].sort_values(
        by=usage_column_name, ascending=False
    )

    final_df.reset_index(drop=True, inplace=True)

    return final_df


@log
def list_server_properties(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Lists the `properties <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.serverproperty?view=analysisservices-dotnet>`_ of the Analysis Services instance.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the server properties.
    """

    tom_server = fabric.create_tom_server(
        dataset=None, readonly=True, workspace=workspace
    )

    rows = [
        {
            "Name": sp.Name,
            "Value": sp.Value,
            "Default Value": sp.DefaultValue,
            "Is Read Only": sp.IsReadOnly,
            "Requires Restart": sp.RequiresRestart,
            "Units": sp.Units,
            "Category": sp.Category,
        }
        for sp in tom_server.ServerProperties
    ]

    tom_server.Dispose()
    df = pd.DataFrame(rows)

    column_map = {
        "Is Read Only": "bool",
        "Requires Restart": "bool",
    }

    _update_dataframe_datatypes(dataframe=df, column_map=column_map)

    return df


@log
def list_semantic_model_errors(
    dataset: str | UUID, workspace: Optional[str | UUID]
) -> pd.DataFrame:
    """
    Shows a list of a semantic model's errors and their error messages (if they exist).

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the errors and error messages for a given semantic model.
    """

    from sempy_labs.tom import connect_semantic_model

    df = pd.DataFrame(
        columns=["Object Type", "Table Name", "Object Name", "Error Message"]
    )

    error_rows = []

    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=True
    ) as tom:
        # Define mappings of TOM objects to object types and attributes
        error_checks = [
            ("Column", tom.all_columns, lambda o: o.ErrorMessage),
            ("Partition", tom.all_partitions, lambda o: o.ErrorMessage),
            (
                "Partition - Data Coverage Expression",
                tom.all_partitions,
                lambda o: (
                    o.DataCoverageDefinition.ErrorMessage
                    if o.DataCoverageDefinition
                    else ""
                ),
            ),
            ("Row Level Security", tom.all_rls, lambda o: o.ErrorMessage),
            ("Calculation Item", tom.all_calculation_items, lambda o: o.ErrorMessage),
            ("Measure", tom.all_measures, lambda o: o.ErrorMessage),
            (
                "Measure - Detail Rows Expression",
                tom.all_measures,
                lambda o: (
                    o.DetailRowsDefinition.ErrorMessage
                    if o.DetailRowsDefinition
                    else ""
                ),
            ),
            (
                "Measure - Format String Expression",
                tom.all_measures,
                lambda o: (
                    o.FormatStringDefinition.ErrorMessage
                    if o.FormatStringDefinition
                    else ""
                ),
            ),
            (
                "Calculation Group - Multiple or Empty Selection Expression",
                tom.all_calculation_groups,
                lambda o: (
                    o.CalculationGroup.MultipleOrEmptySelectionExpression.ErrorMessage
                    if o.CalculationGroup.MultipleOrEmptySelectionExpression
                    else ""
                ),
            ),
            (
                "Calculation Group - No Selection Expression",
                tom.all_calculation_groups,
                lambda o: (
                    o.CalculationGroup.NoSelectionExpression.ErrorMessage
                    if o.CalculationGroup.NoSelectionExpression
                    else ""
                ),
            ),
            ("Function", tom.all_functions, lambda o: o.ErrorMessage),
        ]

        # Iterate over all error checks
        for object_type, getter, error_extractor in error_checks:
            for obj in getter():
                error_message = error_extractor(obj)
                if error_message:  # Only add rows if there's an error message
                    error_rows.append(
                        {
                            "Object Type": object_type,
                            "Table Name": obj.Parent.Name,
                            "Object Name": obj.Name,
                            "Error Message": error_message,
                        }
                    )

    if error_rows:
        df = pd.DataFrame(error_rows)

    return df


@log
def list_synonyms(dataset: str | UUID, workspace: Optional[str] = None):

    from sempy_labs.tom import connect_semantic_model

    columns = {
        "Culture Name": "string",
        "Table Name": "string",
        "Object Name": "string",
        "Object Type": "string",
        "Synonym": "string",
        "Type": "string",
        "State": "string",
        "Source": "string",
        "Weight": "float_fillna",
        "Last Modified": "string",
    }

    df = _create_dataframe(columns=columns)

    rows = []
    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=True
    ) as tom:
        for c in tom.model.Cultures:
            if c.LinguisticMetadata is not None:
                lm = json.loads(c.LinguisticMetadata.Content)
                if "Entities" in lm:
                    for _, v in lm.get("Entities", []).items():
                        binding = v.get("Definition", {}).get("Binding", {})

                        t_name = binding.get("ConceptualEntity")
                        object_name = binding.get("ConceptualProperty")

                        if object_name is None:
                            object_type = "Table"
                            object_name = t_name
                        elif any(
                            m.Name == object_name and m.Parent.Name == t_name
                            for m in tom.all_measures()
                        ):
                            object_type = "Measure"
                        elif any(
                            m.Name == object_name and m.Parent.Name == t_name
                            for m in tom.all_columns()
                        ):
                            object_type = "Column"
                        elif any(
                            m.Name == object_name and m.Parent.Name == t_name
                            for m in tom.all_hierarchies()
                        ):
                            object_type = "Hierarchy"

                        merged_terms = defaultdict(dict)
                        for t in v.get("Terms", []):
                            for term, properties in t.items():
                                merged_terms[term].update(properties)

                        for term, props in merged_terms.items():
                            new_data = {
                                "Culture Name": lm.get("Language"),
                                "Table Name": t_name,
                                "Object Name": object_name,
                                "Object Type": object_type,
                                "Synonym": term,
                                "Type": props.get("Type"),
                                "State": props.get("State"),
                                "Source": props.get("Source", {}).get("Agent"),
                                "Weight": props.get("Weight"),
                                "Last Modified": props.get("LastModified"),
                            }

                            # Skip concatenation if new_data is empty or invalid
                            if any(new_data.values()):
                                rows.append(new_data)

    if rows:
        df = pd.DataFrame(rows)
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df
