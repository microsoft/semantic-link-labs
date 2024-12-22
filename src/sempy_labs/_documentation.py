import sempy
import sempy.fabric as fabric
import pandas as pd
from typing import List, Optional
import sempy_labs._icons as icons
import datetime
from sempy_labs._helper_functions import (
    save_as_delta_table,
    _conv_model_size,
    _resolve_workspace_capacity_name_id_sku,
    resolve_dataset_id,
    _get_max_run_id,
    resolve_workspace_name_and_id,
)
from sempy_labs.lakehouse import lakehouse_attached


def list_all_items(workspaces: Optional[str | List[str]] = None):

    df = pd.DataFrame(
        columns=[
            "Workspace Name",
            "Workspace Id",
            "Item Name",
            "Item Type",
            "Description",
        ]
    )

    if isinstance(workspaces, str):
        workspaces = [workspaces]

    dfW = fabric.list_workspaces()
    if workspaces is not None:
        dfW = dfW[dfW["Name"].isin(workspaces)]

    for _, r in dfW.iterrows():
        workspace_name = r["Name"]
        workspace_id = r["Id"]
        dfI = fabric.list_items(workspace=workspace_name)
        for _, r2 in dfI.iterrows():

            new_data = {
                "Workspace Name": workspace_name,
                "Workspace Id": workspace_id,
                "Item Name": r2["Name"],
                "Item Type": r2["Type"],
                "Description": r2["Description"],
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def data_dictionary(dataset: str, workspace: Optional[str | None] = None):

    from sempy_labs.tom import connect_semantic_model

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM

    df = pd.DataFrame(
        columns=[
            "Workspace Name",
            "Model Name",
            "Table Name",
            "Object Type",
            "Object Name",
            "Hidden Flag",
            "Description",
            "Display Folder",
            "Measure Formula",
        ]
    )

    with connect_semantic_model(
        dataset=dataset, readonly=True, workspace=workspace
    ) as tom:
        for t in tom.model.Tables:
            expr = None
            if tom.is_calculated_table(table_name=t.Name):
                pName = next(p.Name for p in t.Partitions)
                expr = t.Partitions[pName].Source.Expression

            new_data = {
                "Workspace Name": workspace,
                "Model Name": dataset,
                "Table Name": t.Name,
                "Object Type": t.ObjectType,
                "Object Name": t.Name,
                "Hidden Flag": t.IsHidden,
                "Description": t.Description,
                "Display Folder": None,
                "Measure Formula": expr,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
            cols = [c for c in t.Columns if c.Type != TOM.ColumnType.RowNumber]
            for c in cols:

                def get_calc_column_expression(table_name, column_name):
                    expr = None
                    if tom.is_calculated_column(
                        table_name=table_name, column_name=column_name
                    ):
                        expr = c.Expression
                    return expr

                new_data = {
                    "Workspace Name": workspace,
                    "Model Name": dataset,
                    "Table Name": t.Name,
                    "Object Type": c.ObjectType,
                    "Object Name": c.Name,
                    "Hidden Flag": c.IsHidden,
                    "Description": c.Description,
                    "Display Folder": c.DisplayFolder,
                    "Measure Formula": get_calc_column_expression(t.Name, c.Name),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
            for m in t.Measures:
                new_data = {
                    "Workspace Name": workspace,
                    "Model Name": dataset,
                    "Table Name": t.Name,
                    "Object Type": m.ObjectType,
                    "Object Name": m.Name,
                    "Hidden Flag": m.IsHidden,
                    "Description": m.Description,
                    "Display Folder": m.DisplayFolder,
                    "Measure Formula": m.Expression,
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

            if t.CalculationGroup is not None:
                for ci in t.CalculationGroup.CalculationItems:
                    new_data = {
                        "Workspace Name": workspace,
                        "Model Name": dataset,
                        "Table Name": t.Name,
                        "Object Type": "Calculation Item",
                        "Object Name": ci.Name,
                        "Hidden Flag": t.IsHidden,
                        "Description": ci.Description,
                        "Display Folder": None,
                        "Measure Formula": ci.Expression,
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

    return df


def save_semantic_model_metadata(
    dataset: str,
    workspace: Optional[str] = None,
    run_id: Optional[int] = None,
    time_stamp: Optional[datetime.datetime] = None,
):

    from sempy_labs._list_functions import list_tables

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    if run_id is None:
        run_id = _get_run_id(table_name="SLL_Measures")

    if time_stamp is None:
        time_stamp = datetime.datetime.now()

    capacity_id, capacity_name, sku, region = _resolve_workspace_capacity_name_id_sku(
        workspace
    )
    dataset_id = resolve_dataset_id(dataset, workspace)

    print(f"{icons.in_progress} Collecting semantic model metadata...")
    dfM = fabric.list_measures(dataset=dataset, workspace=workspace)[
        ["Table Name", "Measure Name", "Measure Expression"]
    ]
    dfC = fabric.list_columns(dataset=dataset, workspace=workspace, extended=True)[
        [
            "Table Name",
            "Column Name",
            "Type",
            "Data Type",
            "Column Cardinality",
            "Total Size",
            "Data Size",
            "Dictionary Size",
            "Hierarchy Size",
            "Encoding",
        ]
    ]

    total_size = dfC["Total Size"].sum()
    total_size = _conv_model_size(db_total_size=total_size)
    dfModel = pd.DataFrame({"Model Size": [total_size]})

    dfC = dfC[dfC["Type"] != "RowNumber"]
    dfT = list_tables(dataset=dataset, workspace=workspace, extended=True)[
        ["Name", "Type", "Row Count"]
    ]
    dfT = dfT.rename(columns={"Name": "Table Name"})
    dfR = fabric.list_relationships(dataset=dataset, workspace=workspace)
    dfP = fabric.list_partitions(dataset=dataset, workspace=workspace, extended=True)[
        [
            "Table Name",
            "Partition Name",
            "Mode",
            "Source Type",
            "Query",
            "Refreshed Time",
            "Modified Time",
            "Record Count",
            "Records per Segment",
            "Segment Count",
        ]
    ]

    dfRLS = fabric.get_row_level_security_permissions(
        dataset=dataset, workspace=workspace
    )

    dfH = fabric.list_hierarchies(dataset=dataset, workspace=workspace)
    dfCI = fabric.list_calculation_items(dataset=dataset, workspace=workspace)

    def add_cols(df, run_id, time_stamp):
        df.insert(0, "Capacity Name", capacity_name)
        df.insert(1, "Capacity Id", capacity_id)
        df.insert(2, "SKU", sku)
        df.insert(3, "Region", region)
        df.insert(4, "Workspace Name", workspace)
        df.insert(5, "Workspace Id", workspace_id)
        df.insert(6, "Dataset Name", dataset)
        df.insert(7, "Dataset Id", dataset_id)
        df["RunId"] = run_id
        df["Timestamp"] = time_stamp

        return df

    dataframes = [dfM, dfC, dfT, dfR, dfP, dfRLS, dfModel, dfH, dfCI]
    dataframes = [add_cols(df, run_id, time_stamp) for df in dataframes]
    dfM, dfC, dfT, dfR, dfP, dfRLS, dfModel, dfH, dfCI = dataframes

    dfModel_schema = {
        "Capacity_Name": "string",
        "Capacity_Id": "string",
        "SKU": "string",
        "Region": "string",
        "Workspace_Name": "string",
        "Workspace_Id": "string",
        "Dataset_Name": "string",
        "Dataset_Id": "string",
        "Model_Size": "long",
        "RunId": "long",
        "Timestamp": "timestamp",
    }
    dfM_schema = {
        "Capacity_Name": "string",
        "Capacity_Id": "string",
        "SKU": "string",
        "Region": "string",
        "Workspace_Name": "string",
        "Workspace_Id": "string",
        "Dataset_Name": "string",
        "Dataset_Id": "string",
        "Table_Name": "string",
        "Measure_Name": "string",
        "Measure_Expression": "string",
        "RunId": "long",
        "Timestamp": "timestamp",
    }
    dfC_schema = {
        "Capacity_Name": "string",
        "Capacity_Id": "string",
        "SKU": "string",
        "Region": "string",
        "Workspace_Name": "string",
        "Workspace_Id": "string",
        "Dataset_Name": "string",
        "Dataset_Id": "string",
        "Table_Name": "string",
        "Column_Name": "string",
        "Type": "string",
        "Data_Type": "string",
        "Column_Cardinality": "long",
        "Total_Size": "long",
        "Data_Size": "long",
        "Dictionary_Size": "long",
        "Hierarchy_Size": "long",
        "Encoding": "string",
        "RunId": "long",
        "Timestamp": "timestamp",
    }
    dfT_schema = {
        "Capacity_Name": "string",
        "Capacity_Id": "string",
        "SKU": "string",
        "Region": "string",
        "Workspace_Name": "string",
        "Workspace_Id": "string",
        "Dataset_Name": "string",
        "Dataset_Id": "string",
        "Table_Name": "string",
        "Type": "string",
        "Row_Count": "long",
        "Table_Size": "long",
        "RunId": "long",
        "Timestamp": "timestamp",
    }
    dfP_schema = {
        "Capacity_Name": "string",
        "Capacity_Id": "string",
        "SKU": "string",
        "Region": "string",
        "Workspace_Name": "string",
        "Workspace_Id": "string",
        "Dataset_Name": "string",
        "Dataset_Id": "string",
        "Table_Name": "string",
        "Partition_Name": "string",
        "Mode": "string",
        "Source_Type": "string",
        "Query": "string",
        "Refreshed_Time": "timestamp",
        "Modified_Time": "timestamp",
        "Record_Count": "long",
        "Records_per_Segment": "double",
        "Segment_Count": "long",
        "RunId": "long",
        "Timestamp": "timestamp",
    }
    dfH_schema = {
        "Capacity_Name": "string",
        "Capacity_Id": "string",
        "SKU": "string",
        "Region": "string",
        "Workspace_Name": "string",
        "Workspace_Id": "string",
        "Dataset_Name": "string",
        "Dataset_Id": "string",
        "Table_Name": "string",
        "Column_Name": "string",
        "Hierarchy_Name": "string",
        "Hierarchy_Description": "string",
        "Hierarchy_State": "string",
        "Level_Name": "string",
        "Level_Description": "string",
        "Level_Ordinal": "long",
        "RunId": "long",
        "Timestamp": "timestamp",
    }
    dfCI_schema = {
        "Capacity_Name": "string",
        "Capacity_Id": "string",
        "SKU": "string",
        "Region": "string",
        "Workspace_Name": "string",
        "Workspace_Id": "string",
        "Dataset_Name": "string",
        "Dataset_Id": "string",
        "Calculation_Group_Name": "string",
        "Hidden": "bool",
        "Precedence": "long",
        "Description": "string",
        "Calculation_Item_Name": "string",
        "Ordinal": "long",
        "Expression": "string",
        "Format_String_Expression": "string",
        "State": "string",
        "Error_Message": "string",
        "RunId": "long",
        "Timestamp": "timestamp",
    }

    dfs = {
        "Measures": [dfM, dfM_schema],
        "Columns": [dfC, dfC_schema],
        "Tables": [dfT, dfT_schema],
        "Relationships": [dfR, None],
        "Partitions": [dfP, dfP_schema],
        "RowLevelSecurity": [dfRLS, None],
        "Model": [dfModel, dfModel_schema],
        "Hierarchies": [dfH, dfH_schema],
        "CalculationItems": [dfCI, dfCI_schema],
    }
    print(f"{icons.in_progress} Saving semantic model metadata...")
    for name, (df, df_schema) in dfs.items():
        if not df.empty:
            save_as_delta_table(
                dataframe=df,
                delta_table_name=f"SLL_{name}",
                write_mode="append",
                schema=df_schema,
            )
        else:
            print(
                f"{icons.yellow_dot} The '{dataset}' semantic model within the '{workspace}' contains no {name.lower()}."
            )

    return dfC


def save_semantic_model_metadata_bulk(workspace: Optional[str | List[str]] = None):

    time_stamp = datetime.datetime.now()
    run_id = _get_run_id(table_name="SLL_Measures")
    if isinstance(workspace, str):
        workspace = [workspace]

    dfW = fabric.list_workspaces()
    if workspace is None:
        workspaces = dfW["Name"].tolist()
    else:
        workspaces = dfW[dfW["Name"].isin(workspace)]["Name"].tolist()

    for w in workspaces:
        dfD = fabric.list_datasets(workspace=w, mode="rest")
        for _, r in dfD.iterrows():
            d_name = r["Dataset Name"]
            save_semantic_model_metadata(
                dataset=d_name,
                workspace=workspace,
                run_id=run_id,
                time_stamp=time_stamp,
            )


def _get_run_id(table_name: str) -> int:

    from sempy_labs.lakehouse import get_lakehouse_tables

    if not lakehouse_attached():
        raise ValueError(
            f"{icons.red_dot} A lakehouse must be attached to the notebook."
        )

    dfLT = get_lakehouse_tables()
    dfLT_filt = dfLT[dfLT["Table Name"] == table_name]
    if len(dfLT_filt) == 0:
        run_id = 1
    else:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse_name = fabric.resolve_item_name(
            item_id=lakehouse_id, type="Lakehouse", workspace=None
        )

        run_id = _get_max_run_id(lakehouse=lakehouse_name, table_name=table_name) + 1

    return run_id
