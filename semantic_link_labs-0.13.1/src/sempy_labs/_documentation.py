import sempy
import sempy.fabric as fabric
import pandas as pd
from typing import List, Optional
from sempy._utils._log import log


@log
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


@log
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
