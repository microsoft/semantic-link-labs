import sempy.fabric as fabric
import pandas as pd
from sempy_labs._helper_functions import format_dax_object_name
import sempy_labs._icons as icons
from typing import Any, Dict, Optional
from anytree import Node, RenderTree
from sempy._utils._log import log


@log
def get_measure_dependencies(
    dataset: str, workspace: Optional[str] = None
) -> pd.DataFrame:
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

    workspace = fabric.resolve_workspace_name(workspace)

    dep = fabric.evaluate_dax(
        dataset=dataset,
        workspace=workspace,
        dax_string="""
        SELECT
         [TABLE] AS [Table Name]
        ,[OBJECT] AS [Object Name]
        ,[OBJECT_TYPE] AS [Object Type]
        ,[REFERENCED_TABLE] AS [Referenced Table]
        ,[REFERENCED_OBJECT] AS [Referenced Object]
        ,[REFERENCED_OBJECT_TYPE] AS [Referenced Object Type]
        FROM $SYSTEM.DISCOVER_CALC_DEPENDENCY
        WHERE [OBJECT_TYPE] = 'MEASURE'
        """,
    )

    dep["Object Type"] = dep["Object Type"].str.capitalize()
    dep["Referenced Object Type"] = dep["Referenced Object Type"].str.capitalize()

    dep["Full Object Name"] = format_dax_object_name(
        dep["Table Name"], dep["Object Name"]
    )
    dep["Referenced Full Object Name"] = format_dax_object_name(
        dep["Referenced Table"], dep["Referenced Object"]
    )
    dep["Parent Node"] = dep["Object Name"]

    df = dep

    df["Done"] = df.apply(
        lambda row: False if row["Referenced Object Type"] == "Measure" else True,
        axis=1,
    )

    while any(df["Done"] == False):
        for i, r in df.iterrows():
            rObjFull = r["Referenced Full Object Name"]
            rObj = r["Referenced Object"]
            if r["Done"] == False:
                dep_filt = dep[dep["Full Object Name"] == rObjFull]

                for index, dependency in dep_filt.iterrows():
                    d = True
                    if dependency[5] == "Measure":
                        d = False
                        df = pd.concat(
                            [
                                df,
                                pd.DataFrame(
                                    [
                                        {
                                            "Table Name": r["Table Name"],
                                            "Object Name": r["Object Name"],
                                            "Object Type": r["Object Type"],
                                            "Referenced Object": dependency[4],
                                            "Referenced Table": dependency[3],
                                            "Referenced Object Type": dependency[5],
                                            "Done": d,
                                            "Full Object Name": r["Full Object Name"],
                                            "Referenced Full Object Name": dependency[
                                                7
                                            ],
                                            "Parent Node": rObj,
                                        }
                                    ]
                                ),
                            ],
                            ignore_index=True,
                        )
                    else:
                        df = pd.concat(
                            [
                                df,
                                pd.DataFrame(
                                    [
                                        {
                                            "Table Name": r["Table Name"],
                                            "Object Name": r["Object Name"],
                                            "Object Type": r["Object Type"],
                                            "Referenced Object": dependency[4],
                                            "Referenced Table": dependency[3],
                                            "Referenced Object Type": dependency[5],
                                            "Done": d,
                                            "Full Object Name": r["Full Object Name"],
                                            "Referenced Full Object Name": dependency[
                                                7
                                            ],
                                            "Parent Node": rObj,
                                        }
                                    ]
                                ),
                            ],
                            ignore_index=True,
                        )

            df.loc[i, "Done"] = True

    df = df.drop(["Done", "Full Object Name", "Referenced Full Object Name"], axis=1)

    return df


@log
def get_model_calc_dependencies(
    dataset: str, workspace: Optional[str] = None
) -> pd.DataFrame:
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

    workspace = fabric.resolve_workspace_name(workspace)

    dep = fabric.evaluate_dax(
        dataset=dataset,
        workspace=workspace,
        dax_string="""
        SELECT
        [TABLE] AS [Table Name]
        ,[OBJECT] AS [Object Name]
        ,[OBJECT_TYPE] AS [Object Type]
        ,[EXPRESSION] AS [Expression]
        ,[REFERENCED_TABLE] AS [Referenced Table]
        ,[REFERENCED_OBJECT] AS [Referenced Object]
        ,[REFERENCED_OBJECT_TYPE] AS [Referenced Object Type]
        FROM $SYSTEM.DISCOVER_CALC_DEPENDENCY
        """,
    )

    dep["Object Type"] = dep["Object Type"].str.replace("_", " ").str.title()
    dep["Referenced Object Type"] = (
        dep["Referenced Object Type"].str.replace("_", " ").str.title()
    )

    dep["Full Object Name"] = format_dax_object_name(
        dep["Table Name"], dep["Object Name"]
    )
    dep["Referenced Full Object Name"] = format_dax_object_name(
        dep["Referenced Table"], dep["Referenced Object"]
    )
    dep["Parent Node"] = dep["Object Name"]

    df = dep

    objs = ["Measure", "Calc Column", "Calculation Item", "Calc Table"]

    df["Done"] = df.apply(
        lambda row: False if row["Referenced Object Type"] in objs else True, axis=1
    )

    while any(df["Done"] == False):
        for i, r in df.iterrows():
            rObjFull = r["Referenced Full Object Name"]
            rObj = r["Referenced Object"]
            if r["Done"] == False:
                dep_filt = dep[dep["Full Object Name"] == rObjFull]

                for index, dependency in dep_filt.iterrows():
                    d = True
                    if dependency[5] in objs:
                        d = False
                        df = pd.concat(
                            [
                                df,
                                pd.DataFrame(
                                    [
                                        {
                                            "Table Name": r["Table Name"],
                                            "Object Name": r["Object Name"],
                                            "Object Type": r["Object Type"],
                                            "Referenced Object": dependency[4],
                                            "Referenced Table": dependency[3],
                                            "Referenced Object Type": dependency[5],
                                            "Done": d,
                                            "Full Object Name": r["Full Object Name"],
                                            "Referenced Full Object Name": dependency[
                                                7
                                            ],
                                            "Parent Node": rObj,
                                        }
                                    ]
                                ),
                            ],
                            ignore_index=True,
                        )
                    else:
                        df = pd.concat(
                            [
                                df,
                                pd.DataFrame(
                                    [
                                        {
                                            "Table Name": r["Table Name"],
                                            "Object Name": r["Object Name"],
                                            "Object Type": r["Object Type"],
                                            "Referenced Object": dependency[5],
                                            "Referenced Table": dependency[4],
                                            "Referenced Object Type": dependency[6],
                                            "Done": d,
                                            "Full Object Name": r["Full Object Name"],
                                            "Referenced Full Object Name": dependency[
                                                7
                                            ],
                                            "Parent Node": rObj,
                                        }
                                    ]
                                ),
                            ],
                            ignore_index=True,
                        )

            df.loc[i, "Done"] = True

    df = df.drop(["Done"], axis=1)

    return df


@log
def measure_dependency_tree(
    dataset: str, measure_name: str, workspace: Optional[str] = None
):
    """
    Prints a measure dependency tree of all dependent objects for a measure in a semantic model.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    measure_name : str
        Name of the measure.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------

    """

    workspace = fabric.resolve_workspace_name(workspace)

    dfM = fabric.list_measures(dataset=dataset, workspace=workspace)
    dfM_filt = dfM[dfM["Measure Name"] == measure_name]

    if len(dfM_filt) == 0:
        print(
            f"{icons.red_dot} The '{measure_name}' measure does not exist in the '{dataset}' semantic model in the '{workspace}' workspace."
        )
        return

    md = get_measure_dependencies(dataset, workspace)
    df_filt = md[md["Object Name"] == measure_name]

    # Create a dictionary to hold references to nodes
    node_dict: Dict[str, Any] = {}

    # Populate the tree
    for _, row in df_filt.iterrows():
        # measure_name = row['Object Name']
        ref_obj_table_name = row["Referenced Table"]
        ref_obj_name = row["Referenced Object"]
        ref_obj_type = row["Referenced Object Type"]
        parent_node_name = row["Parent Node"]

        # Create or get the parent node
        parent_node = node_dict.get(parent_node_name)
        if parent_node is None:
            parent_node = Node(parent_node_name)
            node_dict[parent_node_name] = parent_node
        parent_node.custom_property = f"{icons.measure_icon} "

        # Create the child node
        child_node_name = ref_obj_name
        child_node = Node(child_node_name, parent=parent_node)
        if ref_obj_type == "Column":
            child_node.custom_property = f"{icons.column_icon} '{ref_obj_table_name}'"
        elif ref_obj_type == "Table":
            child_node.custom_property = f"{icons.table_icon} "
        elif ref_obj_type == "Measure":
            child_node.custom_property = f"{icons.measure_icon} "

        # Update the dictionary with the child node
        node_dict[child_node_name] = child_node

    # Visualize the tree structure using RenderTree
    for pre, _, node in RenderTree(node_dict[measure_name]):
        if icons.table_icon in node.custom_property:
            print(f"{pre}{node.custom_property}'{node.name}'")
        else:
            print(f"{pre}{node.custom_property}[{node.name}]")
