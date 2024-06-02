import sempy
import sempy.fabric as fabric
from anytree import Node, RenderTree
from .GetMeasureDependencies import get_measure_dependencies
from typing import List, Optional, Union
from sempy._utils._log import log

@log
def measure_dependency_tree(dataset: str, measure_name: str, workspace: Optional[str] = None):

    """
    Shows a measure dependency tree of all dependent objects for a measure in a semantic model.

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
    str
        A tree view showing the dependencies for a given measure within the semantic model.
    """


    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    dfM = fabric.list_measures(dataset = dataset, workspace = workspace)
    dfM_filt = dfM[dfM['Measure Name'] == measure_name]

    if len(dfM_filt) == 0:
        print(f"The '{measure_name}' measure does not exist in the '{dataset}' semantic model in the '{workspace}' workspace.")
        return

    md = get_measure_dependencies(dataset, workspace)
    df_filt = md[md['Object Name'] == measure_name]

    # Create a dictionary to hold references to nodes
    node_dict = {}
    measureIcon = "\u2211"
    tableIcon = "\u229E"
    columnIcon = "\u229F"

    # Populate the tree
    for _, row in df_filt.iterrows():
        #measure_name = row['Object Name']
        ref_obj_table_name = row['Referenced Table']
        ref_obj_name = row['Referenced Object']
        ref_obj_type = row['Referenced Object Type']
        parent_node_name = row['Parent Node']
    
        # Create or get the parent node
        parent_node = node_dict.get(parent_node_name)
        if parent_node is None:
            parent_node = Node(parent_node_name)        
            node_dict[parent_node_name] = parent_node
        parent_node.custom_property = measureIcon + " "

        # Create the child node
        child_node_name = ref_obj_name
        child_node = Node(child_node_name, parent=parent_node)
        if ref_obj_type == 'Column':
            child_node.custom_property = columnIcon + " '" + ref_obj_table_name + "'"
        elif ref_obj_type == 'Table':
            child_node.custom_property = tableIcon + " "
        elif ref_obj_type == 'Measure':
            child_node.custom_property = measureIcon + " "

        # Update the dictionary with the child node
        node_dict[child_node_name] = child_node

    # Visualize the tree structure using RenderTree
    for pre, _, node in RenderTree(node_dict[measure_name]):
        if tableIcon in node.custom_property:
            print(f"{pre}{node.custom_property}'{node.name}'")
        else:
            print(f"{pre}{node.custom_property}[{node.name}]")