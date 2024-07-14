import sempy.fabric as fabric
from sempy_labs.tom import connect_semantic_model
from sempy_labs._helper_functions import resolve_lakehouse_name
from sempy_labs._refresh_semantic_model import refresh_semantic_model
from typing import List, Optional, Union
import sempy_labs._icons as icons


def update_direct_lake_partition_entity(
    dataset: str,
    table_name: Union[str, List[str]],
    entity_name: Union[str, List[str]],
    workspace: Optional[str] = None,
    lakehouse: Optional[str] = None,
    lakehouse_workspace: Optional[str] = None,
):
    """
    Remaps a table (or tables) in a Direct Lake semantic model to a table in a lakehouse.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    table_name : str, List[str]
        Name of the table(s) in the semantic model.
    entity_name : str, List[str]
        Name of the lakehouse table to be mapped to the semantic model table.
    workspace : str, default=None
        The Fabric workspace name in which the semantic model exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    lakehouse : str, default=None
        The Fabric lakehouse used by the Direct Lake semantic model.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str, default=None
        The Fabric workspace used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace = fabric.resolve_workspace_name(workspace)

    if lakehouse_workspace is None:
        lakehouse_workspace = workspace

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, lakehouse_workspace)

    # Support both str & list types
    if isinstance(table_name, str):
        table_name = [table_name]
    if isinstance(entity_name, str):
        entity_name = [entity_name]

    if len(table_name) != len(entity_name):
        raise ValueError(
            f"{icons.red_dot} The 'table_name' and 'entity_name' arrays must be of equal length."
        )

    with connect_semantic_model(
        dataset=dataset, readonly=False, workspace=workspace
    ) as tom:

        if not tom.is_direct_lake():
            raise ValueError(
                f"{icons.red_dot} The '{dataset}' semantic model within the '{workspace}' workspace is not in Direct Lake mode."
            )

        for tName in table_name:
            i = table_name.index(tName)
            eName = entity_name[i]
            part_name = (
                p.Name
                for t in tom.model.Tables
                for p in t.Partitions
                if t.Name == tName
            )

            if part_name is None:
                raise ValueError(
                    f"{icons.red_dot} The '{tName}' table in the '{dataset}' semantic model has not been updated."
                )
            else:
                tom.model.Tables[tName].Partitions[part_name].EntityName = eName
                print(
                    f"{icons.green_dot} The '{tName}' table in the '{dataset}' semantic model has been updated to point to the '{eName}' table "
                    f"in the '{lakehouse}' lakehouse within the '{lakehouse_workspace}' workspace."
                )


def add_table_to_direct_lake_semantic_model(
    dataset: str,
    table_name: str,
    lakehouse_table_name: str,
    workspace: Optional[str | None] = None,
):
    """
    Adds a table and all of its columns to a Direct Lake semantic model, based on a Fabric lakehouse table.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    table_name : str, List[str]
        Name of the table in the semantic model.
    lakehouse_table_name : str
        The name of the Fabric lakehouse table.
    workspace : str, default=None
        The name of the Fabric workspace in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    """

    import Microsoft.AnalysisServices.Tabular as TOM
    from sempy_labs.lakehouse._get_lakehouse_columns import get_lakehouse_columns
    from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
    from sempy_labs.directlake._get_directlake_lakehouse import (
        get_direct_lake_lakehouse,
    )

    workspace = fabric.resolve_workspace_name(workspace)

    with connect_semantic_model(
        dataset=dataset, readonly=False, workspace=workspace
    ) as tom:

        if tom.is_direct_lake() is False:
            raise ValueError(
                "This function is only valid for Direct Lake semantic models."
            )

        if any(
            p.Name == lakehouse_table_name
            for p in tom.all_partitions()
            if p.SourceType == TOM.PartitionSourceType.Entity
        ):
            t_name = next(
                p.Parent.Name
                for p in tom.all_partitions()
                if p.Name
                == lakehouse_table_name & p.SourceType
                == TOM.PartitionSourceType.Entity
            )
            raise ValueError(
                f"The '{lakehouse_table_name}' table already exists in the '{dataset}' semantic model within the '{workspace}' workspace as the '{t_name}' table."
            )

        if any(t.Name == table_name for t in tom.model.Tables):
            raise ValueError(
                f"The '{table_name}' table already exists in the '{dataset}' semantic model within the '{workspace}' workspace."
            )

        lake_name, lake_id = get_direct_lake_lakehouse(
            dataset=dataset, workspace=workspace
        )

        dfL = get_lakehouse_tables(lakehouse=lake_name, workspace=workspace)
        dfL_filt = dfL[dfL["Table Name"] == lakehouse_table_name]

        if len(dfL_filt) == 0:
            raise ValueError(
                f"The '{lakehouse_table_name}' table does not exist in the '{lake_name}' lakehouse within the '{workspace}' workspace."
            )

        dfLC = get_lakehouse_columns(lakehouse=lake_name, workspace=workspace)
        dfLC_filt = dfLC[dfLC["Table Name"] == lakehouse_table_name]

        tom.add_table(name=table_name)
        print(
            f"{icons.green_dot} The '{table_name}' table has been added to the '{dataset}' semantic model within the '{workspace}' workspace."
        )
        tom.add_entity_partition(
            table_name=table_name, entity_name=lakehouse_table_name
        )
        print(
            f"{icons.green_dot} The '{lakehouse_table_name}' partition has been added to the '{table_name}' table in the '{dataset}' semantic model within the '{workspace}' workspace."
        )

        for i, r in dfLC_filt.iterrows():
            lakeCName = r["Column Name"]
            dType = r["Data Type"]
            dt = icons.data_type_mapping.get(dType)
            tom.add_data_column(
                table_name=table_name,
                column_name=lakeCName,
                source_column=lakeCName,
                data_type=dt,
            )
            print(
                f"{icons.green_dot} The '{lakeCName}' column has been added to the '{table_name}' table as a '{dt}' data type in the '{dataset}' semantic model within the '{workspace}' workspace."
            )

        refresh_semantic_model(dataset=dataset, tables=table_name, workspace=workspace)
