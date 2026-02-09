import sempy
import pandas as pd
from uuid import UUID
from IPython.display import display
from typing import Optional
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
)
from sempy_labs.lakehouse._get_lakehouse_columns import get_lakehouse_columns
import sempy_labs._icons as icons
from sempy_labs.directlake._sources import get_direct_lake_sources
from sempy_labs.tom import connect_semantic_model


@log
def direct_lake_schema_compare(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Checks that all the tables in a Direct Lake semantic model map to tables in their corresponding lakehouse and that the columns in each table exist.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    sources = get_direct_lake_sources(dataset=dataset_id, workspace=workspace_id)[0]

    source_type = sources.get("itemType")

    if source_type != "Lakehouse":
        raise ValueError(
            f"{icons.red_dot} The source of the '{dataset_name}' semantic model within the '{workspace_name}' workspace is not a Fabric lakehouse. This function only supports Direct Lake semantic models which source from Fabric lakehouses."
        )

    lakehouse_name = sources.get("itemName")
    lakehouse_id = sources.get("itemId")
    lakehouse_workspace_id = sources.get("workspaceId")
    lakehouse_workspace_name = sources.get("workspaceName")
    df_lake = get_lakehouse_columns(
        lakehouse=lakehouse_id, workspace=lakehouse_workspace_id
    )

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM

    with connect_semantic_model(dataset=dataset_id, workspace=workspace_id) as tom:
        if tom.is_direct_lake is False:
            raise ValueError(
                f"{icons.red_dot} The '{dataset_name}' semantic model within the '{workspace_name}' workspace is not in Direct Lake mode."
            )
        missing_tables = []
        missing_columns = []
        for p in tom.all_partitions():
            if p.Mode == TOM.ModeType.DirectLake:
                table_name = p.Parent.Name
                source_table = p.Source.EntityName
                df_lake_filt = df_lake[df_lake["Table Name"] == source_table]
                if df_lake_filt.empty:
                    missing_tables.append(
                        {
                            "Table Name": table_name,
                            "Source Table": source_table,
                        }
                    )
                else:
                    for c in p.Parent.Columns:
                        col_name = c.Name
                        source_col = c.SourceColumn
                        df_lake_filt = df_lake[
                            (df_lake["Table Name"] == source_table)
                            & (df_lake["Column Name"] == source_col)
                        ]
                        if df_lake_filt.empty:
                            missing_columns.append(
                                {
                                    "Table Name": table_name,
                                    "Source Table": source_table,
                                    "Column Name": col_name,
                                    "Source Column": source_col,
                                }
                            )

        if len(missing_tables) == 0:
            print(
                f"{icons.green_dot} All tables exist in the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace_name}' workspace."
            )
        else:
            print(
                f"{icons.yellow_dot} The following tables exist in the '{dataset_name}' semantic model within the '{workspace_name}' workspace"
                f" but do not exist in the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace_name}' workspace."
            )
            display(pd.DataFrame(missing_tables))
        if len(missing_columns) == 0:
            print(
                f"{icons.green_dot} All columns exist in the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace_name}' workspace."
            )
        else:
            print(
                f"{icons.yellow_dot} The following columns exist in the '{dataset_name}' semantic model within the '{workspace_name}' workspace "
                f"but do not exist in the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace_name}' workspace."
            )
            display(pd.DataFrame(missing_columns))
