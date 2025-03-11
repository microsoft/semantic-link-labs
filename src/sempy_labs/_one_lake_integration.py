import sempy.fabric as fabric
import pandas as pd
from typing import Optional
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
    resolve_workspace_id,
)
import sempy_labs._icons as icons
from uuid import UUID


@log
def export_model_to_onelake(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    destination_lakehouse: Optional[str] = None,
    destination_workspace: Optional[str] = None,
):
    """
    Exports a semantic model's tables to delta tables in the lakehouse. Creates shortcuts to the tables if a lakehouse is specified.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    destination_lakehouse : str, default=None
        The name of the Fabric lakehouse where shortcuts will be created to access the delta tables created by the export. If the lakehouse specified does not exist, one will be created with that name. If no lakehouse is specified, shortcuts will not be created.
    destination_workspace : str, default=None
        The name of the Fabric workspace in which the lakehouse resides.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    fabric.refresh_tom_cache(workspace=workspace)

    if destination_workspace is None:
        destination_workspace = workspace_name
        destination_workspace_id = workspace_id
    else:
        destination_workspace_id = resolve_workspace_id(workspace=destination_workspace)

    tmsl = f"""
    {{
    'export': {{
    'layout': 'delta',
    'type': 'full',
    'objects': [
        {{
        'database': '{dataset_name}'
        }}
    ]
    }}
    }}
    """

    # Export model's tables as delta tables
    try:
        fabric.execute_tmsl(script=tmsl, workspace=workspace_id)
        print(
            f"{icons.green_dot} The '{dataset_name}' semantic model's tables have been exported as delta tables to the '{workspace_name}' workspace.\n"
        )
    except Exception as e:
        raise ValueError(
            f"{icons.red_dot} The '{dataset_name}' semantic model's tables have not been exported as delta tables to the '{workspace_name}' workspace.\nMake sure you enable OneLake integration for the '{dataset_name}' semantic model. Follow the instructions here: https://learn.microsoft.com/power-bi/enterprise/onelake-integration-overview#enable-onelake-integration"
        ) from e

    # Create shortcuts if destination lakehouse is specified
    if destination_lakehouse is not None:
        # Destination...
        dfI_Dest = fabric.list_items(workspace=destination_workspace, type="Lakehouse")
        dfI_filt = dfI_Dest[(dfI_Dest["Display Name"] == destination_lakehouse)]

        if len(dfI_filt) == 0:
            print(
                f"{icons.red_dot} The '{destination_lakehouse}' lakehouse does not exist within the '{destination_workspace}' workspace."
            )
            # Create lakehouse
            destination_lakehouse_id = fabric.create_lakehouse(
                display_name=destination_lakehouse, workspace=destination_workspace
            )
            print(
                f"{icons.green_dot} The '{destination_lakehouse}' lakehouse has been created within the '{destination_workspace}' workspace.\n"
            )
        else:
            destination_lakehouse_id = dfI_filt["Id"].iloc[0]

        # Source...
        dfI_Source = fabric.list_items(workspace=workspace_id, type="SemanticModel")
        dfI_filtSource = dfI_Source[(dfI_Source["Display Name"] == dataset)]
        sourceLakehouseId = dfI_filtSource["Id"].iloc[0]

        # Valid tables
        dfP = fabric.list_partitions(
            dataset=dataset_id,
            workspace=workspace_id,
            additional_xmla_properties=["Parent.SystemManaged"],
        )
        dfP_filt = dfP[
            (dfP["Mode"] == "Import")
            & (dfP["Source Type"] != "CalculationGroup")
            & (dfP["Parent System Managed"] == False)
        ]
        dfC = fabric.list_columns(dataset=dataset_id, workspace=workspace_id)
        tmc = pd.DataFrame(dfP.groupby("Table Name")["Mode"].nunique()).reset_index()
        oneMode = tmc[tmc["Mode"] == 1]
        tableAll = dfP_filt[
            dfP_filt["Table Name"].isin(dfC["Table Name"].values)
            & (dfP_filt["Table Name"].isin(oneMode["Table Name"].values))
        ]
        tables = tableAll["Table Name"].unique()

        client = fabric.FabricRestClient()

        print(f"{icons.in_progress} Creating shortcuts...\n")
        for tableName in tables:
            tablePath = f"Tables/{tableName}"
            shortcutName = tableName.replace(" ", "")
            request_body = {
                "path": "Tables",
                "name": shortcutName,
                "target": {
                    "oneLake": {
                        "workspaceId": workspace_id,
                        "itemId": sourceLakehouseId,
                        "path": tablePath,
                    }
                },
            }

            try:
                response = client.post(
                    f"/v1/workspaces/{destination_workspace_id}/items/{destination_lakehouse_id}/shortcuts",
                    json=request_body,
                )
                if response.status_code == 201:
                    print(
                        f"{icons.bullet} The shortcut '{shortcutName}' was created in the '{destination_lakehouse}' lakehouse within the '{destination_workspace}' workspace. It is based on the '{tableName}' table in the '{dataset_name}' semantic model within the '{workspace_name}' workspace.\n"
                    )
                else:
                    print(response.status_code)
            except Exception as e:
                raise ValueError(
                    f"{icons.red_dot} Failed to create a shortcut for the '{tableName}' table."
                ) from e
