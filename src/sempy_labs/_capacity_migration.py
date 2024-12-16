import sempy.fabric as fabric
from typing import Optional, List
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from sempy_labs._workspaces import assign_workspace_to_capacity
from sempy_labs._dataflows import list_dataflow_storage_accounts
from sempy_labs._dataflows import assign_workspace_to_dataflow_storage
from sempy_labs._query_scale_out import set_semantic_model_storage_format
from sempy_labs._clear_cache import (
    backup_semantic_model,
    restore_semantic_model,
    list_backups
)

from sempy_labs.admin._basic_functions import (
    assign_workspaces_to_capacity,
    _list_capacities_meta,
    list_capacities,
    list_workspaces,
    list_datasets,
    
)
from sempy_labs.admin._items import(
    list_items,
)
from sempy_labs._helper_functions import (
    resolve_capacity_id,
    convert_to_alphanumeric_lowercase,
)
from sempy_labs._capacities import create_fabric_capacity
from sempy_labs._refresh_semantic_model import refresh_semantic_model

import pandas as pd

import time



def migrate_settings(source_capacity: str, target_capacity: str):

    migrate_capacity_settings(
        source_capacity=source_capacity,
        target_capacity=target_capacity,
    )
    migrate_access_settings(
        source_capacity=source_capacity,
        target_capacity=target_capacity,
    )
    migrate_notification_settings(
        source_capacity=source_capacity,
        target_capacity=target_capacity,
    )
    migrate_spark_settings(
        source_capacity=source_capacity,
        target_capacity=target_capacity,
    )
    migrate_delegated_tenant_settings(
        source_capacity=source_capacity,
        target_capacity=target_capacity,
    )
    migrate_disaster_recovery_settings(
        source_capacity=source_capacity,
        target_capacity=target_capacity,
    )


@log
def migrate_workspaces(
    source_capacity: str,
    target_capacity: str,
    workspaces: Optional[str | List[str]] = None,
):
    """
    This function migrates the workspace(s) from one capacity to another capacity.
    Limitation: source & target capacities must be in the same region.
    If not all the workspaces succesfully migrated to the target capacity, the migrated workspaces will rollback to be assigned
    to the source capacity.

    Parameters
    ----------
    source_capacity : str
        Name of the source Fabric capacity.
    target_capacity : str
        Name of the target/destination Fabric capacity.
    workspaces : str | List[str], default=None
        The name of the workspace(s) specified will be reassigned from the source capacity to the target capacity.
        Defaults to None which will reassign all workspaces in the source capacity to the target capacity.
    """

    if isinstance(workspaces, str):
        workspaces = [workspaces]

    dfC = _list_capacities_meta()
    dfC_filt = dfC[dfC["Capacity Name"] == source_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} Invalid source capacity. The '{source_capacity}' capacity does not exist."
        )
    source_capacity_region = dfC_filt["Region"].iloc[0]
    source_capacity_id = dfC_filt["Capacity Id"].iloc[0]
    dfC_filt = dfC[dfC["Capacity Name"] == target_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} Invalid target capacity. The '{target_capacity}' capacity does not exist."
        )
    target_capacity_region = dfC_filt["Region"].iloc[0]
    target_capacity_state = dfC_filt["State"].iloc[0]

    if source_capacity_region != target_capacity_region:
        raise ValueError(
            f"{icons.red_dot} The '{source_capacity}' and '{target_capacity}' are not in the same region."
        )
    if target_capacity_state != "Active":
        raise ValueError(
            f"{icons.red_dot} The '{target_capacity}' target capacity is inactive. The capacity must be active in order for workspaces to be migrated."
        )

    dfW = fabric.list_workspaces(filter=f"capacityId eq '{source_capacity_id.upper()}'")
    if workspaces is None:
        workspace_count = len(dfW)
    else:
        workspace_count = len(workspaces)
    migrated_workspaces = []

    for i, r in dfW.iterrows():
        workspace = r["Name"]

        if workspaces is None or workspace in workspaces:
            pass
        else:
            continue

        if assign_workspace_to_capacity(
            capacity_name=target_capacity, workspace=workspace
        ):
            migrated_workspaces.append(workspace)

    if len(migrated_workspaces) < workspace_count:
        print(
            f"{icons.warning} Not all workspaces in the '{source_capacity}' capacity were migrated to the '{target_capacity}' capacity."
        )
        print(f"{icons.in_progress} Initiating rollback...")
        for i, r in dfW.iterrows():
            workspace = r["Name"]
            if workspace in migrated_workspaces:
                assign_workspace_to_capacity(
                    capacity_name=source_capacity, workspace=workspace
                )
        print(
            f"{icons.green_dot} Rollback of the workspaces to the '{source_capacity}' capacity is complete."
        )
    else:
        print(
            f"{icons.green_dot} All workspaces were migrated from the '{source_capacity}' capacity to the '{target_capacity}' capacity succesfully."
        )


@log
def migrate_capacities(
    azure_subscription_id: str,
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
    resource_group: str | dict,
    capacities: Optional[str | List[str]] = None,
    target_region: Optional[str | List[str]] = None, #None will use region from source capacity
    storage_account_name: Optional[str] = None, #Used for backup/restore
    use_existing_rg_for_A_sku: bool = True,
    p_sku_only: bool = True,
):
    """
    This function creates new Fabric capacities for given A or P sku capacities and reassigns their workspaces to the newly created capacity.

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription ID.
    key_vault_uri : str
        The name of the `Azure key vault <https://azure.microsoft.com/products/key-vault>`_ URI. Example: "https://<Key Vault Name>.vault.azure.net/"
    key_vault_tenant_id : str
        The name of the Azure key vault secret storing the Tenant ID.
    key_vault_client_id : str
        The name of the Azure key vault secret storing the Client ID.
    key_vault_client_secret : str
        The name of the Azure key vault secret storing the Client Secret.
    resource_group : str | dict
        The name of the Azure resource group.
        For A skus, this parameter will be ignored and the resource group used for the F sku will be the same as the A sku's resource group.
        For P skus, if this parameter is a string, it will use that resource group for all of the newly created F skus. If this parameter is a dictionary, it will use that mapping (capacity name -> resource group) for creating capacities with the mapped resource groups.
    capacities : str | List[str], default=None
        The capacity(ies) to migrate from A/P -> F sku.
        Defaults to None which migrates all accessible A/P sku capacities to F skus.
    use_existing_rg_for_A_sku : bool, default=True
        If True, the F sku inherits the resource group from the A sku (for A sku migrations)
    p_sku_only : bool, default=True
        If set to True, only migrates P skus. If set to False, migrates both P and A skus.
    """

    #If storage account is provided then check it has been attached previously.
    if storage_account_name is not None:
        df_dataflow_storage_accounts = list_dataflow_storage_accounts()
        df_dataflow_storage_accounts_filt = df_dataflow_storage_accounts[df_dataflow_storage_accounts["Dataflow Storage Account Name"] == storage_account_name]
        if df_dataflow_storage_accounts_filt.empty:
            print(f"{icons.info} Dataflow has not been previously attached to a workspace or tenant.")
            return  # Exit the function if the DataFrame is empty
    if isinstance(capacities, str):
        capacities = [capacities]

    p_sku_list = list(icons.sku_mapping.keys())

    dfC = list_capacities()

    if capacities is None:
        dfC_filt = dfC.copy()
    else:
        dfC_filt = dfC[dfC["Capacity Name"].isin(capacities)]

    if p_sku_only:
        dfC_filt = dfC_filt[dfC_filt["Sku"].str.startswith("P")]
    else:
        dfC_filt = dfC_filt[
            (dfC_filt["Sku"].str.startswith(("P", "A")))
            & (~dfC_filt["Sku"].str.startswith("PP"))
        ]

    dfC_filt = (
        dfC_filt.copy()
    )  # Something strange is happening here. Without this a key error on Display Name occurs

    if len(dfC_filt) == 0:
        print(f"{icons.info} There are no valid capacities to migrate.")
        return


    for _, r in dfC_filt.iterrows():
        cap_name = r["Capacity Name"]
        cap_id = r["Capacity Id"]
        region = r["Region"]
        
        if target_region is None or region == target_region:
            cross_region = 0
        else:
            region = target_region
            cross_region = 1
        
        # Check for non-Power BI items
        power_bi_items = [
            "Dashboard", "Datamart", "PaginatedReport", "Report", "SemanticModel"
        ]
        df_items = list_items(capacity=cap_id)
        df_items
        # Check for non-PBI Items
        nonpbi_df = df_items[~df_items['Type'].isin(power_bi_items)]
        if not nonpbi_df.empty and cross_region == 1:
            print(f"{icons.info} {cap_name} contains Fabric Items and cannot be migrated to a different region.")
            continue  # Move to the next iteration of the loop

        sku_size = r["Sku"]
        admins = r["Admins"]
        tgt_capacity = f"{convert_to_alphanumeric_lowercase(cap_name)}{icons.migrate_capacity_suffix}"

        # Check if target capacity exists
        dfC_tgt = dfC[dfC["Capacity Name"] == tgt_capacity]

        if sku_size[:1] == "A" and use_existing_rg_for_A_sku:
            rg = None
        else:
            if isinstance(resource_group, str):
                rg = resource_group
            elif isinstance(resource_group, dict):
                rg = resource_group.get(cap_name)
            else:
                raise ValueError(f"{icons.red_dot} Invalid 'resource_group' parameter.")

        if sku_size in p_sku_list:
            # Only create the capacity if it does not already exist
            if len(dfC_tgt) > 0:
                print(
                    f"{icons.info} Skipping creating a new capacity for '{cap_name}' as the '{tgt_capacity}' capacity already exists."
                )
            else:
                create_fabric_capacity(
                    capacity_name=tgt_capacity,
                    azure_subscription_id=azure_subscription_id,
                    key_vault_uri=key_vault_uri,
                    key_vault_tenant_id=key_vault_tenant_id,
                    key_vault_client_id=key_vault_client_id,
                    key_vault_client_secret=key_vault_client_secret,
                    resource_group=rg,
                    region=region,
                    sku=icons.sku_mapping.get(sku_size),
                    admin_members=admins,
                )

            #Moved Migrate settings as XMLA Read/Write needs to be enabled before backup/restore of Semantic models.
            migrate_settings(source_capacity=cap_name, target_capacity=tgt_capacity)

            #Check for large semantic models or Fabric Items
            if cross_region ==1:
                _xregion_migration_backup_semantic_models(capacity=cap_name,
                                                                workspace=None, 
                                                                storage_account_name=storage_account_name)

            # Migrate workspaces to new capacity
            assign_workspaces_to_capacity(
                source_capacity=cap_name, target_capacity=tgt_capacity, workspace=None
            )
            if cross_region ==1:
                print(f"{icons.info} Once migration has completed. Please execute:")
                print_function = f"_xregion_migration_restore_semantic_models(capacity='{tgt_capacity}')"
                top_border = "╔" + "═" * (len(print_function) + 2) + "╗"
                bottom_border = "╚" + "═" * (len(print_function) + 2) + "╝"
                print(top_border)
                print(f"║ {print_function} ║")
                print(bottom_border)


@log
def _xregion_migration_backup_semantic_models(
    capacity: str,
    workspace: Optional[str | List[str]] = None,
    storage_account_name: Optional[str] = None, # Used for backup/restore
    backup_suffix: Optional[str] = '_SLL'
):
    """
    This function backs up large semantic models in a specified capacity and workspace.

    Parameters
    ----------
    capacity : str
        Name of the capacity.
    workspace : Optional[str | List[str]]
        Name(s) of the workspace(s). If None, all workspaces in the specified capacity will be considered.
    storage_account_name : Optional[str]
        Name of the storage account used for backup and restore operations.
    backup_suffix : Optional[str]
        Suffix used for backup files. Default is '_SLL'.
    """
    # Convert workspace to list if it's a single string
    if isinstance(workspace, str):
        workspace = [workspace]

    # List all capacities and filter by the specified capacity name
    dfC = list_capacities()
    dfC_filt = dfC[dfC["Capacity Name"] == capacity]
    capacity_id = dfC_filt["Capacity Id"].iloc[0]

    # If no specific workspace is provided, list all workspaces in the specified capacity
    if workspace is None:
        dfW = list_workspaces()
        dfW = dfW[dfW["Capacity Id"].str.upper() == capacity_id.upper()]
        workspaces = dfW
    else:
        # Filter workspaces by the provided names
        dfW = list_workspaces()
        workspaces = dfW[dfW["Name"].isin(workspace)]["Id"]
    
    df_semantic_models = list_datasets()
    # Large models only.
    df_large_semantic_models = df_semantic_models[(df_semantic_models['Target Storage Mode'] == 'PremiumFiles')]
    
    # Iterate over each workspace
    for _, row in workspaces.iterrows():
        workspace_id = row["Id"]
        
        # List all datasets and filter for large semantic models
        df_large_semantic_models_by_workspace = df_large_semantic_models[
            (df_large_semantic_models['Workspace Id'] == workspace_id)
        ]

        # Check if there are any large semantic models
        if df_large_semantic_models_by_workspace.empty:
            print("No large semantic models")
        else:
            # Assign workspace to dataflow storage
            dataflow_client = fabric.PowerBIRestClient()
            dataflow_response = dataflow_client.get(
                f"/v1.0/myorg/groups/{workspace_id}"
                )
            # Extract the JSON content from the Response object
            dataflow_response_json = dataflow_response.json()

            # Extract the dataflowStorageId
            dataflow_storage_id = dataflow_response_json.get("dataflowStorageId")
            
            if dataflow_storage_id is None:
                assign_workspace_to_dataflow_storage(dataflow_storage_account=storage_account_name, workspace=workspace_id)

            # Backup, clear data, and set storage format for each semantic model
            for _, row in df_large_semantic_models_by_workspace.iterrows():
                semantic_model_name = row['Dataset Name']
                backup_semantic_model(
                    dataset=semantic_model_name,
                    file_path=f'{semantic_model_name}{backup_suffix}.abf', 
                    allow_overwrite=True,
                    apply_compression=True,
                    workspace=workspace_id
                )
                refresh_semantic_model(dataset=semantic_model_name, refresh_type='clearValues', workspace=workspace_id)
                set_semantic_model_storage_format(dataset=semantic_model_name, storage_format='Small', workspace=workspace_id)
    

@log
def _xregion_migration_restore_semantic_models(
    capacity: str,
    workspace: Optional[str | List[str]] = None,
    backup_suffix: Optional[str] = '_SLL',
):
    """
    This function restores large semantic models in a specified capacity and workspace.

    Parameters
    ----------
    capacity : str
        Name of the capacity.
    workspace : Optional[str | List[str]]
        Name(s) of the workspace(s). If None, all workspaces in the specified capacity will be considered.
    backup_suffix : Optional[str]
        Suffix used for backup files. Default is '_SLL'.
    """
    # Convert workspace to list if it's a single string
    if isinstance(workspace, str):
        workspace = [workspace]

    # List all capacities and filter by the specified capacity name
    dfC = list_capacities()
    dfC_filt = dfC[dfC["Capacity Name"] == capacity]
    capacity_id = dfC_filt["Capacity Id"].iloc[0]

    # If no specific workspace is provided, list all workspaces in the specified capacity
    if workspace is None:
        dfW = list_workspaces()
        dfW = dfW[dfW["Capacity Id"].str.upper() == capacity_id.upper()]
        workspaces = dfW
    else:
        # Filter workspaces by the provided names
        dfW = list_workspaces()
        workspaces = dfW[dfW["Name"].isin(workspace)]["Id"]
    
    df_semantic_models = list_datasets()

    # Iterate over each workspace
    for _, row in workspaces.iterrows():
        workspace_id = row["Id"]

        # List all datasets and filter for large semantic models
        df_semantic_models_by_workspace = df_semantic_models[
            (df_semantic_models['Workspace Id'] == workspace_id)
        ]
        # Assign workspace to dataflow storage
        dataflow_client = fabric.PowerBIRestClient()
        dataflow_response = dataflow_client.get(
            f"/v1.0/myorg/groups/{workspace_id}"
            )
        # Extract the JSON content from the Response object
        dataflow_response_json = dataflow_response.json()

        # Extract the dataflowStorageId
        dataflow_storage_id = dataflow_response_json.get("dataflowStorageId")
        
        if dataflow_storage_id is None:
                print(f"{icons.info} No storage account linked to workspace - {workspace_id}. No backup required.")
                continue
        
        df_backups = list_backups(workspace=workspace_id)
        df_backups_sll = df_backups[df_backups['File Path'].str.contains('_SLL')]
        # Check if there are any large semantic models
        if df_backups_sll.empty:
            print(f"{icons.info} No back ups present, no semantic models to be restored.")
        else:
            
            # Backup, clear data, and set storage format for each semantic model
            for _, row in df_semantic_models_by_workspace.iterrows():
                semantic_model_name = row['Dataset Name']
                backup_name = f"{semantic_model_name}{backup_suffix}.abf"    

                if df_backups_sll['File Path'].str.contains(backup_name).any():
                    set_semantic_model_storage_format(dataset=semantic_model_name, storage_format='Large', workspace=workspace_id)
                    time.sleep(15)
                    restore_semantic_model(
                        dataset=semantic_model_name,
                        file_path=f'{backup_name}',
                        allow_overwrite=True,
                        workspace=workspace_id
                    )
                


@log
def migrate_capacity_settings(source_capacity: str, target_capacity: str):
    """
    This function migrates a capacity's settings to another capacity.

    Parameters
    ----------
    source_capacity : str
        Name of the source capacity.
    target_capacity : str
        Name of the target capacity.
    """

    dfC = list_capacities()
    dfC_filt = dfC[dfC["Capacity Name"] == source_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{source_capacity}' capacity does not exist."
        )
    source_capacity_id = dfC_filt["Capacity Id"].iloc[0].upper()
    dfC_filt = dfC[dfC["Capacity Name"] == target_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{target_capacity}' capacity does not exist."
        )
    target_capacity_id = dfC_filt["Capacity Id"].iloc[0].upper()

    workloads_params = "capacityCustomParameters?workloadIds=ADM&workloadIds=CDSA&workloadIds=DMS&workloadIds=RsRdlEngine&workloadIds=ScreenshotEngine&workloadIds=AS&workloadIds=QES&workloadIds=DMR&workloadIds=ESGLake&workloadIds=NLS&workloadIds=lake&workloadIds=TIPS&workloadIds=Kusto&workloadIds=Lakehouse&workloadIds=SparkCore&workloadIds=DI&workloadIds=Notebook&workloadIds=ML&workloadIds=ES&workloadIds=Reflex&workloadIds=Must&workloadIds=dmh&workloadIds=PowerBI&workloadIds=HLS"

    client = fabric.PowerBIRestClient()
    response_get_source = client.get(
        f"capacities/{source_capacity_id}/{workloads_params}"
    )
    if response_get_source.status_code != 200:
        raise FabricHTTPException(response_get_source)

    response_source_json = response_get_source.json().get(
        "capacityCustomParameters", {}
    )

    # Create payload for put request
    def remove_empty_workloads(data):
        keys_to_remove = [
            key for key, value in data.items() if not value["workloadCustomParameters"]
        ]
        for key in keys_to_remove:
            del data[key]

    remove_empty_workloads(response_source_json)

    settings_json = {}
    settings_json["capacityCustomParameters"] = {}

    for workload in response_source_json:
        if workload not in ["AI"]:
            settings_json["capacityCustomParameters"][workload] = {}
            settings_json["capacityCustomParameters"][workload][
                "workloadCustomParameters"
            ] = {}

            for workload_part in response_source_json[workload].values():
                for workload_item in workload_part:
                    setting_name = workload_item["name"]
                    setting_value = workload_item["value"]
                    if setting_value is None:
                        settings_json["capacityCustomParameters"][workload][
                            "workloadCustomParameters"
                        ][setting_name] = setting_value
                    elif isinstance(setting_value, bool):
                        settings_json["capacityCustomParameters"][workload][
                            "workloadCustomParameters"
                        ][setting_name] = bool(setting_value)
                    elif isinstance(setting_value, str):
                        settings_json["capacityCustomParameters"][workload][
                            "workloadCustomParameters"
                        ][setting_name] = str(setting_value)
                    else:
                        settings_json["capacityCustomParameters"][workload][
                            "workloadCustomParameters"
                        ][setting_name] = setting_value

    response_put = client.put(
        f"capacities/{target_capacity_id}/{workloads_params}",
        json=settings_json,
    )
    if response_put.status_code != 204:
        raise FabricHTTPException(response_put)

    print(
        f"{icons.green_dot} The capacity settings have been migrated from the '{source_capacity}' capacity to the '{target_capacity}' capacity."
    )


@log
def migrate_disaster_recovery_settings(source_capacity: str, target_capacity: str):
    """
    This function migrates a capacity's disaster recovery settings to another capacity.

    Parameters
    ----------
    source_capacity : str
        Name of the source capacity.
    target_capacity : str
        Name of the target capacity.
    """

    dfC = list_capacities()
    dfC_filt = dfC[dfC["Capacity Name"] == source_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{source_capacity}' capacity does not exist."
        )
    source_capacity_id = dfC_filt["Capacity Id"].iloc[0].upper()
    dfC_filt = dfC[dfC["Capacity Name"] == target_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{target_capacity}' capacity does not exist."
        )
    target_capacity_id = dfC_filt["Capacity Id"].iloc[0].upper()

    client = fabric.PowerBIRestClient()
    response_get_source = client.get(f"capacities/{source_capacity_id}/config")
    if response_get_source.status_code != 200:
        raise FabricHTTPException(response_get_source)

    request_body = {}
    value = response_get_source.json()["bcdr"]["config"]
    request_body["config"] = value

    response_put = client.put(
        f"capacities/{target_capacity_id}/fabricbcdr", json=request_body
    )

    if response_put.status_code != 202:
        raise FabricHTTPException(response_put)
    print(
        f"{icons.green_dot} The disaster recovery settings have been migrated from the '{source_capacity}' capacity to the '{target_capacity}' capacity."
    )


@log
def migrate_access_settings(source_capacity: str, target_capacity: str):
    """
    This function migrates the access settings from a source capacity to a target capacity.

    Parameters
    ----------
    source_capacity : str
        Name of the source capacity.
    target_capacity : str
        Name of the target capacity.
    """

    dfC = list_capacities()
    dfC_filt = dfC[dfC["Capacity Name"] == source_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{source_capacity}' capacity does not exist."
        )
    source_capacity_id = dfC_filt["Capacity Id"].iloc[0].upper()
    dfC_filt = dfC[dfC["Capacity Name"] == target_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{target_capacity}' capacity does not exist."
        )
    target_capacity_id = dfC_filt["Capacity Id"].iloc[0].upper()

    client = fabric.PowerBIRestClient()
    response_get_source = client.get(f"capacities/{source_capacity_id}")
    if response_get_source.status_code != 200:
        raise FabricHTTPException(response_get_source)

    access_settings = response_get_source.json().get("access", {})

    response_put = client.put(
        f"capacities/{target_capacity_id}/access",
        json=access_settings,
    )
    if response_put.status_code != 204:
        raise FabricHTTPException(response_put)

    print(
        f"{icons.green_dot} The access settings have been migrated from the '{source_capacity}' capacity to the '{target_capacity}' capacity."
    )


@log
def migrate_notification_settings(source_capacity: str, target_capacity: str):
    """
    This function migrates the notification settings from a source capacity to a target capacity.

    Parameters
    ----------
    source_capacity : str
        Name of the source capacity.
    target_capacity : str
        Name of the target capacity.
    """

    dfC = list_capacities()
    dfC_filt = dfC[dfC["Capacity Name"] == source_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{source_capacity}' capacity does not exist."
        )
    source_capacity_id = dfC_filt["Capacity Id"].iloc[0].upper()
    dfC_filt = dfC[dfC["Capacity Name"] == target_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{target_capacity}' capacity does not exist."
        )
    target_capacity_id = dfC_filt["Capacity Id"].iloc[0].upper()

    client = fabric.PowerBIRestClient()
    response_get_source = client.get(f"capacities/{source_capacity_id}")
    if response_get_source.status_code != 200:
        raise FabricHTTPException(response_get_source)

    notification_settings = response_get_source.json().get(
        "capacityNotificationSettings", {}
    )

    response_put = client.put(
        f"capacities/{target_capacity_id}/notificationSettings",
        json=notification_settings,
    )
    if response_put.status_code != 204:
        raise FabricHTTPException(response_put)

    print(
        f"{icons.green_dot} The notification settings have been migrated from the '{source_capacity}' capacity to the '{target_capacity}' capacity."
    )


@log
def migrate_delegated_tenant_settings(source_capacity: str, target_capacity: str):
    """
    This function migrates the delegated tenant settings from a source capacity to a target capacity.

    Parameters
    ----------
    source_capacity : str
        Name of the source capacity.
    target_capacity : str
        Name of the target capacity.
    """

    dfC = list_capacities()

    dfC_filt = dfC[dfC["Capacity Name"] == source_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{source_capacity}' capacity does not exist."
        )
    source_capacity_id = dfC_filt["Capacity Id"].iloc[0].upper()

    dfC_filt = dfC[dfC["Capacity Name"] == target_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{target_capacity}' capacity does not exist."
        )
    target_capacity_id = dfC_filt["Capacity Id"].iloc[0].upper()

    client = fabric.FabricRestClient()
    response_get = client.get("v1/admin/capacities/delegatedTenantSettingOverrides")

    if response_get.status_code != 200:
        raise FabricHTTPException(response_get)

    response_json = response_get.json().get("Overrides", [])

    for o in response_json:
        if o.get("id").upper() == source_capacity_id:
            for setting in o.get("tenantSettings", []):
                setting_name = setting.get("settingName")
                feature_switch = {
                    "switchId": -1,
                    "switchName": setting_name,
                    "isEnabled": setting.get("enabled", False),
                    "isGranular": setting.get("canSpecifySecurityGroups", False),
                    "allowedSecurityGroups": [
                        {
                            "id": group.get("graphId"),
                            "name": group.get("name"),
                            "isEmailEnabled": False,
                        }
                        for group in setting.get("enabledSecurityGroups", [])
                    ],
                    "deniedSecurityGroups": [
                        {
                            "id": group.get("graphId"),
                            "name": group.get("name"),
                            "isEmailEnabled": False,
                        }
                        for group in setting.get("excludedSecurityGroups", [])
                    ],
                }

                payload = {"featureSwitches": [feature_switch], "properties": []}

                client = fabric.PowerBIRestClient()
                response_put = client.put(
                    f"metadata/tenantsettings/selfserve?capacityObjectId={target_capacity_id}",
                    json=payload,
                )
                if response_put.status_code != 200:
                    raise FabricHTTPException(response_put)

                print(
                    f"{icons.green_dot} The delegated tenant settings for the '{setting_name}' feature switch of the '{source_capacity}' capacity have been migrated to the '{target_capacity}' capacity."
                )


@log
def migrate_spark_settings(source_capacity: str, target_capacity: str):
    """
    This function migrates a capacity's spark settings to another capacity.

    Requirement: The target capacity must be able to accomodate the spark pools being migrated from the source capacity.

    Parameters
    ----------
    source_capacity : str
        Name of the source capacity.
    target_capacity : str
        Name of the target capacity.
    """

    source_capacity_id = resolve_capacity_id(capacity_name=source_capacity)
    target_capacity_id = resolve_capacity_id(capacity_name=target_capacity)
    client = fabric.PowerBIRestClient()

    # Get source capacity server dns
    response = client.get(f"metadata/capacityInformation/{source_capacity_id}")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    source_server_dns = response.json().get("capacityDns")
    source_url = f"{source_server_dns}/webapi/capacities"

    # Get target capacity server dns
    response = client.get(f"metadata/capacityInformation/{target_capacity_id}")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    target_server_dns = response.json().get("capacityDns")
    target_url = f"{target_server_dns}/webapi/capacities"

    # Construct get and put URLs
    end_url = "workloads/SparkCore/SparkCoreService/automatic/v1/sparksettings"
    get_url = f"{source_url}/{source_capacity_id}/{end_url}"
    put_url = f"{target_url}/{target_capacity_id}/{end_url}/content"

    # Get source capacity spark settings
    response = client.get(get_url)
    if response.status_code != 200:
        raise FabricHTTPException(response)

    payload = response.json().get("content")

    # Update target capacity spark settings
    response_put = client.put(put_url, json=payload)

    if response_put.status_code != 200:
        raise FabricHTTPException(response_put)
    print(
        f"{icons.green_dot} The spark settings have been migrated from the '{source_capacity}' capacity to the '{target_capacity}' capacity."
    )


@log
def migrate_fabric_trial_capacity(
    azure_subscription_id: str,
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
    resource_group: str,
    source_capacity: str,
    target_capacity: str,
    target_capacity_sku: str = "F64",
    target_capacity_admin_members: Optional[str | List[str]] = None,
):
    """
    This function migrates a Fabric trial capacity to a Fabric capacity. If the 'target_capacity' does not exist, it is created with the relevant target capacity parameters (sku, region, admin members).

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription ID.
    key_vault_uri : str
        The name of the `Azure key vault <https://azure.microsoft.com/products/key-vault>`_ URI. Example: "https://<Key Vault Name>.vault.azure.net/"
    key_vault_tenant_id : str
        The name of the Azure key vault secret storing the Tenant ID.
    key_vault_client_id : str
        The name of the Azure key vault secret storing the Client ID.
    key_vault_client_secret : str
        The name of the Azure key vault secret storing the Client Secret.
    resource_group : str
        The name of the Azure resource group.
    source_capacity : str
        The name of the Fabric trial capacity.
    target_capacity : str
        The name of the new Fabric capacity (F SKU). If this capacity does not exist, it will be created.
    target_capacity_sku : str, default="F64"
        If the target capacity does not exist, this property sets the SKU size for the target capacity.
    target_capacity_admin_members : str, default=None
        If the target capacity does not exist, this property sets the admin members for the target capacity.
        Defaults to None which resolves to the admin members on the Trial SKU.
    """

    notebook_workspace_id = fabric.get_notebook_workspace_id()
    dfW = fabric.list_workspaces(filter=f"id eq '{notebook_workspace_id}'")
    notebook_capacity_id = dfW["Capacity Id"].iloc[0].lower()

    dfC = list_capacities()
    dfC_filt = dfC[dfC["Capacity Name"] == source_capacity]

    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The {source_capacity}' capacity does not exist."
        )

    source_capacity_sku = dfC_filt["Sku"].iloc[0]
    if not source_capacity_sku.startswith("FT"):
        raise ValueError(
            f"{icons.red_dot} This function is for migrating Fabric trial capacites to Fabric capacities."
        )

    source_capacity_id = dfC_filt["Capacity Id"].iloc[0].lower()
    if source_capacity_id == notebook_capacity_id:
        print(
            f"{icons.warning} The '{source_capacity}' capacity cannot be both the source capacity as well as the capacity in which the notebook is running."
        )
        return

    target_capacity_region = dfC_filt["Region"].iloc[0]

    # Use same admins as source capacity
    if isinstance(target_capacity_admin_members, str):
        target_capacity_admin_members = [target_capacity_admin_members]

    if target_capacity_admin_members is None:
        target_capacity_admin_members = dfC_filt["Admins"].iloc[0]

    dfC_filt = dfC[dfC["Capacity Name"] == target_capacity]
    if len(dfC_filt) == 0:
        create_fabric_capacity(
            capacity_name=target_capacity,
            azure_subscription_id=azure_subscription_id,
            key_vault_uri=key_vault_uri,
            key_vault_tenant_id=key_vault_tenant_id,
            key_vault_client_id=key_vault_client_id,
            key_vault_client_secret=key_vault_client_secret,
            resource_group=resource_group,
            region=target_capacity_region,
            admin_members=target_capacity_admin_members,
            sku=target_capacity_sku,
        )

    assign_workspaces_to_capacity(
        source_capacity=source_capacity,
        target_capacity=target_capacity,
        workspace=None,
    )

    # This migrates all the capacity settings
    migrate_settings(
        source_capacity=source_capacity,
        target_capacity=target_capacity,
    )
