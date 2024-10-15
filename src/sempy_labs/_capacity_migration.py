import sempy.fabric as fabric
from typing import Optional, List
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from sempy_labs._workspaces import assign_workspace_to_capacity
from sempy_labs.admin._basic_functions import (
    assign_workspaces_to_capacity,
    _list_capacities_meta,
    list_capacities,
)
from sempy_labs._helper_functions import (
    resolve_capacity_id,
    convert_to_alphanumeric_lowercase,
)
from sempy_labs._capacities import create_fabric_capacity


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
        region = r["Region"]
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
            # Migrate workspaces to new capacity
            assign_workspaces_to_capacity(
                source_capacity=cap_name, target_capacity=tgt_capacity, workspace=None
            )

            # Migrate settings to new capacity
            migrate_settings(source_capacity=cap_name, target_capacity=tgt_capacity)


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
