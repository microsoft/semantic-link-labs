import sempy.fabric as fabric
import pandas as pd
import datetime
import requests
from typing import Optional, List, Tuple
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from sempy_labs.lakehouse import lakehouse_attached
from sempy_labs._list_functions import assign_workspace_to_capacity
from sempy_labs.admin._basic_functions import (
    assign_workspaces_to_capacity,
    _list_capacities_meta,
)
from sempy_labs._helper_functions import resolve_capacity_id


def _add_sll_tag(payload, tags):

    if tags is None:
        payload["tags"] = {"SLL": 1}
    else:
        if "tags" not in payload:
            payload["tags"] = tags
        payload["tags"]["SLL"] = 1

    return payload


def get_azure_token_credentials(
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
) -> Tuple[str, str, dict]:

    from notebookutils import mssparkutils
    from azure.identity import ClientSecretCredential

    tenant_id = mssparkutils.credentials.getSecret(key_vault_uri, key_vault_tenant_id)
    client_id = mssparkutils.credentials.getSecret(key_vault_uri, key_vault_client_id)
    client_secret = mssparkutils.credentials.getSecret(
        key_vault_uri, key_vault_client_secret
    )

    credential = ClientSecretCredential(
        tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
    )

    token = credential.get_token("https://management.azure.com/.default").token

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    return token, credential, headers


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
    dfC_filt = dfC[dfC["Display Name"] == source_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} Invalid source capacity. The '{source_capacity}' capacity does not exist."
        )
    source_capacity_region = dfC_filt["Region"].iloc[0]
    # source_capacity_sku = dfC_filt['Sku'].iloc[0]
    source_capacity_id = dfC_filt["Id"].iloc[0]
    dfC_filt = dfC[dfC["Display Name"] == target_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} Invalid target capacity. The '{target_capacity}' capacity does not exist."
        )
    target_capacity_region = dfC_filt["Region"].iloc[0]
    # target_capacity_sku = dfC_filt['Sku'].iloc[0]
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
def create_fabric_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
    resource_group: str,
    region: str,
    sku: str,
    admin_members: str | List[str],
    tags: Optional[dict] = None,
):
    """
    This function creates a new Fabric capacity within an Azure subscription.

    Parameters
    ----------
    capacity_name : str
        Name of the Fabric capacity.
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
    region : str
        The name of the region in which the capacity will be created.
    sku : str
        The `sku size <https://azure.microsoft.com/pricing/details/microsoft-fabric/>`_ of the Fabric capacity.
    admin_members : str | List[str]
        The email address(es) of the admin(s) of the Fabric capacity.
    """

    from azure.mgmt.resource import ResourceManagementClient

    capacity_suffix = "fsku"

    if isinstance(admin_members, str):
        admin_members = [admin_members]

    # list source: https://learn.microsoft.com/fabric/admin/region-availability
    region_list = [
        "Brazil South",
        "North Europe",
        "UAE North",
        "South Africa North",
        "Australia East",
        "Canada Central",
        "West Europe",
        "Australia Southeast",
        "Canada East",
        "France Central",
        "Central India",
        "East US",
        "Germany West Central",
        "East Asia",
        "East US 2",
        "Norway East",
        "Japan East",
        "North Central US",
        "Sweden Central",
        "Korea Central",
        "South Central US",
        "Switzerland North",
        "Southeast Asia",
        "West US",
        "Switzerland West",
        "South India",
        "West US 2",
        "UK South",
        "West US 3",
        "UK West",
        "brazilsouth",
        "northeurope",
        "uaenorth",
        "southafricanorth",
        "australiaeast",
        "canadacentral",
        "westeurope",
        "australiasoutheast",
        "canadaeast",
        "francecentral",
        "centralindia",
        "eastus",
        "germanywestcentral",
        "eastasia",
        "eastus2",
        "norwayeast",
        "japaneast",
        "northcentralus",
        "swedencentral",
        "koreacentral",
        "southcentralus",
        "switzerlandnorth",
        "southeastasia",
        "westus",
        "switzerlandwest",
        "southindia",
        "westus2",
        "uksouth",
        "westus3",
        "ukwest",
    ]

    valid_regions = [
        region for region in region_list if any(char.isupper() for char in region)
    ]

    if region not in region_list:
        raise ValueError(
            f"{icons.red_dot} Invalid region. Valid options: {valid_regions}."
        )

    azure_token, credential, headers = get_azure_token_credentials(
        key_vault_uri=key_vault_uri,
        key_vault_tenant_id=key_vault_tenant_id,
        key_vault_client_id=key_vault_client_id,
        key_vault_client_secret=key_vault_client_secret,
    )

    resource_client = ResourceManagementClient(credential, azure_subscription_id)

    if resource_group is None:
        for i in resource_client.resources.list(
            "resourceType eq 'Microsoft.PowerBIDedicated/capacities'"
        ):
            if i.name == capacity_name.removesuffix(capacity_suffix):
                resource_group = i.id.split("/")[4]
                print(
                    f"{icons.yellow_dot} Override resource group flag detected for A SKUs - using the existing resource group '{resource_group}' for capacity '{capacity_name}'"
                )
    else:
        # Attempt to get the resource group
        try:
            rg = resource_client.resource_groups.get(resource_group)
            if rg.location != region:
                print(
                    f"{icons.yellow_dot} The '{resource_group}' resource group exists, but in a different region."
                )
        except Exception:
            # If the resource group does not exist, create it
            print(
                f"{icons.yellow_dot} The '{resource_group}' resource group does not exist."
            )
            print(
                f"{icons.in_progress} Creating the '{resource_group}' resource group in the '{region}' region"
            )
            rg_result = resource_client.resource_groups.create_or_update(
                resource_group, {"location": region}
            )
            print(
                f"{icons.green_dot} Provisioned resource group with ID: {rg_result.id}"
            )

    payload = {
        "properties": {"administration": {"members": admin_members}},
        "sku": {"name": sku, "tier": "Fabric"},
        "location": region,
    }

    payload = _add_sll_tag(payload, tags)

    print(
        f"{icons.in_progress} Creating the '{capacity_name}' capacity as an '{sku}' SKU within the '{region}' region..."
    )

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version={icons.azure_api_version}"

    response = requests.put(url, headers=headers, json=payload)

    if response.status_code not in [200, 201]:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} Successfully created the '{capacity_name}' capacity within the '{region}' region."
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
    use_existing_rg_for_A_sku: Optional[bool] = True,
    p_sku_only: Optional[bool] = True,
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
        For P skus, if this parameter is a string, it will use that resource group for all of the newly created F skus.
                    if this parameter is a dictionary, it will use that mapping (capacity name -> resource group) for creating capacities with the mapped resource groups.
    capacities : str | List[str], default=None
        The capacity(ies) to migrate from A/P -> F sku.
        Defaults to None which migrates all accessible A/P sku capacities to F skus.
    p_sku_only : bool, default=True
        If set to True, only migrates P skus. If set to False, migrates both P and A skus.
    use_existing_rg_for_A_sku : bool, default=True
        If True, the F sku inherits the resource group from the A sku (for A sku migrations)

    Returns
    -------
    """

    from sempy_labs._list_functions import list_capacities

    capacity_suffix = "fsku"

    if isinstance(capacities, str):
        capacities = [capacities]

    sku_mapping = {
        "A1": "F8",
        "EM1": "F8",
        "A2": "F16",
        "EM2": "F16",
        "A3": "F32",
        "EM3": "F32",
        "A4": "F64",
        "P1": "F64",
        "A5": "F128",
        "P2": "F128",
        "A6": "F256",
        "P3": "F256",
        "A7": "F512",
        "P4": "F512",
        "P5": "F1024",
    }

    p_sku_list = list(sku_mapping.keys())

    dfC = list_capacities()
    # dfW = fabric.list_workspaces()

    # Save existing capacity and workspace info to delta tables in the lakehouse
    if not lakehouse_attached:
        raise ValueError(
            f"{icons.red_dot} Invalid lakehouse. Please attach a lakehouse to this notebook."
        )

    # save_as_delta_table(
    #    dataframe=dfC,
    #    delta_table_name="migration_list_capacities",
    #    write_mode="overwrite",
    # )
    # save_as_delta_table(
    #    dataframe=dfW,
    #    delta_table_name="migration_list_workspaces",
    #    write_mode="overwrite",
    # )

    if capacities is None:
        dfC_filt = dfC.copy()
    else:
        dfC_filt = dfC[dfC["Display Name"].isin(capacities)]

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
        cap_name = r["Display Name"]
        region = r["Region"]
        sku_size = r["Sku"]
        admins = r["Admins"]
        tgt_capacity = f"{cap_name}{capacity_suffix}"

        # Check if target capacity exists
        dfC_tgt = dfC[dfC["Display Name"] == tgt_capacity]

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
            if len(dfC_tgt) != 0:
                print(
                    f"{icons.info} Skipping creating a new capacity for '{cap_name}' as the '{tgt_capacity}' capacity already exists."
                )
            else:
                start_time = datetime.datetime.now()
                create_fabric_capacity(
                    capacity_name=tgt_capacity,
                    azure_subscription_id=azure_subscription_id,
                    key_vault_uri=key_vault_uri,
                    key_vault_tenant_id=key_vault_tenant_id,
                    key_vault_client_id=key_vault_client_id,
                    key_vault_client_secret=key_vault_client_secret,
                    resource_group=rg,
                    region=region,
                    sku=sku_mapping.get(sku_size),
                    admin_members=admins,
                )
                end_time = datetime.datetime.now()
                print(
                    f"Total time for creating the Fabric capacity is {str((end_time - start_time).total_seconds())}"
                )
            # Migrate workspaces to new capacity
            assign_workspaces_to_capacity(
                source_capacity=cap_name, target_capacity=tgt_capacity, workspace=None
            )

            # Migrate settings to new capacity
            start_time = datetime.datetime.now()
            migrate_capacity_settings(
                source_capacity=cap_name, target_capacity=tgt_capacity
            )
            end_time = datetime.datetime.now()
            print(
                f"Total time for migrating capacity settings is {str((end_time - start_time).total_seconds())}"
            )

            start_time = datetime.datetime.now()
            migrate_access_settings(
                source_capacity=cap_name, target_capacity=tgt_capacity
            )
            end_time = datetime.datetime.now()
            print(
                f"Total time for migrating access settings is {str((end_time - start_time).total_seconds())}"
            )

            start_time = datetime.datetime.now()
            migrate_notification_settings(
                source_capacity=cap_name, target_capacity=tgt_capacity
            )
            end_time = datetime.datetime.now()
            print(
                f"Total time for migrating notification settings is {str((end_time - start_time).total_seconds())}"
            )

            start_time = datetime.datetime.now()
            migrate_delegated_tenant_settings(
                source_capacity=cap_name, target_capacity=tgt_capacity
            )
            end_time = datetime.datetime.now()
            print(
                f"Total time for migrating delegated tenant settings is {str((end_time - start_time).total_seconds())}"
            )

            start_time = datetime.datetime.now()
            migrate_disaster_recovery_settings(
                source_capacity=cap_name, target_capacity=tgt_capacity
            )
            end_time = datetime.datetime.now()
            print(
                f"Total time for migrating disaster recovery settings is {str((end_time - start_time).total_seconds())}"
            )

            start_time = datetime.datetime.now()
            migrate_spark_settings(
                source_capacity=cap_name, target_capacity=tgt_capacity
            )
            end_time = datetime.datetime.now()
            print(
                f"Total time for migrating disaster recovery settings is {str((end_time - start_time).total_seconds())}"
            )


def migrate_capacity_settings(source_capacity: str, target_capacity: str):
    """
    This function migrates a capacity's settings to another capacity.

    Parameters
    ----------
    source_capacity : str
        Name of the source capacity.
    target_capacity : str
        Name of the target capacity.

    Returns
    -------
    """

    dfC = fabric.list_capacities()
    dfC_filt = dfC[dfC["Display Name"] == source_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{source_capacity}' capacity does not exist."
        )
    source_capacity_id = dfC_filt["Id"].iloc[0].upper()
    dfC_filt = dfC[dfC["Display Name"] == target_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{target_capacity}' capacity does not exist."
        )
    target_capacity_id = dfC_filt["Id"].iloc[0].upper()

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
        f"{icons.green_dot} The settings of the '{source_capacity}' capacity have been migrated to the '{target_capacity}' capacity."
    )


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

    dfC = fabric.list_capacities()
    dfC_filt = dfC[dfC["Display Name"] == source_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{source_capacity}' capacity does not exist."
        )
    source_capacity_id = dfC_filt["Id"].iloc[0].upper()
    dfC_filt = dfC[dfC["Display Name"] == target_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{target_capacity}' capacity does not exist."
        )
    target_capacity_id = dfC_filt["Id"].iloc[0].upper()

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
        f"{icons.green_dot} Disaster recovery settings have been migrated from the '{source_capacity}' capacity to the '{target_capacity}' capacity."
    )


def list_vcores() -> pd.DataFrame:

    df = pd.DataFrame(columns=["Total Purchased Cores", "Available Cores"])

    client = fabric.PowerBIRestClient()
    response = client.get("capacities/vcores")
    if response.status_code != 200:
        FabricHTTPException(response)
    response_json = response.json()
    new_data = {
        "Total Purchased Cores": response_json.get("totalPurchasedCores"),
        "Available Cores": response_json.get("availableCores"),
    }
    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    int_cols = ["Total Purchased Cores", "Available Cores"]
    df[int_cols] = df[int_cols].astype(int)

    return df


def get_capacity_resource_governance(capacity_name: str):

    dfC = fabric.list_capacities()
    dfC_filt = dfC[dfC["Display Name"] == capacity_name]
    capacity_id = dfC_filt["Id"].iloc[0].upper()
    client = fabric.PowerBIRestClient()
    response = client.get(f"capacities/{capacity_id}/resourceGovernance")

    if response.status_code != 200:
        FabricHTTPException(response)

    return response.json()["workloadSettings"]


def migrate_access_settings(source_capacity: str, target_capacity: str):
    """
    This function migrates the access settings from a source capacity to a target capacity.

    Parameters
    ----------
    source_capacity : str
        Name of the source capacity.
    target_capacity : str
        Name of the target capacity.

    Returns
    -------
    """

    dfC = fabric.list_capacities()
    dfC_filt = dfC[dfC["Display Name"] == source_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{source_capacity}' capacity does not exist."
        )
    source_capacity_id = dfC_filt["Id"].iloc[0].upper()
    dfC_filt = dfC[dfC["Display Name"] == target_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{target_capacity}' capacity does not exist."
        )
    target_capacity_id = dfC_filt["Id"].iloc[0].upper()

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
        f"{icons.green_dot} The access settings of the '{source_capacity}' capacity have been migrated to the '{target_capacity}' capacity."
    )


def migrate_notification_settings(source_capacity: str, target_capacity: str):
    """
    This function migrates the notification settings from a source capacity to a target capacity.

    Parameters
    ----------
    source_capacity : str
        Name of the source capacity.
    target_capacity : str
        Name of the target capacity.

    Returns
    -------
    """

    dfC = fabric.list_capacities()
    dfC_filt = dfC[dfC["Display Name"] == source_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{source_capacity}' capacity does not exist."
        )
    source_capacity_id = dfC_filt["Id"].iloc[0].upper()
    dfC_filt = dfC[dfC["Display Name"] == target_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{target_capacity}' capacity does not exist."
        )
    target_capacity_id = dfC_filt["Id"].iloc[0].upper()

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
        f"{icons.green_dot} The notification settings of the '{source_capacity}' capacity have been migrated to the '{target_capacity}' capacity."
    )


def migrate_delegated_tenant_settings(source_capacity: str, target_capacity: str):
    """
    This function migrates the delegated tenant settings from a source capacity to a target capacity.

    Parameters
    ----------
    source_capacity : str
        Name of the source capacity.
    target_capacity : str
        Name of the target capacity.

    Returns
    -------
    """

    dfC = fabric.list_capacities()

    dfC_filt = dfC[dfC["Display Name"] == source_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{source_capacity}' capacity does not exist."
        )
    source_capacity_id = dfC_filt["Id"].iloc[0].upper()

    dfC_filt = dfC[dfC["Display Name"] == target_capacity]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{target_capacity}' capacity does not exist."
        )
    target_capacity_id = dfC_filt["Id"].iloc[0].upper()

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


def suspend_fabric_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    resource_group: str,
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
):
    # https://learn.microsoft.com/en-us/rest/api/microsoftfabric/fabric-capacities/suspend?view=rest-microsoftfabric-2023-11-01&tabs=HTTP

    azure_token, credential, headers = get_azure_token_credentials(
        key_vault_uri=key_vault_uri,
        key_vault_tenant_id=key_vault_tenant_id,
        key_vault_client_id=key_vault_client_id,
        key_vault_client_secret=key_vault_client_secret,
    )

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/suspend?api-version={icons.azure_api_version}"

    response = requests.post(url, headers=headers)

    if response.status_code != 202:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{capacity_name} capacity has been suspended.")


def resume_fabric_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    resource_group: str,
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
):
    # https://learn.microsoft.com/en-us/rest/api/microsoftfabric/fabric-capacities/resume?view=rest-microsoftfabric-2023-11-01&tabs=HTTP

    azure_token, credential, headers = get_azure_token_credentials(
        key_vault_uri=key_vault_uri,
        key_vault_tenant_id=key_vault_tenant_id,
        key_vault_client_id=key_vault_client_id,
        key_vault_client_secret=key_vault_client_secret,
    )

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/resume?api-version={icons.azure_api_version}"

    response = requests.post(url, headers=headers)

    if response.status_code != 202:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{capacity_name} capacity has been resumed.")


def delete_embedded_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    resource_group: str,
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
):

    # https://learn.microsoft.com/en-us/rest/api/power-bi-embedded/capacities/delete?view=rest-power-bi-embedded-2021-01-01&tabs=HTTP

    azure_token, credential, headers = get_azure_token_credentials(
        key_vault_uri=key_vault_uri,
        key_vault_tenant_id=key_vault_tenant_id,
        key_vault_client_id=key_vault_client_id,
        key_vault_client_secret=key_vault_client_secret,
    )

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.PowerBIDedicated/capacities/{capacity_name}?api-version={icons.azure_api_version}"

    response = requests.delete(url, headers=headers)

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{capacity_name} capacity has been deleted.")


def delete_premium_capacity(capacity_name: str):

    dfC = fabric.list_capacities()

    dfC_filt = dfC[dfC["Display Name"] == capacity_name]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{capacity_name}' capacity does not exist."
        )
    capacity_id = dfC_filt["Id"].iloc[0].upper()

    client = fabric.FabricRestClient()
    response = client.delete(f"capacities/{capacity_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{capacity_name}' capacity has been deleted.")


def delete_fabric_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    resource_group: str,
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
):
    # https://learn.microsoft.com/en-us/rest/api/microsoftfabric/fabric-capacities/delete?view=rest-microsoftfabric-2023-11-01&tabs=HTTP

    azure_token, credential, headers = get_azure_token_credentials(
        key_vault_uri=key_vault_uri,
        key_vault_tenant_id=key_vault_tenant_id,
        key_vault_client_id=key_vault_client_id,
        key_vault_client_secret=key_vault_client_secret,
    )

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version={icons.azure_api_version}"

    response = requests.delete(url, headers=headers)

    if response.status_code != 202:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{capacity_name} capacity has been deleted.")


def update_fabric_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    resource_group: str,
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
    sku: Optional[str] = None,
    admin_members: Optional[str | List[str]] = None,
    tags: Optional[dict] = None,
):

    # https://learn.microsoft.com/en-us/rest/api/microsoftfabric/fabric-capacities/update?view=rest-microsoftfabric-2023-11-01&tabs=HTTP

    if isinstance(admin_members, str):
        admin_members = [admin_members]
    if tags is not None and not isinstance(tags, dict):
        raise ValueError(
            f"{icons.red_dot} If specified, the 'tags' parameter must be a dictionary."
        )

    azure_token, credential, headers = get_azure_token_credentials(
        key_vault_uri=key_vault_uri,
        key_vault_tenant_id=key_vault_tenant_id,
        key_vault_client_id=key_vault_client_id,
        key_vault_client_secret=key_vault_client_secret,
    )

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version={icons.azure_api_version}"

    payload = {}
    if sku is not None:
        payload["sku"] = {"name": sku, "tier": "Fabric"}
    if admin_members is not None:
        payload["properties"] = {"administration": {"members": [admin_members]}}

    payload = _add_sll_tag(payload, tags)

    if payload == {}:
        raise ValueError(
            f"{icons.warning} No parameters have been set to update the '{capacity_name}' capacity."
        )

    response = requests.patch(url, headers=headers, data=payload)

    if response.status_code != 202:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{capacity_name} capacity has been updated accordingly."
    )


def check_fabric_capacity_name_availablility(
    capacity_name: str,
    azure_subscription_id: str,
    region: str,
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
) -> bool:
    # https://learn.microsoft.com/en-us/rest/api/microsoftfabric/fabric-capacities/check-name-availability?view=rest-microsoftfabric-2023-11-01&tabs=HTTP

    azure_token, credential, headers = get_azure_token_credentials(
        key_vault_uri=key_vault_uri,
        key_vault_tenant_id=key_vault_tenant_id,
        key_vault_client_id=key_vault_client_id,
        key_vault_client_secret=key_vault_client_secret,
    )

    payload = {"name": capacity_name, "type": "Microsoft.Fabric/capacities"}

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/providers/Microsoft.Fabric/locations/{region}/checkNameAvailability?api-version={icons.azure_api_version}"

    response = requests.post(url, headers=headers, data=payload)

    if response.status_code != 202:
        raise FabricHTTPException(response)

    return bool(response.json().get("nameAvailable"))


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
