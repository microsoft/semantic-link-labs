import sempy.fabric as fabric
from typing import Optional, List
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
import requests
from sempy_labs._helper_functions import _get_azure_token_credentials
import pandas as pd


def _add_sll_tag(payload, tags):

    if tags is None:
        payload["tags"] = {"SLL": 1}
    else:
        if "tags" not in payload:
            payload["tags"] = tags
        payload["tags"]["SLL"] = 1

    return payload


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

    This is a wrapper function for the following API: `Fabric Capacities - Create Or Update <https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/create-or-update?view=rest-microsoftfabric-2023-11-01>`_.

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
    tags: dict, default=None
        Tag(s) to add to the capacity. Example: {'tagName': 'tagValue'}.
    """

    from azure.mgmt.resource import ResourceManagementClient

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

    azure_token, credential, headers = _get_azure_token_credentials(
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
            if i.name == capacity_name.removesuffix(icons.migrate_capacity_suffix):
                resource_group = i.id.split("/")[4]
                print(
                    f"{icons.yellow_dot} Override resource group flag detected for A SKUs - using the existing resource group '{resource_group}' for the '{capacity_name}' capacity."
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


def suspend_fabric_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    resource_group: str,
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
):
    """
    This function suspends a Fabric capacity.

    This is a wrapper function for the following API: `Fabric Capacities - Suspend <https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/suspend?view=rest-microsoftfabric-2023-11-01>`_.

    Parameters
    ----------
    capacity_name : str
        Name of the Fabric capacity.
    azure_subscription_id : str
        The Azure subscription ID.
    resource_group : str
        The name of the Azure resource group.
    key_vault_uri : str
        The name of the `Azure key vault <https://azure.microsoft.com/products/key-vault>`_ URI. Example: "https://<Key Vault Name>.vault.azure.net/"
    key_vault_tenant_id : str
        The name of the Azure key vault secret storing the Tenant ID.
    key_vault_client_id : str
        The name of the Azure key vault secret storing the Client ID.
    key_vault_client_secret : str
        The name of the Azure key vault secret storing the Client Secret.
    """
    # https://learn.microsoft.com/en-us/rest/api/microsoftfabric/fabric-capacities/suspend?view=rest-microsoftfabric-2023-11-01&tabs=HTTP

    azure_token, credential, headers = _get_azure_token_credentials(
        key_vault_uri=key_vault_uri,
        key_vault_tenant_id=key_vault_tenant_id,
        key_vault_client_id=key_vault_client_id,
        key_vault_client_secret=key_vault_client_secret,
    )

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/suspend?api-version={icons.azure_api_version}"

    response = requests.post(url, headers=headers)

    if response.status_code != 202:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{capacity_name}' capacity has been suspended.")


def resume_fabric_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    resource_group: str,
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
):
    """
    This function resumes a Fabric capacity.

    This is a wrapper function for the following API: `Fabric Capacities - Resume <https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/resume?view=rest-microsoftfabric-2023-11-01>`_.

    Parameters
    ----------
    capacity_name : str
        Name of the Fabric capacity.
    azure_subscription_id : str
        The Azure subscription ID.
    resource_group : str
        The name of the Azure resource group.
    key_vault_uri : str
        The name of the `Azure key vault <https://azure.microsoft.com/products/key-vault>`_ URI. Example: "https://<Key Vault Name>.vault.azure.net/"
    key_vault_tenant_id : str
        The name of the Azure key vault secret storing the Tenant ID.
    key_vault_client_id : str
        The name of the Azure key vault secret storing the Client ID.
    key_vault_client_secret : str
        The name of the Azure key vault secret storing the Client Secret.
    """

    # https://learn.microsoft.com/en-us/rest/api/microsoftfabric/fabric-capacities/resume?view=rest-microsoftfabric-2023-11-01&tabs=HTTP

    azure_token, credential, headers = _get_azure_token_credentials(
        key_vault_uri=key_vault_uri,
        key_vault_tenant_id=key_vault_tenant_id,
        key_vault_client_id=key_vault_client_id,
        key_vault_client_secret=key_vault_client_secret,
    )

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/resume?api-version={icons.azure_api_version}"

    response = requests.post(url, headers=headers)

    if response.status_code != 202:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{capacity_name}' capacity has been resumed.")


def delete_embedded_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    resource_group: str,
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
):
    """
    This function deletes a Power BI Embedded capacity.

    Parameters
    ----------
    capacity_name : str
        Name of the Fabric capacity.
    azure_subscription_id : str
        The Azure subscription ID.
    resource_group : str
        The name of the Azure resource group.
    key_vault_uri : str
        The name of the `Azure key vault <https://azure.microsoft.com/products/key-vault>`_ URI. Example: "https://<Key Vault Name>.vault.azure.net/"
    key_vault_tenant_id : str
        The name of the Azure key vault secret storing the Tenant ID.
    key_vault_client_id : str
        The name of the Azure key vault secret storing the Client ID.
    key_vault_client_secret : str
        The name of the Azure key vault secret storing the Client Secret.
    """

    # https://learn.microsoft.com/en-us/rest/api/power-bi-embedded/capacities/delete?view=rest-power-bi-embedded-2021-01-01&tabs=HTTP

    azure_token, credential, headers = _get_azure_token_credentials(
        key_vault_uri=key_vault_uri,
        key_vault_tenant_id=key_vault_tenant_id,
        key_vault_client_id=key_vault_client_id,
        key_vault_client_secret=key_vault_client_secret,
    )

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.PowerBIDedicated/capacities/{capacity_name}?api-version={icons.azure_api_version}"

    response = requests.delete(url, headers=headers)

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{capacity_name}' capacity has been deleted.")


def delete_premium_capacity(capacity_name: str):
    """
    This function deletes a Power BI Premium capacity.

    Parameters
    ----------
    capacity_name : str
        Name of the Fabric capacity.
    """

    dfC = fabric.list_capacities()

    dfC_filt = dfC[dfC["Display Name"] == capacity_name]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{capacity_name}' capacity does not exist."
        )
    capacity_id = dfC_filt["Id"].iloc[0].upper()

    client = fabric.FabricRestClient()
    response = client.delete(f"capacities/{capacity_id}")

    if response.status_code != 204:
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
    """
    This function deletes a Fabric capacity.

    This is a wrapper function for the following API: `Fabric Capacities - Delete <https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/delete?view=rest-microsoftfabric-2023-11-01>`_.

    Parameters
    ----------
    capacity_name : str
        Name of the Fabric capacity.
    azure_subscription_id : str
        The Azure subscription ID.
    resource_group : str
        The name of the Azure resource group.
    key_vault_uri : str
        The name of the `Azure key vault <https://azure.microsoft.com/products/key-vault>`_ URI. Example: "https://<Key Vault Name>.vault.azure.net/"
    key_vault_tenant_id : str
        The name of the Azure key vault secret storing the Tenant ID.
    key_vault_client_id : str
        The name of the Azure key vault secret storing the Client ID.
    key_vault_client_secret : str
        The name of the Azure key vault secret storing the Client Secret.
    """

    # https://learn.microsoft.com/en-us/rest/api/microsoftfabric/fabric-capacities/delete?view=rest-microsoftfabric-2023-11-01&tabs=HTTP

    azure_token, credential, headers = _get_azure_token_credentials(
        key_vault_uri=key_vault_uri,
        key_vault_tenant_id=key_vault_tenant_id,
        key_vault_client_id=key_vault_client_id,
        key_vault_client_secret=key_vault_client_secret,
    )

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version={icons.azure_api_version}"

    response = requests.delete(url, headers=headers)

    if response.status_code != 202:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{capacity_name}' capacity has been deleted.")


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
    """
    This function updates a Fabric capacity's properties.

    This is a wrapper function for the following API: `Fabric Capacities - Update <https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/update?view=rest-microsoftfabric-2023-11-01>`_.

    Parameters
    ----------
    capacity_name : str
        Name of the Fabric capacity.
    azure_subscription_id : str
        The Azure subscription ID.
    resource_group : str
        The name of the Azure resource group.
    key_vault_uri : str
        The name of the `Azure key vault <https://azure.microsoft.com/products/key-vault>`_ URI. Example: "https://<Key Vault Name>.vault.azure.net/"
    key_vault_tenant_id : str
        The name of the Azure key vault secret storing the Tenant ID.
    key_vault_client_id : str
        The name of the Azure key vault secret storing the Client ID.
    key_vault_client_secret : str
        The name of the Azure key vault secret storing the Client Secret.
    sku : str, default=None
         The `sku size <https://azure.microsoft.com/pricing/details/microsoft-fabric/>`_ of the Fabric capacity.
    admin_members : str | List[str], default=None
        The email address(es) of the admin(s) of the Fabric capacity.
    tags : dict, default=None
        Tag(s) to add to the capacity. Example: {'tagName': 'tagValue'}.
    """

    # https://learn.microsoft.com/en-us/rest/api/microsoftfabric/fabric-capacities/update?view=rest-microsoftfabric-2023-11-01&tabs=HTTP

    if isinstance(admin_members, str):
        admin_members = [admin_members]
    if tags is not None and not isinstance(tags, dict):
        raise ValueError(
            f"{icons.red_dot} If specified, the 'tags' parameter must be a dictionary."
        )

    azure_token, credential, headers = _get_azure_token_credentials(
        key_vault_uri=key_vault_uri,
        key_vault_tenant_id=key_vault_tenant_id,
        key_vault_client_id=key_vault_client_id,
        key_vault_client_secret=key_vault_client_secret,
    )

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version={icons.azure_api_version}"

    get_response = requests.get(url, headers=headers)
    if get_response.status_code != 200:
        raise FabricHTTPException(get_response)

    get_json = get_response.json()
    current_sku = get_json.get("sku", {}).get("name")
    current_admins = (
        get_json.get("properties", {}).get("administration", {}).get("members")
    )
    current_tags = get_json.get("tags")

    payload = {}
    payload["sku"] = {
        "name": current_sku,
        "tier": "Fabric",
    }
    payload["tags"] = current_tags
    payload["properties"] = get_json["properties"]

    if sku is not None:
        payload["sku"]["name"] = sku
    if admin_members is not None:
        payload["properties"] = {"administration": {"members": admin_members}}
    if tags is not None:
        payload["tags"] = tags

    # Do not proceed if no properties are being changed
    if current_sku == sku and current_admins == admin_members and current_tags == tags:
        print(
            f"{icons.yellow_dot} The properties of the '{capacity_name}' are the same as those specified in the parameters of this function. No changes have been made."
        )
        return

    payload = _add_sll_tag(payload, tags)
    response = requests.patch(url, headers=headers, json=payload)

    if response.status_code != 202:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{capacity_name}' capacity has been updated accordingly."
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
    """
    This function updates a Fabric capacity's properties.

    This is a wrapper function for the following API: `Fabric Capacities - Check Name Availability <https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/check-name-availability?view=rest-microsoftfabric-2023-11-01>`_.

    Parameters
    ----------
    capacity_name : str
        Name of the Fabric capacity.
    azure_subscription_id : str
        The Azure subscription ID.
    region : str
        The region name.
    key_vault_uri : str
        The name of the `Azure key vault <https://azure.microsoft.com/products/key-vault>`_ URI. Example: "https://<Key Vault Name>.vault.azure.net/"
    key_vault_tenant_id : str
        The name of the Azure key vault secret storing the Tenant ID.
    key_vault_client_id : str
        The name of the Azure key vault secret storing the Client ID.
    key_vault_client_secret : str
        The name of the Azure key vault secret storing the Client Secret.

    Returns
    -------
    bool
        An indication as to whether the Fabric capacity name is available or not.
    """
    # https://learn.microsoft.com/en-us/rest/api/microsoftfabric/fabric-capacities/check-name-availability?view=rest-microsoftfabric-2023-11-01&tabs=HTTP

    azure_token, credential, headers = _get_azure_token_credentials(
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


def create_resource_group(
    azure_subscription_id: str,
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
    resource_group: str,
    region: str,
):
    """
    This function creates a resource group in a region within an Azure subscription.

    This is a wrapper function for the following API: `ResourceGroupsOperations Class - CreateOrUpdate <https://learn.microsoft.com/python/api/azure-mgmt-resource/azure.mgmt.resource.resources.v2022_09_01.operations.resourcegroupsoperations?view=azure-python#azure-mgmt-resource-resources-v2022-09-01-operations-resourcegroupsoperations-create-or-update>`_.

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
        The name of the Azure resource group to be created.
    region : str
        The name of the region in which the resource group will be created.
    """

    from azure.mgmt.resource import ResourceManagementClient

    azure_token, credential, headers = _get_azure_token_credentials(
        key_vault_uri=key_vault_uri,
        key_vault_tenant_id=key_vault_tenant_id,
        key_vault_client_id=key_vault_client_id,
        key_vault_client_secret=key_vault_client_secret,
    )

    resource_client = ResourceManagementClient(credential, azure_subscription_id)

    if resource_client.resource_groups.check_existence(resource_group):
        print(
            f"{icons.info} The '{resource_group}' resource group already exists in the '{region}' region within the '{azure_subscription_id}' Azure subscription."
        )
        return

    resource_client.resource_groups.create_or_update(
        resource_group, {"location": region}
    )

    print(
        f"{icons.green_dot} The '{resource_group}' resource group has been created within the '{region}' region within the '{azure_subscription_id}' Azure subscription."
    )
