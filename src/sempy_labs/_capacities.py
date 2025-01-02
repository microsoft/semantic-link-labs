import sempy.fabric as fabric
from typing import Optional, List, Tuple
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
import requests
import pandas as pd
from sempy_labs._authentication import _get_headers, ServicePrincipalTokenProvider
from sempy.fabric._token_provider import TokenProvider
from uuid import UUID
from sempy_labs._helper_functions import _is_valid_uuid


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
    resource_group: str,
    region: str,
    sku: str,
    admin_members: str | List[str],
    token_provider: Optional[TokenProvider] = None,
    tags: Optional[dict] = None,
    **kwargs,
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
    resource_group : str
        The name of the Azure resource group.
    region : str
        The name of the region in which the capacity will be created.
    sku : str
        The `sku size <https://azure.microsoft.com/pricing/details/microsoft-fabric/>`_ of the Fabric capacity.
    admin_members : str | List[str]
        The email address(es) of the admin(s) of the Fabric capacity.
    token_provider : TokenProvider, default=None
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.
    tags: dict, default=None
        Tag(s) to add to the capacity. Example: {'tagName': 'tagValue'}.
    """

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

    if token_provider is None:
        token_provider = ServicePrincipalTokenProvider.from_azure_key_vault(
            key_vault_uri=kwargs["key_vault_uri"],
            key_vault_tenant_id=kwargs["key_vault_tenant_id"],
            key_vault_client_id=kwargs["key_vault_client_id"],
            key_vault_client_secret=kwargs["key_vault_client_secret"],
        )
        print(
            f"{icons.info} Please use the 'token_provider' parameter instead of the key vault parameters within this function as the key vault parameters have been deprecated."
        )
    headers = _get_headers(token_provider, audience="azure")

    if resource_group is None:
        dfRG = list_resource_groups(
            azure_subscription_id=azure_subscription_id,
            token_provider=token_provider,
            filter="resourceType eq 'Microsoft.PowerBIDedicated/capacities'",
        )
        dfRG_filt = dfRG[
            dfRG["Resource Group Name"]
            == capacity_name.removesuffix(icons.migrate_capacity_suffix)
        ]
        if not dfRG_filt.empty:
            resource_group = dfRG_filt["Resource Group Name"].iloc[0]
            print(
                f"{icons.yellow_dot} Override resource group flag detected for A SKUs - using the existing resource group '{resource_group}' for the '{capacity_name}' capacity."
            )
    else:
        # Attempt to get the resource group
        try:
            dfRG = get_resource_group(
                azure_subscription_id=azure_subscription_id,
                resource_group=resource_group,
                token_provider=token_provider,
            )
            if dfRG["Location"].iloc[0] != region:
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
            create_or_update_resource_group(
                azure_subscription_id=azure_subscription_id,
                resource_group=resource_group,
                region=region,
                token_provider=token_provider,
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
    token_provider: Optional[TokenProvider] = None,
    **kwargs,
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
    token_provider : TokenProvider, default=None
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.
    """

    if token_provider is None:
        token_provider = ServicePrincipalTokenProvider.from_azure_key_vault(
            key_vault_uri=kwargs["key_vault_uri"],
            key_vault_tenant_id=kwargs["key_vault_tenant_id"],
            key_vault_client_id=kwargs["key_vault_client_id"],
            key_vault_client_secret=kwargs["key_vault_client_secret"],
        )
        print(
            f"{icons.info} Please use the 'token_provider' parameter instead of the key vault parameters within this function as the key vault parameters have been deprecated."
        )

    headers = _get_headers(token_provider, audience="azure")

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/suspend?api-version={icons.azure_api_version}"

    response = requests.post(url, headers=headers)

    if response.status_code != 202:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{capacity_name}' capacity has been suspended.")


def resume_fabric_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    resource_group: str,
    token_provider: Optional[TokenProvider] = None,
    **kwargs,
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
    token_provider : TokenProvider, default=None
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.
    """

    if token_provider is None:
        token_provider = ServicePrincipalTokenProvider.from_azure_key_vault(
            key_vault_uri=kwargs["key_vault_uri"],
            key_vault_tenant_id=kwargs["key_vault_tenant_id"],
            key_vault_client_id=kwargs["key_vault_client_id"],
            key_vault_client_secret=kwargs["key_vault_client_secret"],
        )
        print(
            f"{icons.info} Please use the 'token_provider' parameter instead of the key vault parameters within this function as the key vault parameters have been deprecated."
        )

    headers = _get_headers(token_provider, audience="azure")

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/resume?api-version={icons.azure_api_version}"

    response = requests.post(url, headers=headers)

    if response.status_code != 202:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{capacity_name}' capacity has been resumed.")


def delete_embedded_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    resource_group: str,
    token_provider: Optional[TokenProvider] = None,
    **kwargs,
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
    token_provider : TokenProvider, default=None
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.
    """

    if token_provider is None:
        token_provider = ServicePrincipalTokenProvider.from_azure_key_vault(
            key_vault_uri=kwargs["key_vault_uri"],
            key_vault_tenant_id=kwargs["key_vault_tenant_id"],
            key_vault_client_id=kwargs["key_vault_client_id"],
            key_vault_client_secret=kwargs["key_vault_client_secret"],
        )
        print(
            f"{icons.info} Please use the 'token_provider' parameter instead of the key vault parameters within this function as the key vault parameters have been deprecated."
        )

    headers = _get_headers(token_provider, audience="azure")

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
    token_provider: Optional[TokenProvider] = None,
    **kwargs,
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
    token_provider : TokenProvider, default=None
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.
    """

    if token_provider is None:
        token_provider = ServicePrincipalTokenProvider.from_azure_key_vault(
            key_vault_uri=kwargs["key_vault_uri"],
            key_vault_tenant_id=kwargs["key_vault_tenant_id"],
            key_vault_client_id=kwargs["key_vault_client_id"],
            key_vault_client_secret=kwargs["key_vault_client_secret"],
        )
        print(
            f"{icons.info} Please use the 'token_provider' parameter instead of the key vault parameters within this function as the key vault parameters have been deprecated."
        )

    headers = _get_headers(token_provider, audience="azure")

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version={icons.azure_api_version}"

    response = requests.delete(url, headers=headers)

    if response.status_code != 202:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{capacity_name}' capacity has been deleted.")


def update_fabric_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    resource_group: str,
    token_provider: Optional[TokenProvider] = None,
    sku: Optional[str] = None,
    admin_members: Optional[str | List[str]] = None,
    tags: Optional[dict] = None,
    **kwargs,
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
    token_provider : TokenProvider, default=None
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.
    sku : str, default=None
         The `sku size <https://azure.microsoft.com/pricing/details/microsoft-fabric/>`_ of the Fabric capacity.
    admin_members : str | List[str], default=None
        The email address(es) of the admin(s) of the Fabric capacity.
    tags : dict, default=None
        Tag(s) to add to the capacity. Example: {'tagName': 'tagValue'}.
    """

    if token_provider is None:
        token_provider = ServicePrincipalTokenProvider.from_azure_key_vault(
            key_vault_uri=kwargs["key_vault_uri"],
            key_vault_tenant_id=kwargs["key_vault_tenant_id"],
            key_vault_client_id=kwargs["key_vault_client_id"],
            key_vault_client_secret=kwargs["key_vault_client_secret"],
        )
        print(
            f"{icons.info} Please use the 'token_provider' parameter instead of the key vault parameters within this function as the key vault parameters have been deprecated."
        )

    headers = _get_headers(token_provider, audience="azure")

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
    token_provider: Optional[TokenProvider] = None,
    **kwargs,
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
    token_provider : TokenProvider, default=None
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.

    Returns
    -------
    bool
        An indication as to whether the Fabric capacity name is available or not.
    """

    if token_provider is None:
        token_provider = ServicePrincipalTokenProvider.from_azure_key_vault(
            key_vault_uri=kwargs["key_vault_uri"],
            key_vault_tenant_id=kwargs["key_vault_tenant_id"],
            key_vault_client_id=kwargs["key_vault_client_id"],
            key_vault_client_secret=kwargs["key_vault_client_secret"],
        )
        print(
            f"{icons.info} Please use the 'token_provider' parameter instead of the key vault parameters within this function as the key vault parameters have been deprecated."
        )

    headers = _get_headers(token_provider, audience="azure")

    payload = {"name": capacity_name, "type": "Microsoft.Fabric/capacities"}

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/providers/Microsoft.Fabric/locations/{region}/checkNameAvailability?api-version={icons.azure_api_version}"

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code != 202:
        raise FabricHTTPException(response)

    return bool(response.json().get("nameAvailable"))


def create_resource_group(
    azure_subscription_id: str,
    resource_group: str,
    region: str,
    token_provider: Optional[TokenProvider] = None,
    **kwargs,
):
    """
    This function creates a resource group in a region within an Azure subscription.

    This is a wrapper function for the following API: `ResourceGroupsOperations Class - CreateOrUpdate <https://learn.microsoft.com/python/api/azure-mgmt-resource/azure.mgmt.resource.resources.v2022_09_01.operations.resourcegroupsoperations?view=azure-python#azure-mgmt-resource-resources-v2022-09-01-operations-resourcegroupsoperations-create-or-update>`_.

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription ID.
    resource_group : str
        The name of the Azure resource group to be created.
    region : str
        The name of the region in which the resource group will be created.
    token_provider : TokenProvider, default=None
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.
    """

    from azure.mgmt.resource import ResourceManagementClient

    if token_provider is None:
        token_provider = ServicePrincipalTokenProvider.from_azure_key_vault(
            key_vault_uri=kwargs["key_vault_uri"],
            key_vault_tenant_id=kwargs["key_vault_tenant_id"],
            key_vault_client_id=kwargs["key_vault_client_id"],
            key_vault_client_secret=kwargs["key_vault_client_secret"],
        )
        print(
            f"{icons.info} Please use the 'token_provider' parameter instead of the key vault parameters within this function as the key vault parameters have been deprecated."
        )

    resource_client = ResourceManagementClient(
        token_provider.credential, azure_subscription_id
    )

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


def list_skus_for_capacity(
    capacity: str,
    azure_subscription_id: str,
    resource_group: str,
    token_provider: TokenProvider,
) -> pd.DataFrame:
    """
    Lists eligible SKUs for a Microsoft Fabric resource.

    This is a wrapper function for the following API: `Fabric Capacities - List Skus For Capacity <https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/list-skus-for-capacity?view=rest-microsoftfabric-2023-11-01>`_.

    Parameters
    ----------
    capacity : str
        The capacity name.
    azure_subscription_id : str
        The Azure subscription ID.
    resource_group : str
        The name of the resource group.
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of eligible SKUs for a Microsoft Fabric resource.
    """

    df = pd.DataFrame(columns=["Resource Type", "Sku", "Sku Tier"])
    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity}/skus?api-version=2023-11-01"
    headers = _get_headers(token_provider=token_provider, audience="azure")

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("value", []):
        sku = v.get("sku", {})
        new_data = {
            "Resource Type": v.get("resourceType"),
            "Sku": sku.get("name"),
            "Sku Tier": sku.get("tier"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_skus(
    azure_subscription_id: str,
    token_provider: TokenProvider,
) -> pd.DataFrame:
    """
    Lists eligible SKUs for Microsoft Fabric resource provider.

    This is a wrapper function for the following API: `Fabric Capacities - List Skus For Capacity <https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/list-skus?view=rest-microsoftfabric-2023-11-01>`_.

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription ID.
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of eligible SKUs for Microsoft Fabric resource provider.
    """

    df = pd.DataFrame(columns=["Sku", "Locations"])
    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/providers/Microsoft.Fabric/skus?api-version=2023-11-01"
    headers = _get_headers(token_provider=token_provider, audience="azure")

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("value", []):
        new_data = {
            "Sku": v.get("name"),
            "Locations": v.get("locations", []),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_subscriptions(token_provider: TokenProvider) -> pd.DataFrame:
    """
    Gets all subscriptions for a tenant.

    This is a wrapper function for the following API: `Subscriptions - List <https://learn.microsoft.com/rest/api/resources/subscriptions/list?view=rest-resources-2022-12-01>`_.

    Parameters
    ----------
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of all subscriptions for a tenant.
    """

    df = pd.DataFrame(
        columns=[
            "Subscription Id",
            "Subscription Name",
            "Tenant Id",
            "State",
            "Location Placement Id",
            "Quota Id",
            "Spending Limit",
            "Authorization Source",
            "Managed By Tenants",
            "Tags",
        ]
    )
    url = "https://management.azure.com/subscriptions?api-version=2022-12-01"
    headers = _get_headers(token_provider=token_provider, audience="azure")

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("value", []):
        policy = v.get("subscriptionPolicies", {})
        tenants = v.get("managedByTenants")
        new_data = {
            "Subscription Id": v.get("subscriptionId"),
            "Subscription Name": v.get("displayName"),
            "Tenant Id": v.get("tenantId"),
            "State": v.get("state"),
            "Location Placement Id": policy.get("locationPlacementId"),
            "Quota Id": policy.get("quotaId"),
            "Spending Limit": policy.get("spendingLimit"),
            "Authorization Source": v.get("authorizationSource"),
            "Managed by Tenants": tenants if tenants is not None else [],
            "Tags": v.get("tags", {}),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def get_subscription(
    azure_subscription_id: str, token_provider: TokenProvider
) -> pd.DataFrame:
    """
    Gets details about a specified subscription.

    This is a wrapper function for the following API: `Subscriptions - Get <https://learn.microsoft.com/rest/api/resources/subscriptions/get?view=rest-resources-2022-12-01>`_.

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription ID.
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing details of a specific subscription.
    """

    df = pd.DataFrame(
        columns=[
            "Subscription Id",
            "Subscription Name",
            "Tenant Id",
            "State",
            "Location Placement Id",
            "Quota Id",
            "Spending Limit",
            "Authorization Source",
            "Managed By Tenants",
            "Tags",
        ]
    )
    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}?api-version=2022-12-01"
    headers = _get_headers(token_provider=token_provider, audience="azure")

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise FabricHTTPException(response)

    v = response.json()
    policy = v.get("subscriptionPolicies", {})
    tenants = v.get("managedByTenants")
    new_data = {
        "Subscription Id": v.get("subscriptionId"),
        "Subscription Name": v.get("displayName"),
        "Tenant Id": v.get("tenantId"),
        "State": v.get("state"),
        "Location Placement Id": policy.get("locationPlacementId"),
        "Quota Id": policy.get("quotaId"),
        "Spending Limit": policy.get("spendingLimit"),
        "Authorization Source": v.get("authorizationSource"),
        "Managed by Tenants": tenants if tenants is not None else [],
        "Tags": v.get("tags", {}),
    }

    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def _resolve_subscription_name_and_id(
    azure_subscription: str | UUID, token_provider: TokenProvider
) -> Tuple[str, UUID]:

    if _is_valid_uuid(azure_subscription):
        subscription_id = azure_subscription
        df = get_subscription(
            azure_subscription_id=subscription_id, token_provider=token_provider
        )
        if df.empty:
            raise ValueError(f"{icons.red_dot} The subscription ID does not exist.")
        subscription_name = df["Subscription Name"].iloc[0]
    else:
        subscription_name = azure_subscription
        df = list_subscriptions()
        df_filt = df[df["Subscription Name"] == subscription_name]
        if df_filt.empty:
            raise ValueError(f"{icons.red_dot} The subscription name does not exist.")
        subscription_id = df_filt["Subscription Id"].iloc[0]

    return subscription_name, subscription_id


def list_tenants(token_provider: TokenProvider) -> pd.DataFrame:
    """
    Gets the tenants for your account.

    This is a wrapper function for the following API: `Tenants - List <https://learn.microsoft.com/rest/api/resources/tenants/list?view=rest-resources-2022-12-01>`_.

    Parameters
    ----------
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of all tenants for your account.
    """

    df = pd.DataFrame(
        columns=[
            "Tenant Id",
            "Tenant Name",
            "Country Code",
            "Domains",
            "Tenant Category",
            "Default Domain",
            "Tenant Type",
            "Tenant Branding Logo Url",
        ]
    )
    url = "https://management.azure.com/tenants?api-version=2022-12-01"
    headers = _get_headers(token_provider=token_provider, audience="azure")

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("value", []):
        d = v.get("domains")
        new_data = {
            "Tenant Id": v.get("tenantId"),
            "Tenant Name": v.get("displayName"),
            "Country Code": v.get("countryCode"),
            "Domains": d if d is not None else [],
            "Tenant Category": v.get("tenantCategory"),
            "Default Domain": v.get("defaultDomain"),
            "Tenant Type": v.get("tenantType"),
            "Tenant Branding Logo Url": v.get("tenantBrandingLogoUrl"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def create_or_update_resource_group(
    azure_subscription_id: str,
    resource_group: str,
    region: str,
    token_provider: TokenProvider,
):
    """
    Creates or updates a resource group.

    This is a wrapper function for the following API: `Resource Groups - Create Or Update <https://learn.microsoft.com/rest/api/resources/resource-groups/create-or-update>`_.

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription Id.
    resource_group : str
        The name of the resource group.
    region : str
        The name of the region.
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.
    """

    headers = _get_headers(token_provider, audience="azure")
    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourcegroups/{resource_group}?api-version=2021-04-01"

    payload = {
        "location": region,
    }

    response = requests.put(url, headers=headers, json=payload)
    if response.status_code not in [200, 201]:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{resource_group}' resource group has been created/updated."
    )


def create_storage_account(
    azure_subscription_id: str,
    resource_group: str,
    storage_account: str,
    region: str,
    token_provider: TokenProvider,
):
    """
    Asynchronously creates a new storage account with the specified parameters. If an account is already created and a subsequent create request is issued with different properties, the account properties will be updated. If an account is already created and a subsequent create or update request is issued with the exact same set of properties, the request will succeed.

    This is a wrapper function for the following API: `Storage Accounts - Create <https://learn.microsoft.com/rest/api/storagerp/storage-accounts/create`_.

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription Id.
    resource_group : str
        The name of the resource group.
    storage_account : str
        The name of the storage account to be created.
    region : str
        The name of the region.
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.
    """

    headers = _get_headers(token_provider, audience="azure")
    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Storage/storageAccounts/{storage_account}?api-version=2018-02-01"

    payload = {
        "sku": {"name": "Standard_GRS"},
        "kind": "StorageV2",
        "location": region,
    }

    response = requests.put(url, headers=headers, json=payload)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{storage_account}' storage account has been created."
    )


def list_storage_accounts(
    azure_subscription_id: str,
    token_provider: TokenProvider,
    resource_group: Optional[str] = None,
) -> pd.DataFrame:
    """
    Lists all the storage accounts available under the subscription (or resource group). Note that storage keys are not returned; use the ListKeys operation for this.

    This is a wrapper function for the following APIs: `Storage Accounts - List <https://learn.microsoft.com/rest/api/storagerp/storage-accounts/list>`_, `Storage Accounts - List By Resource Group <https://learn.microsoft.com/rest/api/storagerp/storage-accounts/list-by-resource-group>`_.

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription Id.
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.
    resource_group : str, default=None
        If set to None, retrieves all storage accounts for the subscription. If not None, shows the storage accounts within that resource group.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of all storage accounts within the subscription (or resource group).
    """

    headers = _get_headers(token_provider, audience="azure")

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}"

    if resource_group is not None:
        url += f"/resourceGroups/{resource_group}"

    url += "/providers/Microsoft.Storage/storageAccounts?api-version=2023-05-01"

    df = pd.DataFrame(
        columns=[
            "Storage Account Id",
            "Storage Account Name",
            "Kind",
            "Location",
            "Sku Name",
            "Sku Tier",
            "Is HNS Enabled",
            "Creation Time",
            "Web Endpoint",
            "DFS Endpoint",
            "Blob Endpoint",
            "File Endpoint",
            "Queue Endpoint",
            "Table Endpoint",
            "Primary Location",
            "Provisioning State",
            "Secondary Location",
            "Status of Primary",
            "Status of Secondary",
            "Supports HTTPS Traffic Only",
            "Tags",
        ]
    )
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("value", []):
        p = v.get("properties", {})
        new_data = {
            "Storage Account Id": v.get("id"),
            "Storage Account Name": v.get("name"),
            "Kind": v.get("kind"),
            "Location": v.get("location"),
            "Sku Name": v.get("sku", {}).get("name"),
            "Sku Tier": v.get("sku", {}).get("tier"),
            "Is HNS Enabled": p.get("isHnsEnabled"),
            "Creation Time": p.get("creationTime"),
            "Web Endpoint": p.get("primaryEndpoints", {}).get("web"),
            "DFS Endpoint": p.get("primaryEndpoints", {}).get("dfs"),
            "Blob Endpoint": p.get("primaryEndpoints", {}).get("blob"),
            "File Endpoint": p.get("primaryEndpoints", {}).get("file"),
            "Queue Endpoint": p.get("primaryEndpoints", {}).get("queue"),
            "Table Endpoint": p.get("primaryEndpoints", {}).get("table"),
            "Primary Location": p.get("primaryLocation"),
            "Provisioning State": p.get("provisioningState"),
            "Secondary Location": p.get("secondaryLocation"),
            "Status of Primary": p.get("statusOfPrimary"),
            "Status of Secondary": p.get("statusOfSecondary"),
            "Supports HTTPS Traffic Only": p.get("supportsHttpsTrafficOnly"),
            "Tags": v.get("tags"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    bool_cols = ["Is HNS Enabled", "Supports HTTPS Traffic Only"]
    df[bool_cols] = df[bool_cols].astype(bool)

    return df


def check_resource_group_existence(
    azure_subscription_id: str, resource_group: str, token_provider: TokenProvider
) -> bool:
    """
    Checks whether a resource group exists.

    This is a wrapper function for the following API: `Resource Groups - Check Existence <https://learn.microsoft.com/rest/api/resources/resource-groups/check-existence>`_.

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription Id.
    resource_group : str
        The name of the resource group.
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.

    Returns
    -------
    bool
        True/False indicating if the resource group exists or not.
    """

    headers = _get_headers(token_provider, audience="azure")
    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}?api-version=2021-04-01"

    response = requests.get(url, headers=headers)

    if response.status_code not in [200, 204, 404]:
        raise FabricHTTPException(response)

    if response.status_code == 200:
        return True
    elif response.status_code in [204, 404]:
        return False


def list_resource_groups(
    azure_subscription_id: str,
    token_provider: TokenProvider,
    filter: Optional[str] = None,
    top: Optional[int] = None,
) -> pd.DataFrame:
    """
    Lists all resource groups within a subscription.

    This is a wrapper function for the following API: `Resource Groups - List <https://learn.microsoft.com/rest/api/resources/resource-groups/list>`_.

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription Id.
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.
    filter : str, default=None
        The filter to apply to the operation. Example: filter="tagname eq 'tagvalue'".
    top : int, default=None
        The number of results to return. If not specified, returns all results.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of all resource groups within the subscription.
    """

    headers = _get_headers(token_provider, audience="azure")
    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups?"

    if filter is not None:
        url += f"$filter={filter}&"
    if top is not None:
        url += f"$top={top}&"

    url += "api-version=2021-04-01"

    df = pd.DataFrame(columns=["Resource Group Name", "Location", "Tags"])
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("value", []):
        new_data = {
            "Resource Group Id": v.get("id"),
            "Resource Group Name": v.get("name"),
            "Location": v.get("location"),
            "Managed By": v.get("managedBy"),
            "Tags": v.get("tags"),
            "Type": v.get("type"),
            "Provisioning State": v.get("properties", {}).get("provisioningState"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def get_resource_group(
    azure_subscription_id: str, resource_group: str, token_provider: TokenProvider
) -> pd.DataFrame:
    """
    Gets details about a specified resource group.

    This is a wrapper function for the following API: `Resource Groups - Get <https://learn.microsoft.com/rest/api/resources/resource-groups/get>`_.

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription Id.
    resource_group : str
        The name of the resource group.
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing details of a specific resource group.
    """

    headers = _get_headers(token_provider, audience="azure")
    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}?api-version=2021-04-01"

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    v = response.json()
    new_data = {
        "Resource Group Id": v.get("id"),
        "Resource Group Name": v.get("name"),
        "Location": v.get("location"),
        "Managed By": v.get("managedBy"),
        "Tags": v.get("tags"),
        "Type": v.get("type"),
        "Provisioning State": v.get("properties", {}).get("provisioningState"),
    }

    return pd.DataFrame(new_data, index=[0])
