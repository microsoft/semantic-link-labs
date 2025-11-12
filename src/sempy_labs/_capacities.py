from typing import Optional, List, Tuple
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
import requests
import pandas as pd
from sempy_labs._authentication import (
    _get_headers,
    ServicePrincipalTokenProvider,
)
from uuid import UUID
from sempy_labs._helper_functions import (
    _is_valid_uuid,
    _update_dataframe_datatypes,
    _base_api,
    _create_dataframe,
)
import sempy_labs._authentication as auth


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
    tags: Optional[dict] = None,
    **kwargs,
):
    """
    This function creates a new Fabric capacity within an Azure subscription.

    This is a wrapper function for the following API: `Fabric Capacities - Create Or Update <https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/create-or-update>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    token_provider = auth.token_provider.get()
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
def list_vcores() -> pd.DataFrame:

    columns = {
        "Total Purchased Cores": "int",
        "Available Cores": "int",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(request="capacities/vcores")
    response_json = response.json()
    new_data = {
        "Total Purchased Cores": response_json.get("totalPurchasedCores"),
        "Available Cores": response_json.get("availableCores"),
    }

    df = pd.DataFrame([new_data], columns=columns.keys())
    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def get_capacity_resource_governance(capacity_name: str):

    dfC = list_capacities()
    dfC_filt = dfC[dfC["Display Name"] == capacity_name]
    capacity_id = dfC_filt["Id"].iloc[0].upper()

    response = _base_api(request=f"capacities/{capacity_id}/resourceGovernance")

    return response.json().get("workloadSettings", {})


@log
def suspend_fabric_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    resource_group: str,
):
    """
    This function suspends a Fabric capacity.

    This is a wrapper function for the following API: `Fabric Capacities - Suspend <https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/suspend?view=rest-microsoftfabric-2023-11-01>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity_name : str
        Name of the Fabric capacity.
    azure_subscription_id : str
        The Azure subscription ID.
    resource_group : str
        The name of the Azure resource group.
    """

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/suspend?api-version={icons.azure_api_version}"

    _base_api(request=url, client="azure", method="post", status_codes=202)

    print(f"{icons.green_dot} The '{capacity_name}' capacity has been suspended.")


@log
def resume_fabric_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    resource_group: str,
):
    """
    This function resumes a Fabric capacity.

    This is a wrapper function for the following API: `Fabric Capacities - Resume <https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/resume?view=rest-microsoftfabric-2023-11-01>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity_name : str
        Name of the Fabric capacity.
    azure_subscription_id : str
        The Azure subscription ID.
    resource_group : str
        The name of the Azure resource group.
    """

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/resume?api-version={icons.azure_api_version}"

    _base_api(request=url, client="azure", method="post", status_codes=202)

    print(f"{icons.green_dot} The '{capacity_name}' capacity has been resumed.")


@log
def delete_embedded_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    resource_group: str,
):
    """
    This function deletes a Power BI Embedded capacity.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity_name : str
        Name of the Fabric capacity.
    azure_subscription_id : str
        The Azure subscription ID.
    resource_group : str
        The name of the Azure resource group.
    """

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.PowerBIDedicated/capacities/{capacity_name}?api-version={icons.azure_api_version}"

    _base_api(request=url, client="azure", method="delete", status_codes=[200, 202])

    print(f"{icons.green_dot} The '{capacity_name}' capacity has been deleted.")


@log
def delete_premium_capacity(capacity: str | UUID, **kwargs):
    """
    This function deletes a Power BI Premium capacity.

    Parameters
    ----------
    capacity : str | uuid.UUID
        Name or ID of the Fabric capacity.
    """
    from sempy_labs._helper_functions import resolve_capacity_id

    if "capacity_name" in kwargs:
        capacity = kwargs["capacity_name"]
        print(
            f"{icons.warning} The 'capacity_name' parameter is deprecated. Please use 'capacity' instead."
        )

    capacity_id = resolve_capacity_id(capacity=capacity).upper()

    _base_api(request=f"capacities/{capacity_id}", method="delete", status_codes=204)

    print(f"{icons.green_dot} The '{capacity}' capacity has been deleted.")


@log
def delete_fabric_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    resource_group: str,
):
    """
    This function deletes a Fabric capacity.

    This is a wrapper function for the following API: `Fabric Capacities - Delete <https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/delete?view=rest-microsoftfabric-2023-11-01>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity_name : str
        Name of the Fabric capacity.
    azure_subscription_id : str
        The Azure subscription ID.
    resource_group : str
        The name of the Azure resource group.
    """

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version={icons.azure_api_version}"

    _base_api(request=url, client="azure", method="delete", status_codes=202)

    print(f"{icons.green_dot} The '{capacity_name}' capacity has been deleted.")


@log
def update_fabric_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    resource_group: str,
    sku: Optional[str] = None,
    admin_members: Optional[str | List[str]] = None,
    tags: Optional[dict] = None,
):
    """
    This function updates a Fabric capacity's properties.

    This is a wrapper function for the following API: `Fabric Capacities - Update <https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/update?view=rest-microsoftfabric-2023-11-01>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity_name : str
        Name of the Fabric capacity.
    azure_subscription_id : str
        The Azure subscription ID.
    resource_group : str
        The name of the Azure resource group.
    sku : str, default=None
         The `sku size <https://azure.microsoft.com/pricing/details/microsoft-fabric/>`_ of the Fabric capacity.
    admin_members : str | List[str], default=None
        The email address(es) of the admin(s) of the Fabric capacity.
    tags : dict, default=None
        Tag(s) to add to the capacity. Example: {'tagName': 'tagValue'}.
    """

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version={icons.azure_api_version}"

    get_response = _base_api(request=url, client="azure")

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
    _base_api(
        request=url, client="azure", method="patch", payload=payload, status_codes=202
    )

    print(
        f"{icons.green_dot} The '{capacity_name}' capacity has been updated accordingly."
    )


@log
def check_fabric_capacity_name_availablility(
    capacity_name: str,
    azure_subscription_id: str,
    region: str,
    **kwargs,
) -> bool:
    """
    This function updates a Fabric capacity's properties.

    This is a wrapper function for the following API: `Fabric Capacities - Check Name Availability <https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/check-name-availability?view=rest-microsoftfabric-2023-11-01>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity_name : str
        Name of the Fabric capacity.
    azure_subscription_id : str
        The Azure subscription ID.
    region : str
        The region name.

    Returns
    -------
    bool
        An indication as to whether the Fabric capacity name is available or not.
    """

    payload = {"name": capacity_name, "type": "Microsoft.Fabric/capacities"}

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/providers/Microsoft.Fabric/locations/{region}/checkNameAvailability?api-version={icons.azure_api_version}"

    response = _base_api(
        request=url, client="azure", method="post", payload=payload, status_codes=202
    )

    return bool(response.json().get("nameAvailable"))


@log
def create_resource_group(
    azure_subscription_id: str,
    resource_group: str,
    region: str,
    **kwargs,
):
    """
    This function creates a resource group in a region within an Azure subscription.

    This is a wrapper function for the following API: `Resource Groups - Create Or Update <https://learn.microsoft.com/rest/api/resources/resource-groups/create-or-update>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription ID.
    resource_group : str
        The name of the Azure resource group to be created.
    region : str
        The name of the region in which the resource group will be created.
    """

    if check_resource_group_existence(
        azure_subscription_id=azure_subscription_id,
        resource_group=resource_group,
    ):
        print(
            f"{icons.info} The '{resource_group}' resource group already exists in the '{region}' region within the '{azure_subscription_id}' Azure subscription."
        )
        return

    create_or_update_resource_group(
        azure_subscription_id=azure_subscription_id,
        resource_group=resource_group,
        region=region,
    )


@log
def list_skus_for_capacity(
    capacity: str,
    azure_subscription_id: str,
    resource_group: str,
) -> pd.DataFrame:
    """
    Lists eligible SKUs for a Microsoft Fabric resource.

    This is a wrapper function for the following API: `Fabric Capacities - List Skus For Capacity <https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/list-skus-for-capacity?view=rest-microsoftfabric-2023-11-01>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str
        The capacity name.
    azure_subscription_id : str
        The Azure subscription ID.
    resource_group : str
        The name of the resource group.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of eligible SKUs for a Microsoft Fabric resource.
    """

    columns = {
        "Resource Type": "string",
        "Sku": "string",
        "Sku Tier": "string",
    }
    df = _create_dataframe(columns=columns)

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity}/skus?api-version=2023-11-01"
    response = _base_api(request=url, client="azure")

    rows = []
    for v in response.json().get("value", []):
        sku = v.get("sku", {})
        rows.append(
            {
                "Resource Type": v.get("resourceType"),
                "Sku": sku.get("name"),
                "Sku Tier": sku.get("tier"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df


@log
def list_skus(
    azure_subscription_id: str,
) -> pd.DataFrame:
    """
    Lists eligible SKUs for Microsoft Fabric resource provider.

    This is a wrapper function for the following API: `Fabric Capacities - List Skus For Capacity <https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/list-skus?view=rest-microsoftfabric-2023-11-01>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of eligible SKUs for Microsoft Fabric resource provider.
    """

    columns = {
        "Sku": "string",
        "Locations": "str",
    }
    df = _create_dataframe(columns=columns)

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/providers/Microsoft.Fabric/skus?api-version=2023-11-01"
    response = _base_api(request=url, client="azure")

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
                "Sku": v.get("name"),
                "Locations": v.get("locations", []),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df


@log
def list_subscriptions() -> pd.DataFrame:
    """
    Gets all subscriptions for a tenant.

    This is a wrapper function for the following API: `Subscriptions - List <https://learn.microsoft.com/rest/api/resources/subscriptions/list?view=rest-resources-2022-12-01>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of all subscriptions for a tenant.
    """

    columns = {
        "Subscription Id": "string",
        "Subscription Name": "string",
        "Tenant Id": "string",
        "State": "string",
        "Location Placement Id": "string",
        "Quota Id": "string",
        "Spending Limit": "string",
        "Authorization Source": "string",
        "Managed by Tenants": "string",
        "Tags": "string",
    }
    df = _create_dataframe(columns=columns)

    url = "https://management.azure.com/subscriptions?api-version=2022-12-01"
    response = _base_api(request=url, client="azure")

    rows = []
    for v in response.json().get("value", []):
        policy = v.get("subscriptionPolicies", {})
        tenants = v.get("managedByTenants")
        rows.append(
            {
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
        )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df


@log
def get_subscription(azure_subscription_id: str) -> pd.DataFrame:
    """
    Gets details about a specified subscription.

    This is a wrapper function for the following API: `Subscriptions - Get <https://learn.microsoft.com/rest/api/resources/subscriptions/get?view=rest-resources-2022-12-01>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing details of a specific subscription.
    """

    columns = {
        "Subscription Id": "string",
        "Subscription Name": "string",
        "Tenant Id": "string",
        "State": "string",
        "Location Placement Id": "string",
        "Quota Id": "string",
        "Spending Limit": "string",
        "Authorization Source": "string",
        "Managed by Tenants": "string",
        "Tags": "string",
    }
    df = _create_dataframe(columns=columns)

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}?api-version=2022-12-01"

    response = _base_api(request=url, client="azure")
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

    df = pd.DataFrame([new_data], columns=columns.keys())

    return df


def _resolve_subscription_name_and_id(
    azure_subscription: str | UUID,
) -> Tuple[str, UUID]:

    if _is_valid_uuid(azure_subscription):
        subscription_id = azure_subscription
        df = get_subscription(azure_subscription_id=subscription_id)
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


@log
def list_tenants() -> pd.DataFrame:
    """
    Gets the tenants for your account.

    This is a wrapper function for the following API: `Tenants - List <https://learn.microsoft.com/rest/api/resources/tenants/list?view=rest-resources-2022-12-01>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of all tenants for your account.
    """

    columns = {
        "Tenant Id": "string",
        "Tenant Name": "string",
        "Country Code": "string",
        "Domains": "string",
        "Tenant Category": "string",
        "Default Domain": "string",
        "Tenant Type": "string",
        "Tenant Branding Logo Url": "string",
    }
    df = _create_dataframe(columns=columns)

    url = "https://management.azure.com/tenants?api-version=2022-12-01"

    response = _base_api(request=url, client="azure")

    rows = []
    for v in response.json().get("value", []):
        d = v.get("domains")
        rows.append(
            {
                "Tenant Id": v.get("tenantId"),
                "Tenant Name": v.get("displayName"),
                "Country Code": v.get("countryCode"),
                "Domains": d if d is not None else "",
                "Tenant Category": v.get("tenantCategory"),
                "Default Domain": v.get("defaultDomain"),
                "Tenant Type": v.get("tenantType"),
                "Tenant Branding Logo Url": v.get("tenantBrandingLogoUrl"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df


@log
def create_or_update_resource_group(
    azure_subscription_id: str,
    resource_group: str,
    region: str,
):
    """
    Creates or updates a resource group.

    This is a wrapper function for the following API: `Resource Groups - Create Or Update <https://learn.microsoft.com/rest/api/resources/resource-groups/create-or-update>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription Id.
    resource_group : str
        The name of the resource group.
    region : str
        The name of the region.
    """

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourcegroups/{resource_group}?api-version=2021-04-01"

    payload = {
        "location": region,
    }

    _base_api(
        request=url,
        client="azure",
        method="put",
        payload=payload,
        status_codes=[200, 201],
    )

    print(
        f"{icons.green_dot} The '{resource_group}' resource group has been created/updated."
    )


@log
def create_storage_account(
    azure_subscription_id: str,
    resource_group: str,
    storage_account: str,
    region: str,
):
    """
    Asynchronously creates a new storage account with the specified parameters. If an account is already created and a subsequent create request is issued with different properties, the account properties will be updated. If an account is already created and a subsequent create or update request is issued with the exact same set of properties, the request will succeed.

    This is a wrapper function for the following API: `Storage Accounts - Create <https://learn.microsoft.com/rest/api/storagerp/storage-accounts/create>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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
    """

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Storage/storageAccounts/{storage_account}?api-version=2018-02-01"

    payload = {
        "sku": {"name": "Standard_GRS"},
        "kind": "StorageV2",
        "location": region,
    }

    _base_api(request=url, client="azure", method="put", payload=payload)

    print(
        f"{icons.green_dot} The '{storage_account}' storage account has been created."
    )


@log
def list_storage_accounts(
    azure_subscription_id: str,
    resource_group: Optional[str] = None,
) -> pd.DataFrame:
    """
    Lists all the storage accounts available under the subscription (or resource group). Note that storage keys are not returned; use the ListKeys operation for this.

    This is a wrapper function for the following APIs: `Storage Accounts - List <https://learn.microsoft.com/rest/api/storagerp/storage-accounts/list>`_, `Storage Accounts - List By Resource Group <https://learn.microsoft.com/rest/api/storagerp/storage-accounts/list-by-resource-group>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription Id.
    resource_group : str, default=None
        If set to None, retrieves all storage accounts for the subscription. If not None, shows the storage accounts within that resource group.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of all storage accounts within the subscription (or resource group).
    """

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}"

    if resource_group is not None:
        url += f"/resourceGroups/{resource_group}"

    url += "/providers/Microsoft.Storage/storageAccounts?api-version=2023-05-01"

    columns = {
        "Storage Account Id": "string",
        "Storage Account Name": "string",
        "Kind": "string",
        "Location": "string",
        "Sku Name": "string",
        "Sku Tier": "string",
        "Is HNS Enabled": "bool",
        "Creation Time": "datetime",
        "Web Endpoint": "string",
        "DFS Endpoint": "string",
        "Blob Endpoint": "string",
        "File Endpoint": "string",
        "Queue Endpoint": "string",
        "Table Endpoint": "string",
        "Primary Location": "string",
        "Provisioning State": "string",
        "Secondary Location": "string",
        "Status of Primary": "string",
        "Status of Secondary": "string",
        "Supports HTTPS Traffic Only": "bool",
        "Tags": "string",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(request=url, client="azure")

    rows = []
    for v in response.json().get("value", []):
        p = v.get("properties", {})
        rows.append(
            {
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
        )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def check_resource_group_existence(
    azure_subscription_id: str, resource_group: str
) -> bool:
    """
    Checks whether a resource group exists.

    This is a wrapper function for the following API: `Resource Groups - Check Existence <https://learn.microsoft.com/rest/api/resources/resource-groups/check-existence>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription Id.
    resource_group : str
        The name of the resource group.

    Returns
    -------
    bool
        True/False indicating if the resource group exists or not.
    """

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}?api-version=2021-04-01"

    response = _base_api(request=url, client="azure", status_codes=[200, 204, 404])

    if response.status_code == 200:
        return True
    elif response.status_code in [204, 404]:
        return False


@log
def list_resource_groups(
    azure_subscription_id: str,
    filter: Optional[str] = None,
    top: Optional[int] = None,
) -> pd.DataFrame:
    """
    Lists all resource groups within a subscription.

    This is a wrapper function for the following API: `Resource Groups - List <https://learn.microsoft.com/rest/api/resources/resource-groups/list>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription Id.
    filter : str, default=None
        The filter to apply to the operation. Example: filter="tagname eq 'tagvalue'".
    top : int, default=None
        The number of results to return. If not specified, returns all results.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of all resource groups within the subscription.
    """

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups?"

    if filter is not None:
        url += f"$filter={filter}&"
    if top is not None:
        url += f"$top={top}&"

    url += "api-version=2021-04-01"

    columns = {
        "Resource Group Name": "string",
        "Location": "string",
        "Tags": "string",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(request=url, client="azure")

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
                "Resource Group Id": v.get("id"),
                "Resource Group Name": v.get("name"),
                "Location": v.get("location"),
                "Managed By": v.get("managedBy"),
                "Tags": v.get("tags"),
                "Type": v.get("type"),
                "Provisioning State": v.get("properties", {}).get("provisioningState"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df


@log
def get_resource_group(azure_subscription_id: str, resource_group: str) -> pd.DataFrame:
    """
    Gets details about a specified resource group.

    This is a wrapper function for the following API: `Resource Groups - Get <https://learn.microsoft.com/rest/api/resources/resource-groups/get>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription Id.
    resource_group : str
        The name of the resource group.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing details of a specific resource group.
    """

    url = f"https://management.azure.com/subscriptions/{azure_subscription_id}/resourceGroups/{resource_group}?api-version=2021-04-01"
    response = _base_api(request=url, client="azure")

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


def list_capacities() -> pd.DataFrame:
    """
    Shows the capacities and their properties.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the capacities and their properties
    """

    columns = {
        "Id": "string",
        "Display Name": "string",
        "Sku": "string",
        "Region": "string",
        "State": "string",
        "Admins": "string",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(request="/v1.0/myorg/capacities", client="fabric_sp")

    rows = []
    for i in response.json().get("value", []):
        rows.append(
            {
                "Id": i.get("id").lower(),
                "Display Name": i.get("displayName"),
                "Sku": i.get("sku"),
                "Region": i.get("region"),
                "State": i.get("state"),
                "Admins": [i.get("admins", [])],
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df
