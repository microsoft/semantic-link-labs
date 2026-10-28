from typing import Optional, List, Literal
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
    _base_api,
    _create_dataframe,
    resolve_capacity_id,
)
import sempy_labs._authentication as auth
from sempy_labs._capacities import (
    create_or_update_resource_group, 
    get_resource_group,
    list_resource_groups,
)


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
    admin_members : str | typing.List[str]
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
    admin_members : str | typing.List[str], default=None
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


@log
def get_overage_configuration(
    capacity: Optional[str | UUID] = None,
) -> dict:
    """
    Gets the overage configuration for the specified capacity.

    Capacity overage configuration is currently in preview. The caller must have admin permissions on the capacity.

    This is a wrapper function for the following API: `Capacities - Get Overage Configuration <https://learn.microsoft.com/rest/api/fabric/core/capacities/get-overage-configuration>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.

    Returns
    -------
    dict
        The overage configuration of the capacity, containing the 'state' and 'thresholdCapacityUnitHours' keys.
    """

    capacity_id = resolve_capacity_id(capacity)

    response = _base_api(
        request=f"/v1/capacities/{capacity_id}/overageConfiguration",
        client="fabric_sp",
    )

    return response.json()


@log
def update_overage_configuration(
    capacity: Optional[str | UUID] = None,
    state: Optional[Literal["Enabled", "Disabled"]] = None,
    threshold_capacity_unit_hours: Optional[int] = None,
) -> dict:
    """
    Updates the overage configuration for the specified capacity.

    Capacity overage configuration is currently in preview. The caller must have admin permissions on the capacity.

    This is a wrapper function for the following API: `Capacities - Update Overage Configuration <https://learn.microsoft.com/rest/api/fabric/core/capacities/update-overage-configuration>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.
    state : typing.Literal["Enabled", "Disabled"], default=None
        The overage state. When set to 'Disabled', the threshold is automatically reset to 0.
        When set to 'Enabled', a threshold greater than 0 must be supplied (either in the same request or already configured).
        Defaults to None which leaves the state unchanged.
    threshold_capacity_unit_hours : int, default=None
        The capacity unit hours threshold for overage. Valid range is 0 to 240000.
        Setting this to 0 automatically disables overage.
        Defaults to None which leaves the threshold unchanged.

    Returns
    -------
    dict
        The updated overage configuration of the capacity, containing the 'state' and 'thresholdCapacityUnitHours' keys.
    """

    if state is None and threshold_capacity_unit_hours is None:
        raise ValueError(
            f"{icons.red_dot} At least one of the 'state' or 'threshold_capacity_unit_hours' parameters must be provided."
        )

    payload = {}
    if state is not None:
        valid_states = ["Enabled", "Disabled"]
        state = state.capitalize()
        if state not in valid_states:
            raise ValueError(
                f"{icons.red_dot} Invalid 'state' value. Valid values: {valid_states}."
            )
        payload["state"] = state
    if threshold_capacity_unit_hours is not None:
        if not 0 <= threshold_capacity_unit_hours <= 240000:
            raise ValueError(
                f"{icons.red_dot} The 'threshold_capacity_unit_hours' parameter must be between 0 and 240000."
            )
        payload["thresholdCapacityUnitHours"] = threshold_capacity_unit_hours

    capacity_id = resolve_capacity_id(capacity)

    response = _base_api(
        request=f"/v1/capacities/{capacity_id}/overageConfiguration",
        method="patch",
        payload=payload,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The overage configuration of the '{capacity if capacity is not None else capacity_id}' capacity has been updated."
    )

    return response.json()
