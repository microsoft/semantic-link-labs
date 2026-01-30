from sempy_labs._helper_functions import (
    _update_dataframe_datatypes,
    _base_api,
    _create_dataframe,
)
from sempy._utils._log import log
import pandas as pd
from uuid import UUID
from sempy_labs.admin._capacities import _resolve_capacity_name_and_id
import sempy_labs._icons as icons
from typing import Optional, List


@log
def list_tenant_settings() -> pd.DataFrame:
    """
    Lists all tenant settings.

    This is a wrapper function for the following API: `Tenants - List Tenant Settings <https://learn.microsoft.com/rest/api/fabric/admin/tenants/list-tenant-settings>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the tenant settings.
    """

    columns = {
        "Setting Name": "string",
        "Title": "string",
        "Enabled": "bool",
        "Can Specify Security Groups": "bool",
        "Tenant Setting Group": "string",
        "Enabled Security Groups": "list",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(request="/v1/admin/tenantsettings", client="fabric_sp")

    rows = []
    for i in response.json().get("value", []):
        rows.append(
            {
                "Setting Name": i.get("settingName"),
                "Title": i.get("title"),
                "Enabled": i.get("enabled"),
                "Can Specify Security Groups": i.get("canSpecifySecurityGroups"),
                "Tenant Setting Group": i.get("tenantSettingGroup"),
                "Enabled Security Groups": [i.get("enabledSecurityGroups", [])],
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def list_capacity_tenant_settings_overrides(
    capacity: Optional[str | UUID] = None,
    return_dataframe: bool = True,
) -> pd.DataFrame | dict:
    """
    Returns list of tenant setting overrides that override at the capacities.

    This is a wrapper function for the following API: `Tenants - List Capacities Tenant Settings Overrides <https://learn.microsoft.com/rest/api/fabric/admin/tenants/list-capacities-tenant-settings-overrides>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to showing all capacities.
    return_dataframe : bool, default=True
        If True, returns a dataframe. If False, returns a dictionary.

    Returns
    -------
    pandas.DataFrame | dict
        A pandas dataframe showing a list of tenant setting overrides that override at the capacities.
    """

    columns = {
        "Capacity Id": "string",
        "Setting Name": "string",
        "Setting Title": "string",
        "Setting Enabled": "bool",
        "Can Specify Security Groups": "bool",
        "Enabled Security Groups": "list",
        "Tenant Setting Group": "string",
        "Tenant Setting Properties": "list",
        "Delegate to Workspace": "bool",
        "Delegated From": "string",
    }

    if capacity is None:
        url = "/v1/admin/capacities/delegatedTenantSettingOverrides"
    else:
        (_, capacity_id) = _resolve_capacity_name_and_id(capacity=capacity)
        url = f"/v1/admin/capacities/{capacity_id}/delegatedTenantSettingOverrides"
    responses = _base_api(
        request=url,
        client="fabric_sp",
        uses_pagination=True,
    )

    def create_new_data(setting, capacity_id=None):
        return {
            "Capacity Id": capacity_id or setting.get("id"),
            "Setting Name": setting.get("settingName"),
            "Setting Title": setting.get("title"),
            "Setting Enabled": setting.get("enabled"),
            "Can Specify Security Groups": setting.get("canSpecifySecurityGroups"),
            "Enabled Security Groups": setting.get("enabledSecurityGroups", []),
            "Tenant Setting Group": setting.get("tenantSettingGroup"),
            "Tenant Setting Properties": setting.get("properties", []),
            "Delegate to Workspace": setting.get("delegateToWorkspace"),
            "Delegated From": setting.get("delegatedFrom"),
        }

    def process_responses(responses, capacity_id=None, return_dataframe=False):
        data = []
        df = _create_dataframe(columns=columns)

        for r in responses:
            if capacity_id is None:
                # If capacity_id is None, we access 'Overrides' -> 'tenantSettings'
                for override in r.get("overrides", []):
                    capacity_id = override.get("id")
                    tenant_settings = override.get("tenantSettings", [])
                    for setting in tenant_settings:
                        data.append(create_new_data(setting, capacity_id))
            else:
                # If capacity_id is provided, we access 'value' directly for tenantSettings
                for setting in r.get("value", []):
                    data.append(
                        create_new_data(setting, capacity_id)
                    )  # Use provided capacity_id

        if return_dataframe:
            if data:
                df = pd.DataFrame(data)
                _update_dataframe_datatypes(dataframe=df, column_map=columns)
            return df
        else:
            key = "overrides" if capacity_id is None else "value"
            continuation_uri = r.get("continuationUri", "")
            continuation_token = r.get("continuationToken", "")

            return {
                key: data,
                "continuationUri": continuation_uri,
                "continuationToken": continuation_token,
            }

    # Main logic
    if capacity is None:
        return (
            process_responses(responses, return_dataframe=True)
            if return_dataframe
            else process_responses(responses)
        )
    else:
        return (
            process_responses(responses, capacity_id=capacity_id, return_dataframe=True)
            if return_dataframe
            else process_responses(responses, capacity_id=capacity_id)
        )


@log
def list_capacities_delegated_tenant_settings(
    return_dataframe: bool = True,
) -> pd.DataFrame | dict:
    """
    Returns list of tenant setting overrides that override at the capacities.

    NOTE: This function is to be deprecated. Please use the `list_capacity_tenant_settings_overrides` function instead.

    This is a wrapper function for the following API: `Tenants - List Capacities Tenant Settings Overrides <https://learn.microsoft.com/rest/api/fabric/admin/tenants/list-capacities-tenant-settings-overrides>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    return_dataframe : bool, default=True
        If True, returns a dataframe. If False, returns a dictionary.

    Returns
    -------
    pandas.DataFrame | dict
        A pandas dataframe showing a list of tenant setting overrides that override at the capacities.
    """

    list_capacity_tenant_settings_overrides(return_dataframe=return_dataframe)


@log
def delete_capacity_tenant_setting_override(capacity: str | UUID, tenant_setting: str):
    """
    Remove given tenant setting override for given capacity Id.

    This is a wrapper function for the following API: `Tenants - Delete Capacity Tenant Setting Override <https://learn.microsoft.com/rest/api/fabric/admin/tenants/delete-capacity-tenant-setting-override>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID
        The capacity name or ID.
    tenant_setting : str
        The tenant setting name. Example: "TenantSettingForCapacityDelegatedSwitch"
    """

    (capacity_name, capacity_id) = _resolve_capacity_name_and_id(capacity=capacity)

    _base_api(
        request=f"/v1/admin/capacities/{capacity_id}/delegatedTenantSettingOverrides/{tenant_setting}",
        client="fabric_sp",
        method="delete",
    )

    print(
        f"{icons.green_dot} The '{tenant_setting}' tenant setting has been removed from the '{capacity_name}' capacity."
    )


@log
def update_tenant_setting(
    tenant_setting: str,
    enabled: bool,
    delegate_to_capacity: Optional[bool] = None,
    delegate_to_domain: Optional[bool] = None,
    delegate_to_workspace: Optional[bool] = None,
    enabled_security_groups: Optional[List[dict]] = None,
    excluded_security_groups: Optional[List[dict]] = None,
    properties: Optional[List[dict]] = None,
):
    """
    Update a given tenant setting.

    This is a wrapper function for the following API: `Tenants - Update Tenant Setting <https://learn.microsoft.com/rest/api/fabric/admin/tenants/update-tenant-setting>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    tenant_setting : str
        The tenant setting name. Example: "TenantSettingForCapacityDelegatedSwitch"
    enabled : bool
        The status of the tenant setting. False - Disabled, True - Enabled.
    delegate_to_capacity : bool, default=None
        Indicates whether the tenant setting can be delegated to a capacity admin. False - Capacity admin cannot override the tenant setting. True - Capacity admin can override the tenant setting.
    delegate_to_domain : bool, default=None
        Indicates whether the tenant setting can be delegated to a domain admin. False - Domain admin cannot override the tenant setting. True - Domain admin can override the tenant setting.
    delegate_to_workspace : bool, default=None
        Indicates whether the tenant setting can be delegated to a workspace admin. False - Workspace admin cannot override the tenant setting. True - Workspace admin can override the tenant setting.
    enabled_security_groups : List[dict], default=None
        A list of enabled security groups. Example:
        [
            {
            "graphId": "f51b705f-a409-4d40-9197-c5d5f349e2f0",
            "name": "TestComputeCdsa"
            }
        ]
    excluded_security_groups : List[dict], default=None
        A list of excluded security groups. Example:
        [
            {
            "graphId": "f51b705f-a409-4d40-9197-c5d5f349e2f0",
            "name": "TestComputeCdsa"
            }
        ]
    properties : List[dict], default=None
        Tenant setting properties. Example:
        [
            {
            "name": "CreateP2w",
            "value": "true",
            "type": "Boolean"
            }
        ]
    """

    payload = {"enabled": enabled}

    if delegate_to_capacity is not None:
        payload["delegateToCapacity"] = delegate_to_capacity
    if delegate_to_domain is not None:
        payload["delegateToDomain"] = delegate_to_domain
    if delegate_to_workspace is not None:
        payload["delegateToWorkspace"] = delegate_to_workspace
    if enabled_security_groups is not None:
        payload["enabledSecurityGroups"] = enabled_security_groups
    if excluded_security_groups is not None:
        payload["excludedSecurityGroups"] = excluded_security_groups
    if properties is not None:
        payload["properties"] = properties

    _base_api(
        request=f"/v1/admin/tenantsettings/{tenant_setting}/update",
        client="fabric_sp",
        method="post",
        payload=payload,
    )

    print(f"{icons.green_dot} The '{tenant_setting}' tenant setting has been updated.")


@log
def update_capacity_tenant_setting_override(
    capacity: str | UUID,
    tenant_setting: str,
    enabled: bool,
    delegate_to_workspace: Optional[bool] = None,
    enabled_security_groups: Optional[List[dict]] = None,
    excluded_security_groups: Optional[List[dict]] = None,
):
    """
    Update given tenant setting override for given capacity.

    This is a wrapper function for the following API: `Tenants - Update Capacity Tenant Setting Override <https://learn.microsoft.com/en-us/rest/api/fabric/admin/tenants/update-capacity-tenant-setting-override>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID
        The capacity name or ID.
    tenant_setting : str
        The tenant setting name. Example: "TenantSettingForCapacityDelegatedSwitch"
    enabled : bool
        The status of the tenant setting. False - Disabled, True - Enabled.
    delegate_to_workspace : bool, default=None
        Indicates whether the tenant setting can be delegated to a workspace admin. False - Workspace admin cannot override the tenant setting. True - Workspace admin can override the tenant setting.
    enabled_security_groups : List[dict], default=None
        A list of enabled security groups. Example:
        [
            {
            "graphId": "f51b705f-a409-4d40-9197-c5d5f349e2f0",
            "name": "TestComputeCdsa"
            }
        ]
    excluded_security_groups : List[dict], default=None
        A list of excluded security groups. Example:
        [
            {
            "graphId": "f51b705f-a409-4d40-9197-c5d5f349e2f0",
            "name": "TestComputeCdsa"
            }
        ]
    """

    (capacity_name, capacity_id) = _resolve_capacity_name_and_id(capacity=capacity)

    payload = {"enabled": enabled}

    if delegate_to_workspace is not None:
        payload["delegateToWorkspace"] = delegate_to_workspace
    if enabled_security_groups is not None:
        payload["enabledSecurityGroups"] = enabled_security_groups
    if excluded_security_groups is not None:
        payload["excludedSecurityGroups"] = excluded_security_groups

    _base_api(
        request=f"/v1/admin/capacities/{capacity_id}/delegatedTenantSettingOverrides/{tenant_setting}/update",
        client="fabric_sp",
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The '{tenant_setting}' tenant setting for the '{capacity_name}' capacity has been updated."
    )


@log
def list_workspaces_tenant_settings_overrides() -> pd.DataFrame:
    """
    Shows a list of workspace delegation setting overrides. In order to run this function, you must enable the workspace's delegated OneLake settings. To do this, navigate to the workspace, Workspace Settings -> Delegated Settings -> OneLake settings -> Set to 'On'.

    This is a wrapper function for the following API: `Tenants - List Workspaces Tenant Settings Overrides <https://learn.microsoft.com/rest/api/fabric/admin/tenants/list-workspaces-tenant-settings-overrides>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of workspace delegation setting overrides.
    """

    columns = {
        "Workspace Id": "string",
        "Setting Name": "string",
        "Title": "string",
        "Enabled": "bool",
        "Can Specify Security Groups": "bool",
        "Enabled Security Groups": "list",
        "Tenant Setting Group": "string",
        "Delegated From": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request="/v1/admin/workspaces/delegatedTenantSettingOverrides",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            workspace_id = v.get("id")
            for setting in v.get("tenantSettings", []):
                rows.append(
                    {
                        "Workspace Id": workspace_id,
                        "Setting Name": setting.get("settingName"),
                        "Title": setting.get("title"),
                        "Enabled": setting.get("enabled"),
                        "Can Specify Security Groups": setting.get(
                            "canSpecifySecurityGroups"
                        ),
                        "Enabled Security Groups": [
                            setting.get("enabledSecurityGroups", [])
                        ],
                        "Tenant Setting Group": setting.get("tenantSettingGroup"),
                        "Delegated From": setting.get("delegatedFrom"),
                    }
                )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def list_domain_tenant_settings_overrides() -> pd.DataFrame:
    """
    Shows a list of domain delegation setting overrides.

    This is a wrapper function for the following API: `Tenants - List Domains Tenant Settings Overrides <https://learn.microsoft.com/rest/api/fabric/admin/tenants/list-domains-tenant-settings-overrides>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of domain delegation setting overrides.
    """

    columns = {
        "Domain Id": "string",
        "Setting Name": "string",
        "Title": "string",
        "Enabled": "bool",
        "Can Specify Security Groups": "bool",
        "Enabled Security Groups": "list",
        "Tenant Setting Group": "string",
        "Delegated To Workspace": "bool",
        "Delegated From": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request="/v1/admin/domains/delegatedTenantSettingOverrides",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            domain_id = v.get("id")
            for setting in v.get("tenantSettings", []):
                rows.append(
                    {
                        "Domain Id": domain_id,
                        "Setting Name": setting.get("settingName"),
                        "Title": setting.get("title"),
                        "Enabled": setting.get("enabled"),
                        "Can Specify Security Groups": setting.get(
                            "canSpecifySecurityGroups"
                        ),
                        "Enabled Security Groups": [
                            setting.get("enabledSecurityGroups", [])
                        ],
                        "Tenant Setting Group": setting.get("tenantSettingGroup"),
                        "Delegated To Workspace": setting.get("delegateToWorkspace"),
                        "Delegated From": setting.get("delegatedFrom"),
                    }
                )
    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df
