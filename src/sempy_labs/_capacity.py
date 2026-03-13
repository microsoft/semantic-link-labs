import requests
import pandas as pd
from uuid import UUID
from typing import Optional
from sempy_labs._helper_functions import (
    get_pbi_token_headers,
    _get_url_prefix,
    resolve_capacity_id,
    _create_dataframe,
    _update_dataframe_datatypes,
    _base_api,
)
from sempy.fabric.exceptions import FabricHTTPException
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def get_capacity(
    capacity: Optional[str | UUID] = None, return_dataframe: bool = True
) -> dict:

    prefix = _get_url_prefix()
    capacity_id = resolve_capacity_id(capacity)
    headers = get_pbi_token_headers()

    response = requests.get(f"{prefix}/capacities/{capacity_id}", headers=headers)
    response_json = response.json()

    if not return_dataframe:
        return response_json

    columns = {
        "Capacity Id": "str",
        "Capacity Name": "str",
        "Sku": "str",
        "Sku Scale": "int",
        "VCores": "int",
        "Memory in GB": "int",
        "Region": "str",
        "Mode": "int",
        "Admins": "list",
        "Creation Date": "datetime",
        "Is Copilot Allowed": "bool",
    }
    df = _create_dataframe(columns=columns)

    rows = []
    metadata = response_json.get("metadata", {})
    config = metadata.get("configuration", {})
    license = metadata.get("license", {})
    rows.append(
        {
            "Capacity Id": metadata.get("capacityObjectId"),
            "Capacity Name": config.get("displayName"),
            "Sku": config.get("sku"),
            "Sku Scale": config.get("skuScale"),
            "VCores": license.get("capacityNumberOfVCores"),
            "Memory in GB": license.get("capacityMemoryInGB"),
            "Region": config.get("region"),
            "Mode": config.get("mode"),
            "Admins": metadata.get("admins", []),
            "Creation Date": metadata.get("creationDate"),
            "Is Copilot Allowed": metadata.get("isCopilotAllowed"),
        }
    )

    if rows:
        df = pd.DataFrame(rows)
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


_capacity_settings_suffix = "capacityCustomParameters?workloadIds=AD&workloadIds=ADB&workloadIds=ADM&workloadIds=AI&workloadIds=BkpVault&workloadIds=Config&workloadIds=CosmosDB&workloadIds=CDSA&workloadIds=DMS&workloadIds=AS&workloadIds=QES&workloadIds=Dataverse&workloadIds=DI&workloadIds=dmh&workloadIds=DMR&workloadIds=DO&workloadIds=CDR&workloadIds=DSE&workloadIds=ES&workloadIds=ESGLake&workloadIds=Dataflows&workloadIds=FabricDW&workloadIds=FuncSet&workloadIds=GCM&workloadIds=GeoIntel&workloadIds=Graph&workloadIds=GraphQL&workloadIds=HLS&workloadIds=ScreenshotEngine&workloadIds=Kusto&workloadIds=lake&workloadIds=Lakehouse&workloadIds=LiveTable&workloadIds=ML&workloadIds=Must&workloadIds=Notebook&workloadIds=NLS&workloadIds=OA&workloadIds=OLDI&workloadIds=OneRiver&workloadIds=RsRdlEngine&workloadIds=Pbcd&workloadIds=PowerBI&workloadIds=REP&workloadIds=Rls&workloadIds=Reflex&workloadIds=SparkCore&workloadIds=SQLDb&workloadIds=SqlTools&workloadIds=TDS&workloadIds=TIPS&workloadIds=Transform&workloadIds=Vpaas&workloadIds=Xdw"


@log
def get_capacity_settings(capacity: Optional[str | UUID] = None) -> dict:

    prefix = _get_url_prefix()
    capacity_id = resolve_capacity_id(capacity)
    headers = get_pbi_token_headers()

    response = requests.get(
        f"{prefix}/capacities/{capacity_id}/{_capacity_settings_suffix}",
        headers=headers,
    )
    return response.json()


@log
def migrate_capacity_settings(source_capacity: str | UUID, target_capacity: str | UUID):

    source_capacity_id = resolve_capacity_id(source_capacity)
    target_capacity_id = resolve_capacity_id(target_capacity)
    if source_capacity_id == target_capacity_id:
        raise ValueError(
            f"{icons.red_dot} The source and target capacity cannot be the same."
        )
    source_settings = get_capacity_settings(capacity=source_capacity_id)
    settings = {"capacityCustomParameters": {}}

    # Create payload for put request
    def remove_empty_workloads(data):
        keys_to_remove = [
            key for key, value in data.items() if not value["workloadCustomParameters"]
        ]
        for key in keys_to_remove:
            del data[key]

    remove_empty_workloads(source_settings.get("capacityCustomParameters"))

    url_workload = ""

    for workload, setting in source_settings.get("capacityCustomParameters").items():
        if workload not in ["AI"]:
            url_workload += f"workloadIds={workload}&"
            settings["capacityCustomParameters"][workload] = {
                "workloadCustomParameters": {}
            }
            for item in setting.get("workloadCustomParameters"):
                name = item.get("name")
                value = item.get("value")
                if value is None:
                    settings["capacityCustomParameters"][workload][
                        "workloadCustomParameters"
                    ][name] = value
                elif isinstance(value, bool):
                    settings["capacityCustomParameters"][workload][
                        "workloadCustomParameters"
                    ][name] = bool(value)
                elif isinstance(value, str):
                    settings["capacityCustomParameters"][workload][
                        "workloadCustomParameters"
                    ][name] = str(value)
                else:
                    settings["capacityCustomParameters"][workload][
                        "workloadCustomParameters"
                    ][name] = value

    prefix = _get_url_prefix()
    headers = get_pbi_token_headers()
    response = requests.put(
        f"{prefix}/capacities/{target_capacity_id}/capacityCustomParameters?{url_workload[:-1]}",
        headers=headers,
        json=settings,
    )

    if response.status_code != 204:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The capacity settings have been successfully migrated from capacity {source_capacity} to capacity {target_capacity}."
    )


def migrate_disaster_recovery_settings(
    source_capacity: str | UUID, target_capacity: str | UUID
):

    source_capacity_id = resolve_capacity_id(source_capacity)
    target_capacity_id = resolve_capacity_id(target_capacity)
    if source_capacity_id == target_capacity_id:
        raise ValueError(
            f"{icons.red_dot} The source and target capacity cannot be the same."
        )

    headers = get_pbi_token_headers()
    prefix = _get_url_prefix()
    response = requests.get(
        f"{prefix}/capacities/{source_capacity_id}/config", headers=headers
    )

    payload = {}
    value = response.json()["bcdr"]["config"]
    payload["config"] = value

    response = requests.put(
        f"{prefix}/capacities/{target_capacity_id}/fabricbcdr",
        headers=headers,
        json=payload,
    )
    if response.status_code != 202:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The disaster recovery settings have been successfully migrated from capacity {source_capacity} to capacity {target_capacity}."
    )


def migrate_access_settings(source_capacity: str | UUID, target_capacity: str | UUID):

    source_capacity_id = resolve_capacity_id(source_capacity)
    target_capacity_id = resolve_capacity_id(target_capacity)
    if source_capacity_id == target_capacity_id:
        raise ValueError(
            f"{icons.red_dot} The source and target capacity cannot be the same."
        )

    headers = get_pbi_token_headers()
    prefix = _get_url_prefix()
    response = requests.get(
        f"{prefix}/capacities/{source_capacity_id}", headers=headers
    )

    payload = response.json().get("access", {})

    response = requests.put(
        f"{prefix}/capacities/{target_capacity_id}/access",
        headers=headers,
        json=payload,
    )
    if response.status_code != 204:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The access settings have been successfully migrated from capacity {source_capacity} to capacity {target_capacity}."
    )


def migrate_notification_settings(
    source_capacity: str | UUID, target_capacity: str | UUID
):

    source_capacity_id = resolve_capacity_id(source_capacity)
    target_capacity_id = resolve_capacity_id(target_capacity)
    if source_capacity_id == target_capacity_id:
        raise ValueError(
            f"{icons.red_dot} The source and target capacity cannot be the same."
        )

    headers = get_pbi_token_headers()
    prefix = _get_url_prefix()
    response = requests.get(
        f"{prefix}/capacities/{source_capacity_id}", headers=headers
    )

    payload = response.json().get("capacityNotificationSettings", {})

    response = requests.put(
        f"{prefix}/capacities/{target_capacity_id}/notificationSettings",
        headers=headers,
        json=payload,
    )
    if response.status_code != 204:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The notification settings have been successfully migrated from capacity {source_capacity} to capacity {target_capacity}."
    )


def migrate_spark_settings(source_capacity: str | UUID, target_capacity: str | UUID):
    """
    This function migrates a capacity's spark settings to another capacity.

    Requirement: The target capacity must be able to accomodate the spark pools being migrated from the source capacity.

    Parameters
    ----------
    source_capacity : str | uuid.UUID
        Name or ID of the source capacity.
    target_capacity : str | uuid.UUID
        Name or ID of the target capacity.
    """

    source_capacity_id = resolve_capacity_id(capacity=source_capacity)
    target_capacity_id = resolve_capacity_id(capacity=target_capacity)

    prefix = _get_url_prefix()
    headers = get_pbi_token_headers()

    # Get source capacity server dns
    response = requests.get(
        f"{prefix}/metadata/capacityInformation/{source_capacity_id}", headers=headers
    )
    source_server_dns = response.json().get("capacityDns")
    source_url = f"{source_server_dns}/webapi/capacities"

    # Get target capacity server dns
    response = requests.get(
        f"{prefix}/metadata/capacityInformation/{target_capacity_id}", headers=headers
    )
    target_server_dns = response.json().get("capacityDns")
    target_url = f"{target_server_dns}/webapi/capacities"

    # Construct get and put URLs
    end_url = "workloads/SparkCore/SparkCoreService/automatic/v1/sparksettings"
    get_url = f"{source_url}/{source_capacity_id}/{end_url}"
    put_url = f"{target_url}/{target_capacity_id}/{end_url}/content"

    # Get source capacity spark settings
    response = requests.get(get_url, headers=headers)
    payload = response.json().get("content")

    # Update target capacity spark settings
    response = requests.put(put_url, headers=headers, json=payload)
    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The spark settings have been migrated from the '{source_capacity}' capacity to the '{target_capacity}' capacity."
    )


def migrate_delegated_tenant_settings(
    source_capacity: str | UUID, target_capacity: str | UUID
):
    """
    This function migrates the delegated tenant settings from a source capacity to a target capacity.

    Parameters
    ----------
    source_capacity : str | uuid.UUID
        Name or ID of the source capacity.
    target_capacity : str | uuid.UUID
        Name or ID of the target capacity.
    """

    prefix = _get_url_prefix()
    headers = get_pbi_token_headers()

    source_capacity_id = resolve_capacity_id(capacity=source_capacity)
    target_capacity_id = resolve_capacity_id(capacity=target_capacity)
    response_get = _base_api("v1/admin/capacities/delegatedTenantSettingOverrides")

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

                response = requests.put(
                    f"{prefix}/metadata/tenantsettings/selfserve?capacityObjectId={target_capacity_id}",
                    headers=headers,
                    json=payload,
                )
                if response.status_code != 200:
                    raise FabricHTTPException(response)

                print(
                    f"{icons.green_dot} The delegated tenant settings for the '{setting_name}' feature switch of the '{source_capacity}' capacity have been migrated to the '{target_capacity}' capacity."
                )


def _migrate_settings(source_capacity: str, target_capacity: str):

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