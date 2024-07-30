import sempy.fabric as fabric
from typing import Optional, List
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def migrate_workspaces(
    source_capacity: str,
    target_capacity: str,
    workspaces: Optional[str | List[str]] = None,
):
    """
    This function migrates the workspace(s) from one capacity to another capacity.
    Limitation: source & target capacities must be in the same region.

    Parameters
    ----------
    source_capacity : str
        Name of the source Fabric capacity.
    target_capacity : str
        Name of the target/destination Fabric capacity.
    workspaces : str | List[str], default=None
        The name of the workspace(s) specified will be reassigned from the source capacity to the target capacity.
        Defaults to None which will reassign all workspaces in the source capacity to the target capacity.

    Returns
    -------
    """
    from sempy_labs._list_functions import assign_workspace_to_capacity

    if isinstance(workspaces, str):
        workspaces = [workspaces]

    dfC = fabric.list_capacities()
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

    dfW = fabric.list_workspaces()
    for i, r in dfW.iterrows():
        workspace = r["Name"]
        capacity_id = r["Capacity Id"]

        if capacity_id == source_capacity_id:
            if workspaces is None or workspace in workspaces:
                pass
            else:
                continue

            assign_workspace_to_capacity(
                capacity_name=target_capacity, workspace=workspace
            )


@log
def create_fabric_capacity(
    capacity_name: str,
    azure_subscription_id: str,
    key_vault: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
    resource_group: str,
    region: str,
    sku: str,
    admin_email: List[str],
):
    """
    This function creates a new Fabric capacity within an Azure subscription.

    Parameters
    ----------
    capacity_name : str
        Name of the Fabric capacity.
    azure_subscription_id : str
        The Azure subscription ID.
    key_vault : str
        The name of the `Azure key vault <https://azure.microsoft.com/products/key-vault>`_.
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
    admin_email : List[str]
        The email address(es) of the admin(s) of the Fabric capacity.

    Returns
    -------
    """
    from notebookutils import mssparkutils
    from azure.mgmt.resource import ResourceManagementClient
    from azure.identity import ClientSecretCredential
    from azure.mgmt.resource.resources.models import DeploymentMode

    capacity_suffix = "fsku"
    # is the format of the region provided in list_capacities the same which is required for the create_fabric_capacity function?
    region_list = [
        "West Central US",
        "Australia East",
        "Australia Southeast",
        "Brazil South",
        "Canada Central",
        "Canada East",
        "Central India",
        "Central US",
        "East Asia",
        "East US",
        "East US 2",
        "France Central",
        "France South",
        "Germany North",
        "Germany West Central",
        "West India",
        "Italy North",
        "Mexico Central",
        "Japan East",
        "Japan West",
        "Korea Central",
        "Korea South",
        "North Central US",
        "North Europe",
        "Norway East",
        "Norway West",
        "Qatar Central",
        "Spain Central",
        "Israel Central",
        "Poland Central",
        "South Africa North",
        "South Africa West",
        "South Central US",
        "Southeast Asia",
        "South India",
        "Sweden Central",
        "Switzerland North",
        "Switzerland West",
        "UAE Central",
        "UAE North",
        "UK South",
        "UK West",
        "West Europe",
        "West US",
        "West US 2",
        "West US 3",
        "westcentralus",
        "australiaeast",
        "australiasoutheast",
        "brazilsouth",
        "canadacentral",
        "canadaeast",
        "centralindia",
        "centralus",
        "eastasia",
        "eastus",
        "eastus2",
        "francecentral",
        "francesouth",
        "germanynorth",
        "germanywestcentral",
        "westindia",
        "italynorth",
        "mexicocentral",
        "japaneast",
        "japanwest",
        "koreacentral",
        "koreasouth",
        "northcentralus",
        "northeurope",
        "norwayeast",
        "norwaywest",
        "qatarcentral",
        "spaincentral",
        "israelcentral",
        "polandcentral",
        "southafricanorth",
        "southafricawest",
        "southcentralus",
        "southeastasia",
        "southindia",
        "swedencentral",
        "switzerlandnorth",
        "switzerlandwest",
        "uaecentral",
        "uaenorth",
        "uksouth",
        "ukwest",
        "westeurope",
        "westus",
        "westus2",
        "westus3",
    ]

    valid_regions = [
        region for region in region_list if any(char.isupper() for char in region)
    ]

    if region not in region_list:
        raise ValueError(
            f"{icons.red_dot} Invalid region. Valid options: {valid_regions}."
        )

    deployment_name = "CapacityTest"
    key_vault_uri = f"https://{key_vault}.vault.azure.net/"
    tenant_id = mssparkutils.credentials.getSecret(key_vault_uri, key_vault_tenant_id)
    client_id = mssparkutils.credentials.getSecret(key_vault_uri, key_vault_client_id)
    client_secret = mssparkutils.credentials.getSecret(
        key_vault_uri, key_vault_client_secret
    )
    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
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

    template = {
        "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
        "contentVersion": "1.0.0.1",
        "parameters": {
            "name": {"type": "string"},
            "location": {"type": "string"},
            "sku": {
                "type": "string",
                "allowedValues": [
                    "F2",
                    "F4",
                    "F8",
                    "F16",
                    "F32",
                    "F64",
                    "F128",
                    "F256",
                    "F512",
                    "F1024",
                    "F2048",
                ],
            },
            "admin": {"type": "array"},
            "tagValues": {"type": "object", "defaultValue": {}},
        },
        "variables": {},
        "resources": [
            {
                "apiVersion": "2022-07-01-preview",
                "name": "[parameters('name')]",
                "location": "[parameters('location')]",
                "sku": {"name": "[parameters('sku')]", "tier": "Fabric"},
                "properties": {"administration": {"members": "[parameters('admin')]"}},
                "type": "Microsoft.Fabric/capacities",
                "tags": "[parameters('tagValues')]",
            }
        ],
        "outputs": {},
    }

    parameters = {
        "name": {"value": capacity_name},
        "location": {"value": region},
        "sku": {"value": sku},
        "admin": {"value": admin_email},
        "tagValues": {"value": {}},
    }

    # Deploy the ARM template with the loaded parameters
    print(
        f"{icons.in_progress} Creating the '{capacity_name}' capacity within the '{region}' region..."
    )
    deployment_properties = {
        "properties": {
            "template": template,
            "parameters": parameters,
            "mode": DeploymentMode.incremental,
        }
    }
    deployment_async_operation = resource_client.deployments.begin_create_or_update(
        resource_group, deployment_name, deployment_properties
    )
    deployment_async_operation.wait()
    print(
        f"{icons.green_dot} Successfully created the '{capacity_name}' capacity within the '{region}'"
    )


@log
def migrate_capacities(
    azure_subscription_id: str,
    key_vault: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
    resource_group: str | dict,
    capacities: Optional[str | List[str]] = None,
    use_existing_rg_for_A_sku: Optional[bool] = True,
):
    """
    This function creates new Fabric capacities for given A or P sku capacities and reassigns their workspaces to the newly created capacity.

    Parameters
    ----------
    azure_subscription_id : str
        The Azure subscription ID.
    key_vault : str
        The name of the `Azure key vault <https://azure.microsoft.com/products/key-vault>`_.
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
    use_existing_rg_for_A_sku : bool, default=True

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

    for i, r in dfC.iterrows():
        cap_name = r["Display Name"]
        region = r["Region"]
        sku_size = r["Sku"]
        admins = r["Admins"]
        tgt_capacity = f"{cap_name}{capacity_suffix}"

        # Check if target capacity exists
        dfC_filt = dfC[dfC["Display Name"] == tgt_capacity]

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
            if capacities is None or cap_name in capacities:
                # Only create the capacity if it does not already exist
                if len(dfC_filt) != 0:
                    print(
                        f"{icons.yellow_dot} Skipping creating a new capacity for '{cap_name}' as the '{tgt_capacity}' capacity already exists."
                    )
                else:
                    create_fabric_capacity(
                        capacity_name=tgt_capacity,
                        azure_subscription_id=azure_subscription_id,
                        key_vault=key_vault,
                        key_vault_tenant_id=key_vault_tenant_id,
                        key_vault_client_id=key_vault_client_id,
                        key_vault_client_secret=key_vault_client_secret,
                        resource_group=rg,
                        region=region,
                        sku=sku_mapping.get(sku_size),
                        admin_email=admins,
                    )
                # Migrate workspaces to new capacity
                migrate_workspaces(
                    source_capacity=cap_name,
                    target_capacity=tgt_capacity,
                    workspaces=None,
                )
