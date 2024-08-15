import sempy.fabric as fabric
from typing import Optional, List
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
import pandas as pd


def resolve_domain_id(domain_name: str):

    dfL = list_domains()
    dfL_filt = dfL[dfL["Domain Name"] == domain_name]
    if len(dfL_filt) == 0:
        raise ValueError(f"{icons.red_dot} '{domain_name}' is not a valid domain name.")

    return dfL_filt["Domain ID"].iloc[0]


def list_domains(non_empty_only: Optional[bool] = False):

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/domains/list-domains?tabs=HTTP

    df = pd.DataFrame(
        columns=[
            "Domain ID",
            "Domain Name",
            "Description",
            "Parent Domain ID",
            "Contributors Scope",
        ]
    )

    client = fabric.FabricRestClient()
    url = "/v1/admin/domains"
    if non_empty_only:
        url = f"{url}?nonEmptyOnly=True"
    response = client.get(url)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("domains", []):
        new_data = {
            "Domain ID": v.get("id"),
            "Domain Name": v.get("displayName"),
            "Description": v.get("description"),
            "Parent Domain ID": v.get("parentDomainId"),
            "Contributors Scope": v.get("contributorsScope"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_domain_workspaces(domain_name: str):

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/domains/list-domain-workspaces?tabs=HTTP

    domain_id = resolve_domain_id(domain_name)

    df = pd.DataFrame(columns=["Workspace ID", "Workspace Name"])

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/admin/domains/{domain_id}/workspaces")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("value", []):
        new_data = {
            "Workspace ID": v.get("id"),
            "Workspace Name": v.get("displayName"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def create_domain(
    domain_name: str,
    description: Optional[str] = None,
    parent_domain_name: Optional[str] = None,
):

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/domains/create-domain?tabs=HTTP

    if parent_domain_name is not None:
        parent_domain_id = resolve_domain_id(parent_domain_name)

    payload = {}
    payload["displayName"] = domain_name
    if description is not None:
        payload["description"] = description
    if parent_domain_name is not None:
        payload["parentDomainId"] = parent_domain_id

    client = fabric.FabricRestClient()
    response = client.post("/v1/admin/domains", json=payload)

    if response.status_code != 201:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{domain_name}' domain has been created.")


def delete_domain(domain_name: str):

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/domains/delete-domain?tabs=HTTP

    domain_id = resolve_domain_id(domain_name)

    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/admin/domains/{domain_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{domain_name}' domain has been deleted.")


def update_domain(
    domain_name: str,
    description: Optional[str] = None,
    contributors_scope: Optional[str] = None,
):

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/domains/update-domain?tabs=HTTP

    contributors_scopes = ["AdminsOnly", "AllTenant", "SpecificUsersAndGroups"]

    if contributors_scope not in contributors_scopes:
        raise ValueError(
            f"{icons.red_dot} Invalid contributors scope. Valid options: {contributors_scopes}."
        )

    domain_id = resolve_domain_id(domain_name)

    payload = {}
    payload["displayName"] = domain_name

    if description is not None:
        payload["description"] = description
    if contributors_scope is not None:
        payload["contributorsScope"] = contributors_scope

    client = fabric.FabricRestClient()
    response = client.patch(f"/v1/admin/domains/{domain_id}", json=payload)

    if response != 200:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{domain_name}' domain has been updated.")


def assign_domain_workspaces_by_capacities(
    domain_name: str, capacity_names: str | List[str]
):

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/domains/assign-domain-workspaces-by-capacities?tabs=HTTP

    from sempy_labs.admin import list_capacities

    domain_id = resolve_domain_id(domain_name)

    if isinstance(capacity_names, str):
        capacity_names = [capacity_names]

    dfC = list_capacities()

    # Check for invalid capacities
    invalid_capacities = [
        name for name in capacity_names if name not in dfC["Display Name"].values
    ]

    if len(invalid_capacities) == 1:
        raise ValueError(
            f"{icons.red_dot} The {invalid_capacities} capacity is not valid."
        )
    elif len(invalid_capacities) > 1:
        raise ValueError(
            f"{icons.red_dot} The {invalid_capacities} capacities are not valid."
        )

    # Get list of capacity Ids for the payload
    dfC_filt = dfC[dfC["Display Name"].isin(capacity_names)]
    capacity_list = list(dfC_filt["Id"].str.upper())

    payload = {"capacitiesIds": capacity_list}

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/admin/domains/{domain_id}/assignWorkspacesByCapacities",
        json=payload,
        lro_wait=True,  # TODO remove lro_wait
    )

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The workspaces in the {capacity_names} capacities have been assigned to the '{domain_name}' domain."
    )


def assign_domain_workspaces(domain_name: str, workspace_names: str | List[str]):

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/domains/assign-domain-workspaces-by-ids?tabs=HTTP

    domain_id = resolve_domain_id(domain_name=domain_name)

    if isinstance(workspace_names, str):
        workspace_names = [workspace_names]

    dfW = fabric.list_workspaces()

    # Check for invalid capacities
    invalid_workspaces = [
        name for name in workspace_names if name not in dfW["Name"].values
    ]

    if len(invalid_workspaces) == 1:
        raise ValueError(
            f"{icons.red_dot} The {invalid_workspaces} workspace is not valid."
        )
    elif len(invalid_workspaces) > 1:
        raise ValueError(
            f"{icons.red_dot} The {invalid_workspaces} workspaces are not valid."
        )

    dfW_filt = dfW[dfW["Name"].isin(workspace_names)]
    workspace_list = list(dfW_filt["Id"])

    payload = {"workspacesIds": workspace_list}

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/admin/domains/{domain_id}/assignWorkspaces",
        json=payload,
        lro_wait=True,  # TODO remove lro_wait
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The {workspace_names} workspaces have been assigned to the '{domain_name}' domain."
    )


def unassign_all_domain_workspaces(domain_name: str):

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/domains/unassign-all-domain-workspaces?tabs=HTTP

    domain_id = resolve_domain_id(domain_name=domain_name)

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/admin/domains/{domain_id}/unassignAllWorkspaces")

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} All workspaces assigned to the '{domain_name}' domain have been unassigned."
    )


def unassign_domain_workspaces(domain_name: str, workspace_names: str | List[str]):

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/domains/unassign-domain-workspaces-by-ids?tabs=HTTP

    domain_id = resolve_domain_id(domain_name=domain_name)

    if isinstance(workspace_names, str):
        workspace_names = [workspace_names]

    dfW = fabric.list_workspaces()

    # Check for invalid capacities
    invalid_workspaces = [
        name for name in workspace_names if name not in dfW["Name"].values
    ]

    if len(invalid_workspaces) == 1:
        raise ValueError(
            f"{icons.red_dot} The {invalid_workspaces} workspace is not valid."
        )
    elif len(invalid_workspaces) > 1:
        raise ValueError(
            f"{icons.red_dot} The {invalid_workspaces} workspaces are not valid."
        )

    dfW_filt = dfW[dfW["Name"].isin(workspace_names)]
    workspace_list = list(dfW_filt["Id"])

    payload = {"workspacesIds": workspace_list}
    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/admin/domains/{domain_id}/unassignWorkspaces", json=payload
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The {workspace_names} workspaces assigned to the '{domain_name}' domain have been unassigned."
    )
