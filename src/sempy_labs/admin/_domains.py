from typing import Optional, List
import sempy_labs._icons as icons
import pandas as pd
from uuid import UUID
from sempy_labs.admin._basic_functions import list_workspaces
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    _is_valid_uuid,
)
from sempy._utils._log import log


@log
def resolve_domain_id(domain: Optional[str | UUID] = None, **kwargs) -> UUID:
    """
    Obtains the domain Id for a given domain name.

    Parameters
    ----------
    domain_name : str | uuid.UUID
        The domain name or ID

    Returns
    -------
    uuid.UUID
        The domain Id.
    """

    if "domain_name" in kwargs:
        domain = kwargs["domain_name"]
        print(
            f"{icons.warning} The 'domain_name' parameter is deprecated. Please use 'domain' instead."
        )

    if domain is None:
        raise ValueError(f"{icons.red_dot} Please provide a domain.")

    if _is_valid_uuid(domain):
        return domain

    dfL = list_domains()
    dfL_filt = dfL[dfL["Domain Name"] == domain]
    if dfL_filt.empty:
        raise ValueError(f"{icons.red_dot} '{domain}' is not a valid domain name.")

    return dfL_filt["Domain ID"].iloc[0]


@log
def resolve_domain_name(domain: Optional[str | UUID], **kwargs) -> UUID:
    """
    Obtains the domain name for a given domain ID.

    Parameters
    ----------
    domain : str | uuid.UUID
        The domain name or ID

    Returns
    -------
    str
        The domain Name.
    """

    if "domain_name" in kwargs:
        domain = kwargs["domain_name"]
        print(
            f"{icons.warning} The 'domain_name' parameter is deprecated. Please use 'domain' instead."
        )

    if domain is None:
        raise ValueError(f"{icons.red_dot} Please provide a domain.")

    if not _is_valid_uuid(domain):
        return domain

    dfL = list_domains()
    dfL_filt = dfL[dfL["Domain ID"] == domain]
    if dfL_filt.empty:
        raise ValueError(f"{icons.red_dot} '{domain}' is not a valid domain name.")

    return dfL_filt["Domain Name"].iloc[0]


@log
def list_domains(non_empty_only: bool = False) -> pd.DataFrame:
    """
    Shows a list of domains.

    This is a wrapper function for the following API: `Domains - List Domains <https://learn.microsoft.com/rest/api/fabric/admin/domains/list-domains>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    non_empty_only : bool, default=False
        When True, only return domains that have at least one workspace containing an item.
        Defaults to False.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the domains.
    """

    columns = {
        "Domain ID": "string",
        "Domain Name": "string",
        "Description": "string",
        "Parent Domain ID": "string",
        "Contributors Scope": "string",
    }
    df = _create_dataframe(columns=columns)

    url = "/v1/admin/domains"
    if non_empty_only:
        url = f"{url}?nonEmptyOnly=True"

    response = _base_api(request=url, client="fabric_sp")

    rows = []
    for v in response.json().get("domains", []):
        rows.append(
            {
                "Domain ID": v.get("id"),
                "Domain Name": v.get("displayName"),
                "Description": v.get("description"),
                "Parent Domain ID": v.get("parentDomainId"),
                "Contributors Scope": v.get("contributorsScope"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def list_domain_workspaces(domain: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """
    Shows a list of workspaces within the domain.

    This is a wrapper function for the following API: `Domains - List Domain Workspaces <https://learn.microsoft.com/rest/api/fabric/admin/domains/list-domain-workspaces>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    domain : str | uuid.UUID
        The domain name or ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of workspaces within the domain.
    """

    if "domain_name" in kwargs:
        domain = kwargs["domain_name"]
        print(
            f"{icons.warning} The 'domain_name' parameter is deprecated. Please use 'domain' instead."
        )

    if domain is None:
        raise ValueError(f"{icons.red_dot} Please provide a domain.")

    domain_id = resolve_domain_id(domain)

    columns = {
        "Workspace ID": "string",
        "Workspace Name": "string",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(
        request=f"/v1/admin/domains/{domain_id}/workspaces", client="fabric_sp"
    )

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
                "Workspace ID": v.get("id"),
                "Workspace Name": v.get("displayName"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def create_domain(
    domain_name: str,
    description: Optional[str] = None,
    parent_domain: Optional[str | UUID] = None,
    **kwargs,
):
    """
    Creates a new domain.

    This is a wrapper function for the following API: `Domains - Create Domain <https://learn.microsoft.com/rest/api/fabric/admin/domains/create-domain>`_.

    Parameters
    ----------
    domain_name : str
        The domain name.
    description : str, default=None
        The domain description.
    parent_domain : str | uuid.UUID, default=None
        The parent domain name or ID.
    """

    if "parent_domain_name" in kwargs:
        parent_domain = kwargs["parent_domain_name"]
        print(
            f"{icons.warning} The 'parent_domain_name' parameter is deprecated. Please use 'parent_domain' instead."
        )

    if parent_domain is not None:
        parent_domain_id = resolve_domain_id(domain=parent_domain)

    payload = {}
    payload["displayName"] = domain_name
    if description is not None:
        payload["description"] = description
    if parent_domain is not None:
        payload["parentDomainId"] = parent_domain_id

    _base_api(
        request="/v1/admin/domains", method="post", payload=payload, status_codes=201
    )

    print(f"{icons.green_dot} The '{domain_name}' domain has been created.")


@log
def delete_domain(domain: Optional[str | UUID], **kwargs):
    """
    Deletes a domain.

    This is a wrapper function for the following API: `Domains - Delete Domain <https://learn.microsoft.com/rest/api/fabric/admin/domains/delete-domain>`_.

    Parameters
    ----------
    domain : str | uuid.UUID
        The domain name or ID.
    """

    if "domain_name" in kwargs:
        domain = kwargs["domain_name"]
        print(
            f"{icons.warning} The 'domain_name' parameter is deprecated. Please use 'domain' instead."
        )

    if domain is None:
        raise ValueError(f"{icons.red_dot} Please provide a domain.")

    domain_id = resolve_domain_id(domain)
    _base_api(request=f"/v1/admin/domains/{domain_id}", method="delete")

    print(f"{icons.green_dot} The '{domain}' domain has been deleted.")


@log
def update_domain(
    domain: Optional[str | UUID] = None,
    description: Optional[str] = None,
    contributors_scope: Optional[str] = None,
    **kwargs,
):
    """
    Updates a domain's properties.

    This is a wrapper function for the following API: `Domains - Update Domain <https://learn.microsoft.com/rest/api/fabric/admin/domains/update-domain>`_.

    Parameters
    ----------
    domain : str | uuid.UUID
        The domain name or ID.
    description : str, default=None
        The domain description.
    contributors_scope : str, default=None
        The domain `contributor scope <https://learn.microsoft.com/rest/api/fabric/admin/domains/update-domain?tabs=HTTP#contributorsscopetype>`_.
    """

    if "domain_name" in kwargs:
        domain = kwargs["domain_name"]
        print(
            f"{icons.warning} The 'domain_name' parameter is deprecated. Please use 'domain' instead."
        )

    if domain is None:
        raise ValueError(f"{icons.red_dot} Please provide a domain.")
    contributors_scopes = ["AdminsOnly", "AllTenant", "SpecificUsersAndGroups"]

    if contributors_scope not in contributors_scopes:
        raise ValueError(
            f"{icons.red_dot} Invalid contributors scope. Valid options: {contributors_scopes}."
        )

    domain_id = resolve_domain_id(domain)
    domain_name = resolve_domain_name(domain)

    payload = {}
    payload["displayName"] = domain_name

    if description is not None:
        payload["description"] = description
    if contributors_scope is not None:
        payload["contributorsScope"] = contributors_scope

    _base_api(request=f"/v1/admin/domains/{domain_id}", method="patch", payload=payload)

    print(f"{icons.green_dot} The '{domain_name}' domain has been updated.")


@log
def assign_domain_workspaces_by_capacities(
    domain: str | UUID,
    capacity_names: str | List[str],
    **kwargs,
):
    """
    Assigns all workspaces that reside on the specified capacities to the specified domain.

    This is a wrapper function for the following API: `Domains - Assign Domain Workspaces By Capacities <https://learn.microsoft.com/rest/api/fabric/admin/domains/assign-domain-workspaces-by-capacities>`_.

    Parameters
    ----------
    domain : str | uuid.UUID
        The domain name or ID.
    capacity_names : str | List[str]
        The capacity names.
    """

    from sempy_labs.admin._capacities import list_capacities

    if "domain_name" in kwargs:
        domain = kwargs["domain_name"]
        print(
            f"{icons.warning} The 'domain_name' parameter is deprecated. Please use 'domain' instead."
        )

    if domain is None:
        raise ValueError(f"{icons.red_dot} Please provide a domain.")
    domain_id = resolve_domain_id(domain)

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

    _base_api(
        request=f"/v1/admin/domains/{domain_id}/assignWorkspacesByCapacities",
        method="post",
        payload=payload,
        lro_return_status_code=True,
        status_codes=202,
    )

    print(
        f"{icons.green_dot} The workspaces in the {capacity_names} capacities have been assigned to the '{domain}' domain."
    )


@log
def assign_domain_workspaces(domain: str | UUID, workspace_names: str | List[str]):
    """
    Assigns workspaces to the specified domain by workspace.

    This is a wrapper function for the following API: `Domains - Assign Domain Workspaces By Ids <https://learn.microsoft.com/rest/api/fabric/admin/domains/assign-domain-workspaces-by-ids>`_.

    Parameters
    ----------
    domain : str | uuid.UUID
        The domain name or ID.
    workspace_names : str | List[str]
        The Fabric workspace(s).
    """

    domain_id = resolve_domain_id(domain)

    if isinstance(workspace_names, str):
        workspace_names = [workspace_names]

    dfW = list_workspaces()

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

    _base_api(
        request=f"/v1/admin/domains/{domain_id}/assignWorkspaces",
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The {workspace_names} workspaces have been assigned to the '{domain}' domain."
    )


@log
def unassign_all_domain_workspaces(domain: str | UUID):
    """
    Unassigns all workspaces from the specified domain.

    This is a wrapper function for the following API: `Domains - Unassign All Domain Workspaces <https://learn.microsoft.com/rest/api/fabric/admin/domains/unassign-all-domain-workspaces>`_.

    Parameters
    ----------
    domain : str | uuid.UUID
        The domain name or ID.
    """

    domain_id = resolve_domain_id(domain)

    _base_api(
        request=f"/v1/admin/domains/{domain_id}/unassignAllWorkspaces",
        method="post",
        lro_return_status_code=True,
        status_codes=200,
    )

    print(
        f"{icons.green_dot} All workspaces assigned to the '{domain}' domain have been unassigned."
    )


@log
def unassign_domain_workspaces(
    domain: str | UUID,
    workspace_names: str | List[str],
):
    """
    Unassigns workspaces from the specified domain by workspace.

    This is a wrapper function for the following API: `Domains - Unassign Domain Workspaces By Ids <https://learn.microsoft.com/rest/api/fabric/admin/domains/unassign-domain-workspaces-by-ids>`_.

    Parameters
    ----------
    domain : str | uuid.UUID
        The domain name or ID.
    workspace_names : str | List[str]
        The Fabric workspace(s).
    """

    domain_id = resolve_domain_id(domain)

    if isinstance(workspace_names, str):
        workspace_names = [workspace_names]

    dfW = list_workspaces()

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

    _base_api(
        request=f"/v1/admin/domains/{domain_id}/unassignWorkspaces",
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The {workspace_names} workspaces assigned to the '{domain}' domain have been unassigned."
    )
