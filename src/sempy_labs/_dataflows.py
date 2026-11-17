import pandas as pd
from typing import List, Optional, Tuple
from uuid import UUID
from sempy._utils._log import log
import sempy_labs.dataflow as d


@log
def list_dataflows(workspace: Optional[str | UUID] = None):
    """
    Shows a list of all dataflows which exist within a workspace.

    This is a wrapper function for the following API: `Items - List Dataflows <https://learn.microsoft.com/rest/api/fabric/dataflow/items/list-dataflows>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the dataflows which exist within a workspace.
    """

    return d.list_dataflows(workspace=workspace)


@log
def assign_workspace_to_dataflow_storage(
    dataflow_storage_account: str, workspace: Optional[str | UUID] = None
):
    """
    Assigns a dataflow storage account to a workspace.

    This is a wrapper function for the following API: `Dataflow Storage Accounts - Groups AssignToDataflowStorage <https://learn.microsoft.com/rest/api/power-bi/dataflow-storage-accounts/groups-assign-to-dataflow-storage>`_.

    Parameters
    ----------
    dataflow_storage_account : str
        The name of the dataflow storage account.
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    d.assign_workspace_to_dataflow_storage(
        dataflow_storage_account=dataflow_storage_account, workspace=workspace
    )


@log
def list_dataflow_storage_accounts() -> pd.DataFrame:
    """
    Shows the accessible dataflow storage accounts.

    This is a wrapper function for the following API: `Dataflow Storage Accounts - Get Dataflow Storage Accounts <https://learn.microsoft.com/rest/api/power-bi/dataflow-storage-accounts/get-dataflow-storage-accounts>`_.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the accessible dataflow storage accounts.
    """

    return d.list_dataflow_storage_accounts()


@log
def list_upstream_dataflows(
    dataflow: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a list of upstream dataflows for the specified dataflow.

    This is a wrapper function for the following API: `Dataflows - Get Upstream Dataflows In Group <https://learn.microsoft.com/rest/api/power-bi/dataflows/get-upstream-dataflows-in-group>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataflow : str | uuid.UUID
        Name or UUID of the dataflow.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of upstream dataflows for the specified dataflow.
    """

    return d.list_upstream_dataflows(dataflow=dataflow, workspace=workspace)


@log
def resolve_dataflow_name_and_id_and_generation(
    dataflow: str | UUID, workspace: Optional[str | UUID] = None
) -> Tuple[str, UUID, str]:

    return d.resolve_dataflow_name_and_id_and_generation(
        dataflow=dataflow, workspace=workspace
    )


@log
def get_dataflow_definition(
    dataflow: str | UUID,
    workspace: Optional[str | UUID] = None,
    decode: bool = True,
) -> dict:
    """
    Obtains the definition of a dataflow. This supports Gen1, Gen2 and Gen 2 CI/CD dataflows.

    This is a wrapper function for the following API: `Dataflows - Get Dataflow <https://learn.microsoft.com/rest/api/power-bi/dataflows/get-dataflow>`_.

    Parameters
    ----------
    dataflow : str | uuid.UUID
        The name or ID of the dataflow.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name.
        Defaults to None, which resolves to the workspace of the attached lakehouse
        or if no lakehouse is attached, resolves to the workspace of the notebook.
    decode : bool, optional
        If True, decodes the dataflow definition file.

    Returns
    -------
    dict
        The dataflow definition.
    """

    return d.get_dataflow_definition(
        dataflow=dataflow, workspace=workspace, decode=decode
    )


@log
def upgrade_dataflow(
    dataflow: str | UUID | List[str | UUID],
    workspace: Optional[str | UUID] = None,
    new_dataflow_name: Optional[str] = None,
    new_dataflow_workspace: Optional[str | UUID] = None,
    in_place: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Upgrades a dataflow to Dataflow Gen2.

    By default (``in_place=False``), creates a new Dataflow Gen2 CI/CD item based on the mashup definition from an existing Gen1/Gen2 dataflow
    and leaves the source dataflow unchanged. After running this function, update the connections in the new dataflow to ensure the data can be properly refreshed.

    If ``in_place=True``, upgrades one or more Gen1 dataflows to Dataflow Gen2 in place using the
    `Upgrade - Upgrade Dataflows <https://learn.microsoft.com/rest/api/fabric/dataflow/upgrade/upgrade-dataflows>`_ API.
    Each successful upgrade preserves the dataflow ID and workspace. The in-place upgrade modifies the source dataflow and is not reversible (see
    `Upgrade limitations <https://learn.microsoft.com/fabric/data-factory/migrate-to-dataflow-gen2-using-upgrade-wizard#the-upgrade-is-in-place-and-isnt-reversible>`_).

    Service Principal Authentication is supported for in-place upgrades (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataflow : str | uuid.UUID | List[str | uuid.UUID]
        The name or ID of the dataflow. A list of Gen1 dataflow names or IDs (up to 50) is only supported when ``in_place=True``.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    new_dataflow_name : str, default=None
        Name of the new dataflow. Defaults to None which uses the name of the existing dataflow.
        Only applies when ``in_place=False``.
    new_dataflow_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID of the dataflow to be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
        Only applies when ``in_place=False``.
    in_place : bool, default=False
        If True, upgrades the Gen1 dataflow(s) in place. This modifies the source dataflow(s) and is not reversible.
        If False, creates a new Dataflow Gen2 CI/CD item and leaves the source dataflow unchanged.

    Returns
    -------
    pandas.DataFrame | None
        If ``in_place=True``, a pandas dataframe showing the upgrade result for each dataflow.
        If ``in_place=False``, returns None.

    Raises
    ------
    ValueError
        If ``in_place=True`` and 'new_dataflow_name' or 'new_dataflow_workspace' is specified, if more than 50 Gen1 dataflows are provided,
        if a list of dataflows is provided without ``in_place=True``, or if a dataflow does not exist.
    """

    return d.upgrade_dataflow(
        dataflow=dataflow,
        workspace=workspace,
        new_dataflow_name=new_dataflow_name,
        new_dataflow_workspace=new_dataflow_workspace,
        in_place=in_place,
    )


@log
def create_dataflow(
    name: str,
    workspace: Optional[str | UUID] = None,
    description: Optional[str] = None,
    definition: Optional[dict] = None,
):
    """
    Creates a native Fabric Dataflow Gen2 CI/CD item.

    This is a wrapper function for the following API: `Items - Create Dataflow <https://learn.microsoft.com/rest/api/fabric/dataflow/items/create-dataflow>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str
        The name the dataflow.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    description : str, default=None
        The description of the dataflow.
    definition : dict, default=None
        The definition of the dataflow in the form of a dictionary.
    """

    d.create_dataflow(
        name=name,
        workspace=workspace,
        description=description,
        definition=definition,
    )


@log
def discover_dataflow_parameters(
    dataflow: str | UUID, workspace: str | UUID
) -> pd.DataFrame:
    """
    Retrieves all parameters defined in the specified Dataflow.

    This is a wrapper function for the following API: `Items - Discover Dataflow Parameters <https://learn.microsoft.com/rest/api/fabric/dataflow/items/discover-dataflow-parameters>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataflow : str | uuid.UUID
        Name or ID of the dataflow.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing all parameters defined in the specified Dataflow.
    """

    return d.discover_dataflow_parameters(dataflow=dataflow, workspace=workspace)
