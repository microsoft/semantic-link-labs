import pandas as pd
from typing import Optional
from uuid import UUID
from sempy._utils._log import log
import sempy_labs.mounted_data_factory as mdf


@log
def list_mounted_data_factories(
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows a list of mounted data factories from the specified workspace.

    This is a wrapper function for the following API: `Items - List Mounted Data Factories <https://learn.microsoft.com/rest/api/fabric/mounteddatafactory/items/list-mounted-data-factories>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of mounted data factories from the specified workspace.
    """

    return mdf.list_mounted_data_factories(workspace=workspace)


@log
def get_mounted_data_factory_definition(
    mounted_data_factory: str | UUID, workspace: Optional[str | UUID] = None
) -> dict:
    """
    Returns the specified MountedDataFactory public definition.

    This is a wrapper function for the following API: `Items - Get Mounted Data Factory Definition <https://learn.microsoft.com/rest/api/fabric/mounteddatafactory/items/get-mounted-data-factory-definition>`_.

    Parameters
    ----------
    mounted_data_factory : str | uuid.UUID
        The name or ID of the mounted data factory.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    dict
        The 'mountedDataFactory-content.json' file from the mounted data factory definition.
    """

    return mdf.get_mounted_data_factory_definition(
        mounted_data_factory=mounted_data_factory, workspace=workspace
    )


@log
def delete_mounted_data_factory(
    mounted_data_factory: str | UUID, workspace: Optional[str | UUID]
):
    """
    Deletes the specified mounted data factory.

    This is a wrapper function for the following API: `Items - Delete Mounted Data Factory <https://learn.microsoft.com/rest/api/fabric/mounteddatafactory/items/delete-mounted-data-factory>`_.

    Parameters
    ----------
    mounted_data_factory : str | uuid.UUID
        The name or ID of the mounted data factory.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    mdf.delete_mounted_data_factory(
        mounted_data_factory=mounted_data_factory, workspace=workspace
    )
