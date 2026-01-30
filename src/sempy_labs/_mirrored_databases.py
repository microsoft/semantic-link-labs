import pandas as pd
from typing import Optional
from uuid import UUID
from sempy._utils._log import log
import sempy_labs.mirrored_database as md


@log
def list_mirrored_databases(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the mirrored databases within a workspace.

    This is a wrapper function for the following API: `Items - List Mirrored Databases <https://learn.microsoft.com/rest/api/fabric/mirroredwarehouse/items/list-mirrored-databases>`_.

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
        A pandas dataframe showing the mirrored databases within a workspace.
    """

    return md.list_mirrored_databases(workspace=workspace)


@log
def create_mirrored_database(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a Fabric mirrored database.

    This is a wrapper function for the following API: `Items - Create Mirrored Database <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/items/create-mirrored-database>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name: str
        Name of the mirrored database.
    description : str, default=None
        A description of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    md.create_mirrored_database(name=name, description=description, workspace=workspace)


@log
def delete_mirrored_database(
    mirrored_database: str, workspace: Optional[str | UUID] = None
):
    """
    Deletes a mirrored database.

    This is a wrapper function for the following API: `Items - Delete Mirrored Database <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/items/delete-mirrored-database>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_database: str
        Name of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    md.delete_mirrored_database(
        mirrored_database=mirrored_database, workspace=workspace
    )


@log
def get_mirroring_status(
    mirrored_database: str | UUID, workspace: Optional[str | UUID] = None
) -> str:
    """
    Get the status of the mirrored database.

    This is a wrapper function for the following API: `Mirroring - Get Mirroring Status <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/mirroring/get-mirroring-status>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_database: str | uuid.UUID
        Name or ID of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The status of a mirrored database.
    """

    return md.get_mirroring_status(
        mirrored_database=mirrored_database, workspace=workspace
    )


@log
def get_tables_mirroring_status(
    mirrored_database: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Gets the mirroring status of the tables.

    This is a wrapper function for the following API: `Mirroring - Get Tables Mirroring Status <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/mirroring/get-tables-mirroring-status>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_database: str | uuid.UUID
        Name or ID of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the mirroring status of the tables.
    """

    return md.get_tables_mirroring_status(
        mirrored_database=mirrored_database, workspace=workspace
    )


@log
def start_mirroring(
    mirrored_database: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Starts the mirroring for a database.

    This is a wrapper function for the following API: `Mirroring - Start Mirroring <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/mirroring/start-mirroring>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_database: str | uuid.UUID
        Name or ID of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    md.start_mirroring(mirrored_database=mirrored_database, workspace=workspace)


@log
def stop_mirroring(
    mirrored_database: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Stops the mirroring for a database.

    This is a wrapper function for the following API: `Mirroring - Stop Mirroring <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/mirroring/stop-mirroring>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_database: str | uuid.UUID
        Name or ID of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    md.stop_mirroring(mirrored_database=mirrored_database, workspace=workspace)


@log
def get_mirrored_database_definition(
    mirrored_database: str | UUID,
    workspace: Optional[str | UUID] = None,
    decode: bool = True,
) -> dict:
    """
    Obtains the mirrored database definition.

    This is a wrapper function for the following API: `Items - Get Mirrored Database Definition <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/items/get-mirrored-database-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_database : str | uuid.UUID
        The name or ID of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    decode : bool, default=True
        If True, decodes the mirrored database definition file into .json format.
        If False, obtains the mirrored database definition file in base64 format.

    Returns
    -------
    dict
        The mirrored database definition.
    """

    return md.get_mirrored_database_definition(
        mirrored_database=mirrored_database,
        workspace=workspace,
        decode=decode,
    )


@log
def update_mirrored_database_definition(
    mirrored_database: str | UUID,
    mirrored_database_content: dict,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates an existing notebook with a new definition.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_database : str | uuid.UUID
        The name or ID of the mirrored database to be updated.
    mirrored_database_content : dict
        The mirrored database definition (not in Base64 format).
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    md.update_mirrored_database_definition(
        mirrored_database=mirrored_database,
        mirrored_database_content=mirrored_database_content,
        workspace=workspace,
    )
