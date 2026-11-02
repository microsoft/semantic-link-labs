from typing import Optional, Literal
from uuid import UUID
from sempy._utils._log import log
import pandas as pd


@log
def get_onelake_settings(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Obtains the workspace OneLake settings.

    This is a wrapper function for the following API: `OneLake Settings - Get Settings <https://learn.microsoft.com/rest/api/fabric/core/onelake-settings/get-settings>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        The workspace OneLake settings.
    """

    from sempy_labs.onelake._items import get_onelake_settings as gos
    return gos(workspace=workspace)


def modify_onelake_diagnostics(
    workspace: Optional[str | UUID] = None,
    enabled: bool = True,
    destination_lakehouse: Optional[str | UUID] = None,
    destination_workspace: Optional[str | UUID] = None,
):
    """
    Obtains the workspace OneLake settings.

    This is a wrapper function for the following API: `OneLake Settings - Modify Diagnostics <https://learn.microsoft.com/rest/api/fabric/core/onelake-settings/modify-diagnostics>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    enabled : bool, default=True
        Whether to enable or disable OneLake diagnostics.
    destination_lakehouse : str | uuid.UUID, default=None
        The name or ID of the destination lakehouse.
        Defaults to None which resolves to the lakehouse of the attached lakehouse
        or if no lakehouse attached, resolves to the lakehouse of the notebook.
    destination_workspace : str | uuid.UUID, default=None
        The name or ID of the destination workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    from sempy_labs.onelake._items import modify_onelake_diagnostics as mod_diag
    return mod_diag(
        workspace=workspace,
        enabled=enabled,
        destination_lakehouse=destination_lakehouse,
        destination_workspace=destination_workspace,
    )


@log
def modify_immutability_policy(
    retention_days: int,
    scope: Literal["DiagnosticLogs"] = "DiagnosticLogs",
    workspace: Optional[str | UUID] = None,
):
    """
    Create or update OneLake immutability settings.
    Set immutability policy for data stored in OneLake. Currently, this feature supports configuring a retention period specifically for diagnostic logs within a workspace, ensuring they remain unaltered once written.

    This is a wrapper function for the following API: `OneLake Settings - Modify Immutability Policy <https://learn.microsoft.com/rest/api/fabric/core/onelake-settings/modify-immutability-policy>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    retention_days : int
        Retention Days for the action.
    scope : typing.Literal["DiagnosticLogs"], default="DiagnosticLogs"
        The scope of the immutability policy. Currently, only "DiagnosticLogs" is supported.
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    from sempy_labs.onelake._items import modify_immutability_policy as mod_immut
    return mod_immut(
        retention_days=retention_days,
        scope=scope,
        workspace=workspace,
    )

