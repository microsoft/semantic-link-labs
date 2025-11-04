import pandas as pd
import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
    resolve_item_id,
    delete_item,
    create_item,
)
from uuid import UUID
from sempy._utils._log import log


@log
def create_environment(
    environment: str,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a Fabric environment.

    This is a wrapper function for the following API: `Items - Create Environment <https://learn.microsoft.com/rest/api/fabric/environment/items/create-environment>`_.

    Parameters
    ----------
    environment: str
        Name of the environment.
    description : str, default=None
        A description of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    create_item(
        name=environment,
        description=description,
        type="Environment",
        workspace=workspace,
    )


@log
def list_environments(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the environments within a workspace.

    This is a wrapper function for the following API: `Items - List Environments <https://learn.microsoft.com/rest/api/fabric/environment/items/list-environments>`_.

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
        A pandas dataframe showing the environments within a workspace.
    """

    columns = {
        "Environment Name": "string",
        "Environment Id": "string",
        "Description": "string",
        "Publish State": "string",
        "Publish Target Version": "string",
        "Publish Start Time": "string",
        "Publish End Time": "string",
        "Spark Libraries State": "string",
        "Spark Settings State": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/environments",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            pub = v.get("properties", {}).get("publishDetails", {})
            rows.append(
                {
                    "Environment Name": v.get("displayName"),
                    "Environment Id": v.get("id"),
                    "Description": v.get("description"),
                    "Publish State": pub.get("state"),
                    "Publish Target Version": pub.get("targetVersion"),
                    "Publish Start Time": pub.get("startTime"),
                    "Publish End Time": pub.get("endTime"),
                    "Spark Libraries State": pub.get("componentPublishInfo", {})
                    .get("sparkLibraries", {})
                    .get("state"),
                    "Spark Settings State": pub.get("componentPublishInfo", {})
                    .get("sparkSettings", {})
                    .get("state"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def delete_environment(environment: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Deletes a Fabric environment.

    This is a wrapper function for the following API: `Items - Delete Environment <https://learn.microsoft.com/rest/api/fabric/environment/items/delete-environment>`_.

    Parameters
    ----------
    environment: str | uuid.UUID
        Name or ID of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=environment, type="Environment", workspace=workspace)


@log
def publish_environment(
    environment: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Publishes a Fabric environment.

    This is a wrapper function for the following API: `Spark Libraries - Publish Environment <https://learn.microsoft.com/rest/api/fabric/environment/spark-libraries/publish-environment>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    environment: str | uuid.UUID
        Name or ID of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = resolve_item_id(
        item=environment, type="Environment", workspace=workspace_id
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/environments/{item_id}/staging/publish",
        method="post",
        lro_return_status_code=True,
        status_codes=None,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{environment}' environment within the '{workspace_name}' workspace has been published."
    )


@log
def cancel_publish_environment(
    environment: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Trigger an environment publish cancellation.

    This is a wrapper function for the following API: `Items - Cancel Publish Environment <https://learn.microsoft.com/rest/api/fabric/environment/items/cancel-publish-environment>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    environment: str | uuid.UUID
        Name or ID of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = resolve_item_id(
        item=environment, type="Environment", workspace=workspace_id
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/environments/{item_id}/staging/cancelPublish",
        method="post",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The publish of the '{environment}' environment within the '{workspace_name}' workspace has been cancelled."
    )
