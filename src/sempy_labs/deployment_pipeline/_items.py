import pandas as pd
from sempy_labs._helper_functions import (
    _is_valid_uuid,
    _base_api,
    _update_dataframe_datatypes,
    _create_dataframe,
    resolve_workspace_id,
    resolve_capacity_id,
)
from sempy._utils._log import log
import sempy_labs._icons as icons
from uuid import UUID
from typing import List, Optional


@log
def resolve_deployment_pipeline_id(deployment_pipeline: str | UUID) -> UUID:
    """
    Obtains the Id for a given deployment pipeline.

    Parameters
    ----------
    deployment_pipeline : str | uuid.UUID
        The deployment pipeline name or ID.

    Returns
    -------
    uuid.UUID
        The deployment pipeline Id.
    """

    if _is_valid_uuid(deployment_pipeline):
        return deployment_pipeline
    else:
        dfP = list()
        dfP_filt = dfP[dfP["Deployment Pipeline Name"] == deployment_pipeline]
        if len(dfP_filt) == 0:
            raise ValueError(
                f"{icons.red_dot} The '{deployment_pipeline}' deployment pipeline is not valid."
            )
        return dfP_filt["Deployment Pipeline Id"].iloc[0]


@log
def resolve_stage_id(deployment_pipeline_id: UUID, stage: str | UUID):

    dfPS = list_deployment_pipeline_stages(deployment_pipeline=deployment_pipeline_id)

    if _is_valid_uuid(stage):
        dfPS_filt = dfPS[dfPS["Deployment Pipeline Stage Id"] == stage]
    else:
        dfPS_filt = dfPS[dfPS["Deployment Pipeline Stage Name"] == stage]
    if dfPS.empty:
        raise ValueError(
            f"{icons.red_dot} The '{stage}' stage does not exist within the '{deployment_pipeline_id}' deployment pipeline."
        )
    return dfPS_filt["Deployment Pipeline Stage Id"].iloc[0]


@log
def list_deployment_pipelines() -> pd.DataFrame:
    """
    Shows a list of deployment pipelines the user can access.

    This is a wrapper function for the following API: `Deployment Pipelines - List Deployment Pipelines <https://learn.microsoft.com/rest/api/fabric/core/deployment-pipelines/list-deployment-pipelines>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of deployment pipelines the user can access.
    """

    columns = {
        "Deployment Pipeline Id": "string",
        "Deployment Pipeline Name": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request="/v1/deploymentPipelines",
        status_codes=200,
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Deployment Pipeline Id": v.get("id"),
                    "Deployment Pipeline Name": v.get("displayName"),
                    "Description": v.get("description"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def list_deployment_pipeline_stages(deployment_pipeline: str | UUID) -> pd.DataFrame:
    """
    Shows the specified deployment pipeline stages.

    This is a wrapper function for the following API: `Deployment Pipelines - List Deployment Pipeline Stages <https://learn.microsoft.com/rest/api/fabric/core/deployment-pipelines/list-deployment-pipeline-stages>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    deployment_pipeline : str | uuid.UUID
        The deployment pipeline name or ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the specified deployment pipeline stages.
    """

    columns = {
        "Deployment Pipeline Stage Id": "string",
        "Deployment Pipeline Stage Name": "string",
        "Order": "int",
        "Description": "string",
        "Workspace Id": "string",
        "Workspace Name": "string",
        "Public": "bool",
    }
    df = _create_dataframe(columns=columns)

    deployment_pipeline_id = resolve_deployment_pipeline_id(
        deployment_pipeline=deployment_pipeline
    )

    responses = _base_api(
        request=f"/v1/deploymentPipelines/{deployment_pipeline_id}/stages",
        status_codes=200,
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Deployment Pipeline Stage Id": v.get("id"),
                    "Deployment Pipeline Stage Name": v.get("displayName"),
                    "Description": v.get("description"),
                    "Order": v.get("order"),
                    "Workspace Id": v.get("workspaceId"),
                    "Workspace Name": v.get("workspaceName"),
                    "Public": v.get("isPublic"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def list_deployment_pipeline_stage_items(
    deployment_pipeline: str | UUID,
    stage: str | UUID,
) -> pd.DataFrame:
    """
    Shows the supported items from the workspace assigned to the specified stage of the specified deployment pipeline.

    This is a wrapper function for the following API: `Deployment Pipelines - List Deployment Pipeline Stage Items <https://learn.microsoft.com/rest/api/fabric/core/deployment-pipelines/list-deployment-pipeline-stage-items>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    deployment_pipeline : str | uuid.UUID
        The deployment pipeline name or ID.
    stage : str | uuid.UUID
        The deployment pipeline stage name or ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the supported items from the workspace assigned to the specified stage of the specified deployment pipeline.
    """

    columns = {
        "Deployment Pipeline Stage Item Id": "string",
        "Deployment Pipeline Stage Item Name": "string",
        "Item Type": "string",
        "Source Item Id": "string",
        "Target Item Id": "string",
        "Last Deployment Time": "string",
    }
    df = _create_dataframe(columns=columns)

    deployment_pipeline_id = resolve_deployment_pipeline_id(
        deployment_pipeline=deployment_pipeline
    )

    stage_id = resolve_stage_id(deployment_pipeline_id, stage)

    responses = _base_api(
        request=f"/v1/deploymentPipelines/{deployment_pipeline_id}/stages/{stage_id}/items",
        status_codes=200,
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Deployment Pipeline Stage Item Id": v.get("itemId"),
                    "Deployment Pipeline Stage Item Name": v.get("itemDisplayName"),
                    "Item Type": v.get("itemType"),
                    "Source Item Id": v.get("sourceItemId"),
                    "Target Item Id": v.get("targetItemId"),
                    "Last Deployment Time": v.get("lastDeploymentTime"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def list_deployment_pipeline_role_assignments(
    deployment_pipeline: str | UUID,
) -> pd.DataFrame:
    """
    Shows the role assignments for the specified deployment pipeline.

    This is a wrapper function for the following API: `Deployment Pipelines - List Deployment Pipeline Role Assignments <https://learn.microsoft.com/rest/api/fabric/core/deployment-pipelines/list-deployment-pipeline-role-assignments>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    deployment_pipeline : str | uuid.UUID
        The deployment pipeline name or ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the role assignments for the specified deployment pipeline.
    """

    columns = {
        "Role": "string",
        "Principal Id": "string",
        "Principal Type": "string",
    }
    df = _create_dataframe(columns=columns)

    deployment_pipeline_id = resolve_deployment_pipeline_id(
        deployment_pipeline=deployment_pipeline
    )

    responses = _base_api(
        request=f"/v1/deploymentPipelines/{deployment_pipeline_id}/roleAssignments",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            principal = v.get("principal", {})
            rows.append(
                {
                    "Role": v.get("role"),
                    "Principal Id": principal.get("id"),
                    "Principal Type Name": principal.get("type"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def delete_deployment_pipeline(
    deployment_pipeline: str | UUID,
):
    """
    Deletes the specified deployment pipeline.

    This is a wrapper function for the following API: `Deployment Pipelines - Delete Deployment Pipeline <https://learn.microsoft.com/rest/api/fabric/core/deployment-pipelines/delete-deployment-pipeline>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    deployment_pipeline : str | uuid.UUID
        The deployment pipeline name or ID.
    """

    deployment_pipeline_id = resolve_deployment_pipeline_id(
        deployment_pipeline=deployment_pipeline
    )

    _base_api(
        request=f"/v1/deploymentPipelines/{deployment_pipeline_id}",
        method="delete",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{deployment_pipeline}' deployment pipeline has been deleted successfully."
    )


@log
def list_deployment_pipeline_operations(
    deployment_pipeline: str | UUID,
) -> pd.DataFrame:
    """
    Shows the operations for the specified deployment pipeline.

    This is a wrapper function for the following API: `Deployment Pipelines - List Deployment Pipeline Operations <https://learn.microsoft.com/rest/api/fabric/core/deployment-pipelines/list-deployment-pipeline-operations>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    deployment_pipeline : str | uuid.UUID
        The deployment pipeline name or ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the operations for the specified deployment pipeline.
    """

    columns = {
        "Operation Id": "string",
        "Type": "string",
        "Status": "string",
        "Last Updated Time": "string",
        "Execution Start Time": "datetime_coerce",
        "Execution End Time": "datetime_coerce",
        "Source Stage Id": "string",
        "Target Stage Id": "string",
        "Note": "string",
        "New Items Count": "int",
        "Different Items Count": "int",
        "No Difference Items Count": "int",
        "Performed By Id": "string",
        "Performed By Type": "string",
    }
    df = _create_dataframe(columns=columns)

    deployment_pipeline_id = resolve_deployment_pipeline_id(
        deployment_pipeline=deployment_pipeline
    )

    responses = _base_api(
        request=f"/v1/deploymentPipelines/{deployment_pipeline_id}/operations",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            p = v.get("preDeploymentDiffInformation", {})
            rows.append(
                {
                    "Operation Id": v.get("id"),
                    "Type": v.get("type"),
                    "Status": v.get("status"),
                    "Last Updated Time": v.get("lastUpdatedTime"),
                    "Execution Start Time": v.get("executionStartTime"),
                    "Execution End Time": v.get("executionEndTime"),
                    "Source Stage Id": v.get("sourceStageId"),
                    "Target Stage Id": v.get("targetStageId"),
                    "Note": v.get("note", {}).get("content"),
                    "New Items Count": p.get("newItemsCount"),
                    "Different Items Count": p.get("differentItemsCount"),
                    "No Difference Items Count": p.get("noDifferenceItemsCount"),
                    "Performed By Id": v.get("performedBy", {}).get("id"),
                    "Performed By Type": v.get("performedBy", {}).get("type"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def unassign_workspace_from_stage(
    deployment_pipeline: str | UUID,
    stage: str | UUID,
):
    """
    Unassigns the workspace from the specified stage of the specified deployment pipeline.

    This is a wrapper function for the following API: `Deployment Pipelines - Unassign Workspace From Stage <https://learn.microsoft.com/rest/api/fabric/core/deployment-pipelines/unassign-workspace-from-stage>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    deployment_pipeline : str | uuid.UUID
        The deployment pipeline name or ID.
    stage : str | uuid.UUID
        The deployment pipeline stage name or ID.
    """

    deployment_pipeline_id = resolve_deployment_pipeline_id(
        deployment_pipeline=deployment_pipeline
    )

    stage_id = resolve_stage_id(deployment_pipeline_id, stage)

    _base_api(
        request=f"/v1/deploymentPipelines/{deployment_pipeline_id}/stages/{stage_id}/unassignWorkspace",
        method="post",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The workspace has been unassigned from the '{stage}' stage of the '{deployment_pipeline}' deployment pipeline successfully."
    )


@log
def assign_workspace_to_stage(
    deployment_pipeline: str | UUID,
    stage: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Unassigns the workspace from the specified stage of the specified deployment pipeline.

    This is a wrapper function for the following API: `Deployment Pipelines - Assign Workspace To Stage <https://learn.microsoft.com/rest/api/fabric/core/deployment-pipelines/assign-workspace-to-stage>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    deployment_pipeline : str | uuid.UUID
        The deployment pipeline name or ID.
    stage : str | uuid.UUID
        The deployment pipeline stage name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    deployment_pipeline_id = resolve_deployment_pipeline_id(
        deployment_pipeline=deployment_pipeline
    )

    stage_id = resolve_stage_id(deployment_pipeline_id, stage)
    workspace_id = resolve_workspace_id(workspace=workspace)

    payload = {"workspaceId": workspace_id}

    _base_api(
        request=f"/v1/deploymentPipelines/{deployment_pipeline_id}/stages/{stage_id}/assignWorkspace",
        method="post",
        client="fabric_sp",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The workspace has been assigned to the '{stage}' stage of the '{deployment_pipeline}' deployment pipeline successfully."
    )


@log
def deploy_stage_content(
    deployment_pipeline: str | UUID,
    source_stage_id: UUID,
    target_stage_id: UUID,
    items: Optional[dict | List[dict]] = None,
    note: Optional[str] = None,
    allow_cross_region_deployment: Optional[bool] = False,
    capacity: Optional[str | UUID] = None,
    workspace_name: Optional[str] = None,
):
    """
    Deploys items from the specified stage of the specified deployment pipeline.

    This is a wrapper function for the following API: `Deployment Pipelines - Deploy Stage Content <https://learn.microsoft.com/rest/api/fabric/core/deployment-pipelines/deploy-stage-content>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    deployment_pipeline : str | uuid.UUID
        The deployment pipeline name or ID.
    source_stage_id : uuid.UUID
        The source deployment pipeline stage ID.
    target_stage_id : uuid.UUID
        The target deployment pipeline stage ID.
    items : dict | List[dict], default=None
        A list of items to deploy. Each item should be a dictionary with the following structure:
        {
            "itemId": "1a201f2a-d1d8-45c0-8c61-1676338517de",
            "itemType": "SemanticModel"
        }
        If None, all items will be deployed.
    note : str, default=None
        An optional note to include with the deployment.
    allow_cross_region_deployment : bool, default=False
        Indicates whether cross region deployment is enabled. True - enabled, False - disabled. Default value is False.
    capacity : str | uuid.UUID, default=None
        The capacity name or ID to use for the deployment operation if creating a new workspace. Required when deploying to a stage that has no assigned workspaces, otherwise it is ignored. The deployment will fail if the new workspace configuration details aren't provided when required.
    workspace_name : str, default=None
        The workspace name to use for the deployment operation if creating a new workspace. Required when deploying to a stage that has no assigned workspaces, otherwise it is ignored. The deployment will fail if the new workspace configuration details aren't provided when required.
    """

    deployment_pipeline_id = resolve_deployment_pipeline_id(
        deployment_pipeline=deployment_pipeline
    )

    payload = {
        "sourceStageId": source_stage_id,
        "targetStageId": target_stage_id,
    }
    if note:
        payload["note"] = note

    if items:
        if isinstance(items, dict):
            items = [items]

        if not isinstance(items, list):
            raise ValueError(
                f"{icons.red_dot} The 'items' parameter must be a list of dictionaries."
            )

        payload["items"] = items

    if allow_cross_region_deployment:
        payload["options"] = {"allowCrossRegionDeployment": True}

    if capacity and workspace_name:
        capacity_id = resolve_capacity_id(capacity)
        payload["createdWorkspaceDetails"] = {
            "capacityId": capacity_id,
            "name": workspace_name,
        }

    _base_api(
        request=f"/v1/deploymentPipelines/{deployment_pipeline_id}/deploy",
        method="post",
        payload=payload,
        status_codes=[200, 202],
        lro_return_status_code=True,
    )

    print(
        f"{icons.green_dot} The deployment from stage '{source_stage_id}' to stage '{target_stage_id}' in the '{deployment_pipeline}' deployment pipeline has been initiated successfully."
    )

    if capacity and workspace_name:
        print(
            f"{icons.info} A new workspace '{workspace_name}' will be created in the specified capacity for the deployment."
        )
