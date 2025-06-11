import pandas as pd
from sempy_labs._helper_functions import (
    _is_valid_uuid,
    _base_api,
    _update_dataframe_datatypes,
    _create_dataframe,
)
import sempy_labs._icons as icons
from uuid import UUID


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
        dfP = list_deployment_pipelines()
        dfP_filt = dfP[dfP["Deployment Pipeline Name"] == deployment_pipeline]
        if len(dfP_filt) == 0:
            raise ValueError(
                f"{icons.red_dot} The '{deployment_pipeline}' deployment pipeline is not valid."
            )
        return dfP_filt["Deployment Pipeline Id"].iloc[0]


def resolve_stage_id(deployment_pipeline_id: UUID, stage: str | UUID):

    dfPS = list_stages(
        deployment_pipeline=deployment_pipeline_id
    )

    if _is_valid_uuid(stage):
        dfPS_filt = dfPS[dfPS["Deployment Pipeline Stage Id"] == stage]
    else:
        dfPS_filt = dfPS[dfPS["Deployment Pipeline Stage Name"] == stage]
    if dfPS.empty:
        raise ValueError(
            f"{icons.red_dot} The '{stage}' stage does not exist within the '{deployment_pipeline_id}' deployment pipeline."
        )
    return dfPS_filt["Deployment Pipeline Stage Id"].iloc[0]


def list_deployment_pipelines() -> pd.DataFrame:
    """
    Shows a list of deployment pipelines the user can access.

    This is a wrapper function for the following API: `Deployment Pipelines - List Deployment Pipelines <https://learn.microsoft.com/rest/api/fabric/core/deployment-pipelines/list-deployment-pipelines>`_.

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
    )

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Deployment Pipeline Id": v.get("id"),
                "Deployment Pipeline Name": v.get("displayName"),
                "Description": v.get("description"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_stages(deployment_pipeline: str | UUID) -> pd.DataFrame:
    """
    Shows the specified deployment pipeline stages.

    This is a wrapper function for the following API: `Deployment Pipelines - List Deployment Pipeline Stages <https://learn.microsoft.com/rest/api/fabric/core/deployment-pipelines/list-deployment-pipeline-stages>`_.

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
    )

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Deployment Pipeline Stage Id": v.get("id"),
                "Deployment Pipeline Stage Name": v.get("displayName"),
                "Description": v.get("description"),
                "Order": v.get("order"),
                "Workspace Id": v.get("workspaceId"),
                "Workspace Name": v.get("workspaceName"),
                "Public": v.get("isPublic"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


def list_stage_items(
    deployment_pipeline: str | UUID,
    stage: str | UUID,
) -> pd.DataFrame:
    """
    Shows the supported items from the workspace assigned to the specified stage of the specified deployment pipeline.

    This is a wrapper function for the following API: `Deployment Pipelines - List Deployment Pipeline Stage Items <https://learn.microsoft.com/rest/api/fabric/core/deployment-pipelines/list-deployment-pipeline-stage-items>`_.

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
    )

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Deployment Pipeline Stage Item Id": v.get("itemId"),
                "Deployment Pipeline Stage Item Name": v.get("itemDisplayName"),
                "Item Type": v.get("itemType"),
                "Source Item Id": v.get("sourceItemId"),
                "Target Item Id": v.get("targetItemId"),
                "Last Deployment Time": v.get("lastDeploymentTime"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_role_assignments(
    deployment_pipeline: str | UUID,
) -> pd.DataFrame:
    """
    Shows the role assignments for the specified deployment pipeline.

    This is a wrapper function for the following API: `Deployment Pipelines - List Deployment Pipeline Role Assignments <https://learn.microsoft.com/rest/api/fabric/core/deployment-pipelines/list-deployment-pipeline-role-assignments>`_.

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

    dfs = []

    for r in responses:
        for v in r.get("value", []):
            principal = v.get("principal", {})
            new_data = {
                "Role": v.get("role"),
                "Principal Id": principal.get("id"),
                "Principal Type Name": principal.get("type"),
            }
            dfs.append(pd.DataFrame(new_data, index=[0]))

    if dfs:
        df = pd.concat(dfs, ignore_index=True)

    return df


def delete_deployment_pipeline(
    deployment_pipeline: str | UUID,
):
    """
    Deletes the specified deployment pipeline.

    This is a wrapper function for the following API: `Deployment Pipelines - Delete Deployment Pipeline <https://learn.microsoft.com/rest/api/fabric/core/deployment-pipelines/delete-deployment-pipeline>`_.

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


def list_operations(
    deployment_pipeline: str | UUID,
) -> pd.DataFrame:
    """
    Shows the operations for the specified deployment pipeline.

    This is a wrapper function for the following API: `Deployment Pipelines - List Deployment Pipeline Operations <https://learn.microsoft.com/rest/api/fabric/core/deployment-pipelines/list-deployment-pipeline-operations>`_.

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

    dfs = []
    for r in responses:
        for v in r.get("value", []):
            p = v.get("preDeploymentDiffInformation", {})
            new_data = {
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
            dfs.append(pd.DataFrame(new_data, index=[0]))

    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


def unassign_workspace_from_stage(
    deployment_pipeline: str | UUID,
    stage: str | UUID,
):
    """
    Unassigns the workspace from the specified stage of the specified deployment pipeline.

    This is a wrapper function for the following API: `Deployment Pipelines - Unassign Workspace From Stage <https://learn.microsoft.com/rest/api/fabric/core/deployment-pipelines/unassign-workspace-from-stage>`_.

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
