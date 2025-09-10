import pandas as pd
from sempy_labs._helper_functions import (
    _is_valid_uuid,
    _base_api,
    _update_dataframe_datatypes,
    _create_dataframe,
)
import sempy_labs._icons as icons
from uuid import UUID
from sempy._utils._log import log


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
        df = pd.DataFrame(rows, columns=columns.keys())

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

    from sempy_labs._helper_functions import resolve_deployment_pipeline_id

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
        _update_dataframe_datatypes(df, columns)

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

    from sempy_labs._helper_functions import resolve_deployment_pipeline_id

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

    def resolve_deployment_pipeline_stage_id(
        deployment_pipeline_id: UUID, stage: str | UUID
    ):

        dfPS = list_deployment_pipeline_stages(
            deployment_pipeline=deployment_pipeline_id
        )

        if _is_valid_uuid(stage):
            dfPS_filt = dfPS[dfPS["Deployment Pipeline Stage Id"] == stage]
        else:
            dfPS_filt = dfPS[dfPS["Deployment Pipeline Stage Name"] == stage]
        if dfPS.empty:
            raise ValueError(
                f"{icons.red_dot} The '{stage}' stage does not exist within the '{deployment_pipeline}' deployment pipeline."
            )
        return dfPS_filt["Deployment Pipeline Stage Id"].iloc[0]

    stage_id = resolve_deployment_pipeline_stage_id(deployment_pipeline_id, stage)

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
