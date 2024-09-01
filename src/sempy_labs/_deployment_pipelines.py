import sempy.fabric as fabric
import pandas as pd
from sempy_labs._helper_functions import (
    pagination,
)
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException


def list_deployment_pipelines() -> pd.DataFrame:

    df = pd.DataFrame(
        columns=["Deployment Pipeline ID", "Deployment Pipeline Name", "Description"]
    )

    client = fabric.FabricRestClient()
    response = client.get("/v1/deploymentPipelines")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Deployment Pipeline ID": v.get("id"),
                "Deployment Pipeline Name": v.get("displayName"),
                "Description": v.get("description"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_deployment_pipeline_stages(deployment_pipeline: str) -> pd.DataFrame:

    from sempy_labs._helper_functions import resolve_deployment_pipeline_id

    df = pd.DataFrame(
        columns=[
            "Deployment Pipeline Stage ID",
            "Deployment Pipeline Stage Name",
            "Order",
            "Description",
            "Workspace ID",
            "Workspace Name",
            "Public",
        ]
    )

    deployment_pipeline_id = resolve_deployment_pipeline_id(
        deployment_pipeline=deployment_pipeline
    )
    client = fabric.FabricRestClient()
    response = client.get(f"/v1/deploymentPipelines/{deployment_pipeline_id}/stages")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Deployment Pipeline Stage ID": v["id"],
                "Deployment Pipeline Stage Name": v["displayName"],
                "Description": v["description"],
                "Order": v["order"],
                "Workspace ID": v["workspaceId"],
                "Workspace Name": v["workspaceName"],
                "Public": v["isPublic"],
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df["Order"] = df["Order"].astype(int)
    df["Public"] = df["Public"].astype(bool)

    return df


def list_deployment_pipeline_stage_items(
    deployment_pipeline: str, stage_name: str
) -> pd.DataFrame:

    df = pd.DataFrame(
        columns=[
            "Deployment Pipeline Stage Item ID",
            "Deployment Pipeline Stage Item Name",
            "Item Type",
            "Source Item ID",
            "Target Item ID",
            "Last Deployment Time",
        ]
    )

    deployment_pipeline_id = resolve_deployment_pipeline_id(
        deployment_pipeline=deployment_pipeline
    )
    dfPS = list_deployment_pipeline_stages(deployment_pipeline=deployment_pipeline)
    dfPS_filt = dfPS[dfPS["Deployment Pipeline Stage Name"] == stage_name]

    if len(dfPS_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{stage_name}' stage does not exist within the '{deployment_pipeline}' deployment pipeline."
        )
    stage_id = dfPS_filt["Deployment Pipeline Stage ID"].iloc[0]

    client = fabric.FabricRestClient()
    response = client.get(
        f"/v1/deploymentPipelines/{deployment_pipeline_id}/stages/{stage_id}/items"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Deployment Pipeline Stage Item ID": v.get("itemId"),
                "Deployment Pipeline Stage Item Name": v.get("itemDisplayName"),
                "Item Type": v.get("itemType"),
                "Source Item ID": v.get("sourceItemId"),
                "Target Item ID": v.get("targetItemId"),
                "Last Deployment Time": v.get("lastDeploymentTime"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df["Last Deployment Time"] = pd.to_datetime(df["Last Deployment Time"])

    return df
