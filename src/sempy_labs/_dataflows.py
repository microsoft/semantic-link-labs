import pandas as pd
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _is_valid_uuid,
    _update_dataframe_datatypes,
    _base_api,
    _create_dataframe,
    resolve_workspace_name,
    resolve_workspace_id,
    _decode_b64,
    _conv_b64,
    get_jsonpath_value,
)
from typing import Optional, Tuple, List, Literal
import sempy_labs._icons as icons
from uuid import UUID
from jsonpath_ng.ext import parse
import json


def list_dataflows(workspace: Optional[str | UUID] = None):
    """
    Shows a list of all dataflows which exist within a workspace.

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

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Dataflow Id": "string",
        "Dataflow Name": "string",
        "Description": "string",
        "Configured By": "string",
        "Users": "string",
        "Generation": "string",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/dataflows", client="fabric_sp"
    )

    dfs = []
    for v in response.json().get("value", []):
        gen = v.get("generation")
        new_data = {
            "Dataflow Id": v.get("objectId"),
            "Dataflow Name": v.get("name"),
            "Description": "",
            "Configured By": v.get("configuredBy"),
            "Users": ", ".join(v.get("users", [])),
            "Generation": "Gen2" if gen == 2 else "Gen1",
        }
        dfs.append(pd.DataFrame(new_data, index=[0]))

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/dataflows",
        client="fabric_sp",
        uses_pagination=True,
    )
    for r in responses:
        for v in r.get("value", []):
            gen = v.get("generation")
            new_data = {
                "Dataflow Id": v.get("id"),
                "Dataflow Name": v.get("displayName"),
                "Description": v.get("description"),
                "Configured By": "",
                "Users": "",
                "Generation": "Gen2 CI/CD",
            }
            dfs.append(pd.DataFrame(new_data, index=[0]))

    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = list_dataflow_storage_accounts()
    df_filt = df[df["Dataflow Storage Account Name"] == dataflow_storage_account]

    if len(df_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{dataflow_storage_account}' does not exist."
        )

    dataflow_storage_id = df_filt["Dataflow Storage Account ID"].iloc[0]
    payload = {"dataflowStorageId": dataflow_storage_id}

    _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/AssignToDataflowStorage",
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The '{dataflow_storage_account}' dataflow storage account has been assigned to the '{workspace_name}' workspacce."
    )


def list_dataflow_storage_accounts() -> pd.DataFrame:
    """
    Shows the accessible dataflow storage accounts.

    This is a wrapper function for the following API: `Dataflow Storage Accounts - Get Dataflow Storage Accounts <https://learn.microsoft.com/rest/api/power-bi/dataflow-storage-accounts/get-dataflow-storage-accounts>`_.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the accessible dataflow storage accounts.
    """

    columns = {
        "Dataflow Storage Account ID": "string",
        "Dataflow Storage Account Name": "string",
        "Enabled": "bool",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(request="/v1.0/myorg/dataflowStorageAccounts")

    for v in response.json().get("value", []):
        new_data = {
            "Dataflow Storage Account ID": v.get("id"),
            "Dataflow Storage Account Name": v.get("name"),
            "Enabled": v.get("isEnabled"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


def list_upstream_dataflows(
    dataflow: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a list of upstream dataflows for the specified dataflow.

    This is a wrapper function for the following API: `Dataflows - Get Upstream Dataflows In Group <https://learn.microsoft.com/rest/api/power-bi/dataflows/get-upstream-dataflows-in-group>`_.

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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataflow_name, dataflow_id, dataflow_generation) = (
        _resolve_dataflow_name_and_id_and_generation(
            dataflow=dataflow, workspace=workspace_id
        )
    )

    columns = {
        "Dataflow Name": "string",
        "Dataflow Id": "string",
        "Workspace Name": "string",
        "Workspace Id": "string",
        "Upstream Dataflow Name": "string",
        "Upstream Dataflow Id": "string",
        "Upstream Workspace Name": "string",
        "Upstream Workspace Id": "string",
    }
    df = _create_dataframe(columns=columns)

    def collect_upstreams(dataflow_id, dataflow_name, workspace_id, workspace_name):
        response = _base_api(
            request=f"/v1.0/myorg/groups/{workspace_id}/dataflows/{dataflow_id}/upstreamDataflows"
        )

        values = response.json().get("value", [])
        for v in values:
            tgt_dataflow_id = v.get("targetDataflowId")
            tgt_workspace_id = v.get("groupId")
            tgt_workspace_name = resolve_workspace_name(workspace_id=tgt_workspace_id)
            (tgt_dataflow_name, _, _) = _resolve_dataflow_name_and_id_and_generation(
                dataflow=tgt_dataflow_id, workspace=tgt_workspace_id
            )

            df.loc[len(df)] = {
                "Dataflow Name": dataflow_name,
                "Dataflow Id": dataflow_id,
                "Workspace Name": workspace_name,
                "Workspace Id": workspace_id,
                "Upstream Dataflow Name": tgt_dataflow_name,
                "Upstream Dataflow Id": tgt_dataflow_id,
                "Upstream Workspace Name": tgt_workspace_name,
                "Upstream Workspace Id": tgt_workspace_id,
            }

            collect_upstreams(
                tgt_dataflow_id,
                tgt_dataflow_name,
                tgt_workspace_id,
                tgt_workspace_name,
            )

    collect_upstreams(dataflow_id, dataflow_name, workspace_id, workspace_name)

    return df


def _resolve_dataflow_name_and_id_and_generation(
    dataflow: str | UUID, workspace: Optional[str | UUID] = None
) -> Tuple[str, UUID, str]:

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfD = list_dataflows(workspace=workspace_id)

    if _is_valid_uuid(dataflow):
        dfD_filt = dfD[dfD["Dataflow Id"] == dataflow]
    else:
        dfD_filt = dfD[dfD["Dataflow Name"] == dataflow]

    if dfD_filt.empty:
        raise ValueError(
            f"{icons.red_dot} The '{dataflow}' dataflow does not exist within the '{workspace_name}' workspace."
        )

    dataflow_id = dfD_filt["Dataflow Id"].iloc[0]
    dataflow_name = dfD_filt["Dataflow Name"].iloc[0]
    dataflow_generation = dfD_filt["Generation"].iloc[0]

    return (dataflow_name, dataflow_id, dataflow_generation)


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

    workspace_id = resolve_workspace_id(workspace)

    (dataflow_name, dataflow_id, dataflow_generation) = (
        _resolve_dataflow_name_and_id_and_generation(
            dataflow=dataflow, workspace=workspace_id
        )
    )

    if dataflow_generation == "Gen2 CI/CD":
        result = _base_api(
            request=f"/v1/workspaces/{workspace_id}/items/{dataflow_id}/getDefinition",
            client="fabric_sp",
            method="post",
            lro_return_json=True,
            status_codes=[200, 202],
        )

        if decode:
            # Decode the payload from base64
            definition = {"definition": {"parts": []}}

            for part in result.get("definition", {}).get("parts", []):
                path = part.get("path")
                payload = part.get("payload")
                decoded_payload = _decode_b64(payload)
                definition["definition"]["parts"].append(
                    {"path": path, "payload": decoded_payload}
                )
            return definition
        else:
            return result
    else:
        result = _base_api(
            request=f"/v1.0/myorg/groups/{workspace_id}/dataflows/{dataflow_id}",
            client="fabric_sp",
            method="get",
        ).json()

        return result


def upgrade_dataflow(
    dataflow: str | UUID,
    workspace: Optional[str | UUID] = None,
    new_dataflow_name: Optional[str] = None,
    new_dataflow_workspace: Optional[str | UUID] = None,
):
    """
    Creates a Dataflow Gen2 CI/CD item based on the mashup definition from an existing Gen1/Gen2 dataflow.

    Parameters
    ----------
    dataflow : str | uuid.UUID
        The name or ID of the dataflow.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    new_dataflow_name: str, default=None
        Name of the new dataflow.
    new_dataflow_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID of the dataflow to be created.
        Defaults to None which resolves to the existing workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    # Resolve the workspace name and ID
    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    # Resolve the dataflow name and ID
    (dataflow_name, dataflow_id, dataflow_generation) = (
        _resolve_dataflow_name_and_id_and_generation(dataflow, workspace_id)
    )

    if dataflow_generation == "Gen2 CI/CD":
        # Print an error message that the dataflow is already a native Fabric item
        print(
            f"{icons.info} The dataflow '{dataflow_name}' is already a Fabric native Dataflow Gen2 item. No changes made."
        )
        return

    (new_dataflow_workspace, new_dataflow_workspace_id) = resolve_workspace_name_and_id(
        new_dataflow_workspace
    )

    # If no new dataflow name is provided, use the existing dataflow name
    if not new_dataflow_name:
        new_dataflow_name = dataflow_name

    # Get dataflow definition
    definition = get_dataflow_definition(dataflow, workspace_id)

    # Check for linked table references
    matches = (
        parse("$['pbi:mashup'].connectionOverrides[*].kind").find(definition) or []
    )
    if any(match.value in {"PowerPlatformDataflows", "PowerBI"} for match in matches):
        print(
            f"""{icons.red_dot} The dataflow '{dataflow_name}' contains a linked table reference to an existing dataflow as a connection source and will not be upgraded. No changes were made.
    - To track the upstream lineage of linked tables across dataflows use the list_upstream_dataflows function.
    - To automatically remove the tables and upgrade the existing dataflow use the upgrade_powerbippdf_dataflow function."""
        )
        return

    description = get_jsonpath_value(data=definition, path="$.description")

    payload = {
        "displayName": new_dataflow_name,
    }
    if description:
        payload["description"] = description

    # Query Groups
    matches = parse("$.annotations[?(@.name=='pbi:QueryGroups')].value").find(
        definition
    )
    query_groups_value = json.loads(matches[0].value) if matches else []

    # Prepare the dataflow definition
    query_metadata = {
        "formatVersion": "202502",
        "computeEngineSettings": {},  # How to set this?
        "name": new_dataflow_name,
        "queryGroups": query_groups_value,
        "documentLocale": get_jsonpath_value(data=definition, path="$.culture"),
        "queriesMetadata": get_jsonpath_value(
            data=definition, path="$['pbi:mashup'].queriesMetadata"
        ),
        "fastCombine": get_jsonpath_value(
            data=definition, path="$['pbi:mashup'].fastCombine", default=False
        ),
        "allowNativeQueries": get_jsonpath_value(
            data=definition, path="$['pbi:mashup'].allowNativeQueries", default=False
        ),
        # "connections": [],
    }

    mashup_doc = get_jsonpath_value(data=definition, path="$['pbi:mashup'].document")

    # Add the dataflow definition to the payload
    new_definition = {
        "parts": [
            {
                "path": "queryMetadata.json",
                "payload": _conv_b64(query_metadata),
                "payloadType": "InlineBase64",
            },
            {
                "path": "mashup.pq",
                "payload": _conv_b64(mashup_doc, json_dumps=False),
                "payloadType": "InlineBase64",
            },
        ]
    }

    create_dataflow(
        name=new_dataflow_name,
        workspace=new_dataflow_workspace,
        definition=new_definition,
    )


def create_dataflow(
    name: str,
    workspace: Optional[str | UUID] = None,
    description: Optional[str] = None,
    definition: Optional[dict] = None,
):
    """
    Creates a native Fabric Dataflow Gen2 CI/CD item.

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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {
        "displayName": name,
    }
    if description:
        payload["description"] = description

    if definition:
        payload["definition"] = definition

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/dataflows",
        method="post",
        payload=payload,
        client="fabric_sp",
        lro_return_json=True,
        status_codes=[201, 202],
    )

    print(
        f"{icons.green_dot} The dataflow '{name}' has been created within the '{workspace_name}' workspace."
    )


def run_dataflow(
    dataflow: str | UUID,
    workspace: Optional[str | UUID] = None,
    job_type: Literal["Execute", "ApplyChanges"] = "Execute",
    parameters: Optional[List[dict]] = None,
):
    """
    Executes a dataflow.

    This is a wrapper function for the following APIs: `Background Jobs - Run On Demand Execute <https://learn.microsoft.com/rest/api/fabric/dataflow/background-jobs/run-on-demand-execute>`_ and `Background Jobs - Run On Demand Apply Changes <https://learn.microsoft.com/rest/api/fabric/dataflow/background-jobs/run-on-demand-apply-changes>`_.

    Parameters
    ----------
    dataflow : str | uuid.UUID
        The name or ID of the dataflow.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    job_type : Literal["Execute", "ApplyChanges"], default="Execute"
        The type of job to run. Can be either "Execute" or "ApplyChanges".
    parameters : List[dict], default=None
        A list of parameters to pass to the dataflow. Example:
        [
            {
                "parameterName": "OrderKey",
                "type": "Automatic",
                "value": 25
            },
            {
                "parameterName": "Threshold",
                "type": "Automatic",
                "value": "start"
            }
        ]
    """
    if job_type not in ["Execute", "ApplyChanges"]:
        raise ValueError(
            f"{icons.red_dot} The job_type parameter must be either 'Execute' or 'ApplyChanges'."
        )
    if job_type == "ApplyChanges" and parameters:
        print(
            f"The job type is set to '{job_type}'. Parameters are not accepted for this job type."
        )
        return

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    (dataflow_name, dataflow_id, generation) = (
        _resolve_dataflow_name_and_id_and_generation(dataflow, workspace_id)
    )

    if generation != "Gen2 CI/CD":
        print(
            f"{icons.info} The dataflow '{dataflow_name}' is not a Fabric Dataflow Gen2 CI/CD item. This function only supports Dataflow Gen2 CI/CD."
        )
        return

    payload = None
    if parameters:
        payload = {
            "executionData": {
                "executeOption": "ApplyChangesIfNeeded",
                "parameters": parameters,
            }
        }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/dataflows/{dataflow_id}/jobs/instances?jobType={job_type}",
        method="post",
        payload=payload,
        lro_return_json=True,
        status_codes=[200, 202],
    )

    print(
        f"{icons.green_dot} The dataflow '{dataflow_name}' has been run within the '{workspace_name}' workspace."
    )
