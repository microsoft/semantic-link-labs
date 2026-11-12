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
    resolve_item_id,
)
from typing import Optional, Tuple
import sempy_labs._icons as icons
from uuid import UUID
from sempy._utils._log import log


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

    rows = []
    for v in response.json().get("value", []):
        gen = v.get("generation")
        rows.append(
            {
                "Dataflow Id": v.get("objectId"),
                "Dataflow Name": v.get("name"),
                "Description": "",
                "Configured By": v.get("configuredBy"),
                "Users": ", ".join(v.get("users", [])),
                "Generation": "Gen2" if gen == 2 else "Gen1",
            }
        )

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/dataflows",
        client="fabric_sp",
        uses_pagination=True,
    )
    for r in responses:
        for v in r.get("value", []):
            gen = v.get("generation")
            rows.append(
                {
                    "Dataflow Id": v.get("id"),
                    "Dataflow Name": v.get("displayName"),
                    "Description": v.get("description"),
                    "Configured By": "",
                    "Users": "",
                    "Generation": "Gen2 CI/CD",
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


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

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)

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

    columns = {
        "Dataflow Storage Account ID": "string",
        "Dataflow Storage Account Name": "string",
        "Enabled": "bool",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(request="/v1.0/myorg/dataflowStorageAccounts")

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
                "Dataflow Storage Account ID": v.get("id"),
                "Dataflow Storage Account Name": v.get("name"),
                "Enabled": v.get("isEnabled"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


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

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    dataflow_name, dataflow_id, dataflow_generation = (
        resolve_dataflow_name_and_id_and_generation(
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
            request=f"/v1.0/myorg/groups/{workspace_id}/dataflows/{dataflow_id}/upstreamDataflows",
            client="fabric_sp",
        )

        values = response.json().get("value", [])
        for v in values:
            tgt_dataflow_id = v.get("targetDataflowId")
            tgt_workspace_id = v.get("groupId")
            tgt_workspace_name = resolve_workspace_name(workspace_id=tgt_workspace_id)
            tgt_dataflow_name, _, _ = resolve_dataflow_name_and_id_and_generation(
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


@log
def resolve_dataflow_name_and_id_and_generation(
    dataflow: str | UUID, workspace: Optional[str | UUID] = None
) -> Tuple[str, UUID, str]:

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)

    dfD = list_dataflows(workspace=workspace_id)

    if _is_valid_uuid(dataflow):
        dfD_filt = dfD[dfD["Dataflow Id"] == dataflow]
        if dfD_filt.empty:
            dfD_filt = dfD[dfD["Dataflow Name"] == dataflow]
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

    workspace_id = resolve_workspace_id(workspace)

    dataflow_name, dataflow_id, dataflow_generation = (
        resolve_dataflow_name_and_id_and_generation(
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
        ).json()

        return result


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

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)

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

    workspace_id = resolve_workspace_id(workspace)
    dataflow_id = resolve_item_id(
        item=dataflow, type="Dataflow", workspace=workspace_id
    )
    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/dataflows/{dataflow_id}/parameters",
        client="fabric_sp",
        uses_pagination=True,
    )

    columns = {
        "Parameter Name": "string",
        "Is Required": "bool",
        "Description": "string",
        "Parameter Type": "string",
        "Default Value": "string",
    }

    df = _create_dataframe(columns=columns)
    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Parameter Name": v.get("name"),
                    "Is Required": v.get("isRequired"),
                    "Description": v.get("description"),
                    "Parameter Type": v.get("type"),
                    "Default Value": v.get("defaultValue"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df
