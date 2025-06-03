import pandas as pd
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _is_valid_uuid,
    lro,
    pagination,
    _conv_b64,
    _decode_b64,
)
from typing import Optional, Tuple
import sempy_labs._icons as icons
import json
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID

def list_dataflows(workspace: Optional[str] = None):
    """
    Shows a list of all dataflows which exist within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the dataflows which exist within a workspace.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    # Power BI Dataflow Gen1 and Fabric Dataflow Gen2
    client = fabric.PowerBIRestClient()
    response = client.get(f"/v1.0/myorg/groups/{workspace_id}/dataflows")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(
        columns=["Dataflow Id", "Dataflow Name", "Dataflow Description", "Configured By", "Generation"]
    )

    for v in response.json().get("value", []):
        
        if v.get("generation") == 2:
            generation = 'FabricDataflow'
        else:
            generation = 'PowerBIPPDF'

        new_data = {
            "Dataflow Id": v.get("objectId"),
            "Dataflow Name": v.get("name"),
            "Dataflow Description": v.get("description"),
            "Configured By": v.get("configuredBy"),
            "Generation": generation
        }
        df = pd.concat(
            [df, pd.DataFrame([new_data])],
            ignore_index=True,
        )

    # Fabric Dataflow Gen2 native
    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/dataflows")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Dataflow Id": v.get("id"),
                "Dataflow Name": v.get("displayName"),
                "Dataflow Description": v.get("description"),
                "Generation": "FabricDataflowNative"
            }
            df = pd.concat(
                [df, pd.DataFrame([new_data])],
                ignore_index=True,
        )

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
    (dataflow_name, dataflow_id, dataflow_generation) = _resolve_dataflow_name_id_and_generation(
        dataflow=dataflow, workspace=workspace_id
    )

    df = pd.DataFrame(
        columns=["Dataflow Name", "Dataflow Id", "Workspace Name", "Workspace Id", "Upstream Dataflow Name", "Upstream Dataflow Id", "Upstream Workspace Name", "Upstream Workspace Id"]
    )

    def collect_upstreams(dataflow_id, dataflow_name, workspace_id, workspace_name):
        client = fabric.PowerBIRestClient()
        response = client.get(
            f"/v1.0/myorg/groups/{workspace_id}/dataflows/{dataflow_id}/upstreamDataflows"
        )

        values = response.json().get("value", [])
        for v in values:
            tgt_dataflow_id = v.get("targetDataflowId")
            tgt_workspace_id = v.get("groupId")
                # Resolve the workspace name and ID
            (tgt_workspace_name, workspace_id) = resolve_workspace_name_and_id(tgt_workspace_id)
            (tgt_dataflow_name, _ , tgt_dataflow_generation) = _resolve_dataflow_name_id_and_generation(
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

def _resolve_dataflow_name_id_and_generation(
    dataflow: str | UUID, workspace: Optional[str] = None
) -> Tuple[str, UUID]:

    if workspace is None:
        workspace = fabric.resolve_workspace_name(workspace)

    dfD = list_dataflows(workspace = workspace)

    if _is_valid_uuid(dataflow):
        dfD_filt = dfD[dfD["Dataflow Id"] == dataflow]
    else:
        dfD_filt = dfD[dfD["Dataflow Name"] == dataflow]

    if len(dfD_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{dataflow}' dataflow does not exist within the '{workspace}' workspace."
        )

    dataflow_id = dfD_filt["Dataflow Id"].iloc[0]
    dataflow_name = dfD_filt["Dataflow Name"].iloc[0]
    dataflow_generation = dfD_filt["Generation"].iloc[0]

    return dataflow_name, dataflow_id, dataflow_generation

def get_dataflow_definition(
    dataflow: str | UUID, workspace: Optional[str] = None, decode: bool = True,
) -> pd.DataFrame:
    """
    Obtains the definition of a dataflow.

    This is a wrapper function for the following API: `Dataflows - Get Dataflow <https://learn.microsoft.com/rest/api/power-bi/dataflows/get-dataflow>`_.

    Parameters
    ----------
    dataflow : str | UUID
        The name or ID of the dataflow.
    workspace : str, optional
        The Fabric workspace name. 
        Defaults to None, which resolves to the workspace of the attached lakehouse
        or if no lakehouse is attached, resolves to the workspace of the notebook.
    decode : bool, optional
        If True, decodes the dataflow definition file into JSON format.
        If False, obtains the dataflow definition file
        as a pandas DataFrame. Defaults to True.

    Returns
    -------
    dict or pandas.DataFrame
        A pandas DataFrame showing the dataflow within a workspace or a dictionary with the dataflow definition.
    """

    workspace = fabric.resolve_workspace_name(workspace)
    workspace_id = fabric.resolve_workspace_id(workspace)

    dataflow_name, item_id, dataflow_generation = _resolve_dataflow_name_id_and_generation(dataflow=dataflow, workspace=workspace)

    if dataflow_generation == "FabricDataflowNative":
        # Fabric Dataflow Gen2 native
        client = fabric.FabricRestClient()
        response = client.post(
            f"/v1/workspaces/{workspace_id}/items/{item_id}/getDefinition"
        )

        format = "getDefinition"
    else:
        # Power BI Dataflow Gen1 and Fabric Dataflow Gen2
        client = fabric.PowerBIRestClient()
        response = client.get(
            f"v1.0/myorg/groups/{workspace_id}/dataflows/{item_id}"
        )

        format = "manifest.json"
    
    if response.status_code != 200:
        raise FabricHTTPException(response)
    
    result = lro(client, response).json()
        
    if format == 'getDefinition' and not decode:
        df = pd.json_normalize(result["definition"]["parts"])
        
        # Decode the payload column from base64
        df['payload'] = df['payload'].apply(_decode_b64)

        # Drop the payloadType column
        df = df.drop(columns=['payloadType'])
        return df
    elif format == 'manifest.json' and not decode:
            df = pd.json_normalize(result)
            return df
    else:
        return result

def upgrade_powerbippdf_dataflow(
    dataflow: str | UUID,
    workspace: Optional[str] = None,
):
    """
    Creates a native Dataflow Gen2 item based on the definition from an existing Power BI or Power Platform dataflow.

    Parameters
    ----------
    dataflow_name : str | UUID
        The name or ID of the dataflow.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    (dataflow_name, item_id, dataflow_generation) = _resolve_dataflow_name_id_and_generation(dataflow=dataflow, workspace=workspace)

    # Prepare the header for the new dataflow type
    request_header = {
        'content-type': 'application/json',
        'x-ms-host-context-type': 'PowerBIPPDF',
    }

    client = fabric.FabricRestClient()
    response = client.post(f"v1.0/myorg/groups/{workspace_id}/dataflows/{item_id}/convertToDataflowDefinition", headers=request_header)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{dataflow_name}' has been upgraded within the '{workspace}' workspace."
    )

def upgrade_dataflow(
    dataflow: str | UUID,
    workspace: Optional[str] = None,
    new_dataflow_name: Optional[str] = None,
    new_dataflow_workspace: Optional[str] = None,
):
    """
    Creates a native Fabric Dataflow Gen2 item based on the mashup definition from an existing dataflow.

    Parameters
    ----------
    dataflow : str | UUID
        The name or ID of the dataflow.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    new_dataflow_name: str, default=None
        Name of the new dataflow.
    new_dataflow_workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the existing workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    # Resolve the workspace name and ID
    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    # Resolve the dataflow name and ID
    (dataflow_name, dataflow_id, dataflow_generation) = _resolve_dataflow_name_id_and_generation(dataflow, workspace_id)

    # If a new workspace is provided, resolve its name and ID
    if new_dataflow_workspace is not None:
        (new_dataflow_workspace, new_dataflow_workspace_id) = resolve_workspace_name_and_id(new_dataflow_workspace)
    else:
        new_dataflow_workspace = workspace
        new_dataflow_workspace_id = workspace_id

    # If no new dataflow name is provided, use the existing dataflow name
    if new_dataflow_name is None:
        new_dataflow_name = dataflow_name

    # Get dataflow definition
    definition = get_dataflow_definition(dataflow, workspace_id, True)

    # Exit with no changes made if the dataflow is already a native Fabric item 
    if dataflow_generation == 'FabricDataflowNative':
        # Print an error message that the dataflow is already a native Fabric item
        print(
            f"{icons.yellow_dot} The dataflow '{dataflow_name}' is already a Fabric native Dataflow Gen2 item. No changes made."
        )
        return
    
    # Exit with no changes made if the dataflow is already a native Fabric item 
    connectionOverrides = None
    if 'connectionOverrides' in definition['pbi:mashup']:
        for path in definition['pbi:mashup']['connectionOverrides']:
            if path['kind'] in {'PowerPlatformDataflows','PowerBI'}:
                print(
                    f"""{icons.red_dot} The dataflow '{dataflow_name}' contains a linked table reference to an existing dataflow as a connection source and will not be upgraded. No changes were made.
    - To track the upstream lineage of linked tables across dataflows use the list_upstream_dataflows function.
    - To automatically remove the tables and upgrade the existing dataflow use the upgrade_powerbippdf_dataflow function."""
                )
                return
    
    # Extract the description value if exists
    description = None
    if 'description' in definition:
        description = definition['description']

    # Prepare the payload displayName, descriptiona and type
    payload = {
        "displayName": new_dataflow_name,
        "description": description,
        "type": "Dataflow",
    }

    # Extract the value of pbi:QueryGroups if exists
    query_groups_value = None
    if 'annotations' in definition:
        for annotation in definition['annotations']:
            if annotation['name'] == 'pbi:QueryGroups':
                query_groups_value = json.loads(annotation['value'])

    # Connection properties
    connect_definition = {
        "gatewayObjectId": None,
        "connectionOverrides": None, # definition['pbi:mashup]['connectionOverrides']
        "trustedConnections": None,
        "useHostConnectionProvider": False,
    }

    # Prepare the dataflow definition
    new_dataflow_definition = {
             "mashupName": definition['name'],
             "mashupDocument": definition['pbi:mashup']['document'],
             "queryGroups": query_groups_value,
             "documentLocale": definition['culture'],
             "queriesMetadata": definition['pbi:mashup']['queriesMetadata'],
             "fastCombine": definition['pbi:mashup']['fastCombine'],
             "allowNativeQueries": definition['pbi:mashup']['allowNativeQueries'],
             # "allowedModules": None,
             # "skipAutomaticTypeAndHeaderDetection": False,
             # "disableAutoAnonymousConnectionUpsert": None,
             # "defaultOutputDestinationConfiguration": None,
             "hostProperties": {
                 "DataflowRefreshOutputFileFormat": "Parquet",
                 "EnableDateTimeFieldsForStaging": True,
                 "EnablePublishWithoutLoadedQueries": True
             }
     }

    additional_properties = {}
    if dataflow_generation == 'PowerBIPPDF':
        additional_properties = {}
    if dataflow_generation == 'FabricDataflow':
        additional_properties = {
            "ppdf:fastCopy": definition['ppdf:fastCopy']
        }

    dataflow_definition = {
         "editingSessionMashup": new_dataflow_definition | additional_properties
    }

    # Convert the dataflow definition to Base64
    dataflow_def_payload_conv = _conv_b64(dataflow_definition)

    # Add the dataflow definition to the payload
    payload["definition"] = {
         "parts": [
             {
                 "path": "dataflow-content.json",
                 "payload": dataflow_def_payload_conv,
                 "payloadType": "InlineBase64",
             }
         ]
     }

    # Create a FabricRestClient instance
    client = fabric.FabricRestClient()
    
    # Post the payload to create the new dataflow
    response = client.post(f"/v1/workspaces/{new_dataflow_workspace_id}/items", json=payload)

    # Raise an exception if the response status code is not 201
    if response.status_code != 201:
         raise FabricHTTPException(response)

    # Print a success message
    print(
        f"{icons.green_dot} The dataflow '{new_dataflow_name}' has been created within the '{new_dataflow_workspace}' workspace."
    )
