import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_lakehouse_name,
    resolve_lakehouse_id,
    resolve_workspace_name_and_id,
)
from typing import Optional, Union
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID


def create_shortcut_onelake(
    table_name: str,
    source_lakehouse: str,
    source_workspace: str,
    destination_lakehouse: str,
    destination_workspace: Optional[str] = None,
    shortcut_name: Optional[str] = None,
):
    """
    Creates a `shortcut <https://learn.microsoft.com/fabric/onelake/onelake-shortcuts>`_ to a delta table in OneLake.

    This is a wrapper function for the following API: `OneLake Shortcuts - Create Shortcut <https://learn.microsoft.com/rest/api/fabric/core/onelake-shortcuts/create-shortcut`_.

    Parameters
    ----------
    table_name : str
        The table name for which a shortcut will be created.
    source_lakehouse : str
        The Fabric lakehouse in which the table resides.
    source_workspace : str
        The name of the Fabric workspace in which the source lakehouse exists.
    destination_lakehouse : str
        The Fabric lakehouse in which the shortcut will be created.
    destination_workspace : str, default=None
        The name of the Fabric workspace in which the shortcut will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    shortcut_name : str, default=None
        The name of the shortcut 'table' to be created. This defaults to the 'table_name' parameter value.
    """

    sourceWorkspaceId = fabric.resolve_workspace_id(source_workspace)
    sourceLakehouseId = resolve_lakehouse_id(source_lakehouse, source_workspace)

    if destination_workspace is None:
        destination_workspace = source_workspace

    destinationWorkspaceId = fabric.resolve_workspace_id(destination_workspace)
    destinationLakehouseId = resolve_lakehouse_id(
        destination_lakehouse, destination_workspace
    )

    if shortcut_name is None:
        shortcut_name = table_name

    client = fabric.FabricRestClient()
    tablePath = f"Tables/{table_name}"

    request_body = {
        "path": "Tables",
        "name": shortcut_name.replace(" ", ""),
        "target": {
            "oneLake": {
                "workspaceId": sourceWorkspaceId,
                "itemId": sourceLakehouseId,
                "path": tablePath,
            }
        },
    }

    try:
        response = client.post(
            f"/v1/workspaces/{destinationWorkspaceId}/items/{destinationLakehouseId}/shortcuts",
            json=request_body,
        )
        if response.status_code == 201:
            print(
                f"{icons.green_dot} The shortcut '{shortcut_name}' was created in the '{destination_lakehouse}' lakehouse within"
                f" the '{destination_workspace} workspace. It is based on the '{table_name}' table in the '{source_lakehouse}' lakehouse within the '{source_workspace}' workspace."
            )
        else:
            print(response.status_code)
    except Exception as e:
        raise ValueError(
            f"{icons.red_dot} Failed to create a shortcut for the '{table_name}' table."
        ) from e


def create_shortcut_adls(
    shortcut_name: str,
    location: str,
    subpath: str,
    connection: Union[str, UUID],
    path: str = "Tables",
    lakehouse: Optional[str] = None,
    workspace: Optional[str] = None,
):
    """
    Creates a `shortcut <https://learn.microsoft.com/fabric/onelake/onelake-shortcuts>`_ to an ADLS Gen2 source.

    Parameters
    ----------
    shortcut_name : str
        The name of the shortcut.
    location : str
        Specifies the location of the target ADLS container. The URI must be in the format https://[account-name].dfs.core.windows.net where [account-name] is the name of the target ADLS account.
    subpath : str
        	
Specifies the container and subfolder within the ADLS account where the target folder is located. Must be of the format [container]/[subfolder] where [container] is the name of the container that holds the files and folders; [subfolder] is the name of the subfolder within the container (optional). For example: /mycontainer/mysubfolder.
    connection: Union[str, UUID]
        A string representing the connection that is bound with the shortcut. This can either be the connection Id or the connection name.
    path : str, default="Tables"
        A string representing the full path where the shortcut is created, including either "Files" or "Tables".
    lakehouse : str, default=None
        The Fabric lakehouse in which the shortcut will be created.
        Defaults to None which resolves to the default lakehouse attached to the notebook.
    workspace : str, default=None
        The name of the Fabric workspace in which the shortcut will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    _create_shortcut_base(
        shortcut_name=shortcut_name,
        location=location,
        subpath=subpath,
        source="adlsGen2",
        connection=connection,
        path=path,
        lakehouse=lakehouse,
        workspace=workspace,
    )


def create_shortcut_amazons3(
    shortcut_name: str,
    location: str,
    subpath: str,
    connection: Union[str, UUID],
    path: str = "Tables",
    lakehouse: Optional[str] = None,
    workspace: Optional[str] = None,
):
    """
    Creates a `shortcut <https://learn.microsoft.com/fabric/onelake/onelake-shortcuts>`_ to an Amazon S3 source.

    Parameters
    ----------
    shortcut_name : str
        The name of the shortcut.
    location : str
        HTTP URL that points to the target bucket in S3. The URL should be in the format https://[bucket-name].s3.[region-code].amazonaws.com, where "bucket-name" is the name of the S3 bucket you want to point to, and "region-code" is the code for the region where the bucket is located. For example: https://my-s3-bucket.s3.us-west-2.amazonaws.com.
    subpath : str
        Specifies a target folder or subfolder within the S3 bucket.
    connection: Union[str, UUID]
        A string representing the connection that is bound with the shortcut. This can either be the connection Id or the connection name.
    path : str, default="Tables"
        A string representing the full path where the shortcut is created, including either "Files" or "Tables".
    lakehouse : str, default=None
        The Fabric lakehouse in which the shortcut will be created.
        Defaults to None which resolves to the default lakehouse attached to the notebook.
    workspace : str, default=None
        The name of the Fabric workspace in which the shortcut will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    _create_shortcut_base(
        shortcut_name=shortcut_name,
        location=location,
        subpath=subpath,
        source="amazonS3",
        connection=connection,
        path=path,
        lakehouse=lakehouse,
        workspace=workspace,
    )


def create_shortcut_google_cloud(
    shortcut_name: str,
    location: str,
    subpath: str,
    connection: Union[str, UUID],
    path: str = "Tables",
    lakehouse: Optional[str] = None,
    workspace: Optional[str] = None,
):
    """
    Creates a `shortcut <https://learn.microsoft.com/fabric/onelake/onelake-shortcuts>`_ to a Google Cloud Storage source.

    Parameters
    ----------
    shortcut_name : str
        The name of the shortcut.
    location : str
        HTTP URL that points to the target bucket in GCS. The URL should be in the format https://[bucket-name].storage.googleapis.com, where [bucket-name] is the name of the bucket you want to point to. For example: https://my-gcs-bucket.storage.googleapis.com.
    subpath : str
        Specifies a target folder or subfolder within the GCS bucket. For example: /folder
    connection: Union[str, UUID]
        A string representing the connection that is bound with the shortcut. This can either be the connection Id or the connection name.
    path : str, default="Tables"
        A string representing the full path where the shortcut is created, including either "Files" or "Tables".
    lakehouse : str, default=None
        The Fabric lakehouse in which the shortcut will be created.
        Defaults to None which resolves to the default lakehouse attached to the notebook.
    workspace : str, default=None
        The name of the Fabric workspace in which the shortcut will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    _create_shortcut_base(
        shortcut_name=shortcut_name,
        location=location,
        subpath=subpath,
        source="googleCloudStorage",
        connection=connection,
        path=path,
        lakehouse=lakehouse,
        workspace=workspace,
    )


def create_shortcut_s3_compatible(
    shortcut_name: str,
    location: str,
    subpath: str,
    bucket: str,
    connection: Union[str, UUID],
    path: str = "Tables",
    lakehouse: Optional[str] = None,
    workspace: Optional[str] = None,
):
    """
    Creates a `shortcut <https://learn.microsoft.com/fabric/onelake/onelake-shortcuts>`_ to an S3 Compatible source.

    Parameters
    ----------
    shortcut_name : str
        The name of the shortcut.
    location : str
        HTTP URL of the S3 compatible endpoint. This endpoint must be able to receive ListBuckets S3 API calls. The URL must be in the non-bucket specific format; no bucket should be specified here. For example: https://s3endpoint.contoso.com.
    subpath : str
        Specifies a target folder or subfolder within the S3 compatible bucket. For example: /folder
    bucket : str
        Specifies the target bucket within the S3 compatible location.
    connection: Union[str, UUID]
        A string representing the connection that is bound with the shortcut. This can either be the connection Id or the connection name.
    path : str, default="Tables"
        A string representing the full path where the shortcut is created, including either "Files" or "Tables".
    lakehouse : str, default=None
        The Fabric lakehouse in which the shortcut will be created.
        Defaults to None which resolves to the default lakehouse attached to the notebook.
    workspace : str, default=None
        The name of the Fabric workspace in which the shortcut will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    _create_shortcut_base(
        shortcut_name=shortcut_name,
        location=location,
        subpath=subpath,
        source="s3Compatible",
        connection=connection,
        path=path,
        bucket=bucket,
        lakehouse=lakehouse,
        workspace=workspace,
    )


def _create_shortcut_base(
    shortcut_name: str,
    location: str,
    subpath: str,
    source: str,
    connection: Union[str, UUID],
    path: str = "Tables",
    bucket: Optional[str] = None,
    lakehouse: Optional[str] = None,
    workspace: Optional[str] = None,
):

    from sempy_labs._connections import list_connections

    source_titles = {"adlsGen2": "ADLS Gen2", "amazonS3": "Amazon S3"}
    sources = list(source_titles.keys())

    if source not in sources:
        raise ValueError(
            f"{icons.red_dot} The 'source' parameter must be one of these values: {sources}."
        )

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
    else:
        lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)
        lakehouse = resolve_lakehouse_name(lakehouse_id, workspace)

    client = fabric.FabricRestClient()
    shortcut_name_no_spaces = shortcut_name.replace(" ", "")

    # Validate connection
    dfC = list_connections()
    dfC_filt = dfC[dfC["Connection Id"] == connection]

    if len(dfC_filt) == 0:
        dfC_filt = dfC[dfC["Connection Name"] == connection]
        if len(dfC_filt) == 0:
            raise ValueError(
                f"{icons.red_dot} The '{connection}' connection does not exist."
            )
        connection_id = dfC_filt["Connection Id"].iloc[0]
    else:
        connection_id = dfC_filt["Connection Id"].iloc[0]

    request_body = {
        "path": path,
        "name": shortcut_name_no_spaces,
        "target": {
            source: {
                "location": location,
                "subpath": subpath,
                "connectionId": connection_id,
            }
        },
    }

    if bucket is not None:
        request_body['target'][source] = {
            "bucket": bucket
        }

    response = client.post(
        f"/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts",
        json=request_body,
    )

    if response.status_code != 201:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The shortcut '{shortcut_name_no_spaces}' was created in the '{lakehouse}' lakehouse within the '{workspace} workspace. It is based on the '{subpath}' table in '{source_titles[source]}'."
    )


def delete_shortcut(
    shortcut_name: str, lakehouse: Optional[str] = None, workspace: Optional[str] = None
):
    """
    Deletes a shortcut.

    This is a wrapper function for the following API: `OneLake Shortcuts - Delete Shortcut <https://learn.microsoft.com/rest/api/fabric/core/onelake-shortcuts/delete-shortcut`_.

    Parameters
    ----------
    shortcut_name : str
        The name of the shortcut.
    lakehouse : str, default=None
        The Fabric lakehouse name in which the shortcut resides.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str, default=None
        The name of the Fabric workspace in which lakehouse resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, workspace)
    else:
        lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)

    client = fabric.FabricRestClient()
    response = client.delete(
        f"/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts/Tables/{shortcut_name}"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{shortcut_name}' shortcut in the '{lakehouse}' within the '{workspace}' workspace has been deleted."
    )
