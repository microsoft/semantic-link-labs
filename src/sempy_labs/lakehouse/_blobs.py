from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_lakehouse_id,
    _xml_to_dict,
    _create_dataframe,
    _update_dataframe_datatypes,
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
)
from sempy._utils._log import log
from uuid import UUID
from typing import Optional, List
import sempy_labs._icons as icons
import xml.etree.ElementTree as ET
import pandas as pd


def _get_storage_token():

    import notebookutils

    return notebookutils.credentials.getToken("storage")


def _request_blob_api(
    request: str,
    method: str = "get",
    payload: Optional[dict] = None,
    status_codes: int | List[int] = 200,
    extra_headers: Optional[dict] = None,
):

    import requests
    from sempy.fabric.exceptions import FabricHTTPException

    if isinstance(status_codes, int):
        status_codes = [status_codes]

    token = _get_storage_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-ms-version": "2025-05-05",
    }

    if extra_headers:
        for k, v in extra_headers.items():
            headers[k] = v

    response = requests.request(
        method.upper(),
        f"https://onelake.blob.fabric.microsoft.com/{request}",
        headers=headers,
        json=payload,
    )

    if response.status_code not in status_codes:
        raise FabricHTTPException(response)

    return response


@log
def list_blobs(
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    container: Optional[str] = None,
) -> pd.DataFrame:
    """
    Returns a list of blobs for a given lakehouse.

    This function leverages the following API: `List Blobs <https://learn.microsoft.com/rest/api/storageservices/list-blobs?tabs=microsoft-entra-id>`_.

    Parameters
    ----------
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    container : str, default=None
        The container name to list blobs from. If None, lists all blobs in the lakehouse.
        Valid values are "Tables" or "Files". If not specified, the function will list all blobs in the lakehouse.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of blobs in the lakehouse.
    """

    workspace_id = resolve_workspace_id(workspace)
    lakehouse_id = resolve_lakehouse_id(lakehouse, workspace_id)

    if container is None:
        path_prefix = f"{workspace_id}/{lakehouse_id}"
    else:
        if container not in ["Tables", "Files"]:
            raise ValueError(
                f"{icons.red_dot} Invalid container '{container}' within the file_path parameter. Expected 'Tables' or 'Files'."
            )
        path_prefix = f"{workspace_id}/{lakehouse_id}/{container}"

    response = _request_blob_api(
        request=f"{path_prefix}?restype=container&comp=list&include=deleted"
    )
    root = ET.fromstring(response.content)
    response_json = _xml_to_dict(root)

    columns = {
        "Blob Name": "str",
        "Is Deleted": "bool",
        "Deletion Id": "str",
        "Creation Time": "datetime",
        "Expiry Time": "datetime",
        "Etag": "str",
        "Resource Type": "str",
        "Content Length": "int",
        "Content Type": "str",
        "Content Encoding": "str",
        "Content Language": "str",
        "Content CRC64": "str",
        "Content MD5": "str",
        "Cache Control": "str",
        "Content Disposition": "str",
        "Blob Type": "str",
        "Access Tier": "str",
        "Access Tier Inferred": "str",
        "Server Encrypted": "bool",
        "Deleted Time": "str",
        "Remaining Retention Days": "str",
    }

    df = _create_dataframe(columns=columns)

    for blob in (
        response_json.get("EnumerationResults", {}).get("Blobs", {}).get("Blob", {})
    ):
        p = blob.get("Properties", {})
        new_data = {
            "Blob Name": blob.get("Name"),
            "Is Deleted": blob.get("Deleted", False),
            "Deletion Id": blob.get("DeletionId"),
            "Creation Time": p.get("Creation-Time"),
            "Expiry Time": p.get("Expiry-Time"),
            "Etag": p.get("Etag"),
            "Resource Type": p.get("ResourceType"),
            "Content Length": p.get("Content-Length"),
            "Content Type": p.get("Content-Type"),
            "Content Encoding": p.get("Content-Encoding"),
            "Content Language": p.get("Content-Language"),
            "Content CRC64": p.get("Content-CRC64"),
            "Content MD5": p.get("Content-MD5"),
            "Cache Control": p.get("Cache-Control"),
            "Content Disposition": p.get("Content-Disposition"),
            "Blob Type": p.get("BlobType"),
            "Access Tier": p.get("AccessTier"),
            "Access Tier Inferred": p.get("AccessTierInferred"),
            "Server Encrypted": p.get("ServerEncrypted"),
            "Deleted Time": p.get("DeletedTime"),
            "Remaining Retention Days": p.get("RemainingRetentionDays"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def recover_lakehouse_object(
    file_path: str,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Recovers an object (i.e. table, file, folder) in a lakehouse from a deleted state. Only `soft-deleted objects <https://learn.microsoft.com/fabric/onelake/onelake-disaster-recovery#soft-delete-for-onelake-files>`_ can be recovered (deleted for less than 7 days).

    Parameters
    ----------
    file_path : str
        The file path of the object to restore. For example: "Tables/my_delta_table".
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_id = resolve_workspace_id(workspace)
    lakehouse_id = resolve_lakehouse_id(lakehouse, workspace_id)

    blob_path_prefix = f"{lakehouse_id}/{file_path}"

    container = file_path.split("/")[0]
    if container not in ["Tables", "Files"]:
        raise ValueError(
            f"{icons.red_dot} Invalid container '{container}' within the file_path parameter. Expected 'Tables' or 'Files'."
        )

    df = list_blobs(lakehouse=lakehouse, workspace=workspace, container=container)

    for _, r in df.iterrows():
        blob_name = r.get("Blob Name")
        is_deleted = r.get("Is Deleted")
        if blob_name.startswith(blob_path_prefix) and is_deleted:
            print(f"{icons.in_progress} Restoring the '{blob_name}' blob...")
            _request_blob_api(
                request=f"{workspace_id}/{lakehouse_id}/{file_path}?comp=undelete",
                method="put",
            )
            print(f"{icons.green_dot} The '{blob_name}' blob has been restored.")


def delete_lakehouse_object(
    file_path: str,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Deletes an object (i.e. table, file, folder) in a lakehouse. The object is soft-deleted and can be recovered within 7 days (use the `recover_lakehouse_object <https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.recover_lakehouse_object>`_ function).

    Parameters
    ----------
    file_path : str
        The file path of the object to delete. For example: "Tables/my_delta_table" or "Files/myfile.json" or "Files/myfolder".
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse, workspace_id
    )

    if not file_path.startswith("Tables") and not file_path.startswith("Files"):
        raise ValueError(
            f"{icons.red_dot} Invalid container '{file_path}' within the file_path parameter. Expected 'Tables' or 'Files'."
        )

    _request_blob_api(
        request=f"{workspace_id}/{lakehouse_id}/{file_path}?recursive=True",
        method="delete",
    )

    print(
        f"{icons.green_dot} The '{file_path}' blob has been soft-deleted in the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace. It can be recovered within 7 days."
    )


def copy_lakehouse_object(
    source_file_path: str,
    destination_file_path: Optional[str] = None,
    source_lakehouse: Optional[str | UUID] = None,
    source_workspace: Optional[str | UUID] = None,
    destination_lakehouse: Optional[str | UUID] = None,
    destination_workspace: Optional[str | UUID] = None,
):
    """
    Copies a blob from one lakehouse to another. The source and destination lakehouses must be different.

    Parameters
    ----------
    source_file_path : str
        The file path of the object to copy. For example: "Tables/my_delta_table" or "Files/myfile.json" or "Files/myfolder".
    destination_file_path : str, default=None
        The destination file path of the object being copied. For example: "Tables/my_delta_table" or "Files/myfile.json" or "Files/myfolder".
        Defaults to None which resolves to the source_file_path.
    source_lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID of the source.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    source_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the source lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    destination_lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID of the destination.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    destination_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the destination lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if destination_file_path is None:
        destination_file_path = source_file_path
    (source_workspace_name, source_workspace_id) = resolve_workspace_name_and_id(
        source_workspace
    )
    (source_lakehouse_name, source_lakehouse_id) = resolve_lakehouse_name_and_id(
        source_lakehouse, source_workspace
    )
    (destination_workspace_name, destination_workspace_id) = (
        resolve_workspace_name_and_id(destination_workspace)
    )
    (destination_lakehouse_name, destination_lakehouse_id) = (
        resolve_lakehouse_name_and_id(destination_lakehouse, destination_workspace)
    )

    if (source_lakehouse_id == destination_lakehouse_id) and (
        source_workspace_id == destination_workspace_id
    ):
        raise ValueError(
            f"{icons.red_dot} The source and destination lakehouse/workspace cannot be the same."
        )

    token = _get_storage_token()

    source_file_path = source_file_path.replace(" ", "%20")

    extra_headers = {
        "x-ms-copy-source": f"https://onelake.blob.fabric.microsoft.com/{source_workspace_id}/{source_lakehouse_id}/{source_file_path}?sv=2024-04-01&ss=bfqt&srt=sco&sp=rwdlacupx&se=2025-04-05T00:00:00Z&st=2025-04-02T00:00:00Z&spr=https&sig={token}"
    }
    print(extra_headers)

    _request_blob_api(request=f"{destination_workspace_id}/{destination_lakehouse_id}/{destination_file_path}", method="put", status_codes=202, extra_headers=extra_headers)

    print(f"{icons.green_dot} The '{source_file_path}' blob has been copied to the '{destination_file_path}' blob in the '{destination_lakehouse_name}' lakehouse within the '{destination_workspace_name}' workspace.")


def _get_user_delegation_key():

    # https://learn.microsoft.com/rest/api/storageservices/get-user-delegation-key

    from datetime import datetime, timedelta, timezone

    utc_now = datetime.now(timezone.utc)
    start_time = utc_now + timedelta(minutes=2)
    expiry_time = start_time + timedelta(minutes=45)
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    expiry_str = expiry_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    payload = f"""<?xml version="1.0" encoding="utf-8"?>
    <KeyInfo>
        <Start>{start_str}</Start>
        <Expiry>{expiry_str}</Expiry>
    </KeyInfo>"""

    response = _request_blob_api(
        request="restype=service&comp=userdelegationkey",
        method="post",
        payload=payload,
    )

    return response.content

