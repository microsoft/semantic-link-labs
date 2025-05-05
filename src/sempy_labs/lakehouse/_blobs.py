from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_lakehouse_id,
    _xml_to_dict,
    _create_dataframe,
    _update_dataframe_datatypes,
)
from sempy._utils._log import log
from uuid import UUID
from typing import Optional, List
import sempy_labs._icons as icons
import xml.etree.ElementTree as ET
import pandas as pd


def _request_blob_api(
    request: str,
    method: str = "get",
    payload: Optional[dict] = None,
    status_codes: int | List[int] = 200,
    uses_pagination: bool = False,
):

    import requests
    import notebookutils
    from sempy.fabric.exceptions import FabricHTTPException

    if isinstance(status_codes, int):
        status_codes = [status_codes]

    token = notebookutils.credentials.getToken("storage")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-ms-version": "2025-05-05",
    }

    base_url = "https://onelake.blob.fabric.microsoft.com/"
    full_url = f"{base_url}{request}"
    results = []

    while True:
        response = requests.request(
            method.upper(),
            full_url,
            headers=headers,
            json=payload if method.lower() != "get" else None,
        )

        if response.status_code not in status_codes:
            raise FabricHTTPException(response)

        if not uses_pagination:
            return response

        # Parse XML to find blobs and NextMarker
        root = ET.fromstring(response.content)
        results.append(root)

        next_marker = root.findtext(".//NextMarker")
        if not next_marker:
            break  # No more pages

        # Append the marker to the original request (assuming query string format)
        delimiter = "&" if "?" in request else "?"
        full_url = f"{base_url}{request}{delimiter}marker={next_marker}"

    return results


@log
def list_blobs(
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    container: Optional[str] = None,
    prefix: Optional[str] = None,
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
    prefix : str, default=None
        The prefix to filter blobs by (i.e. the blob name). If None, lists all blobs in the specified container.

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

    url = f"{path_prefix}?restype=container&comp=list&include=deleted"
    if prefix:
        if container:
            url += f"&prefix={lakehouse_id}/{container}/{prefix}&delimiter=/"
        else:
            url += f"&prefix={lakehouse_id}/{prefix}&delimiter=/"

    responses = _request_blob_api(
        request=url,
        uses_pagination=True,
    )

    dfs = []
    for root in responses:
        response_json = _xml_to_dict(root)

        if prefix:
            blobs = (
                response_json.get("EnumerationResults", {})
                .get("Blobs", {})
                .get("BlobPrefix", [])
            )
        else:
            blobs = (
                response_json.get("EnumerationResults", {})
                .get("Blobs", {})
                .get("Blob", [])
            )

        if isinstance(blobs, dict):
            blobs = [blobs]

        for blob in blobs:
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

            dfs.append(pd.DataFrame(new_data, index=[0]))

    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        _update_dataframe_datatypes(dataframe=df, column_map=columns)
        if prefix:
            # These columns do not show when the prefix is used
            df.drop(['Is Deleted', 'Deletion Id'], axis=1, inplace=True)

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

    blob_name = f"{lakehouse_id}/{file_path}"

    container = file_path.split("/")[0]
    if container not in ["Tables", "Files"]:
        raise ValueError(
            f"{icons.red_dot} Invalid container '{container}' within the file_path parameter. Expected 'Tables' or 'Files'."
        )

    # Filter list_blobs by the file
    prefix = file_path[len(container) + 1 :]

    df = list_blobs(
        lakehouse=lakehouse, workspace=workspace, container=container, prefix=prefix
    )
    # df_filt = df[df["Blob Name"] == blob_name]
    # df_filt_deleted = df_filt[df_filt['Is Deleted'] == True]

    # if df_filt_deleted.empty:
    if df.empty:
        print(f"{icons.warning} The '{file_path}' blob was not found. No action taken.")
        return
    #    else:
    #        print(f"{icons.info} The '{file_path}' blob was not found in a deleted state. No action taken.")
    #        return

    # Undelete the blob
    print(f"{icons.in_progress} Restoring the '{blob_name}' blob...")
    _request_blob_api(
        request=f"{workspace_id}/{lakehouse_id}/{file_path}?comp=undelete",
        method="put",
    )
    print(f"{icons.green_dot} The '{blob_name}' blob restore attempt was successful.")


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
