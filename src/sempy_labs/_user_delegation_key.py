from sempy_labs.lakehouse._blobs import _request_blob_api
from sempy_labs._helper_functions import (
    _xml_to_dict,
)
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET
from sempy._utils._log import log


@log
def get_user_delegation_key():
    """
    Gets a key that can be used to sign a user delegation SAS (shared access signature). A user delegation SAS grants access to Azure Blob Storage resources by using Microsoft Entra credentials.

    This is a wrapper function for the following API: `Get User Delegation Key <https://learn.microsoft.com/rest/api/storageservices/get-user-delegation-key>`_.

    Returns
    -------
    str
        The user delegation key value.
    """

    utc_now = datetime.now(timezone.utc)
    start_time = utc_now + timedelta(minutes=2)
    expiry_time = start_time + timedelta(minutes=60)
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    expiry_str = expiry_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    payload = f"""<?xml version="1.0" encoding="utf-8"?>
    <KeyInfo>
        <Start>{start_str}</Start>
        <Expiry>{expiry_str}</Expiry>
    </KeyInfo>"""

    response = _request_blob_api(
        request="?restype=service&comp=userdelegationkey",
        method="post",
        payload=payload,
    )

    root = ET.fromstring(response.content)
    response_json = _xml_to_dict(root)

    return response_json.get("UserDelegationKey", {}).get("Value", None)
