import pandas as pd
import os
import base64
from uuid import UUID
import sempy_labs._icons as icons
from typing import List, Literal, Optional
from .._helper_functions import (
    _is_valid_uuid,
    _base_api,
    _create_dataframe,
    _mount,
)
from sempy._utils._log import log


@log
def resolve_user_id(user: str | UUID) -> UUID:
    """
    Resolves the user ID from the user principal name or ID.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    user : str | uuid.UUID
        The user ID or user principal name.

    Returns
    -------
    uuid.UUID
        The user ID.
    """

    if _is_valid_uuid(user):
        return user
    else:
        result = _base_api(request=f"users/{user}", client="graph").json()
        return result.get("id")


@log
def get_user(user: str | UUID) -> pd.DataFrame:
    """
    Shows properties of a given user.

    This is a wrapper function for the following API: `Get a user <https://learn.microsoft.com/graph/api/user-get>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    user : str | uuid.UUID
        The user ID or user principal name.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing properties of a given user.
    """

    result = _base_api(request=f"users/{user}", client="graph").json()

    new_data = {
        "User Id": result.get("id"),
        "User Principal Name": result.get("userPrincipalName"),
        "User Name": result.get("displayName"),
        "Mail": result.get("mail"),
        "Job Title": result.get("jobTitle"),
        "Office Location": result.get("officeLocation"),
        "Mobile Phone": result.get("mobilePhone"),
        "Business Phones": str(result.get("businessPhones")),
        "Preferred Language": result.get("preferredLanguage"),
        "Surname": result.get("surname"),
    }

    return pd.DataFrame([new_data])


@log
def list_users() -> pd.DataFrame:
    """
    Shows a list of users and their properties.

    This is a wrapper function for the following API: `List users <https://learn.microsoft.com/graph/api/user-list>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of users and their properties.
    """

    result = _base_api(request="users", client="graph").json()

    columns = {
        "User Id": "string",
        "User Principal Name": "string",
        "User Name": "string",
        "Mail": "string",
        "Job Title": "string",
        "Office Location": "string",
        "Mobile Phone": "string",
        "Business Phones": "string",
        "Preferred Language": "string",
        "Surname": "string",
    }

    df = _create_dataframe(columns=columns)

    for v in result.get("value"):
        new_data = {
            "User Id": v.get("id"),
            "User Principal Name": v.get("userPrincipalName"),
            "User Name": v.get("displayName"),
            "Mail": v.get("mail"),
            "Job Title": v.get("jobTitle"),
            "Office Location": v.get("officeLocation"),
            "Mobile Phone": v.get("mobilePhone"),
            "Business Phones": str(v.get("businessPhones")),
            "Preferred Language": v.get("preferredLanguage"),
            "Surname": v.get("surname"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


@log
def send_mail(
    user: UUID | str,
    subject: str,
    to_recipients: str | List[str],
    content: str,
    content_type: Literal["Text", "HTML"] = "Text",
    cc_recipients: Optional[str | List[str]] = None,
    bcc_recipients: Optional[str | List[str]] = None,
    priority: Literal["Normal", "High", "Low"] = "Normal",
    follow_up_flag: bool = False,
    attachments: Optional[str | List[str]] = None,
):
    """
    Sends an email to the specified recipients.

    This is a wrapper function for the following API: `user: sendMail <https://learn.microsoft.com/graph/api/user-sendmail>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    user : uuid.UUID | str
        The user ID or user principal name.
    subject : str
        The email subject.
    to_recipients : str | List[str]
        The email address of the recipients.
    content : str
        The email content.
    content_type : Literal["Text", "HTML"], default="Text"
        The email content type. Options: "Text" or "HTML".
    cc_recipients : str | List[str], default=None
        The email address of the CC recipients.
    bcc_recipients : str | List[str], default=None
        The email address of the BCC recipients.
    priority : Literal["Normal", "High", "Low"], default="Normal"
        The email priority.
    follow_up_flag : bool, default=False
        Whether to set a follow-up flag for the email.
    attachments : str | List[str], default=None
        The abfss path or a list of the abfss paths of the attachments to include in the email.
    """

    content_type = "HTML" if content_type.lower() == "html" else "Text"

    priority = priority.capitalize()
    if priority not in ["Normal", "High", "Low"]:
        raise ValueError(
            f"{icons.red_dot} Invalid priority: {priority}. Options are: Normal, High, Low."
        )

    user_id = resolve_user_id(user=user)

    if isinstance(to_recipients, str):
        to_recipients = [to_recipients]

    if isinstance(cc_recipients, str):
        cc_recipients = [cc_recipients]

    to_email_addresses = [
        {"emailAddress": {"address": email}} for email in to_recipients
    ]

    cc_email_addresses = (
        [{"emailAddress": {"address": email}} for email in cc_recipients]
        if cc_recipients
        else None
    )
    bcc_email_addresses = (
        [{"emailAddress": {"address": email}} for email in bcc_recipients]
        if bcc_recipients
        else None
    )

    payload = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": content_type,
                "content": content,
            },
            "toRecipients": to_email_addresses,
            "importance": priority,
        },
    }

    if cc_email_addresses:
        payload["message"]["ccRecipients"] = cc_email_addresses

    if bcc_email_addresses:
        payload["message"]["bccRecipients"] = bcc_email_addresses

    if follow_up_flag:
        payload["message"]["flag"] = {"flagStatus": "flagged"}

    content_types = {
        ".txt": "text/plain",
        ".pdf": "application/pdf",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".csv": "text/csv",
        ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".bmp": "image/bmp",
        ".zip": "application/zip",
        ".json": "application/json",
        ".xml": "application/xml",
        ".html": "text/html",
        ".bim": "application/json",
        ".pbix": "application/vnd.ms-powerbi.report",
        ".pbip": "application/vnd.ms-powerbi.report",
        ".pbit": "application/vnd.ms-powerbi.report",
        ".vpax": "application/zip",
    }

    def file_path_to_content_bytes(file_path):

        path_parts = file_path.split("abfss://")[1].split("@")
        workspace = path_parts[0]

        rest = path_parts[1].split(".microsoft.com/")[1]
        lakehouse, *file_parts = rest.split("/")
        if lakehouse.endswith(".Lakehouse"):
            lakehouse = lakehouse.removesuffix(".Lakehouse")
        relative_path = os.path.join(*file_parts)

        local_path = _mount(lakehouse, workspace)
        full_path = os.path.join(local_path, relative_path)

        with open(full_path, "rb") as file:
            return base64.b64encode(file.read()).decode("utf-8")

    if isinstance(attachments, str):
        attachments = [attachments]
    if attachments:
        attachments_list = []
        for attach_path in attachments:
            content_bytes = file_path_to_content_bytes(attach_path)
            file_extension = os.path.splitext(attach_path)[1]
            content_type = content_types.get(file_extension)
            if not content_type:
                raise ValueError(
                    f"{icons.red_dot} Unsupported file type: {file_extension}. Supported types are: {', '.join(content_types.keys())}."
                )
            attachments_list.append(
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": attach_path.split("/")[-1],
                    "contentType": content_type,
                    "contentBytes": content_bytes,
                }
            )

        # Add to payload
        payload["message"]["attachments"] = attachments_list

    _base_api(
        request=f"users/{user_id}/sendMail",
        client="graph",
        status_codes=202,
        payload=payload,
        method="post",
    )

    printout = f"{icons.green_dot} The email has been sent to {to_recipients}"
    if cc_recipients:
        printout += f" and CCed to {cc_recipients}"
    if bcc_recipients:
        printout += f" and BCCed to {bcc_recipients}"
    if attachments:
        printout += f" with {len(attachments)} attachment(s)"
    print(f"{printout}.")
