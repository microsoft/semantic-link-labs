import pandas as pd
from uuid import UUID
import sempy_labs._icons as icons
from typing import List
from sempy_labs._helper_functions import (
    _is_valid_uuid,
    _base_api,
    _create_dataframe,
)


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


def send_mail(
    user: UUID | str,
    subject: str,
    to_recipients: str | List[str],
    content: str,
    content_type: str = "Text",
    cc_recipients: str | List[str] = None,
    bcc_recipients: str | List[str] = None,
    priority: str = "Normal",
    follow_up_flag: bool = False,
    # attachments: str | List[str] = None,
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
    content_type : str, default="Text"
        The email content type. Options: "Text" or "HTML".
    cc_recipients : str | List[str], default=None
        The email address of the CC recipients.
    bcc_recipients : str | List[str], default=None
        The email address of the BCC recipients.
    priority : str, default="Normal"
        The email priority. Options: "Normal", "High", or "Low".
    follow_up_flag : bool, default=False
        Whether to set a follow-up flag for the email.
    """

    #import base64
    #from sempy_labs._helper_functions import _create_spark_session
    #attachments : str | List[str], default=None
        #The abfss path or a list of the abfss paths of the attachments to include in the email.

    if content_type.lower() == "html":
        content_type = "HTML"
    else:
        content_type = "Text"

    priority = priority.capitalize()
    if priority not in ["Normal", "High", "Low"]:
        raise ValueError(
            f"Invalid priority: {priority}. Options are: Normal, High, Low."
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

    #if attachments:
    #    if isinstance(attachments, str):
    #        attachments = [attachments]

    #    for abfss_path in attachments:
    #        file_name = abfss_path.split("/")[-1]
    #        if "attachments" not in payload["message"]:
    #            payload["message"]["attachments"] = []

    #        spark = _create_spark_session()
    #        spark_file = (
    #            spark.read.format("binaryFile")
    #            .load(abfss_path)
    #            .select("content")
    #            .collect()[0][0]
    #        )
    #        content_bytes = base64.b64encode(spark_file).decode(
    #            "utf-8"
    #        )  # Convert bytearray to base64 string

    #        payload["message"]["attachments"].append(
    #            {
    #                "@odata.type": "#microsoft.graph.fileAttachment",
    #                "name": file_name,
    #                "contentBytes": content_bytes,
    #            }
    #        )

    _base_api(
        request=f"users/{user_id}/sendMail",
        client="graph",
        status_codes=202,
        payload=payload,
        method="post",
    )

    print(f"{icons.green_dot} The email has been sent to {to_recipients}.")
