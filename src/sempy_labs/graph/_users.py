import pandas as pd
from uuid import UUID
from sempy.fabric._token_provider import TokenProvider
import sempy_labs._icons as icons
from typing import List
from sempy_labs.graph._util import (
    _ms_graph_base,
)
from sempy_labs._helper_functions import _is_valid_uuid


def resolve_user_id(user: str | UUID, token_provider: TokenProvider) -> UUID:
    """
    Resolves the user ID from the user principal name or ID.

    Parameters
    ----------
    user : str | uuid.UUID
        The user ID or user principal name.
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.

    Returns
    -------
    uuid.UUID
        The user ID.
    """

    if _is_valid_uuid(user):
        return user
    else:
        result = _ms_graph_base(api_name=f"users/{user}", token_provider=token_provider)
        return result.get("id")


def get_user(user: str | UUID, token_provider: TokenProvider) -> pd.DataFrame:
    """
    Shows properties of a given user.

    This is a wrapper function for the following API: `Get a user <https://learn.microsoft.com/graph/api/user-get>`_.

    Parameters
    ----------
    user : str | uuid.UUID
        The user ID or user principal name.
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing properties of a given user.
    """

    result = _ms_graph_base(api_name=f"users/{user}", token_provider=token_provider)

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


def list_users(token_provider: TokenProvider) -> pd.DataFrame:
    """
    Shows a list of users and their properties.

    This is a wrapper function for the following API: `List users <https://learn.microsoft.com/graph/api/user-list>`_.

    Parameters
    ----------
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of users and their properties.
    """

    result = _ms_graph_base(api_name="users", token_provider=token_provider)

    df = pd.DataFrame(
        columns=[
            "User Id",
            "User Principal Name",
            "User Name",
            "Mail",
            "Job Title",
            "Office Location",
            "Mobile Phone",
            "Business Phones",
            "Preferred Language",
            "Surname",
        ]
    )

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
    token_provider: TokenProvider,
    cc_recipients: str | List[str] = None,
):
    """
    Sends an email to the specified recipients.

    This is a wrapper function for the following API: `user: sendMail <https://learn.microsoft.com/graph/api/user-sendmail>`_.

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
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.
    cc_recipients : str | List[str], default=None
        The email address of the CC recipients.
    """

    user_id = resolve_user_id(user=user, token_provider=token_provider)

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

    payload = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": content,
            },
            "toRecipients": to_email_addresses,
        },
    }

    if cc_email_addresses:
        payload["message"]["ccRecipients"] = cc_email_addresses

    _ms_graph_base(
        api_name=f"users/{user_id}/sendMail",
        token_provider=token_provider,
        status_success_code=202,
        return_json=False,
        payload=payload,
        call_type="post",
    )

    print(f"{icons.green_dot} The email has been sent to {to_recipients}.")
