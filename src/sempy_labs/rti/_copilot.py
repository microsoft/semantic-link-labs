from typing import Literal, Optional, List
from sempy_labs._helper_functions import (
    resolve_item_id,
    resolve_workspace_id,
    _base_api,
)
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs.kql_database import resolve_cluster_uri


@log
def nl_to_kql(
    kql_database: str | UUID,
    billing_item: str | UUID,
    billing_item_type: Literal["KQLQueryset", "KQLDashboard", "Eventhouse"],
    prompt: str,
    chat_messages: Optional[dict | List[dict]] = None,
    user_shots: Optional[List[dict]] = None,
    workspace: Optional[str | UUID] = None,
) -> str:
    """
    Returns a KQL query generated from natural language.

    This is a wrapper function for the following API: `Copilot - NL To KQL <https://learn.microsoft.com/rest/api/fabric/realtimeintelligence/copilot/nl-to-kql(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    kql_database : str | uuid.UUID
        The name or UUID of the KQL database.
    billing_item : str | uuid.UUID
        The name or ID of the item for the request. This can be a KQLQueryset, KQLDashboard, or Eventhouse item. This item is used for billing the request.
    billing_item_type : typing.Literal["KQLQueryset", "KQLDashboard", "Eventhouse"]
        The type of the billing item.
    prompt : str
        The natural language to generate the KQL query from.
    chat_messages : Optional[str | List[str]], default=None
        The `chat messages <https://learn.microsoft.com/rest/api/fabric/realtimeintelligence/copilot/nl-to-kql(beta)?tabs=HTTP#chatmessage>`_ for the request. The chat messages provide additional context for generating the KQL query if necessary.

        Example:
            chat_messages = [
                {
                    "content": "Content....",
                    "role": "User"
                },
                {
                    "content": "Content...",
                    "role": "Assistant"
                }
            ]
    user_shots : Optional[List[str]], default=None
        The `user shots <https://learn.microsoft.com/rest/api/fabric/realtimeintelligence/copilot/nl-to-kql(beta)?tabs=HTTP#usershot>`_ for the request. This consists of user provided pairs of natural language and KQL queries in order to help in generating the current requested KQL query.

        Example:
            user_shots = [
                {
                    "kqlQuery": "KQL Query....",
                    "naturalLanguage": "Natural language...."
                },
                {
                    "kqlQuery": "KQL Query...",
                    "naturalLanguage": "Natural language..."
                }
            ]
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The generated KQL query.
    """

    workspace_id = resolve_workspace_id(workspace=workspace)
    db_id = resolve_item_id(
        item=kql_database, type="KQLDatabase", workspace=workspace_id
    )
    cluster_uri = resolve_cluster_uri(kql_database=kql_database, workspace=workspace_id)

    valid_types = ["KQLQueryset", "KQLDashboard", "Eventhouse"]
    if billing_item_type not in valid_types:
        raise ValueError(
            f"{icons.red_dot} The billing_item_type must be either 'KQLQueryset', 'KQLDashboard', or 'Eventhouse'."
        )

    billing_item_id = resolve_item_id(
        item=billing_item, type=billing_item_type, workspace=workspace_id
    )

    payload = {
        "clusterUrl": cluster_uri,
        "databaseName": db_id,
        "itemIdForBilling": billing_item_id,
        "naturalLanguage": prompt,
    }

    if chat_messages:
        if isinstance(chat_messages, str):
            chat_messages = [chat_messages]
        payload["chatMessages"] = chat_messages
    if user_shots:
        if isinstance(user_shots, str):
            user_shots = [user_shots]
        payload["userShots"] = user_shots

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/realtimeintelligence/nltokql?beta=True",
        payload=payload,
        method="post",
        client="fabric_sp",
    )
    return response.json().get("kqlQuery")
