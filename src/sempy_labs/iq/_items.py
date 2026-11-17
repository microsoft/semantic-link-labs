import pandas as pd
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
)
import sempy_labs._icons as icons
from sempy._utils._log import log


@log
def retrieve(query: str, token_budget: int = 2048) -> pd.DataFrame:
    """
    Retrieves Microsoft IQ organizational context relevant to a natural-language query.

    Results are ranked by relevance and only include context from Microsoft IQ sections on which the caller holds an effective
    *User* or *Curator* section role. The service returns a maximum of 50 results.

    This is a wrapper function for the following API: `Microsoft IQ - Retrieve <https://learn.microsoft.com/rest/api/fabric/core/microsoft-iq/retrieve>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    query : str
        The non-empty natural-language query text.
    token_budget : int, default=2048
        The maximum total token count of the returned summaries. Only results that fit within the budget are returned.
        The minimum is 1.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the relevance-ranked organizational context. Empty if no relevant, permitted context is found.
        The 'Reference' column contains the type-specific properties of the reference (i.e. 'url' for a 'MicrosoftIQSkillReference').
    """

    if not isinstance(query, str) or not query.strip():
        raise ValueError(
            f"{icons.red_dot} The 'query' parameter must be a non-empty string."
        )
    if (
        isinstance(token_budget, bool)
        or not isinstance(token_budget, int)
        or token_budget < 1
    ):
        raise ValueError(
            f"{icons.red_dot} The 'token_budget' parameter must be an integer greater than or equal to 1."
        )

    columns = {
        "Rank": "int",
        "Identifier": "str",
        "Type": "str",
        "Score": "float",
        "Token Count": "int",
        "Lexical Match": "bool",
        "Summary": "str",
        "Reference Type": "str",
        "Reference": "object",
    }
    df = _create_dataframe(columns=columns)

    payload = {
        "query": {"text": query},
        "tokenBudget": token_budget,
    }

    response = _base_api(
        request="/v1/microsoftIQ/retrieve",
        method="post",
        payload=payload,
        client="fabric_sp",
    ).json()

    rows = []
    for r in response.get("results", []):
        data = r.get("data") or {}
        uri = data.get("uri") or {}
        rows.append(
            {
                "Rank": r.get("rank"),
                "Identifier": r.get("identifier"),
                "Type": r.get("type"),
                "Score": r.get("score"),
                "Token Count": r.get("tokenCount"),
                "Lexical Match": r.get("lexicalMatch"),
                "Summary": data.get("summary"),
                "Reference Type": uri.get("$type"),
                "Reference": {k: v for k, v in uri.items() if k != "$type"} or None,
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def am_i_in_scope() -> bool:
    """
    Checks whether Microsoft IQ holds organizational context that the caller is entitled to see.

    Use this to skip a :meth:`sempy_labs.iq.retrieve_context` call that would return no context. A value of True doesn't
    guarantee that a particular query will match anything.

    This is a wrapper function for the following API: `Microsoft IQ - Am I In Scope <https://learn.microsoft.com/rest/api/fabric/core/microsoft-iq/am-i-in-scope>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    bool
        True if Microsoft IQ holds at least one piece of organizational context that the caller is entitled to see, otherwise False.
    """

    response = _base_api(
        request="/v1/microsoftIQ/amIInScope",
        method="post",
        client="fabric_sp",
    ).json()

    return bool(response.get("inScope", False))
