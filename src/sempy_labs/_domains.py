import pandas as pd
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
)
from sempy._utils._log import log


@log
def list_domains() -> pd.DataFrame:
    """
    Returns a list of all the tenant's domains.

    This is a wrapper function for the following API: `Domains - List Domains <https://learn.microsoft.com/rest/api/fabric/core/domains/list-domains>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the domains.
    """

    columns = {
        "Domain Id": "string",
        "Domain Name": "string",
        "Description": "string",
        "Parent Domain Id": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request="/v1/domains", client="fabric_sp", uses_pagination=True
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Domain Id": v.get("id"),
                    "Domain Name": v.get("displayName"),
                    "Description": v.get("description"),
                    "Parent Domain Id": v.get("parentDomainId"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df
