import pandas as pd
from sempy_labs._helper_functions import (
    _create_dataframe,
    _update_dataframe_datatypes,
    _base_api,
)
from sempy._utils._log import log


@log
def list_tenant_keys() -> pd.DataFrame:
    """
    Returns the encryption keys for the tenant.

    This is a wrapper function for the following API: `Admin - Get Power BI Encryption Keys <https://learn.microsoft.com/rest/api/power-bi/admin/get-power-bi-encryption-keys>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the encryption keys for the tenant.
    """

    columns = {
        "Tenant Key Id": "str",
        "Tenant Key Name": "str",
        "Key Vault Key Identifier": "str",
        "Is Default": "bool",
        "Created At": "datetime",
        "Updated At": "datetime",
    }

    df = _create_dataframe(columns=columns)

    result = _base_api(
        request="/v1.0/myorg/admin/tenantKeys", client="fabric_sp"
    ).json()

    rows = []
    for i in result.get("value", []):
        rows.append(
            {
                "Tenant Key Id": i.get("id"),
                "Tenant Key Name": i.get("name"),
                "Key Vault Key Identifier": i.get("keyVaultKeyIdentifier"),
                "Is Default": i.get("isDefault"),
                "Created At": i.get("createdAt"),
                "Updated At": i.get("updatedAt"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df
