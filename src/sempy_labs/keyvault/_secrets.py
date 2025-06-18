from typing import Any
import pandas as pd
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
)
import sempy_labs._icons as icons
from sempy._utils._log import log


@log
def get_secret(key_vault_uri: str, secret_name: str) -> Any:
    """
    Retrieves the latest version of a secret from the specified Azure Key Vault.

    This is a wrapper function for the following API: `Get Secret - Get Secret <https://learn.microsoft.com/rest/api/keyvault/secrets/get-secret/get-secret>`_.

    Parameters
    ----------
    key_vault_uri : str
        Azure Key Vault URI.
    secret_name : str
        Name of the secret in the Key Vault.

    Returns
    -------
    Any
        The value of the latest version of the secret.
    """

    response = _base_api(
        request=f"{key_vault_uri}/secrets/{secret_name}/versions", client="keyvault"
    )
    url = response.json().get("value")[-1].get("id")
    response = _base_api(request=f"{url}", client="keyvault")
    return response.json().get("value")


@log
def set_secret(key_vault_uri: str, secret_name: str, secret_value: Any):
    """
    Sets a secret in the specified Azure Key Vault.

    This is a wrapper function for the following API: `Set Secret - Set Secret <https://learn.microsoft.com/rest/api/keyvault/secrets/set-secret/set-secret>`_.

    Parameters
    ----------
    key_vault_uri : str
        Azure Key Vault URI.
    secret_name : str
        Name of the secret to be set in the Key Vault.
    secret_value : Any
        Value of the secret to be set in the Key Vault.
    """

    payload = {"value": secret_value}
    _base_api(
        request=f"{key_vault_uri}/secrets/{secret_name}",
        client="keyvault",
        method="put",
        payload=payload,
    )
    print(
        f"{icons.green_dot} The '{secret_name}' secret has been successfully set within the '{key_vault_uri}' Key Vault."
    )


@log
def list_secrets(key_vault_uri: str) -> pd.DataFrame:
    """
    Lists all secrets in the specified Azure Key Vault.

    This is a wrapper function for the following API: `Get Secrets - Get Secrets <https://learn.microsoft.com/rest/api/keyvault/secrets/get-secrets/get-secrets>`_.

    Parameters
    ----------
    key_vault_uri : str
        Azure Key Vault URI.

    Returns
    -------
    pandas.DataFrame
        A pandas DataFrame containing details of all secrets in the Key Vault.
    """

    columns = {
        "Secret Id": "str",
        "Enabled": "bool",
        "Created Date": "datetime",
        "Updated Date": "datetime",
        "Recovery Level": "str",
        "Recoverable Days": "int",
    }

    df = _create_dataframe(columns=columns)

    all_items = []
    url = f"{key_vault_uri}/secrets"
    while url:
        response = _base_api(request=url, client="keyvault")
        result = response.json()
        items = result.get("value", [])
        all_items.extend(items)
        url = result.get("nextLink")  # Update to next page if it exists

    if not all_items:
        return df

    df = pd.json_normalize(all_items)
    df.rename(
        columns={
            "id": "Secret Id",
            "attributes.enabled": "Enabled",
            "attributes.created": "Created Date",
            "attributes.updated": "Updated Date",
            "attributes.recoveryLevel": "Recovery Level",
            "attributes.recoverableDays": "Recoverable Days",
        },
        inplace=True,
    )
    df["Created Date"] = pd.to_datetime(df["Created Date"], unit="s")
    df["Updated Date"] = pd.to_datetime(df["Updated Date"], unit="s")
    return df


@log
def list_deleted_secrets(key_vault_uri: str) -> pd.DataFrame:
    """
    Lists all deleted secrets in the specified Azure Key Vault.

    This is a wrapper function for the following API: `Get Deleted Secrets - GetDeleted Secrets <https://learn.microsoft.com/rest/api/keyvault/secrets/get-deleted-secrets/get-deleted-secrets>`_.

    Parameters
    ----------
    key_vault_uri : str
        Azure Key Vault URI.

    Returns
    -------
    pandas.DataFrame
        A pandas DataFrame containing details of all secrets in the Key Vault.
    """

    columns = {
        "Recovery Id": "str",
        "Deleted Date": "datetime",
        "Scheduled Purge Date": "datetime",
        "Content Type": "str",
        "Secret Id": "str",
        "Enabled": "bool",
        "Created Date": "datetime",
        "Updated Date": "datetime",
        "Recovery Level": "str",
    }

    df = _create_dataframe(columns=columns)

    all_items = []
    url = f"{key_vault_uri}/deletedSecrets"
    while url:
        response = _base_api(request=url, client="keyvault")
        result = response.json()
        items = result.get("value", [])
        all_items.extend(items)
        url = result.get("nextLink")  # Update to next page if it exists

    if not all_items:
        return df

    df = pd.json_normalize(all_items)
    df.rename(
        columns={
            "recoveryId": "Recovery Id",
            "deletedDate": "Deleted Date",
            "scheduledPurgeDate": "Scheduled Purge Date",
            "contentType": "Content Type",
            "id": "Secret Id",
            "attributes.enabled": "Enabled",
            "attributes.created": "Created Date",
            "attributes.updated": "Updated Date",
            "attributes.recoveryLevel": "Recovery Level",
        },
        inplace=True,
    )
    df["Scheduled Purge Date"] = pd.to_datetime(df["Scheduled Purge Date"], unit="s")
    df["Deleted Date"] = pd.to_datetime(df["Deleted Date"], unit="s")
    df["Created Date"] = pd.to_datetime(df["Created Date"], unit="s")
    df["Updated Date"] = pd.to_datetime(df["Updated Date"], unit="s")

    return df


@log
def recover_deleted_secret(key_vault_uri: str, secret_name: str):
    """
    Recovers a deleted secret in the specified Azure Key Vault.

    This is a wrapper function for the following API: `Recover Deleted Secret - Recover Deleted Secret <https://learn.microsoft.com/rest/api/keyvault/secrets/recover-deleted-secret/recover-deleted-secret>`_.

    Parameters
    ----------
    key_vault_uri : str
        Azure Key Vault URI.
    secret_name : str
        Name of the deleted secret to be recovered in the Key Vault.
    """

    _base_api(
        request=f"{key_vault_uri}/deletedsecrets/{secret_name}/recover",
        client="keyvault",
        method="post",
    )
    print(
        f"{icons.green_dot} The '{secret_name}' secret has been successfully recovered within the '{key_vault_uri}' Key Vault."
    )


@log
def delete_secret(key_vault_uri: str, secret_name: str):
    """
    Deletes a secret in the specified Azure Key Vault.

    This is a wrapper function for the following API: `Delete Secret - Delete Secret <https://learn.microsoft.com/rest/api/keyvault/secrets/delete-secret/delete-secret>`_.

    Parameters
    ----------
    key_vault_uri : str
        Azure Key Vault URI.
    secret_name : str
        Name of the secret to be deleted in the Key Vault.
    """

    _base_api(
        request=f"{key_vault_uri}/secrets/{secret_name}",
        client="keyvault",
        method="delete",
    )
    print(
        f"{icons.green_dot} The '{secret_name}' secret has been successfully deleted within the '{key_vault_uri}' Key Vault."
    )
