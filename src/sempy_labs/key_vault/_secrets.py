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

    responses = _base_api(
        request=f"{key_vault_uri}/secrets", client="keyvault", uses_pagination=True
    )

    rows = []
    for r in responses:
        rows.append(
            {
                "Secret Id": r.get("id"),
                "Enabled": r.get("attributes", {}).get("enabled"),
                "Created Date": r.get("attributes", {}).get("created"),
                "Updated Date": r.get("attributes", {}).get("updated"),
                "Recovery Level": r.get("attributes", {}).get("recoveryLevel"),
                "Recoverable Days": r.get("attributes", {}).get("recoverableDays"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        df["Created Date"] = pd.to_datetime(df["Created Date"], unit="s")
        df["Updated Date"] = pd.to_datetime(df["Updated Date"], unit="s")
        df["Recoverable Days"] = df["Recoverable Days"].astype(int)

    return df


@log
def list_secret_versions(key_vault_uri: str, secret_name: str) -> pd.DataFrame:
    """
    Lists all versions of a specific secret in the specified Azure Key Vault.

    This is a wrapper function for the following API: `Get Secret Versions - Get Secret Versions <https://learn.microsoft.com/rest/api/keyvault/secrets/get-secret-versions/get-secret-versions>`_.

    Parameters
    ----------
    key_vault_uri : str
        Azure Key Vault URI.
    secret_name : str
        Name of the secret.

    Returns
    -------
    pandas.DataFrame
        A pandas DataFrame containing details of all versions of the secret in the Key Vault.
    """

    columns = {
        "Secret Id": "str",
        "Enabled": "bool",
        "Created Date": "datetime",
        "Updated Date": "datetime",
    }

    df = _create_dataframe(columns=columns)

    url = f"{key_vault_uri}/secrets/{secret_name}/versions"
    responses = _base_api(request=url, client="keyvault", uses_pagination=True)

    rows = []
    for r in responses:
        rows.append(
            {
                "Secret Id": r.get("id"),
                "Enabled": r.get("attributes", {}).get("enabled"),
                "Created Date": r.get("attributes", {}).get("created"),
                "Updated Date": r.get("attributes", {}).get("updated"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
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

    responses = _base_api(
        request=f"{key_vault_uri}/deletedSecrets",
        client="keyvault",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        rows.append(
            {
                "Recovery Id": r.get("recoveryId"),
                "Deleted Date": r.get("deletedDate"),
                "Scheduled Purge Date": r.get("scheduledPurgeDate"),
                "Content Type": r.get("contentType"),
                "Secret Id": r.get("id"),
                "Enabled": r.get("attributes", {}).get("enabled"),
                "Created Date": r.get("attributes", {}).get("created"),
                "Updated Date": r.get("attributes", {}).get("updated"),
                "Recovery Level": r.get("attributes", {}).get("recoveryLevel"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        df["Scheduled Purge Date"] = pd.to_datetime(
            df["Scheduled Purge Date"], unit="s"
        )
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
        f"{icons.green_dot} The '{secret_name}' secret within the '{key_vault_uri}' Key Vault has been successfully recovered."
    )


@log
def purge_deleted_secret(key_vault_uri: str, secret_name: str):
    """
    Permanently deletes the specified secret.
    The purge deleted secret operation removes the secret permanently, without the possibility of recovery. This operation can only be enabled on a soft-delete enabled vault. This operation requires the secrets/purge permission.

    This is a wrapper function for the following API: `Purge Deleted Secret - Purge Deleted Secret <https://learn.microsoft.com/rest/api/keyvault/secrets/purge-deleted-secret/purge-deleted-secret>`_.

    Parameters
    ----------
    key_vault_uri : str
        Azure Key Vault URI.
    secret_name : str
        Name of the deleted secret to be recovered in the Key Vault.
    """

    _base_api(
        request=f"{key_vault_uri}/deletedsecrets/{secret_name}",
        client="keyvault",
        method="delete",
        status_codes=204,
    )
    print(
        f"{icons.green_dot} The '{secret_name}' secret within the '{key_vault_uri}' Key Vault has been successfully purged."
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


@log
def backup_secret(key_vault_uri: str, secret_name: str) -> bytes:
    """
    Backs up a secret from the specified Azure Key Vault.

    This is a wrapper function for the following API: `Backup Secret - Backup Secret <https://learn.microsoft.com/rest/api/keyvault/secrets/backup-secret/backup-secret>`_.

    Parameters
    ----------
    key_vault_uri : str
        Azure Key Vault URI.
    secret_name : str
        Name of the secret to be backed up in the Key Vault.

    Returns
    -------
    bytes
        The backup blob of the secret.
    """

    response = _base_api(
        request=f"{key_vault_uri}/secrets/{secret_name}/backup", client="keyvault"
    )
    return response.json().get("value")


@log
def restore_secret(key_vault_uri: str, backup_blob: bytes) -> dict:
    """
    Restores a backed up secret to the specified Azure Key Vault.

    This is a wrapper function for the following API: `Restore Secret - Restore Secret <https://learn.microsoft.com/rest/api/keyvault/secrets/restore-secret/restore-secret>`_.

    Parameters
    ----------
    key_vault_uri : str
        Azure Key Vault URI.
    backup_blob : bytes
        The backup blob of the secret to be restored.

    Returns
    -------
    dict
        The restored secret details.
    """

    payload = {"value": backup_blob}
    response = _base_api(
        request=f"{key_vault_uri}/secrets/restore",
        client="keyvault",
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The secret has been successfully restored within the '{key_vault_uri}' Key Vault."
    )

    return response.json()


@log
def update_secret(
    key_vault_uri: str, secret_name: str, secret_version: str, enabled: bool = None
) -> dict:
    """
    Updates the attributes of a secret in the specified Azure Key Vault.

    This is a wrapper function for the following API: `Update Secret - Update Secret <https://learn.microsoft.com/rest/api/keyvault/secrets/update-secret/update-secret>`_.

    Parameters
    ----------
    key_vault_uri : str
        Azure Key Vault URI.
    secret_name : str
        Name of the secret to be updated in the Key Vault.
    secret_version : str
        Version of the secret to be updated in the Key Vault.
    enabled : bool, optional
        Specifies whether the secret is enabled or disabled.

    Returns
    -------
    dict
        The updated secret attributes.
    """

    payload = {"attributes": {"enabled": enabled}}

    response = _base_api(
        request=f"{key_vault_uri}/secrets/{secret_name}/{secret_version}",
        client="keyvault",
        method="patch",
        payload=payload,
    )
    print(
        f"{icons.green_dot} The '{secret_name}' secret has been successfully updated within the '{key_vault_uri}' Key Vault."
    )

    return response.json()
