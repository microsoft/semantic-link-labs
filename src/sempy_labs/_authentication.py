from typing import Literal, Optional
from sempy.fabric._token_provider import TokenProvider
from azure.identity import ClientSecretCredential
from sempy._utils._log import log
from contextlib import contextmanager
import contextvars


class ServicePrincipalTokenProvider(TokenProvider):
    """
    Implementation of the sempy.fabric.TokenProvider to be used with Service Principal.

    For more information on Service Principal see: `Application and service principal objects in Microsoft Entra ID <https://learn.microsoft.com/en-us/entra/identity-platform/app-objects-and-service-principals?tabs=browser#service-principal-object>`_
    """

    def __init__(self, credential: ClientSecretCredential):

        self.credential = credential

    @classmethod
    def from_aad_application_key_authentication(
        cls, tenant_id: str, client_id: str, client_secret: str
    ):
        """
        Generates the ServicePrincipalTokenProvider, providing the Service Principal information.

        ***USE THIS ONE ONLY FOR TEST PURPOSE. FOR PRODUCTION WE RECOMMEND CALLING ServicePrincipalTokenProvider.from_azure_key_vault()***

        Parameters
        ----------
        tenant_id : str
            The Fabric Tenant ID.
        client_id : str
            The Service Principal Application Client ID.
        client_secret : str
            The Service Principal Client Secret.

        Returns
        -------
        sempy.fabric.TokenProvider
            Token provider to be used with FabricRestClient or PowerBIRestClient.
        """
        credential = ClientSecretCredential(
            tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
        )

        cls.tenant_id = tenant_id
        cls.client_id = client_id
        cls.client_secret = client_secret

        return cls(credential)

    @classmethod
    def from_azure_key_vault(
        cls,
        key_vault_uri: str,
        key_vault_tenant_id: str,
        key_vault_client_id: str,
        key_vault_client_secret: str,
    ):
        """
        Generates the ServicePrincipalTokenProvider, providing the Azure Key Vault details.

        For more information on Azure Key Vault, `click here <https://learn.microsoft.com/en-us/azure/key-vault/general/overview>`_.

        Parameters
        ----------
        key_vault_uri : str
            Azure Key Vault URI.
        key_vault_tenant_id : str
            Name of the secret in the Key Vault with the Fabric Tenant ID.
        key_vault_client_id : str
            Name of the secret in the Key Vault with the Service Principal Client ID.
        key_vault_client_secret : str
            Name of the secret in the Key Vault with the Service Principal Client Secret.

        Returns
        -------
        sempy.fabric.TokenProvider
            Token provider to be used with FabricRestClient or PowerBIRestClient.
        """

        import notebookutils

        tenant_id = notebookutils.credentials.getSecret(
            key_vault_uri, key_vault_tenant_id
        )
        client_id = notebookutils.credentials.getSecret(
            key_vault_uri, key_vault_client_id
        )
        client_secret = notebookutils.credentials.getSecret(
            key_vault_uri, key_vault_client_secret
        )

        credential = ClientSecretCredential(
            tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
        )

        cls.tenant_id = tenant_id
        cls.client_id = client_id
        cls.client_secret = client_secret

        return cls(credential)

    def __call__(
        self,
        audience: Literal[
            "pbi", "storage", "azure", "graph", "asazure", "keyvault"
        ] = "pbi",
        region: Optional[str] = None,
    ) -> str:
        """
        Parameters
        ----------
        audience : Literal["pbi", "storage", "azure", "graph", "asazure", "keyvault"] = "pbi") -> str
            Literal if it's for PBI/Fabric API call or OneLake/Storage Account call.
        region : str, default=None
            The region of the Azure Analysis Services. For example: 'westus2'.
        """
        if audience == "pbi":
            return self.credential.get_token(
                "https://analysis.windows.net/powerbi/api/.default"
            ).token
        elif audience == "storage":
            return self.credential.get_token("https://storage.azure.com/.default").token
        elif audience == "azure":
            return self.credential.get_token(
                "https://management.azure.com/.default"
            ).token
        elif audience == "graph":
            return self.credential.get_token(
                "https://graph.microsoft.com/.default"
            ).token
        elif audience == "asazure":
            return self.credential.get_token(
                f"https://{region}.asazure.windows.net/.default"
            ).token
        elif audience == "keyvault":
            return self.credential.get_token("https://vault.azure.net/.default").token
        else:
            raise NotImplementedError


def _get_headers(
    token_provider: str,
    audience: Literal[
        "pbi", "storage", "azure", "graph", "asazure", "keyvault"
    ] = "azure",
):
    """
    Generates headers for an API request.
    """

    token = token_provider(audience=audience)

    headers = {"Authorization": f"Bearer {token}"}

    if audience == "graph":
        headers["ConsistencyLevel"] = "eventual"
    else:
        headers["Content-Type"] = "application/json"

    return headers


token_provider = contextvars.ContextVar("token_provider", default=None)


@log
@contextmanager
def service_principal_authentication(
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
):
    """
    Establishes an authentication via Service Principal.

    Parameters
    ----------
    key_vault_uri : str
        Azure Key Vault URI.
    key_vault_tenant_id : str
        Name of the secret in the Key Vault with the Fabric Tenant ID.
    key_vault_client_id : str
        Name of the secret in the Key Vault with the Service Principal Client ID.
    key_vault_client_secret : str
        Name of the secret in the Key Vault with the Service Principal Client Secret.
    """

    # Save the prior state
    prior_token = token_provider.get()

    # Set the new token_provider in a thread-safe manner
    token_provider.set(
        ServicePrincipalTokenProvider.from_azure_key_vault(
            key_vault_uri=key_vault_uri,
            key_vault_tenant_id=key_vault_tenant_id,
            key_vault_client_id=key_vault_client_id,
            key_vault_client_secret=key_vault_client_secret,
        )
    )
    try:
        yield
    finally:
        # Restore the prior state
        if prior_token is None:
            token_provider.set(None)
        else:
            token_provider.set(prior_token)
