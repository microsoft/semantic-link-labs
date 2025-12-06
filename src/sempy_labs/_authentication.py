from typing import Dict, Literal, Optional
from azure.core.credentials import AccessToken, TokenCredential
from azure.identity import ClientSecretCredential
from sempy._utils._log import log
from contextlib import contextmanager
import contextvars


class ServicePrincipalTokenProvider(TokenCredential):
    """
    A class to acquire authentication token with Service Principal.

    For more information on Service Principal see: `Application and service principal objects in Microsoft Entra ID <https://learn.microsoft.com/en-us/entra/identity-platform/app-objects-and-service-principals?tabs=browser#service-principal-object>`_
    """

    _shorthand_scopes: Dict[str, str] = {
        "pbi": "https://analysis.windows.net/powerbi/api/.default",
        "storage": "https://storage.azure.com/.default",
        "azure": "https://management.azure.com/.default",
        "graph": "https://graph.microsoft.com/.default",
        "asazure": "https://{region}.asazure.windows.net/.default",
        "keyvault": "https://vault.azure.net/.default",
    }

    def __init__(self, credential: ClientSecretCredential):

        self.credential = credential

    @classmethod
    def from_aad_application_key_authentication(
        cls, tenant_id: str, client_id: str, client_secret: str
    ) -> "ServicePrincipalTokenProvider":
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
        ServicePrincipalTokenProvider
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
    ) -> "ServicePrincipalTokenProvider":
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
        ServicePrincipalTokenProvider
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
        # Check if audience is supported
        if audience not in self._shorthand_scopes:
            raise NotImplementedError

        return self.get_token(audience, region=region).token

    def get_token(self, *scopes, **kwargs) -> AccessToken:
        """
        Gets a token for the specified scopes.

        Parameters
        ----------
        *scopes : str
            The scopes for which to obtain a token.
        **kwargs : dict
            Additional parameters to pass to the token request.

        Returns
        -------
        AccessToken
            The access token.
        """
        if len(scopes) == 0:
            scopes = ("pbi",)

        region = kwargs.pop("region", None)
        scopes = [
            self._get_fully_qualified_scope(scope, region=region) for scope in scopes
        ]
        return self.credential.get_token(*scopes, **kwargs)

    def _get_fully_qualified_scope(
        self, scope: str, region: Optional[str] = None
    ) -> str:
        """
        Resolve to fully qualified scope if Fabric short-handed scope is given.
        Otherwise, return the original scope.

        Parameters
        ----------
        scope : str
            The scope to resolve.
        region : str, default=None
            The specific region to use to resolve scope.
            Required if scope is "asazure".

        Returns
        -------
        str
            The resolved scope.
        """
        fully_qualified_scope = self._shorthand_scopes.get(scope, scope)

        if scope == "asazure":
            if region is None:
                raise ValueError("Region is required for 'asazure' scope")
            return fully_qualified_scope.format(region=region)

        return fully_qualified_scope


def _get_headers(
    token_provider: TokenCredential,
    audience: Literal[
        "pbi", "storage", "azure", "graph", "asazure", "keyvault"
    ] = "azure",
):
    """
    Generates headers for an API request.
    """

    token = token_provider.get_token(audience).token

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
        from sempy.fabric import set_service_principal

        with set_service_principal(
            (key_vault_uri, key_vault_tenant_id),
            (key_vault_uri, key_vault_client_id),
            client_secret=(key_vault_uri, key_vault_client_secret),
        ):
            yield
    finally:
        # Restore the prior state
        if prior_token is None:
            token_provider.set(None)
        else:
            token_provider.set(prior_token)
