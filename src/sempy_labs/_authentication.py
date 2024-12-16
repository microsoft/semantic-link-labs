from typing import Literal
from sempy.fabric._token_provider import TokenProvider
from azure.identity import ClientSecretCredential


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

        return cls(credential)

    def __call__(
        self, audience: Literal["pbi", "storage", "azure", "graph"] = "pbi"
    ) -> str:
        """
        Parameters
        ----------
        audience : Literal["pbi", "storage", "azure", "graph"] = "pbi") -> str
            Literal if it's for PBI/Fabric API call or OneLake/Storage Account call.
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
        else:
            raise NotImplementedError


def _get_headers(
    token_provider: str, audience: Literal["pbi", "storage", "azure", "graph"] = "azure"
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
