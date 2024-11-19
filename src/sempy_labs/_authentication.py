from typing import Literal
from sempy.fabric._token_provider import TokenProvider
import notebookutils
from azure.identity import ClientSecretCredential


class ServicePrincipalTokenProvider(TokenProvider):
    """
    Implementation of the sempy.fabric.TokenProvider to be used with Service Principal.

    For more information on Service Principal check `Application and service principal objects in Microsoft Entra ID <https://learn.microsoft.com/en-us/entra/identity-platform/app-objects-and-service-principals?tabs=browser#service-principal-object>`_
    """

    def __init__(self, credential: ClientSecretCredential):
        self.credential = credential

    @classmethod
    def with_aad_application_key_authentication(
        cls, tenant_id: str, client_id: str, client_secret: str
    ):
        """
        Create the ServicePrincipalTokenProvider with the Service Principal information.

        ***USE THIS ONE ONLY FOR TEST PURPOSE. FOR PRODUCTION WE RECOMMEND CALLING ServicePrincipalTokenProvider.from_key_vault()***

        Parameters
        ----------
        tenant_id : str
            Fabric Tenant ID.
        client_id : str
            Service Principal App Client ID.
        client_secret : str
            Service Principal Secret.

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
    def with_azure_key_vault(
        cls,
        key_vault_uri: str,
        key_vault_tenant_id: str,
        key_vault_client_id: str,
        key_vault_client_secret: str,
    ):
        """
        Create the ServicePrincipalTokenProvider providing the information with the Service Principal information.

        For more information on Azure Key Vault check `About Azure Key Vault <https://learn.microsoft.com/en-us/azure/key-vault/general/overview>`_

        Parameters
        ----------
        key_vault_uri : str
            Azure Key Vault URI.
        key_vault_tenant_id : str,
            Name of the secret in the Key Vault with the Fabric Tenant ID.
        key_vault_client_id : str,
            Name of the secret in the Key Vault with the Service Principal Client ID.
        key_vault_client_secret : str
            Name of the secret in the Key Vault with the Service Principal Client Secret.

        Returns
        -------
        sempy.fabric.TokenProvider
            Token provider to be used with FabricRestClient or PowerBIRestClient.
        """
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

    def __call__(self, audience: Literal["pbi", "storage"] = "pbi") -> str:
        """
        Parameters
        ----------
        audience : Literal["pbi", "storage"] = "pbi") -> str
            Literal if it's for PBI/Fabric API call or OneLake/Storage Account call.
        """
        if audience == "pbi":
            return self.credential.get_token(
                "https://analysis.windows.net/powerbi/api/.default"
            ).token
        elif audience == "storage":
            return self.credential.get_token("https://storage.azure.com/.default").token
        else:
            raise NotImplementedError
