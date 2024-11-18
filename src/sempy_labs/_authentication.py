from typing import Literal
from sempy.fabric._token_provider import TokenProvider
import notebookutils
from azure.identity import ClientSecretCredential

class ServicePrincipalTokenProviderFromKeyVault(TokenProvider):   
    """
    Implementation of the sempy.fabric.TokenProvider to be used with Service Principal providing the Azure Key Vault information.

    For more information on Serice Principal check `Application and service principal objects in Microsoft Entra ID <https://learn.microsoft.com/en-us/entra/identity-platform/app-objects-and-service-principals?tabs=browser#service-principal-object>`_

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

    def __init__(self, 
        key_vault_uri:str, 
        key_vault_tenant_id:str,
        key_vault_client_id:str,
        key_vault_client_secret:str
        ):
        """
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

        self.key_vault_uri = key_vault_uri
        self.key_vault_tenant_id = key_vault_tenant_id
        self.key_vault_client_id = key_vault_client_id
        self.key_vault_client_secret = key_vault_client_secret

        self.tenant_id = notebookutils.credentials.getSecret(key_vault_uri, key_vault_tenant_id)
        self.client_id = notebookutils.credentials.getSecret(key_vault_uri, key_vault_client_id)
        self.client_secret = notebookutils.credentials.getSecret(key_vault_uri, key_vault_client_secret)

        self.credential = ClientSecretCredential(
            tenant_id=self.tenant_id, client_id=self.client_id, client_secret=self.client_secret
        ) 

    def __call__(self, audience: Literal["pbi", "storage"] = "pbi") -> str:
        """
        Parameters
        ----------
        audience : Literal["pbi", "storage"] = "pbi") -> str
            Literal if it's for PBI/Fabric API call or OneLake/Storage Account call.
        """
        if audience == "pbi":
            return self.credential.get_token("https://analysis.windows.net/powerbi/api/.default").token
        elif audience == "storage":
            return self.credential.get_token("https://storage.azure.com/.default").token
        else:
            raise NotImplementedError

class ServicePrincipalTokenProvider(TokenProvider):   
    """
    Implementation of the sempy.fabric.TokenProvider to be used with Service Principal providing the SP properties.

    For more information on Serice Principal check `Application and service principal objects in Microsoft Entra ID <https://learn.microsoft.com/en-us/entra/identity-platform/app-objects-and-service-principals?tabs=browser#service-principal-object>`_

    ***USE THIS ONE ONLY FOR TEST PURPOSE. FOR PRODUCTION WE RECOMEND THE ServicePrincipalTokenProviderWithKeyVault***

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

    def __init__(self, 
        tenant_id:str,
        client_id:str,
        client_secret:str
        ):
        """
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
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

        self.credential = ClientSecretCredential(
            tenant_id=self.tenant_id, client_id=self.client_id, client_secret=self.client_secret
        ) 

    def __call__(self, audience: Literal["pbi", "storage"] = "pbi") -> str:
        """
        Parameters
        ----------
        audience : Literal["pbi", "storage"] = "pbi") -> str
            Literal if it's for PBI/Fabric API call or OneLake/Storage Account call.
        """
        if audience == "pbi":
            return self.credential.get_token("https://analysis.windows.net/powerbi/api/.default").token
        elif audience == "storage":
            return self.credential.get_token("https://storage.azure.com/.default").token
        else:
            raise NotImplementedError
