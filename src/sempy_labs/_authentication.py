from typing import Literal
from sempy.fabric._token_provider import TokenProvider
import notebookutils
from azure.identity import ClientSecretCredential

class ServicePrincipalTokenProviderWithKeyVault(TokenProvider):   

    def __init__(self, key_vault_uri, key_vault_tenant_id, key_vault_client_id, key_vault_client_secret):
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
        if audience == "pbi":
            return self.credential.get_token("https://analysis.windows.net/powerbi/api/.default").token
        elif audience == "storage":
            return self.credential.get_token("https://storage.azure.com/.default").token
        else:
            raise NotImplementedError

class ServicePrincipalTokenProvider(TokenProvider):   

    def __init__(self, tenant_id, client_id, client_secret):

        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

        self.credential = ClientSecretCredential(
            tenant_id=self.tenant_id, client_id=self.client_id, client_secret=self.client_secret
        ) 

    def __call__(self, audience: Literal["pbi", "storage"] = "pbi") -> str:
        if audience == "pbi":
            return self.credential.get_token("https://analysis.windows.net/powerbi/api/.default").token
        elif audience == "storage":
            return self.credential.get_token("https://storage.azure.com/.default").token
        else:
            raise NotImplementedError
