import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

_credential = DefaultAzureCredential()

_keyVaultName = os.environ["AZURE_VAULT_NAME"]
_KVUri = f"https://{_keyVaultName}.vault.azure.net"

_client = SecretClient(vault_url=_KVUri, credential=_credential)

NULOGY_SECRET_KEY           = _client.get_secret("NulogySecretKey").value
AZURE_DB_CONNECTION_STRING  = _client.get_secret("AzureDatabaseConnectionString").value
DAX_DB_CONNECTION_STRING    = _client.get_secret("DAX-DB-ConnectionString").value
SMTP_USERNAME               = _client.get_secret("SMTP-Username").value
SMTP_PASSWORD               = _client.get_secret("SMTP-Password").value
