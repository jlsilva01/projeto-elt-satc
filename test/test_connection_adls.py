import os
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import AzureError

token = os.environ["ADLS_SAS_TOKEN"]
print("DEBUG token decodificado:", token)

account_url = "https://datalake00a2a47be9c43488.dfs.core.windows.net"
try:
    client = DataLakeServiceClient(account_url=account_url, credential=token)
    print("Tentando listar file systems...")
    for fs in client.list_file_systems():
        print(" -", fs.name)
except AzureError as e:
    print("AZURE ERROR:", e)            # mensagem de erro resumida
    print("AZURE DETAILS:", e.error_code, getattr(e, "response", None))
    raise  # relança para traceback completo
