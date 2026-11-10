"""Teste de conectividade: envia um JSON do seu computador para o Volume (landing zone).

Valida 3 coisas de uma vez: token, rede e permissão de escrita no Unity Catalog.
"""
import io
import json
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()  # lê DATABRICKS_HOST e DATABRICKS_TOKEN do .env

VOLUME_DIR = "/Volumes/credito_lab/landing/api_raw"

w = WorkspaceClient()  # usa as variáveis de ambiente automaticamente
print("Conectado como:", w.current_user.me().user_name)

payload = {"origem": "teste_local", "enviado_em": datetime.now(timezone.utc).isoformat()}
caminho = f"{VOLUME_DIR}/teste_upload.json"

# overwrite=True: reenviar o mesmo arquivo não duplica (ideia de idempotência)
w.files.upload(caminho, io.BytesIO(json.dumps(payload).encode()), overwrite=True)
print("Enviado:", caminho)

for item in w.files.list_directory_contents(VOLUME_DIR):
    print(" -", item.name)
