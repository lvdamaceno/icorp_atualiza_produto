import json
import logging
import os
import requests
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_fixed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(5),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    retry_error_callback=lambda state: logger.error("Todas as tentativas falharam.")
)
def icorp_post(service, payload):
    tenant = os.getenv('ICORP_TENANT')
    logger.info(f"🟢 POST Icorp")
    url = f'https://cc01.csicorpnet.com.br/CS50Integracao_API/rest/CS_IntegracaoV1/{service}?In_Tenant_ID={tenant}'
    resp = requests.post(url, json=payload, timeout=30)
    logging.info(f"STATUS: {resp.status_code}")
    logging.info(f"BODY: {resp.text!r}")
    resp.raise_for_status()
    # se não veio nada no corpo, considere sucesso e retorne algo
    if not resp.text.strip():
        return {}  # ou outro indicador de sucesso
    # só aí faça o parse de JSON
    result = resp.json()
    logger.info(json.dumps(result, indent=2, ensure_ascii=False))
    return result

