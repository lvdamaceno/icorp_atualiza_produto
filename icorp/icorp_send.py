import json
import logging
import os
import requests
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_fixed

from utils import logging_config

logging_config()
logger = logging.getLogger(__name__)

session = requests.Session()

@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(5),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    retry_error_callback=lambda state: logger.error("Todas as tentativas falharam.")
)
def icorp_post(service, payload):
    logging.debug(json.dumps(payload, indent=2, ensure_ascii=False))
    tenant = os.getenv('ICORP_TENANT')
    logger.debug(f"🔗 POST Icorp")
    url = f'https://cc01.csicorpnet.com.br/CS50Integracao_API/rest/CS_IntegracaoV1/{service}?In_Tenant_ID={tenant}'
    try:
        resp = session.post(url, json=payload, timeout=(5, 60))
        logging.debug(f"STATUS: {resp.status_code}")
        logging.debug(f"BODY: {resp.text!r}")
        resp.raise_for_status()
        # if not resp.text.strip():
        #     return {}
        logging.info(f"✅ POST '{service}' bem-sucedido: {resp.status_code}")
        return
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Falha no POST '{service}': {e}")
        raise
