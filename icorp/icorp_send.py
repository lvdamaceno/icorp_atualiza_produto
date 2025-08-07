import json
import logging
import os
import time

import requests
from requests.adapters import HTTPAdapter
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_fixed

from utils import logging_config

logging_config()
logger = logging.getLogger(__name__)

session = requests.Session()
adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=5, pool_block=True)
session.mount("https://", adapter)
session.mount("http://", adapter)

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
        start = time.perf_counter()
        resp = session.post(url, json=payload, timeout=(5, 60))
        logging.debug(f"STATUS: {resp.status_code}")
        logging.debug(f"BODY: {resp.text!r}")
        resp.raise_for_status()
        logging.info(f"✅ POST '{service}' bem-sucedido: {resp.status_code}")
        elapsed = time.perf_counter() - start  # tempo decorrido
        logging.info(f"🕐 [Icorp] Latência: {elapsed:.3f}s para serviço {service}")
        return
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Falha no POST '{service}': {e}")
        raise
