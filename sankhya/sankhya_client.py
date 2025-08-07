import json
import logging
import os

from dotenv import load_dotenv
import requests
from requests.adapters import HTTPAdapter
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_fixed

from utils import logging_config

load_dotenv()
logging_config()
logger = logging.getLogger(__name__)

snk_url_base = os.getenv('SANKHYA_URL_BASE')
snk_token    = os.getenv("SANKHYA_TOKEN")
snk_appkey   = os.getenv("SANKHYA_APP_KEY")
snk_password = os.getenv("SANKHYA_PASSWORD")
snk_username = os.getenv("SANKHYA_USERNAME")

session = requests.Session()
adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=3, pool_block=True)
session.mount("https://", adapter)
session.mount("http://", adapter)

@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(5),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    retry_error_callback=lambda state: logger.error("Todas as tentativas falharam.")
)
def login():
    logger.info("🔒 Autenticando na API do Sankhya")
    url = f'{snk_url_base}/login'
    headers = {
        'token':    snk_token,
        'appkey':   snk_appkey,
        'password': snk_password,
        'username': snk_username
    }
    try:
        resp = session.post(url, headers=headers, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        token = data.get("bearerToken")
        if not token:
            raise ValueError("Resposta não trouxe 'bearerToken'")
        return token
    except requests.exceptions.RequestException as e:
        logger.error(e)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(5),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    retry_error_callback=lambda state: logger.error("Todas as tentativas falharam.")
)
def snk_post(token, service, payload):
    logger.debug(f"🔗 POST {service}")
    url = f'{snk_url_base}/gateway/v1/mge/service.sbr'
    params = {'serviceName': service, 'outputType': 'json'}
    headers = {'Authorization': f'Bearer {token}'}
    try:
        resp = session.post(url, headers=headers, params=params, json=payload, timeout=10)
        resp.raise_for_status()
        result = resp.json()
        logger.debug(json.dumps(result, indent=2, ensure_ascii=False))
        return result
    except requests.exceptions.RequestException as e:
        logger.error(e)
