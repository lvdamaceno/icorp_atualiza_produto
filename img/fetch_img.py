import logging

import requests
from requests.adapters import HTTPAdapter
from tenacity import retry, stop_after_attempt, wait_fixed, before_sleep_log

from utils import logging_config

session = requests.Session()
adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=3, pool_block=True)
session.mount("https://", adapter)
session.mount("http://", adapter)

logging_config()
logger = logging.getLogger(__name__)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(5),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    retry_error_callback=lambda state: logger.error("Todas as tentativas falharam.")
)
def fetch_img(product):
    url = f'https://img.casacontente.com.br/{product}.jpg'
    try:
        response = session.get(url)
        response.raise_for_status()
        logging.info(f"✅ GET 'Imagem' bem-sucedido: {response.status_code}")
        return url
    except requests.exceptions.RequestException as e:
        logging.info(f"🚫 GET 'Imagem' mal-sucedido")
        logging.error(e)
