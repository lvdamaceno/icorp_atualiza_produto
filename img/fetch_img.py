import logging
import time

import requests
from tenacity import before_sleep_log, retry, retry_if_exception, stop_after_attempt, wait_exponential

from utils import is_retryable, logging_config

session = requests.Session()

logging_config()
logger = logging.getLogger(__name__)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, max=10),
    retry=retry_if_exception(is_retryable),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True
)
def fetch_img(product):
    """Retorna a URL da imagem se ela existir, ou None se não houver imagem (404)."""
    url = f'https://img.casacontente.com.br/{product}.jpg'
    start = time.perf_counter()
    # stream=True: só verifica a existência, sem baixar a imagem inteira
    with session.get(url, stream=True, timeout=(5, 30)) as response:
        if response.status_code == 404:
            logger.info(f"🖼️ Produto {product} sem imagem")
            return None
        response.raise_for_status()
    elapsed = time.perf_counter() - start
    logger.info(f"🕐 [Imagem] Latência: {elapsed:.3f}s para imagem do produto {product}")
    return url
