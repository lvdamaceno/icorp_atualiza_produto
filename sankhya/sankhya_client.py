import json
import logging
import os

import requests
from dotenv import load_dotenv
from tenacity import before_sleep_log, retry, retry_if_exception, stop_after_attempt, wait_exponential

from utils import is_retryable, logging_config

load_dotenv()
logging_config()
logger = logging.getLogger(__name__)

snk_url_base = os.getenv('SANKHYA_URL_BASE')
snk_token    = os.getenv("SANKHYA_TOKEN")
snk_appkey   = os.getenv("SANKHYA_APP_KEY")
snk_password = os.getenv("SANKHYA_PASSWORD")
snk_username = os.getenv("SANKHYA_USERNAME")
# Conexão central do Sankhya (sankhya-central, rede interna do Render): com SANKHYA_PROXY_URL, as consultas passam por ela,
# que guarda o token, respeita os limites e pausa quando o Sankhya falha, junto com os outros apps. Sem a variável, segue o
# login direto de sempre (usuário e senha). 23/11/2026.
snk_proxy_url   = os.getenv("SANKHYA_PROXY_URL", "").rstrip("/")
snk_proxy_chave = os.getenv("SANKHYA_PROXY_CHAVE", "")
# a central pode segurar o pedido 120 s na fila e mais 60 s no Sankhya
TIMEOUT_CENTRAL = (10, 200)

# (connect, read) — consultas de listagem podem passar de 30s
TIMEOUT = (10, 120)

# status do service.sbr
STATUS_OK = ("1", "2")
STATUS_SESSAO_EXPIRADA = "3"
STATUS_CONCORRENCIA = "4"

session = requests.Session()

# token da sessão atual (um por processo)
_token = None


class SankhyaError(Exception):
    def __init__(self, message, retryable=False):
        super().__init__(message)
        self.retryable = retryable


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, max=30),
    retry=retry_if_exception(is_retryable),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True
)
def login():
    global _token
    if snk_proxy_url:
        return None  # a central cuida do login
    logger.info("🔒 Autenticando na API do Sankhya")
    headers = {
        'token':    snk_token,
        'appkey':   snk_appkey,
        'password': snk_password,
        'username': snk_username
    }
    resp = session.post(f'{snk_url_base}/login', headers=headers, timeout=30)
    resp.raise_for_status()
    token = resp.json().get("bearerToken")
    if not token:
        raise SankhyaError("Resposta de login não trouxe 'bearerToken'")
    _token = token
    return token


def _pela_central(service, payload):
    resp = session.post(
        f'{snk_proxy_url}/servico',
        headers={'x-proxy-chave': snk_proxy_chave},
        json={'nome': service, 'corpo': payload.get('requestBody', {}), 'rotulo': 'icorp'},
        timeout=TIMEOUT_CENTRAL
    )
    try:
        corpo = resp.json()
    except ValueError:
        corpo = {}
    if resp.status_code != 200 or not isinstance(corpo.get('d'), dict):
        # 502/503/504/429: Sankhya falhando, fila cheia ou pausa da central; vale tentar de novo (com a espera do @retry)
        raise SankhyaError(
            f"Conexão central HTTP {resp.status_code}: {corpo.get('erro') or resp.text[:300]}",
            retryable=resp.status_code in (429, 502, 503, 504)
        )
    return corpo['d']


def _send(service, payload):
    resp = session.post(
        f'{snk_url_base}/gateway/v1/mge/service.sbr',
        headers={'Authorization': f'Bearer {_token}'},
        params={'serviceName': service, 'outputType': 'json'},
        json=payload,
        timeout=TIMEOUT
    )
    if resp.status_code == 401:
        return resp, None
    resp.raise_for_status()
    return resp, resp.json()


@retry(
    stop=stop_after_attempt(5),
    # espera maior que o timeout de concorrência: a requisição anterior pode ainda estar rodando no Sankhya
    wait=wait_exponential(multiplier=5, max=60),
    retry=retry_if_exception(is_retryable),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True
)
def snk_post(service, payload):
    logger.debug(f"🔗 POST {service}")
    if snk_proxy_url:
        result = _pela_central(service, payload)
        status = str(result.get("status"))
        if status not in STATUS_OK:
            raise SankhyaError(f"{service} retornou status {status}: {result.get('statusMessage')}",
                               retryable=status == STATUS_CONCORRENCIA)
        return result
    if _token is None:
        login()

    resp, result = _send(service, payload)
    if resp.status_code == 401 or (result and str(result.get("status")) == STATUS_SESSAO_EXPIRADA):
        logger.warning("🔒 Sessão Sankhya expirada, refazendo login")
        login()
        resp, result = _send(service, payload)
        if resp.status_code == 401:
            resp.raise_for_status()

    logger.debug(json.dumps(result, indent=2, ensure_ascii=False))
    status = str(result.get("status"))
    if status not in STATUS_OK:
        raise SankhyaError(
            f"{service} retornou status {status}: {result.get('statusMessage')}",
            retryable=status == STATUS_CONCORRENCIA
        )
    return result
