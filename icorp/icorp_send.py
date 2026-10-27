import json
import logging
import os
import time

import requests
from tenacity import before_sleep_log, retry, retry_if_exception, stop_after_attempt, wait_exponential

from utils import is_retryable, logging_config

logging_config()
logger = logging.getLogger(__name__)

session = requests.Session()
session.headers.update({
    "User-Agent": "CasaContente-IcorpSync/1.0",
    "Accept": "application/json",
    "Content-Type": "application/json",
})


class IcorpBlockedError(Exception):
    """Resposta HTML (Cloudflare/WAF): costuma ser temporária, tenta de novo."""
    retryable = True


def _is_html_block(resp):
    content_type = resp.headers.get("Content-Type", "")
    body_start = (resp.text or "")[:1024].lower()
    return ("text/html" in content_type
            or body_start.startswith("<!doctype html")
            or "sorry, you have been blocked" in body_start)


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, max=30),
    retry=retry_if_exception(is_retryable),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True
)
def icorp_post(service, payload):
    if not payload:
        raise ValueError(f"Payload vazio para o serviço {service}")

    logger.debug(json.dumps(payload, indent=2, ensure_ascii=False))
    tenant = os.getenv('ICORP_TENANT')
    url = f'https://cc01.csicorpnet.com.br/CS50Integracao_API/rest/CS_IntegracaoV1/{service}?In_Tenant_ID={tenant}'

    start = time.perf_counter()
    # timeout: (connect, read)
    resp = session.post(url, json=payload, timeout=(10, 60))
    elapsed = time.perf_counter() - start
    logger.debug(f"STATUS: {resp.status_code} | Latency: {elapsed:.3f}s")

    if _is_html_block(resp):
        cf_ray = resp.headers.get("cf-ray")
        logger.debug("Resposta (trecho): %s", resp.text[:2000])
        raise IcorpBlockedError(
            f"Resposta HTML em {service} (status {resp.status_code}, cf-ray {cf_ray}) — possível bloqueio Cloudflare/WAF"
        )

    if not resp.ok:
        raise requests.HTTPError(
            f"{resp.status_code} em {service}: {resp.text[:500]}", response=resp
        )

    logger.info("✅ POST '%s' bem-sucedido: %s", service, resp.status_code)
    logger.info("🕐 [Icorp] Latência: %.3fs para serviço %s", elapsed, service)
    try:
        return resp.json()
    except ValueError:
        logger.debug("Resposta sem JSON, retornando texto.")
        return resp.text
