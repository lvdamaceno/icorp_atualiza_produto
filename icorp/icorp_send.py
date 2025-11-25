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
# default headers (ajusta o User-Agent se no seu PC funcionar outro)
session.headers.update({
    "User-Agent": "MyApp/1.0 (+https://seudominio.example)",
    "Accept": "application/json",
    "Content-Type": "application/json",
})
adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=5, pool_block=True)
session.mount("https://", adapter)
session.mount("http://", adapter)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    retry_error_callback=lambda state: logger.error("Todas as tentativas falharam.")
)


def icorp_post(service, payload, max_attempts=3):
    logging.debug(json.dumps(payload, indent=2, ensure_ascii=False))
    tenant = os.getenv('ICORP_TENANT')
    url = f'https://cc01.csicorpnet.com.br/CS50Integracao_API/rest/CS_IntegracaoV1/{service}?In_Tenant_ID={tenant}'

    for attempt in range(1, max_attempts + 1):
        start = time.perf_counter()
        try:
            logging.debug(f"🔗 POST Icorp attempt {attempt} -> {url}")
            # timeout: (connect, read)
            resp = session.post(url, json=payload, timeout=(5, 60))
            elapsed = time.perf_counter() - start

            status = resp.status_code
            logging.debug(f"STATUS: {status} | Latency: {elapsed:.3f}s")
            # tentamos capturar o cf-ray tanto no header quanto no body
            cf_ray = resp.headers.get("cf-ray") or resp.headers.get("CF-RAY")
            if cf_ray:
                logging.warning(f"cf-ray header presente: {cf_ray}")

            # se o content-type indicar HTML ou o body começar com <!doctype html, provavelmente Cloudflare/WAF
            content_type = resp.headers.get("Content-Type", "")
            body_start = (resp.text or "")[:1024].lower()

            if "text/html" in content_type or body_start.startswith("<!doctype html") or "sorry, you have been blocked" in body_start:
                logging.error("Resposta HTML recebida — possível bloqueio Cloudflare/WAF.")
                # Logue o cf-ray se disponível (muito útil para infra do site)
                logging.error("cf-ray: %s", cf_ray)
                logging.debug("Resposta (trecho): %s", resp.text[:2000])

                # Não tentar contornar. Se ainda quiser tentar mais vezes, o loop fará.
                # Se for a última tentativa, retorna None (ou raise), conforme sua preferência.
                if attempt == max_attempts:
                    logging.error("Bloqueio persistiu após %s tentativas. Abortando.", max_attempts)
                    return None
                else:
                    wait = 1 + (attempt - 1)   # 1, 2, 3
                    logging.info("Aguardando %s segundo(s) antes de nova tentativa...", wait)
                    time.sleep(wait)
                    continue

            # Se chegou aqui e status 2xx -> OK
            try:
                resp.raise_for_status()
            except requests.HTTPError as e:
                logging.warning("HTTPError recebido: %s | body trecho: %s", e, resp.text[:500])
                # 403/401: não adianta retry sem mudar credenciais/allowlist
                if status in (401, 403):
                    logging.error("Status %s: verifique credenciais / allowlist.", status)
                    return None
                # para outros status, permite retry conforme tentativa
                if attempt == max_attempts:
                    raise
                wait = 1 + (attempt - 1)
                logging.info("Aguardando %s segundo(s) antes de nova tentativa...", wait)
                time.sleep(wait)
                continue

            # sucesso
            logging.info("✅ POST '%s' bem-sucedido: %s", service, status)
            logging.info("🕐 [Icorp] Latência: %.3fs para serviço %s", elapsed, service)
            # retorna o json quando houver (ou None se vazio)
            try:
                return resp.json()
            except ValueError:
                logging.debug("Resposta sem JSON, retornando texto.")
                return resp.text

        except requests.RequestException as e:
            logging.exception("Erro na requisição (tentativa %s): %s", attempt, e)
            if attempt == max_attempts:
                logging.error("Falhou após %s tentativas.", max_attempts)
                raise
            wait = 1 + (attempt - 1)
            logging.info("Aguardando %s segundo(s) antes de nova tentativa...", wait)
            time.sleep(wait)