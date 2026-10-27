import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

from icorp.icorp_send import icorp_post
from sankhya.sankhya_fetch import sankhya_fetch_json_produto, sankhya_fetch_json_estoque, sankhya_fetch_json_parceiro

logger = logging.getLogger(__name__)

# aborta a execução se tudo está falhando (API fora do ar), em vez de insistir por horas
MAX_FALHAS_CONSECUTIVAS = 20

IMG_URL = 'https://img.casacontente.com.br/{codprod}.jpg'


def process_product(product):
    logger.info(f"🚀 Enviando dados do produto {product}")
    dados_produto = sankhya_fetch_json_produto(product)
    if not dados_produto:
        raise ValueError(f"CC_CS_JSON_PRODUTO não retornou dados para o produto {product}")
    icorp_post('ProdutoUpdate', dados_produto)


def process_codbarra(product):
    logger.info(f"🚀 Enviando codbarra do produto {product}")
    codbarra = [
        {
            "CodigoProduto": f"{product}",
            "EAN_Tributavel": f"{product}",
            "UnVendaVarejo": "UN",
            "FatorConversao": "",
            "TipoConversao": "",
            "TipoCBarra": "Sistema",
            "IsImprimeEtq": False
        }
    ]
    icorp_post('CodigoBarra', codbarra)


def process_image(product):
    # URL padrão sempre enviada, mesmo sem foto: passa a funcionar quando a foto for publicada
    img_url = IMG_URL.format(codprod=product)
    logger.info(f"🚀 Enviando imagem do produto {product}")
    imagem = [
        {
            "CodigoProduto": f"{product}",
            "Ordem": 0,
            "Descricao": f"{product}.jpg",
            "URLImagem": img_url,
            "IsImgPadrao": False
        }
    ]
    icorp_post('ProdutoImagens', imagem)


def process_estoque(product):
    logger.info(f"🚀 Enviando estoque do produto {product}")
    estoque = sankhya_fetch_json_estoque(product)
    if not estoque:
        logger.warning(f"⚠️ CC_CS_JSON_ESTOQUE não retornou dados para o produto {product}, estoque não enviado")
        return
    icorp_post('Saldos_Atualiza', estoque)


def integrate_product(product):
    process_product(product)
    process_codbarra(product)
    process_image(product)
    process_estoque(product)


def process_parceiro(parceiro):
    logger.info(f"🚀 Enviando dados do parceiro {parceiro}")

    tentativas = 3
    dados_parceiro = None
    for tentativa in range(1, tentativas + 1):
        dados_parceiro = sankhya_fetch_json_parceiro(parceiro)
        if dados_parceiro:
            break
        if tentativa < tentativas:
            logger.warning(
                f"⚠️ dados do parceiro {parceiro} vazio na tentativa {tentativa}/{tentativas}. "
                f"Tentando novamente em {tentativa} segundo(s)..."
            )
            time.sleep(tentativa)

    if not dados_parceiro:
        logger.warning(f"⚠️ dados do parceiro {parceiro} vazio após {tentativas} tentativas, envio ignorado")
        return

    icorp_post('Cliente', dados_parceiro)


def _run_batch(items, label, handler):
    """Processa cada item isoladamente e retorna a lista dos que falharam."""
    total = len(items)
    logger.info(f"Total de envios programados: {total}")
    logger.info('=' * 45)

    failures = []
    consecutivas = 0
    media = 0.0
    inicio = time.perf_counter()

    for count, item in enumerate(items, start=1):
        start = time.perf_counter()
        logger.info(f"▶️ Iniciando integração do {label}: {item}")

        try:
            handler(item)
            consecutivas = 0
        except Exception:
            logger.exception(f"❌ Falha na integração do {label} {item}")
            failures.append(item)
            consecutivas += 1
            if consecutivas >= MAX_FALHAS_CONSECUTIVAS:
                raise RuntimeError(
                    f"{consecutivas} falhas consecutivas ({label}s {failures[-consecutivas:]}), abortando execução"
                )

        elapsed = time.perf_counter() - start
        # atualiza média incremental
        media += (elapsed - media) / count

        restante = total - count
        minutes = (restante * media) / 60
        hours = minutes / 60

        logger.info(f"🕐 Tempo desta iteração: {elapsed:.3f}s")
        logger.info(f"🕐 Tempo médio até agora: {media:.3f}s")
        logger.info(f"⚙️ Envio restante: {restante} ({minutes:.3f}m) / ({hours:.3f}h)")
        logger.info('=' * 45)

    elapsed = time.perf_counter() - inicio
    logger.info(f"🕐 Tempo total: {elapsed:.3f}s")
    logger.info(f"📊 Resultado: {total - len(failures)} ok / {len(failures)} falha(s)")
    if failures:
        logger.error(f"❌ {label.capitalize()}s com falha: {failures}")
    return failures


def process_integration(products):
    return _run_batch(products, 'produto', integrate_product)


def process_parceiros_integration(parceiros):
    return _run_batch(parceiros, 'parceiro', process_parceiro)


def run_lotes(run_lote, lotes):
    """Executa os lotes em processos paralelos e retorna o total de itens com falha.

    Cada processo faz seu próprio login (sessão HTTP própria no Sankhya).
    Paralelismo alto derruba o Sankhya (503) e o iCorp (timeouts), por isso é limitado por MAX_WORKERS.
    """
    max_workers = int(os.getenv('MAX_WORKERS', '10'))
    total_falhas = 0
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(run_lote, start, end): (start, end) for start, end in lotes}

        for future in as_completed(futures):
            start, end = futures[future]
            try:
                processados, falhas = future.result()
                total_falhas += len(falhas)
                logger.info(f"Lote {start}-{end} finalizado com {processados} itens ({len(falhas)} falha(s))")
            except Exception:
                total_falhas += 1
                logger.exception(f"❌ Erro no lote {start}-{end}")
    return total_falhas
