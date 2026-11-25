import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

from icorp.icorp_send import icorp_post
from sankhya.sankhya_fetch import (sankhya_fetch_json_produto, sankhya_fetch_json_estoque, sankhya_fetch_json_parceiro,
                                    sankhya_fetch_json_lote, sankhya_fetch_json_parceiro_lote,
                                    sankhya_fetch_json_estoque_lote)

logger = logging.getLogger(__name__)

# aborta a execução se tudo está falhando (API fora do ar), em vez de insistir por horas
MAX_FALHAS_CONSECUTIVAS = 20

IMG_URL = 'https://img.casacontente.com.br/{codprod}.jpg'


_NAO_LIDO = object()


def process_product(product, dados_produto=_NAO_LIDO):
    logger.info(f"🚀 Enviando dados do produto {product}")
    if dados_produto is _NAO_LIDO:
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


def process_estoque(product, estoque=_NAO_LIDO, zerar=False):
    logger.info(f"🚀 Enviando estoque {'zerado ' if zerar else ''}do produto {product}")
    if estoque is _NAO_LIDO:
        estoque = sankhya_fetch_json_estoque(product)
    if not estoque:
        logger.warning(f"⚠️ CC_CS_JSON_ESTOQUE não retornou dados para o produto {product}, estoque não enviado")
        return
    if zerar:
        # produto inativo/sem TGFEST vai zerado no iCorp mesmo que ainda tenha saldo na TGFEST
        estoque = [{**saldo, 'Qtd': 0} for saldo in estoque]
    icorp_post('Saldos_Atualiza', estoque)


def integrate_product(product, dados=None):
    """Envia um produto ao iCorp. dados = (produto, estoque) já lidos do Sankhya; sem eles, lê aqui, um a um."""
    produto, estoque = dados if dados is not None else (_NAO_LIDO, _NAO_LIDO)
    process_product(product, produto)
    process_codbarra(product)
    process_image(product)
    process_estoque(product, estoque)


def process_parceiro(parceiro, dados=None):
    """Envia um parceiro ao iCorp. dados = o JSON já lido em lote; sem ele (ou vazio), lê aqui, com as 3 tentativas."""
    logger.info(f"🚀 Enviando dados do parceiro {parceiro}")
    if dados:
        icorp_post('Cliente', dados)
        return

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


def _run_lote_paralelo(items, label, ler_lote, enviar_um):
    """Integração em lote (23/11/2026), feita para rodar com poucos processos (MAX_WORKERS=2) pela conexão central:

    - o Sankhya é lido em lotes de SANKHYA_LOTE itens (padrão 40), numa consulta só, e o lote seguinte já é lido
      enquanto este vai para o iCorp;
    - o iCorp recebe ICORP_THREADS itens ao mesmo tempo por processo (padrão 4; com 2 processos, 8 no total, abaixo
      dos 10 de antes, que o iCorp aguentava);
    - lote que o Sankhya não conseguir ler volta ao caminho antigo, um item por vez, lendo cada um.
    ler_lote(itens) -> {item: dados}; enviar_um(item, lidos) envia um item (lidos None = a leitura do lote falhou).
    Devolve a lista dos itens com falha."""
    tamanho = max(1, int(os.getenv('SANKHYA_LOTE', '40')))
    threads = max(1, int(os.getenv('ICORP_THREADS', '4')))
    lotes = [items[i:i + tamanho] for i in range(0, len(items), tamanho)]
    total, feitos, failures, consecutivas = len(items), 0, [], 0
    inicio = time.perf_counter()
    logger.info(f"Total de envios programados: {total} {label}(s) ({len(lotes)} lote(s) de {tamanho}, {threads} envio(s) ao iCorp por vez)")

    def ler(lote):
        try:
            return ler_lote(lote)
        except Exception:
            logger.exception(f"❌ Leitura em lote falhou ({lote[0]}..{lote[-1]}); estes vão um a um")
            return None

    def enviar(item, lidos):
        try:
            enviar_um(item, lidos)
            return None
        except Exception:
            logger.exception(f"❌ Falha na integração do {label} {item}")
            return item

    with ThreadPoolExecutor(max_workers=1) as leitor, ThreadPoolExecutor(max_workers=threads) as envio:
        proxima = leitor.submit(ler, lotes[0]) if lotes else None
        for n, lote in enumerate(lotes, start=1):
            lidos = proxima.result()
            proxima = leitor.submit(ler, lotes[n]) if n < len(lotes) else None
            falhas = [i for i in envio.map(lambda i: enviar(i, lidos), lote) if i is not None]
            failures += falhas
            feitos += len(lote)
            consecutivas = consecutivas + len(falhas) if len(falhas) == len(lote) else len(falhas)
            if consecutivas >= MAX_FALHAS_CONSECUTIVAS:
                if proxima is not None:
                    proxima.cancel()
                raise RuntimeError(f"{consecutivas} falhas seguidas ({label}s {falhas[-5:]}...), abortando execução")
            passou = time.perf_counter() - inicio
            ritmo = feitos / passou if passou else 0
            logger.info(f"⚙️ Lote {n}/{len(lotes)}: {len(lote) - len(falhas)} ok, {len(falhas)} falha(s) · "
                        f"{ritmo:.1f} {label}(s)/s · restante {total - feitos} (~{(total - feitos) / ritmo / 60 if ritmo else 0:.1f} min)")

    logger.info(f"🕐 Tempo total: {time.perf_counter() - inicio:.3f}s")
    logger.info(f"📊 Resultado: {total - len(failures)} ok / {len(failures)} falha(s)")
    if failures:
        logger.error(f"❌ {label.capitalize()}s com falha: {failures}")
    return failures


def _enviar_produto(product, lidos):
    if lidos is None:
        integrate_product(product)  # leitura do lote falhou: lê este sozinho
    else:
        integrate_product(product, lidos.get(int(product), (None, None)))


def _enviar_estoque_zerado(product, lidos):
    if lidos is None:
        process_estoque(product, zerar=True)  # leitura do lote falhou: lê este sozinho
    else:
        process_estoque(product, lidos.get(int(product)), zerar=True)


def _enviar_parceiro(parceiro, lidos):
    # Sem o lote, ou vazio no lote: o caminho antigo, que lê sozinho e tenta 3 vezes.
    process_parceiro(parceiro, (lidos or {}).get(int(parceiro)))


def process_parceiros_integration(parceiros):
    return _run_batch(parceiros, 'parceiro', process_parceiro)


def process_integration_lote(products):
    """Produtos (total, minutes, daily e weekly): os mesmos dados de integrate_product, lidos em lote."""
    return _run_lote_paralelo(products, 'produto', sankhya_fetch_json_lote, _enviar_produto)


def process_estoque_zerado_lote(products, ja_enviados=()):
    """Produtos inativos ou sem TGFEST: só o estoque, sempre zerado. Pula os que a integração completa já enviou."""
    ja_enviados = {int(p) for p in ja_enviados}
    pendentes = [p for p in products if int(p) not in ja_enviados]
    return _run_lote_paralelo(pendentes, 'produto (estoque zerado)', sankhya_fetch_json_estoque_lote, _enviar_estoque_zerado)


def process_parceiros_integration_lote(parceiros):
    """Parceiros (mensal): os mesmos dados de process_parceiro, lidos em lote."""
    return _run_lote_paralelo(parceiros, 'parceiro', sankhya_fetch_json_parceiro_lote, _enviar_parceiro)


def run_lotes(run_lote, lotes):
    """Executa os lotes em processos paralelos e retorna o total de itens com falha.

    Cada processo faz seu próprio login (sessão HTTP própria no Sankhya).
    Paralelismo alto derruba o Sankhya (503) e o iCorp (timeouts), por isso é limitado por MAX_WORKERS.
    """
    # Padrão 2 (23/11/2026; era 10): cada processo já envia ICORP_THREADS itens ao mesmo tempo ao iCorp, e lê o Sankhya
    # em lote pela conexão central. 10 processos x 4 envios seriam 40 ao mesmo tempo no iCorp.
    max_workers = int(os.getenv('MAX_WORKERS', '2'))
    total_falhas = 0
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Cada lote é (início, fim) de uma fatia da lista ou, na carga total, a própria lista de códigos.
        futures = {executor.submit(run_lote, *(lote if isinstance(lote, tuple) else (lote,))):
                   (lote if isinstance(lote, tuple) else (lote[0], lote[-1])) for lote in lotes}

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
