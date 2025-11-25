import logging
import time

from icorp.icorp_send import icorp_post
from img.fetch_img import fetch_img
from sankhya.sankhya_fetch import sankhya_fetch_json_produto, sankhya_fetch_json_estoque, sankhya_fetch_json_parceiro


def process_product(token, product):
    logging.info(f"🚀 Enviando dados do produto")
    dados_produto = sankhya_fetch_json_produto(token, product)
    icorp_post('ProdutoUpdate', dados_produto)


def process_codbarra(product):
    logging.info(f"🚀 Enviando codbarra do produto")
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
    img_url = fetch_img(product)
    logging.info(f"🚀 Enviando imagem do produto")
    imagem = [
        {
            "CodigoProduto": f"{product}",
            "Ordem": 0,
            "Descricao": f"{product}.jpg",
            "URLImagem": f"{img_url}",
            "IsImgPadrao": False
        }
    ]
    icorp_post('ProdutoImagens', imagem)


def process_estoque(token, product):
    logging.info(f"🚀 Enviando estoque do produto")
    estoque = sankhya_fetch_json_estoque(token, product)
    icorp_post('Saldos_Atualiza', estoque)


def process_integration(token, products):
    total = len(products)
    logging.info(f"Total de envios programados: {total}")
    logging.info('=' * 45)

    media = 0.0
    count = 0
    start = time.perf_counter()

    for product in products:
        start = time.perf_counter()
        logging.info(f"▶️ Iniciando integração do produto: {product}")

        process_product(token, product)
        process_codbarra(product)
        process_image(product)
        process_estoque(token, product)

        elapsed = time.perf_counter() - start
        count += 1
        # atualiza média incremental
        media += (elapsed - media) / count

        total -= 1
        minutes = (total * media) / 60
        hours = minutes / 60

        logging.info(f"🕐 Tempo desta iteração: {elapsed:.3f}s")
        logging.info(f"🕐 Tempo médio até agora: {media:.3f}s")
        logging.info(f"⚙️ Envio restante: {total} ({minutes:.3f}m) / ({hours:.3f}h)")
        logging.info('=' * 45)

    elapsed = time.perf_counter() - start
    logging.info(f"🕐 Tempo total: {elapsed:.3f}s")



def process_parceiro(token, parceiro):
    logging.info("🚀 Enviando dados do parceiro")

    tentativas = 10
    dados_parceiro = None
    base_wait = 1  # tempo base para calcular o delay

    for tentativa in range(1, tentativas + 1):
        dados_parceiro = sankhya_fetch_json_parceiro(token, parceiro)

        if dados_parceiro:
            break  # Sucesso — sai do loop

        # Se estiver vazio → calcula tempo de espera progressivo
        wait_time = base_wait + (tentativa - 1)

        logging.warning(
            f"⚠️ dados do parceiro {parceiro} vazio na tentativa {tentativa}/{tentativas}. "
            f"Tentando novamente em {wait_time} segundo(s)..."
        )

        time.sleep(wait_time)

    # Depois das tentativas, verifica se ainda está vazio
    if not dados_parceiro:
        logging.error("❌ dados_parceiro continua vazio após 3 tentativas. Abortando envio.")
        return

    logging.info("✅ Dados obtidos, enviando para a ICORP")
    icorp_post('Cliente', dados_parceiro)

def process_parceiros_integration(token, parceiros):
    total = len(parceiros)
    logging.info(f"Total de envios programados: {total}")
    logging.info('=' * 45)

    media = 0.0
    count = 0
    start = time.perf_counter()

    for parceiro in parceiros:
        start = time.perf_counter()
        logging.info(f"▶️ Iniciando integração do parceiro: {parceiro}")

        process_parceiro(token, parceiro)

        elapsed = time.perf_counter() - start
        count += 1
        # atualiza média incremental
        media += (elapsed - media) / count

        total -= 1
        minutes = (total * media) / 60
        hours = minutes / 60

        logging.info(f"🕐 Tempo desta iteração: {elapsed:.3f}s")
        logging.info(f"🕐 Tempo médio até agora: {media:.3f}s")
        logging.info(f"⚙️ Envio restante: {total} ({minutes:.3f}m) / ({hours:.3f}h)")
        logging.info('=' * 45)

    elapsed = time.perf_counter() - start
    logging.info(f"🕐 Tempo total: {elapsed:.3f}s")
