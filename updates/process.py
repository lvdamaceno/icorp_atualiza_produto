import logging
import time

from icorp.icorp_send import icorp_post
from sankhya.sankhya_fetch import sankhya_fetch_json_produto, sankhya_fetch_json_estoque


def process_product(token, product):
    logging.info(f"🚀 Enviando dados do produto")
    dados_produto = sankhya_fetch_json_produto(token, product)
    icorp_post('ProdutoUpdate', dados_produto)


def process_image(product):
    logging.info(f"🚀 Enviando imagem do produto")
    imagem = [
        {
            "CodigoProduto": f"{product}",
            "Ordem": 0,
            "Descricao": f"{product}.jpg",
            "URLImagem": f"https://img.casacontente.com.br/{product}.jpg",
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
    logging.info(45 * '=')

    for product in products:
        start = time.perf_counter()
        logging.info(f"▶️ Iniciando integração do produto: {product}")

        # process_product(token, product)
        process_image(product)
        process_estoque(token, product)

        total -= 1
        elapsed = time.perf_counter() - start
        logging.info(f"🕐 Tempo de execução total: {elapsed:.3f}s")
        logging.info(f"Faltam {total} envios")
        logging.info(45 * '=')

