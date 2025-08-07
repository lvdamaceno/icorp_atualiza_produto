import logging
import time

from process import process_product, process_image, process_estoque
from sankhya.sankhya_client import login
from sankhya.sankhya_fetch import sankhya_list_codprod
from utils import logging_config

logging_config()

if __name__ == "__main__":
    token = login()

    products = sankhya_list_codprod(token)
    # products = ['444923', '444924']

    for product in products:
        start = time.perf_counter()

        logging.info(f"▶️ Iniciando integração do produto: {product}")

        process_product(token, product)
        process_image(product)
        process_estoque(token, product)

        elapsed = time.perf_counter() - start  # tempo decorrido
        logging.info(f"🕐 Tempo total de processamento: {elapsed:.3f}s para {product}")
        logging.info(45*'=')