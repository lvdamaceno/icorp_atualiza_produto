import logging

from process import process_product, process_image, process_estoque
from sankhya.sankhya_client import login
from sankhya.sankhya_fetch import sankhya_list_codprod
from utils import logging_config

logging_config()

if __name__ == "__main__":
    token = login()

    # products = sankhya_list_codprod(token)
    products = ['444923', '444924']

    for product in products:
        logging.info(f"▶️ Iniciando integração do produto: {product}")

        process_product(token, product)
        process_image(product)
        process_estoque(token, product)

        logging.info(45*'=')