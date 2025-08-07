import logging
import time

from process import process_product, process_image, process_estoque
from sankhya.sankhya_client import login
from sankhya.sankhya_fetch import sankhya_list_codprod
from utils import logging_config
from concurrent.futures import ThreadPoolExecutor, as_completed

logging_config()


def _process_one(token: str, product: str) -> float:
    """
    Executa todo o fluxo de integração de um produto e retorna o tempo gasto.
    """
    start = time.perf_counter()
    logging.info(f"▶️ Iniciando integração do produto: {product}")

    process_product(token, product)
    process_image(product)
    process_estoque(token, product)

    elapsed = time.perf_counter() - start
    return elapsed

def process_all(products: list[str], token: str, max_workers: int = 5):
    """
    Processa a lista de produtos em paralelo, até `max_workers` simultâneos.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # dispara todas as tarefas
        futures = {
            executor.submit(_process_one, token, prod): prod
            for prod in products
        }
        # coleta resultados à medida que ficarem prontos
        for future in as_completed(futures):
            product = futures[future]
            try:
                elapsed = future.result()
                logging.info(f"🕐 Tempo total de processamento: {elapsed:.3f}s para {product}")
            except Exception as e:
                logging.error(f"❌ Erro ao processar {product}: {e}")


if __name__ == "__main__":
    token = login()
    products = sankhya_list_codprod(token)
    process_all(products, token, max_workers=10)