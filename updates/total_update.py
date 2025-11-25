from sankhya.sankhya_client import login
from sankhya.sankhya_fetch import sankhya_list_total_codprod
from updates.process import process_integration
from utils import logging_config
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

logging_config()

def run_lote(start, end):
    token = login()  # cada processo faz seu login independente
    products = sankhya_list_total_codprod(token, start, end)
    process_integration(token, products)
    return f"Lote {start}-{end} finalizado com {len(products)} produtos"

if __name__ == "__main__":
    lotes = [
        # total de produtos
        (1, 2000),
        (2001, 4000),
        (4001, 6000),
        (6001, 8000),
        (8001, 10000),
        (10001, 12000),
        (12001, 14000),
        (14001, 16000),
        (16001, 18000),
        (18001, 20000)
    ]

    with ProcessPoolExecutor(max_workers=len(lotes)) as executor:
        futures = {executor.submit(run_lote, start, end): (start, end) for start, end in lotes}

        for future in as_completed(futures):
            start, end = futures[future]
            try:
                result = future.result()
                logging.info(result)
            except Exception as e:
                logging.info(f"Erro no lote {start}-{end}: {e}")