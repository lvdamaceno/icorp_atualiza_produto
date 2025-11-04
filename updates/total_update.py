from sankhya.sankhya_client import login
from sankhya.sankhya_fetch import sankhya_list_total_codprod
from updates.process import process_integration
from utils import logging_config

logging_config()

from concurrent.futures import ThreadPoolExecutor, as_completed

def main():
    token = login()

    # define os intervalos (start, end) de cada lote
    lotes = [(1, 2000), (2001, 4000), (4001, 6000), (6001, 8000), (8001, 10000), (10001, 12000), (12001, 14000),
             (14001, 16000), (16001, 18000), (18001, 20000)]

    results = []

    # cria um pool de threads (2 nesse caso)
    with ThreadPoolExecutor(max_workers=10) as executor:
        # dispara as chamadas em paralelo
        futures = {
            executor.submit(sankhya_list_total_codprod, token, start, end): (start, end)
            for start, end in lotes
        }

        # espera cada lote terminar
        for future in as_completed(futures):
            start, end = futures[future]
            try:
                products = future.result()
                print(f"Lote {start}-{end} retornou {len(products)} produtos")
                results.extend(products)
            except Exception as e:
                print(f"Erro no lote {start}-{end}: {e}")

    # agora processa a integração depois que ambos terminaram
    process_integration(token, results)


if __name__ == "__main__":
    main()