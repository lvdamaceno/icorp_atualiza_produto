from sankhya.sankhya_client import login
from sankhya.sankhya_fetch import sankhya_list_total_codprod, sankhya_list_total_parceiros
from updates.process import process_integration, process_parceiros_integration
from utils import logging_config
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

logging_config()

def run_lote(start, end):
    token = login()  # cada processo faz seu login independente
    parceiros = sankhya_list_total_parceiros(token, start, end)
    process_parceiros_integration(token, parceiros)
    return f"Lote {start}-{end} finalizado com {len(parceiros)} parceiros"

if __name__ == "__main__":
    lotes = [
        # total de parceiros
        (1, 5000),
        (5001, 10000),
        (10001, 15000),
        (15001, 20000),
        (20001, 25000),
        (25001, 30000),
        (30001, 35000),
        (35001, 40000),
        (40001, 45000),
        (45001, 50000),
        (50001, 55000),
        (55001, 60000),
        (60001, 65000),
        (65001, 70000),
        (70001, 75000),
        (75001, 80000),
        (80001, 85000),
        (85001, 90000),
        (90001, 95000),
        (95001, 100000),
        (100001, 105000),
        (105001, 110000),
        (110001, 115000),
        (115001, 120000),
        (120001, 125000),
        (125001, 130000),
        (130001, 135000),
        (135001, 140000),
        (140001, 145000),
        (145001, 150000),
        (150001, 155000),
        (155001, 160000),
        (160001, 165000),
        (165001, 170000),
        (170001, 175000),
        (175001, 180000),
        (180001, 185000),
        (185001, 190000),
        (190001, 195000),
        (195001, 200000)
    ]

    with ProcessPoolExecutor(10) as executor:
        futures = {executor.submit(run_lote, start, end): (start, end) for start, end in lotes}

        for future in as_completed(futures):
            start, end = futures[future]
            try:
                result = future.result()
                logging.info(result)
            except Exception as e:
                logging.info(f"Erro no lote {start}-{end}: {e}")

    # for lote in lotes:
    #     run_lote(lote[0], lote[1])