import sys

from sankhya.sankhya_client import login
from sankhya.sankhya_fetch import sankhya_list_total_codprod
from updates.process import process_integration, run_lotes
from utils import logging_config

logging_config()

TAMANHO_LOTE = 2000
TOTAL_PRODUTOS = 50000


def run_lote(start, end):
    login()  # cada processo faz seu login independente
    products = sankhya_list_total_codprod(start, end)
    return len(products), process_integration(products)


if __name__ == "__main__":
    lotes = [(start, start + TAMANHO_LOTE - 1) for start in range(1, TOTAL_PRODUTOS + 1, TAMANHO_LOTE)]
    falhas = run_lotes(run_lote, lotes)
    sys.exit(1 if falhas else 0)
