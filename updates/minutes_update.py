import sys

from sankhya.sankhya_client import login
from sankhya.sankhya_fetch import sankhya_list_minutes_codprod, sankhya_list_minutes_zerar_estoque
from updates.process import process_estoque_zerado_lote, process_integration_lote
from utils import logging_config

logging_config()

if __name__ == "__main__":
    login()
    products = sankhya_list_minutes_codprod()
    falhas = process_integration_lote(products)
    falhas += process_estoque_zerado_lote(sankhya_list_minutes_zerar_estoque(), ja_enviados=products)
    sys.exit(1 if falhas else 0)
