import sys

from sankhya.sankhya_client import login
from sankhya.sankhya_fetch import sankhya_list_weekly_codprod
from updates.process import process_integration
from utils import logging_config

logging_config()

if __name__ == "__main__":
    login()
    products = sankhya_list_weekly_codprod()
    falhas = process_integration(products)
    sys.exit(1 if falhas else 0)
