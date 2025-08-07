from process import process_integration
from sankhya.sankhya_client import login
from sankhya.sankhya_fetch import sankhya_list_daily_codprod
from utils import logging_config

logging_config()

if __name__ == "__main__":
    token = login()
    products = sankhya_list_daily_codprod(token)
    process_integration(token, products)