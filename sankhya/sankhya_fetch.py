import json
import logging
import time

from sankhya.sankhya_client import snk_post
from utils import logging_config

logging_config()

def sankhya_list_codprod(token):
    logging.info(f"🔍 Buscando lista de produtos")
    snk_service = 'CRUDServiceProvider.loadRecords'
    offset_page = 0
    has_more_pages = True

    while has_more_pages:
        logging.info(f"offsetPage é {offset_page}")
        products = []
        request_body = {
            "serviceName": "CRUDServiceProvider.loadRecords",
            "requestBody": {
                "dataSet": {
                    "rootEntity": "Produto",
                    "offsetPage": f"{offset_page}",
                    "criteria": {
                        "expression": {
                            "$": "ATIVO = 'S' AND USOPROD = 'R' AND CODGRUPOPROD <= '1159999'"
                        }
                    },
                    "entity": [
                        {
                            "fieldset": {
                                "list": "CODPROD"
                            }
                        }
                    ]
                }
            }
        }
        response = snk_post(token, snk_service, request_body)
        has_more_pages = response["responseBody"]["entities"]["hasMoreResult"]
        logging.info(f"hasMoreResult é {has_more_pages}")
        entities = response["responseBody"]["entities"]["entity"]
        for entity in entities:
            products.append(entity['f0']['$'])
        offset_page += 1
        logging.info(f"{products}")
        return products

    return None


def sankhya_fetch_json_produto(token, codprod):
    logging.info(f"🔍 Buscando dados do produto")
    snk_service = 'DbExplorerSP.executeQuery'
    request_body = {
        "serviceName": "DbExplorerSP.executeQuery",
        "requestBody": {
            "sql": f"SELECT [sankhya].[CC_CS_JSON_PRODUTO]({codprod})"
        }
    }
    try:
        start = time.perf_counter()
        response = snk_post(token, snk_service, request_body)
        json_str = response["responseBody"]["rows"][0][0]
        produto = json.loads(json_str)
        logging.debug(json.dumps(produto, indent=2, ensure_ascii=False))
        elapsed = time.perf_counter() - start  # tempo decorrido
        logging.debug(f"🕐 [Sankhya] Latência: {elapsed:.3f}s para {codprod}")
        return produto
    except Exception as e:
        logging.error(e)


def sankhya_fetch_json_estoque(token, codprod):
    logging.info(f"🔍 Buscando estoque do produto")
    snk_service = 'DbExplorerSP.executeQuery'
    request_body = {
        "serviceName": "DbExplorerSP.executeQuery",
        "requestBody": {
            "sql": f"SELECT [sankhya].[CC_CS_JSON_ESTOQUE]({codprod})"
        }
    }
    try:
        start = time.perf_counter()
        response = snk_post(token, snk_service, request_body)
        json_str = response["responseBody"]["rows"][0][0]
        estoque = json.loads(json_str)
        logging.debug(json.dumps(estoque, indent=2, ensure_ascii=False))
        elapsed = time.perf_counter() - start  # tempo decorrido
        logging.debug(f"🕐 [Sankhya] Latência: {elapsed:.3f}s para {codprod}")
        return estoque
    except Exception as e:
        logging.error(e)