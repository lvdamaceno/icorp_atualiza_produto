import json
import logging

from sankhya.sankhya_client import snk_post

def sankhya_fetch_json_produto(token, codprod):
    logging.info(f"🟢 Buscando dados do produto {codprod}")
    snk_service = 'DbExplorerSP.executeQuery'
    request_body = {
        "serviceName": "DbExplorerSP.executeQuery",
        "requestBody": {
            "sql": f"SELECT [sankhya].[CC_CS_JSON_PRODUTO]({codprod})"
        }
    }
    response = snk_post(token, snk_service, request_body)
    json_str = response["responseBody"]["rows"][0][0]
    produto = json.loads(json_str)
    logging.info(json.dumps(produto, indent=2, ensure_ascii=False))

    return produto