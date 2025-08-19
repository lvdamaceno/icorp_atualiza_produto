import json
import logging
import time

from sankhya.sankhya_client import snk_post
from utils import logging_config

logging_config()


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
        logging.info(f"🕐 [Sankhya] Latência: {elapsed:.3f}s para dados do produto {codprod}")
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
        logging.info(f"🕐 [Sankhya] Latência: {elapsed:.3f}s para estoque do produto {codprod}")
        return estoque
    except Exception as e:
        logging.error(e)


def sankhya_list_total_codprod(token, batch_size=5000):
    all_products = []
    offset = 0

    while True:
        # calcula o intervalo RN para este lote
        start = offset * batch_size + 1
        end = (offset + 1) * batch_size

        # monta o SQL com row_number + between
        sql = f"""
            SELECT CODPROD
                FROM (
                    SELECT
                        PRO.CODPROD,
                        ROW_NUMBER() OVER (ORDER BY PRO.CODPROD) AS RN
                    FROM 
                        TGFPRO PRO
                    INNER JOIN TGFEST EST ON PRO.CODPROD = EST.CODPROD
                    WHERE 
                        PRO.ATIVO = 'S'
                        AND PRO.USOPROD = 'R'
                        AND PRO.CODGRUPOPROD <= '1159999'
                    GROUP BY PRO.CODPROD
                    HAVING SUM(EST.ESTOQUE) > 0
                ) AS T
            WHERE RN BETWEEN {start} AND {end}
            ORDER BY CODPROD
        """

        body = {
            "serviceName": "DbExplorerSP.executeQuery",
            "requestBody": {"sql": sql}
        }

        # OBS: passe "DbExplorerSP.executeQuery" no service, não "loadView"
        resp = snk_post(token, "DbExplorerSP.executeQuery", body)

        # executeQuery retorna os dados em responseBody.rows como lista de listas
        rows = resp["responseBody"]["rows"]
        if not rows:
            break

        # extrai o primeiro campo de cada row (CODPROD) e estende a lista
        batch = [row[0] for row in rows]
        all_products.extend(batch)

        offset += 1

    return all_products


def sankhya_list_weekly_codprod(token, batch_size=5000):
    all_products = []
    offset = 0

    while True:
        # calcula o intervalo RN para este lote
        start = offset * batch_size + 1
        end = (offset + 1) * batch_size

        # monta o SQL com row_number + between
        sql = f"""
            SELECT CODPROD FROM (
                SELECT *, ROW_NUMBER() OVER (ORDER BY D.CODPROD) AS RN FROM (
                        SELECT DISTINCT ITE.CODPROD
                        FROM TGFITE ITE
                        INNER JOIN TGFPRO PRO ON ITE.CODPROD = PRO.CODPROD
                        INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA
                        INNER JOIN TGFEST EST ON ITE.CODPROD = EST.CODPROD
                        WHERE PRO.ATIVO = 'S'
                        AND PRO.USOPROD = 'R'
                        AND PRO.CODGRUPOPROD <= '1159999'
                        AND CAST(CAB.DTNEG AS DATE) BETWEEN DATEADD(DAY, -6, CAST(GETDATE() AS DATE)) AND CAST(GETDATE() AS DATE)
                        GROUP BY ITE.CODPROD
                        HAVING SUM(EST.ESTOQUE) > 0
            
                        UNION
            
                        SELECT DISTINCT PRO.CODPROD 
                        FROM TGFPRO PRO
                        INNER JOIN TGFEST EST ON PRO.CODPROD = EST.CODPROD
                        WHERE CAST(DTALTER AS DATE) BETWEEN DATEADD(DAY, -6, CAST(GETDATE() AS DATE)) AND CAST(GETDATE() AS DATE)
                        AND PRO.ATIVO = 'S'
                        AND PRO.USOPROD = 'R'
                        AND PRO.CODGRUPOPROD <= '1159999'
                        GROUP BY PRO.CODPROD
                        HAVING SUM(EST.ESTOQUE) > 0
            
                        UNION
            
                        SELECT CODPROD FROM (
                        SELECT DISTINCT LTRIM(RTRIM(SUBSTRING(CHAVE, CHARINDEX('CODPROD=', CHAVE) + LEN('CODPROD='),CHARINDEX('CODLOCAL=', CHAVE) - (CHARINDEX('CODPROD=', CHAVE) + LEN('CODPROD='))))) AS CODPROD 
                        FROM TSILGT 
                        WHERE CAST(DHACAO AS DATE) BETWEEN DATEADD(DAY, -6, CAST(GETDATE() AS DATE)) AND CAST(GETDATE() AS DATE)) AS C
                        WHERE C.CODPROD IN (SELECT CODPROD FROM TGFEST GROUP BY CODPROD HAVING CODPROD > 0)
                ) AS D
            ) AS T           
            WHERE RN BETWEEN {start} AND {end}
        """

        body = {
            "serviceName": "DbExplorerSP.executeQuery",
            "requestBody": {"sql": sql}
        }

        # OBS: passe "DbExplorerSP.executeQuery" no service, não "loadView"
        resp = snk_post(token, "DbExplorerSP.executeQuery", body)

        # executeQuery retorna os dados em responseBody.rows como lista de listas
        rows = resp["responseBody"]["rows"]
        if not rows:
            break

        # extrai o primeiro campo de cada row (CODPROD) e estende a lista
        batch = [row[0] for row in rows]
        all_products.extend(batch)

        offset += 1

    return all_products


def sankhya_list_daily_codprod(token, batch_size=5000):
    all_products = []
    offset = 0

    while True:
        # calcula o intervalo RN para este lote
        start = offset * batch_size + 1
        end = (offset + 1) * batch_size

        # monta o SQL com row_number + between
        sql = f"""
            SELECT CODPROD FROM (
                SELECT *, ROW_NUMBER() OVER (ORDER BY D.CODPROD) AS RN FROM (
                    SELECT DISTINCT
                    ITE.CODPROD
                    FROM TGFITE ITE
                    INNER JOIN TGFPRO PRO ON ITE.CODPROD = PRO.CODPROD
                    INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA
                    INNER JOIN TGFEST EST ON ITE.CODPROD = EST.CODPROD
                    WHERE PRO.ATIVO = 'S'
                    AND PRO.USOPROD = 'R'
                    AND PRO.CODGRUPOPROD <= '1159999'
                    AND CAST(CAB.DTNEG AS DATE) = CAST(GETDATE() AS DATE)
                    GROUP BY ITE.CODPROD
                    HAVING SUM(EST.ESTOQUE) > 0 
            
                    UNION
            
                    SELECT DISTINCT PRO.CODPROD FROM TGFPRO PRO
                    INNER JOIN TGFEST EST ON PRO.CODPROD = EST.CODPROD
                    WHERE CAST(DTALTER AS DATE) = CAST(GETDATE() AS DATE)
                    AND PRO.ATIVO = 'S'
                    AND PRO.USOPROD = 'R'
                    AND PRO.CODGRUPOPROD <= '1159999'
                    GROUP BY PRO.CODPROD
                    HAVING SUM(EST.ESTOQUE) > 0
            
                    UNION
            
                    SELECT CODPROD FROM (
                    SELECT DISTINCT LTRIM(RTRIM(SUBSTRING(CHAVE, CHARINDEX('CODPROD=', CHAVE) + LEN('CODPROD='),CHARINDEX('CODLOCAL=', CHAVE) - (CHARINDEX('CODPROD=', CHAVE) + LEN('CODPROD='))))) AS CODPROD 
                    FROM TSILGT 
                    WHERE CAST(DHACAO AS DATE) = CAST(GETDATE() AS DATE)) AS C
                    WHERE C.CODPROD IN (SELECT CODPROD FROM TGFEST GROUP BY CODPROD HAVING CODPROD > 0)
                ) AS D
            ) AS T
            WHERE RN BETWEEN {start} AND {end}
        """

        body = {
            "serviceName": "DbExplorerSP.executeQuery",
            "requestBody": {"sql": sql}
        }

        # OBS: passe "DbExplorerSP.executeQuery" no service, não "loadView"
        resp = snk_post(token, "DbExplorerSP.executeQuery", body)

        # executeQuery retorna os dados em responseBody.rows como lista de listas
        rows = resp["responseBody"]["rows"]
        if not rows:
            break

        # extrai o primeiro campo de cada row (CODPROD) e estende a lista
        batch = [row[0] for row in rows]
        all_products.extend(batch)

        offset += 1

    return all_products


def sankhya_list_minutes_codprod(token, batch_size=5000):
    all_products = []
    offset = 0

    while True:
        # calcula o intervalo RN para este lote
        start = offset * batch_size + 1
        end = (offset + 1) * batch_size

        # monta o SQL com row_number + between
        sql = f"""
            SELECT CODPROD FROM (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY D.CODPROD) AS RN FROM (
                        SELECT DISTINCT
                        ITE.CODPROD
                        FROM TGFITE ITE
                        INNER JOIN TGFPRO PRO ON ITE.CODPROD = PRO.CODPROD
                        INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA
                        INNER JOIN TGFEST EST ON ITE.CODPROD = EST.CODPROD
                        WHERE PRO.ATIVO = 'S'
                        AND PRO.USOPROD = 'R'
                        AND PRO.CODGRUPOPROD <= '1159999'
                        AND CAB.DTALTER BETWEEN DATEADD(MINUTE, -10, GETDATE()) AND GETDATE()
                        GROUP BY ITE.CODPROD
                        HAVING SUM(EST.ESTOQUE) > 0
            
                        UNION
                        
                        SELECT DISTINCT PRO.CODPROD FROM TGFPRO PRO
                        INNER JOIN TGFEST EST ON PRO.CODPROD = EST.CODPROD
                        WHERE DTALTER BETWEEN DATEADD(MINUTE, -10, GETDATE()) AND GETDATE()
                        AND PRO.ATIVO = 'S'
                        AND PRO.USOPROD = 'R'
                        AND PRO.CODGRUPOPROD <= '1159999'
                        GROUP BY PRO.CODPROD
                        HAVING SUM(EST.ESTOQUE) > 0
                    ) AS D
            ) AS T
            WHERE RN BETWEEN {start} AND {end}
        """

        body = {
            "serviceName": "DbExplorerSP.executeQuery",
            "requestBody": {"sql": sql}
        }

        # OBS: passe "DbExplorerSP.executeQuery" no service, não "loadView"
        resp = snk_post(token, "DbExplorerSP.executeQuery", body)

        # executeQuery retorna os dados em responseBody.rows como lista de listas
        rows = resp["responseBody"]["rows"]
        if not rows:
            break

        # extrai o primeiro campo de cada row (CODPROD) e estende a lista
        batch = [row[0] for row in rows]
        all_products.extend(batch)

        offset += 1

    return all_products