import json
import logging
import os
import time

from sankhya.sankhya_client import snk_post
from utils import logging_config

logging_config()
logger = logging.getLogger(__name__)

SNK_SERVICE = 'DbExplorerSP.executeQuery'

# extrai o CODPROD da CHAVE do log (TSILGT); CASE garante que o SUBSTRING não recebe tamanho negativo
TSILGT_CODPROD = """
    TRY_CAST(LTRIM(RTRIM(
        CASE WHEN CHARINDEX('CODPROD=', CHAVE) > 0
              AND CHARINDEX('CODLOCAL=', CHAVE) > CHARINDEX('CODPROD=', CHAVE) + LEN('CODPROD=')
        THEN SUBSTRING(CHAVE, CHARINDEX('CODPROD=', CHAVE) + LEN('CODPROD='),
                       CHARINDEX('CODLOCAL=', CHAVE) - (CHARINDEX('CODPROD=', CHAVE) + LEN('CODPROD=')))
        END
    )) AS INT)
"""


def _execute_query(sql):
    resp = snk_post(SNK_SERVICE, {"serviceName": SNK_SERVICE, "requestBody": {"sql": sql}})
    return resp["responseBody"]["rows"]


def _fetch_json(funcao, codigo, descricao):
    """Executa uma função JSON do Sankhya. Retorna None quando a função não devolve dados."""
    logger.info(f"🔍 Buscando {descricao} {codigo}")
    start = time.perf_counter()
    rows = _execute_query(f"SELECT [sankhya].[{funcao}]({int(codigo)})")
    json_str = rows[0][0] if rows else None
    if not json_str:
        return None
    data = json.loads(json_str)
    logger.debug(json.dumps(data, indent=2, ensure_ascii=False))
    elapsed = time.perf_counter() - start
    logger.info(f"🕐 [Sankhya] Latência: {elapsed:.3f}s para {descricao} {codigo}")
    return data


def _list_range(inner_sql, start, end, column='CODPROD'):
    sql = f"""
        SELECT {column} FROM (
            SELECT *, ROW_NUMBER() OVER (ORDER BY D.{column}) AS RN FROM (
                {inner_sql}
            ) AS D
        ) AS T
        WHERE RN BETWEEN {start} AND {end}
    """
    return [row[0] for row in _execute_query(sql)]


def _list_paginated(inner_sql, batch_size, column='CODPROD'):
    all_codes = []
    offset = 0
    while True:
        batch = _list_range(inner_sql, offset * batch_size + 1, (offset + 1) * batch_size, column)
        all_codes.extend(batch)
        # página incompleta = última página; evita uma consulta extra (que pode levar dezenas de segundos)
        if len(batch) < batch_size:
            break
        offset += 1
    return all_codes


def sankhya_fetch_json_produto(codprod):
    return _fetch_json('CC_CS_JSON_PRODUTO', codprod, 'dados do produto')


def sankhya_fetch_json_estoque(codprod):
    return _fetch_json('CC_CS_JSON_ESTOQUE', codprod, 'estoque do produto')


def sankhya_fetch_json_parceiro(codparc):
    return _fetch_json('CC_CS_JSON_PARCEIRO', codparc, 'dados do parceiro')


def sankhya_fetch_json_lote(codprods):
    """Dados e estoque de vários produtos numa consulta só: {CODPROD: (produto, estoque)}, com None no que vier vazio.

    Um a um eram 2 chamadas por produto (0,48 s); em lotes de 40, 0,06 s por produto e 80 vezes menos chamadas
    (medido em produção, 23/11/2026). Produto que não voltar na consulta fica de fora do dicionário."""
    cods = sorted({int(c) for c in codprods})
    if not cods:
        return {}
    start = time.perf_counter()
    rows = _execute_query(f"""
        SELECT PRO.CODPROD, [sankhya].[CC_CS_JSON_PRODUTO](PRO.CODPROD), [sankhya].[CC_CS_JSON_ESTOQUE](PRO.CODPROD)
        FROM TGFPRO PRO WHERE PRO.CODPROD IN ({','.join(map(str, cods))})
    """)
    logger.info(f"🕐 [Sankhya] Latência: {time.perf_counter() - start:.3f}s para {len(cods)} produtos em lote")
    ler = lambda x: json.loads(x) if x else None
    return {int(r[0]): (ler(r[1]), ler(r[2])) for r in rows}


TOTAL_PRODUTOS_SQL = """
        SELECT DISTINCT PRO.CODPROD
        FROM TGFPRO PRO
        WHERE PRO.ATIVO = 'S'
        AND PRO.CODGRUPOPROD <= '1159999'
"""


def sankhya_list_total_codprod(start, end):
    return _list_range(TOTAL_PRODUTOS_SQL, start, end)


def sankhya_list_total_todos(batch_size=5000):
    """A lista inteira da carga total, lida de uma vez no começo (ver updates/total_update.py)."""
    return _list_paginated(TOTAL_PRODUTOS_SQL, batch_size)


def sankhya_list_weekly_codprod(batch_size=5000):
    return _list_paginated(f"""
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
            SELECT DISTINCT {TSILGT_CODPROD} AS CODPROD
            FROM TSILGT
            WHERE CAST(DHACAO AS DATE) BETWEEN DATEADD(DAY, -6, CAST(GETDATE() AS DATE)) AND CAST(GETDATE() AS DATE)
        ) AS C
        WHERE C.CODPROD IN (SELECT CODPROD FROM TGFEST GROUP BY CODPROD HAVING CODPROD > 0)
    """, batch_size)


def sankhya_list_daily_codprod(batch_size=5000):
    return _list_paginated(f"""
        SELECT DISTINCT ITE.CODPROD
        FROM TGFITE ITE
        INNER JOIN TGFPRO PRO ON ITE.CODPROD = PRO.CODPROD
        INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA
        WHERE PRO.ATIVO = 'S'
        AND PRO.USOPROD = 'R'
        AND PRO.CODGRUPOPROD <= '1159999'
        AND CAST(CAB.DTNEG AS DATE) = CAST(GETDATE() AS DATE)
        AND EXISTS (SELECT 1 FROM TGFEST EST WHERE EST.CODPROD = ITE.CODPROD)

        UNION

        SELECT DISTINCT PRO.CODPROD
        FROM TGFPRO PRO
        WHERE CAST(DTALTER AS DATE) = CAST(GETDATE() AS DATE)
        AND PRO.ATIVO = 'S'
        AND PRO.USOPROD = 'R'
        AND PRO.CODGRUPOPROD <= '1159999'
        AND EXISTS (SELECT 1 FROM TGFEST EST WHERE EST.CODPROD = PRO.CODPROD)

        UNION

        SELECT CODPROD FROM (
            SELECT DISTINCT {TSILGT_CODPROD} AS CODPROD
            FROM TSILGT
            WHERE CAST(DHACAO AS DATE) = CAST(GETDATE() AS DATE)
        ) AS C
        WHERE C.CODPROD IN (SELECT CODPROD FROM TGFEST GROUP BY CODPROD HAVING CODPROD > 0)
    """, batch_size)


def sankhya_list_minutes_codprod(batch_size=5000):
    # janela deve cobrir o intervalo do cron no Render (*/10) com folga; reenviar é idempotente
    minutos = int(os.getenv('MINUTES_WINDOW', '15'))
    return _list_paginated(f"""
        SELECT DISTINCT ITE.CODPROD
        FROM TGFITE ITE
        INNER JOIN TGFPRO PRO ON ITE.CODPROD = PRO.CODPROD
        INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA
        WHERE PRO.ATIVO = 'S'
        AND PRO.USOPROD = 'R'
        AND PRO.CODGRUPOPROD <= '1159999'
        AND CAB.DTALTER BETWEEN DATEADD(MINUTE, -{minutos}, GETDATE()) AND GETDATE()
        AND EXISTS (SELECT 1 FROM TGFEST EST WHERE EST.CODPROD = ITE.CODPROD)

        UNION

        SELECT DISTINCT PRO.CODPROD
        FROM TGFPRO PRO
        WHERE DTALTER BETWEEN DATEADD(MINUTE, -{minutos}, GETDATE()) AND GETDATE()
        AND PRO.ATIVO = 'S'
        AND PRO.USOPROD = 'R'
        AND PRO.CODGRUPOPROD <= '1159999'
        AND EXISTS (SELECT 1 FROM TGFEST EST WHERE EST.CODPROD = PRO.CODPROD)

        UNION

        SELECT DISTINCT EXC.CODPROD
        FROM TGFEXC EXC
        INNER JOIN TGFPRO PRO ON EXC.CODPROD = PRO.CODPROD
        WHERE EXC.DHALTREG BETWEEN DATEADD(MINUTE, -{minutos}, GETDATE()) AND GETDATE()
        AND PRO.ATIVO = 'S'
    """, batch_size)


# produtos que as listagens acima descartam (inativos ou sem TGFEST): para eles só o estoque é enviado, sempre
# zerado — senão o iCorp fica com o último saldo recebido antes da inativação/remoção do estoque
ZERAR_ESTOQUE_SQL = """
    SELECT DISTINCT PRO.CODPROD
    FROM TGFPRO PRO
    WHERE {filtro_data}
    AND PRO.USOPROD = 'R'
    AND PRO.CODGRUPOPROD <= '1159999'
    AND (PRO.ATIVO = 'N' OR NOT EXISTS (SELECT 1 FROM TGFEST EST WHERE EST.CODPROD = PRO.CODPROD))
"""


def sankhya_list_minutes_zerar_estoque(batch_size=5000):
    minutos = int(os.getenv('MINUTES_WINDOW', '15'))
    filtro = f"PRO.DTALTER BETWEEN DATEADD(MINUTE, -{minutos}, GETDATE()) AND GETDATE()"
    return _list_paginated(ZERAR_ESTOQUE_SQL.format(filtro_data=filtro), batch_size)


def sankhya_list_daily_zerar_estoque(batch_size=5000):
    filtro = "CAST(PRO.DTALTER AS DATE) = CAST(GETDATE() AS DATE)"
    return _list_paginated(ZERAR_ESTOQUE_SQL.format(filtro_data=filtro), batch_size)


def sankhya_list_weekly_zerar_estoque(batch_size=5000):
    filtro = "CAST(PRO.DTALTER AS DATE) BETWEEN DATEADD(DAY, -6, CAST(GETDATE() AS DATE)) AND CAST(GETDATE() AS DATE)"
    return _list_paginated(ZERAR_ESTOQUE_SQL.format(filtro_data=filtro), batch_size)


def sankhya_list_total_zerar_estoque(batch_size=5000):
    return _list_paginated(ZERAR_ESTOQUE_SQL.format(filtro_data="1 = 1"), batch_size)


def sankhya_fetch_json_estoque_lote(codprods):
    """Estoque de vários produtos numa consulta só: {CODPROD: estoque}, com None no que vier vazio."""
    cods = sorted({int(c) for c in codprods})
    if not cods:
        return {}
    start = time.perf_counter()
    rows = _execute_query(f"""
        SELECT PRO.CODPROD, [sankhya].[CC_CS_JSON_ESTOQUE](PRO.CODPROD)
        FROM TGFPRO PRO WHERE PRO.CODPROD IN ({','.join(map(str, cods))})
    """)
    logger.info(f"🕐 [Sankhya] Latência: {time.perf_counter() - start:.3f}s para estoque de {len(cods)} produtos em lote")
    return {int(r[0]): (json.loads(r[1]) if r[1] else None) for r in rows}


TOTAL_PARCEIROS_SQL = """
        SELECT DISTINCT PAR.CODPARC
        FROM TGFPAR PAR
        WHERE CODPARC > 0
"""


def sankhya_list_total_parceiros(start, end):
    return _list_range(TOTAL_PARCEIROS_SQL, start, end, column='CODPARC')


def sankhya_list_parceiros_todos(batch_size=5000):
    """A lista inteira de parceiros, lida de uma vez no começo (ver updates/parceiros.py)."""
    return _list_paginated(TOTAL_PARCEIROS_SQL, batch_size, column='CODPARC')


def sankhya_fetch_json_parceiro_lote(codparcs):
    """Dados de vários parceiros numa consulta só: {CODPARC: dados ou None}. Um a um, 0,22 s por parceiro; em lotes de
    40, 0,009 s (medido em produção, 23/11/2026). Parceiro que não voltar na consulta fica de fora do dicionário."""
    cods = sorted({int(c) for c in codparcs})
    if not cods:
        return {}
    start = time.perf_counter()
    rows = _execute_query(f"""
        SELECT PAR.CODPARC, [sankhya].[CC_CS_JSON_PARCEIRO](PAR.CODPARC)
        FROM TGFPAR PAR WHERE PAR.CODPARC IN ({','.join(map(str, cods))})
    """)
    logger.info(f"🕐 [Sankhya] Latência: {time.perf_counter() - start:.3f}s para {len(cods)} parceiros em lote")
    return {int(r[0]): (json.loads(r[1]) if r[1] else None) for r in rows}
