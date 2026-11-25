import sys

from sankhya.sankhya_client import login
from sankhya.sankhya_fetch import sankhya_list_total_todos, sankhya_list_total_zerar_estoque
from updates.process import process_estoque_zerado_lote, process_integration_lote, run_lotes
from utils import logging_config

logging_config()

TAMANHO_LOTE = 2000


def run_lote(products):
    login()  # cada processo faz seu login independente (sem efeito pela conexão central)
    return len(products), process_integration_lote(products)


def run_lote_zerar(products):
    login()
    return len(products), process_estoque_zerado_lote(products)


if __name__ == "__main__":
    # A lista é lida uma vez só, aqui, e repartida entre os processos (23/11/2026). Antes cada processo lia a sua fatia
    # (linhas N a N+1999) na hora em que começava, ao longo de 1 h ou mais: produto ativado ou desativado nesse meio-tempo
    # deslocava a numeração, e um produto na emenda entre duas fatias era pulado. Também não há mais o teto de 50.000.
    login()
    products = sankhya_list_total_todos()
    lotes = [products[i:i + TAMANHO_LOTE] for i in range(0, len(products), TAMANHO_LOTE)]
    print(f"Carga total: {len(products)} produtos em {len(lotes)} lote(s) de até {TAMANHO_LOTE}", flush=True)
    falhas = run_lotes(run_lote, lotes)

    # inativos ou sem TGFEST: só o estoque, zerado (senão o iCorp fica com o último saldo recebido)
    zerar = sankhya_list_total_zerar_estoque()
    lotes_zerar = [zerar[i:i + TAMANHO_LOTE] for i in range(0, len(zerar), TAMANHO_LOTE)]
    print(f"Carga total (estoque zerado): {len(zerar)} produtos em {len(lotes_zerar)} lote(s) de até {TAMANHO_LOTE}", flush=True)
    falhas += run_lotes(run_lote_zerar, lotes_zerar)
    sys.exit(1 if falhas else 0)
