import sys

from sankhya.sankhya_client import login
from sankhya.sankhya_fetch import sankhya_list_parceiros_todos
from updates.process import process_parceiros_integration_lote, run_lotes
from utils import logging_config

logging_config()

TAMANHO_LOTE = 5000


def run_lote(parceiros):
    login()  # cada processo faz seu login independente (sem efeito pela conexão central)
    return len(parceiros), process_parceiros_integration_lote(parceiros)


if __name__ == "__main__":
    # A lista é lida uma vez só, aqui, e repartida entre os processos (23/11/2026), como na carga total de produtos:
    # sem parceiro pulado na emenda das fatias e sem o teto de 200.000.
    login()
    parceiros = sankhya_list_parceiros_todos()
    lotes = [parceiros[i:i + TAMANHO_LOTE] for i in range(0, len(parceiros), TAMANHO_LOTE)]
    print(f"Carga de parceiros: {len(parceiros)} em {len(lotes)} lote(s) de até {TAMANHO_LOTE}", flush=True)
    falhas = run_lotes(run_lote, lotes)
    sys.exit(1 if falhas else 0)
