
import logging

from icorp.icorp_send import icorp_post
from sankhya.sankhya_client import login
from sankhya.sankhya_fetch import sankhya_fetch_json_produto, sankhya_fetch_json_estoque, sankhya_list_codprod
from utils import logging_config

logging_config()

if __name__ == "__main__":
    token = login()

    products = sankhya_list_codprod(token)

    for product in products:
        logging.info(f"▶️ Iniciando integração do produto: {product}")

        # envio do cadastro
        logging.info(f"🚀 Enviando dados do produto")
        dados_produto = sankhya_fetch_json_produto(token, product)
        icorp_post('ProdutoUpdate', dados_produto)

        # envio da imagem
        logging.info(f"🚀 Enviando imagem do produto")
        imagem = [
            {
                "CodigoProduto": f"{product}",
                "Ordem": 0,
                "Descricao": f"{product}.jpg",
                "URLImagem": f"https://img.casacontente.com.br/{product}.jpg",
                "IsImgPadrao": False
            }
        ]
        icorp_post('ProdutoImagens', imagem)

        # enviar estoque
        logging.info(f"🚀 Enviando estoque do produto")
        estoque = sankhya_fetch_json_estoque(token, product)
        icorp_post('Saldos_Atualiza', estoque)

        logging.info(45*'=')