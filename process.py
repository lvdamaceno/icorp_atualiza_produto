import logging

from icorp.icorp_send import icorp_post
from sankhya.sankhya_fetch import sankhya_fetch_json_produto, sankhya_fetch_json_estoque

def process_product(token, product):
    logging.info(f"🚀 Enviando dados do produto")
    dados_produto = sankhya_fetch_json_produto(token, product)
    icorp_post('ProdutoUpdate', dados_produto)

def process_image(product):
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

def process_estoque(token, product):
    logging.info(f"🚀 Enviando estoque do produto")
    estoque = sankhya_fetch_json_estoque(token, product)
    icorp_post('Saldos_Atualiza', estoque)