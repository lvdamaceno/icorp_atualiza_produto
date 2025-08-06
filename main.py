
import logging

from icorp.icorp_send import icorp_post
from sankhya.sankhya_client import snk_post, login
from sankhya.sankhya_fetch import sankhya_fetch_json_produto

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

if __name__ == "__main__":
    token = login()

    produto = 442248

    # envio do cadastro
    dados_produto = sankhya_fetch_json_produto(token, produto)
    icorp_post('ProdutoUpdate', dados_produto)

    # envio da imagem
    imagem = [
        {
            "CodigoProduto": f"{produto}",
            "Ordem": 0,
            "Descricao": f"{produto}.jpg",
            "URLImagem": f"https://img.casacontente.com.br/{produto}.jpg",
            "IsImgPadrao": False
        }
    ]
    icorp_post('ProdutoImagens', imagem)

    # enviar estoque