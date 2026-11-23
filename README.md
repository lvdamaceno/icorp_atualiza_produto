# icorp_atualiza_produto

Lê produtos, estoque e parceiros do Sankhya e envia ao iCorp (crons no Render: `minutes`, `total` e `parceiro`).

## Conexão com o Sankhya

- **Direto (padrão):** `SANKHYA_URL_BASE`, `SANKHYA_TOKEN`, `SANKHYA_APP_KEY`, `SANKHYA_USERNAME` e `SANKHYA_PASSWORD` (login antigo, a cada execução).
- **Pela conexão central (`sankhya-central`):** com `SANKHYA_PROXY_URL=http://sankhya-central:10000` e `SANKHYA_PROXY_CHAVE` (chave do app criada na central), as consultas passam por ela, que guarda o token, respeita os limites e pausa quando o Sankhya falha, junto com os outros apps. O login direto deixa de ser usado. Só funciona com o cron na mesma região da central no Render (Virgínia).
