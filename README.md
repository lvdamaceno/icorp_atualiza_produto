# icorp_atualiza_produto

Lê produtos, estoque e parceiros do Sankhya e envia ao iCorp (crons no Render: `minutes`, `total` e `parceiro`).

## Conexão com o Sankhya

- **Direto (padrão):** `SANKHYA_URL_BASE`, `SANKHYA_TOKEN`, `SANKHYA_APP_KEY`, `SANKHYA_USERNAME` e `SANKHYA_PASSWORD` (login antigo, a cada execução).
- **Pela conexão central (`sankhya-central`):** com `SANKHYA_PROXY_URL=http://sankhya-central:10000` e `SANKHYA_PROXY_CHAVE` (chave do app criada na central), as consultas passam por ela, que guarda o token, respeita os limites e pausa quando o Sankhya falha, junto com os outros apps. O login direto deixa de ser usado. Só funciona com o cron na mesma região da central no Render (Virgínia).

## Carga total (`updates.total_update`)

Lê os produtos ativos (cerca de 37 mil em 23/11/2026) em lotes de 2.000 por processo (`MAX_WORKERS`, 2 no Render) e, em cada processo:

- lê o Sankhya em lotes de `SANKHYA_LOTE` produtos (padrão 40) numa consulta só, e já lê o lote seguinte enquanto este vai para o iCorp. Um a um eram 2 chamadas e 0,48 s por produto; em lote, 0,06 s por produto e 80 vezes menos chamadas à conexão central;
- envia ao iCorp `ICORP_THREADS` produtos ao mesmo tempo (padrão 4; com 2 processos, 8 no total);
- se a leitura de um lote falhar, aqueles produtos vão um a um, como antes.

Os dados enviados ao iCorp são os mesmos de sempre (ProdutoUpdate, CodigoBarra, ProdutoImagens e Saldos_Atualiza por produto). `ICORP_URL` troca o endereço do iCorp (só para teste).
