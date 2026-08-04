# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Visão Geral

Microserviço backend Flask (Python 3.11) do **Módulo PCP Confecção** do Grupo MPL. Expõe APIs REST consumidas por um frontend de PCP (Planejamento e Controle da Produção) e também serve algumas telas HTML. Integra dois mundos:

- **ERP CSW** (InterSystems Caché) — somente leitura, via JDBC/JayDeBeApi + `src/connection/CacheDB.jar`. Requer JDK no ambiente.
- **PostgreSQL** (schemas `pcp` e `backup`) — banco da própria aplicação, leitura e escrita.

Código, comentários, docstrings e nomes de identificadores estão em **português**. Mantenha esse padrão ao editar.

## Comandos

```bash
# Instalar dependências
pip install -r requirements.txt

# App principal (Flask; porta vem de PORTA_APLICACAO no _ambiente.env)
python app_run.py

# Serviço de automação (um ciclo completo e encerra)
python run_automacao.py

# Build/execução via Docker — dois containers a partir do mesmo código
docker build -f Dockerfile.appPrincipal -t pcp-app .      # gunicorn, 3 workers, porta 9000
docker build -f Dockerfile.automacao   -t pcp-automacao . # python run_automacao.py
```

Não há suíte de testes, linter ou formatter configurados no repositório. Verificação é feita executando o app/automação e chamando as APIs.

## Configuração obrigatória

Duas peças de configuração precisam estar corretas ou **nada funciona**:

1. `src/configApp/configApp.py` — `localProjeto` e `localArquivoParquet` apontam para a raiz do deploy (`/app` no Docker). Todos os caminhos de `.env`, `.csv` e `.parquet` derivam dessas variáveis. Em execução local fora do Docker é preciso ajustá-las.
2. `_ambiente.env` no diretório apontado por `localProjeto` (gitignored). Chaves usadas: `POSTGRES_DB`, `POSTGRES_DB2`, `POSTGRES_USER`, `POSTGRES_PASSWORD_SRV1`, `POSTGRES_PASSWORD_SRV2`, `POSTGRES_HOST_SRV1`, `POSTGRES_HOST_SRV2`, `POSTGRES_PORT`, `CSW_USER`, `CSW_PASSWORD`, `CSW_HOST`, `CAMINHO_PARQUET_FAT`, `PORTA_APLICACAO`, `freq_seg_realizado_fase`.

`app_run.py` falha na inicialização se `PORTA_APLICACAO` não existir (`int(None)`).

## Arquitetura em camadas

```
app_run.py  →  src/routes/__init__.py (blueprint raiz)  →  src/routes/*  →  src/models/* | src/service/*  →  src/connection/*
```

- **`src/routes/`** — um Blueprint por domínio, todos registrados manualmente em `src/routes/__init__.py`. **Ao criar uma rota nova é obrigatório adicionar o import E o `register_blueprint` nesse arquivo** — não há descoberta automática.
- **`src/models/`** — classes de domínio que encapsulam SQL. Arquivos com sufixo `_Csw` / `_CSW` falam com o ERP (isolados de propósito, ver diagrama em `docsProject/`); os demais falam com o PostgreSQL.
- **`src/service/`** — orquestração de fluxos multi-modelo (automação, separação/endereçamento de aviamentos, lead time, conferência).
- **`src/connection/`** — fábricas de conexão. `ConexaoPostgre.py` expõe várias funções para os dois servidores (`SRV1` = PCP principal, `SRV2` = WMS/Matriz) e helpers `Funcao_Inserir*` que fazem `to_sql` em chunks. `ConexaoERP.ConexaoInternoMPL()` é um context manager JDBC.

### Convenções das rotas

Padrão repetido em praticamente todos os arquivos de `src/routes/`:

- Prefixo `/pcp/api/...` para o módulo PCP; `/api/...` para o dashboard de 2ª Qualidade; rotas sem prefixo em `PortalWeb/rotasPlataformaWeb.py` que apenas fazem `render_template`.
- Auth por decorator `token_required` **redefinido localmente em cada arquivo de rota**, comparando o header `Authorization` com um token fixo hardcoded. Ao criar uma rota, copie o decorator do arquivo vizinho — é o padrão vigente.
- Modelos retornam `DataFrame` do pandas; a rota converte para lista de dicts iterando `dados.columns` / `dados.iterrows()` e devolve `jsonify`. Vários endpoints fazem `del dados` no fim (o volume de dados é grande e a memória importa).
- Parâmetros são lidos com defaults string (`request.args.get('codEmpresa','1')`, `data.get('x','-')`), e construtores de modelo são chamados posicionalmente com `''` para preencher argumentos não usados (ex.: `Plano.Plano('','','','','','','',codEmpresa)`). Multi-empresa é sempre `codEmpresa`, default `'1'`.
- SQL é montado por f-string com interpolação direta dos parâmetros nas consultas de leitura; inserts/updates usam placeholders `%s`. Ao adicionar escrita, siga o padrão parametrizado.

### Congelamento em CSV/Parquet (estratégia de performance)

Rotinas caras (tendência de plano, análise de materiais, simulações, faturamento) gravam resultados intermediários em `dados/*.csv` e `dados/*.parquet`, e APIs de detalhamento leem esses arquivos em vez de recalcular. Os nomes carregam a chave do contexto (`tendenciaPlano-{codPlano}.csv`, `EstruturacaoPrevisao{codPlano}_Simulacao{nomeSimulacao}.csv`, `requisicoesEmAberto.csv`, ...), e `dados/backup/` guarda histórico diário.

Implicações práticas: **existe acoplamento por arquivo entre endpoints** — mudar o schema/nome de um congelado quebra as APIs que o consomem depois. A tabela completa de arquivo → descrição → API que o gera está no `Readme.md`, seção 4. Nota: alguns caminhos são relativos (`./dados/compVar.parquet`), portanto o processo precisa rodar com CWD na raiz do projeto.

### Serviço de automação (ETL ERP → PostgreSQL)

`run_automacao.py` executa **um ciclo** das rotinas e encerra — o agendamento é externo (cron/scheduler do host, sincronizado com o SO Linux). Cada rotina faz seu próprio controle de intervalo:

1. Instancia `ServicoAutomacao(idServico, descricaoServico)`.
2. Chama `obtentendo_intervalo_atualizacao_servico()` — segundos desde a última execução registrada.
3. Só executa se o intervalo for maior que `intervalo_automacao`; registra início com `inserindo_automacao()` e status com `update_controle_automacao()`.

O histórico e o controle vivem nas tabelas `pcp."ServicoAutomacao"` (catálogo, com `intervaloAtualizacao(min)`) e `pcp."ControleAutomacao"` (execuções). `GET /pcp/api/ServicoAutomacao` expõe a última atualização e a próxima prevista. IDs de serviço em uso são strings inconsistentes (`'1'`, `'02'`, `'3'`, `'004'`, `'05'`, `'006'`, `'007'`) — ao adicionar um serviço, confirme o id existente na tabela antes de escolher.

Rotinas ativas e periodicidades estão documentadas no `Readme.md`, seção 5.2.

## Frontend servido

`templates/` (HTML) + `static/` (css/js/imagens) são servidos pelo Flask via `PortalWeb/rotasPlataformaWeb.py`. Existe uma cópia paralela em `src/static/` — o Flask usa a de **raiz** (`static/`); ao editar assets, confirme qual arquivo está realmente sendo servido antes de assumir.
