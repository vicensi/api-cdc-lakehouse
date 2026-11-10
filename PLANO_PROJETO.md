# Projeto: API -> CDC -> Databricks -> dbt (preparação para o trabalho de Eng. de Dados Jr)

## Objetivo
Treinar o ciclo completo que um engenheiro júnior encontra: ingerir dados de uma API, capturar mudanças (CDC),
organizar em camadas no Databricks (Bronze/Silver/Gold) e transformar com dbt, com testes e documentação.

## Decisões já tomadas
- Domínio: crédito (contratos, parcelas, pagamentos, clientes).
- Fonte: API mock própria em FastAPI (Docker) com inserts, updates e deletes controláveis.
- Plataforma: Databricks Free Edition + dbt-databricks. Sem Airflow no início (entra na Fase 7, opcional).
- Modo: conceito antes de código; estudo vs produção sempre marcados.

## Conceito-chave: CDC com fonte API
APIs não expõem log de transações. O CDC aqui é DERIVADO:
1. A API devolve registros com `updated_at` (incremental) e um endpoint de exclusões (ou flag `is_deleted`).
2. Bronze guarda tudo que chegou, append-only, com `_ingested_at` e `_source_file`.
3. Silver aplica as mudanças sobre o estado atual (MERGE / Auto CDC), com Change Data Feed ligado.
4. dbt snapshots (SCD2) guardam o histórico das mudanças na Gold.
[PRODUÇÃO] CDC de banco usa Debezium/Kafka lendo o WAL; aqui comparamos estado em vez de ler log.

## Arquitetura
Mock API (FastAPI) -> Ingestor Python (local) -> Volume Unity Catalog (JSON) -> Bronze (Auto Loader)
-> Silver (CDC com MERGE + Change Data Feed) -> Gold (dbt: staging, snapshots, marts, testes)

## Ponto de atenção de rede (verificar na Fase 1)
Databricks Free Edition roda na nuvem e tem saída de internet restrita; ele NÃO enxerga sua API em localhost.
Por isso o ingestor roda local e envia os JSON para um Volume via Databricks SDK/CLI (padrão "landing zone").
Alternativa: expor a API com túnel (cloudflared/ngrok) ou hospedar a mock em serviço gratuito.

## Fases
1. Ambiente: workspace Free, catálogo/schemas, Volume, token, repo Git. Validar rede e conexão dbt.
2. Mock API: entidades (clientes, contratos, parcelas, pagamentos), `updated_at`, soft delete, paginação, endpoint que simula mudanças.
3. Ingestão: extrator incremental (watermark), idempotência, retries, envio ao Volume.
4. Bronze: Auto Loader, schema evolution, metadados de ingestão.
5. Silver CDC: MERGE por chave, tratamento de deletes, Change Data Feed, reprocessamento.
6. dbt: staging, snapshots SCD2, marts (carteira, inadimplência), testes, freshness, docs.
7. Opcional: orquestração (Databricks Workflows ou Airflow), CI com GitHub Actions.
8. Documentação: README, diagrama, PROJECT_REFERENCE.md, decisões de estudo vs produção.

## Cenários de CDC que o projeto precisa cobrir
- Insert novo; update de status (ex.: parcela pendente -> paga); delete lógico e físico;
  evento duplicado; evento atrasado (chega fora de ordem); campo novo na API (schema drift).

## Validação (como saber que aprendeu)
- Rodar o mesmo lote duas vezes sem duplicar (idempotência).
- Reprocessar Bronze e chegar ao mesmo Silver.
- Explicar em voz alta a diferença entre CDC por log e CDC por comparação.
