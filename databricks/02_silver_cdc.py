# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: CDC com MERGE
# MAGIC Bronze (versões de cada registro) -> Silver (1 linha por id, estado atual, exclusões aplicadas).
# MAGIC Idempotente: rodar de novo sobre os mesmos dados não altera nada.

# COMMAND ----------
CAT = "credito_lab"

# Contrato explícito da Silver: tipos fixos, escolhidos por nós (colunas novas da fonte NÃO entram sozinhas)
ENTIDADES = {
    "clientes": dict(
        colunas="id BIGINT, nome STRING, renda_mensal DECIMAL(12,2), cidade STRING, score INT, "
                "created_at TIMESTAMP, updated_at TIMESTAMP, is_deleted BOOLEAN",
        tipado="CAST(id AS BIGINT) AS id, nome, CAST(renda_mensal AS DECIMAL(12,2)) AS renda_mensal, cidade, "
               "CAST(score AS INT) AS score, to_timestamp(created_at) AS created_at, "
               "to_timestamp(updated_at) AS updated_at, CAST(is_deleted AS BOOLEAN) AS is_deleted",
        tombstones=None,
    ),
    "contratos": dict(
        colunas="id BIGINT, cliente_id BIGINT, valor_principal DECIMAL(12,2), taxa_mensal DECIMAL(6,4), "
                "n_parcelas INT, status STRING, created_at TIMESTAMP, updated_at TIMESTAMP, is_deleted BOOLEAN",
        tipado="CAST(id AS BIGINT) AS id, CAST(cliente_id AS BIGINT) AS cliente_id, "
               "CAST(valor_principal AS DECIMAL(12,2)) AS valor_principal, "
               "CAST(taxa_mensal AS DECIMAL(6,4)) AS taxa_mensal, CAST(n_parcelas AS INT) AS n_parcelas, status, "
               "to_timestamp(created_at) AS created_at, to_timestamp(updated_at) AS updated_at, "
               "CAST(is_deleted AS BOOLEAN) AS is_deleted",
        tombstones=None,
    ),
    "parcelas": dict(
        colunas="id BIGINT, contrato_id BIGINT, numero INT, valor DECIMAL(12,2), vencimento DATE, "
                "data_pagamento DATE, status STRING, created_at TIMESTAMP, updated_at TIMESTAMP, is_deleted BOOLEAN",
        tipado="CAST(id AS BIGINT) AS id, CAST(contrato_id AS BIGINT) AS contrato_id, "
               "CAST(numero AS INT) AS numero, CAST(valor AS DECIMAL(12,2)) AS valor, "
               "CAST(vencimento AS DATE) AS vencimento, CAST(data_pagamento AS DATE) AS data_pagamento, status, "
               "to_timestamp(created_at) AS created_at, to_timestamp(updated_at) AS updated_at, "
               "CAST(is_deleted AS BOOLEAN) AS is_deleted",
        tombstones="parcelas_deletions",   # tabela bronze com os hard deletes
    ),
}

# COMMAND ----------
# Cria as tabelas Silver com Change Data Feed ligado (registra insert/update/delete de cada MERGE)
for nome, cfg in ENTIDADES.items():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CAT}.silver.{nome} ({cfg['colunas']})
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
print("tabelas silver prontas")

# COMMAND ----------
def sincronizar(nome: str) -> None:
    cfg = ENTIDADES[nome]
    tomb = cfg["tombstones"]

    # Passo 1: a versão MAIS RECENTE de cada id (o MERGE exige 1 linha de origem por chave).
    # Se existe tombstone igual ou posterior à versão, o registro foi apagado e não entra.
    filtro_tomb = ""
    if tomb:
        filtro_tomb = f"""AND NOT EXISTS (
            SELECT 1 FROM {CAT}.bronze.{tomb} d
            WHERE d.id = b.id AND to_timestamp(d.deleted_at) >= b.updated_at)"""
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_{nome} AS
        SELECT * EXCEPT (rn) FROM (
            SELECT {cfg['tipado']},
                   row_number() OVER (PARTITION BY id
                                      ORDER BY to_timestamp(updated_at) DESC, _ingested_at DESC) AS rn
            FROM {CAT}.bronze.{nome}
        ) b
        WHERE b.rn = 1 {filtro_tomb}
    """)

    # Passo 2: upsert. A guarda "s.updated_at > t.updated_at" protege contra dado atrasado/repetido.
    r = spark.sql(f"""
        MERGE INTO {CAT}.silver.{nome} t
        USING v_{nome} s ON t.id = s.id
        WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).first().asDict()
    print(f"[{nome}] upsert:", r)

    # Passo 3: hard deletes. Só apaga se o tombstone for igual ou posterior à última versão conhecida.
    if tomb:
        r = spark.sql(f"""
            MERGE INTO {CAT}.silver.{nome} t
            USING (SELECT id, max(to_timestamp(deleted_at)) AS deleted_at
                   FROM {CAT}.bronze.{tomb} GROUP BY id) d
            ON t.id = d.id
            WHEN MATCHED AND d.deleted_at >= t.updated_at THEN DELETE
        """).first().asDict()
        print(f"[{nome}] deletes:", r)

# COMMAND ----------
for nome in ENTIDADES:
    sincronizar(nome)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Conferência 1: as parcelas que vimos na Bronze com várias versões. Devem estar na versão mais recente.
# MAGIC SELECT id, status, data_pagamento, updated_at FROM credito_lab.silver.parcelas WHERE id IN (517, 781, 882);

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Conferência 2 (reconciliação): Silver deve ter 1 linha por id, menos os ids apagados
# MAGIC SELECT
# MAGIC   (SELECT count(DISTINCT id) FROM credito_lab.bronze.parcelas)
# MAGIC   - (SELECT count(DISTINCT d.id) FROM credito_lab.bronze.parcelas_deletions d
# MAGIC      WHERE d.id IN (SELECT id FROM credito_lab.bronze.parcelas)) AS esperado,
# MAGIC   (SELECT count(*) FROM credito_lab.silver.parcelas) AS silver;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Conferência 3: o Change Data Feed registrou o que o MERGE fez (insert, update_postimage, delete...)
# MAGIC SELECT _change_type, count(*) AS linhas
# MAGIC FROM table_changes('credito_lab.silver.parcelas', 0)
# MAGIC GROUP BY _change_type;
