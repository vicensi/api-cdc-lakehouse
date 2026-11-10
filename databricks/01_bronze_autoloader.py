# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Auto Loader (Landing Volume -> Delta)
# MAGIC Cópia fiel e append-only. Cada linha é uma versão do registro, com metadados de origem.

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Volume para checkpoints e schemas do Auto Loader (estado do streaming)
# MAGIC CREATE VOLUME IF NOT EXISTS credito_lab.landing.control;

# COMMAND ----------
from pyspark.sql import functions as F

CATALOGO = "credito_lab"
LANDING = f"/Volumes/{CATALOGO}/landing/api_raw"
CONTROLE = f"/Volumes/{CATALOGO}/landing/control"  # checkpoints + schemas inferidos

FONTES = {  # pasta no landing -> tabela na bronze
    "clientes": "clientes",
    "contratos": "contratos",
    "parcelas": "parcelas",
    "parcelas_deletions": "parcelas_deletions",
}

# COMMAND ----------
def ingerir_bronze(pasta: str, tabela: str) -> None:
    destino = f"{CATALOGO}.bronze.{tabela}"
    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CONTROLE}/schemas/{tabela}")  # guarda o schema inferido
        .option("cloudFiles.inferColumnTypes", "true")  # sem isso tudo viria como string
        .load(f"{LANDING}/{pasta}")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        )
        .writeStream
        .option("checkpointLocation", f"{CONTROLE}/checkpoints/{tabela}")  # lembra quais arquivos já leu
        .option("mergeSchema", "true")
        .trigger(availableNow=True)  # processa o que existe e encerra
        .toTable(destino)
    )
    query.awaitTermination()
    print(f"ok: {destino}")

# COMMAND ----------
for pasta, tabela in FONTES.items():
    ingerir_bronze(pasta, tabela)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Verificação 1: linhas por tabela
# MAGIC SELECT 'clientes' AS t, count(*) AS linhas, count(DISTINCT id) AS ids FROM credito_lab.bronze.clientes
# MAGIC UNION ALL SELECT 'contratos', count(*), count(DISTINCT id) FROM credito_lab.bronze.contratos
# MAGIC UNION ALL SELECT 'parcelas', count(*), count(DISTINCT id) FROM credito_lab.bronze.parcelas
# MAGIC UNION ALL SELECT 'parcelas_deletions', count(*), count(DISTINCT id) FROM credito_lab.bronze.parcelas_deletions;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Verificação 2: linhas por arquivo de origem (rastreabilidade)
# MAGIC SELECT _source_file, count(*) AS linhas FROM credito_lab.bronze.parcelas GROUP BY _source_file ORDER BY _source_file;
