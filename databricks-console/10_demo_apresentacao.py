# Databricks notebook source
# MAGIC %md
# MAGIC # StartupCRM | 10 | Roteiro da apresentação
# MAGIC
# MAGIC Execute este notebook após a pipeline. Ele mostra os objetos criados, os volumes por camada, amostras dos dados e os dois relatórios finais.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

volume_rows = []
for layer_name, objects in {
    "LANDING": [
        ("users", TABLES["landing_users"]),
        ("posts", TABLES["landing_posts"]),
        ("todos", TABLES["landing_todos"]),
    ],
    "BRONZE": [
        ("users", TABLES["bronze_users"]),
        ("posts", TABLES["bronze_posts"]),
        ("todos", TABLES["bronze_todos"]),
    ],
    "SILVER": [
        ("dim_usuario", TABLES["silver_dim_usuario"]),
        ("fato_post", TABLES["silver_fato_post"]),
        ("fato_todo", TABLES["silver_fato_todo"]),
    ],
}.items():
    for object_name, full_name in objects:
        volume_rows.append((layer_name, object_name, spark.table(full_name).count()))

volume_df = spark.createDataFrame(
    volume_rows,
    ["CAMADA", "OBJETO", "QUANTIDADE_REGISTROS"],
)
display(volume_df.orderBy("CAMADA", "OBJETO"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Landing
# MAGIC
# MAGIC A Landing mantém o payload da API com as estruturas aninhadas e adiciona apenas as duas colunas obrigatórias de auditoria.

# COMMAND ----------

display(spark.table(TABLES["landing_users"]).orderBy(F.col("data_atualizacao").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze
# MAGIC
# MAGIC A Bronze apresenta o usuário achatado, sem mnemônicos e com uma única versão atual por ID.

# COMMAND ----------

display(spark.table(TABLES["bronze_users"]).orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver
# MAGIC
# MAGIC A Silver aplica modelagem dimensional, mnemônicos, tratamento de nulos e surrogate keys determinísticas. O usuário de código menos um representa o membro desconhecido.

# COMMAND ----------

display(spark.table(TABLES["silver_dim_usuario"]).orderBy("COD_USR"))
display(spark.table(TABLES["silver_fato_post"]).orderBy("COD_POST").limit(20))
display(spark.table(TABLES["silver_fato_todo"]).orderBy("COD_TODO").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relatório 1
# MAGIC
# MAGIC Quantidade de posts e tarefas por usuário, ordenada pela quantidade de posts em ordem decrescente.

# COMMAND ----------

display(
    spark.table(TABLES["gold_relatorio_usuario"])
         .orderBy(F.col("NUM_QTD_POST").desc(), F.col("COD_USR"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relatório 2
# MAGIC
# MAGIC Taxa de conclusão de tarefas por cidade, ordenada pelo percentual em ordem decrescente.

# COMMAND ----------

display(
    spark.table(TABLES["gold_relatorio_cidade"])
         .orderBy(F.col("NUM_PCT_TODO_CONCLUIDO").desc(), F.col("NME_CID"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validações mais recentes

# COMMAND ----------

display(
    spark.table(TABLES["gold_validacoes"])
         .orderBy(F.col("DAT_EXECUCAO").desc())
         .limit(100)
)
