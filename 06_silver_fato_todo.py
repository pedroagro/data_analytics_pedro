# Databricks notebook source
# MAGIC %md
# MAGIC # StartupCRM | 06 | Silver fato de tarefas

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

bronze_todos = spark.table(TABLES["bronze_todos"])
assert_not_empty(bronze_todos, TABLES["bronze_todos"])

fato_todo = bronze_todos.select(
    surrogate_key("TODO", F.col("id")).alias("SRK_TODO"),
    surrogate_key("USR", F.col("userId")).alias("SRK_USR"),
    normalize_integer(F.col("id")).alias("COD_TODO"),
    normalize_integer(F.col("userId")).alias("COD_USR"),
    normalize_text(F.col("title")).alias("DSC_TITULO_TODO"),
    normalize_boolean(F.col("completed")).alias("FLG_CONCLUIDO"),
    F.coalesce(F.col("data_insercao"), F.current_timestamp()).alias("DAT_INSCE"),
    F.coalesce(F.col("data_atualizacao"), F.current_timestamp()).alias("DAT_ATLC"),
)

duplicate_count = (
    fato_todo.groupBy("SRK_TODO")
             .count()
             .filter(F.col("count") > 1)
             .count()
)
if duplicate_count > 0:
    raise ValueError("fato_todo: surrogate keys duplicadas")

overwrite_delta(fato_todo, TABLES["silver_fato_todo"])
print("Fato de tarefas criada com sucesso.")
