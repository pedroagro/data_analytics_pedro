# Databricks notebook source
# MAGIC %md
# MAGIC # StartupCRM | 99 | Reinicialização opcional
# MAGIC
# MAGIC Execute somente quando desejar apagar todo o projeto e reconstruí-lo do zero.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

for schema_name in [
    SCHEMAS["gold"],
    SCHEMAS["silver"],
    SCHEMAS["bronze"],
    SCHEMAS["landing"],
]:
    object_type = "SCHEMA" if USE_UNITY_CATALOG else "DATABASE"
    spark.sql(f"DROP {object_type} IF EXISTS {namespace_name(schema_name)} CASCADE")
    print(f"Removido: {namespace_name(schema_name)}")

print("Ambiente StartupCRM removido.")
