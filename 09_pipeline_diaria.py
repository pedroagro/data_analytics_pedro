# Databricks notebook source
# MAGIC %md
# MAGIC # StartupCRM | 09 | Pipeline diária
# MAGIC
# MAGIC Este é o notebook que deve ser executado ou agendado diariamente. Cada etapa interrompe a pipeline automaticamente em caso de erro.

# COMMAND ----------

# MAGIC %run ./01_setup

# COMMAND ----------

# MAGIC %run ./02_landing_ingestion

# COMMAND ----------

# MAGIC %run ./03_bronze

# COMMAND ----------

# MAGIC %run ./04_silver_dim_usuario

# COMMAND ----------

# MAGIC %run ./05_silver_fato_post

# COMMAND ----------

# MAGIC %run ./06_silver_fato_todo

# COMMAND ----------

# MAGIC %run ./07_gold_relatorios

# COMMAND ----------

# MAGIC %run ./08_validacoes

# COMMAND ----------

print("Pipeline diária StartupCRM finalizada com sucesso.")
