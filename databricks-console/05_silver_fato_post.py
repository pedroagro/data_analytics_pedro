# Databricks notebook source
# MAGIC %md
# MAGIC # StartupCRM | 05 | Silver fato de posts

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

bronze_posts = spark.table(TABLES["bronze_posts"])
assert_not_empty(bronze_posts, TABLES["bronze_posts"])

fato_post = bronze_posts.select(
    surrogate_key("POST", F.col("id")).alias("SRK_POST"),
    surrogate_key("USR", F.col("userId")).alias("SRK_USR"),
    normalize_integer(F.col("id")).alias("COD_POST"),
    normalize_integer(F.col("userId")).alias("COD_USR"),
    normalize_text(F.col("title")).alias("DSC_TITULO_POST"),
    normalize_text(F.col("body")).alias("DSC_CORPO_POST"),
    F.coalesce(F.col("data_insercao"), F.current_timestamp()).alias("DAT_INSCE"),
    F.coalesce(F.col("data_atualizacao"), F.current_timestamp()).alias("DAT_ATLC"),
)

duplicate_count = (
    fato_post.groupBy("SRK_POST")
             .count()
             .filter(F.col("count") > 1)
             .count()
)
if duplicate_count > 0:
    raise ValueError("fato_post: surrogate keys duplicadas")

overwrite_delta(fato_post, TABLES["silver_fato_post"])
print("Fato de posts criada com sucesso.")
