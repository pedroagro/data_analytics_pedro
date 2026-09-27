# Databricks notebook source
# MAGIC %md
# MAGIC # StartupCRM | 03 | Camada Bronze
# MAGIC
# MAGIC Achata as estruturas aninhadas de usuários, seleciona os campos de interesse, mantém os nomes técnicos da origem e deduplica pela versão mais recente de cada ID.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

landing_users = spark.table(TABLES["landing_users"])
landing_posts = spark.table(TABLES["landing_posts"])
landing_todos = spark.table(TABLES["landing_todos"])

assert_not_empty(landing_users, TABLES["landing_users"])
assert_not_empty(landing_posts, TABLES["landing_posts"])
assert_not_empty(landing_todos, TABLES["landing_todos"])

# COMMAND ----------

bronze_users = (
    latest_by_key(landing_users, ["id"])
    .select(
        "id",
        "name",
        "username",
        "email",
        F.col("address.street").alias("street"),
        F.col("address.suite").alias("suite"),
        F.col("address.city").alias("city"),
        F.col("address.zipcode").alias("zipcode"),
        F.col("address.geo.lat").alias("lat"),
        F.col("address.geo.lng").alias("lng"),
        "phone",
        "website",
        F.col("company.name").alias("company_name"),
        F.col("company.catchPhrase").alias("catchPhrase"),
        F.col("company.bs").alias("bs"),
        "data_insercao",
        "data_atualizacao",
    )
)

bronze_posts = (
    latest_by_key(landing_posts, ["id"])
    .select(
        "userId",
        "id",
        "title",
        "body",
        "data_insercao",
        "data_atualizacao",
    )
)

bronze_todos = (
    latest_by_key(landing_todos, ["id"])
    .select(
        "userId",
        "id",
        "title",
        "completed",
        "data_insercao",
        "data_atualizacao",
    )
)

# COMMAND ----------

for object_name, dataframe, key_column in [
    ("bronze_users", bronze_users, "id"),
    ("bronze_posts", bronze_posts, "id"),
    ("bronze_todos", bronze_todos, "id"),
]:
    assert_not_empty(dataframe, object_name)
    duplicate_count = (
        dataframe.groupBy(key_column)
                 .count()
                 .filter(F.col("count") > 1)
                 .count()
    )
    if duplicate_count > 0:
        raise ValueError(f"{object_name}: foram encontradas chaves duplicadas")

overwrite_delta(bronze_users, TABLES["bronze_users"])
overwrite_delta(bronze_posts, TABLES["bronze_posts"])
overwrite_delta(bronze_todos, TABLES["bronze_todos"])

print("Camada Bronze concluída com sucesso.")
