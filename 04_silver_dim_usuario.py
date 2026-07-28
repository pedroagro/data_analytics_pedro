# Databricks notebook source
# MAGIC %md
# MAGIC # StartupCRM | 04 | Silver dimensão de usuários
# MAGIC
# MAGIC Padroniza os dados de usuários, aplica mnemônicos, trata nulos e cria a surrogate key determinística.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

bronze_users = spark.table(TABLES["bronze_users"])
assert_not_empty(bronze_users, TABLES["bronze_users"])

dim_usuario = bronze_users.select(
    surrogate_key("USR", F.col("id")).alias("SRK_USR"),
    normalize_integer(F.col("id")).alias("COD_USR"),
    normalize_text(F.col("name")).alias("NME_COMP_USR"),
    normalize_text(F.col("username")).alias("NME_USR"),
    normalize_text(F.col("email")).alias("DSC_EMAIL"),
    normalize_text(F.col("street")).alias("DSC_LOGRADOURO"),
    normalize_text(F.col("suite")).alias("DSC_COMPL_END"),
    normalize_text(F.col("city")).alias("NME_CID"),
    normalize_text(F.col("zipcode")).alias("NUM_CEP"),
    normalize_decimal(F.col("lat")).alias("NUM_LATITUDE"),
    normalize_decimal(F.col("lng")).alias("NUM_LONGITUDE"),
    normalize_text(F.col("phone")).alias("NUM_FONE"),
    normalize_text(F.col("website")).alias("DSC_SITE"),
    normalize_text(F.col("company_name")).alias("NME_EMPRESA"),
    normalize_text(F.col("catchPhrase")).alias("DSC_SLOGAN_EMPRESA"),
    normalize_text(F.col("bs")).alias("DSC_NEGOCIO_EMPRESA"),
    F.coalesce(F.col("data_insercao"), F.current_timestamp()).alias("DAT_INSCE"),
    F.coalesce(F.col("data_atualizacao"), F.current_timestamp()).alias("DAT_ATLC"),
)

unknown_member = spark.range(1).select(
    surrogate_key("USR", F.lit(-1)).alias("SRK_USR"),
    F.lit(-1).cast("bigint").alias("COD_USR"),
    F.lit("N/I").alias("NME_COMP_USR"),
    F.lit("N/I").alias("NME_USR"),
    F.lit("N/I").alias("DSC_EMAIL"),
    F.lit("N/I").alias("DSC_LOGRADOURO"),
    F.lit("N/I").alias("DSC_COMPL_END"),
    F.lit("N/I").alias("NME_CID"),
    F.lit("N/I").alias("NUM_CEP"),
    F.lit(-1).cast("decimal(18,6)").alias("NUM_LATITUDE"),
    F.lit(-1).cast("decimal(18,6)").alias("NUM_LONGITUDE"),
    F.lit("N/I").alias("NUM_FONE"),
    F.lit("N/I").alias("DSC_SITE"),
    F.lit("N/I").alias("NME_EMPRESA"),
    F.lit("N/I").alias("DSC_SLOGAN_EMPRESA"),
    F.lit("N/I").alias("DSC_NEGOCIO_EMPRESA"),
    F.current_timestamp().alias("DAT_INSCE"),
    F.current_timestamp().alias("DAT_ATLC"),
)

final_dim_usuario = (
    dim_usuario.unionByName(unknown_member)
               .dropDuplicates(["SRK_USR"])
)

duplicate_count = (
    final_dim_usuario.groupBy("SRK_USR")
                     .count()
                     .filter(F.col("count") > 1)
                     .count()
)
if duplicate_count > 0:
    raise ValueError("dim_usuario: surrogate keys duplicadas")

overwrite_delta(final_dim_usuario, TABLES["silver_dim_usuario"])
print("Dimensão de usuários criada com sucesso.")
