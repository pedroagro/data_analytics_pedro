# Databricks notebook source
# MAGIC %md
# MAGIC # StartupCRM | 01 | Preparação do ambiente
# MAGIC
# MAGIC Cria os schemas e todas as tabelas Delta necessárias. A execução é idempotente.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

for schema_name in SCHEMAS.values():
    create_schema_if_not_exists(schema_name)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLES["landing_users"]} (
    id BIGINT,
    name STRING,
    username STRING,
    email STRING,
    address STRUCT<
        street: STRING,
        suite: STRING,
        city: STRING,
        zipcode: STRING,
        geo: STRUCT<lat: STRING, lng: STRING>
    >,
    phone STRING,
    website STRING,
    company STRUCT<
        name: STRING,
        catchPhrase: STRING,
        bs: STRING
    >,
    data_insercao TIMESTAMP,
    data_atualizacao TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLES["landing_posts"]} (
    userId BIGINT,
    id BIGINT,
    title STRING,
    body STRING,
    data_insercao TIMESTAMP,
    data_atualizacao TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLES["landing_todos"]} (
    userId BIGINT,
    id BIGINT,
    title STRING,
    completed BOOLEAN,
    data_insercao TIMESTAMP,
    data_atualizacao TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLES["bronze_users"]} (
    id BIGINT,
    name STRING,
    username STRING,
    email STRING,
    street STRING,
    suite STRING,
    city STRING,
    zipcode STRING,
    lat STRING,
    lng STRING,
    phone STRING,
    website STRING,
    company_name STRING,
    catchPhrase STRING,
    bs STRING,
    data_insercao TIMESTAMP,
    data_atualizacao TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLES["bronze_posts"]} (
    userId BIGINT,
    id BIGINT,
    title STRING,
    body STRING,
    data_insercao TIMESTAMP,
    data_atualizacao TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLES["bronze_todos"]} (
    userId BIGINT,
    id BIGINT,
    title STRING,
    completed BOOLEAN,
    data_insercao TIMESTAMP,
    data_atualizacao TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLES["silver_dim_usuario"]} (
    SRK_USR STRING,
    COD_USR BIGINT,
    NME_COMP_USR STRING,
    NME_USR STRING,
    DSC_EMAIL STRING,
    DSC_LOGRADOURO STRING,
    DSC_COMPL_END STRING,
    NME_CID STRING,
    NUM_CEP STRING,
    NUM_LATITUDE DECIMAL(18,6),
    NUM_LONGITUDE DECIMAL(18,6),
    NUM_FONE STRING,
    DSC_SITE STRING,
    NME_EMPRESA STRING,
    DSC_SLOGAN_EMPRESA STRING,
    DSC_NEGOCIO_EMPRESA STRING,
    DAT_INSCE TIMESTAMP,
    DAT_ATLC TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLES["silver_fato_post"]} (
    SRK_POST STRING,
    SRK_USR STRING,
    COD_POST BIGINT,
    COD_USR BIGINT,
    DSC_TITULO_POST STRING,
    DSC_CORPO_POST STRING,
    DAT_INSCE TIMESTAMP,
    DAT_ATLC TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLES["silver_fato_todo"]} (
    SRK_TODO STRING,
    SRK_USR STRING,
    COD_TODO BIGINT,
    COD_USR BIGINT,
    DSC_TITULO_TODO STRING,
    FLG_CONCLUIDO BOOLEAN,
    DAT_INSCE TIMESTAMP,
    DAT_ATLC TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLES["gold_validacoes"]} (
    DAT_EXECUCAO TIMESTAMP,
    NME_CAMADA STRING,
    NME_OBJETO STRING,
    NME_VALIDACAO STRING,
    FLG_SUCESSO BOOLEAN,
    DSC_RESULTADO STRING
)
USING DELTA
""")

print("Schemas e tabelas criados com sucesso.")
