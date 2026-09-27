# Databricks notebook source
# MAGIC %md
# MAGIC # StartupCRM | Configurações e utilitários
# MAGIC
# MAGIC Este notebook centraliza configurações, nomes de objetos, tratamento de nulos, geração determinística de surrogate keys e funções compartilhadas.

# COMMAND ----------

from typing import Dict, Iterable, List, Optional, Sequence
from pyspark.sql import DataFrame, Column, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

API_BASE_URL = "https://jsonplaceholder.typicode.com"
PAGE_SIZE = 25
REQUEST_TIMEOUT_SECONDS = 30
MAX_PAGES = 1000

EXPECTED_COUNTS: Dict[str, int] = {
    "users": 10,
    "posts": 100,
    "todos": 200,
}

SCHEMAS: Dict[str, str] = {
    "landing": "startupcrm_landing",
    "bronze": "startupcrm_bronze",
    "silver": "startupcrm_silver",
    "gold": "startupcrm_gold",
}

CURRENT_CATALOG = spark.sql("SELECT current_catalog() AS catalogo").first()["catalogo"]
USE_UNITY_CATALOG = CURRENT_CATALOG not in {"spark_catalog", "hive_metastore"}

def quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"

def namespace_name(schema_name: str) -> str:
    if USE_UNITY_CATALOG:
        return f"{quote_identifier(CURRENT_CATALOG)}.{quote_identifier(schema_name)}"
    return quote_identifier(schema_name)

def table_name(schema_name: str, object_name: str) -> str:
    return f"{namespace_name(schema_name)}.{quote_identifier(object_name)}"

TABLES: Dict[str, str] = {
    "landing_users": table_name(SCHEMAS["landing"], "users"),
    "landing_posts": table_name(SCHEMAS["landing"], "posts"),
    "landing_todos": table_name(SCHEMAS["landing"], "todos"),
    "bronze_users": table_name(SCHEMAS["bronze"], "users"),
    "bronze_posts": table_name(SCHEMAS["bronze"], "posts"),
    "bronze_todos": table_name(SCHEMAS["bronze"], "todos"),
    "silver_dim_usuario": table_name(SCHEMAS["silver"], "dim_usuario"),
    "silver_fato_post": table_name(SCHEMAS["silver"], "fato_post"),
    "silver_fato_todo": table_name(SCHEMAS["silver"], "fato_todo"),
    "gold_relatorio_usuario": table_name(SCHEMAS["gold"], "vw_usuario_posts_tarefas"),
    "gold_relatorio_cidade": table_name(SCHEMAS["gold"], "vw_cidade_conclusao_tarefas"),
    "gold_validacoes": table_name(SCHEMAS["gold"], "ctl_validacao_pipeline"),
}

def create_schema_if_not_exists(schema_name: str) -> None:
    object_type = "SCHEMA" if USE_UNITY_CATALOG else "DATABASE"
    spark.sql(f"CREATE {object_type} IF NOT EXISTS {namespace_name(schema_name)}")

def table_exists(full_name: str) -> bool:
    try:
        return spark.catalog.tableExists(full_name.replace("`", ""))
    except Exception:
        try:
            spark.table(full_name).limit(1)
            return True
        except Exception:
            return False

def assert_required_columns(df: DataFrame, required_columns: Sequence[str], object_name: str) -> None:
    missing = sorted(set(required_columns) - set(df.columns))
    if missing:
        raise ValueError(f"{object_name}: colunas obrigatórias ausentes: {missing}")

def assert_not_empty(df: DataFrame, object_name: str) -> None:
    if df.limit(1).count() == 0:
        raise ValueError(f"{object_name}: DataFrame vazio")

def latest_by_key(
    df: DataFrame,
    key_columns: Sequence[str],
    order_columns: Sequence[str] = ("data_atualizacao", "data_insercao"),
) -> DataFrame:
    ordering = [F.col(column_name).desc_nulls_last() for column_name in order_columns]
    window = Window.partitionBy(*[F.col(column_name) for column_name in key_columns]).orderBy(*ordering)
    return (
        df.withColumn("_row_number", F.row_number().over(window))
          .filter(F.col("_row_number") == 1)
          .drop("_row_number")
    )

def normalize_text(column: Column) -> Column:
    value = F.trim(column.cast("string"))
    return F.when(column.isNull() | (value == ""), F.lit("N/I")).otherwise(value)

def normalize_integer(column: Column) -> Column:
    return F.coalesce(column.cast("bigint"), F.lit(-1).cast("bigint"))

def normalize_decimal(column: Column, precision: int = 18, scale: int = 6) -> Column:
    decimal_type = T.DecimalType(precision, scale)
    return F.coalesce(column.cast(decimal_type), F.lit(-1).cast(decimal_type))

def normalize_boolean(column: Column) -> Column:
    return F.coalesce(column.cast("boolean"), F.lit(False))

def surrogate_key(prefix: str, *columns: Column) -> Column:
    normalized_columns = [
        F.coalesce(F.trim(column.cast("string")), F.lit("-1"))
        for column in columns
    ]
    return F.sha2(
        F.concat_ws("||", F.lit(prefix.upper().strip()), *normalized_columns),
        256,
    )

def overwrite_delta(df: DataFrame, target_table: str) -> None:
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(target_table)
    )

def append_delta(df: DataFrame, target_table: str) -> None:
    (
        df.write
          .format("delta")
          .mode("append")
          .saveAsTable(target_table)
    )

def print_environment() -> None:
    print(f"Catálogo atual: {CURRENT_CATALOG}")
    print(f"Unity Catalog ativo: {USE_UNITY_CATALOG}")
    print(f"API: {API_BASE_URL}")
    print("Objetos:")
    for key, value in TABLES.items():
        print(f"  {key}: {value}")

print_environment()
