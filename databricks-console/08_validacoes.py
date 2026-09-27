# Databricks notebook source
# MAGIC %md
# MAGIC # StartupCRM | 08 | Validações de qualidade
# MAGIC
# MAGIC Executa validações de volume, unicidade, nulidade, integridade referencial, surrogate keys e regras dos relatórios. Os resultados são persistidos na tabela de controle.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

validation_results = []

def add_validation(
    layer_name: str,
    object_name: str,
    validation_name: str,
    success: bool,
    result_description: str,
) -> None:
    validation_results.append({
        "DAT_EXECUCAO": datetime.now(timezone.utc).replace(tzinfo=None),
        "NME_CAMADA": layer_name,
        "NME_OBJETO": object_name,
        "NME_VALIDACAO": validation_name,
        "FLG_SUCESSO": bool(success),
        "DSC_RESULTADO": result_description,
    })

def validate_exact_count(layer_name: str, object_name: str, dataframe, expected: int) -> None:
    actual = dataframe.count()
    add_validation(
        layer_name,
        object_name,
        "Quantidade exata de registros",
        actual == expected,
        f"esperado={expected}; recebido={actual}",
    )

def validate_unique(layer_name: str, object_name: str, dataframe, key_columns) -> None:
    duplicates = (
        dataframe.groupBy(*key_columns)
                 .count()
                 .filter(F.col("count") > 1)
                 .count()
    )
    add_validation(
        layer_name,
        object_name,
        f"Unicidade de {','.join(key_columns)}",
        duplicates == 0,
        f"grupos_duplicados={duplicates}",
    )

def validate_no_nulls(layer_name: str, object_name: str, dataframe, column_names) -> None:
    conditions = [F.col(column_name).isNull() for column_name in column_names]
    combined_condition = conditions[0]
    for condition in conditions[1:]:
        combined_condition = combined_condition | condition

    null_rows = dataframe.filter(combined_condition).count()
    add_validation(
        layer_name,
        object_name,
        "Ausência de nulos nas colunas padronizadas",
        null_rows == 0,
        f"linhas_com_nulo={null_rows}",
    )

def validate_foreign_key(
    layer_name: str,
    object_name: str,
    child_dataframe,
    parent_dataframe,
    foreign_key: str,
    parent_key: str,
) -> None:
    orphan_count = (
        child_dataframe.select(foreign_key).distinct()
        .join(
            parent_dataframe.select(F.col(parent_key).alias(foreign_key)).distinct(),
            on=foreign_key,
            how="left_anti",
        )
        .count()
    )
    add_validation(
        layer_name,
        object_name,
        f"Integridade referencial de {foreign_key}",
        orphan_count == 0,
        f"chaves_orfas={orphan_count}",
    )

# COMMAND ----------

for endpoint, table_key in [
    ("users", "landing_users"),
    ("posts", "landing_posts"),
    ("todos", "landing_todos"),
]:
    landing_df = spark.table(TABLES[table_key])
    latest_timestamp = landing_df.agg(F.max("data_atualizacao").alias("max_ts")).first()["max_ts"]
    latest_snapshot = landing_df.filter(F.col("data_atualizacao") == F.lit(latest_timestamp))
    validate_exact_count("LANDING", table_key, latest_snapshot, EXPECTED_COUNTS[endpoint])
    validate_no_nulls(
        "LANDING",
        table_key,
        latest_snapshot,
        ["data_insercao", "data_atualizacao"],
    )

# COMMAND ----------

bronze_users = spark.table(TABLES["bronze_users"])
bronze_posts = spark.table(TABLES["bronze_posts"])
bronze_todos = spark.table(TABLES["bronze_todos"])

validate_exact_count("BRONZE", "users", bronze_users, EXPECTED_COUNTS["users"])
validate_exact_count("BRONZE", "posts", bronze_posts, EXPECTED_COUNTS["posts"])
validate_exact_count("BRONZE", "todos", bronze_todos, EXPECTED_COUNTS["todos"])

validate_unique("BRONZE", "users", bronze_users, ["id"])
validate_unique("BRONZE", "posts", bronze_posts, ["id"])
validate_unique("BRONZE", "todos", bronze_todos, ["id"])

# COMMAND ----------

dim_usuario = spark.table(TABLES["silver_dim_usuario"])
fato_post = spark.table(TABLES["silver_fato_post"])
fato_todo = spark.table(TABLES["silver_fato_todo"])

validate_exact_count(
    "SILVER",
    "dim_usuario",
    dim_usuario,
    EXPECTED_COUNTS["users"] + 1,
)
validate_exact_count("SILVER", "fato_post", fato_post, EXPECTED_COUNTS["posts"])
validate_exact_count("SILVER", "fato_todo", fato_todo, EXPECTED_COUNTS["todos"])

validate_unique("SILVER", "dim_usuario", dim_usuario, ["SRK_USR"])
validate_unique("SILVER", "fato_post", fato_post, ["SRK_POST"])
validate_unique("SILVER", "fato_todo", fato_todo, ["SRK_TODO"])

validate_no_nulls("SILVER", "dim_usuario", dim_usuario, dim_usuario.columns)
validate_no_nulls("SILVER", "fato_post", fato_post, fato_post.columns)
validate_no_nulls("SILVER", "fato_todo", fato_todo, fato_todo.columns)

validate_foreign_key(
    "SILVER",
    "fato_post",
    fato_post,
    dim_usuario,
    "SRK_USR",
    "SRK_USR",
)
validate_foreign_key(
    "SILVER",
    "fato_todo",
    fato_todo,
    dim_usuario,
    "SRK_USR",
    "SRK_USR",
)

# COMMAND ----------

invalid_dim_srk = (
    dim_usuario.filter(
        F.col("SRK_USR") != surrogate_key("USR", F.col("COD_USR"))
    ).count()
)
add_validation(
    "SILVER",
    "dim_usuario",
    "Surrogate key determinística de usuário",
    invalid_dim_srk == 0,
    f"chaves_invalidas={invalid_dim_srk}",
)

invalid_post_srk = (
    fato_post.filter(
        F.col("SRK_POST") != surrogate_key("POST", F.col("COD_POST"))
    ).count()
)
add_validation(
    "SILVER",
    "fato_post",
    "Surrogate key determinística de post",
    invalid_post_srk == 0,
    f"chaves_invalidas={invalid_post_srk}",
)

invalid_todo_srk = (
    fato_todo.filter(
        F.col("SRK_TODO") != surrogate_key("TODO", F.col("COD_TODO"))
    ).count()
)
add_validation(
    "SILVER",
    "fato_todo",
    "Surrogate key determinística de tarefa",
    invalid_todo_srk == 0,
    f"chaves_invalidas={invalid_todo_srk}",
)

# COMMAND ----------

gold_usuario = spark.table(TABLES["gold_relatorio_usuario"])
gold_cidade = spark.table(TABLES["gold_relatorio_cidade"])

validate_exact_count(
    "GOLD",
    "vw_usuario_posts_tarefas",
    gold_usuario,
    EXPECTED_COUNTS["users"],
)

gold_totals = gold_usuario.agg(
    F.sum("NUM_QTD_POST").alias("posts"),
    F.sum("NUM_QTD_TODO").alias("todos"),
    F.sum("NUM_QTD_TODO_CONCLUIDO").alias("todos_concluidos"),
).first()

add_validation(
    "GOLD",
    "vw_usuario_posts_tarefas",
    "Total de posts do relatório",
    gold_totals["posts"] == EXPECTED_COUNTS["posts"],
    f"esperado={EXPECTED_COUNTS['posts']}; recebido={gold_totals['posts']}",
)
add_validation(
    "GOLD",
    "vw_usuario_posts_tarefas",
    "Total de tarefas do relatório",
    gold_totals["todos"] == EXPECTED_COUNTS["todos"],
    f"esperado={EXPECTED_COUNTS['todos']}; recebido={gold_totals['todos']}",
)

invalid_percentage_count = (
    gold_cidade.filter(
        (F.col("NUM_PCT_TODO_CONCLUIDO") < 0)
        | (F.col("NUM_PCT_TODO_CONCLUIDO") > 100)
        | F.col("NUM_PCT_TODO_CONCLUIDO").isNull()
    ).count()
)
add_validation(
    "GOLD",
    "vw_cidade_conclusao_tarefas",
    "Percentual de conclusão entre 0 e 100",
    invalid_percentage_count == 0,
    f"linhas_invalidas={invalid_percentage_count}",
)

city_totals = gold_cidade.agg(
    F.sum("NUM_QTD_TODO").alias("todos"),
    F.sum("NUM_QTD_TODO_CONCLUIDO").alias("todos_concluidos"),
).first()
add_validation(
    "GOLD",
    "vw_cidade_conclusao_tarefas",
    "Total de tarefas por cidade",
    city_totals["todos"] == EXPECTED_COUNTS["todos"],
    f"esperado={EXPECTED_COUNTS['todos']}; recebido={city_totals['todos']}",
)

# COMMAND ----------

validation_schema = T.StructType([
    T.StructField("DAT_EXECUCAO", T.TimestampType(), False),
    T.StructField("NME_CAMADA", T.StringType(), False),
    T.StructField("NME_OBJETO", T.StringType(), False),
    T.StructField("NME_VALIDACAO", T.StringType(), False),
    T.StructField("FLG_SUCESSO", T.BooleanType(), False),
    T.StructField("DSC_RESULTADO", T.StringType(), False),
])

validation_df = spark.createDataFrame(validation_results, schema=validation_schema)
append_delta(validation_df, TABLES["gold_validacoes"])

display(
    validation_df.orderBy(
        F.col("FLG_SUCESSO").asc(),
        F.col("NME_CAMADA"),
        F.col("NME_OBJETO"),
    )
)

failures = [result for result in validation_results if not result["FLG_SUCESSO"]]
if failures:
    failure_names = [
        f"{item['NME_CAMADA']}.{item['NME_OBJETO']}: {item['NME_VALIDACAO']}"
        for item in failures
    ]
    raise AssertionError(
        "Pipeline reprovada nas validações: " + " | ".join(failure_names)
    )

print(f"Todas as {len(validation_results)} validações foram aprovadas.")
