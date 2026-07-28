# Databricks notebook source
# MAGIC %md
# MAGIC # StartupCRM | 02 | Ingestão Landing
# MAGIC
# MAGIC Extrai todas as páginas da API até receber uma lista vazia, preserva a estrutura original e adiciona as colunas obrigatórias de auditoria.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

import hashlib
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

users_schema = T.StructType([
    T.StructField("id", T.LongType(), True),
    T.StructField("name", T.StringType(), True),
    T.StructField("username", T.StringType(), True),
    T.StructField("email", T.StringType(), True),
    T.StructField(
        "address",
        T.StructType([
            T.StructField("street", T.StringType(), True),
            T.StructField("suite", T.StringType(), True),
            T.StructField("city", T.StringType(), True),
            T.StructField("zipcode", T.StringType(), True),
            T.StructField(
                "geo",
                T.StructType([
                    T.StructField("lat", T.StringType(), True),
                    T.StructField("lng", T.StringType(), True),
                ]),
                True,
            ),
        ]),
        True,
    ),
    T.StructField("phone", T.StringType(), True),
    T.StructField("website", T.StringType(), True),
    T.StructField(
        "company",
        T.StructType([
            T.StructField("name", T.StringType(), True),
            T.StructField("catchPhrase", T.StringType(), True),
            T.StructField("bs", T.StringType(), True),
        ]),
        True,
    ),
])

posts_schema = T.StructType([
    T.StructField("userId", T.LongType(), True),
    T.StructField("id", T.LongType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("body", T.StringType(), True),
])

todos_schema = T.StructType([
    T.StructField("userId", T.LongType(), True),
    T.StructField("id", T.LongType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("completed", T.BooleanType(), True),
])

# COMMAND ----------

def build_http_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.headers.update({"Accept": "application/json"})
    return session

def page_fingerprint(payload: list) -> str:
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

def fetch_all_pages(session: requests.Session, endpoint: str) -> list:
    records = []
    seen_page_fingerprints = set()
    page = 1

    while True:
        if page > MAX_PAGES:
            raise RuntimeError(
                f"{endpoint}: limite de segurança de {MAX_PAGES} páginas excedido"
            )

        try:
            response = session.get(
                f"{API_BASE_URL}/{endpoint}",
                params={"_page": page, "_limit": PAGE_SIZE},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            raise RuntimeError(
                f"{endpoint}: falha ao acessar a API. Verifique a conexão de saída do compute "
                "e as permissões de acesso à internet do workspace"
            ) from exc
        except ValueError as exc:
            raise RuntimeError(
                f"{endpoint}: a API respondeu com conteúdo que não é JSON válido"
            ) from exc

        if not isinstance(payload, list):
            raise TypeError(
                f"{endpoint}: resposta inválida. Esperada lista, recebido {type(payload).__name__}"
            )

        if payload == []:
            print(f"{endpoint}: paginação encerrada na página {page}")
            break

        fingerprint = page_fingerprint(payload)
        if fingerprint in seen_page_fingerprints:
            raise RuntimeError(
                f"{endpoint}: a API repetiu uma página. A paginação pode não estar sendo respeitada"
            )

        seen_page_fingerprints.add(fingerprint)
        records.extend(payload)
        print(f"{endpoint}: página {page}, registros recebidos: {len(payload)}")
        page += 1

    return records

def ingest_entity(
    session: requests.Session,
    endpoint: str,
    schema: T.StructType,
    target_table: str,
) -> None:
    payload = fetch_all_pages(session, endpoint)
    expected_count = EXPECTED_COUNTS[endpoint]

    if len(payload) != expected_count:
        raise ValueError(
            f"{endpoint}: quantidade inesperada. Esperado {expected_count}, recebido {len(payload)}"
        )

    source_columns = [field.name for field in schema.fields]
    unexpected_columns = sorted(
        {
            key
            for record in payload
            for key in record.keys()
            if key not in source_columns
        }
    )
    if unexpected_columns:
        raise ValueError(
            f"{endpoint}: novas colunas detectadas na API: {unexpected_columns}. Atualize o schema"
        )

    df = spark.createDataFrame(payload, schema=schema)
    audit_timestamp = F.current_timestamp()
    landing_df = (
        df.withColumn("data_insercao", audit_timestamp)
          .withColumn("data_atualizacao", audit_timestamp)
    )

    assert_required_columns(
        landing_df,
        source_columns + ["data_insercao", "data_atualizacao"],
        endpoint,
    )
    assert_not_empty(landing_df, endpoint)

    append_delta(landing_df, target_table)
    print(f"{endpoint}: {len(payload)} registros adicionados em {target_table}")

# COMMAND ----------

http_session = build_http_session()

try:
    ingest_entity(http_session, "users", users_schema, TABLES["landing_users"])
    ingest_entity(http_session, "posts", posts_schema, TABLES["landing_posts"])
    ingest_entity(http_session, "todos", todos_schema, TABLES["landing_todos"])
finally:
    http_session.close()

print("Ingestão Landing concluída com sucesso.")
