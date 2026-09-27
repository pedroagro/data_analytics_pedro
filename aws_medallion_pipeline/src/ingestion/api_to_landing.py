import json
import os
import uuid
from datetime import datetime, timezone
from urllib.request import Request, urlopen

import boto3


s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")


def get_api_token(secret_name: str) -> str:
    """
    Recupera o token da API armazenado no AWS Secrets Manager.
    """

    response = secrets_client.get_secret_value(
        SecretId=secret_name
    )

    secret = json.loads(response["SecretString"])

    return secret["token"]


def request_api(api_url: str, token: str) -> dict:
    """
    Executa uma chamada HTTP GET para a API.
    """

    request = Request(
        api_url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "aws-medallion-data-pipeline"
        },
        method="GET"
    )

    with urlopen(request, timeout=30) as response:
        payload = response.read().decode("utf-8")

    return json.loads(payload)


def build_s3_key(
    prefix: str,
    ingestion_timestamp: datetime,
    batch_id: str
) -> str:
    """
    Cria uma chave particionada para a camada Landing.
    """

    return (
        f"{prefix}/"
        f"ingestion_date={ingestion_timestamp:%Y-%m-%d}/"
        f"hour={ingestion_timestamp:%H}/"
        f"{batch_id}.json"
    )


def lambda_handler(event, context):
    """
    Lambda responsável pela ingestão da API
    e persistência do payload na camada Landing.
    """

    api_url = os.environ["API_URL"]
    bucket = os.environ["DATA_LAKE_BUCKET"]
    secret_name = os.environ["API_SECRET_NAME"]

    landing_prefix = os.environ.get(
        "LANDING_PREFIX",
        "landing/orders"
    )

    ingestion_timestamp = datetime.now(timezone.utc)

    batch_id = str(uuid.uuid4())

    token = get_api_token(secret_name)

    api_payload = request_api(
        api_url=api_url,
        token=token
    )

    landing_payload = {
        "metadata": {
            "batch_id": batch_id,
            "source_system": "orders_api",
            "ingestion_timestamp": ingestion_timestamp.isoformat()
        },
        "data": api_payload
    }

    s3_key = build_s3_key(
        prefix=landing_prefix,
        ingestion_timestamp=ingestion_timestamp,
        batch_id=batch_id
    )

    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json.dumps(
            landing_payload,
            ensure_ascii=False
        ).encode("utf-8"),
        ContentType="application/json"
    )

    return {
        "statusCode": 200,
        "batch_id": batch_id,
        "bucket": bucket,
        "key": s3_key
    }