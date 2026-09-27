import argparse
import boto3


glue = boto3.client("glue")


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--bucket",
        required=True,
        help="Nome do bucket S3 do Data Lake"
    )

    parser.add_argument(
        "--database",
        default="medallion_data_lake",
        help="Nome do database no Glue Catalog"
    )

    return parser.parse_args()


def create_database(database_name):
    """
    Cria o database no Glue caso ainda não exista.
    """

    try:
        glue.get_database(
            Name=database_name
        )

        print(
            f"Database já existe: {database_name}"
        )

    except glue.exceptions.EntityNotFoundException:

        glue.create_database(
            DatabaseInput={
                "Name": database_name,
                "Description": (
                    "AWS Medallion Data Pipeline Catalog"
                )
            }
        )

        print(
            f"Database criado: {database_name}"
        )


def create_or_update_table(
    database_name,
    table_name,
    location,
    columns,
    partition_keys=None
):
    """
    Cria ou atualiza uma tabela externa
    no AWS Glue Data Catalog.
    """

    partition_keys = partition_keys or []

    table_input = {
        "Name": table_name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "EXTERNAL": "TRUE"
        },
        "StorageDescriptor": {
            "Columns": columns,
            "Location": location,
            "InputFormat": (
                "org.apache.hadoop.hive.ql.io.parquet."
                "MapredParquetInputFormat"
            ),
            "OutputFormat": (
                "org.apache.hadoop.hive.ql.io.parquet."
                "MapredParquetOutputFormat"
            ),
            "SerdeInfo": {
                "SerializationLibrary": (
                    "org.apache.hadoop.hive.ql.io.parquet."
                    "serde.ParquetHiveSerDe"
                )
            }
        },
        "PartitionKeys": partition_keys
    }

    try:
        glue.get_table(
            DatabaseName=database_name,
            Name=table_name
        )

        glue.update_table(
            DatabaseName=database_name,
            TableInput=table_input
        )

        print(
            f"Tabela atualizada: {table_name}"
        )

    except glue.exceptions.EntityNotFoundException:

        glue.create_table(
            DatabaseName=database_name,
            TableInput=table_input
        )

        print(
            f"Tabela criada: {table_name}"
        )


def register_silver_orders(
    database,
    bucket
):
    create_or_update_table(
        database_name=database,
        table_name="silver_orders",
        location=f"s3://{bucket}/silver/orders/",
        columns=[
            {
                "Name": "batch_id",
                "Type": "string"
            },
            {
                "Name": "source_system",
                "Type": "string"
            },
            {
                "Name": "ingestion_date",
                "Type": "date"
            },
            {
                "Name": "order_id",
                "Type": "bigint"
            },
            {
                "Name": "customer_id",
                "Type": "bigint"
            },
            {
                "Name": "amount",
                "Type": "decimal(18,2)"
            },
            {
                "Name": "order_status",
                "Type": "string"
            },
            {
                "Name": "order_timestamp",
                "Type": "timestamp"
            },
            {
                "Name": "silver_processed_at",
                "Type": "timestamp"
            }
        ],
        partition_keys=[
            {
                "Name": "order_date",
                "Type": "date"
            }
        ]
    )


def register_gold_tables(
    database,
    bucket
):
    create_or_update_table(
        database_name=database,
        table_name="gold_daily_sales",
        location=(
            f"s3://{bucket}/gold/orders/"
            "daily_sales/"
        ),
        columns=[
            {
                "Name": "order_date",
                "Type": "date"
            },
            {
                "Name": "total_orders",
                "Type": "bigint"
            },
            {
                "Name": "total_customers",
                "Type": "bigint"
            },
            {
                "Name": "gross_revenue",
                "Type": "decimal(28,2)"
            },
            {
                "Name": "average_ticket",
                "Type": "decimal(22,6)"
            },
            {
                "Name": "gold_processed_at",
                "Type": "timestamp"
            }
        ]
    )

    create_or_update_table(
        database_name=database,
        table_name="gold_customer_revenue",
        location=(
            f"s3://{bucket}/gold/orders/"
            "customer_revenue/"
        ),
        columns=[
            {
                "Name": "customer_id",
                "Type": "bigint"
            },
            {
                "Name": "total_orders",
                "Type": "bigint"
            },
            {
                "Name": "total_revenue",
                "Type": "decimal(28,2)"
            },
            {
                "Name": "average_order_value",
                "Type": "decimal(22,6)"
            },
            {
                "Name": "last_order_timestamp",
                "Type": "timestamp"
            },
            {
                "Name": "gold_processed_at",
                "Type": "timestamp"
            }
        ]
    )


def register_diamond_table(
    database,
    bucket
):
    create_or_update_table(
        database_name=database,
        table_name="diamond_daily_sales",
        location=(
            f"s3://{bucket}/diamond/orders/"
            "daily_sales/"
        ),
        columns=[
            {
                "Name": "order_date",
                "Type": "date"
            },
            {
                "Name": "total_orders",
                "Type": "bigint"
            },
            {
                "Name": "total_customers",
                "Type": "bigint"
            },
            {
                "Name": "gross_revenue",
                "Type": "decimal(28,2)"
            },
            {
                "Name": "average_ticket",
                "Type": "decimal(22,6)"
            },
            {
                "Name": "diamond_processed_at",
                "Type": "timestamp"
            }
        ],
        partition_keys=[
            {
                "Name": "year",
                "Type": "int"
            },
            {
                "Name": "month",
                "Type": "int"
            },
            {
                "Name": "day",
                "Type": "int"
            }
        ]
    )


def main():
    args = get_args()

    print(
        f"Configurando Glue Catalog: {args.database}"
    )

    create_database(
        args.database
    )

    register_silver_orders(
        database=args.database,
        bucket=args.bucket
    )

    register_gold_tables(
        database=args.database,
        bucket=args.bucket
    )

    register_diamond_table(
        database=args.database,
        bucket=args.bucket
    )

    print(
        "Glue Data Catalog configurado com sucesso"
    )


if __name__ == "__main__":
    main()