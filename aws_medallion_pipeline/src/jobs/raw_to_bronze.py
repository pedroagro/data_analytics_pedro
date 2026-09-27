import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="Caminho da camada Raw no S3"
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Caminho da camada Bronze no S3"
    )

    return parser.parse_args()


def create_spark_session():
    return (
        SparkSession.builder
        .appName("raw_to_bronze")
        .config(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic"
        )
        .getOrCreate()
    )


def main():
    args = get_args()

    spark = create_spark_session()

    print(f"Lendo camada Raw: {args.input}")

    raw_df = spark.read.parquet(args.input)

    print(
        f"Registros encontrados na Raw: {raw_df.count()}"
    )

    bronze_df = (
        raw_df

        # Cada item retornado pela API passa a representar
        # um registro individual
        .withColumn(
            "record",
            F.explode_outer("data")
        )

        # Seleciona metadados técnicos e dados de negócio
        .select(
            F.col("batch_id"),
            F.col("source_system"),
            F.col("ingestion_date"),
            F.col("raw_processed_at"),

            F.col("record.id")
            .cast("long")
            .alias("order_id"),

            F.col("record.customer_id")
            .cast("long")
            .alias("customer_id"),

            F.col("record.amount")
            .cast("decimal(18,2)")
            .alias("amount"),

            F.upper(
                F.trim(
                    F.col("record.order_status")
                )
            ).alias("order_status"),

            F.to_timestamp(
                F.col("record.order_timestamp")
            ).alias("order_timestamp")
        )

        # Validação técnica mínima
        .filter(
            F.col("order_id").isNotNull()
        )

        # Auditoria da camada
        .withColumn(
            "bronze_processed_at",
            F.current_timestamp()
        )
    )

    total_bronze = bronze_df.count()

    print(
        f"Registros válidos para Bronze: {total_bronze}"
    )

    (
        bronze_df.write
        .mode("append")
        .partitionBy("ingestion_date")
        .parquet(args.output)
    )

    print(
        f"Dados gravados na Bronze: {args.output}"
    )

    spark.stop()


if __name__ == "__main__":
    main()