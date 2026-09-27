import argparse

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="Caminho da camada Bronze no S3"
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Caminho da camada Silver no S3"
    )

    return parser.parse_args()


def create_spark_session():
    return (
        SparkSession.builder
        .appName("bronze_to_silver")
        .config(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic"
        )
        .getOrCreate()
    )


def main():
    args = get_args()

    spark = create_spark_session()

    print(f"Lendo camada Bronze: {args.input}")

    bronze_df = spark.read.parquet(args.input)

    print(
        f"Registros encontrados na Bronze: {bronze_df.count()}"
    )

    dedup_window = (
        Window
        .partitionBy("order_id")
        .orderBy(
            F.col("order_timestamp").desc_nulls_last()
        )
    )

    silver_df = (
        bronze_df

        # Regra de negócio
        .filter(
            F.col("amount").isNotNull()
        )

        .filter(
            F.col("amount") >= 0
        )

        .filter(
            F.col("customer_id").isNotNull()
        )

        # Deduplicação
        .withColumn(
            "row_number",
            F.row_number().over(dedup_window)
        )

        .filter(
            F.col("row_number") == 1
        )

        .drop("row_number")

        # Padronização de status
        .withColumn(
            "order_status",
            F.when(
                F.col("order_status").isin(
                    "PAID",
                    "APPROVED"
                ),
                "COMPLETED"
            )
            .when(
                F.col("order_status").isin(
                    "CANCELLED",
                    "CANCELED"
                ),
                "CANCELLED"
            )
            .otherwise(
                F.col("order_status")
            )
        )

        # Colunas analíticas
        .withColumn(
            "order_date",
            F.to_date("order_timestamp")
        )

        .withColumn(
            "year",
            F.year("order_timestamp")
        )

        .withColumn(
            "month",
            F.month("order_timestamp")
        )

        # Auditoria
        .withColumn(
            "silver_processed_at",
            F.current_timestamp()
        )
    )

    total_silver = silver_df.count()

    print(
        f"Registros válidos na Silver: {total_silver}"
    )

    (
        silver_df.write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet(args.output)
    )

    print(
        f"Dados gravados na Silver: {args.output}"
    )

    spark.stop()


if __name__ == "__main__":
    main()