import argparse

from pyspark.sql import DataFrame
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


def transform_bronze_to_silver(
    bronze_df: DataFrame
) -> DataFrame:
    """
    Aplica as regras de negócio da camada Silver.

    Responsabilidades:
    1. Remover registros inválidos
    2. Eliminar duplicidades
    3. Padronizar status
    4. Criar atributos analíticos
    5. Adicionar auditoria
    """

    dedup_window = (
        Window
        .partitionBy("order_id")
        .orderBy(
            F.col("order_timestamp").desc_nulls_last()
        )
    )

    silver_df = (
        bronze_df

        .filter(
            F.col("amount").isNotNull()
        )

        .filter(
            F.col("amount") >= 0
        )

        .filter(
            F.col("customer_id").isNotNull()
        )

        .filter(
            F.col("order_id").isNotNull()
        )

        .withColumn(
            "row_number",
            F.row_number().over(dedup_window)
        )

        .filter(
            F.col("row_number") == 1
        )

        .drop("row_number")

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
                F.upper(
                    F.trim(
                        F.col("order_status")
                    )
                )
            )
        )

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

        .withColumn(
            "silver_processed_at",
            F.current_timestamp()
        )
    )

    return silver_df


def main():
    args = get_args()

    spark = create_spark_session()

    print(
        f"Lendo camada Bronze: {args.input}"
    )

    bronze_df = spark.read.parquet(
        args.input
    )

    total_bronze = bronze_df.count()

    print(
        f"Registros encontrados na Bronze: "
        f"{total_bronze}"
    )

    silver_df = transform_bronze_to_silver(
        bronze_df
    )

    total_silver = silver_df.count()

    print(
        f"Registros válidos na Silver: "
        f"{total_silver}"
    )

    (
        silver_df.write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet(args.output)
    )

    print(
        f"Dados gravados na Silver: "
        f"{args.output}"
    )

    spark.stop()


if __name__ == "__main__":
    main()