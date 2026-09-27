import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="Caminho da camada Gold no S3"
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Caminho da camada Diamond no S3"
    )

    return parser.parse_args()


def create_spark_session():
    return (
        SparkSession.builder
        .appName("gold_to_diamond")
        .config(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic"
        )
        .getOrCreate()
    )


def main():
    args = get_args()

    spark = create_spark_session()

    gold_path = f"{args.input}/daily_sales"

    print(f"Lendo camada Gold: {gold_path}")

    gold_df = spark.read.parquet(gold_path)

    print(
        f"Registros encontrados na Gold: {gold_df.count()}"
    )

    diamond_df = (
        gold_df

        .withColumn(
            "year",
            F.year("order_date")
        )

        .withColumn(
            "month",
            F.month("order_date")
        )

        .withColumn(
            "day",
            F.dayofmonth("order_date")
        )

        .withColumn(
            "diamond_processed_at",
            F.current_timestamp()
        )

        .repartition(
            "year",
            "month"
        )
    )

    output_path = f"{args.output}/daily_sales"

    print(
        f"Gravando camada Diamond: {output_path}"
    )

    (
        diamond_df.write
        .mode("overwrite")
        .partitionBy(
            "year",
            "month",
            "day"
        )
        .parquet(output_path)
    )

    print("Camada Diamond criada com sucesso")

    spark.stop()


if __name__ == "__main__":
    main()