import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="Caminho da camada Landing no S3"
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Caminho da camada Raw no S3"
    )

    return parser.parse_args()


def create_spark_session():
    return (
        SparkSession.builder
        .appName("landing_to_raw")
        .getOrCreate()
    )


def main():
    args = get_args()

    spark = create_spark_session()

    print(f"Lendo Landing: {args.input}")

    landing_df = (
        spark.read
        .option("multiline", True)
        .json(args.input)
    )

    raw_df = (
        landing_df
        .withColumn(
            "raw_processed_at",
            F.current_timestamp()
        )
        .withColumn(
            "ingestion_date",
            F.to_date(
                F.col("metadata.ingestion_timestamp")
            )
        )
        .withColumn(
            "batch_id",
            F.col("metadata.batch_id")
        )
        .withColumn(
            "source_system",
            F.col("metadata.source_system")
        )
    )

    print(
        f"Registros processados: {raw_df.count()}"
    )

    (
        raw_df.write
        .mode("append")
        .partitionBy("ingestion_date")
        .parquet(args.output)
    )

    print(f"Dados gravados na Raw: {args.output}")

    spark.stop()


if __name__ == "__main__":
    main()