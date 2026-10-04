from src.utils.spark import get_spark_session
from src.ingestion.bronze_ingestion import read_bronze_csv


def main():
    spark = get_spark_session(
        app_name="dq-lakehouse-bronze"
    )

    bronze_df = read_bronze_csv(
        spark=spark,
        input_path="data/samples/transactions.csv",
        source_system="sample_csv",
        batch_id="batch_20261004_001",
    )

    print("\n=== BRONZE SCHEMA ===")
    bronze_df.printSchema()

    print("\n=== BRONZE DATA ===")
    bronze_df.show(
        truncate=False
    )

    print(
        f"\nTotal de registros Bronze: {bronze_df.count()}"
    )

    bronze_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("data/bronze/transactions")

    print(
        "\nBronze gravada com sucesso em: data/bronze/transactions"
    )

    spark.stop()


if __name__ == "__main__":
    main()
