from src.utils.spark import get_spark_session


def main():
    spark = get_spark_session(
        app_name="dq-lakehouse-smoke-test"
    )

    data = [
        ("A001", 120.50, "CLOSED"),
        ("A002", -15.00, "OPEN"),
        ("A003", 80.00, "CANCELLED"),
    ]

    columns = [
        "record_id",
        "amount",
        "status"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    print("\n=== SCHEMA ===")
    df.printSchema()

    print("\n=== DATA ===")
    df.show(truncate=False)

    print(
        f"\nTotal de registros: {df.count()}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
