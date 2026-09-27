import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="Caminho da camada Silver no S3"
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Caminho onde a auditoria será gravada"
    )

    return parser.parse_args()


def create_spark_session():
    return (
        SparkSession.builder
        .appName("data_quality_checks")
        .getOrCreate()
    )


def validate_required_columns(df):
    required_columns = [
        "order_id",
        "customer_id",
        "amount",
        "order_status",
        "order_timestamp"
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    return missing_columns


def count_nulls(df):
    return {
        "order_id": df.filter(
            F.col("order_id").isNull()
        ).count(),

        "customer_id": df.filter(
            F.col("customer_id").isNull()
        ).count(),

        "amount": df.filter(
            F.col("amount").isNull()
        ).count(),

        "order_timestamp": df.filter(
            F.col("order_timestamp").isNull()
        ).count()
    }


def count_duplicates(df):
    return (
        df
        .groupBy("order_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )


def count_invalid_amounts(df):
    return (
        df
        .filter(
            F.col("amount") < 0
        )
        .count()
    )


def count_invalid_status(df):
    valid_status = [
        "COMPLETED",
        "CANCELLED",
        "PENDING"
    ]

    return (
        df
        .filter(
            ~F.col("order_status").isin(valid_status)
        )
        .count()
    )


def define_quality_status(
    total_rows,
    missing_columns,
    nulls,
    duplicates,
    invalid_amounts,
    invalid_status
):
    if total_rows == 0:
        return "FAILED"

    if missing_columns:
        return "FAILED"

    if duplicates > 0:
        return "WARNING"

    if invalid_amounts > 0:
        return "WARNING"

    if invalid_status > 0:
        return "WARNING"

    if any(value > 0 for value in nulls.values()):
        return "WARNING"

    return "SUCCESS"


def main():
    args = get_args()

    spark = create_spark_session()

    print(
        f"Lendo dataset para validação: {args.input}"
    )

    df = spark.read.parquet(
        args.input
    )

    total_rows = df.count()

    missing_columns = validate_required_columns(
        df
    )

    if missing_columns:
        quality_result = {
            "status": "FAILED",
            "total_rows": total_rows,
            "missing_columns": missing_columns
        }

    else:
        nulls = count_nulls(df)

        duplicates = count_duplicates(df)

        invalid_amounts = count_invalid_amounts(
            df
        )

        invalid_status = count_invalid_status(
            df
        )

        status = define_quality_status(
            total_rows=total_rows,
            missing_columns=missing_columns,
            nulls=nulls,
            duplicates=duplicates,
            invalid_amounts=invalid_amounts,
            invalid_status=invalid_status
        )

        quality_result = {
            "status": status,
            "total_rows": total_rows,
            "null_count": nulls,
            "duplicate_count": duplicates,
            "invalid_amount_count": invalid_amounts,
            "invalid_status_count": invalid_status
        }

    print(
        json.dumps(
            quality_result,
            indent=2,
            ensure_ascii=False
        )
    )

    audit_df = spark.createDataFrame(
        [
            {
                "quality_status": quality_result["status"],
                "total_rows": total_rows,
                "duplicate_count": quality_result.get(
                    "duplicate_count",
                    0
                ),
                "invalid_amount_count": quality_result.get(
                    "invalid_amount_count",
                    0
                ),
                "invalid_status_count": quality_result.get(
                    "invalid_status_count",
                    0
                )
            }
        ]
    )

    audit_df = (
        audit_df
        .withColumn(
            "quality_checked_at",
            F.current_timestamp()
        )
    )

    (
        audit_df.write
        .mode("append")
        .parquet(args.output)
    )

    print(
        f"Auditoria gravada em: {args.output}"
    )

    if quality_result["status"] == "FAILED":
        raise RuntimeError(
            "Pipeline interrompido por falha de qualidade"
        )

    spark.stop()


if __name__ == "__main__":
    main()