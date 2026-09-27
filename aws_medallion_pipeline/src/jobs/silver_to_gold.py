import argparse

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
        help="Caminho da camada Gold no S3"
    )

    return parser.parse_args()


def create_spark_session():
    return (
        SparkSession.builder
        .appName("silver_to_gold")
        .getOrCreate()
    )


def create_daily_sales(df):
    """
    Visão diária de vendas.
    """

    return (
        df
        .filter(F.col("order_status") == "COMPLETED")
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("total_customers"),
            F.sum("amount").alias("gross_revenue"),
            F.avg("amount").alias("average_ticket")
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp()
        )
    )


def create_customer_revenue(df):
    """
    Visão consolidada de receita por cliente.
    """

    return (
        df
        .filter(F.col("order_status") == "COMPLETED")
        .groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("average_order_value"),
            F.max("order_timestamp").alias("last_order_timestamp")
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp()
        )
    )


def create_sales_summary(df):
    """
    Visão mensal consolidada.
    """

    return (
        df
        .filter(F.col("order_status") == "COMPLETED")
        .groupBy(
            F.year("order_date").alias("year"),
            F.month("order_date").alias("month")
        )
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("amount").alias("gross_revenue"),
            F.avg("amount").alias("average_ticket")
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp()
        )
    )


def main():
    args = get_args()

    spark = create_spark_session()

    print(f"Lendo camada Silver: {args.input}")

    silver_df = spark.read.parquet(args.input)

    print(
        f"Registros disponíveis na Silver: {silver_df.count()}"
    )

    daily_sales = create_daily_sales(silver_df)

    customer_revenue = create_customer_revenue(silver_df)

    sales_summary = create_sales_summary(silver_df)

    print("Criando visão Gold: daily_sales")

    (
        daily_sales.write
        .mode("overwrite")
        .parquet(
            f"{args.output}/daily_sales"
        )
    )

    print("Criando visão Gold: customer_revenue")

    (
        customer_revenue.write
        .mode("overwrite")
        .parquet(
            f"{args.output}/customer_revenue"
        )
    )

    print("Criando visão Gold: sales_summary")

    (
        sales_summary.write
        .mode("overwrite")
        .parquet(
            f"{args.output}/sales_summary"
        )
    )

    print("Camada Gold criada com sucesso")

    spark.stop()


if __name__ == "__main__":
    main()