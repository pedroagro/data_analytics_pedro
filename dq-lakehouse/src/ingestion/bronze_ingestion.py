from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    DateType,
)


BRONZE_SCHEMA = StructType(
    [
        StructField("record_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("event_date", DateType(), True),
    ]
)


def read_bronze_csv(
    spark: SparkSession,
    input_path: str,
    source_system: str,
    batch_id: str,
) -> DataFrame:
    df = (
        spark.read
        .option("header", True)
        .schema(BRONZE_SCHEMA)
        .csv(input_path)
    )

    return (
        df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_system", F.lit(source_system))
        .withColumn("source_file", F.input_file_name())
    )
