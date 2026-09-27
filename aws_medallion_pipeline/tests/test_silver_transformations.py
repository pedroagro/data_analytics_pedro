import os
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.jobs.bronze_to_silver import transform_bronze_to_silver


PYTHON_EXECUTABLE = r"C:\venvs\aws-medallion\Scripts\python.exe"

os.environ["PYSPARK_PYTHON"] = PYTHON_EXECUTABLE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXECUTABLE


BRONZE_SCHEMA = StructType(
    [
        StructField("order_id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField(
            "amount",
            DecimalType(18, 2),
            True
        ),
        StructField(
            "order_status",
            StringType(),
            True
        ),
        StructField(
            "order_timestamp",
            TimestampType(),
            True
        ),
    ]
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test_bronze_to_silver")
        .config(
            "spark.pyspark.python",
            PYTHON_EXECUTABLE
        )
        .config(
            "spark.pyspark.driver.python",
            PYTHON_EXECUTABLE
        )
        .config(
            "spark.ui.enabled",
            "false"
        )
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


def create_bronze_df(spark, data):
    return spark.createDataFrame(
        data=data,
        schema=BRONZE_SCHEMA
    )


def test_bronze_to_silver_business_rules(spark):
    data = [
        (
            1,
            100,
            Decimal("100.00"),
            "PAID",
            datetime(2026, 9, 27, 10, 0, 0)
        ),
        (
            1,
            100,
            Decimal("120.00"),
            "APPROVED",
            datetime(2026, 9, 27, 11, 0, 0)
        ),
        (
            2,
            200,
            Decimal("-50.00"),
            "PAID",
            datetime(2026, 9, 27, 12, 0, 0)
        ),
        (
            3,
            None,
            Decimal("300.00"),
            "PAID",
            datetime(2026, 9, 27, 13, 0, 0)
        ),
        (
            4,
            400,
            Decimal("500.00"),
            " cancelled ",
            datetime(2026, 9, 27, 14, 0, 0)
        ),
    ]

    bronze_df = create_bronze_df(
        spark,
        data
    )

    result_df = transform_bronze_to_silver(
        bronze_df
    )

    result = {
        row["order_id"]: row
        for row in result_df.collect()
    }

    assert len(result) == 2

    assert 1 in result
    assert 4 in result

    assert result[1]["amount"] == Decimal(
        "120.00"
    )

    assert (
        result[1]["order_status"]
        == "COMPLETED"
    )

    assert (
        result[4]["order_status"]
        == "CANCELLED"
    )

    assert result[1]["order_date"] is not None

    assert result[1]["year"] == 2026

    assert result[1]["month"] == 9


def test_negative_amount_is_rejected(spark):
    data = [
        (
            10,
            500,
            Decimal("-10.00"),
            "PAID",
            datetime(2026, 9, 27, 10, 0, 0)
        )
    ]

    bronze_df = create_bronze_df(
        spark,
        data
    )

    result_df = transform_bronze_to_silver(
        bronze_df
    )

    assert result_df.count() == 0


def test_null_customer_is_rejected(spark):
    data = [
        (
            20,
            None,
            Decimal("50.00"),
            "PAID",
            datetime(2026, 9, 27, 10, 0, 0)
        )
    ]

    bronze_df = create_bronze_df(
        spark,
        data
    )

    result_df = transform_bronze_to_silver(
        bronze_df
    )

    assert result_df.count() == 0