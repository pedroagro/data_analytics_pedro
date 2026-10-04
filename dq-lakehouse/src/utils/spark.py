import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")

hadoop_bin = os.path.join(
    os.environ["HADOOP_HOME"],
    "bin"
)

if hadoop_bin not in os.environ.get("PATH", ""):
    os.environ["PATH"] = (
        hadoop_bin
        + os.pathsep
        + os.environ.get("PATH", "")
    )

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "dq-lakehouse",
    master: str = "local[*]"
) -> SparkSession:

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config(
            "spark.sql.session.timeZone",
            "UTC"
        )
        .config(
            "spark.sql.adaptive.enabled",
            "true"
        )
        .config(
            "spark.sql.shuffle.partitions",
            "4"
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark = configure_spark_with_delta_pip(
        builder
    ).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark
