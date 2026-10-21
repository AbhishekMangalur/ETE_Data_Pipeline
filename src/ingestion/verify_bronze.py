
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_PATH = PROJECT_ROOT / "data" / "bronze"

TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
]

builder = (
    SparkSession.builder
    .appName("VerifyBronzeTables")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

try:
    for table in TABLES:
        path = str(BRONZE_PATH / table)
        df = spark.read.format("delta").load(path)
        print(f"{table}: {df.count()} records")
finally:
    spark.stop()
