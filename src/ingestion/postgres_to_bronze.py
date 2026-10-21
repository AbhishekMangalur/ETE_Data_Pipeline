
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

PROJECT_ROOT = Path(__file__).resolve().parents[2]

if not os.getenv("AIRFLOW_HOME"):
    load_dotenv(PROJECT_ROOT / ".env")
JDBC_JAR = PROJECT_ROOT / "jars" / "postgresql-42.7.7.jar"
BRONZE_PATH = PROJECT_ROOT / "data" / "bronze"

TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
]


def main():
    builder = (
        SparkSession.builder
        .appName("EcommercePostgresToBronze")
        .master("local[*]")
        .config("spark.jars", str(JDBC_JAR))
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

    jdbc_url = (
        f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.environ['POSTGRES_DB']}"
    )

    connection_properties = {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }

    try:
        for table in TABLES:
            print(f"Reading raw.{table} from PostgreSQL...")

            df = (
                spark.read
                .jdbc(
                    url=jdbc_url,
                    table=f"raw.{table}",
                    properties=connection_properties,
                )
            )

            output_path = BRONZE_PATH / table

            (
                df.write
                .format("delta")
                .mode("overwrite")
                .save(str(output_path))
            )

            print(f"Bronze table saved: {output_path}")
            print(f"Records: {df.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
