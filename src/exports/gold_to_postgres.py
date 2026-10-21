
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

PROJECT_DIR = Path(__file__).resolve().parents[2]
GOLD_DIR = PROJECT_DIR / "data" / "gold"

load_dotenv(PROJECT_DIR / ".env")


def create_spark():
    builder = (
        SparkSession.builder
        .appName("GoldToPostgres")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


TABLES = {
    "monthly_sales": {
        "columns": """
            sales_month TIMESTAMP,
            total_orders BIGINT,
            total_revenue NUMERIC(38, 2),
            average_order_value NUMERIC(34, 2)
        """,
        "fields": (
            "sales_month", "total_orders",
            "total_revenue", "average_order_value",
        ),
    },
    "product_performance": {
        "columns": """
            product_id TEXT,
            product_name TEXT,
            category TEXT,
            units_sold BIGINT,
            orders_count BIGINT,
            sales_revenue NUMERIC(34, 2)
        """,
        "fields": (
            "product_id", "product_name", "category",
            "units_sold", "orders_count", "sales_revenue",
        ),
    },
    "customer_summary": {
        "columns": """
            customer_id TEXT,
            customer_name TEXT,
            city TEXT,
            state TEXT,
            total_orders BIGINT,
            total_spent NUMERIC(38, 2),
            last_order_date TIMESTAMP
        """,
        "fields": (
            "customer_id", "customer_name", "city", "state",
            "total_orders", "total_spent", "last_order_date",
        ),
    },
}


def main():
    spark = create_spark()

    try:
        datasets = {
            name: spark.read.format("delta").load(str(GOLD_DIR / name))
            for name in TABLES
        }

        with psycopg.connect(
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS analytics")

                for name, config in TABLES.items():
                    columns = config["columns"]
                    fields = config["fields"]
                    dataset = datasets[name]

                    cur.execute(
                        f"CREATE TABLE IF NOT EXISTS analytics.{name} "
                        f"({columns})"
                    )

                    # Refresh only the analytics table, not raw data.
                    cur.execute(f"TRUNCATE TABLE analytics.{name}")

                    quoted_fields = ", ".join(fields)
                    placeholders = ", ".join(["%s"] * len(fields))
                    insert_sql = (
                        f"INSERT INTO analytics.{name} "
                        f"({quoted_fields}) VALUES ({placeholders})"
                    )

                    count = 0
                    for row in dataset.select(*fields).toLocalIterator():
                        cur.execute(insert_sql, tuple(row))
                        count += 1

                    print(f"Published analytics.{name}: {count} rows")

        print("Gold datasets published successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
