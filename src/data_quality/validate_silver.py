
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_PATH = PROJECT_ROOT / "data" / "silver"

builder = (
    SparkSession.builder
    .appName("SilverDataQualityChecks")
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

failed_checks = 0


def check(name, condition):
    global failed_checks

    if condition:
        print(f"PASS: {name}")
    else:
        print(f"FAIL: {name}")
        failed_checks += 1


def read_silver(table):
    return spark.read.format("delta").load(
        str(SILVER_PATH / table)
    )


try:
    customers = read_silver("customers")
    products = read_silver("products")
    orders = read_silver("orders")
    order_items = read_silver("order_items")
    payments = read_silver("payments")

    # Primary-key checks
    primary_keys = {
        "customers": (customers, "customer_id"),
        "products": (products, "product_id"),
        "orders": (orders, "order_id"),
        "order_items": (order_items, "order_item_id"),
        "payments": (payments, "payment_id"),
    }

    for table_name, (df, key) in primary_keys.items():
        check(
            f"{table_name}: primary key has no nulls",
            df.filter(F.col(key).isNull()).limit(1).count() == 0,
        )

        check(
            f"{table_name}: primary key is unique",
            df.groupBy(key).count()
            .filter(F.col("count") > 1)
            .limit(1).count() == 0,
        )

    # Business-rule checks
    check(
        "Products: unit prices are non-negative",
        products.filter(
            F.col("unit_price").isNull() |
            (F.col("unit_price") < 0)
        ).limit(1).count() == 0,
    )

    check(
        "Order items: quantities are positive",
        order_items.filter(
            F.col("quantity").isNull() |
            (F.col("quantity") <= 0)
        ).limit(1).count() == 0,
    )

    check(
        "Order items: prices and freight are non-negative",
        order_items.filter(
            F.col("unit_price").isNull() |
            (F.col("unit_price") < 0) |
            F.col("freight_value").isNull() |
            (F.col("freight_value") < 0)
        ).limit(1).count() == 0,
    )

    check(
        "Payments: values are non-negative",
        payments.filter(
            F.col("payment_value").isNull() |
            (F.col("payment_value") < 0)
        ).limit(1).count() == 0,
    )

    # Referential-integrity checks
    check(
        "Orders: all customers exist",
        orders.join(
            customers.select("customer_id"),
            on="customer_id",
            how="left_anti",
        ).limit(1).count() == 0,
    )

    check(
        "Order items: all orders exist",
        order_items.join(
            orders.select("order_id"),
            on="order_id",
            how="left_anti",
        ).limit(1).count() == 0,
    )

    check(
        "Order items: all products exist",
        order_items.join(
            products.select("product_id"),
            on="product_id",
            how="left_anti",
        ).limit(1).count() == 0,
    )

    check(
        "Payments: all orders exist",
        payments.join(
            orders.select("order_id"),
            on="order_id",
            how="left_anti",
        ).limit(1).count() == 0,
    )

    print("\nData-quality validation finished.")
    print(f"Failed checks: {failed_checks}")

    if failed_checks:
        raise SystemExit(1)

finally:
    spark.stop()
