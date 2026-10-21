
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_PATH = PROJECT_ROOT / "data" / "gold"

builder = (
    SparkSession.builder
    .appName("EcommerceGoldDataQuality")
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


def check(name, passed):
    global failed_checks

    if passed:
        print(f"PASS: {name}")
    else:
        print(f"FAIL: {name}")
        failed_checks += 1


def read_gold(table):
    return spark.read.format("delta").load(
        str(GOLD_PATH / table)
    )


try:
    monthly_sales = read_gold("monthly_sales")
    product_performance = read_gold("product_performance")
    customer_summary = read_gold("customer_summary")

    # 1. Monthly sales checks
    check(
        "Monthly sales contains records",
        monthly_sales.count() > 0
    )

    check(
        "Monthly sales has unique months",
        monthly_sales.count()
        == monthly_sales.select("sales_month").distinct().count()
    )

    check(
        "Monthly revenue is non-negative",
        monthly_sales.filter(
            F.col("total_revenue").isNull()
            | (F.col("total_revenue") < 0)
        ).count() == 0
    )

    check(
        "Monthly order counts are positive",
        monthly_sales.filter(
            F.col("total_orders").isNull()
            | (F.col("total_orders") <= 0)
        ).count() == 0
    )

    # 2. Product performance checks
    check(
        "Product performance contains records",
        product_performance.count() > 0
    )

    check(
        "Product IDs are unique",
        product_performance.count()
        == product_performance.select("product_id").distinct().count()
    )

    check(
        "Product quantities are non-negative",
        product_performance.filter(
            F.col("units_sold").isNull()
            | (F.col("units_sold") < 0)
        ).count() == 0
    )

    check(
        "Product sales revenue is non-negative",
        product_performance.filter(
            F.col("sales_revenue").isNull()
            | (F.col("sales_revenue") < 0)
        ).count() == 0
    )

    # 3. Customer summary checks
    check(
        "Customer summary contains 1,000 customers",
        customer_summary.count() == 1000
    )

    check(
        "Customer IDs are unique",
        customer_summary.count()
        == customer_summary.select("customer_id").distinct().count()
    )

    check(
        "Customer order counts are non-negative",
        customer_summary.filter(
            F.col("total_orders").isNull()
            | (F.col("total_orders") < 0)
        ).count() == 0
    )

    check(
        "Customer spending is non-negative",
        customer_summary.filter(
            F.col("total_spent").isNull()
            | (F.col("total_spent") < 0)
        ).count() == 0
    )

    print(f"\nFailed checks: {failed_checks}")

    if failed_checks > 0:
        raise SystemExit(1)

    print("Gold data quality validation passed!")

finally:
    spark.stop()
