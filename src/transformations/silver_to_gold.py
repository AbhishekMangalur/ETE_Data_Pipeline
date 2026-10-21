
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_PATH = PROJECT_ROOT / "data" / "silver"
GOLD_PATH = PROJECT_ROOT / "data" / "gold"


builder = (
    SparkSession.builder
    .appName("EcommerceSilverToGold")
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


def read_silver(table):
    """Read a Silver Delta table."""
    return spark.read.format("delta").load(
        str(SILVER_PATH / table)
    )


def write_gold(df, table):
    """Write a Gold Delta table using a full refresh."""
    destination = GOLD_PATH / table

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(str(destination))
    )

    print(f"Gold {table}: {df.count()} records")


try:
    # Read Silver tables
    customers = read_silver("customers")
    products = read_silver("products")
    orders = read_silver("orders")
    order_items = read_silver("order_items")

    # Business rule:
    # Only delivered orders count toward realized sales revenue.
    delivered_orders = orders.filter(
        F.lower(F.col("order_status")) == "delivered"
    )

    # Keep only order items belonging to delivered orders.
    delivered_order_items = order_items.join(
        delivered_orders.select("order_id"),
        on="order_id",
        how="inner"
    )

    # Calculate revenue from order items.
    # Revenue = quantity * unit_price.
    item_sales = delivered_order_items.withColumn(
        "sales_amount",
        F.col("quantity") * F.col("unit_price")
    )

    # Calculate revenue per delivered order.
    order_sales = (
        item_sales
        .groupBy("order_id")
        .agg(
            F.sum("sales_amount").alias("order_revenue")
        )
    )

    # --------------------------------------------------
    # 1. Gold: Monthly Sales
    # Delivered orders, revenue, and average order value
    # --------------------------------------------------

    monthly_sales = (
        delivered_orders
        .join(
            order_sales,
            on="order_id",
            how="inner"
        )
        .withColumn(
            "sales_month",
            F.date_trunc("month", F.col("order_date"))
        )
        .groupBy("sales_month")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(
                F.sum("order_revenue"), 2
            ).alias("total_revenue"),
            F.round(
                F.avg("order_revenue"), 2
            ).alias("average_order_value"),
        )
        .orderBy("sales_month")
    )

    write_gold(monthly_sales, "monthly_sales")

    # --------------------------------------------------
    # 2. Gold: Product Performance
    # Units sold, delivered order count, and sales revenue
    # --------------------------------------------------

    product_performance = (
        item_sales
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("order_id").alias("orders_count"),
            F.round(
                F.sum("sales_amount"), 2
            ).alias("sales_revenue"),
        )
        .join(
            products,
            on="product_id",
            how="left"
        )
        .select(
            "product_id",
            "product_name",
            "category",
            "units_sold",
            "orders_count",
            "sales_revenue",
        )
        .orderBy(F.desc("sales_revenue"))
    )

    write_gold(product_performance, "product_performance")

    # --------------------------------------------------
    # 3. Gold: Customer Summary
    # Delivered order count, spending, and last delivered
    # order date. Retain all customers, even with no
    # delivered orders.
    # --------------------------------------------------

    customer_orders = (
        delivered_orders
        .groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.max("order_date").alias("last_order_date"),
        )
    )

    customer_spending = (
        delivered_orders
        .join(
            order_sales,
            on="order_id",
            how="inner"
        )
        .groupBy("customer_id")
        .agg(
            F.round(
                F.sum("order_revenue"), 2
            ).alias("total_spent")
        )
    )

    customer_summary = (
        customers
        .join(
            customer_orders,
            on="customer_id",
            how="left"
        )
        .join(
            customer_spending,
            on="customer_id",
            how="left"
        )
        .fillna({
            "total_orders": 0,
            "total_spent": 0.0
        })
        .select(
            "customer_id",
            "customer_name",
            "city",
            "state",
            "total_orders",
            "total_spent",
            "last_order_date",
        )
        .orderBy(F.desc("total_spent"))
    )

    write_gold(customer_summary, "customer_summary")

    print("\nGold layer generation completed successfully.")

finally:
    spark.stop()
