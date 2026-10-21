
from pathlib import Path
import shutil

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_PATH = PROJECT_ROOT / "data" / "bronze"
SILVER_PATH = PROJECT_ROOT / "data" / "silver"
QUARANTINE_PATH = PROJECT_ROOT / "data" / "quarantine"


builder = (
    SparkSession.builder
    .appName("EcommerceBronzeToSilver")
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


def read_bronze(table):
    return spark.read.format("delta").load(
        str(BRONZE_PATH / table)
    )


def write_silver(df, table):
    destination = SILVER_PATH / table

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(str(destination))
    )

    print(f"Silver {table}: {df.count()} records")


def write_quarantine(df, table):
    """Write rejected records to a Delta quarantine table."""
    destination = QUARANTINE_PATH / table
    count = df.count()

    if count == 0 and destination.exists():
        print(f"Quarantine {table}: 0 rejected records")
        return

    mode = "append" if destination.exists() else "overwrite"

    (
        df.write
        .format("delta")
        .mode(mode)
        .save(str(destination))
    )

    print(f"Quarantine {table}: {count} rejected records")


def validate_records(df, table, key_column, rules):
    """
    Validate records, quarantine invalid rows, and return valid rows.

    All duplicate non-null primary keys are quarantined because
    choosing one duplicate arbitrarily could discard valid information.
    """
    duplicate_count = (
        F.count(F.lit(1))
        .over(Window.partitionBy(key_column))
    )

    conditions = [
        (
            "duplicate_primary_key",
            F.col(key_column).isNotNull()
            & (duplicate_count > 1)
        )
    ]

    conditions.extend(rules)

    reason_expressions = [
        F.when(condition, F.lit(reason))
        for reason, condition in conditions
    ]

    checked = df.withColumn(
        "_rejection_reason",
        F.concat_ws("; ", *reason_expressions)
    )

    original_columns = df.columns

    rejected = (
        checked
        .filter(F.col("_rejection_reason") != "")
        .select(
            *original_columns,
            F.col("_rejection_reason").alias("rejection_reason")
        )
    )

    valid = (
        checked
        .filter(F.col("_rejection_reason") == "")
        .select(*original_columns)
    )

    write_quarantine(rejected, table)

    print(
        f"Validation {table}: "
        f"{valid.count()} valid records"
    )

    return valid


def quarantine_missing_reference(
    df,
    table,
    foreign_key,
    valid_ids,
    reason
):
    """Quarantine records whose foreign key has no valid parent record."""
    reference_ids = (
        valid_ids
        .select(F.col(valid_ids.columns[0]).alias("_reference_id"))
        .withColumn("_reference_found", F.lit(True))
    )

    joined = df.join(
        reference_ids,
        df[foreign_key] == reference_ids["_reference_id"],
        "left"
    )

    original_columns = df.columns

    rejected = (
        joined
        .filter(F.col("_reference_found").isNull())
        .select(
            *original_columns,
            F.lit(reason).alias("rejection_reason")
        )
    )

    valid = (
        joined
        .filter(F.col("_reference_found").isNotNull())
        .select(*original_columns)
    )

    write_quarantine(rejected, table)

    return valid


try:
    # Start a fresh quarantine run.
    # Bronze and Silver are not deleted here.
    shutil.rmtree(QUARANTINE_PATH, ignore_errors=True)

    # --------------------------------------------------
    # 1. CUSTOMERS
    # --------------------------------------------------

    customers = read_bronze("customers")

    customers = validate_records(
        customers,
        "customers",
        "customer_id",
        [
            (
                "missing_customer_id",
                F.col("customer_id").isNull()
                | (F.trim(F.col("customer_id")) == "")
            ),
            (
                "missing_customer_name",
                F.col("customer_name").isNull()
                | (F.trim(F.col("customer_name")) == "")
            ),
            (
                "missing_email",
                F.col("email").isNull()
                | (F.trim(F.col("email")) == "")
            ),
        ]
    )

    write_silver(customers, "customers")

    # --------------------------------------------------
    # 2. PRODUCTS
    # --------------------------------------------------

    products = read_bronze("products")

    products = validate_records(
        products,
        "products",
        "product_id",
        [
            (
                "missing_product_id",
                F.col("product_id").isNull()
                | (F.trim(F.col("product_id")) == "")
            ),
            (
                "missing_product_name",
                F.col("product_name").isNull()
                | (F.trim(F.col("product_name")) == "")
            ),
            (
                "missing_unit_price",
                F.col("unit_price").isNull()
            ),
            (
                "negative_unit_price",
                F.col("unit_price") < 0
            ),
        ]
    )

    write_silver(products, "products")

    # --------------------------------------------------
    # 3. ORDERS
    # --------------------------------------------------

    orders = read_bronze("orders")

    orders = validate_records(
        orders,
        "orders",
        "order_id",
        [
            (
                "missing_order_id",
                F.col("order_id").isNull()
                | (F.trim(F.col("order_id")) == "")
            ),
            (
                "missing_customer_id",
                F.col("customer_id").isNull()
                | (F.trim(F.col("customer_id")) == "")
            ),
            (
                "missing_order_date",
                F.col("order_date").isNull()
            ),
            (
                "missing_order_status",
                F.col("order_status").isNull()
                | (F.trim(F.col("order_status")) == "")
            ),
        ]
    )

    valid_customer_ids = customers.select("customer_id").distinct()

    orders = quarantine_missing_reference(
        orders,
        "orders",
        "customer_id",
        valid_customer_ids,
        "customer_not_found"
    )

    write_silver(orders, "orders")

    # --------------------------------------------------
    # 4. ORDER ITEMS
    # --------------------------------------------------

    order_items = read_bronze("order_items")

    order_items = validate_records(
        order_items,
        "order_items",
        "order_item_id",
        [
            (
                "missing_order_item_id",
                F.col("order_item_id").isNull()
                | (F.trim(F.col("order_item_id")) == "")
            ),
            (
                "missing_order_id",
                F.col("order_id").isNull()
                | (F.trim(F.col("order_id")) == "")
            ),
            (
                "missing_product_id",
                F.col("product_id").isNull()
                | (F.trim(F.col("product_id")) == "")
            ),
            (
                "missing_quantity",
                F.col("quantity").isNull()
            ),
            (
                "non_positive_quantity",
                F.col("quantity") <= 0
            ),
            (
                "missing_unit_price",
                F.col("unit_price").isNull()
            ),
            (
                "negative_unit_price",
                F.col("unit_price") < 0
            ),
            (
                "missing_freight_value",
                F.col("freight_value").isNull()
            ),
            (
                "negative_freight_value",
                F.col("freight_value") < 0
            ),
        ]
    )

    valid_order_ids = orders.select("order_id").distinct()
    valid_product_ids = products.select("product_id").distinct()

    order_items = quarantine_missing_reference(
        order_items,
        "order_items",
        "order_id",
        valid_order_ids,
        "order_not_found"
    )

    order_items = quarantine_missing_reference(
        order_items,
        "order_items",
        "product_id",
        valid_product_ids,
        "product_not_found"
    )

    write_silver(order_items, "order_items")

    # --------------------------------------------------
    # 5. PAYMENTS
    # --------------------------------------------------

    payments = read_bronze("payments")

    payments = validate_records(
        payments,
        "payments",
        "payment_id",
        [
            (
                "missing_payment_id",
                F.col("payment_id").isNull()
                | (F.trim(F.col("payment_id")) == "")
            ),
            (
                "missing_order_id",
                F.col("order_id").isNull()
                | (F.trim(F.col("order_id")) == "")
            ),
            (
                "missing_payment_type",
                F.col("payment_type").isNull()
                | (F.trim(F.col("payment_type")) == "")
            ),
            (
                "missing_payment_value",
                F.col("payment_value").isNull()
            ),
            (
                "negative_payment_value",
                F.col("payment_value") < 0
            ),
            (
                "missing_payment_status",
                F.col("payment_status").isNull()
                | (F.trim(F.col("payment_status")) == "")
            ),
        ]
    )

    payments = quarantine_missing_reference(
        payments,
        "payments",
        "order_id",
        valid_order_ids,
        "order_not_found"
    )

    write_silver(payments, "payments")

    print("\nBronze-to-Silver pipeline completed.")
    print(f"Quarantine location: {QUARANTINE_PATH}")

finally:
    spark.stop()
