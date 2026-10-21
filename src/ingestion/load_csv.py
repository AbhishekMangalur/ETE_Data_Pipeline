
import csv
from pathlib import Path

from psycopg import sql

from src.common.db import get_db_connection

RAW_DIR = Path("data/raw")

TABLE_COLUMNS = {
    "customers": [
        "customer_id", "customer_name", "email",
        "city", "state", "signup_date",
    ],
    "products": [
        "product_id", "product_name", "category", "unit_price",
    ],
    "orders": [
        "order_id", "customer_id", "order_date", "order_status",
    ],
    "order_items": [
        "order_item_id", "order_id", "product_id",
        "quantity", "unit_price", "freight_value",
    ],
    "payments": [
        "payment_id", "order_id", "payment_type",
        "payment_value", "payment_status",
    ],
}


def load_csv(table_name: str) -> int:
    if table_name not in TABLE_COLUMNS:
        raise ValueError(f"Unsupported table: {table_name}")

    file_path = RAW_DIR / f"{table_name}.csv"

    if not file_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    columns = TABLE_COLUMNS[table_name]
    inserted = 0

    with file_path.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        if reader.fieldnames != columns:
            raise ValueError(
                f"Unexpected CSV headers in {file_path}. "
                f"Expected {columns}, got {reader.fieldnames}"
            )

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                statement = sql.SQL(
                    "INSERT INTO {}.{} ({}) VALUES ({}) "
                    "ON CONFLICT DO NOTHING"
                ).format(
                    sql.Identifier("raw"),
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                    sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                )

                for row in reader:
                    cur.execute(
                        statement,
                        [row[column] or None for column in columns],
                    )
                    inserted += cur.rowcount

    print(f"Inserted {inserted} new rows into raw.{table_name}")
    return inserted


if __name__ == "__main__":
    for table in TABLE_COLUMNS:
        load_csv(table)
