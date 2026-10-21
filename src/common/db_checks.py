
from src.common.db import get_db_connection

EXPECTED_TABLES = {
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
}


def validate_raw_tables():
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'raw'
          AND table_type = 'BASE TABLE';
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            existing_tables = {row[0] for row in cur.fetchall()}

    missing_tables = EXPECTED_TABLES - existing_tables

    if missing_tables:
        raise RuntimeError(
            f"Missing raw tables: {sorted(missing_tables)}"
        )

    print("All expected raw tables exist:")
    for table in sorted(EXPECTED_TABLES):
        print(f"  ✓ raw.{table}")


if __name__ == "__main__":
    validate_raw_tables()
