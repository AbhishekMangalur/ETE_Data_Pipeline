
import csv
import random
from pathlib import Path
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("en_IN")
random.seed(42)
Faker.seed(42)

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)


def write_csv(filename, rows):
    path = RAW_DIR / filename

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"Created {path}: {len(rows)} records")


def generate_customers(count=1000):
    rows = []

    for i in range(1, count + 1):
        rows.append({
            "customer_id": f"C{i:06d}",
            "customer_name": fake.name(),
            "email": fake.unique.email(),
            "city": fake.city(),
            "state": fake.state(),
            "signup_date": fake.date_between(
                start_date="-3y", end_date="today"
            ).isoformat(),
        })

    write_csv("customers.csv", rows)


def generate_products(count=200):
    categories = [
        "electronics", "fashion", "home",
        "beauty", "books", "sports",
    ]

    rows = []

    for i in range(1, count + 1):
        rows.append({
            "product_id": f"P{i:05d}",
            "product_name": fake.catch_phrase(),
            "category": random.choice(categories),
            "unit_price": round(random.uniform(99, 25000), 2),
        })

    write_csv("products.csv", rows)


def generate_orders(order_count=5000):
    customers = list(csv.DictReader(
        (RAW_DIR / "customers.csv").open(encoding="utf-8")
    ))
    products = list(csv.DictReader(
        (RAW_DIR / "products.csv").open(encoding="utf-8")
    ))

    orders = []
    items = []
    payments = []

    for i in range(1, order_count + 1):
        order_id = f"O{i:07d}"
        customer = random.choice(customers)
        order_date = fake.date_time_between(
            start_date="-2y", end_date="now"
        )
        status = random.choices(
            ["delivered", "shipped", "processing", "cancelled"],
            weights=[70, 15, 10, 5],
            k=1,
        )[0]

        orders.append({
            "order_id": order_id,
            "customer_id": customer["customer_id"],
            "order_date": order_date.isoformat(timespec="seconds"),
            "order_status": status,
        })

        selected_products = random.sample(
            products, k=random.randint(1, min(4, len(products)))
        )
        total = 0.0

        for item_no, product in enumerate(selected_products, start=1):
            quantity = random.randint(1, 3)
            price = float(product["unit_price"])
            freight = round(random.uniform(20, 250), 2)
            total += quantity * price + freight

            items.append({
                "order_item_id": f"{order_id}-I{item_no}",
                "order_id": order_id,
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": f"{price:.2f}",
                "freight_value": f"{freight:.2f}",
            })

        payments.append({
            "payment_id": f"PAY{i:07d}",
            "order_id": order_id,
            "payment_type": random.choice([
                "credit_card", "debit_card", "upi",
                "net_banking", "wallet",
            ]),
            "payment_value": f"{total:.2f}",
            "payment_status": (
                "refunded" if status == "cancelled" else "paid"
            ),
        })

from datetime import datetime, timedelta


def generate_orders(order_count=5000):
    customers = list(csv.DictReader(
        (RAW_DIR / "customers.csv").open(encoding="utf-8")
    ))
    products = list(csv.DictReader(
        (RAW_DIR / "products.csv").open(encoding="utf-8")
    ))

    orders = []
    items = []
    payments = []

    for i in range(1, order_count + 1):
        order_id = f"O{i:07d}"
        customer = random.choice(customers)
        order_date = fake.date_time_between(
            start_date="-2y", end_date="now"
        )
        status = random.choices(
            ["delivered", "shipped", "processing", "cancelled"],
            weights=[70, 15, 10, 5],
            k=1,
        )[0]

        orders.append({
            "order_id": order_id,
            "customer_id": customer["customer_id"],
            "order_date": order_date.isoformat(timespec="seconds"),
            "order_status": status,
        })

        selected_products = random.sample(
            products, k=random.randint(1, min(4, len(products)))
        )
        total = 0.0

        for item_no, product in enumerate(selected_products, start=1):
            quantity = random.randint(1, 3)
            price = float(product["unit_price"])
            freight = round(random.uniform(20, 250), 2)
            total += quantity * price + freight

            items.append({
                "order_item_id": f"{order_id}-I{item_no}",
                "order_id": order_id,
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": f"{price:.2f}",
                "freight_value": f"{freight:.2f}",
            })

        payments.append({
            "payment_id": f"PAY{i:07d}",
            "order_id": order_id,
            "payment_type": random.choice([
                "credit_card", "debit_card", "upi",
                "net_banking", "wallet",
            ]),
            "payment_value": f"{total:.2f}",
            "payment_status": (
                "refunded" if status == "cancelled" else "paid"
            ),
        })

    write_csv("orders.csv", orders)
    write_csv("order_items.csv", items)
    write_csv("payments.csv", payments)

    write_csv("orders.csv", orders)
    write_csv("order_items.csv", items)
    write_csv("payments.csv", payments)


if __name__ == "__main__":
    generate_customers()
    generate_products()
    generate_orders()
