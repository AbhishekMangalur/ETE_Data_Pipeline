
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

PROJECT_DIR = Path(__file__).resolve().parents[1]
GOLD_DIR = PROJECT_DIR / "data" / "gold"

st.set_page_config(
    page_title="E-commerce Analytics",
    page_icon="📊",
    layout="wide",
)

st.title("📊 E-commerce Analytics Dashboard")
st.caption("Gold-layer analytics | Powered by PySpark, Delta Lake and Streamlit")


@st.cache_resource
def get_spark():
    builder = (
        SparkSession.builder
        .appName("EcommerceAnalyticsDashboard")
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


@st.cache_data(ttl=300)
def load_gold_data():
    spark = get_spark()

    monthly = (
        spark.read.format("delta")
        .load(str(GOLD_DIR / "monthly_sales"))
        .toPandas()
    )

    products = (
        spark.read.format("delta")
        .load(str(GOLD_DIR / "product_performance"))
        .toPandas()
    )

    customers = (
        spark.read.format("delta")
        .load(str(GOLD_DIR / "customer_summary"))
        .toPandas()
    )

    monthly["sales_month"] = pd.to_datetime(monthly["sales_month"])
    monthly["total_revenue"] = pd.to_numeric(monthly["total_revenue"])
    monthly["average_order_value"] = pd.to_numeric(
        monthly["average_order_value"]
    )
    products["sales_revenue"] = pd.to_numeric(products["sales_revenue"])
    customers["total_spent"] = pd.to_numeric(customers["total_spent"])

    return monthly, products, customers


try:
    monthly, products, customers = load_gold_data()
except Exception as exc:
    st.error(f"Could not load Gold Delta datasets: {exc}")
    st.info(
        "Check that the pipeline has generated the Gold tables "
        "and that the matching Delta JVM package is available."
    )
    st.stop()


# Sidebar filters
st.sidebar.header("Dashboard Filters")

categories = sorted(products["category"].dropna().unique().tolist())
selected_categories = st.sidebar.multiselect(
    "Product categories",
    categories,
    default=categories,
)

if selected_categories:
    filtered_products = products[
        products["category"].isin(selected_categories)
    ].copy()
else:
    filtered_products = products.iloc[0:0].copy()

# Monthly date filter
min_month = monthly["sales_month"].min().date()
max_month = monthly["sales_month"].max().date()

start_date, end_date = st.sidebar.date_input(
    "Sales period",
    value=(min_month, max_month),
    min_value=min_month,
    max_value=max_month,
)

if not isinstance(start_date, tuple) and not isinstance(start_date, list):
    # Streamlit normally returns a tuple for a date range.
    start_date, end_date = min_month, max_month

filtered_monthly = monthly[
    monthly["sales_month"].dt.date.between(start_date, end_date)
].copy()

# KPI cards
total_revenue = filtered_monthly["total_revenue"].sum()
total_orders = filtered_monthly["total_orders"].sum()
avg_order_value = (
    total_revenue / total_orders if total_orders else 0
)
total_customers = customers["customer_id"].nunique()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Revenue", f"₹{total_revenue:,.0f}")
col2.metric("Total Orders", f"{total_orders:,.0f}")
col3.metric("Average Order Value", f"₹{avg_order_value:,.0f}")
col4.metric("Customers", f"{total_customers:,}")

st.divider()

# Revenue trend
st.subheader("Monthly Sales Trend")

if not filtered_monthly.empty:
    fig = px.line(
        filtered_monthly.sort_values("sales_month"),
        x="sales_month",
        y="total_revenue",
        markers=True,
        title="Revenue by Month",
        labels={
            "sales_month": "Month",
            "total_revenue": "Revenue (₹)",
        },
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No monthly sales data for the selected period.")

left, right = st.columns(2)

with left:
    st.subheader("Top Products by Revenue")
    top_products = filtered_products.nlargest(10, "sales_revenue")

    if not top_products.empty:
        fig = px.bar(
            top_products.sort_values("sales_revenue"),
            x="sales_revenue",
            y="product_name",
            orientation="h",
            color="category",
            labels={
                "sales_revenue": "Revenue (₹)",
                "product_name": "Product",
                "category": "Category",
            },
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Select at least one product category.")

with right:
    st.subheader("Revenue by Category")

    if not filtered_products.empty:
        category_sales = (
            filtered_products.groupby("category", as_index=False)
            .agg(sales_revenue=("sales_revenue", "sum"))
            .sort_values("sales_revenue", ascending=False)
        )

        fig = px.pie(
            category_sales,
            names="category",
            values="sales_revenue",
            hole=0.45,
            title="Product Revenue Share",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Select at least one product category.")

st.divider()

# Customer insights
st.subheader("Top Customers")

top_customers = customers.nlargest(10, "total_spent").copy()
top_customers["total_spent"] = top_customers["total_spent"].round(2)

st.dataframe(
    top_customers[
        [
            "customer_name",
            "city",
            "state",
            "total_orders",
            "total_spent",
            "last_order_date",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)

st.subheader("Product Performance")

st.dataframe(
    filtered_products.sort_values("sales_revenue", ascending=False),
    use_container_width=True,
    hide_index=True,
)

st.caption(
    "Data source: Gold Delta Lake tables | "
    "Dashboard cache refreshes every 5 minutes."
)
