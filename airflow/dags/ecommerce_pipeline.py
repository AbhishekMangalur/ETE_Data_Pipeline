
import os
from datetime import datetime
from html import escape

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

PROJECT_DIR = "/opt/airflow/project"
ALERT_EMAIL = os.environ["AIRFLOW__SMTP__SMTP_USER"]


def send_alert(subject, html_content):
    """Send a Gmail notification."""
    send_email(
        to=ALERT_EMAIL,
        subject=subject,
        html_content=html_content,
    )


def report_failure(context):
    """Notify by email when a task finally fails."""
    ti = context["task_instance"]

    subject = f"[FAILED] Ecommerce Pipeline - {ti.task_id}"
    html_content = f"""
    <h2>Ecommerce Data Pipeline Failed</h2>
    <p><b>DAG:</b> {escape(ti.dag_id)}</p>
    <p><b>Task:</b> {escape(ti.task_id)}</p>
    <p><b>Run ID:</b> {escape(ti.run_id)}</p>
    <p><b>Attempt:</b> {ti.try_number}</p>
    <p>Check the Airflow UI and task logs for details.</p>
    """

    try:
        send_alert(subject, html_content)
        ti.log.info("Failure notification email sent.")
    except Exception:
        ti.log.exception("Failed to send failure notification email.")


def log_pipeline_summary():
    """Log and email the successful pipeline completion."""
    print("===================================")
    print("ECOMMERCE PIPELINE COMPLETED")
    print("Status: SUCCESS")
    print("Layers: Bronze -> Silver -> Gold")
    print("Data quality: Silver and Gold validated")
    print("===================================")

    send_alert(
        "[SUCCESS] Ecommerce Data Pipeline",
        """
        <h2>Ecommerce Data Pipeline Completed Successfully</h2>
        <p><b>Status:</b> SUCCESS</p>
        <p><b>Layers:</b> Bronze -> Silver -> Gold</p>
        <p><b>Data quality:</b> Silver and Gold validations passed</p>
        """,
    )


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "on_failure_callback": report_failure,
}


with DAG(
    dag_id="ecommerce_data_pipeline",
    default_args=default_args,
    description="PostgreSQL to Bronze, Silver, and Gold with data quality checks",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "pyspark", "delta-lake", "monitoring"],
) as dag:

    ingest_bronze = BashOperator(
        task_id="postgres_to_bronze",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m src.ingestion.postgres_to_bronze"
        ),
    )

    transform_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m src.transformations.bronze_to_silver"
        ),
    )

    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m src.data_quality.validate_silver"
        ),
    )

    transform_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m src.transformations.silver_to_gold"
        ),
    )

    validate_gold = BashOperator(
        task_id="validate_gold",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m src.data_quality.validate_gold"
        ),
    )

    pipeline_summary = PythonOperator(
        task_id="pipeline_success_summary",
        python_callable=log_pipeline_summary,
    )

    (
        ingest_bronze
        >> transform_silver
        >> validate_silver
        >> transform_gold
        >> validate_gold
        >> pipeline_summary
    )
