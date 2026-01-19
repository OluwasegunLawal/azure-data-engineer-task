from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="product_data_pipeline",
    description="Fetch, transform, and load product data into Azure SQL",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",  # simulated daily run
    catchup=False,
    tags=["azure", "etl", "portfolio"],
) as dag:

    fetch_data = BashOperator(
        task_id="fetch_data",
        bash_command="python scripts/fetch_data.py",
    )

    transform_data = BashOperator(
        task_id="transform_data",
        bash_command="python scripts/transform_data.py",
    )

    load_to_db = BashOperator(
        task_id="load_to_db",
        bash_command="python scripts/load_to_sql.py",
    )

    # Define task dependencies (DAG)
    fetch_data >> transform_data >> load_to_db
