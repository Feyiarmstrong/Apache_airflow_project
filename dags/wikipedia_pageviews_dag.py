from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime
from datetime import timedelta
import sys


# Make project importable
sys.path.insert(0, "/opt/airflow")


from wikipedia_project.tasks import (
    download_wrapper,
    extract_wrapper,
    filter_wrapper,
    load_wrapper,
    analyze_pageviews,
)


default_args = {
    "owner": "Feyiarmstrong",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["solapeajiboye@gmail.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="wikipedia_pageviews_pipeline",
    default_args=default_args,
    description="Download and analyze Wikipedia pageviews for tech companies",
    schedule=None,
    start_date=datetime(2025, 12, 1, 0, tz="UTC"),
    catchup=False,
    #tags=["wikipedia", "pageviews", "sentiment-analysis"],
) as dag:

    # Task 1: Download pageviews data
    download_task = PythonOperator(
        task_id="download_pageviews",
        python_callable=download_wrapper,
    )

    # Task 2: Extract (decompress) the .gz file
    extract_task = PythonOperator(
        task_id="extract_pageviews",
        python_callable=extract_wrapper,
    )

    # Task 3: Filter for company pages only
    filter_task = PythonOperator(
        task_id="filter_pageviews",
        python_callable=filter_wrapper,
    )

    # Task 4: Load filtered data into database
    load_task = PythonOperator(
        task_id="load_to_database",
        python_callable=load_wrapper,
    )

    # Task 5: Analyze highest pageviews
    analyze_task = PythonOperator(
        task_id="analyze_highest_pageviews",
        python_callable=analyze_pageviews,
    )

    # Task dependencies
    download_task >> extract_task >> filter_task >> load_task >> analyze_task