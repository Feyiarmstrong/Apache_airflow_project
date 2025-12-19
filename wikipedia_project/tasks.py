import psycopg2
from pendulum import datetime

from wikipedia_project.scripts.download import download_pageviews_by_date
from wikipedia_project.scripts.extract import extract_pageviews_by_date
from wikipedia_project.scripts.filter import filter_pageviews_by_date
from wikipedia_project.scripts.load import load_pageviews_by_date


# Target date for analysis: December 1, 2025 at 12:00 PM UTC
TARGET_DATE = datetime(2025, 12, 1, 12, tz="UTC")

RAW_DIR = "/opt/airflow/data/wikipedia_project/raw"
PROCESSED_DIR = "/opt/airflow/data/wikipedia_project/processed"
CONFIG_PATH = "/opt/airflow/wikipedia_project/config/companies.json"


DB_CONFIG = {
    "host": "postgres",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
    "port": 5432,
}


def download_wrapper(**context):
    """Download Wikipedia pageviews for target date."""
    return download_pageviews_by_date(
        execution_date=TARGET_DATE,
        output_dir=RAW_DIR,
    )


def extract_wrapper(**context):
    """Extract (decompress) pageviews file."""
    return extract_pageviews_by_date(
        execution_date=TARGET_DATE,
        raw_dir=RAW_DIR,
        processed_dir=PROCESSED_DIR,
    )


def filter_wrapper(**context):
    """Filter pageviews for tracked companies only."""
    return filter_pageviews_by_date(
        execution_date=TARGET_DATE,
        processed_dir=PROCESSED_DIR,
        config_path=CONFIG_PATH,
    )


def load_wrapper(**context):
    """Load filtered data into Postgres database."""
    return load_pageviews_by_date(
        execution_date=TARGET_DATE,
        processed_dir=PROCESSED_DIR,
        db_host=DB_CONFIG["host"],
        db_name=DB_CONFIG["database"],
        db_user=DB_CONFIG["user"],
        db_password=DB_CONFIG["password"],
        db_port=DB_CONFIG["port"],
    )


def analyze_pageviews(**context):
    """
    Query database to find company with highest pageviews.
    Prints results to task logs.
    """
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        cursor = conn.cursor()

        query = """
            SELECT
                company,
                view_count AS total_views,
                page_title,
                execution_date
            FROM wikipedia_pageviews
            WHERE execution_date = %s
            ORDER BY view_count DESC;
        """

        cursor.execute(query, (TARGET_DATE.to_datetime_string(),))
        results = cursor.fetchall()

        if not results:
            print("No data found for this execution date")
            return

        print("\n" + "=" * 60)
        print("WIKIPEDIA PAGEVIEWS ANALYSIS")
        print("=" * 60)
        print("Target Date: December 1, 2025 at 12:00 PM UTC\n")

        for i, (company, views, page, _) in enumerate(results, 1):
            print(f"{i}. {company}: {views:,} views ({page})")

        winner = results[0]
        print("\n" + "=" * 60)
        print(f" HIGHEST PAGEVIEWS: {winner[0]} with {winner[1]:,} views")
        print("=" * 60 + "\n")

        cursor.close()

    finally:
        conn.close()         