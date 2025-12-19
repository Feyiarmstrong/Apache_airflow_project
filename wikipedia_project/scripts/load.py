import csv
import logging
import os

import psycopg2
from psycopg2.extras import execute_values
from pendulum import DateTime, datetime


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_db_connection(
    host: str = "postgres",
    database: str = "airflow",
    user: str = "airflow",
    password: str = "airflow",
    port: int = 5432,
):
    """
    Create and return a Postgres database connection.
    """
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
        )
        logger.info("Connected to database: %s", database)
        return conn

    except Exception as exc:
        logger.error("Database connection failed: %s", exc)
        raise


def create_table_if_not_exists(conn) -> None:
    """
    Ensure the wikipedia_pageviews table exists.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS wikipedia_pageviews (
        id SERIAL PRIMARY KEY,
        company VARCHAR(100) NOT NULL,
        page_title VARCHAR(255) NOT NULL,
        view_count INTEGER NOT NULL,
        domain VARCHAR(100) NOT NULL,
        execution_date TIMESTAMP NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (company, execution_date)
    );

    CREATE INDEX IF NOT EXISTS idx_pageviews_company
        ON wikipedia_pageviews(company);

    CREATE INDEX IF NOT EXISTS idx_pageviews_execution_date
        ON wikipedia_pageviews(execution_date);

    CREATE INDEX IF NOT EXISTS idx_pageviews_view_count
        ON wikipedia_pageviews(view_count);
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()

        logger.info("Table wikipedia_pageviews is ready")

    except Exception as exc:
        conn.rollback()
        logger.error("Table creation failed: %s", exc)
        raise


def load_csv_to_db(
    csv_path: str,
    execution_date: DateTime,
    conn,
) -> int:
    """
    Load filtered CSV data into Postgres.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    logger.info("Loading data from CSV: %s", csv_path)

    records = []

    # Read CSV and prepare rows for bulk insert
    with open(csv_path, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            records.append(
                (
                    row["company"],
                    row["page_title"],
                    int(row["view_count"]),
                    row["domain"],
                    execution_date.to_datetime_string(),
                )
            )

    if not records:
        logger.warning("No records found in CSV")
        return 0

    insert_sql = """
    INSERT INTO wikipedia_pageviews
        (company, page_title, view_count, domain, execution_date)
    VALUES %s
    ON CONFLICT (company, execution_date)
    DO UPDATE SET
        view_count = EXCLUDED.view_count,
        page_title = EXCLUDED.page_title,
        domain = EXCLUDED.domain;
    """

    try:
        with conn.cursor() as cursor:
            execute_values(cursor, insert_sql, records)
            conn.commit()
            rows_affected = cursor.rowcount

        logger.info("Inserted/updated %s rows", rows_affected)
        return rows_affected

    except Exception as exc:
        conn.rollback()
        logger.error("Data load failed: %s", exc)
        raise


def load_pageviews_by_date(
    execution_date: DateTime,
    processed_dir: str = "/opt/airflow/data/wikipedia_project/processed",
    db_host: str = "postgres",
    db_name: str = "airflow",
    db_user: str = "airflow",
    db_password: str = "airflow",
    db_port: int = 5432,
) -> int:
    """
    Load pageviews data for a specific execution date into Postgres.
    """
    csv_filename = (
        f"filtered_pageviews-"
        f"{execution_date.year}"
        f"{execution_date.month:02d}"
        f"{execution_date.day:02d}-"
        f"{execution_date.hour:02d}0000.csv"
    )

    csv_path = os.path.join(processed_dir, csv_filename)

    conn = None
    try:
        conn = get_db_connection(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port,
        )

        create_table_if_not_exists(conn)
        return load_csv_to_db(csv_path, execution_date, conn)

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    # December 17, 2025 at 4 PM (UTC)
    test_date = datetime(2025, 12, 17, 16, tz="UTC")

    try:
        rows = load_pageviews_by_date(
            test_date,
            processed_dir="./data/processed",
            db_host="airflow-postgres-1",
            db_name="airflow",
            db_user="airflow",
            db_password="airflow",
            db_port=5432,
        )
        print(f"Load successful: {rows} rows inserted/updated")

    except Exception as error:
        print(f"Load failed: {error}")