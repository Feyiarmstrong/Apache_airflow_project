import os
import logging
from pathlib import Path

import requests
from pendulum import DateTime, datetime


# Set up basic logging so you can see what the script is doing
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def construct_pageviews_url(year: int, month: int, day: int, hour: int) -> tuple[str, str]:
    """
    Build the Wikimedia pageviews URL and filename
    for a given date and hour.
    """
    # Base location where Wikimedia stores pageviews dumps
    base_url = "https://dumps.wikimedia.org/other/pageviews"

    # Example filename:
    # pageviews-20251217-160000.gz
    filename = f"pageviews-{year}{month:02d}{day:02d}-{hour:02d}0000.gz"

    # Full URL pointing to the hourly dump file
    url = f"{base_url}/{year}/{year}-{month:02d}/{filename}"

    logger.info("Constructed URL: %s", url)
    return url, filename


def download_pageviews(
    year: int,
    month: int,
    day: int,
    hour: int,
    output_dir: str = "/opt/airflow/data/wikipedia_project/raw",
) -> str:
    """
    Download the Wikipedia pageviews file for a specific hour
    and save it locally.
    """
    # Make sure the output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Get the download URL and expected filename
    url, filename = construct_pageviews_url(year, month, day, hour)
    output_path = os.path.join(output_dir, filename)

    # If the file already exists, don’t download again
    # This makes the function safe to rerun (idempotent)
    if os.path.exists(output_path):
        size = os.path.getsize(output_path)
        logger.info("File already exists: %s (%s bytes)", output_path, size)
        return output_path

    logger.info("Downloading pageviews from %s", url)

    try:
        # Stream=True avoids loading the full file into memory
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        # Write the file in chunks to handle large downloads
        total_bytes = 0
        with open(output_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
                    total_bytes += len(chunk)

        logger.info("Download complete: %s (%s bytes)", filename, total_bytes)
        return output_path

    except Exception as exc:
        # If anything fails, remove the partially downloaded file
        logger.error("Download failed: %s", exc)
        if os.path.exists(output_path):
            os.remove(output_path)
        raise


def download_pageviews_by_date(
    execution_date: DateTime,
    output_dir: str = "/opt/airflow/data/wikipedia_project/raw",
) -> str:
    return download_pageviews(
        year=execution_date.year,
        month=execution_date.month,
        day=execution_date.day,
        hour=execution_date.hour,
        output_dir=output_dir,
    )


if __name__ == "__main__":
    # Example local test:
    # December 17, 2025 at 4 PM (UTC)
    test_date = datetime(2025, 12, 17, 16, tz="UTC")

    try:
        output_file = download_pageviews_by_date(test_date, output_dir="./data/raw")
        print(f"Download successful: {output_file}")
    except Exception as error:
        print(f"Download failed: {error}")