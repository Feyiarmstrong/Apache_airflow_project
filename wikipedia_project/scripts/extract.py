import gzip
import os
import logging
from pathlib import Path

from pendulum import DateTime, datetime


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_gz_file(
    input_path: str,
    output_dir: str = "/opt/airflow/data/wikipedia_project/processed",
) -> str:
    """
    Decompress a .gz file and save the extracted content.
    """
    # Ensure the processed directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Derive output filename by removing .gz
    input_filename = os.path.basename(input_path)
    output_filename = input_filename[:-3] if input_filename.endswith(".gz") else f"{input_filename}.extracted"
    output_path = os.path.join(output_dir, output_filename)

    # Skip extraction if file already exists
    if os.path.exists(output_path):
        size = os.path.getsize(output_path)
        logger.info("File already extracted: %s (%s bytes)", output_path, size)
        return output_path

    logger.info("Extracting %s", input_path)

    try:
        total_bytes = 0

        # Stream decompression to handle large files safely
        with gzip.open(input_path, "rb") as gz_file, open(output_path, "wb") as out_file:
            for chunk in iter(lambda: gz_file.read(8192), b""):
                out_file.write(chunk)
                total_bytes += len(chunk)

        logger.info("Extraction complete: %s (%s bytes)", output_filename, total_bytes)
        return output_path

    except Exception as exc:
        logger.error("Extraction failed: %s", exc)
        if os.path.exists(output_path):
            os.remove(output_path)
        raise


def extract_pageviews_by_date(
    execution_date: DateTime,
    raw_dir: str = "/opt/airflow/data/wikipedia_project/raw",
    processed_dir: str = "/opt/airflow/data/wikipedia_project/processed",
) -> str:
    """
    Extract the Wikipedia pageviews file for a given execution date.
    """
    # Build expected filename from execution_date
    filename = (
        f"pageviews-"
        f"{execution_date.year}"
        f"{execution_date.month:02d}"
        f"{execution_date.day:02d}-"
        f"{execution_date.hour:02d}0000.gz"
    )

    input_path = os.path.join(raw_dir, filename)

    # Fail fast if raw file does not exist
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    return extract_gz_file(input_path, processed_dir)


if __name__ == "__main__":
    # December 17, 2025 at 4 PM (UTC)
    test_date = datetime(2025, 12, 17, 16, tz="UTC")

    try:
        output_file = extract_pageviews_by_date(
            test_date,
            raw_dir="./data/raw",
            processed_dir="./data/processed",
        )
        print(f"Extraction successful: {output_file}")
    except Exception as error:
        print(f"Extraction failed: {error}")