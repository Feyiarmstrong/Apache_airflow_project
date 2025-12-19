import csv
import json
import logging
import os
from pathlib import Path

from pendulum import DateTime, datetime


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_companies_config(
    config_path: str = "/opt/airflow/wikipedia_project/config/companies.json",
) -> dict:
    """
    Load company-to-Wikipedia-page mappings from JSON config.
    """
    try:
        with open(config_path, "r", encoding="utf-8") as file:
            companies = json.load(file)

        logger.info("Loaded %s companies from config", len(companies))
        return companies

    except Exception as exc:
        logger.error("Failed to load companies config: %s", exc)
        raise


def parse_pageview_line(line: str) -> dict | None:
    """
    Parse a single pageviews line.

    Format:
    domain page_title view_count response_size
    """
    try:
        domain, page_title, view_count, response_size = line.strip().split(" ", 3)

        return {
            "domain": domain,
            "page_title": page_title,
            "view_count": int(view_count),
            "response_size": int(response_size),
        }

    except Exception:
        return None


def filter_pageviews(
    input_path: str,
    output_path: str,
    companies_config: dict,
    domain_filter: str = "en",
) -> str:
    """
    Filter pageviews for specific company pages and save as CSV.
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    if os.path.exists(output_path):
        logger.info("Filtered file already exists: %s", output_path)
        return output_path

    # Reverse mapping for fast lookup: page_title → company
    page_to_company = {page: name for name, page in companies_config.items()}

    total_lines = 0
    matched_records = []

    logger.info("Processing file: %s", input_path)

    try:
        with open(input_path, "r", encoding="utf-8") as infile:
            for line in infile:
                total_lines += 1

                if total_lines % 1_000_000 == 0:
                    logger.info(
                        "Processed %s lines, %s matches found",
                        total_lines,
                        len(matched_records),
                    )

                record = parse_pageview_line(line)
                if not record:
                    continue

                if record["domain"] != domain_filter:
                    continue

                company = page_to_company.get(record["page_title"])
                if not company:
                    continue

                matched_records.append(
                    {
                        "company": company,
                        "page_title": record["page_title"],
                        "view_count": record["view_count"],
                        "domain": record["domain"],
                    }
                )

        logger.info(
            "Filtering complete: %s lines processed, %s matches found",
            total_lines,
            len(matched_records),
        )

        # Always create output file to keep runs idempotent
        with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["company", "page_title", "view_count", "domain"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(matched_records)

        logger.info("Saved filtered data to %s", output_path)
        return output_path

    except Exception as exc:
        logger.error("Filtering failed: %s", exc)
        if os.path.exists(output_path):
            os.remove(output_path)
        raise


def filter_pageviews_by_date(
    execution_date: DateTime,
    processed_dir: str = "/opt/airflow/data/wikipedia_project/processed",
    config_path: str = "/opt/airflow/wikipedia_project/config/companies.json",
) -> str:
    """
    Filter pageviews for a specific execution date/hour.
    """
    companies = load_companies_config(config_path)
    filename = (
        f"pageviews-"
        f"{execution_date.year}"
        f"{execution_date.month:02d}"
        f"{execution_date.day:02d}-"
        f"{execution_date.hour:02d}0000"
    )

    input_path = os.path.join(processed_dir, filename)
    output_path = os.path.join(
        processed_dir,
        f"filtered_{filename}.csv",
    )

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    return filter_pageviews(input_path, output_path, companies)


if __name__ == "__main__":
    # December 17, 2025 at 4 PM (UTC)
    test_date = datetime(2025, 12, 17, 16, tz="UTC")

    try:
        output_file = filter_pageviews_by_date(
            test_date,
            processed_dir="./data/processed",
            config_path="./config/companies.json",
        )
        print(f"Filtering successful: {output_file}")

        # Preview first few lines
        with open(output_file, "r", encoding="utf-8") as file:
            print("\nSample output:")
            for line in file.readlines()[:10]:
                print(line.strip())

    except Exception as error:
        print(f"Filtering failed: {error}")