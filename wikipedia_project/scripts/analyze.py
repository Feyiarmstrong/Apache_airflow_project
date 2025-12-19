

import csv
import logging
import os
from collections import defaultdict
from pendulum import DateTime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def analyze_pageviews(csv_path: str, output_dir: str = "./data/processed") -> str:
    """
    Aggregate total pageviews per company.

    Args:
        csv_path: Path to filtered CSV
        output_dir: Directory to save analysis CSV

    Returns:
        str: Path to analysis output CSV
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # Aggregation dicts
    company_totals = defaultdict(int)
    page_totals = defaultdict(lambda: defaultdict(int))  # company -> page_title -> views

    # Read filtered CSV
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            company = row['company']
            page_title = row['page_title']
            view_count = int(row['view_count'])
            company_totals[company] += view_count
            page_totals[company][page_title] += view_count

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Output aggregated company totals
    output_file = os.path.join(output_dir, "analysis_company_totals.csv")
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['company', 'total_views']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for company, total in company_totals.items():
            writer.writerow({'company': company, 'total_views': total})

    logger.info("Analysis complete. Company totals saved to %s", output_file)

    # Optional: print top pages per company
    for company, pages in page_totals.items():
        top_pages = sorted(pages.items(), key=lambda x: x[1], reverse=True)[:5]
        logger.info("Top pages for %s: %s", company, top_pages)

    return output_file


if __name__ == "__main__":
    # Example usage
    csv_file = "./data/processed/filtered_pageviews-20251217-160000.csv"
    analyze_pageviews(csv_file)