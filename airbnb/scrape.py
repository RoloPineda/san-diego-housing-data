"""Download Airbnb listing data from Inside Airbnb for San Diego."""

import logging
import re
import sys
from pathlib import Path
from urllib.request import urlopen, Request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

INDEX_URL = "https://insideairbnb.com/get-the-data/"
BASE_URL = "https://data.insideairbnb.com/united-states/ca/san-diego"
DATA_DIR = Path(__file__).parent / "data"

# Files available per scrape date under the san-diego directory.
RELATIVE_PATHS = [
    "data/listings.csv.gz",
    "data/calendar.csv.gz",
    "data/reviews.csv.gz",
    "visualisations/listings.csv",
    "visualisations/reviews.csv",
    "visualisations/neighbourhoods.csv",
    "visualisations/neighbourhoods.geojson",
]


def find_latest_date() -> str:
    """Scrape the Inside Airbnb index page to find the most recent San Diego date."""
    req = Request(INDEX_URL, headers={"User-Agent": "sandiego-data-scraper/1.0"})
    with urlopen(req, timeout=60) as resp:
        html = resp.read().decode("utf-8", errors="replace")

    dates = re.findall(
        r"data\.insideairbnb\.com/united-states/ca/san-diego/(\d{4}-\d{2}-\d{2})/",
        html,
    )
    if not dates:
        raise RuntimeError("could not find any San Diego dates on the index page")

    latest = sorted(set(dates))[-1]
    return latest


def download(url: str, dest: Path) -> bool:
    """Download a single file. Returns True on success."""
    req = Request(url, headers={"User-Agent": "sandiego-data-scraper/1.0"})
    try:
        with urlopen(req, timeout=300) as resp:
            data = resp.read()
        dest.write_bytes(data)
        size_mb = len(data) / 1_048_576
        log.info("%s  %.1f MB", dest.name, size_mb)
        return True
    except Exception:
        log.warning("failed to download %s", url, exc_info=True)
        return False


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    log.info("finding latest scrape date for San Diego...")
    date = find_latest_date()
    log.info("latest scrape date: %s", date)

    failed: list[str] = []
    for rel_path in RELATIVE_PATHS:
        url = f"{BASE_URL}/{date}/{rel_path}"
        filename = rel_path.replace("/", "_")
        dest = DATA_DIR / filename
        if not download(url, dest):
            failed.append(filename)

    if failed:
        log.error("failed downloads: %s", ", ".join(sorted(failed)))
        return 1

    log.info("all downloads complete (date=%s)", date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
