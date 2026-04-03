"""Download all Get It Done datasets from the San Diego open data portal."""

import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.request import urlopen, Request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

BASE = "https://seshat.datasd.org"
DATA_DIR = Path(__file__).parent / "data"

# Year range for closed request files (launched May 2016).
CLOSED_YEAR_START = 2016
CLOSED_YEAR_END = 2026

FILES: dict[str, str] = {
    # Main reports: open requests
    "get_it_done_requests_open_datasd.csv": (
        f"{BASE}/get_it_done_reports/get_it_done_requests_open_datasd.csv"
    ),
    # Data dictionary
    "get_it_done_requests_dictionary_datasd.csv": (
        f"{BASE}/get_it_done_reports/get_it_done_requests_dictionary_datasd.csv"
    ),
    # Problem-type specific datasets
    "get_it_done_72_hour_violation_requests_datasd.csv": (
        f"{BASE}/get_it_done_parking_violations"
        "/get_it_done_72_hour_violation_requests_datasd.csv"
    ),
    "get_it_done_graffiti_requests_datasd.csv": (
        f"{BASE}/get_it_done_graffiti/get_it_done_graffiti_requests_datasd.csv"
    ),
    "get_it_done_illegal_dumping_requests_datasd.csv": (
        f"{BASE}/get_it_done_illegal_dumping"
        "/get_it_done_illegal_dumping_requests_datasd.csv"
    ),
    "get_it_done_pothole_requests_datasd.csv": (
        f"{BASE}/get_it_done_potholes/get_it_done_pothole_requests_datasd.csv"
    ),
}

# Closed requests, one file per year.
for _year in range(CLOSED_YEAR_START, CLOSED_YEAR_END + 1):
    _fname = f"get_it_done_requests_closed_{_year}_datasd.csv"
    FILES[_fname] = f"{BASE}/get_it_done_reports/{_fname}"


def download(name: str, url: str) -> tuple[str, bool]:
    """Download a single file. Returns (name, success)."""
    dest = DATA_DIR / name
    req = Request(url, headers={"User-Agent": "sandiego-data-scraper/1.0"})
    try:
        with urlopen(req, timeout=300) as resp:
            data = resp.read()
        dest.write_bytes(data)
        size_mb = len(data) / 1_048_576
        log.info("%s  %.1f MB", name, size_mb)
        return name, True
    except Exception:
        log.warning("failed to download %s from %s", name, url, exc_info=True)
        return name, False


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log.info("downloading %d files to %s", len(FILES), DATA_DIR)

    failed: list[str] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(download, name, url): name
            for name, url in FILES.items()
        }
        for future in as_completed(futures):
            name, ok = future.result()
            if not ok:
                failed.append(name)

    if failed:
        log.error("failed downloads: %s", ", ".join(sorted(failed)))
        return 1

    log.info("all downloads complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())