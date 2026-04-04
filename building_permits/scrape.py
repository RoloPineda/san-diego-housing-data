"""Download building permits and code enforcement data from the San Diego portal."""

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

FILES: dict[str, str] = {
    # Code enforcement violations (2015-2018)
    "code_enf_past_3_yr_datasd.csv": (
        f"{BASE}/code_enforcement_violations/code_enf_past_3_yr_datasd.csv"
    ),
    "code_enforcement_dictionary_datasd.csv": (
        f"{BASE}/code_enforcement_violations/code_enforcement_dictionary_datasd.csv"
    ),
    "complaint_types_datasd.csv": (
        f"{BASE}/code_enforcement_violations/complaint_types_datasd.csv"
    ),
    "code_enf_remedies_datasd.csv": (
        f"{BASE}/code_enforcement_violations/code_enf_remedies_datasd.csv"
    ),
    # Development permits set 1 (legacy, pre-2018)
    "permits_set1_active_datasd.csv": (
        f"{BASE}/development_permits_set1/permits_set1_active_datasd.csv"
    ),
    "permits_set1_closed_datasd.csv": (
        f"{BASE}/development_permits_set1/permits_set1_closed_datasd.csv"
    ),
    "permits_set1_datasd_dict.csv": (
        f"{BASE}/development_permits_set1/permits_set1_datasd_dict.csv"
    ),
    # Development permits set 2 (current, 2018+)
    "permits_set2_active_datasd.csv": (
        f"{BASE}/development_permits_set2/permits_set2_active_datasd.csv"
    ),
    "permits_set2_closed_datasd.csv": (
        f"{BASE}/development_permits_set2/permits_set2_closed_datasd.csv"
    ),
    "permits_set2_datasd_dict.csv": (
        f"{BASE}/development_permits_set2/permits_set2_datasd_dict.csv"
    ),
}


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
