"""Download police NIBRS, calls for service, and traffic collision data from the San Diego portal."""

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

FILES: dict[str, str] = {}

# NIBRS crime offenses (2020-2026)
for year in range(2020, 2027):
    fname = f"pd_nibrs_{year}_datasd.csv"
    FILES[fname] = f"{BASE}/police_nibrs/{fname}"
FILES["pd_nibrs_dictionary.csv"] = f"{BASE}/police_nibrs/pd_nibrs_dictionary.csv"

# Police calls for service (2015-2026)
for year in range(2015, 2027):
    fname = f"pd_calls_for_service_{year}_datasd.csv"
    FILES[fname] = f"{BASE}/police_calls_for_service/{fname}"
FILES["pd_calls_for_service_dictionary_datasd.csv"] = (
    f"{BASE}/police_calls_for_service/pd_calls_for_service_dictionary_datasd.csv"
)
FILES["pd_cfs_calltypes_datasd.csv"] = (
    f"{BASE}/police_calls_for_service/pd_cfs_calltypes_datasd.csv"
)

# Traffic collisions
FILES["pd_collisions_datasd.csv"] = (
    f"{BASE}/traffic_collisions/pd_collisions_datasd.csv"
)
FILES["pd_collisions_dictionary_datasd.csv"] = (
    f"{BASE}/traffic_collisions/pd_collisions_dictionary_datasd.csv"
)


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