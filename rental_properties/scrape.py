"""Download rental property, business tax, STRO, TOT, and city property data from the San Diego portal."""

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
    # Business tax certificates
    "sd_businesses_active_datasd.csv": (
        f"{BASE}/business_tax_certificates/sd_businesses_active_datasd.csv"
    ),
    "sd_businesses_inactive_2015tocurr_datasd.csv": (
        f"{BASE}/business_tax_certificates/sd_businesses_inactive_2015tocurr_datasd.csv"
    ),
    "sd_businesses_inactive_2010to2015_datasd.csv": (
        f"{BASE}/business_tax_certificates/sd_businesses_inactive_2010to2015_datasd.csv"
    ),
    "sd_businesses_inactive_2000to2010_datasd.csv": (
        f"{BASE}/business_tax_certificates/sd_businesses_inactive_2000to2010_datasd.csv"
    ),
    "sd_businesses_inactive_1990to2000_datasd.csv": (
        f"{BASE}/business_tax_certificates/sd_businesses_inactive_1990to2000_datasd.csv"
    ),
    "sd_businesses_dictionary_datasd.csv": (
        f"{BASE}/business_tax_certificates/sd_businesses_dictionary_datasd.csv"
    ),
    # STRO (Short-Term Residential Occupancy) licenses
    "stro_licenses_datasd.csv": (
        f"{BASE}/stro_licenses/stro_licenses_datasd.csv"
    ),
    "stro_licenses_dictionary_datasd.csv": (
        f"{BASE}/stro_licenses/stro_licenses_dictionary_datasd.csv"
    ),
    "stro_licenses_tier_definitions_datasd.csv": (
        f"{BASE}/stro_licenses/stro_licenses_tier_definitions_datasd.csv"
    ),
    # TOT (Transient Occupancy Tax) establishments
    "tot_establishments_datasd.csv": (
        f"{BASE}/tot_establishments/tot_establishments_datasd.csv"
    ),
    "tot_establishments_datasd_dict.csv": (
        f"{BASE}/tot_establishments/tot_establishments_datasd_dict.csv"
    ),
    # City-owned properties
    "city_property_details_datasd.csv": (
        f"{BASE}/city_owned_properties/city_property_details_datasd.csv"
    ),
    "city_property_details_dictionary_datasd.csv": (
        f"{BASE}/city_owned_properties/city_property_details_dictionary_datasd.csv"
    ),
    "city_property_leases_datasd.csv": (
        f"{BASE}/city_owned_properties_leases/city_property_leases_datasd.csv"
    ),
    "city_property_leases_dictionary_datasd.csv": (
        f"{BASE}/city_owned_properties_leases/city_property_leases_dictionary_datasd.csv"
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
