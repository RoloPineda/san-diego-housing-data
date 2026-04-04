"""Scrape Greystar's San Diego property listings directly from greystar.com.

Greystar's site is a Next.js app that embeds all property data as JSON in
the __NEXT_DATA__ script tag. One HTTP request returns all 114 properties
with names, addresses, and pricing. No pagination or bot detection needed.

Output: data/greystar_direct.csv
"""

import csv
import json
import logging
import re
import sys
import time
from pathlib import Path
from urllib.request import urlopen, Request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
OUTPUT_FILE = DATA_DIR / "greystar_direct.csv"
URL = "https://www.greystar.com/homes-to-rent/us/ca/san-diego-metro"


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    log.info("fetching %s", URL)
    req = Request(URL, headers={
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        ),
    })
    with urlopen(req, timeout=30) as resp:
        html = resp.read().decode("utf-8")

    log.info("received %d bytes", len(html))

    m = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
    )
    if not m:
        log.error("__NEXT_DATA__ not found in page")
        return 1

    data = json.loads(m.group(1))
    component_props = data["props"]["pageProps"]["componentProps"]

    properties = []
    for uid, val in component_props.items():
        if "properties" in val:
            properties = val["properties"]
            break

    if not properties:
        log.error("no properties found in __NEXT_DATA__")
        return 1

    log.info("found %d properties in __NEXT_DATA__", len(properties))

    rows = []
    for p in properties:
        af = p.get("additionalFields", {})
        rows.append({
            "name": p.get("ec_brand", ""),
            "address": af.get("address", ""),
            "city": af.get("city", "").strip(),
            "state": af.get("state", ""),
            "zip": af.get("postal_code", ""),
            "price": p.get("ec_price", ""),
            "source_url": p.get("clickUri", ""),
        })

    fields = ["name", "address", "city", "state", "zip", "price", "source_url"]
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    log.info("wrote %d properties to %s", len(rows), OUTPUT_FILE)

    # Quick summary
    cities = {}
    for r in rows:
        cities[r["city"]] = cities.get(r["city"], 0) + 1
    log.info("by city:")
    for city, count in sorted(cities.items(), key=lambda x: -x[1]):
        log.info("  %-25s %d", city, count)

    return 0


if __name__ == "__main__":
    sys.exit(main())
