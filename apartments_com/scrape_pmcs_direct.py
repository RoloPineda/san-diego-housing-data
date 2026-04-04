"""Scrape property listings directly from PMC company websites.

Supplements the apartments.com data by scraping each company's own site.
Uses Playwright Firefox for JS-rendered sites and curl_cffi for static ones.

Output: data/pmc_direct_properties.csv
"""

import csv
import json
import logging
import re
import sys
import time
from pathlib import Path

from curl_cffi import requests as curl_requests
from playwright.sync_api import sync_playwright

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
OUTPUT_FILE = DATA_DIR / "pmc_direct_properties.csv"

SD_CITIES = {
    "San Diego", "Chula Vista", "Escondido", "Oceanside", "Carlsbad",
    "La Mesa", "El Cajon", "Vista", "San Marcos", "Santee", "Encinitas",
    "National City", "Imperial Beach", "Poway", "Solana Beach",
    "Spring Valley", "Lemon Grove", "Coronado", "Del Mar",
}


def scrape_conam(page) -> list[dict]:
    """CONAM: server-rendered HTML with address blocks."""
    page.goto("https://www.conam.com/properties/", wait_until="networkidle", timeout=30000)
    html = page.content()

    properties = []
    # Pattern: address line followed by "City, California ZIP"
    blocks = re.findall(
        r'>\s*(\d{3,5}\s+[\w\s.\']+?)\s*<.*?'
        r'>\s*([\w\s]+?)\s*<.*?'
        r'>\s*,\s*<.*?'
        r'>\s*California\s*<.*?'
        r'>\s*(\d{5})\s*<',
        html, re.DOTALL,
    )
    seen = set()
    for addr, city, zipcode in blocks:
        addr = addr.strip()
        city = city.strip()
        if city not in SD_CITIES:
            continue
        key = f"{addr}|{zipcode}"
        if key in seen:
            continue
        seen.add(key)
        properties.append({
            "pmc_name": "CONAM Management Corporation",
            "name": "",
            "address": addr,
            "city": city,
            "zip": zipcode,
        })

    # Try to get property names from nearby headings
    for prop in properties:
        addr_idx = html.find(prop["address"])
        if addr_idx > 0:
            ctx = html[max(0, addr_idx - 500):addr_idx]
            names = re.findall(r'<h[23][^>]*>([^<]+)</h', ctx)
            if names:
                prop["name"] = names[-1].strip()

    return properties


def scrape_irvine(page) -> list[dict]:
    """Irvine Company: Angular app, need to extract from rendered content."""
    page.goto(
        "https://www.irvinecompanyapartments.com/locations/san-diego.html",
        wait_until="networkidle", timeout=30000,
    )
    # Wait for Angular to render
    time.sleep(3)
    html = page.content()

    properties = []
    # Look for property cards with links to /apartments/
    links = re.findall(r'href="(/apartments/[^"]+)"', html)
    unique_links = sorted(set(links))

    for link in unique_links:
        # Extract community name from URL slug
        slug = link.rstrip("/").split("/")[-1]
        name = slug.replace("-", " ").title()
        properties.append({
            "pmc_name": "Irvine Company",
            "name": name,
            "address": "",
            "city": "",
            "zip": "",
            "url": f"https://www.irvinecompanyapartments.com{link}",
        })

    # If no links found, try text extraction
    if not properties:
        # Look for community names in the rendered HTML
        cards = re.findall(
            r'class="[^"]*card[^"]*"[^>]*>.*?<(?:h[23]|a)[^>]*>([^<]+)<',
            html, re.DOTALL,
        )
        for name in cards:
            name = name.strip()
            if len(name) > 3 and name[0].isupper():
                properties.append({
                    "pmc_name": "Irvine Company",
                    "name": name,
                    "address": "",
                    "city": "San Diego",
                    "zip": "",
                })

    return properties


def scrape_ra_snyder(page) -> list[dict]:
    """R.A. Snyder: check their properties page."""
    page.goto("https://www.rasnyder.com/properties", wait_until="networkidle", timeout=30000)
    time.sleep(2)
    html = page.content()

    properties = []
    # Look for property cards with addresses
    blocks = re.findall(
        r'(\d{3,5}\s+[\w\s.\']+?)\s*(?:,|\n)\s*([\w\s]+?)\s*,\s*CA\s+(\d{5})',
        html,
    )
    seen = set()
    for addr, city, zipcode in blocks:
        addr = addr.strip()
        city = city.strip()
        if city not in SD_CITIES:
            continue
        key = f"{addr}|{zipcode}"
        if key in seen:
            continue
        seen.add(key)
        properties.append({
            "pmc_name": "R.A. Snyder Properties, Inc.",
            "name": "",
            "address": addr,
            "city": city,
            "zip": zipcode,
        })

    return properties


def scrape_sunrise(page) -> list[dict]:
    """Sunrise Management: check their listings page."""
    page.goto(
        "https://www.sunrisemanagement.com/apartments-for-rent",
        wait_until="networkidle", timeout=30000,
    )
    time.sleep(2)
    html = page.content()

    properties = []
    blocks = re.findall(
        r'(\d{3,5}\s+[\w\s.\']+?)\s*(?:,|\n)\s*([\w\s]+?)\s*,\s*CA\s+(\d{5})',
        html,
    )
    seen = set()
    for addr, city, zipcode in blocks:
        addr = addr.strip()
        city = city.strip()
        if city not in SD_CITIES:
            continue
        key = f"{addr}|{zipcode}"
        if key in seen:
            continue
        seen.add(key)
        properties.append({
            "pmc_name": "Sunrise Management",
            "name": "",
            "address": addr,
            "city": city,
            "zip": zipcode,
        })

    return properties


def scrape_torrey_pines(page) -> list[dict]:
    """Torrey Pines PM: check their properties page."""
    page.goto(
        "https://www.torreypinespm.com/properties",
        wait_until="networkidle", timeout=30000,
    )
    time.sleep(2)
    html = page.content()

    properties = []
    blocks = re.findall(
        r'(\d{3,5}\s+[\w\s.\']+?)\s*(?:,|\n)\s*([\w\s]+?)\s*,\s*CA\s+(\d{5})',
        html,
    )
    seen = set()
    for addr, city, zipcode in blocks:
        addr = addr.strip()
        city = city.strip()
        if city not in SD_CITIES:
            continue
        key = f"{addr}|{zipcode}"
        if key in seen:
            continue
        seen.add(key)
        properties.append({
            "pmc_name": "Torrey Pines Property Management",
            "name": "",
            "address": addr,
            "city": city,
            "zip": zipcode,
        })

    return properties


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    pw = sync_playwright().start()
    browser = pw.firefox.launch(headless=True)
    page = browser.new_page()

    all_properties: list[dict] = []

    scrapers = [
        ("CONAM", scrape_conam),
        ("Irvine Company", scrape_irvine),
        ("R.A. Snyder", scrape_ra_snyder),
        ("Sunrise Management", scrape_sunrise),
        ("Torrey Pines PM", scrape_torrey_pines),
    ]

    for name, scraper in scrapers:
        log.info("scraping %s...", name)
        try:
            props = scraper(page)
            all_properties.extend(props)
            log.info("  %s: %d properties found", name, len(props))
        except Exception:
            log.error("  %s: failed", name, exc_info=True)
        time.sleep(2)

    # Also load the Greystar direct scrape if it exists
    greystar_file = DATA_DIR / "greystar_direct.csv"
    if greystar_file.exists():
        import polars as pl
        gdf = pl.read_csv(greystar_file)
        for row in gdf.to_dicts():
            city = row.get("city", "").strip()
            if city in SD_CITIES:
                all_properties.append({
                    "pmc_name": "Greystar",
                    "name": row.get("name", ""),
                    "address": row.get("address", ""),
                    "city": city,
                    "zip": str(row.get("zip", "")),
                })
        log.info("  Greystar (from greystar_direct.csv): %d SD-area properties",
                 sum(1 for p in all_properties if p["pmc_name"] == "Greystar"))

    browser.close()
    pw.stop()

    # Write CSV
    fields = ["pmc_name", "name", "address", "city", "zip"]
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_properties)

    log.info("wrote %d properties to %s", len(all_properties), OUTPUT_FILE)

    # Summary
    from collections import Counter
    by_pmc = Counter(p["pmc_name"] for p in all_properties)
    log.info("by PMC:")
    for pmc, count in by_pmc.most_common():
        log.info("  %-45s %d", pmc, count)

    return 0


if __name__ == "__main__":
    sys.exit(main())
