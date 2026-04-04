"""ETL pipeline: read scraped CSVs, clean, and load into Postgres."""

import io
import logging
import os
import sys
import time
from pathlib import Path

import polars as pl
import psycopg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://sandiego:sandiego@localhost:5432/sandiego_data"
)


def copy_df_to_table(conn, df: pl.DataFrame, table: str) -> int:
    """Bulk load a polars DataFrame into a Postgres table using COPY."""
    buf = io.BytesIO()
    df.write_csv(buf)
    buf.seek(0)
    with conn.cursor() as cur:
        with cur.copy(f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true)") as copy:
            copy.write(buf.read())
    conn.commit()
    return df.shape[0]


def empty_to_null(df: pl.DataFrame) -> pl.DataFrame:
    """Replace empty strings with null across all string columns."""
    str_cols = [col for col, dtype in df.schema.items() if dtype == pl.String]
    return df.with_columns(
        pl.when(pl.col(col) == "").then(None).otherwise(pl.col(col)).alias(col)
        for col in str_cols
    )


def load_reference_tables(conn) -> None:
    """Load small reference/lookup tables."""
    # CFS call types
    path = ROOT / "police_nibrs_crime/data/pd_cfs_calltypes_datasd.csv"
    if path.exists():
        df = pl.read_csv(path).select(
            pl.col("call_type").cast(pl.String),
            pl.col("description"),
        ).unique(subset=["call_type"])
        df = empty_to_null(df)
        n = copy_df_to_table(conn, df, "cfs_call_types")
        log.info("cfs_call_types: %d rows", n)

    # Complaint types
    path = ROOT / "building_permits/data/complaint_types_datasd.csv"
    if path.exists():
        df = pl.read_csv(path, encoding="utf8-lossy").select(
            pl.col("Complaint Type ID").alias("complaint_type_id"),
            pl.col("Complaint Type").alias("complaint_type"),
        ).unique(subset=["complaint_type_id"])
        n = copy_df_to_table(conn, df, "complaint_types")
        log.info("complaint_types: %d rows", n)


def load_service_requests(conn) -> None:
    """Load Get It Done 311 data."""
    data_dir = ROOT / "get_it_done/data"
    overrides = {"case_age_days": pl.Float64}

    closed_files = sorted(data_dir.glob("get_it_done_requests_closed_*_datasd.csv"))
    open_file = data_dir / "get_it_done_requests_open_datasd.csv"
    all_files = [*closed_files, open_file]

    dfs = [pl.read_csv(f, infer_schema_length=10_000, schema_overrides=overrides) for f in all_files]
    df = pl.concat(dfs, how="diagonal_relaxed")
    df = empty_to_null(df)

    # Cast and clean
    df = df.with_columns(
        pl.col("lat").cast(pl.Float64, strict=False),
        pl.col("lng").cast(pl.Float64, strict=False),
        pl.col("date_requested").str.to_datetime("%Y-%m-%dT%H:%M:%S", strict=False),
        pl.col("date_closed").str.to_date("%Y-%m-%d", strict=False),
    )

    # Null out-of-bounds coords
    df = df.with_columns(
        pl.when((pl.col("lat") < 32.0) | (pl.col("lat") > 34.0) | (pl.col("lng") < -118.5) | (pl.col("lng") > -116.0))
        .then(None).otherwise(pl.col("lat")).alias("lat"),
        pl.when((pl.col("lat") < 32.0) | (pl.col("lat") > 34.0) | (pl.col("lng") < -118.5) | (pl.col("lng") > -116.0))
        .then(None).otherwise(pl.col("lng")).alias("lng"),
    )

    # Deduplicate
    df = df.unique(subset=["service_request_id"], keep="first")

    # Select columns matching the table schema
    df = df.select(
        "service_request_id", "service_request_parent_id", "sap_notification_number",
        "date_requested", "case_age_days", "case_record_type", "service_name",
        "service_name_detail", "date_closed", "status", "lat", "lng", "street_address",
        "zipcode", "council_district", "comm_plan_code", "comm_plan_name", "park_name",
        "case_origin", "referred", "iamfloc", "floc", "public_description",
    )

    n = copy_df_to_table(conn, df, "service_requests")
    log.info("service_requests: %d rows", n)


def load_crime_offenses(conn) -> None:
    """Load NIBRS crime data."""
    data_dir = ROOT / "police_nibrs_crime/data"
    overrides = {
        "case_number": pl.String, "beat": pl.String, "zip": pl.String,
        "latitude": pl.String, "longitude": pl.String, "service_area": pl.String,
        "division_number": pl.String, "geocode_score": pl.String,
    }
    files = sorted(f for f in data_dir.glob("pd_nibrs_*_datasd.csv") if "dictionary" not in f.name)
    df = pl.concat([pl.read_csv(f, infer_schema_length=10_000, schema_overrides=overrides) for f in files], how="diagonal_relaxed")
    df = empty_to_null(df)

    df = df.with_columns(
        pl.col("latitude").cast(pl.Float64, strict=False).alias("lat"),
        pl.col("longitude").cast(pl.Float64, strict=False).alias("lng"),
        pl.col("occured_on").str.to_date("%Y-%m-%d", strict=False),
        pl.col("approved_on").str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False),
    )

    df = df.select(
        "nibrs_uniq", "objectid", "case_number", "occured_on", "approved_on",
        "day_of_week", "month", "year", "code_section", "group_type", "ibr_category",
        "crime_against", "ibr_offense", "ibr_offense_description", "pd_offense_category",
        "violent_crime", "property_crime", "beat", "neighborhood", "service_area",
        "division_number", "division", "block_addr", "city", "state", "zip",
        "geocode_status", "geocode_score", "lat", "lng",
    )

    n = copy_df_to_table(conn, df, "crime_offenses")
    log.info("crime_offenses: %d rows", n)


def load_calls_for_service(conn) -> None:
    """Load police calls for service with column name normalization."""
    data_dir = ROOT / "police_nibrs_crime/data"
    files = sorted(
        f for f in data_dir.glob("pd_calls_for_service_*_datasd.csv")
        if "dictionary" not in f.name
    )
    frames = []
    for f in files:
        frame = pl.read_csv(f, infer_schema_length=10_000, schema_overrides={
            "beat": pl.String, "BEAT": pl.String,
            "priority": pl.String, "PRIORITY": pl.String,
            "address_number_primary": pl.String, "ADDRESS_NUMBER_PRIMARY": pl.String,
        })
        frame = frame.rename({col: col.lower() for col in frame.columns})
        frames.append(frame)

    df = pl.concat(frames, how="diagonal_relaxed")
    df = empty_to_null(df)

    df = df.with_columns(
        pl.col("date_time").str.to_datetime("%Y-%m-%d %H:%M:%S%.3f", strict=False),
    )

    df = df.select(
        "incident_num", "date_time", "day_of_week", "address_number_primary",
        "address_dir_primary", "address_road_primary", "address_sfx_primary",
        "address_dir_intersecting", "address_road_intersecting", "address_sfx_intersecting",
        "call_type", "disposition", "beat", "priority",
    )

    # Drop rows with null PK
    df = df.filter(pl.col("incident_num").is_not_null())
    df = df.unique(subset=["incident_num"], keep="first")

    n = copy_df_to_table(conn, df, "calls_for_service")
    log.info("calls_for_service: %d rows", n)


def load_traffic_collisions(conn) -> None:
    """Load traffic collision data."""
    path = ROOT / "police_nibrs_crime/data/pd_collisions_datasd.csv"
    df = pl.read_csv(path, infer_schema_length=10_000, schema_overrides={
        "report_id": pl.String, "police_beat": pl.String,
    })
    df = empty_to_null(df)
    df = df.with_columns(
        pl.col("date_time").str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False),
    )
    df = df.unique(subset=["report_id"], keep="first")
    n = copy_df_to_table(conn, df, "traffic_collisions")
    log.info("traffic_collisions: %d rows", n)


def load_building_permits(conn) -> None:
    """Load building permits (set1 + set2, active + closed)."""
    data_dir = ROOT / "building_permits/data"
    overrides = {
        "JOB_APN": pl.String, "JOB_BC_CODE": pl.String,
        "APPROVAL_FLOOR_AREA": pl.Float64, "APPROVAL_STORIES": pl.Float64,
        "APPROVAL_VALUATION": pl.Float64,
    }

    dfs = []
    for fname in ["permits_set1_active_datasd.csv", "permits_set1_closed_datasd.csv",
                   "permits_set2_active_datasd.csv", "permits_set2_closed_datasd.csv"]:
        df = pl.read_csv(data_dir / fname, infer_schema_length=10_000, schema_overrides=overrides)
        df = df.rename({c: c.upper() for c in df.columns})
        dfs.append(df)

    df = pl.concat(dfs, how="diagonal_relaxed")
    df = empty_to_null(df)

    df = df.with_columns(
        pl.col("LAT_JOB").cast(pl.Float64, strict=False).alias("lat"),
        pl.col("LNG_JOB").cast(pl.Float64, strict=False).alias("lng"),
    )

    # Parse date columns (some are date-only, some are datetime with .000)
    for col in ["DATE_PROJECT_CREATE", "DATE_PROJECT_COMPLETE", "DATE_APPROVAL_CREATE",
                "DATE_APPROVAL_ISSUE", "DATE_APPROVAL_CLOSE", "DATE_APPROVAL_EXPIRE"]:
        if col in df.columns:
            df = df.with_columns(pl.col(col).str.to_date("%Y-%m-%d", strict=False).alias(col))

    df = df.select(
        pl.col("APPROVAL_ID").alias("approval_id"),
        pl.col("PROJECT_ID").alias("project_id"),
        pl.col("JOB_ID").alias("job_id"),
        pl.col("DEVELOPMENT_ID").alias("development_id") if "DEVELOPMENT_ID" in df.columns else pl.lit(None).alias("development_id"),
        pl.col("PROJECT_TITLE").alias("project_title"),
        pl.col("PROJECT_SCOPE").alias("project_scope"),
        pl.col("APPROVAL_SCOPE").alias("approval_scope"),
        pl.col("APPROVAL_TYPE").alias("approval_type"),
        pl.col("PROJECT_TYPE").alias("project_type") if "PROJECT_TYPE" in df.columns else pl.lit(None).alias("project_type"),
        pl.col("PROJECT_STATUS").alias("project_status") if "PROJECT_STATUS" in df.columns else pl.lit(None).alias("project_status"),
        pl.col("PROJECT_PROCESSING_CODE").alias("project_processing_code") if "PROJECT_PROCESSING_CODE" in df.columns else pl.lit(None).alias("project_processing_code"),
        pl.col("APPROVAL_STATUS").alias("approval_status"),
        pl.col("JOB_BC_CODE").alias("job_bc_code"),
        pl.col("JOB_BC_CODE_DESCRIPTION").alias("job_bc_code_description") if "JOB_BC_CODE_DESCRIPTION" in df.columns else pl.lit(None).alias("job_bc_code_description"),
        pl.col("APPROVAL_PERMIT_HOLDER").alias("approval_permit_holder"),
        pl.col("ADDRESS_JOB").alias("address_job"),
        pl.col("JOB_APN").alias("job_apn"),
        pl.col("lat"),
        pl.col("lng"),
        pl.col("DATE_PROJECT_CREATE").alias("date_project_create"),
        pl.col("DATE_PROJECT_COMPLETE").alias("date_project_complete") if "DATE_PROJECT_COMPLETE" in df.columns else pl.lit(None).cast(pl.Date).alias("date_project_complete"),
        pl.col("DATE_APPROVAL_CREATE").alias("date_approval_create"),
        pl.col("DATE_APPROVAL_ISSUE").alias("date_approval_issue"),
        pl.col("DATE_APPROVAL_CLOSE").alias("date_approval_close"),
        pl.col("DATE_APPROVAL_EXPIRE").alias("date_approval_expire"),
        pl.col("APPROVAL_VALUATION").alias("approval_valuation"),
        pl.col("APPROVAL_STORIES").alias("approval_stories"),
        pl.col("APPROVAL_FLOOR_AREA").alias("approval_floor_area"),
    )

    df = df.filter(pl.col("approval_id").is_not_null())
    df = df.unique(subset=["approval_id"], keep="first")
    n = copy_df_to_table(conn, df, "building_permits")
    log.info("building_permits: %d rows", n)


def load_code_enforcement(conn) -> None:
    """Load code enforcement violations."""
    path = ROOT / "building_permits/data/code_enf_past_3_yr_datasd.csv"
    df = pl.read_csv(path, infer_schema_length=10_000)
    df = empty_to_null(df)

    df = df.with_columns(
        pl.col("lat").cast(pl.Float64, strict=False),
        pl.col("lng").cast(pl.Float64, strict=False),
        pl.col("date_open").str.to_date("%Y-%m-%d", strict=False),
        pl.col("date_closed").str.to_date("%Y-%m-%d", strict=False),
        pl.col("date_last_action_due").str.to_date("%Y-%m-%d", strict=False),
    )

    df = df.select(
        "case_id", "apn", "address_street", "case_source", "description",
        "date_open", "date_closed", "close_reason", "close_note", "lat", "lng",
        "workgroup", "investigator_name", "last_action", "date_last_action_due",
        "remedy_msg",
    )

    df = df.unique(subset=["case_id"], keep="first")
    n = copy_df_to_table(conn, df, "code_enforcement")
    log.info("code_enforcement: %d rows", n)


def load_businesses(conn) -> None:
    """Load business tax certificates."""
    data_dir = ROOT / "rental_properties/data"
    files = [
        "sd_businesses_active_datasd.csv",
        "sd_businesses_inactive_1990to2000_datasd.csv",
        "sd_businesses_inactive_2000to2010_datasd.csv",
        "sd_businesses_inactive_2010to2015_datasd.csv",
        "sd_businesses_inactive_2015tocurr_datasd.csv",
    ]
    dfs = [pl.read_csv(data_dir / f, infer_schema_length=10_000, schema_overrides={"address_no": pl.String}) for f in files]
    df = pl.concat(dfs, how="diagonal_relaxed")
    df = empty_to_null(df)

    df = df.with_columns(
        pl.col("lat").cast(pl.Float64, strict=False),
        pl.col("lng").cast(pl.Float64, strict=False),
        pl.col("council_district").cast(pl.String, strict=False),
        pl.col("account_key").cast(pl.String, strict=False),
    )

    for col in ["date_account_creation", "date_cert_expiration", "date_cert_effective", "date_business_start"]:
        if col in df.columns:
            df = df.with_columns(pl.col(col).str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False).alias(col))

    df = df.select(
        "account_key", "account_status", "date_account_creation", "date_cert_expiration",
        "date_cert_effective", "business_owner_name", "ownership_type", "date_business_start",
        "dba_name", "naics_sector", "naics_code", "naics_description", "address_no",
        "address_pd", "address_road", "address_sfx", "address_city", "address_state",
        "address_zip", "address_suite", "council_district", "lat", "lng",
    )

    n = copy_df_to_table(conn, df, "businesses")
    log.info("businesses: %d rows", n)


def load_stro_licenses(conn) -> None:
    """Load short-term rental licenses."""
    path = ROOT / "rental_properties/data/stro_licenses_datasd.csv"
    df = pl.read_csv(path, infer_schema_length=10_000)
    df = empty_to_null(df)

    df = df.with_columns(
        pl.col("latitude").cast(pl.Float64, strict=False).alias("lat"),
        pl.col("longitude").cast(pl.Float64, strict=False).alias("lng"),
        pl.col("date_expiration").str.to_date("%Y-%m-%d", strict=False),
        pl.col("council_district").cast(pl.String, strict=False),
    )

    df = df.select(
        "license_id", "address", "street_number", "street_name", "street_type",
        "unit_number", "city", "state", "zip", "tier", "community_planning_area",
        "date_expiration", "rtax_no", "tot_no", "lat", "lng",
        pl.col("local_contact_contact_name").alias("local_contact_name"),
        "local_contact_phone", "host_contact_name", "council_district",
    )

    df = df.unique(subset=["license_id"], keep="first")
    n = copy_df_to_table(conn, df, "stro_licenses")
    log.info("stro_licenses: %d rows", n)


def load_tot_establishments(conn) -> None:
    """Load TOT establishments."""
    path = ROOT / "rental_properties/data/tot_establishments_datasd.csv"
    df = pl.read_csv(path, infer_schema_length=10_000)
    df = empty_to_null(df)

    df = df.with_columns(
        pl.col("certificate_no").cast(pl.String, strict=False),
        pl.col("status").cast(pl.String, strict=False),
        pl.col("certificate_type").cast(pl.String, strict=False),
        pl.col("date_created").str.to_date("%Y-%m-%d", strict=False),
    )

    df = df.unique(subset=["certificate_no"], keep="first")
    n = copy_df_to_table(conn, df, "tot_establishments")
    log.info("tot_establishments: %d rows", n)


def load_city_properties(conn) -> None:
    """Load city-owned property details."""
    path = ROOT / "rental_properties/data/city_property_details_datasd.csv"
    df = pl.read_csv(path, infer_schema_length=10_000, schema_overrides={
        "land_cost": pl.String, "building_cost": pl.String, "closing_cost": pl.String,
    })
    df = empty_to_null(df)

    df = df.select(
        "site_code", "file_code", "grantor", "month_acquired", "year_acquired",
        "purchase_fund", "land_cost", "building_cost", "closing_cost", "site_name",
        "managing_group", "managing_dept", "designated_use", "site_acres", "file_acres",
        "original_acres",
    )

    n = copy_df_to_table(conn, df, "city_properties")
    log.info("city_properties: %d rows", n)


def load_city_leases(conn) -> None:
    """Load city property leases."""
    path = ROOT / "rental_properties/data/city_property_leases_datasd.csv"
    df = pl.read_csv(path, infer_schema_length=10_000, schema_overrides={
        "cost_line_amt_USD": pl.String,
    })
    df = empty_to_null(df)

    df = df.rename({"lessee_DBA": "lessee_dba", "cost_line_amt_USD": "cost_line_amt_usd"})

    date_cols = ["date_effective", "date_sched_termination"]
    for col in date_cols:
        if col in df.columns and df[col].dtype == pl.String:
            df = df.with_columns(pl.col(col).str.to_date("%Y-%m-%d", strict=False).alias(col))

    n = copy_df_to_table(conn, df, "city_leases")
    log.info("city_leases: %d rows", n)


def load_parcels(conn) -> None:
    """Load SANDAG parcel data from parquet."""
    path = ROOT / "sandag/data/parcels.parquet"
    if not path.exists():
        log.warning("parcels.parquet not found, skipping")
        return
    df = pl.read_parquet(path)
    df = df.select(
        pl.col("APN").alias("apn"),
        pl.col("PARCELID").alias("parcelid"),
        pl.col("SITUS_JURI").alias("situs_juri"),
        pl.col("SITUS_ADDR").alias("situs_addr"),
        pl.col("SITUS_PRE_").alias("situs_pre"),
        pl.col("SITUS_STRE").alias("situs_stre"),
        pl.col("SITUS_SUFF").alias("situs_suff"),
        pl.col("SITUS_FRAC").alias("situs_frac"),
        pl.col("SITUS_SUIT").alias("situs_suit"),
        pl.col("SITUS_COMM").alias("situs_comm"),
        pl.col("SITUS_ZIP").alias("situs_zip"),
        pl.col("ASR_LAND").alias("asr_land"),
        pl.col("ASR_IMPR").alias("asr_impr"),
        pl.col("ASR_TOTAL").alias("asr_total"),
        pl.col("ACREAGE").alias("acreage"),
        pl.col("OWNEROCC").alias("ownerocc"),
        pl.col("ASR_LANDUS").alias("asr_landus"),
        pl.col("UNITQTY").alias("unitqty"),
        pl.col("TOTAL_LVG_").alias("total_lvg"),
        pl.col("BEDROOMS").alias("bedrooms"),
        pl.col("BATHS").alias("baths"),
        pl.col("X_COORD").alias("x_coord"),
        pl.col("Y_COORD").alias("y_coord"),
    )
    df = empty_to_null(df)
    n = copy_df_to_table(conn, df, "parcels")
    log.info("parcels: %d rows", n)


def load_pmc_properties(conn) -> None:
    """Load PMC-managed properties from enriched CSV."""
    path = ROOT / "apartments_com/data/pmc_properties_enriched.csv"
    if not path.exists():
        log.warning("pmc_properties_enriched.csv not found, skipping")
        return
    df = pl.read_csv(path, infer_schema_length=10000)
    cols = [c for c in [
        "pmc_name", "name", "street_address", "city", "zip5",
        "source", "APN", "total_units", "assessed_value",
        "landuse_label", "SITUS_COMM", "confidence",
    ] if c in df.columns]
    df = df.select(cols)
    rename_map = {
        "APN": "apn",
        "SITUS_COMM": "situs_comm",
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.rename({old: new})
    df = empty_to_null(df)
    n = copy_df_to_table(conn, df, "pmc_properties")
    log.info("pmc_properties: %d rows", n)


def load_airbnb_listings(conn) -> None:
    """Load Inside Airbnb listings."""
    path = ROOT / "airbnb/data/visualisations_listings.csv"
    if not path.exists():
        log.warning("airbnb listings not found, skipping")
        return
    df = pl.read_csv(path, infer_schema_length=10000)
    df = df.with_columns(
        pl.col("latitude").cast(pl.Float64, strict=False).alias("lat"),
        pl.col("longitude").cast(pl.Float64, strict=False).alias("lng"),
        pl.col("last_review").str.to_date("%Y-%m-%d", strict=False),
    )
    df = df.select(
        "id", "name", "host_id", "host_name", "neighbourhood_group",
        "neighbourhood", "lat", "lng", "room_type", "price",
        "minimum_nights", "number_of_reviews", "last_review",
        "reviews_per_month", "calculated_host_listings_count",
        "availability_365", "number_of_reviews_ltm", "license",
    )
    df = empty_to_null(df)
    df = df.unique(subset=["id"], keep="first")
    n = copy_df_to_table(conn, df, "airbnb_listings")
    log.info("airbnb_listings: %d rows", n)


def load_dre_licensees(conn) -> None:
    """Load California DRE licensee data."""
    path = ROOT / "dre_licensees/data/CurrList.csv"
    if not path.exists():
        log.warning("DRE CurrList.csv not found, skipping")
        return
    df = pl.read_csv(path, infer_schema_length=10000, encoding="utf8-lossy")
    df = df.select(
        "multiple_license_ind", "lastname_primary", "firstname_secondary",
        "name_suffix", "lic_number", "lic_type", "lic_status",
        "lic_effective_date", "lic_expiration_date", "original_date_of_license",
        "related_lic_number", "related_lastname_primary",
        "related_firstname_secondary", "related_name_suffix", "related_lic_type",
        "address_1", "address_2", "city", "state", "zip_code",
        "county_name", "restricted_flag",
    )
    df = empty_to_null(df)
    n = copy_df_to_table(conn, df, "dre_licensees")
    log.info("dre_licensees: %d rows", n)


def main() -> int:
    start = time.time()
    log.info("connecting to %s", DATABASE_URL.split("@")[-1])

    with psycopg.connect(DATABASE_URL) as conn:
        # Run schema
        schema_path = Path(__file__).parent / "schema.sql"
        schema_sql = schema_path.read_text()

        # Split on the DO $$ block: run DDL first, then post-load block after data
        parts = schema_sql.split("-- Post-load:")
        ddl = parts[0]
        post_load = "-- Post-load:" + parts[1] if len(parts) > 1 else ""

        log.info("creating tables...")
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()

        # Load data
        load_reference_tables(conn)
        load_service_requests(conn)
        load_crime_offenses(conn)
        load_calls_for_service(conn)
        load_traffic_collisions(conn)
        load_building_permits(conn)
        load_code_enforcement(conn)
        load_businesses(conn)
        load_stro_licenses(conn)
        load_tot_establishments(conn)
        load_city_properties(conn)
        load_city_leases(conn)
        load_parcels(conn)
        load_pmc_properties(conn)
        load_airbnb_listings(conn)
        load_dre_licensees(conn)

        # Post-load: geometry columns and indexes
        if post_load:
            log.info("building geometry columns and spatial indexes...")
            with conn.cursor() as cur:
                cur.execute(post_load)
            conn.commit()

        # Print summary
        log.info("verifying row counts...")
        with conn.cursor() as cur:
            for table in [
                "cfs_call_types", "complaint_types", "service_requests",
                "crime_offenses", "calls_for_service", "traffic_collisions",
                "building_permits", "code_enforcement", "businesses",
                "stro_licenses", "tot_establishments", "city_properties", "city_leases",
                "parcels", "pmc_properties", "airbnb_listings", "dre_licensees",
            ]:
                cur.execute(f"SELECT count(*) FROM {table}")
                count = cur.fetchone()[0]
                log.info("  %-25s %10d rows", table, count)

    elapsed = time.time() - start
    log.info("ETL complete in %.1f seconds", elapsed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
