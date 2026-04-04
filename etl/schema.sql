CREATE EXTENSION IF NOT EXISTS postgis;

-- Reference tables

DROP TABLE IF EXISTS cfs_call_types CASCADE;
CREATE TABLE cfs_call_types (
    call_type TEXT PRIMARY KEY,
    description TEXT
);

DROP TABLE IF EXISTS complaint_types CASCADE;
CREATE TABLE complaint_types (
    complaint_type_id INTEGER PRIMARY KEY,
    complaint_type TEXT
);

-- Get It Done service requests

DROP TABLE IF EXISTS service_requests CASCADE;
CREATE TABLE service_requests (
    service_request_id BIGINT PRIMARY KEY,
    service_request_parent_id TEXT,
    sap_notification_number TEXT,
    date_requested TIMESTAMP,
    case_age_days REAL,
    case_record_type TEXT,
    service_name TEXT,
    service_name_detail TEXT,
    date_closed DATE,
    status TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    street_address TEXT,
    zipcode TEXT,
    council_district TEXT,
    comm_plan_code TEXT,
    comm_plan_name TEXT,
    park_name TEXT,
    case_origin TEXT,
    referred TEXT,
    iamfloc TEXT,
    floc TEXT,
    public_description TEXT
);

-- NIBRS crime offenses

DROP TABLE IF EXISTS crime_offenses CASCADE;
CREATE TABLE crime_offenses (
    nibrs_uniq TEXT PRIMARY KEY,
    objectid INTEGER,
    case_number TEXT,
    occured_on DATE,
    approved_on TIMESTAMP,
    day_of_week INTEGER,
    month INTEGER,
    year INTEGER,
    code_section TEXT,
    group_type TEXT,
    ibr_category TEXT,
    crime_against TEXT,
    ibr_offense TEXT,
    ibr_offense_description TEXT,
    pd_offense_category TEXT,
    violent_crime INTEGER,
    property_crime INTEGER,
    beat TEXT,
    neighborhood TEXT,
    service_area TEXT,
    division_number TEXT,
    division TEXT,
    block_addr TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    geocode_status TEXT,
    geocode_score TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION
);


DROP TABLE IF EXISTS calls_for_service CASCADE;
CREATE TABLE calls_for_service (
    incident_num TEXT PRIMARY KEY,
    date_time TIMESTAMP,
    day_of_week INTEGER,
    address_number_primary TEXT,
    address_dir_primary TEXT,
    address_road_primary TEXT,
    address_sfx_primary TEXT,
    address_dir_intersecting TEXT,
    address_road_intersecting TEXT,
    address_sfx_intersecting TEXT,
    call_type TEXT,
    disposition TEXT,
    beat TEXT,
    priority TEXT
);

DROP TABLE IF EXISTS traffic_collisions CASCADE;
CREATE TABLE traffic_collisions (
    report_id TEXT PRIMARY KEY,
    date_time TIMESTAMP,
    police_beat TEXT,
    address_no_primary INTEGER,
    address_pd_primary TEXT,
    address_road_primary TEXT,
    address_sfx_primary TEXT,
    address_pd_intersecting TEXT,
    address_name_intersecting TEXT,
    address_sfx_intersecting TEXT,
    violation_section TEXT,
    violation_type TEXT,
    charge_desc TEXT,
    injured INTEGER,
    killed INTEGER,
    hit_run_lvl TEXT
);

-- Building permits (set1 + set2 combined)

DROP TABLE IF EXISTS building_permits CASCADE;
CREATE TABLE building_permits (
    approval_id TEXT PRIMARY KEY,
    project_id TEXT,
    job_id TEXT,
    development_id TEXT,
    project_title TEXT,
    project_scope TEXT,
    approval_scope TEXT,
    approval_type TEXT,
    project_type TEXT,
    project_status TEXT,
    project_processing_code TEXT,
    approval_status TEXT,
    job_bc_code TEXT,
    job_bc_code_description TEXT,
    approval_permit_holder TEXT,
    address_job TEXT,
    job_apn TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    date_project_create DATE,
    date_project_complete DATE,
    date_approval_create DATE,
    date_approval_issue DATE,
    date_approval_close DATE,
    date_approval_expire DATE,
    approval_valuation DOUBLE PRECISION,
    approval_stories DOUBLE PRECISION,
    approval_floor_area DOUBLE PRECISION
);

-- Code enforcement

DROP TABLE IF EXISTS code_enforcement CASCADE;
CREATE TABLE code_enforcement (
    case_id INTEGER PRIMARY KEY,
    apn TEXT,
    address_street TEXT,
    case_source TEXT,
    description TEXT,
    date_open DATE,
    date_closed DATE,
    close_reason TEXT,
    close_note TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    workgroup TEXT,
    investigator_name TEXT,
    last_action TEXT,
    date_last_action_due DATE,
    remedy_msg TEXT
);

-- Business tax certificates

DROP TABLE IF EXISTS businesses CASCADE;
CREATE TABLE businesses (
    account_key TEXT,
    account_status TEXT,
    date_account_creation TIMESTAMP,
    date_cert_expiration TIMESTAMP,
    date_cert_effective TIMESTAMP,
    business_owner_name TEXT,
    ownership_type TEXT,
    date_business_start TIMESTAMP,
    dba_name TEXT,
    naics_sector TEXT,
    naics_code INTEGER,
    naics_description TEXT,
    address_no TEXT,
    address_pd TEXT,
    address_road TEXT,
    address_sfx TEXT,
    address_city TEXT,
    address_state TEXT,
    address_zip TEXT,
    address_suite TEXT,
    council_district TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION
);

-- STRO licenses

DROP TABLE IF EXISTS stro_licenses CASCADE;
CREATE TABLE stro_licenses (
    license_id TEXT PRIMARY KEY,
    address TEXT,
    street_number TEXT,
    street_name TEXT,
    street_type TEXT,
    unit_number TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    tier TEXT,
    community_planning_area TEXT,
    date_expiration DATE,
    rtax_no TEXT,
    tot_no TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    local_contact_name TEXT,
    local_contact_phone TEXT,
    host_contact_name TEXT,
    council_district TEXT
);

-- TOT establishments

DROP TABLE IF EXISTS tot_establishments CASCADE;
CREATE TABLE tot_establishments (
    certificate_no TEXT PRIMARY KEY,
    status TEXT,
    certificate_type TEXT,
    property_address TEXT,
    property_city TEXT,
    property_state TEXT,
    property_zip TEXT,
    date_created DATE
);

-- City-owned properties

DROP TABLE IF EXISTS city_properties CASCADE;
CREATE TABLE city_properties (
    site_code TEXT,
    file_code TEXT,
    grantor TEXT,
    month_acquired DOUBLE PRECISION,
    year_acquired DOUBLE PRECISION,
    purchase_fund TEXT,
    land_cost TEXT,
    building_cost TEXT,
    closing_cost TEXT,
    site_name TEXT,
    managing_group TEXT,
    managing_dept TEXT,
    designated_use TEXT,
    site_acres DOUBLE PRECISION,
    file_acres DOUBLE PRECISION,
    original_acres DOUBLE PRECISION
);

-- City leases

DROP TABLE IF EXISTS city_leases CASCADE;
CREATE TABLE city_leases (
    site_code TEXT,
    lessee_name TEXT,
    lessee_company TEXT,
    lessee_dba TEXT,
    address_zip TEXT,
    lease_record_type TEXT,
    lease_description TEXT,
    lease_status TEXT,
    lease_location_name TEXT,
    nonprofit_lessee TEXT,
    date_effective DATE,
    date_sched_termination DATE,
    rent_code TEXT,
    cost_line_amt_usd TEXT
);

-- SANDAG parcels

DROP TABLE IF EXISTS parcels CASCADE;
CREATE TABLE parcels (
    apn TEXT,
    parcelid INTEGER,
    situs_juri TEXT,
    situs_addr INTEGER,
    situs_pre TEXT,
    situs_stre TEXT,
    situs_suff TEXT,
    situs_frac TEXT,
    situs_suit TEXT,
    situs_comm TEXT,
    situs_zip TEXT,
    asr_land BIGINT,
    asr_impr BIGINT,
    asr_total BIGINT,
    acreage DOUBLE PRECISION,
    ownerocc TEXT,
    asr_landus INTEGER,
    unitqty INTEGER,
    total_lvg INTEGER,
    bedrooms TEXT,
    baths TEXT,
    x_coord DOUBLE PRECISION,
    y_coord DOUBLE PRECISION
);

-- PMC-managed properties

DROP TABLE IF EXISTS pmc_properties CASCADE;
CREATE TABLE pmc_properties (
    pmc_name TEXT,
    name TEXT,
    street_address TEXT,
    city TEXT,
    zip5 TEXT,
    source TEXT,
    apn TEXT,
    total_units INTEGER,
    assessed_value BIGINT,
    landuse_label TEXT,
    situs_comm TEXT,
    confidence TEXT
);

-- Airbnb listings (Inside Airbnb)

DROP TABLE IF EXISTS airbnb_listings CASCADE;
CREATE TABLE airbnb_listings (
    id BIGINT PRIMARY KEY,
    name TEXT,
    host_id BIGINT,
    host_name TEXT,
    neighbourhood_group TEXT,
    neighbourhood TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    room_type TEXT,
    price INTEGER,
    minimum_nights INTEGER,
    number_of_reviews INTEGER,
    last_review DATE,
    reviews_per_month DOUBLE PRECISION,
    calculated_host_listings_count INTEGER,
    availability_365 INTEGER,
    number_of_reviews_ltm INTEGER,
    license TEXT
);

-- DRE licensees

DROP TABLE IF EXISTS dre_licensees CASCADE;
CREATE TABLE dre_licensees (
    multiple_license_ind TEXT,
    lastname_primary TEXT,
    firstname_secondary TEXT,
    name_suffix TEXT,
    lic_number INTEGER,
    lic_type TEXT,
    lic_status TEXT,
    lic_effective_date TEXT,
    lic_expiration_date TEXT,
    original_date_of_license TEXT,
    related_lic_number TEXT,
    related_lastname_primary TEXT,
    related_firstname_secondary TEXT,
    related_name_suffix TEXT,
    related_lic_type TEXT,
    address_1 TEXT,
    address_2 TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    county_name TEXT,
    restricted_flag TEXT
);

-- Post-load: add geometry columns and spatial indexes
-- (ETL script splits on this marker)

DO $$
BEGIN
    -- service_requests
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='service_requests' AND column_name='geom') THEN
        ALTER TABLE service_requests ADD COLUMN geom geometry(Point, 4326);
    END IF;
    UPDATE service_requests SET geom = ST_SetSRID(ST_MakePoint(lng, lat), 4326) WHERE lat IS NOT NULL AND lng IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_service_requests_geom ON service_requests USING GIST (geom);

    -- crime_offenses
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='crime_offenses' AND column_name='geom') THEN
        ALTER TABLE crime_offenses ADD COLUMN geom geometry(Point, 4326);
    END IF;
    UPDATE crime_offenses SET geom = ST_SetSRID(ST_MakePoint(lng, lat), 4326) WHERE lat IS NOT NULL AND lng IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_crime_offenses_geom ON crime_offenses USING GIST (geom);

    -- building_permits
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='building_permits' AND column_name='geom') THEN
        ALTER TABLE building_permits ADD COLUMN geom geometry(Point, 4326);
    END IF;
    UPDATE building_permits SET geom = ST_SetSRID(ST_MakePoint(lng, lat), 4326) WHERE lat IS NOT NULL AND lng IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_building_permits_geom ON building_permits USING GIST (geom);

    -- code_enforcement
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='code_enforcement' AND column_name='geom') THEN
        ALTER TABLE code_enforcement ADD COLUMN geom geometry(Point, 4326);
    END IF;
    UPDATE code_enforcement SET geom = ST_SetSRID(ST_MakePoint(lng, lat), 4326) WHERE lat IS NOT NULL AND lng IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_code_enforcement_geom ON code_enforcement USING GIST (geom);

    -- businesses
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='businesses' AND column_name='geom') THEN
        ALTER TABLE businesses ADD COLUMN geom geometry(Point, 4326);
    END IF;
    UPDATE businesses SET geom = ST_SetSRID(ST_MakePoint(lng, lat), 4326) WHERE lat IS NOT NULL AND lng IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_businesses_geom ON businesses USING GIST (geom);

    -- stro_licenses
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stro_licenses' AND column_name='geom') THEN
        ALTER TABLE stro_licenses ADD COLUMN geom geometry(Point, 4326);
    END IF;
    UPDATE stro_licenses SET geom = ST_SetSRID(ST_MakePoint(lng, lat), 4326) WHERE lat IS NOT NULL AND lng IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_stro_licenses_geom ON stro_licenses USING GIST (geom);
    -- parcels (coordinates are State Plane Zone 6 NAD83 feet, convert to WGS84)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='parcels' AND column_name='geom') THEN
        ALTER TABLE parcels ADD COLUMN geom geometry(Point, 4326);
    END IF;
    UPDATE parcels SET geom = ST_Transform(
        ST_SetSRID(ST_MakePoint(x_coord, y_coord), 2230), 4326
    ) WHERE x_coord IS NOT NULL AND y_coord IS NOT NULL AND x_coord > 0 AND y_coord > 0;
    CREATE INDEX IF NOT EXISTS idx_parcels_geom ON parcels USING GIST (geom);

    -- airbnb_listings
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='airbnb_listings' AND column_name='geom') THEN
        ALTER TABLE airbnb_listings ADD COLUMN geom geometry(Point, 4326);
    END IF;
    UPDATE airbnb_listings SET geom = ST_SetSRID(ST_MakePoint(lng, lat), 4326) WHERE lat IS NOT NULL AND lng IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_airbnb_geom ON airbnb_listings USING GIST (geom);
END $$;

-- B-tree indexes for common queries

CREATE INDEX IF NOT EXISTS idx_sr_date_requested ON service_requests (date_requested);
CREATE INDEX IF NOT EXISTS idx_sr_service_name ON service_requests (service_name);
CREATE INDEX IF NOT EXISTS idx_sr_status ON service_requests (status);
CREATE INDEX IF NOT EXISTS idx_sr_street_address ON service_requests (street_address);

CREATE INDEX IF NOT EXISTS idx_co_occured_on ON crime_offenses (occured_on);
CREATE INDEX IF NOT EXISTS idx_co_category ON crime_offenses (pd_offense_category);
CREATE INDEX IF NOT EXISTS idx_co_neighborhood ON crime_offenses (neighborhood);

CREATE INDEX IF NOT EXISTS idx_cfs_date_time ON calls_for_service (date_time);
CREATE INDEX IF NOT EXISTS idx_cfs_call_type ON calls_for_service (call_type);
CREATE INDEX IF NOT EXISTS idx_cfs_beat ON calls_for_service (beat);

CREATE INDEX IF NOT EXISTS idx_bp_date_create ON building_permits (date_approval_create);
CREATE INDEX IF NOT EXISTS idx_bp_type ON building_permits (approval_type);
CREATE INDEX IF NOT EXISTS idx_bp_address ON building_permits (address_job);
CREATE INDEX IF NOT EXISTS idx_bp_holder ON building_permits (approval_permit_holder);

CREATE INDEX IF NOT EXISTS idx_ce_date_open ON code_enforcement (date_open);
CREATE INDEX IF NOT EXISTS idx_ce_address ON code_enforcement (address_street);

CREATE INDEX IF NOT EXISTS idx_biz_owner ON businesses (business_owner_name);
CREATE INDEX IF NOT EXISTS idx_biz_naics ON businesses (naics_description);
CREATE INDEX IF NOT EXISTS idx_biz_status ON businesses (account_status);

CREATE INDEX IF NOT EXISTS idx_stro_tier ON stro_licenses (tier);
CREATE INDEX IF NOT EXISTS idx_stro_expiration ON stro_licenses (date_expiration);

CREATE INDEX IF NOT EXISTS idx_parcels_apn ON parcels (apn);
CREATE INDEX IF NOT EXISTS idx_parcels_situs_addr ON parcels (situs_addr);
CREATE INDEX IF NOT EXISTS idx_parcels_situs_stre ON parcels (situs_stre);
CREATE INDEX IF NOT EXISTS idx_parcels_situs_juri ON parcels (situs_juri);
CREATE INDEX IF NOT EXISTS idx_parcels_asr_landus ON parcels (asr_landus);
CREATE INDEX IF NOT EXISTS idx_parcels_ownerocc ON parcels (ownerocc);
CREATE INDEX IF NOT EXISTS idx_parcels_situs_zip ON parcels (situs_zip);

CREATE INDEX IF NOT EXISTS idx_pmc_name ON pmc_properties (pmc_name);
CREATE INDEX IF NOT EXISTS idx_pmc_apn ON pmc_properties (apn);
CREATE INDEX IF NOT EXISTS idx_pmc_confidence ON pmc_properties (confidence);

CREATE INDEX IF NOT EXISTS idx_airbnb_neighbourhood ON airbnb_listings (neighbourhood);
CREATE INDEX IF NOT EXISTS idx_airbnb_room_type ON airbnb_listings (room_type);
CREATE INDEX IF NOT EXISTS idx_airbnb_host_id ON airbnb_listings (host_id);

CREATE INDEX IF NOT EXISTS idx_dre_lic_type ON dre_licensees (lic_type);
CREATE INDEX IF NOT EXISTS idx_dre_lic_status ON dre_licensees (lic_status);
CREATE INDEX IF NOT EXISTS idx_dre_county ON dre_licensees (county_name);
CREATE INDEX IF NOT EXISTS idx_dre_lastname ON dre_licensees (lastname_primary);
