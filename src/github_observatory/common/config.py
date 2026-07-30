"""Central configuration: Unity Catalog names and paths.

Every module must reference these constants instead of hard-coding
catalog/schema/table names, so a future environment change is a
one-file edit.
"""

CATALOG = "github_observatory"

BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"

# Unity Catalog Volume where raw GH Archive hours are stored.
# On Databricks serverless this path is FUSE-mounted and writable with
# ordinary Python file APIs. Do NOT use file:/tmp or /dbfs/tmp.
RAW_VOLUME_PATH = "/Volumes/github_observatory/bronze/raw_files"

# Bronze tables
EVENTS_RAW_TABLE = f"{BRONZE_SCHEMA}.events_raw"
EVENTS_QUARANTINE_TABLE = f"{BRONZE_SCHEMA}.events_quarantine"
INGESTION_AUDIT_TABLE = f"{BRONZE_SCHEMA}.ingestion_audit"
SCHEMA_PROFILE_TABLE = f"{BRONZE_SCHEMA}.schema_profile"

# GH Archive source. NOTE (verified empirically 2026-07-30): the hour
# segment is NOT zero-padded — 2026-07-29-3.json.gz exists while
# 2026-07-29-03.json.gz returns 404.
GHARCHIVE_URL_TEMPLATE = "https://data.gharchive.org/{date}-{hour}.json.gz"

# GH Archive returns HTTP 403 to the default urllib User-Agent; a
# browser-like UA is required.
HTTP_USER_AGENT = "Mozilla/5.0"
