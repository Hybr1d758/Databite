import os
import re
import duckdb
import boto3
from dotenv import load_dotenv, find_dotenv

# Load environment from .env (project root or etl/.env), allow override via DOTENV_PATH
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
_DOTENV_PATH = os.environ.get("DOTENV_PATH", "")
if _DOTENV_PATH and os.path.exists(_DOTENV_PATH):
    load_dotenv(_DOTENV_PATH)
else:
    for path in (os.path.join(_PROJECT_ROOT, ".env"), os.path.join(os.path.dirname(__file__), ".env")):
        if os.path.exists(path):
            load_dotenv(path)
            break
    else:
        found = find_dotenv(usecwd=True)
        if found:
            load_dotenv(found)

BUCKET = os.environ.get("S3_BUCKET") or os.environ.get("S3_BUCKET_NAME")
if not BUCKET:
    raise SystemExit("S3_BUCKET (or S3_BUCKET_NAME) is required for transform.")

REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw")
CURATED_PREFIX = os.environ.get("CURATED_PREFIX", "curated")

RAW_KEY = os.environ.get("RAW_KEY")  # optional override; else derive from DATE
DATE = os.environ.get("DATE")        # e.g. 20250926


def _find_latest_raw_key(bucket: str, prefix: str, region: str) -> str | None:
    """Return the Key of the most recently modified .jsonl.gz under the prefix."""
    s3 = boto3.client("s3", region_name=region)
    continuation: str | None = None
    latest_obj: dict | None = None
    while True:
        params = {"Bucket": bucket, "Prefix": f"{prefix.strip('/')}/"}
        if continuation:
            params["ContinuationToken"] = continuation
        resp = s3.list_objects_v2(**params)
        for obj in resp.get("Contents", []):
            key = obj.get("Key", "")
            if key.endswith(".jsonl.gz"):
                if latest_obj is None or obj["LastModified"] > latest_obj["LastModified"]:
                    latest_obj = obj
        if not resp.get("IsTruncated"):
            break
        continuation = resp.get("NextContinuationToken")
    return latest_obj["Key"] if latest_obj else None


def _extract_date_from_key(key: str) -> str | None:
    match = re.search(r"(\d{8})\.jsonl\.gz$", key)
    return match.group(1) if match else None


if not RAW_KEY and not DATE:
    latest_key = _find_latest_raw_key(BUCKET, RAW_PREFIX, REGION)
    if not latest_key:
        raise SystemExit(
            f"No raw .jsonl.gz files found under s3://{BUCKET}/{RAW_PREFIX.strip('/')}/. "
            "Set DATE=YYYYMMDD or provide RAW_KEY=s3://.../file.jsonl.gz"
        )
    RAW_KEY = f"s3://{BUCKET}/{latest_key}"
    DATE = _extract_date_from_key(latest_key) or "unknown"

raw_uri = RAW_KEY or f"s3://{BUCKET}/{RAW_PREFIX.strip('/')}/openfoodfacts-products-{DATE}.jsonl.gz"
curated_prefix = f"s3://{BUCKET}/{CURATED_PREFIX.strip('/')}/date={DATE}"
max_rows_env = os.environ.get("MAX_ROWS")
MAX_ROWS = int(max_rows_env) if max_rows_env and max_rows_env.isdigit() else 0

con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql(f"SET s3_region='{REGION}';")  # picks up AWS creds from env
try:
    import multiprocessing
    con.sql(f"PRAGMA threads={multiprocessing.cpu_count()}")
except Exception:
    pass
con.sql("PRAGMA enable_progress_bar")

# Explicitly pass AWS credentials to DuckDB if available (helps in some setups)
_AK = os.environ.get("AWS_ACCESS_KEY_ID")
_SK = os.environ.get("AWS_SECRET_ACCESS_KEY")
_ST = os.environ.get("AWS_SESSION_TOKEN")
if _AK and _SK:
    con.sql(f"SET s3_access_key_id='{_AK}';")
    con.sql(f"SET s3_secret_access_key='{_SK}';")
if _ST:
    con.sql(f"SET s3_session_token='{_ST}';")

print(f"Reading raw from: {raw_uri}")
print(f"Writing curated to prefix: {curated_prefix}")
if MAX_ROWS:
    print(f"Limiting to MAX_ROWS={MAX_ROWS} for faster development runs")

# Read JSONL.gz, normalize a few columns, then write Parquet
limit_clause = f" LIMIT {MAX_ROWS} " if MAX_ROWS else ""
con.sql(
    """
    CREATE OR REPLACE TABLE t AS
    SELECT
      json_extract_string(r.json, '$.code') AS code,
      json_extract_string(r.json, '$.product_name') AS product_name,
      json_extract_string(r.json, '$.brands') AS brands,
      json_extract_string(r.json, '$.categories') AS categories,
      json_extract_string(r.json, '$.countries') AS countries,
      try_cast(json_extract_string(r.json, '$.nutriments."energy-kcal_100g"') AS DOUBLE) AS energy_kcal_100g,
      try_cast(json_extract_string(r.json, '$.nutriments.sugars_100g') AS DOUBLE)        AS sugars_100g,
      try_cast(json_extract_string(r.json, '$.nutriments.fat_100g') AS DOUBLE)           AS fat_100g,
      try_cast(json_extract_string(r.json, '$.nutriments.proteins_100g') AS DOUBLE)      AS proteins_100g
    FROM read_json(?) AS r
    """ + limit_clause,
    params=[raw_uri],
)

con.sql(f"""
COPY (SELECT * FROM t)
TO '{curated_prefix}/part-{{i}}.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);
""")
print(f"Wrote curated parquet to {curated_prefix}")