import os
import re
import logging
from typing import Iterable, List, Tuple

import boto3
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import psycopg
from psycopg import sql
from psycopg.extras import execute_values
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


def get_logger() -> logging.Logger:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    return logging.getLogger(__name__)


LOGGER = get_logger()


def _find_latest_curated_date(bucket: str, curated_prefix: str, region: str) -> str | None:
    """Find the most recent date=YYYYMMDD folder under the curated prefix."""
    s3 = boto3.client("s3", region_name=region)
    prefix = f"{curated_prefix.strip('/')}/"
    continuation: str | None = None
    latest_date: str | None = None
    latest_modified = None

    while True:
        params = {"Bucket": bucket, "Prefix": prefix}
        if continuation:
            params["ContinuationToken"] = continuation
        resp = s3.list_objects_v2(**params)
        for obj in resp.get("Contents", []):
            key = obj.get("Key", "")
            m = re.search(r"date=(\d{8})/", key)
            if m:
                d = m.group(1)
                lm = obj.get("LastModified")
                if latest_modified is None or (lm and lm > latest_modified):
                    latest_modified = lm
                    latest_date = d
        if not resp.get("IsTruncated"):
            break
        continuation = resp.get("NextContinuationToken")
    return latest_date


def _iter_batches(dataset_uri: str, filesystem: pafs.FileSystem, batch_size: int = 5000) -> Iterable:
    dataset = ds.dataset(dataset_uri, format="parquet", filesystem=filesystem)
    scanner = dataset.scanner(batch_size=batch_size)
    return scanner.to_reader()


def _rows_from_batch(batch) -> List[Tuple]:
    tbl = batch.to_pydict()
    codes = tbl.get("code", [])
    product_names = tbl.get("product_name", [])
    brands = tbl.get("brands", [])
    categories = tbl.get("categories", [])
    countries = tbl.get("countries", [])
    energy = tbl.get("energy_kcal_100g", [])
    sugars = tbl.get("sugars_100g", [])
    fat = tbl.get("fat_100g", [])
    proteins = tbl.get("proteins_100g", [])
    rows: List[Tuple] = []
    for i in range(len(codes)):
        rows.append(
            (
                codes[i],
                product_names[i] if i < len(product_names) else None,
                brands[i] if i < len(brands) else None,
                categories[i] if i < len(categories) else None,
                countries[i] if i < len(countries) else None,
                energy[i] if i < len(energy) else None,
                sugars[i] if i < len(sugars) else None,
                fat[i] if i < len(fat) else None,
                proteins[i] if i < len(proteins) else None,
            )
        )
    return rows


def main() -> None:
    bucket = os.environ.get("S3_BUCKET") or os.environ.get("S3_BUCKET_NAME")
    if not bucket:
        raise SystemExit("S3_BUCKET (or S3_BUCKET_NAME) is required.")

    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    curated_prefix = os.environ.get("CURATED_PREFIX", "curated")
    date = os.environ.get("DATE")

    if not date:
        date = _find_latest_curated_date(bucket, curated_prefix, region)
        if not date:
            raise SystemExit(
                f"No curated data found under s3://{bucket}/{curated_prefix.strip('/')}/. Set DATE=YYYYMMDD."
            )

    dataset_uri = f"s3://{bucket}/{curated_prefix.strip('/')}/date={date}"
    LOGGER.info("Loading curated dataset from %s", dataset_uri)

    pg_dsn = os.environ.get("PG_DSN")
    if not pg_dsn:
        raise SystemExit("PG_DSN is required to load into Postgres (e.g. postgresql://user:pass@host:5432/db)")

    # Prepare filesystem for S3
    fs = pafs.S3FileSystem(region=region)

    table_name = os.environ.get("PG_TABLE", "openfoodfacts_products")
    create_sql = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {table} (
          code TEXT PRIMARY KEY,
          product_name TEXT,
          brands TEXT,
          categories TEXT,
          countries TEXT,
          energy_kcal_100g DOUBLE PRECISION,
          sugars_100g DOUBLE PRECISION,
          fat_100g DOUBLE PRECISION,
          proteins_100g DOUBLE PRECISION
        );
        """
    ).format(table=sql.Identifier(table_name))

    upsert_sql = sql.SQL(
        """
        INSERT INTO {table} (
          code, product_name, brands, categories, countries,
          energy_kcal_100g, sugars_100g, fat_100g, proteins_100g
        ) VALUES %s
        ON CONFLICT (code) DO UPDATE SET
          product_name=EXCLUDED.product_name,
          brands=EXCLUDED.brands,
          categories=EXCLUDED.categories,
          countries=EXCLUDED.countries,
          energy_kcal_100g=EXCLUDED.energy_kcal_100g,
          sugars_100g=EXCLUDED.sugars_100g,
          fat_100g=EXCLUDED.fat_100g,
          proteins_100g=EXCLUDED.proteins_100g
        ;
        """
    ).format(table=sql.Identifier(table_name))

    batch_size = int(os.environ.get("PG_BATCH_SIZE", "5000"))

    with psycopg.connect(pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            # Stream batches from S3 parquet and upsert
            count_rows = 0
            for batch in _iter_batches(dataset_uri, fs, batch_size=batch_size):
                rows = _rows_from_batch(batch)
                if not rows:
                    continue
                execute_values(
                    cur,
                    upsert_sql.as_string(conn),
                    rows,
                    template=(
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    ),
                    page_size=batch_size,
                )
                count_rows += len(rows)
                LOGGER.info("Upserted %d rows (total %d)", len(rows), count_rows)
        conn.commit()

    LOGGER.info("Completed load of curated data for date=%s into table %s", date, table_name)


if __name__ == "__main__":
    main()


