import os
import argparse
import re
import duckdb
import boto3
from dotenv import load_dotenv, find_dotenv


def load_env() -> None:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    dotenv_path = os.environ.get("DOTENV_PATH", "")
    if dotenv_path and os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)
        return
    for path in (os.path.join(project_root, ".env"), os.path.join(project_root, "etl", ".env")):
        if os.path.exists(path):
            load_dotenv(path)
            return
    found = find_dotenv(usecwd=True)
    if found:
        load_dotenv(found)


def find_latest_curated_date(bucket: str, curated_prefix: str, region: str) -> str | None:
    s3 = boto3.client("s3", region_name=region)
    prefix = f"{curated_prefix.strip('/')}/"
    continuation = None
    latest_date = None
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Query curated Parquet in S3 with DuckDB (optionally create a view)")
    parser.add_argument("--date", help="date partition YYYYMMDD; if omitted, auto-detect latest")
    parser.add_argument("--limit", type=int, default=5, help="number of sample rows to display")
    parser.add_argument("--db", help="path to a DuckDB file to use (e.g., openfoodfacts.duckdb)")
    parser.add_argument(
        "--save-view",
        action="store_true",
        help="create/replace a view that points at the curated Parquet (no data copy)",
    )
    parser.add_argument(
        "--view",
        default="curated.products",
        help="fully-qualified view name to create when --save-view is set (default: curated.products)",
    )
    args = parser.parse_args()

    load_env()

    bucket = os.environ.get("S3_BUCKET") or os.environ.get("S3_BUCKET_NAME")
    if not bucket:
        raise SystemExit("S3_BUCKET (or S3_BUCKET_NAME) is required")
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    curated_prefix = os.environ.get("CURATED_PREFIX", "curated")

    date = args.date or os.environ.get("DATE")
    if not date:
        date = find_latest_curated_date(bucket, curated_prefix, region)
        if not date:
            raise SystemExit(f"No curated partitions found under s3://{bucket}/{curated_prefix}/")

    parquet_glob = f"s3://{bucket}/{curated_prefix.strip('/')}/date={date}/*.parquet"

    # Connect to DuckDB (file if provided, otherwise in-memory)
    con = duckdb.connect(args.db) if args.db else duckdb.connect()
    con.sql("INSTALL httpfs; LOAD httpfs;")
    con.sql(f"SET s3_region='{region}';")

    ak = os.environ.get("AWS_ACCESS_KEY_ID")
    sk = os.environ.get("AWS_SECRET_ACCESS_KEY")
    st = os.environ.get("AWS_SESSION_TOKEN")
    if ak and sk:
        con.sql(f"SET s3_access_key_id='{ak}';")
        con.sql(f"SET s3_secret_access_key='{sk}';")
    if st:
        con.sql(f"SET s3_session_token='{st}';")

    print(f"Querying: {parquet_glob}")
    cnt = con.sql(f"SELECT count(*) AS rows FROM parquet_scan('{parquet_glob}')").fetchall()[0][0]
    print(f"Rows: {cnt}")

    limit = max(1, args.limit)
    rows = con.sql(f"SELECT * FROM parquet_scan('{parquet_glob}') LIMIT {limit}").fetchall()
    # Print header then rows without requiring pandas/numpy
    cols = [d[0] for d in con.sql(f"SELECT * FROM parquet_scan('{parquet_glob}') LIMIT 0").description]
    print(" | ".join(cols))
    for r in rows:
        print(" | ".join("" if v is None else str(v) for v in r))

    # Optionally create a persistent view pointing to S3 Parquet (virtual, no copy)
    if args.save_view:
        view_name = args.view
        if "." in view_name:
            schema_name, simple_view = view_name.split(".", 1)
            con.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            con.sql(
                f"CREATE OR REPLACE VIEW {schema_name}.{simple_view} AS SELECT * FROM parquet_scan('{parquet_glob}')"
            )
        else:
            con.sql(
                f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM parquet_scan('{parquet_glob}')"
            )
        print(f"Created/updated view '{view_name}' pointing to {parquet_glob}")


if __name__ == "__main__":
    main()


