# OpenFoodFacts ETL

Extract raw OpenFoodFacts data to S3, transform it with DuckDB into curated Parquet on S3, and optionally load into Postgres.

## Prerequisites
- Python 3.10+
- AWS account with permissions to write to your S3 bucket
- (Optional) Postgres for the Load stage

## Setup
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt || pip install requests boto3 python-dotenv duckdb pyarrow psycopg[binary]
```

Create an environment file with your settings (place at project root or in `etl/.env`).

Example `.env` (do not commit real secrets):
```bash
# S3
S3_BUCKET=your-bucket            # or S3_BUCKET_NAME
RAW_PREFIX=raw
CURATED_PREFIX=curated
AWS_REGION=us-east-1             # or AWS_DEFAULT_REGION

# Credentials (prefer AWS profiles/roles in production)
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
# AWS_SESSION_TOKEN=

# Logging
LOG_LEVEL=INFO

# Transform controls (pick one)
DATE=YYYYMMDD
# RAW_KEY=s3://your-bucket/raw/openfoodfacts-products-YYYYMMDD.jsonl.gz

# (Optional) Postgres DSN for load stage
# PG_DSN=postgresql://user:pass@host:5432/openfoodfacts
```

## Extract (download -> S3)
Streams the gzipped JSONL file directly into S3 (no local storage). Requires `S3_BUCKET`.
```bash
python etl/etl.py
```
Uploads to:
```
s3://$S3_BUCKET/$RAW_PREFIX/openfoodfacts-products-YYYYMMDD.jsonl.gz
```

## Transform (S3 JSONL.gz -> S3 Parquet)
Reads the raw file from S3 and writes curated Parquet to S3.
- If `DATE` and `RAW_KEY` are not set, it will auto-pick the most recent raw file under the raw prefix.
```bash
python etl/transform.py
```
Writes to:
```
s3://$S3_BUCKET/$CURATED_PREFIX/date=$DATE/
```

## Notes
- Keep secrets out of git. Use a `.env` that is ignored by git.
- Ensure your IAM user/role has permissions for:
  - `s3:ListBucket` on the bucket (scoped to the raw/curated prefixes is fine)
  - Multipart upload actions on `raw/*` (CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload) and `s3:PutObject`.
- DuckDB S3 access uses your environment AWS credentials.
- Postgres Load step can be added when needed (upserts from curated Parquet).

## Load (S3 Parquet -> Postgres)
Load curated data into Postgres with upsert:
```bash
export PG_DSN=postgresql://user:pass@host:5432/openfoodfacts
python etl/load.py
```
Creates/updates table `openfoodfacts_products` by default.

## Troubleshooting (What we ran into and fixes)
- S3 AccessDenied on upload:
  - Missing multipart permissions. Add: `s3:CreateMultipartUpload`, `s3:UploadPart`, `s3:ListMultipartUploadParts`, `s3:CompleteMultipartUpload`, `s3:AbortMultipartUpload` on `raw/*`, plus `s3:PutObject` and `s3:ListBucket` (prefix-scoped).
- .env not loading:
  - Code now searches `DOTENV_PATH`, project root `.env`, then `etl/.env`, then auto-detects with `find_dotenv`.
- DuckDB JSON binding errors:
  - Switched from `read_json_auto` to `read_json` and explicit `json_extract_string` paths. Keys with hyphens require quotes: `$.nutriments."energy-kcal_100g"`.
- DuckDB parameter error:
  - Use `params=[raw_uri]` when passing parameters to `con.sql`.
- ModuleNotFoundError (duckdb, dotenv):
  - Install packages in the venv: `pip install duckdb python-dotenv` (or `-r requirements.txt`).

## Challenges & Resolutions (project retrospective)
- Environment discovery
  - Challenge: `.env` sometimes lived under `etl/.env`; scripts looked only in the root.
  - Resolution: Load order is `DOTENV_PATH` → project root `.env` → `etl/.env` → auto-find with `find_dotenv`.

- S3 permissions and regions
  - Challenge: 403 Forbidden uploading curated outputs; IAM allowed only `raw/*`, region mismatches caused signature errors.
  - Resolution: Ensure region is set to the bucket’s region (e.g., `eu-north-1`). Grant `s3:PutObject` and `s3:ListBucket` for both `raw/*` and `curated/*` (prefix-scoped). For multipart, `s3:PutObject` is the key permission; optionally allow `s3:AbortMultipartUpload`.

- IAM action name confusion
  - Challenge: Using API operation names like `s3:CreateMultipartUpload` caused policy validation errors.
  - Resolution: Use IAM actions (e.g., `s3:PutObject`). Multipart is covered by `PutObject`; add `AbortMultipartUpload` optionally.

- DuckDB JSON shape and quoting
  - Challenge: `read_json_auto` didn’t flatten to expected fields; binder errors; hyphenated keys (e.g., `energy-kcal_100g`) broke JSON paths.
  - Resolution: Use `read_json` with `json` column + `json_extract_string(...)` and quote hyphen keys `$.nutriments."energy-kcal_100g"`.

- Parameter passing to DuckDB
  - Challenge: Passing bind params positionally raised `TypeError`.
  - Resolution: Use `params=[raw_uri]` keyword.

- Performance of transform
  - Challenge: Large gzipped JSONL over network is slow; gzip decompression is single-threaded.
  - Resolution: Added `MAX_ROWS` for fast dev runs, enabled DuckDB threads and progress bar, recommended running near S3 (EC2 same region), and outlined a split step to shard JSONL into many parts for parallel reads. Removed unsupported settings (`s3_max_connections`).

- Credentials for DuckDB S3
  - Challenge: CLI/engine sometimes didn’t pick up creds automatically.
  - Resolution: Set `s3_region`, and when needed set `s3_access_key_id`, `s3_secret_access_key`, `s3_session_token` via SQL from env.

- Query script dependencies
  - Challenge: `pandas/numpy` not installed.
  - Resolution: Avoided `.df()`; print rows using `fetchall()`.

- SSE-KMS (optional)
  - Challenge: Buckets that enforce KMS require KMS permissions.
  - Resolution: Added env-driven SSE config (`SSE_ALGO`, `SSE_KMS_KEY_ID`) to extract; ensure IAM has `kms:Encrypt` and `kms:GenerateDataKey` on the key.

- Postgres optionality
  - Challenge: No Postgres available.
  - Resolution: Provided DuckDB queries over Parquet in S3 (virtual view, no data copy) and optional scripts to materialize later.


## Design decisions (Why these tools)
- DuckDB over Pandas for transform:
  - Columnar engine with pushdown; reads JSON/Parquet directly from S3; minimal memory; SQL-first.
- S3 + Parquet as curated store:
  - Cheap, durable, analytics-friendly; enables downstream tools; decouples transform from serving.
- Postgres for serving (optional):
  - Transactions, indexes, constraints, concurrent reads; ideal for APIs/dashboards.
- Streaming extract (requests + boto3):
  - Avoids loading multi-GB files in memory; uses multipart uploads for resilience.
- `.env` and config via env:
  - Keeps secrets out of source; easy to change environments.

