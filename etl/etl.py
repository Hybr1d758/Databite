import os
import logging
from datetime import datetime
import shutil
import requests
import boto3
from botocore.config import Config
from tenacity import retry, wait_exponential_jitter, stop_after_attempt, retry_if_exception_type
from dotenv import load_dotenv, find_dotenv


# Load environment variables from .env before initializing logger
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
_DOTENV_PATH = os.environ.get("DOTENV_PATH", "")
if _DOTENV_PATH and os.path.exists(_DOTENV_PATH):
    load_dotenv(_DOTENV_PATH)
else:
    candidate_paths = [
        os.path.join(_PROJECT_ROOT, ".env"),                     # project root
        os.path.join(os.path.dirname(__file__), ".env"),         # etl/.env
    ]
    used_path = ""
    for path in candidate_paths:
        if os.path.exists(path):
            load_dotenv(path)
            used_path = path
            break
    if not used_path:
        found = find_dotenv(usecwd=True)
        if found:
            load_dotenv(found)
            used_path = found
    _DOTENV_PATH = used_path or "<not-found>"


def _get_logger() -> logging.Logger:
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        level = os.environ.get("LOG_LEVEL", "INFO").upper()
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        )
    return logging.getLogger(__name__)


LOGGER = _get_logger()


@retry(
    retry=retry_if_exception_type((requests.RequestException, Exception)),
    wait=wait_exponential_jitter(initial=1, max=30),
    stop=stop_after_attempt(5),
)
def stream_download_to_s3(url: str, bucket: str, key: str, region: str = None) -> None:
    """Stream a remote file directly into S3 without loading into memory."""
    s3cfg = Config(region_name=region) if region else Config()
    s3 = boto3.client("s3", config=s3cfg)
    with requests.get(url, stream=True, timeout=(10, 120)) as resp:
        resp.raise_for_status()
        extra_args = {"ContentType": "application/gzip", "ContentEncoding": "gzip"}
        sse_algo = os.environ.get("SSE_ALGO")
        sse_kms_key = os.environ.get("SSE_KMS_KEY_ID")
        if sse_algo:
            extra_args["ServerSideEncryption"] = sse_algo
        if sse_kms_key:
            extra_args["SSEKMSKeyId"] = sse_kms_key
        s3.upload_fileobj(
            Fileobj=resp.raw,
            Bucket=bucket,
            Key=key,
            ExtraArgs=extra_args,
        )


def stream_download_to_file(url: str, out_path: str) -> None:
    """Stream a remote file to a local path."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with requests.get(url, stream=True, timeout=(10, 120)) as resp, open(out_path, "wb") as fh:
        resp.raise_for_status()
        shutil.copyfileobj(resp.raw, fh)


def main() -> None:
    url = "https://static.openfoodfacts.org/data/openfoodfacts-products.jsonl.gz"

    bucket = os.environ.get("S3_BUCKET") or os.environ.get("S3_BUCKET_NAME")
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    raw_prefix = os.environ.get("RAW_PREFIX", "raw")

    date_part = datetime.utcnow().strftime("%Y%m%d")
    filename = f"openfoodfacts-products-{date_part}.jsonl.gz"

    if not bucket:
        LOGGER.error(
            "S3 bucket not configured. Looked for S3_BUCKET or S3_BUCKET_NAME. Checked .env at: %s",
            _DOTENV_PATH,
        )
        raise SystemExit(
            "S3_BUCKET is not set. Set S3_BUCKET (or S3_BUCKET_NAME) in your .env."
        )

    key = f"{raw_prefix.rstrip('/')}/{filename}"
    LOGGER.info("Downloading and uploading to s3://%s/%s", bucket, key)
    stream_download_to_s3(url=url, bucket=bucket, key=key, region=region)
    LOGGER.info("Completed upload to s3://%s/%s", bucket, key)


if __name__ == "__main__":
    main()