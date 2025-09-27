# Data Map

Curated dataset schema and field lineage from OpenFoodFacts raw JSONL.gz.

## Sources and sinks
- Raw (S3): `s3://$S3_BUCKET/$RAW_PREFIX/openfoodfacts-products-YYYYMMDD.jsonl.gz`
- Curated (S3, Parquet, partitioned): `s3://$S3_BUCKET/$CURATED_PREFIX/date=YYYYMMDD/part-*.parquet`
- Optional Postgres table: `openfoodfacts_products`
- Optional DuckDB view: `curated.products` → points to `curated/date=YYYYMMDD/*.parquet`

## Field mapping

| Curated column           | Type     | Raw JSON path                                  | Postgres column        | Notes |
|--------------------------|----------|-----------------------------------------------|------------------------|-------|
| code                     | TEXT     | `$.code`                                      | `code` (PK)            | Primary key in serving DB |
| product_name             | TEXT     | `$.product_name`                              | `product_name`         | May be null/empty |
| brands                   | TEXT     | `$.brands`                                    | `brands`               | Comma-separated in source |
| categories               | TEXT     | `$.categories`                                | `categories`           | Pipe-/comma-separated in source |
| countries                | TEXT     | `$.countries`                                 | `countries`            | Localized names possible |
| energy_kcal_100g         | DOUBLE   | `$.nutriments."energy-kcal_100g"`           | `energy_kcal_100g`     | Parsed with `try_cast` |
| sugars_100g              | DOUBLE   | `$.nutriments.sugars_100g`                    | `sugars_100g`          | Parsed with `try_cast` |
| fat_100g                 | DOUBLE   | `$.nutriments.fat_100g`                       | `fat_100g`             | Parsed with `try_cast` |
| proteins_100g            | DOUBLE   | `$.nutriments.proteins_100g`                  | `proteins_100g`        | Parsed with `try_cast` |

Partition column (implicit from path):

| Partition column | Type | Derivation                 | Notes |
|------------------|------|----------------------------|-------|
| date             | TEXT | `date=YYYYMMDD` in path    | Available with `HIVE_PARTITIONING=1` in DuckDB |

## Transform rules (summary)
- Read raw line as `json` and extract fields using DuckDB `json_extract_string`.
- Numeric nutriments parsed with `try_cast(... AS DOUBLE)`; non-parsable values become NULL.
- No de-duplication at transform time; `code` uniqueness enforced if loaded into Postgres.

## Quality considerations
- `code` required for serving table primary key.
- Text fields may contain delimiters or localized values; normalize downstream if needed.
- Nutriment units assumed per field name (per 100g); validate before aggregation.
