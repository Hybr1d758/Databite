import os
import duckdb

SAMPLE = os.path.join(os.path.dirname(__file__), 'data', 'sample.jsonl')

def test_transform_projection_and_types():
    con = duckdb.connect()
    con.sql("""
    CREATE OR REPLACE TABLE t AS
    SELECT
      CAST(r.code AS VARCHAR) AS code,
      CAST(r.product_name AS VARCHAR) AS product_name,
      CAST(r.brands AS VARCHAR) AS brands,
      CAST(r.categories AS VARCHAR) AS categories,
      CAST(r.countries AS VARCHAR) AS countries,
      try_cast(r.nutriments."energy-kcal_100g" AS DOUBLE) AS energy_kcal_100g,
      try_cast(r.nutriments.sugars_100g AS DOUBLE)        AS sugars_100g,
      try_cast(r.nutriments.fat_100g AS DOUBLE)           AS fat_100g,
      try_cast(r.nutriments.proteins_100g AS DOUBLE)      AS proteins_100g
    FROM read_json_auto(?) AS r
    """, params=[SAMPLE])

    # Schema
    # PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk)
    cols = [c[1] for c in con.sql("PRAGMA table_info('t')").fetchall()]
    assert cols == [
        'code','product_name','brands','categories','countries',
        'energy_kcal_100g','sugars_100g','fat_100g','proteins_100g'
    ]

    # Row count
    cnt = con.sql("SELECT COUNT(*) FROM t").fetchone()[0]
    assert cnt == 3

    # Sample values
    row = con.sql("SELECT code, product_name, energy_kcal_100g FROM t WHERE code='0001'").fetchone()
    assert row == ('0001', 'Apple Juice', 46.0)
