"""Idempotent schema setup for the bronze/silver/gold warehouse.

Safe to run on every DAG run - everything is CREATE ... IF NOT EXISTS.
Column types are derived from TABLE_CONFIGS so adding/adjusting a table only
requires touching config.py.
"""

from sqlalchemy import text

from retail_pipeline.config import TABLE_CONFIGS, get_engine

_NUMERIC_SQL = {"float": "NUMERIC(18,4)", "int": "BIGINT"}


def _silver_column_type(table: str, column: str) -> str:
    cfg = TABLE_CONFIGS[table]
    if column in cfg["numeric"]:
        return _NUMERIC_SQL[cfg["numeric"][column]]
    if column in cfg["dates"]:
        return "TIMESTAMP"
    return "TEXT"


def bronze_table_ddl(table: str) -> str:
    cfg = TABLE_CONFIGS[table]
    cols = ",\n    ".join(f'"{c}" TEXT' for c in cfg["columns"])
    return f"""
    CREATE TABLE IF NOT EXISTS bronze.{table} (
        _bronze_id BIGSERIAL PRIMARY KEY,
        {cols},
        _source_file TEXT NOT NULL,
        _batch_ts TIMESTAMP,
        _loaded_at TIMESTAMP NOT NULL DEFAULT now()
    );
    """


def silver_table_ddl(table: str) -> str:
    cfg = TABLE_CONFIGS[table]
    cols = ",\n    ".join(
        f'"{c}" {_silver_column_type(table, c)}' for c in cfg["columns"]
    )
    pk_cols = ", ".join(f'"{c}"' for c in cfg["pk"])
    return f"""
    CREATE TABLE IF NOT EXISTS silver.{table} (
        {cols},
        _source_file TEXT,
        _loaded_at TIMESTAMP NOT NULL,
        PRIMARY KEY ({pk_cols})
    );
    """


GOLD_DDL = """
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    day INTEGER NOT NULL,
    day_name TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    phone TEXT,
    city TEXT,
    segment TEXT,
    join_date TIMESTAMP,
    _loaded_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.dim_store (
    store_id TEXT PRIMARY KEY,
    store_name TEXT,
    city TEXT,
    opened_date TIMESTAMP,
    _loaded_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    category_id TEXT,
    category_name TEXT,
    unit_price NUMERIC(18,4),
    _loaded_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.fact_sales (
    order_item_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    date_key INTEGER REFERENCES gold.dim_date(date_key),
    customer_id TEXT REFERENCES gold.dim_customer(customer_id),
    product_id TEXT REFERENCES gold.dim_product(product_id),
    store_id TEXT REFERENCES gold.dim_store(store_id),
    payment_method TEXT,
    order_status TEXT,
    quantity BIGINT,
    unit_price NUMERIC(18,4),
    line_amount NUMERIC(18,4),
    _loaded_at TIMESTAMP NOT NULL
);
"""

CONTROL_DDL = """
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.load_log (
    file_path TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT now(),
    row_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.load_log (
    table_name TEXT PRIMARY KEY,
    last_loaded_at TIMESTAMP NOT NULL DEFAULT '1970-01-01'
);

-- Generic rejects sink: rows silver couldn't safely upsert (e.g. missing
-- primary key). One table for all source tables keeps this from becoming a
-- combinatorial mess while still giving a data-quality trail.
CREATE TABLE IF NOT EXISTS silver.rejects (
    reject_id BIGSERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    reason TEXT NOT NULL,
    row_json JSONB NOT NULL,
    source_file TEXT,
    rejected_at TIMESTAMP NOT NULL DEFAULT now()
);
"""


def run_all_ddl():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(CONTROL_DDL))
        for table in TABLE_CONFIGS:
            conn.execute(text(bronze_table_ddl(table)))
            conn.execute(text(silver_table_ddl(table)))
        conn.execute(text(GOLD_DDL))

    # Analysis views live in analysis.py; import locally to avoid a
    # module-load cycle (analysis.py also imports from this package).
    from retail_pipeline.analysis import create_views

    create_views(engine)


if __name__ == "__main__":
    run_all_ddl()
