"""Shared configuration for the retail medallion pipeline: DB connection,
per-table schema definitions (mirrors FIELD_MAP in
dataset_generator/retail_csv_generator.py), and small path/id helpers.
"""

import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

WAREHOUSE_HOST = os.environ.get("WAREHOUSE_HOST", "postgres")
WAREHOUSE_PORT = os.environ.get("WAREHOUSE_PORT", "5432")
WAREHOUSE_DB = os.environ.get("WAREHOUSE_DB", "warehouse")
WAREHOUSE_USER = os.environ.get("WAREHOUSE_USER", "root")
WAREHOUSE_PASSWORD = os.environ.get("WAREHOUSE_PASSWORD", "dibimbing")

_engine: Engine | None = None


def get_engine() -> Engine:
    """Lazily-created, process-wide SQLAlchemy engine for the warehouse DB."""
    global _engine
    if _engine is None:
        url = (
            f"postgresql+psycopg2://{WAREHOUSE_USER}:{WAREHOUSE_PASSWORD}"
            f"@{WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DB}"
        )
        _engine = create_engine(url, pool_pre_ping=True)
    return _engine


def get_unique_id() -> str:
    """The --unique-id the dataset was generated with. Prefers the Airflow
    Variable `retail_unique_id` (settable from the UI without redeploying),
    falls back to the RETAIL_UNIQUE_ID env var, then a hardcoded default."""
    try:
        from airflow.models import Variable

        return Variable.get("retail_unique_id", default_var="zidan")
    except Exception:
        return os.environ.get("RETAIL_UNIQUE_ID", "zidan")


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

# Matches the `./airflow/data:/opt/airflow/data` volume mount in
# docker-compose.yaml, and the --output-dir ./airflow/data/raw the assignment
# tells students to pass to the generator.
AIRFLOW_DATA_DIR = Path(os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data"))
RAW_DATA_DIR = AIRFLOW_DATA_DIR / "raw"


def raw_table_dir(table: str, unique_id: str | None = None) -> Path:
    return RAW_DATA_DIR / (unique_id or get_unique_id()) / table


# ---------------------------------------------------------------------------
# Table schemas (columns, primary key, columns needing numeric/date casts)
# ---------------------------------------------------------------------------
# Mirrors FIELD_MAP in dataset_generator/retail_csv_generator.py. `pk` is
# always a tuple - two bridge tables (product_suppliers, order_promotions)
# have no dedicated id column and use a composite key instead.

CORE_TABLES = ["categories", "stores", "products", "customers", "orders", "order_items"]

TABLE_CONFIGS = {
    "categories": {
        "columns": ["category_id", "category_name"],
        "pk": ("category_id",),
        "numeric": {},
        "dates": [],
    },
    "stores": {
        "columns": ["store_id", "store_name", "city", "opened_date"],
        "pk": ("store_id",),
        "numeric": {},
        "dates": ["opened_date"],
    },
    "products": {
        "columns": ["product_id", "product_name", "category_id", "unit_price"],
        "pk": ("product_id",),
        "numeric": {"unit_price": "float"},
        "dates": [],
    },
    "customers": {
        "columns": ["customer_id", "name", "email", "phone", "city", "join_date", "segment"],
        "pk": ("customer_id",),
        "numeric": {},
        "dates": ["join_date"],
    },
    "suppliers": {
        "columns": ["supplier_id", "supplier_name", "city"],
        "pk": ("supplier_id",),
        "numeric": {},
        "dates": [],
    },
    "product_suppliers": {
        "columns": ["product_id", "supplier_id", "cost_price"],
        "pk": ("product_id", "supplier_id"),
        "numeric": {"cost_price": "float"},
        "dates": [],
    },
    "employees": {
        "columns": ["employee_id", "name", "store_id", "role", "hire_date"],
        "pk": ("employee_id",),
        "numeric": {},
        "dates": ["hire_date"],
    },
    "promotions": {
        "columns": ["promo_id", "promo_code", "discount_pct", "start_date", "end_date"],
        "pk": ("promo_id",),
        "numeric": {"discount_pct": "int"},
        "dates": ["start_date", "end_date"],
        "bounds": {"discount_pct": (0, 100)},
    },
    "orders": {
        "columns": ["order_id", "customer_id", "store_id", "order_date", "payment_method", "status"],
        "pk": ("order_id",),
        "numeric": {},
        "dates": ["order_date"],
    },
    "order_items": {
        "columns": ["order_item_id", "order_id", "product_id", "quantity", "unit_price"],
        "pk": ("order_item_id",),
        "numeric": {"quantity": "int", "unit_price": "float"},
        "dates": [],
    },
    "payments": {
        "columns": ["payment_id", "order_id", "method", "amount", "status", "paid_at"],
        "pk": ("payment_id",),
        "numeric": {"amount": "float"},
        "dates": ["paid_at"],
    },
    "shipments": {
        "columns": ["shipment_id", "order_id", "courier", "shipped_date", "delivered_date", "status"],
        "pk": ("shipment_id",),
        "numeric": {},
        "dates": ["shipped_date", "delivered_date"],
    },
    "product_reviews": {
        "columns": ["review_id", "customer_id", "product_id", "rating", "review_text", "review_date"],
        "pk": ("review_id",),
        "numeric": {"rating": "int"},
        "dates": ["review_date"],
        "bounds": {"rating": (1, 5)},
    },
    "order_promotions": {
        "columns": ["order_id", "promo_id"],
        "pk": ("order_id", "promo_id"),
        "numeric": {},
        "dates": [],
    },
    "support_tickets": {
        "columns": ["ticket_id", "customer_id", "order_id", "issue_type", "status", "created_at"],
        "pk": ("ticket_id",),
        "numeric": {},
        "dates": ["created_at"],
    },
    "loyalty_points": {
        "columns": ["loyalty_id", "customer_id", "order_id", "points_earned", "points_redeemed"],
        "pk": ("loyalty_id",),
        "numeric": {"points_earned": "int", "points_redeemed": "int"},
        "dates": [],
    },
}

ALL_TABLES = list(TABLE_CONFIGS.keys())
