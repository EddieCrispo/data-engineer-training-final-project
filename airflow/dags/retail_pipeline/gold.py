"""Gold layer: the star-schema datamart, built from the 6 core tables only
(categories/stores/products/customers/orders/order_items) - these are
guaranteed to exist for every generated dataset regardless of which random
extension modules a given --unique-id happened to get, so the datamart
never depends on that randomness.

Uses natural keys (product_id, customer_id, ...) as dimension keys rather
than generated surrogate integers - simpler, and still a valid star schema.

Every statement here is an idempotent INSERT ... SELECT ... ON CONFLICT DO
UPDATE straight from silver, so re-running is always safe. Dims must be
built before the fact table (enforced by FK constraints + DAG task order).
"""

import logging

from sqlalchemy import text

from retail_pipeline.config import get_engine

logger = logging.getLogger(__name__)

DIM_DATE_SQL = """
INSERT INTO gold.dim_date (date_key, full_date, year, quarter, month, month_name, day, day_name, is_weekend)
SELECT
    to_char(d, 'YYYYMMDD')::int,
    d::date,
    extract(year FROM d)::int,
    extract(quarter FROM d)::int,
    extract(month FROM d)::int,
    trim(to_char(d, 'Month')),
    extract(day FROM d)::int,
    trim(to_char(d, 'Day')),
    extract(isodow FROM d) IN (6, 7)
FROM generate_series('2015-01-01'::date, '2035-12-31'::date, interval '1 day') d
ON CONFLICT (date_key) DO NOTHING;
"""

DIM_CUSTOMER_SQL = """
INSERT INTO gold.dim_customer (customer_id, name, email, phone, city, segment, join_date, _loaded_at)
SELECT customer_id, name, email, phone, city, segment, join_date, _loaded_at
FROM silver.customers
ON CONFLICT (customer_id) DO UPDATE SET
    name = EXCLUDED.name,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    city = EXCLUDED.city,
    segment = EXCLUDED.segment,
    join_date = EXCLUDED.join_date,
    _loaded_at = EXCLUDED._loaded_at
WHERE EXCLUDED._loaded_at >= gold.dim_customer._loaded_at;
"""

DIM_STORE_SQL = """
INSERT INTO gold.dim_store (store_id, store_name, city, opened_date, _loaded_at)
SELECT store_id, store_name, city, opened_date, _loaded_at
FROM silver.stores
ON CONFLICT (store_id) DO UPDATE SET
    store_name = EXCLUDED.store_name,
    city = EXCLUDED.city,
    opened_date = EXCLUDED.opened_date,
    _loaded_at = EXCLUDED._loaded_at
WHERE EXCLUDED._loaded_at >= gold.dim_store._loaded_at;
"""

DIM_PRODUCT_SQL = """
INSERT INTO gold.dim_product (product_id, product_name, category_id, category_name, unit_price, _loaded_at)
SELECT p.product_id, p.product_name, p.category_id, c.category_name, p.unit_price, p._loaded_at
FROM silver.products p
LEFT JOIN silver.categories c ON p.category_id = c.category_id
ON CONFLICT (product_id) DO UPDATE SET
    product_name = EXCLUDED.product_name,
    category_id = EXCLUDED.category_id,
    category_name = EXCLUDED.category_name,
    unit_price = EXCLUDED.unit_price,
    _loaded_at = EXCLUDED._loaded_at
WHERE EXCLUDED._loaded_at >= gold.dim_product._loaded_at;
"""

FACT_SALES_SQL = """
INSERT INTO gold.fact_sales (
    order_item_id, order_id, date_key, customer_id, product_id, store_id,
    payment_method, order_status, quantity, unit_price, line_amount, _loaded_at
)
SELECT
    oi.order_item_id,
    oi.order_id,
    to_char(o.order_date, 'YYYYMMDD')::int,
    o.customer_id,
    oi.product_id,
    o.store_id,
    o.payment_method,
    o.status,
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price,
    GREATEST(oi._loaded_at, o._loaded_at)
FROM silver.order_items oi
JOIN silver.orders o ON oi.order_id = o.order_id
WHERE oi.quantity IS NOT NULL
  AND oi.unit_price IS NOT NULL
  AND o.order_date IS NOT NULL
  AND o.customer_id IS NOT NULL
  AND o.store_id IS NOT NULL
ON CONFLICT (order_item_id) DO UPDATE SET
    order_id = EXCLUDED.order_id,
    date_key = EXCLUDED.date_key,
    customer_id = EXCLUDED.customer_id,
    product_id = EXCLUDED.product_id,
    store_id = EXCLUDED.store_id,
    payment_method = EXCLUDED.payment_method,
    order_status = EXCLUDED.order_status,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    line_amount = EXCLUDED.line_amount,
    _loaded_at = EXCLUDED._loaded_at
WHERE EXCLUDED._loaded_at >= gold.fact_sales._loaded_at;
"""


def build_dims(engine=None) -> None:
    engine = engine or get_engine()
    with engine.begin() as conn:
        conn.execute(text(DIM_DATE_SQL))
        conn.execute(text(DIM_CUSTOMER_SQL))
        conn.execute(text(DIM_STORE_SQL))
        conn.execute(text(DIM_PRODUCT_SQL))
    logger.info("gold dims built/refreshed")


def build_fact(engine=None) -> None:
    engine = engine or get_engine()
    with engine.begin() as conn:
        conn.execute(text(FACT_SALES_SQL))
    logger.info("gold.fact_sales built/refreshed")


def run_all() -> None:
    engine = get_engine()
    build_dims(engine)
    build_fact(engine)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_all()
