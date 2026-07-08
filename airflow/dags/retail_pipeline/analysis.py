"""Analysis layer: SQL views on top of the gold star schema, plus a helper
to fetch each one as a DataFrame (used by scripts/run_analysis_report.py to
produce the charts/summary for the PPT).

Cancelled orders are excluded from every revenue-oriented view - a
cancelled line item was never actually fulfilled, so counting its
line_amount as revenue would overstate sales.
"""

import pandas as pd
from sqlalchemy import text

from retail_pipeline.config import get_engine

VIEWS_SQL = """
CREATE OR REPLACE VIEW gold.vw_revenue_by_store AS
SELECT s.store_id, s.store_name, s.city,
       COUNT(DISTINCT f.order_id) AS order_count,
       SUM(f.line_amount) AS revenue
FROM gold.fact_sales f
JOIN gold.dim_store s ON f.store_id = s.store_id
WHERE f.order_status <> 'cancelled'
GROUP BY s.store_id, s.store_name, s.city
ORDER BY revenue DESC;

CREATE OR REPLACE VIEW gold.vw_top_products AS
SELECT p.product_id, p.product_name, p.category_name,
       SUM(f.quantity) AS units_sold,
       SUM(f.line_amount) AS revenue
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.product_id = p.product_id
WHERE f.order_status <> 'cancelled'
GROUP BY p.product_id, p.product_name, p.category_name
ORDER BY revenue DESC;

CREATE OR REPLACE VIEW gold.vw_monthly_sales_trend AS
SELECT d.year, d.month, d.month_name,
       SUM(f.line_amount) AS revenue,
       COUNT(DISTINCT f.order_id) AS order_count
FROM gold.fact_sales f
JOIN gold.dim_date d ON f.date_key = d.date_key
WHERE f.order_status <> 'cancelled'
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

CREATE OR REPLACE VIEW gold.vw_customer_segment_revenue AS
SELECT c.segment,
       COUNT(DISTINCT c.customer_id) AS customer_count,
       SUM(f.line_amount) AS revenue,
       SUM(f.line_amount) / NULLIF(COUNT(DISTINCT f.order_id), 0) AS avg_order_value
FROM gold.fact_sales f
JOIN gold.dim_customer c ON f.customer_id = c.customer_id
WHERE f.order_status <> 'cancelled'
GROUP BY c.segment
ORDER BY revenue DESC;

CREATE OR REPLACE VIEW gold.vw_payment_method_mix AS
SELECT payment_method,
       COUNT(DISTINCT order_id) AS order_count,
       SUM(line_amount) AS revenue
FROM gold.fact_sales
WHERE order_status <> 'cancelled'
GROUP BY payment_method
ORDER BY revenue DESC;
"""

VIEW_NAMES = [
    "vw_revenue_by_store",
    "vw_top_products",
    "vw_monthly_sales_trend",
    "vw_customer_segment_revenue",
    "vw_payment_method_mix",
]


def create_views(engine=None) -> None:
    engine = engine or get_engine()
    with engine.begin() as conn:
        conn.execute(text(VIEWS_SQL))


def fetch(view_name: str, engine=None) -> pd.DataFrame:
    engine = engine or get_engine()
    return pd.read_sql(f"SELECT * FROM gold.{view_name}", engine)


if __name__ == "__main__":
    create_views()
    for name in VIEW_NAMES:
        print(f"\n=== {name} ===")
        print(fetch(name).to_string(index=False))
