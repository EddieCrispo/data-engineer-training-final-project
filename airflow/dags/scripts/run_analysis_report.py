#!/usr/bin/env python3
"""Standalone report generator for the PPT deliverable.

Not part of the DAG - run it manually after the pipeline has produced some
data, e.g.:

    docker compose exec airflow-webserver python /opt/airflow/dags/scripts/run_analysis_report.py

Writes chart PNGs + a text summary to /opt/airflow/data/analysis_report/
(-> ./airflow/data/analysis_report/ on the host), ready to screenshot/paste
into slides.
"""

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # airflow/dags on sys.path

from retail_pipeline.analysis import create_views, fetch  # noqa: E402
from retail_pipeline.config import AIRFLOW_DATA_DIR, get_unique_id  # noqa: E402

OUT_DIR = AIRFLOW_DATA_DIR / "analysis_report"


def chart_revenue_by_store(out_dir: Path):
    df = fetch("vw_revenue_by_store")
    if df.empty:
        return None
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(df["store_name"], df["revenue"])
    ax.set_title("Revenue by Store")
    ax.set_ylabel("Revenue")
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    path = out_dir / "revenue_by_store.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def chart_top_products(out_dir: Path, top_n=10):
    df = fetch("vw_top_products").head(top_n)
    if df.empty:
        return None
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.barh(df["product_name"][::-1], df["revenue"][::-1])
    ax.set_title(f"Top {top_n} Products by Revenue")
    ax.set_xlabel("Revenue")
    fig.tight_layout()
    path = out_dir / "top_products.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def chart_monthly_trend(out_dir: Path):
    df = fetch("vw_monthly_sales_trend")
    if df.empty:
        return None
    labels = [f"{int(y)}-{int(m):02d}" for y, m in zip(df["year"], df["month"])]
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(labels, df["revenue"], marker="o")
    ax.set_title("Monthly Sales Trend")
    ax.set_ylabel("Revenue")
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    path = out_dir / "monthly_sales_trend.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def chart_segment_revenue(out_dir: Path):
    df = fetch("vw_customer_segment_revenue")
    if df.empty:
        return None
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.bar(df["segment"], df["revenue"])
    ax.set_title("Revenue by Customer Segment")
    ax.set_ylabel("Revenue")
    fig.tight_layout()
    path = out_dir / "revenue_by_segment.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def chart_payment_mix(out_dir: Path):
    df = fetch("vw_payment_method_mix")
    if df.empty:
        return None
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.pie(df["revenue"], labels=df["payment_method"], autopct="%1.1f%%")
    ax.set_title("Revenue Share by Payment Method")
    fig.tight_layout()
    path = out_dir / "payment_method_mix.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def write_summary(out_dir: Path, unique_id: str):
    lines = [f"# Analysis Summary (unique_id={unique_id})\n"]
    for title, view in [
        ("Revenue by Store", "vw_revenue_by_store"),
        ("Top Products", "vw_top_products"),
        ("Monthly Sales Trend", "vw_monthly_sales_trend"),
        ("Revenue by Customer Segment", "vw_customer_segment_revenue"),
        ("Payment Method Mix", "vw_payment_method_mix"),
    ]:
        df = fetch(view)
        lines.append(f"\n## {title}\n")
        lines.append(df.to_markdown(index=False) if not df.empty else "_no data yet_")
    summary_path = out_dir / "summary.md"
    summary_path.write_text("\n".join(lines), encoding="utf-8")
    return summary_path


def main():
    create_views()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    unique_id = get_unique_id()

    produced = []
    for fn in (
        chart_revenue_by_store,
        chart_top_products,
        chart_monthly_trend,
        chart_segment_revenue,
        chart_payment_mix,
    ):
        path = fn(OUT_DIR)
        if path:
            produced.append(path)

    summary_path = write_summary(OUT_DIR, unique_id)

    print(f"unique_id: {unique_id}")
    print(f"output dir: {OUT_DIR}")
    if not produced:
        print("No data in gold.fact_sales yet - run the DAG first.")
    for p in produced:
        print(f"chart: {p}")
    print(f"summary: {summary_path}")


if __name__ == "__main__":
    main()
