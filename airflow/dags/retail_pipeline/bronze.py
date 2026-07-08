"""Bronze layer: generic, incremental, append-only CSV ingestion.

Every table under data/raw/{unique_id}/{table}/*.csv is loaded as-is (all
columns as TEXT, no cleaning) into bronze.{table}. A control table
(bronze.load_log) tracks which files have already been loaded so re-running
the DAG only picks up files it hasn't seen before - that's what makes
ingestion both incremental and idempotent.
"""

import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from retail_pipeline.config import ALL_TABLES, TABLE_CONFIGS, get_engine, get_unique_id, raw_table_dir

logger = logging.getLogger(__name__)


def _parse_batch_ts(filename: str, table: str):
    # filenames look like "{table}_{YYYYmmdd_HHMMSS}.csv"
    stem = filename[: -len(".csv")] if filename.endswith(".csv") else filename
    ts_part = stem[len(table) + 1 :]
    try:
        return datetime.strptime(ts_part, "%Y%m%d_%H%M%S")
    except ValueError:
        return None


def _already_loaded(conn, table: str) -> set:
    rows = conn.execute(
        text("SELECT file_path FROM bronze.load_log WHERE table_name = :t"), {"t": table}
    )
    return {r[0] for r in rows}


def ingest_table(table: str, unique_id: str, engine=None) -> dict:
    engine = engine or get_engine()
    table_dir = raw_table_dir(table, unique_id)
    if not table_dir.exists():
        return {"table": table, "files_loaded": 0, "rows_loaded": 0}

    files = sorted(table_dir.glob(f"{table}_*.csv"))
    with engine.connect() as conn:
        already = _already_loaded(conn, table)

    new_files = [f for f in files if str(f) not in already]
    columns = TABLE_CONFIGS[table]["columns"]
    files_loaded = 0
    rows_loaded = 0

    for f in new_files:
        df = pd.read_csv(f, dtype=str, keep_default_na=False)
        for col in columns:
            if col not in df.columns:
                df[col] = ""
        df = df[columns].copy()
        df["_source_file"] = str(f)
        df["_batch_ts"] = _parse_batch_ts(f.name, table)
        df["_loaded_at"] = datetime.utcnow()

        with engine.begin() as conn:
            df.to_sql(table, conn, schema="bronze", if_exists="append", index=False)
            conn.execute(
                text(
                    "INSERT INTO bronze.load_log (file_path, table_name, row_count) "
                    "VALUES (:fp, :t, :rc) ON CONFLICT (file_path) DO NOTHING"
                ),
                {"fp": str(f), "t": table, "rc": len(df)},
            )
        files_loaded += 1
        rows_loaded += len(df)

    if files_loaded:
        logger.info("bronze.%s: loaded %d file(s), %d row(s)", table, files_loaded, rows_loaded)
    return {"table": table, "files_loaded": files_loaded, "rows_loaded": rows_loaded}


def ingest_all(unique_id: str = None) -> list:
    unique_id = unique_id or get_unique_id()
    engine = get_engine()
    results = [ingest_table(t, unique_id, engine) for t in ALL_TABLES]
    total_files = sum(r["files_loaded"] for r in results)
    total_rows = sum(r["rows_loaded"] for r in results)
    logger.info(
        "bronze ingest done for unique_id=%s: %d file(s), %d row(s) total",
        unique_id,
        total_files,
        total_rows,
    )
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(ingest_all())
