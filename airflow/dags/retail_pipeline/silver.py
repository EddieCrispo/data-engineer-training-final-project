"""Silver layer: clean bronze rows and upsert the current-truth state per
primary key into silver.{table}.

Handles the data-quality issues the generator deliberately injects:
  - decimal-comma / garbled numbers -> cast to numeric, invalid -> NULL
  - negative measures (price/qty/points/...) -> NULL (a measure can't be
    negative in this domain)
  - out-of-bounds values for a couple of known-bounded columns (rating,
    discount_pct) -> NULL
  - two date formats (ISO and dd/mm/yyyy) -> parsed to a single TIMESTAMP
  - whitespace-padded strings -> trimmed, empty string -> NULL
  - duplicate / re-emitted rows (CDC-style updates, same PK reappearing
    with new values) -> deduped by PK, latest `_loaded_at` wins, then
    upserted with ON CONFLICT DO UPDATE

A watermark per table (silver.load_log) means each run only reads bronze
rows loaded since the last run, so this is both incremental and idempotent
(safe to re-run; nothing left un-advanced only gets reprocessed once).
"""

import json
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from retail_pipeline.config import ALL_TABLES, TABLE_CONFIGS, get_engine

logger = logging.getLogger(__name__)

EPOCH = datetime(1970, 1, 1)


def _get_watermark(conn, table: str) -> datetime:
    row = conn.execute(
        text("SELECT last_loaded_at FROM silver.load_log WHERE table_name = :t"), {"t": table}
    ).fetchone()
    if row is None:
        conn.execute(
            text(
                "INSERT INTO silver.load_log (table_name, last_loaded_at) "
                "VALUES (:t, :ts) ON CONFLICT (table_name) DO NOTHING"
            ),
            {"t": table, "ts": EPOCH},
        )
        return EPOCH
    return row[0]


def _parse_dates_robust(series: pd.Series) -> pd.Series:
    s = series.replace("", pd.NA)
    parsed = pd.to_datetime(s, errors="coerce", dayfirst=False)
    still_na = parsed.isna() & s.notna()
    if still_na.any():
        parsed.loc[still_na] = pd.to_datetime(s[still_na], errors="coerce", dayfirst=True)
    return parsed


def _clean(df: pd.DataFrame, table: str, cfg: dict) -> pd.DataFrame:
    df = df.copy()
    bounds = cfg.get("bounds", {})

    for col in cfg["columns"]:
        if col in cfg["numeric"]:
            s = df[col].astype(str).str.strip().str.replace(",", ".", regex=False)
            s = s.replace("", pd.NA)
            s = pd.to_numeric(s, errors="coerce")
            s = s.where(s >= 0)  # measures can't be negative - null out instead of guessing
            if col in bounds:
                lo, hi = bounds[col]
                s = s.where((s >= lo) & (s <= hi))
            if cfg["numeric"][col] == "int":
                s = s.round().astype("Int64")
            df[col] = s
        elif col in cfg["dates"]:
            df[col] = _parse_dates_robust(df[col])
        else:
            s = df[col].astype(str).str.strip()
            df[col] = s.replace("", None)

    return df


def _split_missing_pk(df: pd.DataFrame, pk_cols: tuple) -> tuple:
    mask = pd.Series(True, index=df.index)
    for c in pk_cols:
        mask &= df[c].notna() & (df[c].astype(str).str.strip() != "")
    return df[mask], df[~mask]


def _upsert(engine, table: str, cfg: dict, df: pd.DataFrame) -> None:
    if df.empty:
        return
    columns = cfg["columns"] + ["_source_file", "_loaded_at"]
    pk_cols = cfg["pk"]
    update_cols = [c for c in columns if c not in pk_cols]
    col_list = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(f":{c}" for c in columns)
    pk_list = ", ".join(f'"{c}"' for c in pk_cols)
    set_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
    sql = text(
        f'INSERT INTO silver.{table} ({col_list}) VALUES ({placeholders}) '
        f'ON CONFLICT ({pk_list}) DO UPDATE SET {set_clause} '
        f'WHERE EXCLUDED."_loaded_at" >= silver.{table}."_loaded_at"'
    )
    frame = df[columns].astype(object).where(pd.notnull(df[columns]), None)
    records = frame.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(sql, records)


def _log_rejects(engine, table: str, df: pd.DataFrame, reason: str) -> None:
    if df.empty:
        return
    records = []
    for _, row in df.iterrows():
        payload = {k: (None if pd.isna(v) else str(v)) for k, v in row.items()}
        records.append(
            {
                "t": table,
                "r": reason,
                "j": json.dumps(payload),
                "sf": row.get("_source_file"),
            }
        )
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO silver.rejects (table_name, reason, row_json, source_file) "
                "VALUES (:t, :r, CAST(:j AS JSONB), :sf)"
            ),
            records,
        )


def transform_table(table: str, engine=None) -> dict:
    engine = engine or get_engine()
    cfg = TABLE_CONFIGS[table]

    with engine.begin() as conn:
        watermark = _get_watermark(conn, table)

    df = pd.read_sql(
        text(f"SELECT * FROM bronze.{table} WHERE _loaded_at > :wm ORDER BY _loaded_at"),
        engine,
        params={"wm": watermark},
    )
    if df.empty:
        return {"table": table, "rows_upserted": 0, "rows_rejected": 0}

    max_loaded_at = df["_loaded_at"].max()

    df = _clean(df, table, cfg)
    ok, rejects = _split_missing_pk(df, cfg["pk"])
    ok = ok.sort_values("_loaded_at").drop_duplicates(subset=list(cfg["pk"]), keep="last")

    _upsert(engine, table, cfg, ok)
    _log_rejects(engine, table, rejects, reason="missing_primary_key")

    with engine.begin() as conn:
        conn.execute(
            text("UPDATE silver.load_log SET last_loaded_at = :m WHERE table_name = :t"),
            {"m": max_loaded_at, "t": table},
        )

    logger.info("silver.%s: upserted %d row(s), rejected %d row(s)", table, len(ok), len(rejects))
    return {"table": table, "rows_upserted": len(ok), "rows_rejected": len(rejects)}


def transform_all() -> list:
    engine = get_engine()
    return [transform_table(t, engine) for t in ALL_TABLES]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(transform_all())
