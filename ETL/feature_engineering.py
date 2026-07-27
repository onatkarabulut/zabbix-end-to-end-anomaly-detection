import os
import logging
import sqlite3
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ROLLING_WINDOWS = [5, 30, 60]

ENRICHED_TABLE = "ml_features_enriched"

METRIC_PREFIXES = (
    "system.cpu", "vm.memory", "vfs.dev", "vfs.fs",
    "zabbix[process", "zabbix[preprocessing", "zabbix[vcache",
    "zabbix[rcache", "zabbix[tcache", "zabbix[wcache", "zabbix[vps"
)


def _is_metric(col: str) -> bool:
    return col not in ("datetime_minute", "host") and any(
        col.startswith(p) for p in METRIC_PREFIXES
    )


def _ensure_enriched_table(conn, columns):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {ENRICHED_TABLE} (
            datetime_minute TIMESTAMP,
            host TEXT,
            PRIMARY KEY (host, datetime_minute)
        )
    """)
    existing = {row[1] for row in conn.execute(
        f"PRAGMA table_info({ENRICHED_TABLE})"
    ).fetchall()}
    for col in columns:
        if col not in existing:
            col_type = "REAL"
            if col == "datetime_minute":
                col_type = "TIMESTAMP"
            elif col == "host":
                col_type = "TEXT"
            conn.execute(f'ALTER TABLE {ENRICHED_TABLE} ADD COLUMN "{col}" {col_type}')


def _rolling_trend(series, window):
    def _slope(y):
        if len(y) < 2:
            return 0.0
        return float((y.iloc[-1] - y.iloc[0]) / max(len(y), 1))
    return series.rolling(window, min_periods=2).apply(_slope, raw=False)


def compute_enriched_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["host", "datetime_minute"]).reset_index(drop=True)
    metric_cols = [c for c in df.columns if _is_metric(c)]
    logging.info(f"Feature engineering: {len(metric_cols)} metrik, {len(df)} satir")

    enriched = df[["datetime_minute", "host"]].copy()

    for col in metric_cols:
        for w in ROLLING_WINDOWS:
            enriched[f"{col}_avg_{w}m"] = df.groupby("host")[col].transform(
                lambda x: x.rolling(w, min_periods=1).mean()
            )
            enriched[f"{col}_std_{w}m"] = df.groupby("host")[col].transform(
                lambda x: x.rolling(w, min_periods=1).std().fillna(0)
            )
            enriched[f"{col}_trend_{w}m"] = df.groupby("host")[col].transform(
                lambda x: _rolling_trend(x, w)
            )

    logging.info(f"Enriched feature sayisi: {len(enriched.columns)}")
    return enriched


def run_feature_engineering(
    db_path: str,
    target_start: int = None,
    target_end: int = None,
) -> bool:
    buffer = max(ROLLING_WINDOWS)

    if target_start is not None and target_end is not None:
        chunk_begin = datetime.utcfromtimestamp(target_start)
        read_begin = chunk_begin - timedelta(minutes=buffer)
        chunk_end = datetime.utcfromtimestamp(target_end)

        with sqlite3.connect(db_path) as conn:
            df = pd.read_sql(
                "SELECT * FROM ml_feature_matrix "
                "WHERE datetime_minute >= ? AND datetime_minute < ? "
                "ORDER BY host, datetime_minute",
                conn,
                params=(
                    read_begin.strftime("%Y-%m-%d %H:%M:%S"),
                    chunk_end.strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        logging.info(
            f"Chunk modu: [{read_begin} → {chunk_end}) okundu, "
            f"sadece [{chunk_begin} → {chunk_end}) yazilacak"
        )
    else:
        with sqlite3.connect(db_path) as conn:
            df = pd.read_sql(
                "SELECT * FROM ml_feature_matrix ORDER BY host, datetime_minute",
                conn,
            )
        logging.info("Full backfill modu: tum veri isleniyor")

    if df.empty:
        logging.warning("ml_feature_matrix bos, feature engineering atlaniyor.")
        return False

    df["datetime_minute"] = pd.to_datetime(df["datetime_minute"])
    df_enriched = compute_enriched_features(df)

    if target_start is not None and target_end is not None:
        chunk_begin = pd.to_datetime(datetime.utcfromtimestamp(target_start))
        chunk_end = pd.to_datetime(datetime.utcfromtimestamp(target_end))
        mask = (df_enriched["datetime_minute"] >= chunk_begin) & (
            df_enriched["datetime_minute"] < chunk_end
        )
        rows_to_write = df_enriched[mask].copy()
        skipped = int((~mask).sum())
        logging.info(
            f"Buffer'dan {skipped} satir atlandi, "
            f"{len(rows_to_write)} satir yazilacak"
        )
    else:
        rows_to_write = df_enriched

    if rows_to_write.empty:
        logging.info("Yeni satir yok, yazma adimi atlaniyor.")
        return True

    try:
        with sqlite3.connect(db_path) as conn:
            _ensure_enriched_table(conn, rows_to_write.columns)
            rows_to_write.to_sql(
                f"{ENRICHED_TABLE}_staging", conn,
                if_exists="replace", index=False,
            )
            conn.execute(f"""
                INSERT OR REPLACE INTO {ENRICHED_TABLE}
                SELECT * FROM {ENRICHED_TABLE}_staging
            """)
            conn.execute(f"DROP TABLE {ENRICHED_TABLE}_staging")
    except Exception as e:
        logging.error(f"SQLite yazma hatasi: {e}")
        return False

    logging.info(
        f"Feature engineering tamam: {len(rows_to_write)} satir, "
        f"{len(rows_to_write.columns)} kolon → {ENRICHED_TABLE}"
    )
    return True


if __name__ == "__main__":
    db_path = os.getenv("SQLITE_DB_PATH", "/opt/airflow/data/zabbix_ml.db")
    success = run_feature_engineering(db_path)
    logging.info(f"Feature engineering {'basarili' if success else 'basarisiz'}")