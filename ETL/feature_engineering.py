import os
import json
import logging
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ROLLING_WINDOWS = [5, 30, 60]

ENRICHED_TABLE = "ml_features_enriched"

ALARM_LABEL_WINDOW_MIN = int(os.getenv("ALARM_LABEL_WINDOW_MIN", "10"))

GROUND_TRUTH_FILE = os.getenv(
    "GROUND_TRUTH_FILE",
    os.path.join(os.path.dirname(__file__), "..", "data", "ground_truth.jsonl"),
)

DEFAULT_HOST = "Zabbix server"

TARGET_COLUMNS = ("is_anomaly",)

METRIC_PREFIXES = (
    "system.cpu", "vm.memory", "vfs.dev", "vfs.fs",
    "system.swap",
    "zabbix[process", "zabbix[preprocessing", "zabbix[vcache",
    "zabbix[rcache", "zabbix[tcache", "zabbix[wcache", "zabbix[vps"
)


def _is_metric(col: str) -> bool:
    if col in ("datetime_minute", "host") or "_hourly_" in col:
        return False
    return any(col.startswith(p) for p in METRIC_PREFIXES)


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


def _read_ground_truth_windows():
    windows = defaultdict(list)
    if not os.path.exists(GROUND_TRUTH_FILE):
        return windows
    try:
        with open(GROUND_TRUTH_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                host = rec.get("host") or DEFAULT_HOST
                windows[host].append(
                    (
                        np.datetime64(pd.Timestamp(rec["start_ts"], unit="s"), "ns"),
                        np.datetime64(pd.Timestamp(rec["end_ts"], unit="s"), "ns"),
                    )
                )
    except Exception as e:
        logging.warning(f"ground_truth.jsonl okunamadi (is_anomaly sadece alarmlardan): {e}")
    return windows


def _interval_mask(minutes, intervals):
    mask = np.zeros(len(minutes), dtype=bool)
    for start, end in intervals:
        mask |= (minutes >= start) & (minutes < end)
    return mask


def compute_enriched_features(df: pd.DataFrame, db_path: str = None) -> pd.DataFrame:
    df = df.sort_values(["host", "datetime_minute"]).reset_index(drop=True)
    metric_cols = [c for c in df.columns if _is_metric(c)]
    logging.info(f"Feature engineering: {len(metric_cols)} metrik, {len(df)} satir")

    enriched = df[["datetime_minute", "host"]].copy()
    enriched["datetime_minute"] = pd.to_datetime(
        enriched["datetime_minute"]
    ).astype("datetime64[ns]")

    rolling_cols = []
    for col in metric_cols:
        for w in ROLLING_WINDOWS:
            rolling_cols.append(df.groupby("host")[col].transform(
                lambda x: x.rolling(w, min_periods=1).mean()
            ).rename(f"{col}_avg_{w}m"))
            rolling_cols.append(df.groupby("host")[col].transform(
                lambda x: x.rolling(w, min_periods=1).std().fillna(0)
            ).rename(f"{col}_std_{w}m"))
            rolling_cols.append(df.groupby("host")[col].transform(
                lambda x: _rolling_trend(x, w)
            ).rename(f"{col}_trend_{w}m"))
    if rolling_cols:
        enriched = pd.concat([enriched, pd.concat(rolling_cols, axis=1)], axis=1)

    alarm_intervals = defaultdict(list)
    if db_path is not None:
        try:
            with sqlite3.connect(db_path) as conn:
                events_df = pd.read_sql(
                    "SELECT e.eventid, e.clock, h.host "
                    "FROM events e "
                    "LEFT JOIN triggers t ON e.objectid = t.triggerid "
                    "LEFT JOIN functions f ON t.triggerid = f.triggerid "
                    "LEFT JOIN items i ON f.itemid = i.itemid "
                    "LEFT JOIN hosts h ON i.hostid = h.hostid "
                    "WHERE e.source = 0 AND e.value = 1 "
                    "ORDER BY h.host, e.clock",
                    conn
                )
        except Exception as e:
            logging.warning(f"events tablosu okunamadi (time_since_last_alarm atlaniyor): {e}")
            events_df = pd.DataFrame()

        if events_df.empty:
            try:
                with sqlite3.connect(db_path) as conn:
                    events_df = pd.read_sql(
                        "SELECT p.eventid, p.clock, h.host "
                        "FROM problem p "
                        "LEFT JOIN triggers t ON p.objectid = t.triggerid "
                        "LEFT JOIN functions f ON t.triggerid = f.triggerid "
                        "LEFT JOIN items i ON f.itemid = i.itemid "
                        "LEFT JOIN hosts h ON i.hostid = h.hostid "
                        "WHERE p.clock IS NOT NULL "
                        "ORDER BY h.host, p.clock",
                        conn
                    )
                logging.info("events tablosu bos, problem tablosu kullanildi.")
            except Exception as e:
                logging.warning(f"problem tablosu okunamadi (time_since_last_alarm atlaniyor): {e}")
                events_df = pd.DataFrame()

        if not events_df.empty:
            events_df = events_df.dropna(subset=["host"])
            events_df = events_df.drop_duplicates(subset=["eventid", "host"])
            clock = events_df["clock"]
            if pd.api.types.is_numeric_dtype(clock.dtype):
                events_df["clock_dt"] = pd.to_datetime(clock, unit="s", errors="coerce")
            else:
                events_df["clock_dt"] = pd.to_datetime(clock, errors="coerce")
            events_df = events_df.dropna(subset=["clock_dt"])
            alarms = events_df[["host", "clock_dt"]].drop_duplicates().sort_values("clock_dt")
            alarms["datetime_minute"] = alarms["clock_dt"]

            for host, clock_dt in zip(alarms["host"], alarms["clock_dt"]):
                alarm_intervals[host].append(
                    (
                        np.datetime64(pd.Timestamp(clock_dt), "ns"),
                        np.datetime64(pd.Timestamp(clock_dt) + timedelta(minutes=ALARM_LABEL_WINDOW_MIN), "ns"),
                    )
                )

            enriched = enriched.sort_values(["host", "datetime_minute"])
            alarms = alarms.sort_values(["host", "datetime_minute"])

            enriched["datetime_minute"] = pd.to_datetime(
                enriched["datetime_minute"]
            ).astype("datetime64[ns]")
            alarms["datetime_minute"] = pd.to_datetime(
                alarms["datetime_minute"]
            ).astype("datetime64[ns]")

            enriched = pd.merge_asof(
                enriched,
                alarms[["host", "datetime_minute", "clock_dt"]],
                on="datetime_minute",
                by="host",
                direction="backward"
            )

            enriched["time_since_last_alarm"] = (
                enriched["datetime_minute"] - enriched["clock_dt"]
            ).dt.total_seconds() / 60.0
            enriched["time_since_last_alarm"] = (
                enriched["time_since_last_alarm"].fillna(999999)
            )
            enriched = enriched.drop(columns=["clock_dt"])
            logging.info("time_since_last_alarm hesaplandi (point-in-time, data leakage yok)")
        else:
            enriched["time_since_last_alarm"] = 999999

    gt_windows = _read_ground_truth_windows()
    if gt_windows:
        for host, intervals in gt_windows.items():
            alarm_intervals[host].extend(intervals)

    is_anomaly = np.zeros(len(enriched), dtype=float)
    for host, group in enriched.groupby("host", sort=False):
        minutes = group["datetime_minute"].to_numpy(dtype="datetime64[ns]")
        intervals = alarm_intervals.get(host, [])
        if intervals:
            is_anomaly[group.index] = _interval_mask(minutes, intervals).astype(float)
    enriched["is_anomaly"] = is_anomaly
    n_pos = int(is_anomaly.sum())
    logging.info(f"is_anomaly hesaplandi: {n_pos} satir etiketli ({len(alarm_intervals)} host).")

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
    df_enriched = compute_enriched_features(df, db_path=db_path)

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
            col_names = [f'"{c}"' for c in rows_to_write.columns]
            cols_sql = ", ".join(col_names)
            conn.execute(f"""
                INSERT OR REPLACE INTO {ENRICHED_TABLE} ({cols_sql})
                SELECT {cols_sql} FROM {ENRICHED_TABLE}_staging
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