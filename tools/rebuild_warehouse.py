#!/usr/bin/env python3
"""
Warehouse'u (zabbix_ml.db) MinIO'daki kaynak verilerden sifirdan yeniden insa eder.

Bozuk/sifreli DB durumunda calistirin. Tablolari sirasiyla olusturur:
  ml_feature_matrix  <- zabbix-processed-data/features/*.parquet (89+ chunk)
  events/problem/triggers/functions/items/hosts <- zabbix-raw-data (load_raw_tables)
  ml_features_enriched <- feature_engineering full backfill (is_anomaly dahil)

Kullanim:
  python tools/rebuild_warehouse.py --db data/zabbix_ml.db
"""

import argparse
import logging
import os
import sqlite3
import sys

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ETL.load import ZabbixLoader
from ETL.feature_engineering import run_feature_engineering


def list_feature_files(loader):
    keys = []
    paginator = loader.s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=loader.processed_bucket, Prefix="features/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return sorted(keys)


def rebuild(db_path):
    loader = ZabbixLoader(
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        db_path=db_path,
    )

    logging.info("1/4: ml_feature_matrix MinIO processed chunk'lardan insa ediliyor...")
    keys = list_feature_files(loader)
    logging.info(f"{len(keys)} feature parquet bulundu.")
    loaded = 0
    for key in keys:
        s3_path = f"s3://{loader.processed_bucket}/{key}"
        try:
            df = pd.read_parquet(s3_path, storage_options=loader.storage_options)
        except Exception as e:
            logging.warning(f"{key} okunamadi (atlaniyor): {e}")
            continue
        if loader.load_to_sqlite(df, table_name="ml_feature_matrix"):
            loaded += 1
    logging.info(f"{loaded}/{len(keys)} chunk ml_feature_matrix'e islendi.")

    logging.info("2/4: Ham tablolar (events/problem/triggers/functions/items/hosts) yukleniyor...")
    if not loader.load_raw_tables():
        logging.error("Ham tablo yuklenemedi, durduruluyor.")
        return False

    logging.info("3/4: Feature engineering full backfill (is_anomaly dahil)...")
    if not run_feature_engineering(db_path):
        logging.error("Feature engineering basarisiz.")
        return False

    logging.info("4/4: Integrity kontrolu...")
    with sqlite3.connect(db_path) as conn:
        check = conn.execute("PRAGMA integrity_check").fetchone()[0]
    logging.info(f"integrity_check: {check}")

    with sqlite3.connect(db_path) as conn:
        for t in ["ml_feature_matrix", "ml_features_enriched", "events",
                  "problem", "triggers", "functions", "items", "hosts"]:
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            logging.info(f"  {t}: {n}")

    return check == "ok"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=os.getenv("SQLITE_DB_PATH", "/opt/airflow/data/zabbix_ml.db"))
    args = parser.parse_args()
    ok = rebuild(args.db)
    logging.info(f"Rebuild {'BASARILI' if ok else 'BASARISIZ'}: {args.db}")
    sys.exit(0 if ok else 1)
