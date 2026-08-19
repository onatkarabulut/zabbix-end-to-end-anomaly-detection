import os
import sys
import logging
import sqlite3
import pandas as pd
import boto3
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROCESSED_TABLE = "_processed_chunks"

def _ensure_processed_table(db_path):
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {PROCESSED_TABLE} (
                chunk_id TEXT PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_count INTEGER DEFAULT 0
            )
        """)
        conn.commit()

def is_chunk_processed(chunk_id: str, db_path: str = None) -> bool:
    if db_path is None:
        db_path = os.getenv("SQLITE_DB_PATH", "data/zabbix_ml.db")
    _ensure_processed_table(db_path)
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(f"SELECT 1 FROM {PROCESSED_TABLE} WHERE chunk_id = ?", (chunk_id,))
        return cur.fetchone() is not None

def _mark_chunk_processed(chunk_id: str, row_count: int, db_path: str):
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"""
            INSERT OR IGNORE INTO {PROCESSED_TABLE} (chunk_id, row_count)
            VALUES (?, ?)
        """, (chunk_id, row_count))
        conn.commit()


class ZabbixLoader:
    def __init__(self, minio_endpoint, minio_access_key, minio_secret_key, db_path="zabbix_ml.db", processed_bucket="zabbix-processed-data", raw_bucket="zabbix-raw-data"):
        self.processed_bucket = processed_bucket
        self.raw_bucket = raw_bucket
        self.db_path = db_path
        self.storage_options = {
            "client_kwargs": {"endpoint_url": minio_endpoint},
            "key": minio_access_key,
            "secret": minio_secret_key
        }
        self.s3_client = boto3.client(
            's3',
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            config=Config(
                signature_version='s3v4',
                connect_timeout=30,
                read_timeout=60,
                retries={'max_attempts': 2}
            ),
            region_name='us-east-1'
        )

    def read_processed_data(self, chunk_id):
        s3_path = f"s3://{self.processed_bucket}/features/{chunk_id}_features.parquet"
        try:
            return pd.read_parquet(s3_path, storage_options=self.storage_options)
        except Exception as e:
            logging.warning(f"Minio'da islenmis veri yok ({chunk_id}): {e}")
            return None

    def _list_raw_objects(self, prefix):
        keys = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.raw_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    keys.append(obj["Key"])
        return sorted(keys)

    def _coerce_epoch(self, series):
        if pd.api.types.is_integer_dtype(series.dtype) or pd.api.types.is_float_dtype(series.dtype):
            return series.astype("float64")
        dt = pd.to_datetime(series, errors="coerce")
        return dt.astype("int64").floordiv(10**9)

    def _normalize_raw(self, df, pk_col, epoch_cols=("clock",)):
        if df is None or df.empty:
            return df
        df = df.drop_duplicates(subset=[pk_col], keep="last")
        for col in epoch_cols:
            if col in df.columns:
                df[col] = self._coerce_epoch(df[col])
        return df

    def _load_with_pk(self, conn, table_name, df, pk_col):
        if df is None or df.empty:
            logging.warning(f"{table_name}: bos veri, tablo degismedi.")
            return
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_{pk_col} "
            f"ON {table_name} ({pk_col})"
        )
        logging.info(
            f"Ham tablo guncellendi: {table_name} ({len(df)} satir, "
            f"PK: {pk_col})"
        )

    def load_raw_tables(self):
        with sqlite3.connect(self.db_path) as conn:
            for prefix, table_name, pk_col in [
                ("events/", "events", "eventid"),
                ("problem/", "problem", "eventid"),
            ]:
                frames = []
                for key in self._list_raw_objects(prefix):
                    s3_path = f"s3://{self.raw_bucket}/{key}"
                    try:
                        frames.append(pd.read_parquet(s3_path, storage_options=self.storage_options))
                    except Exception as e:
                        logging.warning(f"{key} okunamadi (atlaniyor): {e}")
                if not frames:
                    logging.warning(f"{table_name}: Minio'da veri yok.")
                    continue
                df = pd.concat(frames, ignore_index=True)
                df = self._normalize_raw(df, pk_col)
                self._load_with_pk(conn, table_name, df, pk_col)

            for table_name, key, pk_col in [
                ("hosts", "hosts/latest_hosts.parquet", "hostid"),
                ("items", "items/latest_items.parquet", "itemid"),
                ("triggers", "triggers/latest_triggers.parquet", "triggerid"),
                ("functions", "functions/latest_functions.parquet", "functionid"),
            ]:
                s3_path = f"s3://{self.raw_bucket}/{key}"
                try:
                    df = pd.read_parquet(s3_path, storage_options=self.storage_options)
                except Exception as e:
                    logging.error(f"{key} okunamadi: {e}")
                    return False
                self._load_with_pk(conn, table_name, df, pk_col)

        return True

    def _get_existing_columns(self, conn, table_name):
        try:
            cur = conn.execute(f"PRAGMA table_info({table_name})")
            return {row[1] for row in cur.fetchall()}
        except Exception:
            return set()

    def _dynamic_create_table(self, conn, table_name, df):
        existing_cols = self._get_existing_columns(conn, table_name)
        new_cols = set(df.columns) - existing_cols

        if new_cols:
            for col in new_cols:
                col_type = "REAL"
                if col in ("datetime_minute",):
                    col_type = "TIMESTAMP"
                elif col in ("host", "chunk_id"):
                    col_type = "TEXT"
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN \"{col}\" {col_type}")
            logging.info(f"Tabloya {len(new_cols)} yeni kolon eklendi: {', '.join(new_cols)}")

    def load_to_sqlite(self, df, table_name="ml_feature_matrix"):
        if df is None or df.empty:
            return False

        staging_table = f"{table_name}_staging"

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        datetime_minute TIMESTAMP,
                        host TEXT,
                        PRIMARY KEY (host, datetime_minute)
                    )
                """)

                self._dynamic_create_table(conn, table_name, df)

                df.to_sql(staging_table, conn, if_exists='replace', index=False)

                col_names = [f'"{c}"' for c in df.columns]
                cols_sql = ", ".join(col_names)
                cursor.execute(f"""
                    INSERT OR REPLACE INTO {table_name} ({cols_sql})
                    SELECT {cols_sql} FROM {staging_table}
                """)

                cursor.execute(f"DROP TABLE {staging_table}")

            logging.info(f"Load basarili: {len(df)} satir '{table_name}' tablosuna islendi.")
            return True

        except Exception as e:
            logging.error(f"SQLite Yazma Hatasi: {e}")
            return False


def run_load_pipeline(chunk_id: str) -> bool:
    db_path = os.getenv("SQLITE_DB_PATH", "data/zabbix_ml.db")

    if is_chunk_processed(chunk_id, db_path):
        logging.info(f"Chunk zaten islenmis, atlaniyor: {chunk_id}")
        return True

    loader = ZabbixLoader(
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        db_path=db_path
    )

    feature_matrix = loader.read_processed_data(chunk_id)
    if feature_matrix is None or feature_matrix.empty:
        logging.warning(f"Islenmis veri bos veya yok, isaretlenmiyor: {chunk_id}")
        return False

    success = loader.load_to_sqlite(feature_matrix)
    if success:
        _mark_chunk_processed(chunk_id, len(feature_matrix), db_path)
        logging.info(f"Chunk isaretlendi: {chunk_id} ({len(feature_matrix)} satir)")

    return success


def run_load_raw_pipeline() -> bool:
    db_path = os.getenv("SQLITE_DB_PATH", "data/zabbix_ml.db")

    loader = ZabbixLoader(
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        db_path=db_path
    )

    return loader.load_raw_tables()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        chunk_param = sys.argv[1]
        success = run_load_pipeline(chunk_param)
        logging.info(f"Load {'basarili' if success else 'basarisiz'}: {chunk_param}")
    else:
        logging.warning("Lutfen terminalden parametre olarak bir chunk_id giriniz.")
