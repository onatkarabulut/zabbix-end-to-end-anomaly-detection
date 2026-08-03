import io
import os
import time
import logging
import psycopg2
import pandas as pd
import pendulum
import boto3
from botocore.client import Config
from colorama import Fore, Style, init

init(autoreset=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ZabbixExtractor:
    def __init__(self, pg_host, pg_user, pg_pass, pg_db, minio_endpoint, minio_access_key, minio_secret_key, bucket_name="zabbix-raw-data"):
        self.pg_host = pg_host
        self.pg_user = pg_user
        self.pg_pass = pg_pass
        self.pg_db = pg_db
        self.bucket_name = bucket_name
        self.timeout_ms = int(os.getenv("EXTRACT_TIMEOUT_MS", "15000"))
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
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except Exception:
            self.s3_client.create_bucket(Bucket=self.bucket_name)

    def get_pg_connection(self):
        return psycopg2.connect(
            host=self.pg_host,
            user=self.pg_user,
            password=self.pg_pass,
            dbname=self.pg_db,
            connect_timeout=15
        )

    def _execute_query_to_df(self, query, description_prefix="Sorgu", page_size=None, attempts=3):
        if page_size is None:
            page_size = int(os.getenv("EXTRACT_PAGE_SIZE", "50000"))
        last_exc = None
        for attempt in range(1, attempts + 1):
            try:
                return self._execute_query_to_df_once(query, description_prefix, page_size)
            except psycopg2.errors.QueryCanceled:
                logging.error(f"{description_prefix} ZORLA DURDURULDU (Timeout - {self.timeout_ms}ms asildi).")
                raise
            except Exception as e:
                last_exc = e
                if attempt < attempts:
                    wait = 2 ** attempt
                    logging.warning(
                        f"{description_prefix} deneme {attempt}/{attempts} basarisiz "
                        f"({e}). {wait}s sonra tekrar denenecek."
                    )
                    time.sleep(wait)
        logging.error(f"{description_prefix} {attempts} denemeden sonra basarisiz: {last_exc}")
        raise last_exc

    def _execute_query_to_df_once(self, query, description_prefix="Sorgu", page_size=None):
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SET statement_timeout = {self.timeout_ms};")
                    all_rows = []
                    offset = 0
                    cols = None
                    while True:
                        paginated = f"{query} LIMIT {page_size} OFFSET {offset}"
                        cur.execute(paginated)
                        rows = cur.fetchall()
                        if not rows:
                            break
                        if cols is None:
                            cols = [desc[0] for desc in cur.description]
                        all_rows.extend(rows)
                        offset += page_size
                        if len(rows) < page_size:
                            break
                    if not all_rows:
                        logging.warning(f"{description_prefix} icin kayit bulunamadi.")
                        return None
                    logging.info(f"{description_prefix}: {len(all_rows)} satir {page_size}'lik {offset // page_size} sayfada getirildi.")
                    return pd.DataFrame(all_rows, columns=cols)
        except psycopg2.errors.QueryCanceled:
            logging.error(f"{description_prefix} ZORLA DURDURULDU (Timeout - {self.timeout_ms}ms asildi).")
            raise
        except Exception as e:
            logging.error(f"PostgreSQL Baglanti/Sorgu Hatasi: {e}")
            raise

    def push_to_minio(self, df, table_category, chunk_identifier):
        if df is None or df.empty:
            return False
        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, compression="snappy")
            buffer.seek(0)
            file_key = f"{table_category}/{chunk_identifier}.parquet"
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_key,
                Body=buffer.getvalue(),
                ContentType="application/octet-stream"
            )
            logging.info(f"{Fore.GREEN}Minio'ya yuklendi: {file_key}{Style.RESET_ALL}")
            return True
        except Exception as e:
            logging.error(f"Minio Yukleme Hatasi (Key: {table_category}/{chunk_identifier}): {e}")
            raise

    def extract_history_chunk(self, start_ts, end_ts):
        query = f"SELECT itemid, clock, value, ns FROM history WHERE clock >= {start_ts} AND clock < {end_ts}"
        return self._execute_query_to_df(query, "history")

    def extract_trends_chunk(self, start_ts, end_ts):
        query = f"SELECT itemid, clock, num, value_min, value_avg, value_max FROM trends WHERE clock >= {start_ts} AND clock < {end_ts}"
        return self._execute_query_to_df(query, "trends")

    def extract_events_chunk(self, start_ts, end_ts):
        query = f"SELECT eventid, source, object, objectid, clock, ns, value, name, severity FROM events WHERE clock >= {start_ts} AND clock < {end_ts}"
        return self._execute_query_to_df(query, "events")

    def extract_problems_chunk(self, start_ts, end_ts):
        query = f"SELECT eventid, objectid, clock, r_eventid, r_clock, name, severity FROM problem WHERE clock >= {start_ts} AND clock < {end_ts}"
        return self._execute_query_to_df(query, "problem")

    def extract_hosts(self):
        query = "SELECT hostid, host, name, status FROM hosts WHERE status IN (0, 1)"
        return self._execute_query_to_df(query, "hosts")

    def extract_items(self):
        query = "SELECT itemid, hostid, name, key_, value_type, status FROM items WHERE status = 0"
        return self._execute_query_to_df(query, "items")

    def extract_triggers(self):
        query = "SELECT triggerid, expression, description, priority, status FROM triggers"
        return self._execute_query_to_df(query, "triggers")

    def extract_functions(self):
        query = "SELECT functionid, itemid, triggerid, name, parameter FROM functions"
        return self._execute_query_to_df(query, "functions")


def run_extraction_pipeline(start_ts: int, end_ts: int) -> bool:
    start_str = pendulum.from_timestamp(start_ts).to_datetime_string()
    end_str = pendulum.from_timestamp(end_ts).to_datetime_string()
    logging.info(f"Extract basladi: {start_ts} ({start_str}) -> {end_ts} ({end_str})")

    extractor = ZabbixExtractor(
        pg_host=os.getenv("DB_HOST", "localhost"),
        pg_user=os.getenv("DB_USER", "zabbix"),
        pg_pass=os.getenv("DB_PASSWORD", "zabbix_sifresi"),
        pg_db=os.getenv("DB_NAME", "zabbix"),
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    )

    chunk_id = f"chunk_{start_ts}_{end_ts}"

    tables = [
        ("history", extractor.extract_history_chunk),
        ("trends", extractor.extract_trends_chunk),
        ("events", extractor.extract_events_chunk),
        ("problem", extractor.extract_problems_chunk)
    ]

    data_found = False
    for table_name, extract_func in tables:
        df = extract_func(start_ts, end_ts)
        if df is not None and not df.empty:
            logging.info(f"{table_name}: {len(df)} kayit bulundu.")
        success = extractor.push_to_minio(df, table_name, chunk_id)
        if success:
            data_found = True

    if not data_found:
        logging.warning(f"{start_ts} - {end_ts} araliginda veri bulunamadi.")
        return False

    snapshot_tables = [
        ("hosts", "latest_hosts", extractor.extract_hosts),
        ("items", "latest_items", extractor.extract_items),
        ("triggers", "latest_triggers", extractor.extract_triggers),
        ("functions", "latest_functions", extractor.extract_functions),
    ]
    for table_name, snapshot_id, extract_func in snapshot_tables:
        df = extract_func()
        extractor.push_to_minio(df, table_name, snapshot_id)

    return True


if __name__ == "__main__":
    now = int(pendulum.now('UTC').timestamp())
    one_hour_ago = now - 3600
    run_extraction_pipeline(start_ts=one_hour_ago, end_ts=now)
