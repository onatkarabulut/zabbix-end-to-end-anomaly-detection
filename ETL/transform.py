import os
import sys
import logging
import pandas as pd
import boto3
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ZabbixTransformer:
    def __init__(self, minio_endpoint, minio_access_key, minio_secret_key, raw_bucket="zabbix-raw-data", processed_bucket="zabbix-processed-data"):
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
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
        self._ensure_processed_bucket()

    def _ensure_processed_bucket(self):
        try:
            self.s3_client.head_bucket(Bucket=self.processed_bucket)
        except Exception:
            self.s3_client.create_bucket(Bucket=self.processed_bucket)

    def _s3_key_exists(self, bucket, file_key):
        try:
            self.s3_client.head_object(Bucket=bucket, Key=file_key)
            return True
        except Exception:
            return False

    def read_parquet_from_minio(self, file_key):
        s3_path = f"s3://{self.raw_bucket}/{file_key}"
        try:
            return pd.read_parquet(s3_path, storage_options=self.storage_options)
        except Exception as e:
            logging.warning(f"Okunamadi ({file_key}): {e}")
            return None

    def build_feature_matrix(self, chunk_id):
        history_key = f"history/{chunk_id}.parquet"
        trends_key = f"trends/{chunk_id}.parquet"
        items_key = "items/latest_items.parquet"
        hosts_key = "hosts/latest_hosts.parquet"

        if not self._s3_key_exists(self.raw_bucket, history_key):
            logging.warning(f"History verisi yok Minio'da: {history_key}")
            return None

        df_hist = self.read_parquet_from_minio(history_key)
        df_items = self.read_parquet_from_minio(items_key)
        df_hosts = self.read_parquet_from_minio(hosts_key)

        if any(df is None or df.empty for df in [df_hist, df_items, df_hosts]):
            return None

        df_merged = pd.merge(df_hist, df_items[['itemid', 'hostid', 'key_', 'name']], on='itemid', how='inner')
        df_merged = pd.merge(df_merged, df_hosts[['hostid', 'host']], on='hostid', how='inner')

        df_merged['datetime'] = pd.to_datetime(df_merged['clock'], unit='s')
        df_merged['datetime_minute'] = df_merged['datetime'].dt.floor('min')

        df_pivot = pd.pivot_table(
            df_merged,
            index=['datetime_minute', 'host'],
            columns='key_',
            values='value',
            aggfunc='mean'
        ).reset_index()

        df_pivot.columns.name = None
        df_pivot = df_pivot.sort_values(by=['host', 'datetime_minute'])

        df_trends = self.read_parquet_from_minio(trends_key)
        if df_trends is not None and not df_trends.empty:
            df_trends = pd.merge(df_trends, df_items[['itemid', 'hostid', 'key_']], on='itemid', how='inner')
            df_trends['datetime_hour'] = pd.to_datetime(df_trends['clock'], unit='s').dt.floor('h')

            for suffix, col in [('_hourly_avg', 'value_avg'), ('_hourly_min', 'value_min'), ('_hourly_max', 'value_max')]:
                trend_pivot = df_trends.pivot_table(
                    index=['datetime_hour', 'hostid'],
                    columns='key_',
                    values=col,
                    aggfunc='mean'
                ).reset_index()

                rename_cols = {
                    c: f"{c}{suffix}"
                    for c in trend_pivot.columns
                    if c not in ('datetime_hour', 'hostid')
                }
                trend_pivot = trend_pivot.rename(columns=rename_cols)
                trend_pivot = pd.merge(trend_pivot, df_hosts[['hostid', 'host']], on='hostid', how='inner')
                trend_pivot = trend_pivot.drop(columns=['hostid'])

                df_pivot['datetime_hour'] = df_pivot['datetime_minute'].dt.floor('h')
                df_pivot = pd.merge(df_pivot, trend_pivot, on=['datetime_hour', 'host'], how='left')
                df_pivot = df_pivot.drop(columns=['datetime_hour'])

            logging.info(f"Trends verisi eklendi: {chunk_id}")

        df_pivot = df_pivot.drop_duplicates(subset=['host', 'datetime_minute'])
        df_pivot = df_pivot.groupby('host').apply(lambda x: x.ffill(limit=3)).reset_index(drop=True)
        df_pivot = df_pivot.dropna(thresh=int(len(df_pivot.columns) * 0.7))

        return df_pivot

    def save_processed_data(self, df, chunk_id):
        if df is None or df.empty:
            return False
        s3_path = f"s3://{self.processed_bucket}/features/{chunk_id}_features.parquet"
        try:
            df.to_parquet(s3_path, index=False, storage_options=self.storage_options, compression="snappy")
            logging.info(f"Transform basarili: {s3_path} ({len(df)} satir)")
            return True
        except Exception as e:
            logging.error(f"Yazma Hatasi: {e}")
            return False


def run_transform_pipeline(chunk_id: str) -> bool:
    transformer = ZabbixTransformer(
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    )
    feature_matrix = transformer.build_feature_matrix(chunk_id)
    return transformer.save_processed_data(feature_matrix, chunk_id)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        chunk_param = sys.argv[1]
        success = run_transform_pipeline(chunk_param)
        logging.info(f"Transform {'basarili' if success else 'basarisiz'}: {chunk_param}")
    else:
        logging.warning("Lutfen terminalden parametre olarak bir chunk_id giriniz.")
