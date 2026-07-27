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
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        self._ensure_processed_bucket()

    def _ensure_processed_bucket(self):
        try:
            self.s3_client.head_bucket(Bucket=self.processed_bucket)
        except Exception:
            self.s3_client.create_bucket(Bucket=self.processed_bucket)

    def read_parquet_from_minio(self, file_key):
        s3_path = f"s3://{self.raw_bucket}/{file_key}"
        try:
            return pd.read_parquet(s3_path, storage_options=self.storage_options)
        except Exception as e:
            logging.error(f"Okuma Hatasi ({file_key}): {e}")
            return None

    def build_feature_matrix(self, chunk_id):
        df_hist = self.read_parquet_from_minio(f"history/{chunk_id}.parquet")
        df_items = self.read_parquet_from_minio("items/latest_items.parquet")
        df_hosts = self.read_parquet_from_minio("hosts/latest_hosts.parquet")

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
        
        df_pivot = df_pivot.groupby('host').apply(lambda x: x.ffill(limit=3)).reset_index(drop=True)
        df_pivot = df_pivot.dropna(thresh=int(len(df_pivot.columns) * 0.7))

        return df_pivot

    def save_processed_data(self, df, chunk_id):
        if df is None or df.empty:
            return False

        s3_path = f"s3://{self.processed_bucket}/features/{chunk_id}_features.parquet"
        try:
            df.to_parquet(s3_path, index=False, storage_options=self.storage_options, compression="snappy")
            logging.info(f"Transform basarili: {s3_path}")
            return True
        except Exception as e:
            logging.error(f"Yazma Hatasi: {e}")
            return False

def run_transform_pipeline(chunk_id: str):
    transformer = ZabbixTransformer(
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    )
    feature_matrix = transformer.build_feature_matrix(chunk_id)
    transformer.save_processed_data(feature_matrix, chunk_id)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        chunk_param = sys.argv[1]
        run_transform_pipeline(chunk_param)
    else:
        logging.warning("Lutfen terminalden parametre olarak bir chunk_id giriniz.")