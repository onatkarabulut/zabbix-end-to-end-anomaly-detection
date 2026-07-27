import os
import sys
import logging
import sqlite3
import pandas as pd
import boto3
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ZabbixLoader:
    def __init__(self, minio_endpoint, minio_access_key, minio_secret_key, db_path="zabbix_ml.db", processed_bucket="zabbix-processed-data"):
        self.processed_bucket = processed_bucket
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
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )

    def read_processed_data(self, chunk_id):
        s3_path = f"s3://{self.processed_bucket}/features/{chunk_id}_features.parquet"
        try:
            return pd.read_parquet(s3_path, storage_options=self.storage_options)
        except Exception as e:
            logging.error(f"Minio Okuma Hatasi ({chunk_id}): {e}")
            return None

    def load_to_sqlite(self, df, table_name="ml_feature_matrix"):
        if df is None or df.empty:
            return False

        try:
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql(table_name, conn, if_exists='append', index=False)
            logging.info(f"Load basarili: {len(df)} satir SQLite '{table_name}' tablosuna eklendi.")
            return True
        except Exception as e:
            logging.error(f"SQLite Yazma Hatasi: {e}")
            return False

def run_load_pipeline(chunk_id: str):
    loader = ZabbixLoader(
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        db_path=os.getenv("SQLITE_DB_PATH", "zabbix_ml.db")
    )
    
    feature_matrix = loader.read_processed_data(chunk_id)
    loader.load_to_sqlite(feature_matrix)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        chunk_param = sys.argv[1]
        run_load_pipeline(chunk_param)
    else:
        logging.warning("Lutfen terminalden parametre olarak bir chunk_id giriniz.")