import os
import logging
import pandas as pd
import boto3
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

REQUIRED_PER_CHUNK = [
    ("history", "history/{chunk_id}.parquet"),
]

OPTIONAL_PER_CHUNK = [
    ("trends", "trends/{chunk_id}.parquet"),
    ("events", "events/{chunk_id}.parquet"),
    ("problem", "problem/{chunk_id}.parquet"),
]

REQUIRED_SNAPSHOTS = [
    ("hosts", "hosts/latest_hosts.parquet"),
    ("items", "items/latest_items.parquet"),
    ("triggers", "triggers/latest_triggers.parquet"),
    ("functions", "functions/latest_functions.parquet"),
]


class ZabbixValidator:
    def __init__(self, minio_endpoint, minio_access_key, minio_secret_key, raw_bucket="zabbix-raw-data"):
        self.raw_bucket = raw_bucket
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

    def _object_exists(self, file_key):
        try:
            self.s3_client.head_object(Bucket=self.raw_bucket, Key=file_key)
            return True
        except Exception:
            return False

    def _read_row_count(self, file_key):
        try:
            s3_path = f"s3://{self.raw_bucket}/{file_key}"
            return len(pd.read_parquet(s3_path, storage_options=self.storage_options, columns=None))
        except Exception as e:
            logging.warning(f"{file_key} okunamadi: {e}")
            return 0

    def validate_chunk(self, chunk_id: str) -> bool:
        errors = []
        warnings = []

        for table_name, pattern in REQUIRED_PER_CHUNK:
            key = pattern.format(chunk_id=chunk_id)
            if not self._object_exists(key):
                errors.append(f"{key}: dosya YOK")
            else:
                rows = self._read_row_count(key)
                if rows == 0:
                    errors.append(f"{key}: 0 satir (bos parquet)")

        for table_name, pattern in OPTIONAL_PER_CHUNK:
            key = pattern.format(chunk_id=chunk_id)
            if self._object_exists(key):
                rows = self._read_row_count(key)
                if rows == 0:
                    warnings.append(f"{key}: 0 satir")
            else:
                warnings.append(f"{key}: yok (saat icinde veri olmamasi normal)")

        for table_name, key in REQUIRED_SNAPSHOTS:
            if not self._object_exists(key):
                errors.append(f"{key}: snapshot YOK")

        for w in warnings:
            logging.warning(f"[validate] {w}")
        for e in errors:
            logging.error(f"[validate] {e}")

        if errors:
            logging.error(f"Validasyon basarisiz: {len(errors)} hata")
            return False

        logging.info(f"Validasyon basarili: {chunk_id}")
        return True


def run_validation_pipeline(chunk_id: str) -> bool:
    validator = ZabbixValidator(
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    )
    return validator.validate_chunk(chunk_id)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        success = run_validation_pipeline(sys.argv[1])
        logging.info(f"Validation {'basarili' if success else 'basarisiz'}: {sys.argv[1]}")
    else:
        logging.warning("Lutfen parametre olarak bir chunk_id giriniz.")
