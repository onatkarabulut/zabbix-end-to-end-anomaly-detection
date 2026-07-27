import sys
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

sys.path.insert(0, '/opt/airflow')

from ETL.extract import run_extraction_pipeline
from ETL.transform import run_transform_pipeline
from ETL.load import run_load_pipeline

logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='zabbix_ml_pipeline',
    default_args=default_args,
    description='Zabbix metriklerini detayli adimlarla ML feature matrisine cevirir',
    schedule='@hourly',
    start_date=datetime(2026, 7, 22),
    catchup=False,
    tags=['zabbix', 'machine-learning', 'etl', 'faz-1']
)
def zabbix_etl_dag():

    @task(task_id='extract_zabbix_data')
    def extract_task(data_interval_start=None, data_interval_end=None) -> str:
        start_ts = int(data_interval_start.timestamp())
        end_ts = int(data_interval_end.timestamp())
        logger.info(f"==== EXTRACT BASLADI: {start_ts} -> {end_ts} ====")
        try:
            run_extraction_pipeline(start_ts, end_ts)
            chunk_id = f"chunk_{start_ts}_{end_ts}"
            return chunk_id
        except Exception as e:
            raise AirflowException(f"Extract failed: {e}")

    @task(task_id='validate_raw_parquet')
    def validate_task(chunk_id: str) -> str:
        logger.info(f"==== RAW VERI DOGRULAMA: {chunk_id} ====")
        return chunk_id

    @task(task_id='transform_to_feature_matrix')
    def transform_task(chunk_id: str) -> str:
        logger.info(f"==== TRANSFORM (FEATURE ENGINEERING) BASLADI: {chunk_id} ====")
        try:
            run_transform_pipeline(chunk_id)
            return chunk_id
        except Exception as e:
            raise AirflowException(f"Transform failed: {e}")

    @task(task_id='load_to_sqlite_warehouse')
    def load_task(chunk_id: str):
        logger.info(f"==== LOAD (SQLITE) BASLADI: {chunk_id} ====")
        try:
            run_load_pipeline(chunk_id)
            logger.info("Load islemi basariyla tamamlandi.")
        except Exception as e:
            raise AirflowException(f"Load failed: {e}")

    chunk = extract_task()
    validated_chunk = validate_task(chunk)
    transformed_chunk = transform_task(validated_chunk)
    load_task(transformed_chunk)

pipeline = zabbix_etl_dag()