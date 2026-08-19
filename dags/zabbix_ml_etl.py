import os
import sys
import logging
import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowSkipException

# AIRFLOW_HOME dis makinelerde override edilebilir (portabilite).
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
sys.path.insert(0, AIRFLOW_HOME)

from ETL.extract import run_extraction_pipeline
from ETL.transform import run_transform_pipeline
from ETL.load import run_load_pipeline, run_load_raw_pipeline
from ETL.validate import run_validation_pipeline
from ETL.feature_engineering import run_feature_engineering

logger = logging.getLogger("airflow.task")

LOCAL_TZ = pendulum.timezone("Europe/Istanbul")
MAX_BACKFILL_HOURS = int(os.getenv("MAX_BACKFILL_HOURS", "720"))

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'retries': 0,
}

@dag(
    dag_id='zabbix_ml_pipeline',
    default_args=default_args,
    schedule='@hourly',
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=['zabbix', 'machine-learning', 'etl', 'faz-2']
)
def zabbix_etl_dag():

    @task(task_id='extract_zabbix_data')
    def extract_task(data_interval_start=None, data_interval_end=None) -> str:
        try:
            start_local = data_interval_start.in_timezone(LOCAL_TZ)
            now_utc = pendulum.now('UTC')
            time_diff = now_utc - data_interval_start

            if time_diff.total_hours() > MAX_BACKFILL_HOURS:
                raise AirflowSkipException(
                    f"Geriye donuk tarama limiti ({MAX_BACKFILL_HOURS} saat). "
                    f"Atlaniyor: {start_local.format('YYYY-MM-DD HH:mm')}"
                )

            raw_start = int(data_interval_start.timestamp())
            raw_end = int(data_interval_end.timestamp())
            hour_start = (raw_start // 3600) * 3600
            start_ts = hour_start
            end_ts = hour_start + 3600
            chunk_id = f"chunk_{start_ts}_{end_ts}"

            has_data = run_extraction_pipeline(start_ts, end_ts)
            if not has_data:
                raise AirflowSkipException(
                    f"Veri yok: {start_local.format('YYYY-MM-DD HH:mm')}"
                )
            return chunk_id
        except AirflowSkipException:
            raise
        except Exception as e:
            raise AirflowException(f"Extract failed: {e}")

    @task(task_id='validate_raw_parquet')
    def validate_task(chunk_id: str) -> str:
        if not run_validation_pipeline(chunk_id):
            raise AirflowException(f"Validasyon basarisiz: {chunk_id}")
        return chunk_id

    @task(task_id='transform_to_feature_matrix')
    def transform_task(chunk_id: str) -> str:
        if not run_transform_pipeline(chunk_id):
            raise AirflowException(f"Transform basarisiz: {chunk_id}")
        return chunk_id

    @task(task_id='load_to_sqlite_warehouse')
    def load_task(chunk_id: str) -> str:
        if not run_load_pipeline(chunk_id):
            raise AirflowException(f"Load basarisiz: {chunk_id}")
        return chunk_id

    @task(task_id='load_raw_tables')
    def load_raw_task(loaded_chunk: str) -> str:
        if not run_load_raw_pipeline():
            raise AirflowException("Ham tablo yuklenemedi (events/problem/triggers/functions)")
        return "raw_tables_ok"

    @task(task_id='feature_engineering')
    def fe_task(chunk_id: str, raw_ok: str):
        db_path = os.getenv("SQLITE_DB_PATH", os.path.join(AIRFLOW_HOME, "data", "zabbix_ml.db"))
        parts = chunk_id.split("_")
        target_start = int(parts[1])
        target_end = int(parts[2])
        if not run_feature_engineering(db_path, target_start=target_start, target_end=target_end):
            raise AirflowException("Feature engineering basarisiz")

    chunk = extract_task()
    validated_chunk = validate_task(chunk)
    transformed_chunk = transform_task(validated_chunk)
    loaded_chunk = load_task(transformed_chunk)
    raw_ok = load_raw_task(loaded_chunk)
    fe_task(loaded_chunk, raw_ok)

pipeline = zabbix_etl_dag()
