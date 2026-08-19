import os
import sys
import logging
import pendulum
import sqlite3
import subprocess
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowSkipException

logger = logging.getLogger("airflow.task")

# Airflow container'inda ml/ ve tools/ mount edilmis; script'ler sys.path'te.
# AIRFLOW_HOME dis makinelerde override edilebilir (portabilite).
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
sys.path.insert(0, AIRFLOW_HOME)

LOCAL_TZ = pendulum.timezone("Europe/Istanbul")
DB_PATH = os.getenv("SQLITE_DB_PATH", os.path.join(AIRFLOW_HOME, "data", "zabbix_ml.db"))
MODELS_DIR = os.getenv("ML_MODELS_DIR", os.path.join(AIRFLOW_HOME, "data", "models"))

# Split, SABIT bir tarih degil 'now - SPLIT_OFFSET_DAYS' olarak kayar.
# Boylece archive DAG'inin retention'ina ragmen egitim verisi hep tam kalir
# ve model surekli guncel veriyle yeniden egitilir.
SPLIT_OFFSET_DAYS = int(os.getenv("SPLIT_OFFSET_DAYS", "14"))

# Retrain tetiklenmesi icin split sonrasi birikmesi gereken minimum test
# dakikasi. Asil egitim split oncesi (train) + sonrasi (test) veriyle yapilir.
MIN_TEST_MINUTES = int(os.getenv("MIN_TEST_MINUTES", "1440"))  # 1 gun

# Retrain hiperparametreleri
THR_QUANTILE = float(os.getenv("THR_QUANTILE", "0.9995"))
TOP_K = int(os.getenv("TOP_K", "150"))
SEQ_LEN = int(os.getenv("SEQ_LEN", "60"))
EPOCHS = int(os.getenv("EPOCHS", "30"))

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'retries': 0,
}


@dag(
    dag_id='zabbix_ml_retrain',
    default_args=default_args,
    schedule='@daily',
    # start_date: Airflow yalnizca gecmis bir tarihle planlama yapar.
    # Uzak-gemis sentinel kullanarak DAG ne zaman kurulursa kurulsun ayni
    # davranir; gercek islem tarihleri hep now-relative hesaplanir.
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=['zabbix', 'machine-learning', 'retrain', 'faz-3'],
)
def zabbix_retrain_dag():

    @task(task_id='compute_split')
    def compute_split() -> int:
        """Split noktasi: now - SPLIT_OFFSET_DAYS (UTC epoch). Surekli kayar."""
        split = pendulum.now('UTC').subtract(days=SPLIT_OFFSET_DAYS)
        split_ts = int(split.start_of('hour').timestamp())
        logger.info("Split noktasi (dinamik): %s = %d", split.format('YYYY-MM-DD HH:mm'), split_ts)
        return split_ts

    @task(task_id='check_data_sufficiency')
    def check_data_sufficiency(split_ts: int) -> int:
        """Split sonrasi test penceresinde MIN_TEST_MINUTES satir var mi?"""
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM ml_feature_matrix WHERE datetime_minute >= ?",
                (pendulum.from_timestamp(split_ts, tz='UTC').format('YYYY-MM-DD HH:mm:ss'),)
            )
            n_test = cur.fetchone()[0]
        finally:
            conn.close()
        logger.info("Split sonrasi test satiri: %s (gereken min: %s)", n_test, MIN_TEST_MINUTES)
        if n_test < MIN_TEST_MINUTES:
            raise AirflowSkipException(
                f"Yeterli yeni veri yok: test={n_test} < min={MIN_TEST_MINUTES} dakika"
            )
        return n_test

    @task(task_id='retrain_models')
    def retrain_task(split_ts: int, n_test: int) -> str:
        """Yeni run olusturur (promote ETMEZ)."""
        run_id = f"run_retrain_{pendulum.now('UTC').format('YYYYMMDD_HHmm')}"
        cmd = [
            sys.executable, os.path.join(AIRFLOW_HOME, 'ml', 'train_anomaly_models.py'),
            '--db', DB_PATH,
            '--split-ts', str(split_ts),
            '--thr-quantile', str(THR_QUANTILE),
            '--top-k', str(TOP_K),
            '--seq-len', str(SEQ_LEN),
            '--epochs', str(EPOCHS),
            '--run-id', run_id,
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, cwd=AIRFLOW_HOME)
        if proc.returncode != 0:
            raise AirflowException(f"Retrain basarisiz\n{proc.stdout}\n{proc.stderr}")
        logger.info("Retrain tamamlandi: %s\n%s", run_id, proc.stdout[-2000:])
        return run_id

    @task(task_id='audit_new_model')
    def audit_task(new_run_id: str, split_ts: int) -> str:
        """Yeni run'i denetler; FAIL varsa AirflowException ile promote'i engeller."""
        run_dir = os.path.join(MODELS_DIR, "runs", new_run_id)
        cmd = [
            sys.executable, os.path.join(AIRFLOW_HOME, 'tools', 'model_audit.py'),
            '--db', DB_PATH,
            '--split-ts', str(split_ts),
            '--top-k', str(TOP_K),
            '--thr-quantile', str(THR_QUANTILE),
            '--seq-len', str(SEQ_LEN),
            '--run-dir', run_dir,
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, cwd=AIRFLOW_HOME)
        if proc.returncode != 0:
            raise AirflowException(
                f"Audit basarisiz, run promote edilmeyecek:\n{proc.stdout}\n{proc.stderr}"
            )
        logger.info("Audit PASS (%s):\n%s", new_run_id, proc.stdout[-2000:])
        return new_run_id

    @task(task_id='promote_new_run')
    def promote_task(new_run_id: str) -> str:
        """Audit gectiyse yeni run'i aktif model yapar."""
        cmd = [
            sys.executable, os.path.join(AIRFLOW_HOME, 'tools', 'model_cli.py'),
            'promote', new_run_id,
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, cwd=AIRFLOW_HOME)
        if proc.returncode != 0:
            raise AirflowException(f"Promote basarisiz: {proc.stdout}\n{proc.stderr}")
        logger.info("Promote tamamlandi: %s", new_run_id)
        return new_run_id

    split_ts = compute_split()
    n_test = check_data_sufficiency(split_ts)
    run_id = retrain_task(split_ts, n_test)
    audited = audit_task(run_id, split_ts)
    promote_task(audited)


retrain_dag = zabbix_retrain_dag()