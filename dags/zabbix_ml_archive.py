import os
import sys
import logging
import pendulum
import sqlite3
import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowSkipException

logger = logging.getLogger("airflow.task")

# AIRFLOW_HOME dis makinelerde override edilebilir (portabilite).
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
sys.path.insert(0, AIRFLOW_HOME)

LOCAL_TZ = pendulum.timezone("Europe/Istanbul")
DB_PATH = os.getenv("SQLITE_DB_PATH", os.path.join(AIRFLOW_HOME, "data", "zabbix_ml.db"))

# ml_feature_matrix / ml_features_enriched kac gunden eski kaydedilecekse sil
RETENTION_DAYS = int(os.getenv("ML_RETENTION_DAYS", "30"))

# Arsiy dizini (yerel dosya). MinIO'ya yazmak icin MINIO_* env'leri + s3fs
# kurulu oldugundan ayni kod 's3://bucket/...' ile de calisir.
ARCHIVE_DIR = os.getenv("ML_ARCHIVE_DIR", os.path.join(AIRFLOW_HOME, "data", "archive"))

TABLES = ["ml_feature_matrix", "ml_features_enriched"]

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'retries': 0,
}


def _cutoff_str():
    """Retention sinirinin UTC timestring'i (datetime_minute TIMESTAMP ile karsilastirilir)."""
    return pendulum.now('UTC').subtract(days=RETENTION_DAYS).format('YYYY-MM-DD HH:mm:ss')


def _archive_storage_options():
    """MinIO'ya s3:// yazimi icin s3fs storage_options (transform/load ile ayni).

    MinIO yoksa (yerel test) None doner -> pandas pyarrow'un varsayilan S3
    davranisina gecer. MinIO varsa MINIO_* env'leri endpoint/credential olarak
    kullanilir; boylece parquet gercekten MinIO bucket'ina yazilir.
    """
    endpoint = os.getenv("MINIO_ENDPOINT")
    key = os.getenv("MINIO_ACCESS_KEY")
    secret = os.getenv("MINIO_SECRET_KEY")
    if not (endpoint and key and secret):
        return None
    return {
        "client_kwargs": {"endpoint_url": endpoint},
        "key": key,
        "secret": secret,
    }


def _archive_table(conn, table: str, cutoff: str) -> int:
    """table icin cutoff oncesi satirlari parquet'e yazar ve sayisini dondurur."""
    df = pd.read_sql_query(
        f"SELECT * FROM {table} WHERE datetime_minute < ?",
        conn, params=(cutoff,))
    if df.empty:
        logger.info("%s: silinecek veri yok (hepsi %.0f gun icinde)", table, RETENTION_DAYS)
        return 0
    # S3 yolu icin mkdir gerekmez (s3fs baglanti noktasinda otomatik olusur).
    if not ARCHIVE_DIR.startswith("s3://"):
        os.makedirs(ARCHIVE_DIR, exist_ok=True)
    stamp = pendulum.now('UTC').format('YYYYMMDD_HHmm')
    path = os.path.join(ARCHIVE_DIR, f"{table}_{stamp}.parquet")
    df.to_parquet(path, index=False, storage_options=_archive_storage_options())
    logger.info("%s: %d satir -> %s", table, len(df), path)
    return path


def _verify_archive(path: str, expected: int, conn, table: str) -> bool:
    """Parquet'i geri okuyup satir sayisini dogrular; uyusmazsa False.

    Silme oncesi zorunlu guvenlik adimi: yedek yazilmis ve okunabilir oldugu
    kanitlanmadan orijinal veriye dokunulmaz.
    """
    try:
        df = pd.read_parquet(path, storage_options=_archive_storage_options())
        ok = len(df) == expected
        logger.info("%s: dogrulama %s (parquet=%d, DB'den=%d)",
                    table, "OK" if ok else "HATA", len(df), expected)
        return ok
    except Exception as e:
        logger.error("%s: parquet geri okunamadi: %s", table, e)
        return False


def _count_before(conn, table: str, cutoff: str) -> int:
    return conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE datetime_minute < ?", (cutoff,)
    ).fetchone()[0]


def _delete_before(conn, table: str, cutoff: str) -> int:
    """Once parquet dogrulanir, sonra orijinal satirlar silinir (guvenli tasima)."""
    cur = conn.execute(f"DELETE FROM {table} WHERE datetime_minute < ?", (cutoff,))
    return cur.rowcount


@dag(
    dag_id='zabbix_ml_archive',
    default_args=default_args,
    schedule='@daily',
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=['zabbix', 'machine-learning', 'retention', 'faz-3'],
)
def zabbix_archive_dag():

    @task(task_id='archive_and_purge')
    def archive_purge() -> dict:
        """Eski satirlari parquet'e yaz, DB'den sil, VACUUM ile kucult."""
        cutoff = _cutoff_str()
        logger.info("Retention siniri: %s (%.0f gun)", cutoff, RETENTION_DAYS)

        conn = sqlite3.connect(DB_PATH)
        try:
            archived = {}
            for table in TABLES:
                path = _archive_table(conn, table, cutoff)
                if path:
                    n = _count_before(conn, table, cutoff)
                    if not _verify_archive(path, n, conn, table):
                        raise AirflowException(
                            f"{table}: parquet dogrulamasi basarisiz, orijinal veri "
                            f"SILINMEDI. {path} inceleyin."
                        )
                    deleted = _delete_before(conn, table, cutoff)
                    logger.info("%s: %d silindi", table, deleted)
                    archived[table] = {"archived": n, "deleted": deleted}
            conn.commit()
            # silinen sayfalar diskten geri kazanilir
            conn.execute("VACUUM")
            return {"cutoff": cutoff, "tables": archived}
        finally:
            conn.close()

    archive_purge()


archive_dag = zabbix_archive_dag()