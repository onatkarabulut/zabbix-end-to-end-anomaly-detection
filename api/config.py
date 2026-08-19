"""API yapilandirmasi - tamami env ile ezilebilir (git-clone sonrasi
docker compose ile calisir; host'ta calistirmak icin varsayilanlar yerel
dizinlere gore ayarlanmistir)."""

import os

# Proje koku (api/ klasorunun bir ustu)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _path(env_key: str, default_rel: str) -> str:
    val = os.environ.get(env_key)
    if val:
        return val
    return os.path.join(ROOT, default_rel)


# --- Veri kaynaklari ---
WAREHOUSE_DB = _path("SQLITE_DB_PATH", os.path.join("data", "zabbix_ml.db"))
REALTIME_DB = _path("REALTIME_DB_PATH", os.path.join("data", "realtime.db"))
MODELS_DIR = _path("ML_MODELS_DIR", os.path.join("data", "models"))
GROUND_TRUTH_FILE = _path("GROUND_TRUTH_FILE", os.path.join("data", "ground_truth.jsonl"))
LOAD_TESTS_DIR = _path("LOAD_TESTS_DIR", "load_tests")

# --- Airflow REST ---
AIRFLOW_API_URL = os.environ.get("AIRFLOW_API_URL", "http://localhost:8081/api/v1")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "admin")
RETRAIN_DAG_ID = os.environ.get("RETRAIN_DAG_ID", "zabbix_ml_retrain")
ETL_DAG_ID = os.environ.get("ETL_DAG_ID", "zabbix_ml_etl")
ARCHIVE_DAG_ID = os.environ.get("ARCHIVE_DAG_ID", "zabbix_ml_archive")

# --- Redis (stream sagligi icin) ---
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
STREAM_KEY = os.environ.get("STREAM_KEY", "zabbix_history")

# --- Geri bildirim dongusu (Faz 4) ---
# Skor esigi asilinca consumer bu API'ye POST atar; asagidaki URL doluysa
# bildirim Zabbix'e (veya baska bir webhook'a) iletilir.
ZABBIX_WEBHOOK_URL = os.environ.get("ZABBIX_WEBHOOK_URL", "")

# --- Sunucu ---
API_HOST = os.environ.get("API_HOST", "0.0.0.0")
API_PORT = int(os.environ.get("API_PORT", "8000"))
