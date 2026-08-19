"""FastAPI yonetim API'si smoke testleri.

Gercek veri dosyalarina (data/) degil, tmp_path'e kurulan mini fixture'lara
karsi calisir - CI/temiz klonda da gecer.
"""

import json
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient  # noqa: E402


@pytest.fixture()
def client(tmp_path, monkeypatch):
    # --- mini veri ambari ---
    wh = tmp_path / "zabbix_ml.db"
    conn = sqlite3.connect(wh)
    conn.execute("""CREATE TABLE ml_features_enriched (
        datetime_minute TIMESTAMP, host TEXT, "system.cpu.util" REAL,
        PRIMARY KEY (host, datetime_minute))""")
    conn.execute("""CREATE TABLE ml_feature_matrix (
        datetime_minute TIMESTAMP, host TEXT, "system.cpu.util" REAL,
        PRIMARY KEY (host, datetime_minute))""")
    for i in range(5):
        for tbl in ("ml_features_enriched", "ml_feature_matrix"):
            conn.execute(
                f"INSERT INTO {tbl} VALUES (?, ?, ?)",
                (f"2026-08-19 10:0{i}:00", "Zabbix server", 10.0 + i))
    conn.commit()
    conn.close()

    # --- mini realtime db ---
    rt = tmp_path / "realtime.db"
    conn = sqlite3.connect(rt)
    conn.execute("""CREATE TABLE alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, host TEXT,
        datetime_minute TEXT, model TEXT, score REAL, threshold REAL,
        source TEXT, created_at TEXT)""")
    conn.execute(
        "INSERT INTO alerts (host, datetime_minute, model, score, threshold,"
        " source, created_at) VALUES ('Zabbix server',"
        " '2026-08-19 10:00:00', 'lstm-autoencoder', 5.0, 3.0, 'realtime',"
        " '2026-08-19 10:01:00')")
    conn.commit()
    conn.close()

    # --- mini model registry ---
    models_dir = tmp_path / "models"
    run_dir = models_dir / "runs" / "run_test"
    run_dir.mkdir(parents=True)
    (models_dir / "current.json").write_text(
        json.dumps({"run_id": "run_test"}))
    (run_dir / "isolationforest.joblib").write_bytes(b"fake")
    (run_dir / "eval_results.csv").write_text(
        "model,thr_quantile,threshold,precision,recall,f1,n_pred_pos,"
        "n_test_rows,test_min_ts,test_max_ts,n_train_rows,train_min_ts,"
        "train_max_ts\n"
        "LSTM-Autoencoder,0.9995,3.19,0.11,0.73,0.19,198,3805,1,2,6007,1,2\n")
    (run_dir / "comp_vs_zabbix.csv").write_text(
        "model,window_start,window_end,label,latency_min,caught,"
        "detected_minutes,zabbix_alarm_overlap\n"
        "LSTM-Autoencoder,100,200,memory_pressure,3.0,True,12,False\n")

    # --- ground truth ---
    gt = tmp_path / "ground_truth.jsonl"
    gt.write_text(json.dumps({
        "anomaly_id": "ram-1", "type": "ram", "host": "Zabbix server",
        "label": "memory_pressure", "start_ts": 100, "end_ts": 200,
        "metadata": {}}) + "\n")

    from api import config
    monkeypatch.setattr(config, "WAREHOUSE_DB", str(wh))
    monkeypatch.setattr(config, "REALTIME_DB", str(rt))
    monkeypatch.setattr(config, "MODELS_DIR", str(models_dir))
    monkeypatch.setattr(config, "GROUND_TRUTH_FILE", str(gt))

    # registry modul-seviyesi yollari da guncellenmeli
    from api.models import registry
    monkeypatch.setattr(registry, "RUNS_DIR", str(models_dir / "runs"))
    monkeypatch.setattr(registry, "CURRENT_FILE",
                        str(models_dir / "current.json"))

    from api.main import app
    return TestClient(app)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_enriched_query(client):
    r = client.get("/api/v1/features/enriched",
                   params={"host": "Zabbix server", "limit": 3})
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 5
    assert len(body["rows"]) == 3
    assert body["rows"][0]["host"] == "Zabbix server"


def test_enriched_column_filter(client):
    r = client.get("/api/v1/features/enriched",
                   params={"columns": "system.cpu.util"})
    assert r.status_code == 200
    assert "system.cpu.util" in r.json()["rows"][0]


def test_enriched_bad_column(client):
    r = client.get("/api/v1/features/enriched", params={"columns": "yok"})
    assert r.status_code == 400


def test_hosts_and_columns(client):
    assert client.get("/api/v1/features/hosts").json()["hosts"] == [
        "Zabbix server"]
    cols = client.get("/api/v1/features/columns").json()["columns"]
    assert "system.cpu.util" in cols


def test_alerts(client):
    r = client.get("/api/v1/alerts")
    assert r.status_code == 200
    assert r.json()["total"] == 1
    s = client.get("/api/v1/alerts/summary", params={"hours": 24 * 365})
    assert s.status_code == 200


def test_models_list_and_detail(client):
    r = client.get("/api/v1/models")
    assert r.status_code == 200
    body = r.json()
    assert body["current"]["run_id"] == "run_test"
    assert body["runs"][0]["run_id"] == "run_test"

    d = client.get("/api/v1/models/run_test")
    assert d.status_code == 200
    assert d.json()["has_models"] is True

    assert client.get("/api/v1/models/olmayan").status_code == 404


def test_model_comparison(client):
    r = client.get("/api/v1/models/comparison")
    assert r.status_code == 200
    body = r.json()
    assert body["run_id"] == "run_test"
    assert body["summary"]["LSTM-Autoencoder"]["caught"] == 1


def test_promote_and_rollback(client):
    r = client.post("/api/v1/models/run_test/promote")
    assert r.status_code == 200
    assert r.json()["run_id"] == "run_test"
    r = client.post("/api/v1/models/olmayan/promote")
    assert r.status_code == 404


def test_ground_truth_crud(client):
    r = client.get("/api/v1/ground-truth")
    assert r.status_code == 200
    assert r.json()["total"] == 1

    r = client.post("/api/v1/ground-truth", json={
        "type": "cpu", "label": "cpu_burn", "host": "Zabbix server",
        "start_ts": 300, "end_ts": 400})
    assert r.status_code == 200
    assert client.get("/api/v1/ground-truth").json()["total"] == 2

    # gecersiz aralik
    r = client.post("/api/v1/ground-truth", json={
        "type": "cpu", "label": "x", "start_ts": 400, "end_ts": 300})
    assert r.status_code == 400


def test_loadtest_types(client):
    r = client.get("/api/v1/loadtests")
    assert r.status_code == 200
    types = {t["type"] for t in r.json()["tests"]}
    assert {"cpu", "ram", "disk", "net"} <= types


def test_reports_summary_and_metrics(client):
    r = client.get("/api/v1/reports/summary")
    assert r.status_code == 200
    body = r.json()
    assert body["run_id"] == "run_test"
    assert body["best_model_by_f1"]["model"] == "LSTM-Autoencoder"

    m = client.get("/api/v1/reports/metrics")
    assert m.status_code == 200
    assert m.json()["metrics"][0]["model"] == "LSTM-Autoencoder"


def test_reports_charts(client):
    for url in ("/api/v1/reports/charts/comparison",
                "/api/v1/reports/charts/latency",
                "/api/v1/reports/charts/scores?model=lstm-autoencoder",
                "/api/v1/reports/charts/alerts-timeline"):
        r = client.get(url)
        assert r.status_code == 200, url
        assert r.headers["content-type"] == "image/png"
        assert r.content[:8] == b"\x89PNG\r\n\x1a\n"


def test_report_html(client):
    r = client.get("/api/v1/reports/full")
    assert r.status_code == 200
    assert "Model Karsilastirma Raporu" in r.text


def test_architecture(client):
    r = client.get("/api/v1/reports/architecture")
    assert r.status_code == 200
    assert "flowchart" in r.json()["diagram"]


def test_feedback_roundtrip(client):
    r = client.post("/api/v1/feedback/anomaly", json={
        "host": "Zabbix server", "datetime_minute": "2026-08-19 10:05:00",
        "model": "lstm-autoencoder", "score": 9.9, "threshold": 3.0})
    assert r.status_code == 200
    assert r.json()["forwarded"] is False  # webhook tanimsiz

    log = client.get("/api/v1/feedback/log")
    assert log.status_code == 200
    assert len(log.json()["records"]) == 1
