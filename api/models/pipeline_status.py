"""Boru hatti saglik durumu: SQLite, Redis stream, Airflow, aktif model."""

import os
import time

from .. import config
from . import airflow_client, registry
from .database import connect_ro


def _warehouse_status() -> dict:
    try:
        with connect_ro(config.WAREHOUSE_DB) as conn:
            r = conn.execute(
                "SELECT COUNT(*) AS n, MAX(datetime_minute) AS latest "
                "FROM ml_features_enriched").fetchone()
        return {"ok": True, "rows": r["n"], "latest_minute": r["latest"]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _realtime_status() -> dict:
    try:
        with connect_ro(config.REALTIME_DB) as conn:
            r = conn.execute(
                "SELECT COUNT(*) AS n, MAX(created_at) AS latest "
                "FROM alerts").fetchone()
        return {"ok": True, "alerts": r["n"], "latest_alert": r["latest"]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _redis_status() -> dict:
    try:
        import redis
        r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT,
                        socket_timeout=3, decode_responses=True)
        stream_len = r.xlen(config.STREAM_KEY)
        out = {"ok": True, "stream_len": stream_len}
        try:
            groups = r.xinfo_groups(config.STREAM_KEY)
            out["groups"] = [
                {"name": g.get("name"), "pending": g.get("pending"),
                 "lag": g.get("lag")} for g in groups]
        except Exception:
            out["groups"] = []
        return out
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _model_status() -> dict:
    cur = registry.read_current()
    run_id = cur.get("run_id")
    if not run_id:
        return {"ok": False, "error": "aktif model yok (current.json bos)"}
    run_dir = os.path.join(registry.RUNS_DIR, run_id)
    return {"ok": os.path.isdir(run_dir), "run_id": run_id,
            "active_at": cur.get("active_at")}


def full_status() -> dict:
    return {
        "timestamp": int(time.time()),
        "warehouse": _warehouse_status(),
        "realtime": _realtime_status(),
        "redis_stream": _redis_status(),
        "airflow": airflow_client.health(),
        "active_model": _model_status(),
    }
