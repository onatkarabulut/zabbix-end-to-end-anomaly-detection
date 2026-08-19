"""Airflow REST API istemcisi - train/ETL tetikleme ve durum sorgulama.

Airflow 2.10 stable REST API kullanilir (basic auth). docker-compose'ta
webserver'a AIRFLOW__API__AUTH_BACKENDS=basic_auth eklenmis olmalidir.
"""

from datetime import datetime, timezone

import requests

from .. import config

_TIMEOUT = 15


def _auth():
    return (config.AIRFLOW_USER, config.AIRFLOW_PASSWORD)


def _url(path: str) -> str:
    return f"{config.AIRFLOW_API_URL.rstrip('/')}/{path.lstrip('/')}"


def health() -> dict:
    try:
        r = requests.get(_url("health"), auth=_auth(), timeout=_TIMEOUT)
        return {"ok": r.status_code == 200, "detail": r.json()}
    except Exception as e:
        return {"ok": False, "detail": str(e)}


def trigger_dag(dag_id: str, conf: dict | None = None) -> dict:
    """DAG run tetikler; benzersiz run_id uretir."""
    run_id = f"api__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
    r = requests.post(
        _url(f"dags/{dag_id}/dagRuns"),
        json={"dag_run_id": run_id, "conf": conf or {}},
        auth=_auth(), timeout=_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


def dag_runs(dag_id: str, limit: int = 10) -> list:
    r = requests.get(
        _url(f"dags/{dag_id}/dagRuns"),
        params={"limit": limit, "order_by": "-execution_date"},
        auth=_auth(), timeout=_TIMEOUT,
    )
    r.raise_for_status()
    return r.json().get("dag_runs", [])


def task_instances(dag_id: str, dag_run_id: str) -> list:
    r = requests.get(
        _url(f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"),
        auth=_auth(), timeout=_TIMEOUT,
    )
    r.raise_for_status()
    return [
        {"task_id": t.get("task_id"), "state": t.get("state"),
         "start_date": t.get("start_date"), "end_date": t.get("end_date"),
         "duration": t.get("duration")}
        for t in r.json().get("task_instances", [])
    ]


def list_dags() -> list:
    r = requests.get(
        _url("dags"), params={"limit": 100}, auth=_auth(), timeout=_TIMEOUT)
    r.raise_for_status()
    return [
        {"dag_id": d.get("dag_id"), "is_paused": d.get("is_paused"),
         "schedule": (d.get("schedule_interval") or {}).get("value")
         if isinstance(d.get("schedule_interval"), dict)
         else d.get("schedule_interval")}
        for d in r.json().get("dags", [])
    ]
