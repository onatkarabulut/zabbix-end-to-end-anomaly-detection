"""Egitim / ETL tetikleme ve DAG durumu (Airflow REST uzerinden)."""

import requests as _requests
from fastapi import APIRouter, HTTPException

from .. import config
from ..models import airflow_client
from ..views.schemas import TrainRequest

router = APIRouter(prefix="/api/v1", tags=["train"])


def _airflow_guard(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except _requests.HTTPError as e:
        code = e.response.status_code if e.response is not None else 502
        detail = e.response.text[:300] if e.response is not None else str(e)
        raise HTTPException(code, f"Airflow hatasi: {detail}")
    except _requests.RequestException as e:
        raise HTTPException(503, f"Airflow'a erisilemiyor: {e}")


@router.post("/train")
def trigger_train(body: TrainRequest | None = None):
    """Retrain DAG'ini tetikler (compute_split -> check -> retrain ->
    audit -> promote zinciri)."""
    conf = body.conf if body else {}
    run = _airflow_guard(airflow_client.trigger_dag,
                         config.RETRAIN_DAG_ID, conf)
    return {"message": "retrain tetiklendi", "dag_id": config.RETRAIN_DAG_ID,
            "dag_run_id": run.get("dag_run_id"), "state": run.get("state")}


@router.get("/train/status")
def train_status():
    """Son retrain calismasinin durumu (task bazinda)."""
    runs = _airflow_guard(airflow_client.dag_runs, config.RETRAIN_DAG_ID, 1)
    if not runs:
        return {"message": "hic retrain calismasi yok"}
    last = runs[0]
    tasks = _airflow_guard(airflow_client.task_instances,
                           config.RETRAIN_DAG_ID, last["dag_run_id"])
    return {"dag_run_id": last["dag_run_id"], "state": last["state"],
            "start_date": last.get("start_date"),
            "end_date": last.get("end_date"), "tasks": tasks}


@router.get("/train/history")
def train_history(limit: int = 10):
    """Son N retrain calismasi."""
    runs = _airflow_guard(airflow_client.dag_runs,
                          config.RETRAIN_DAG_ID, limit)
    return {"runs": [
        {"dag_run_id": r["dag_run_id"], "state": r["state"],
         "start_date": r.get("start_date"), "end_date": r.get("end_date")}
        for r in runs]}


@router.post("/etl/trigger")
def trigger_etl():
    """Saatlik ETL DAG'ini manuel tetikler."""
    run = _airflow_guard(airflow_client.trigger_dag, config.ETL_DAG_ID)
    return {"message": "etl tetiklendi", "dag_id": config.ETL_DAG_ID,
            "dag_run_id": run.get("dag_run_id"), "state": run.get("state")}


@router.get("/dags")
def dags():
    """Airflow DAG listesi ve durumlari."""
    return {"dags": _airflow_guard(airflow_client.list_dags)}
