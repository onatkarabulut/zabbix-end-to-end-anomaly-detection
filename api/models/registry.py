"""Model kayit defteri: runs listesi, aktif model, promote/rollback,
degerlendirme metrikleri ve Zabbix karsilastirmasi.

tools/model_cli.py ile ayni dosya duzenini kullanir (current.json + runs/).
"""

import csv
import json
import os
from datetime import datetime, timezone

from .. import config

RUNS_DIR = os.path.join(config.MODELS_DIR, "runs")
CURRENT_FILE = os.path.join(config.MODELS_DIR, "current.json")

# Bir run'in "model iceriyor" sayilmasi icin en az bir tanesi bulunmali
_MODEL_FILES = ("isolationforest.joblib", "oneclasssvm.joblib",
                "lstm-autoencoder.pt")


def read_current() -> dict:
    try:
        with open(CURRENT_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def _write_current(payload: dict):
    os.makedirs(config.MODELS_DIR, exist_ok=True)
    with open(CURRENT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def list_runs() -> list:
    if not os.path.isdir(RUNS_DIR):
        return []
    current = read_current().get("run_id")
    out = []
    for d in sorted(os.listdir(RUNS_DIR), reverse=True):
        p = os.path.join(RUNS_DIR, d)
        if not os.path.isdir(p):
            continue
        out.append({
            "run_id": d,
            "active": d == current,
            "has_models": _has_models(d),
            "created": datetime.fromtimestamp(
                os.path.getmtime(p), tz=timezone.utc).isoformat(),
            "metrics": read_eval(d),
        })
    return out


def _has_models(run_id: str) -> bool:
    p = os.path.join(RUNS_DIR, run_id)
    return any(os.path.exists(os.path.join(p, f)) for f in _MODEL_FILES)


def read_eval(run_id: str) -> list:
    """Run dizinindeki eval_results.csv -> metrik listesi."""
    path = os.path.join(RUNS_DIR, run_id, "eval_results.csv")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def read_comp_vs_zabbix(run_id: str) -> list:
    """Run dizinindeki comp_vs_zabbix.csv -> ground truth pencere sonuclari."""
    path = os.path.join(RUNS_DIR, run_id, "comp_vs_zabbix.csv")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def read_feature_spec(run_id: str) -> dict:
    path = os.path.join(RUNS_DIR, run_id, "feature_spec.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def read_predictions(run_id: str, model: str | None = None,
                     limit: int = 2000) -> list:
    """Run dizinindeki predictions.csv (grafikler icin skor serisi)."""
    path = os.path.join(RUNS_DIR, run_id, "predictions.csv")
    if not os.path.exists(path):
        return []
    out = []
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if model and row.get("model") != model:
                continue
            out.append(row)
            if len(out) >= limit:
                break
    return out


def run_detail(run_id: str) -> dict:
    p = os.path.join(RUNS_DIR, run_id)
    if not os.path.isdir(p):
        raise FileNotFoundError(f"run bulunamadi: {run_id}")
    spec = read_feature_spec(run_id)
    return {
        "run_id": run_id,
        "active": read_current().get("run_id") == run_id,
        "has_models": _has_models(run_id),
        "files": sorted(os.listdir(p)),
        "metrics": read_eval(run_id),
        "comp_vs_zabbix": read_comp_vs_zabbix(run_id),
        "feature_spec": {
            "n_features": len(spec.get("feature_cols", [])),
            "seq_len": spec.get("seq_len"),
            "rolling_windows": spec.get("rolling_windows"),
            "thresholds": spec.get("thresholds"),
        } if spec else {},
    }


def promote(run_id: str) -> dict:
    """Run'i aktif model yap. Rollback ayni islemdir (eski run'a promote)."""
    p = os.path.join(RUNS_DIR, run_id)
    if not os.path.isdir(p):
        raise FileNotFoundError(f"run bulunamadi: {run_id}")
    if not _has_models(run_id):
        raise ValueError(f"run model dosyasi icermiyor: {run_id}")
    previous = read_current().get("run_id")
    _write_current({
        "run_id": run_id,
        "active_at": datetime.now(timezone.utc).isoformat(),
        "promoted_by": "api",
        "previous": previous,
    })
    return {"run_id": run_id, "previous": previous}
