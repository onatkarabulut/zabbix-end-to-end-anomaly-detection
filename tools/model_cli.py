#!/usr/bin/env python3
"""
Model versiyon yonetimi (list / promote / rollback / current).

data/models/ yapisal duzeni:
  data/models/
  ├── current.json              # aktif run gostergesi (run_id)
  └── runs/
      ├── run_baseline/         # ilk onaylanmis model (yedek)
      ├── run_20260819_0042/    # retrain ciktisi (feature_spec, *.joblib, *.pt)
      └── ...

Kullanim:
  python tools/model_cli.py list                    # tum runlar + metrikler
  python tools/model_cli.py current                 # aktif run
  python tools/model_cli.py promote <run_id>        # aktif model yap
  python tools/model_cli.py rollback <run_id>       # geri al (promote ile ayni)
  python tools/model_cli.py remove <run_id>         # sil (aktif olamaz)
"""

import argparse
import json
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODELS_DIR = os.path.join(ROOT, "data", "models")
RUNS_DIR = os.path.join(MODELS_DIR, "runs")
CURRENT_FILE = os.path.join(MODELS_DIR, "current.json")


def _read_current():
    try:
        with open(CURRENT_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def _write_current(payload):
    os.makedirs(MODELS_DIR, exist_ok=True)
    with open(CURRENT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _list_runs():
    if not os.path.isdir(RUNS_DIR):
        return []
    return sorted(
        (d for d in os.listdir(RUNS_DIR)
         if os.path.isdir(os.path.join(RUNS_DIR, d))),
        reverse=True,
    )


def _read_eval_metrics(run_id):
    """run dizinindeki eval sonuclarini okur (varsa)."""
    eval_path = os.path.join(RUNS_DIR, run_id, "eval_results.csv")
    if not os.path.exists(eval_path):
        return None
    import pandas as pd
    try:
        df = pd.read_csv(eval_path)
    except Exception:
        return None
    rows = {}
    for _, r in df.iterrows():
        rows[r["model"]] = {
            "P": r.get("precision"), "R": r.get("recall"),
            "F1": r.get("f1"), "thr": r.get("threshold"),
        }
    return rows


def _has_models(run_id):
    d = os.path.join(RUNS_DIR, run_id)
    return os.path.isdir(d) and (
        os.path.exists(os.path.join(d, "feature_spec.json"))
        or any(f.endswith((".joblib", ".pt")) for f in os.listdir(d))
    )


def cmd_list():
    current = _read_current().get("run_id")
    runs = _list_runs()
    if not runs:
        print("run yok. data/models/runs/ bos.")
        return
    print(f"{'RUN ID':<22} {'MODEL':<6} {'AKTIF':<6} METRIKLER (P/R/F1)")
    print("-" * 70)
    for run_id in runs:
        is_cur = "->" if run_id == current else ""
        meta = _read_eval_metrics(run_id)
        if meta:
            lstm = meta.get("LSTM-Autoencoder")
            ifs = meta.get("IsolationForest")
            ocs = meta.get("OneClassSVM")
            m = f"LSTM {lstm['P']}/{lstm['R']}/{lstm['F1']} | IF {ifs['P']}/{ifs['R']}/{ifs['F1']} | OC {ocs['P']}/{ocs['R']}/{ocs['F1']}"
        else:
            m = "(metrik yok)"
        has = "OK" if _has_models(run_id) else "-"
        print(f"{run_id:<22} {has:<6} {is_cur:<6} {m}")


def cmd_current():
    cur = _read_current()
    if not cur.get("run_id"):
        print("aktif model yok (current.json bos).")
        return
    print(f"aktif: {cur['run_id']} (aktif_at: {cur.get('active_at')})")
    run_dir = os.path.join(RUNS_DIR, cur["run_id"])
    if os.path.isdir(run_dir):
        print(f"dizin: {run_dir}")
    else:
        print(f"UYARI: {run_dir} bulunamadi!")


def cmd_promote(run_id):
    run_dir = os.path.join(RUNS_DIR, run_id)
    if not os.path.isdir(run_dir) or not _has_models(run_id):
        print(f"HATA: {run_id} gecersiz (data/models/runs/ icinde yok).")
        sys.exit(1)
    import datetime
    _write_current({
        "run_id": run_id,
        "active_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    })
    print(f"AKTIF model: {run_id}")


def cmd_rollback(run_id):
    cmd_promote(run_id)


def cmd_remove(run_id):
    current = _read_current().get("run_id")
    if run_id == current:
        print("HATA: aktif run silinemez. Once rollback yapin.")
        sys.exit(1)
    run_dir = os.path.join(RUNS_DIR, run_id)
    if not os.path.isdir(run_dir):
        print(f"HATA: {run_id} yok.")
        sys.exit(1)
    shutil.rmtree(run_dir)
    print(f"silindi: {run_id}")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("command", choices=[
        "list", "promote", "rollback", "current", "remove",
    ])
    ap.add_argument("run_id", nargs="?", default=None)
    args = ap.parse_args()

    if args.command == "list":
        cmd_list()
    elif args.command == "current":
        cmd_current()
    elif args.command in ("promote", "rollback"):
        if not args.run_id:
            print(f"HATA: {args.command} <run_id> gerekli.")
            sys.exit(1)
        cmd_promote(args.run_id)
    elif args.command == "remove":
        if not args.run_id:
            print("HATA: remove <run_id> gerekli.")
            sys.exit(1)
        cmd_remove(args.run_id)


if __name__ == "__main__":
    main()