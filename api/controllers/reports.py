"""Rapor endpoint'leri - gorevin nihai teslimatlarini API'den sunar:

  * model karsilastirma raporu (metrikler + grafikler)
  * mimari diyagram (mermaid)
  * genel ozet (veri kapsami, aktif model, alert ozeti)
  * HTML rapor sayfasi (grafikler gomulu)
"""

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, Response

from ..models import alerts_repo, ground_truth_repo, registry, warehouse
from ..views import charts

router = APIRouter(prefix="/api/v1/reports", tags=["reports"])

_EVAL_MODELS = ("LSTM-Autoencoder", "IsolationForest", "OneClassSVM")


def _active_run(run_id: str | None = None) -> str:
    rid = run_id or registry.read_current().get("run_id")
    if not rid:
        raise HTTPException(404, "aktif model yok; run_id verin")
    return rid


@router.get("/summary")
def summary(run_id: str | None = None):
    """Genel rapor ozeti (JSON): veri kapsami, model metrikleri,
    ground truth sayilari, realtime alert ozeti."""
    rid = _active_run(run_id)
    metrics = registry.read_eval(rid)
    comp = registry.read_comp_vs_zabbix(rid)
    gt = ground_truth_repo.list_records()

    try:
        coverage = warehouse.get_coverage()
    except Exception as e:
        coverage = {"error": str(e)}
    try:
        rt = alerts_repo.alerts_summary(hours=24)
    except Exception as e:
        rt = {"error": str(e)}

    best = None
    if metrics:
        best = max(metrics, key=lambda r: float(r.get("f1") or 0))

    return {
        "run_id": rid,
        "data_coverage": coverage,
        "model_metrics": metrics,
        "best_model_by_f1": {"model": best["model"], "f1": best["f1"]}
        if best else None,
        "ground_truth": {
            "total": len(gt),
            "by_label": _count_by(gt, "label"),
        },
        "zabbix_comparison_windows": len(comp),
        "realtime_last_24h": rt,
    }


def _count_by(records: list, key: str) -> dict:
    out = {}
    for r in records:
        out[r.get(key, "?")] = out.get(r.get(key, "?"), 0) + 1
    return out


@router.get("/metrics")
def metrics(run_id: str | None = None):
    """Model bazli precision/recall/F1 + esikler + train/test aralik bilgisi."""
    rid = _active_run(run_id)
    rows = registry.read_eval(rid)
    if not rows:
        raise HTTPException(404, f"degerlendirme bulunamadi: {rid}")
    return {"run_id": rid, "metrics": rows}


@router.get("/architecture")
def architecture():
    """Mimari diyagram (mermaid) - Zabbix -> ETL -> model -> geri bildirim."""
    mermaid = """flowchart LR
    subgraph Zabbix
      ZS[Zabbix Server] --> PG[(PostgreSQL)]
      ZS -->|real-time export| EXP[export/*.ndjson]
    end
    subgraph Batch[ETL - Airflow saatlik]
      PG -->|chunk + timeout + retry| EXT[extract]
      EXT --> MINIO[(MinIO parquet)]
      MINIO --> VAL[validate] --> TRF[transform] --> LOAD[load]
      LOAD --> WH[(SQLite ambar)]
      WH --> FE[feature_engineering]
      FE --> ENR[(ml_features_enriched)]
    end
    subgraph Train[Retrain - Airflow]
      ENR --> SPLIT[zaman bazli split] --> TRAIN[IF / OCSVM / LSTM-AE]
      TRAIN --> AUDIT[model audit] --> PROM[promote current.json]
    end
    subgraph Realtime[Faz 4 - realtime]
      EXP -->|tailer + checkpoint| REDIS[(Redis Stream)]
      REDIS -->|consumer group| SCORE[online skorlama]
      PROM -.->|aktif model| SCORE
      SCORE -->|esik asimi| ALERTS[(alerts)]
      SCORE -->|POST /feedback| API[FastAPI]
      API -->|webhook| ZS
    end
"""
    return {"format": "mermaid", "diagram": mermaid}


# ---------------------------------------------------------------- grafikler

@router.get("/charts/comparison",
            responses={200: {"content": {"image/png": {}}}})
def chart_comparison(run_id: str | None = None):
    """Precision/Recall/F1 bar grafigi (PNG)."""
    rid = _active_run(run_id)
    rows = registry.read_eval(rid)
    if not rows:
        raise HTTPException(404, f"degerlendirme bulunamadi: {rid}")
    return Response(charts.metrics_comparison(rows), media_type="image/png")


@router.get("/charts/latency",
            responses={200: {"content": {"image/png": {}}}})
def chart_latency(run_id: str | None = None):
    """Ground truth pencerelerinde tespit gecikmesi grafigi (PNG)."""
    rid = _active_run(run_id)
    rows = registry.read_comp_vs_zabbix(rid)
    if not rows:
        raise HTTPException(404, f"karsilastirma bulunamadi: {rid}")
    return Response(charts.latency_chart(rows), media_type="image/png")


@router.get("/charts/scores",
            responses={200: {"content": {"image/png": {}}}})
def chart_scores(model: str = Query("lstm-autoencoder"),
                 hours: int = 24):
    """Realtime skor zaman serisi + esik (PNG)."""
    try:
        series = alerts_repo.score_series(model, hours=hours)
    except FileNotFoundError as e:
        raise HTTPException(503, str(e))
    return Response(charts.score_timeseries(series, model),
                    media_type="image/png")


@router.get("/charts/predictions",
            responses={200: {"content": {"image/png": {}}}})
def chart_predictions(model: str = Query("LSTM-Autoencoder"),
                      run_id: str | None = None):
    """Test donemi skorlari + ground truth pencere overlay'i (PNG)."""
    rid = _active_run(run_id)
    preds = registry.read_predictions(rid, model=model, limit=20000)
    gt = [(r["start_ts"], r["end_ts"], r.get("label", ""))
          for r in ground_truth_repo.list_records()]
    return Response(charts.predictions_timeline(preds, gt, model),
                    media_type="image/png")


@router.get("/charts/alerts-timeline",
            responses={200: {"content": {"image/png": {}}}})
def chart_alerts_timeline(hours: int = 48):
    """Saatlik alert yogunlugu (PNG)."""
    try:
        data = alerts_repo.query_alerts(limit=5000)
    except FileNotFoundError as e:
        raise HTTPException(503, str(e))
    return Response(charts.alerts_per_hour(data["rows"]),
                    media_type="image/png")


# ---------------------------------------------------------------- HTML rapor

@router.get("/full", response_class=HTMLResponse)
def full_report(run_id: str | None = None):
    """Grafikleri gomulu tek sayfalik HTML rapor - raporun teslim edilebilir
    hali; tarayicida acilir, PDF'e yazdirilabilir."""
    rid = _active_run(run_id)
    metrics_rows = registry.read_eval(rid)
    comp = registry.read_comp_vs_zabbix(rid)
    gt = ground_truth_repo.list_records()

    metric_table = "".join(
        f"<tr><td>{r['model']}</td><td>{r['precision']}</td>"
        f"<td>{r['recall']}</td><td>{r['f1']}</td>"
        f"<td>{r['threshold']}</td><td>{r['n_pred_pos']}</td></tr>"
        for r in metrics_rows)

    comp_table = "".join(
        f"<tr><td>{r['model']}</td><td>{r['label']}</td>"
        f"<td>{r['caught']}</td><td>{r.get('latency_min','')}</td>"
        f"<td>{r.get('zabbix_alarm_overlap','')}</td></tr>"
        for r in comp)

    html = f"""<!DOCTYPE html>
<html lang="tr"><head><meta charset="utf-8">
<title>Zabbix ML - Model Karsilastirma Raporu</title>
<style>
 body {{ font-family: system-ui, sans-serif; margin: 2rem auto;
        max-width: 1100px; color: #222; }}
 h1, h2 {{ border-bottom: 2px solid #eee; padding-bottom: .3rem; }}
 table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; }}
 th, td {{ border: 1px solid #ddd; padding: 6px 10px; font-size: .9rem; }}
 th {{ background: #f5f5f5; }}
 img {{ max-width: 100%; margin: .5rem 0; border: 1px solid #eee; }}
 .meta {{ color: #666; font-size: .85rem; }}
</style></head><body>
<h1>Model Karsilastirma Raporu</h1>
<p class="meta">Aktif run: <b>{rid}</b> &middot; Ground truth kayit:
{len(gt)} &middot; Karsilastirma penceresi: {len(comp)}</p>

<h2>1. Metrikler (Precision / Recall / F1)</h2>
<table><tr><th>Model</th><th>Precision</th><th>Recall</th><th>F1</th>
<th>Esik</th><th>Anomali tahmini</th></tr>{metric_table}</table>
<img src="/api/v1/reports/charts/comparison?run_id={rid}" alt="karsilastirma">

<h2>2. Ground Truth Pencereleri - Yakalama ve Gecikme</h2>
<table><tr><th>Model</th><th>Etiket</th><th>Yakalandi</th>
<th>Gecikme (dk)</th><th>Zabbix de yakaladi mi</th></tr>{comp_table}</table>
<img src="/api/v1/reports/charts/latency?run_id={rid}" alt="gecikme">

<h2>3. Test Donemi Skorlari (Ground Truth Overlay)</h2>
<img src="/api/v1/reports/charts/predictions?model=LSTM-Autoencoder&run_id={rid}"
     alt="LSTM predictions">
<img src="/api/v1/reports/charts/predictions?model=OneClassSVM&run_id={rid}"
     alt="OCSVM predictions">
<img src="/api/v1/reports/charts/predictions?model=IsolationForest&run_id={rid}"
     alt="IF predictions">

<h2>4. Realtime Skorlar (son 24 saat)</h2>
<img src="/api/v1/reports/charts/scores?model=lstm-autoencoder&hours=24"
     alt="realtime lstm">
<img src="/api/v1/reports/charts/alerts-timeline" alt="alert yogunlugu">

<h2>5. Mimari</h2>
<p>Zabbix &rarr; ETL (Airflow, saatlik) &rarr; SQLite ambar &rarr; feature
engineering &rarr; egitim (IF / OCSVM / LSTM-AE) &rarr; realtime skorlama
(export &rarr; tailer &rarr; Redis Stream &rarr; consumer) &rarr; alert +
API geri bildirimi. Ayrinti: <code>GET /api/v1/reports/architecture</code></p>
</body></html>"""
    return HTMLResponse(html)
