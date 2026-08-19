"""Model yonetimi: runs, aktif model, promote/rollback, karsilastirma."""

from fastapi import APIRouter, HTTPException

from ..models import registry
from ..views.schemas import PromoteResponse

router = APIRouter(prefix="/api/v1/models", tags=["models"])


@router.get("")
def list_models():
    """Tum model runlari (metrikleriyle)."""
    return {"current": registry.read_current(), "runs": registry.list_runs()}


@router.get("/current")
def current():
    """Aktif model."""
    cur = registry.read_current()
    if not cur.get("run_id"):
        raise HTTPException(404, "aktif model yok")
    return cur


@router.get("/comparison")
def comparison(run_id: str | None = None):
    """3 modelin karsilastirmasi: precision/recall/F1 + Zabbix'e karsi
    yakalama/gecikme. run_id verilmezse aktif run kullanilir."""
    rid = run_id or registry.read_current().get("run_id")
    if not rid:
        raise HTTPException(404, "aktif model yok; run_id verin")
    metrics = registry.read_eval(rid)
    comp = registry.read_comp_vs_zabbix(rid)
    if not metrics:
        raise HTTPException(404, f"degerlendirme bulunamadi: {rid}")

    # ozet: her model kac ground truth penceresi yakaladi, ort. gecikme
    summary = {}
    for row in comp:
        m = row["model"]
        s = summary.setdefault(m, {"windows": 0, "caught": 0, "latencies": []})
        s["windows"] += 1
        if str(row.get("caught")).lower() == "true":
            s["caught"] += 1
            lat = row.get("latency_min")
            if lat not in (None, "", "nan"):
                s["latencies"].append(float(lat))
    for m, s in summary.items():
        lats = s.pop("latencies")
        s["avg_latency_min"] = round(sum(lats) / len(lats), 2) if lats else None
        s["catch_rate"] = round(s["caught"] / s["windows"], 3) if s["windows"] else None

    return {"run_id": rid, "metrics": metrics,
            "ground_truth_windows": comp, "summary": summary}


@router.get("/{run_id}")
def detail(run_id: str):
    """Run detayi: dosyalar, metrikler, feature spec ozeti."""
    try:
        return registry.run_detail(run_id)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))


@router.post("/{run_id}/promote", response_model=PromoteResponse)
def promote(run_id: str):
    """Run'i aktif model yap. Consumer bir sonraki skorlamada yeni modeli
    kullanir (current.json uzerinden cozumlenir)."""
    try:
        result = registry.promote(run_id)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))
    return PromoteResponse(**result, message=f"{run_id} aktif model yapildi")


@router.post("/{run_id}/rollback", response_model=PromoteResponse)
def rollback(run_id: str):
    """Eski bir run'a geri don (promote ile ayni islem, anlamsal ayrim)."""
    try:
        result = registry.promote(run_id)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))
    return PromoteResponse(**result, message=f"{run_id} run'ina geri donuldu")
