"""Load test (anomali uretimi) ve ground truth endpoint'leri."""

from fastapi import APIRouter, HTTPException

from ..models import ground_truth_repo, loadtest_runner
from ..views.schemas import GroundTruthCreate, LoadTestStartRequest

router = APIRouter(prefix="/api/v1", tags=["loadtests"])


@router.get("/loadtests")
def list_tests():
    """Kullanilabilir test turleri ve parametreleri."""
    return {"tests": loadtest_runner.available_tests(),
            "note": ("Testler host kaynaklarini yorar; API container'da "
                     "calisiyorsa yuk container ile sinirli kalir.")}


@router.post("/loadtests/{test_type}/start")
def start_test(test_type: str, body: LoadTestStartRequest | None = None):
    """Load test baslatir (cpu|ram|disk|net). Test bitiminde script kendisi
    ground truth kaydi ekler."""
    params = body.params if body else {}
    try:
        state = loadtest_runner.start(test_type, params)
    except (ValueError, FileNotFoundError) as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return {"message": f"{test_type} testi baslatildi", **state}


@router.get("/loadtests/status")
def test_status():
    """Calisan test durumu + log kuyrugu."""
    return loadtest_runner.status()


@router.post("/loadtests/stop")
def stop_test():
    """Calisan testi durdurur (SIGTERM, tum surec grubu)."""
    return loadtest_runner.stop()


@router.get("/ground-truth")
def list_ground_truth(label: str | None = None, since_ts: int | None = None):
    """Ground truth kayitlari (load testlerin loglari)."""
    records = ground_truth_repo.list_records(label=label, since_ts=since_ts)
    return {"total": len(records), "records": records}


@router.post("/ground-truth")
def add_ground_truth(body: GroundTruthCreate):
    """Manuel ground truth kaydi ekle (elle yapilan anomali denemeleri)."""
    if body.end_ts <= body.start_ts:
        raise HTTPException(400, "end_ts > start_ts olmali")
    rec = ground_truth_repo.add_record(
        body.type, body.label, body.host, body.start_ts, body.end_ts,
        body.metadata)
    return {"message": "kayit eklendi", "record": rec}
