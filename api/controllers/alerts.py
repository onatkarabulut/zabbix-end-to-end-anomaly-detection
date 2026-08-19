"""Realtime alert endpoint'leri."""

from fastapi import APIRouter, HTTPException

from ..models import alerts_repo

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.get("")
def list_alerts(model: str | None = None, host: str | None = None,
                start: str | None = None, end: str | None = None,
                limit: int = 200, offset: int = 0):
    """Realtime alert kayitlari (filtreli, sayfalamali)."""
    try:
        return alerts_repo.query_alerts(
            model=model, host=host, start=start, end=end,
            limit=limit, offset=offset)
    except FileNotFoundError as e:
        raise HTTPException(503, str(e))


@router.get("/summary")
def summary(hours: int = 24):
    """Model bazli alert ozetleri (son N saat)."""
    try:
        return alerts_repo.alerts_summary(hours=hours)
    except FileNotFoundError as e:
        raise HTTPException(503, str(e))
