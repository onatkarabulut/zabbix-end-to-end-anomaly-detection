"""Saglik / durum endpoint'leri."""

from fastapi import APIRouter

from ..models import pipeline_status

router = APIRouter(tags=["health"])


@router.get("/health")
def health():
    """API canlilik kontrolu."""
    return {"status": "ok"}


@router.get("/health/pipeline")
def pipeline():
    """Tum boru hatti bilesenlerinin durumu: ambar, realtime, Redis stream,
    Airflow, aktif model."""
    return pipeline_status.full_status()
