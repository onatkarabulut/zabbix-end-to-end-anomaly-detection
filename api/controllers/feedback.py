"""Faz 4 geri bildirim dongusu endpoint'leri.

Consumer, skor esigi asildiginda buraya POST atar. Kayit loglanir ve
ZABBIX_WEBHOOK_URL tanimliysa Zabbix'e iletilir.
"""

from fastapi import APIRouter

from ..models import feedback_repo
from ..views.schemas import FeedbackIn

router = APIRouter(prefix="/api/v1/feedback", tags=["feedback"])


@router.post("/anomaly")
def anomaly(body: FeedbackIn):
    """Anomali bildirimi al (consumer -> API). Webhook yapilandirildiysa
    Zabbix'e iletilir."""
    result = feedback_repo.record(
        body.host, body.datetime_minute, body.model,
        body.score, body.threshold, body.extra)
    return {"message": "bildirim alindi", **result}


@router.get("/log")
def log(limit: int = 100):
    """Gelen anomali bildirimleri."""
    return {"records": feedback_repo.list_log(limit=limit)}
