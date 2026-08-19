"""Pydantic istek/yanit semalari."""

from pydantic import BaseModel, Field


# --- Istekler ---

class TrainRequest(BaseModel):
    """Retrain DAG tetikleme istegi. conf Airflow dag_run.conf'a gecer."""
    conf: dict = Field(default_factory=dict,
                       description="Airflow dag_run.conf (ops.)")


class LoadTestStartRequest(BaseModel):
    params: dict = Field(
        default_factory=dict,
        description="Test parametreleri (env olarak gecer, orn. RAM_PERCENT)")


class GroundTruthCreate(BaseModel):
    type: str = Field(description="anomali turu: cpu|ram|disk|net|custom")
    label: str = Field(description="etiket, orn. memory_pressure")
    host: str = Field(default="Zabbix server")
    start_ts: int = Field(description="baslangic epoch (sn)")
    end_ts: int = Field(description="bitis epoch (sn)")
    metadata: dict = Field(default_factory=dict)


class FeedbackIn(BaseModel):
    """Consumer'in esik asiminda POST ettigi anomali bildirimi (Faz 4)."""
    host: str
    datetime_minute: str
    model: str
    score: float
    threshold: float
    extra: dict = Field(default_factory=dict)


# --- Yanitlar (genel amacli) ---

class Message(BaseModel):
    message: str


class PromoteResponse(BaseModel):
    run_id: str
    previous: str | None = None
    message: str
