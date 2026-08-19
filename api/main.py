"""FastAPI uygulamasi - MVC giris noktasi.

Calistirma (host):
  .venv/bin/uvicorn api.main:app --host 0.0.0.0 --port 8000
Calistirma (docker):
  docker compose up -d ml-api

Swagger UI: http://localhost:8000/docs
"""

from fastapi import FastAPI

from .controllers import (alerts, features, feedback, health, loadtests,
                          models_ctl, reports, train)

app = FastAPI(
    title="Zabbix ML Pipeline API",
    description=(
        "Zabbix tabanli anomali tespit hattinin yonetim ve rapor API'si.\n\n"
        "* **features**: enriched / feature matrix verisi (filtreli)\n"
        "* **alerts**: realtime anomali alertleri\n"
        "* **models**: model runlari, karsilastirma, promote/rollback\n"
        "* **train**: Airflow uzerinden retrain / ETL tetikleme\n"
        "* **loadtests**: anomali uretim testleri + ground truth\n"
        "* **reports**: metrikler, grafikler (PNG), HTML rapor, mimari\n"
        "* **feedback**: Faz 4 geri bildirim dongusu (consumer -> Zabbix)\n"
    ),
    version="1.0.0",
)

app.include_router(health.router)
app.include_router(features.router)
app.include_router(alerts.router)
app.include_router(models_ctl.router)
app.include_router(train.router)
app.include_router(loadtests.router)
app.include_router(reports.router)
app.include_router(feedback.router)


@app.get("/", include_in_schema=False)
def root():
    return {
        "service": "zabbix-ml-pipeline-api",
        "docs": "/docs",
        "report": "/api/v1/reports/full",
        "health": "/health/pipeline",
    }
