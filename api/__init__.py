"""Zabbix ML Pipeline API - MVC mimarisi.

Katmanlar:
  api/models/       (M) veri erisim katmani: SQLite, dosyalar, Airflow REST
  api/views/        (V) pydantic semalari + matplotlib grafik uretimi
  api/controllers/  (C) FastAPI router'lari (endpoint'ler)
  api/main.py           uygulama fabrikasi
"""
