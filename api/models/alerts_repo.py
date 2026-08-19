"""Realtime alert deposu (realtime.db / alerts tablosu)."""

from .. import config
from .database import connect_ro, rows_to_dicts


def query_alerts(model: str | None = None, host: str | None = None,
                 start: str | None = None, end: str | None = None,
                 limit: int = 200, offset: int = 0) -> dict:
    where, params = [], []
    if model:
        where.append("model = ?")
        params.append(model)
    if host:
        where.append("host = ?")
        params.append(host)
    if start:
        where.append("datetime_minute >= ?")
        params.append(start)
    if end:
        where.append("datetime_minute <= ?")
        params.append(end)
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    limit = max(1, min(int(limit), 5000))
    offset = max(0, int(offset))

    with connect_ro(config.REALTIME_DB) as conn:
        total = conn.execute(
            f"SELECT COUNT(*) AS n FROM alerts{where_sql}", params
        ).fetchone()["n"]
        rows = conn.execute(
            f"SELECT * FROM alerts{where_sql} "
            f"ORDER BY datetime_minute DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
    return {"total": total, "limit": limit, "offset": offset,
            "rows": rows_to_dicts(rows)}


def alerts_summary(hours: int = 24) -> dict:
    """Model bazinda alert sayilari + skor istatistikleri (son N saat)."""
    hours = max(1, min(int(hours), 24 * 30))
    with connect_ro(config.REALTIME_DB) as conn:
        rows = conn.execute(
            """
            SELECT model,
                   COUNT(*)            AS n_alerts,
                   ROUND(AVG(score),4) AS avg_score,
                   ROUND(MIN(score),4) AS min_score,
                   ROUND(MAX(score),4) AS max_score,
                   ROUND(AVG(threshold),4) AS threshold,
                   MIN(datetime_minute) AS first_alert,
                   MAX(datetime_minute) AS last_alert
            FROM alerts
            WHERE datetime_minute >= datetime('now', ?)
            GROUP BY model
            """,
            (f"-{hours} hours",),
        ).fetchall()
    return {"hours": hours, "models": rows_to_dicts(rows)}


def score_series(model: str, hours: int = 24) -> list:
    """Grafik icin skor zaman serisi."""
    hours = max(1, min(int(hours), 24 * 30))
    with connect_ro(config.REALTIME_DB) as conn:
        rows = conn.execute(
            """
            SELECT datetime_minute, score, threshold
            FROM alerts
            WHERE model = ? AND datetime_minute >= datetime('now', ?)
            ORDER BY datetime_minute
            """,
            (model, f"-{hours} hours"),
        ).fetchall()
    return rows_to_dicts(rows)


def list_models_in_alerts() -> list:
    with connect_ro(config.REALTIME_DB) as conn:
        rows = conn.execute("SELECT DISTINCT model FROM alerts").fetchall()
    return [r["model"] for r in rows]
