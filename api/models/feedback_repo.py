"""Faz 4 geri bildirim dongusu deposu.

Consumer skoru esik asinca bu API'ye POST atar; kayit realtime.db'deki
feedback_log tablosuna yazilir. ZABBIX_WEBHOOK_URL doluysa bildirim
Zabbix'e (veya baska bir webhook'a) iletilir ve iletim sonucu saklanir.
"""

import json
from datetime import datetime, timezone

import requests

from .. import config
from .database import connect_rw, connect_ro, rows_to_dicts

_DDL = """
CREATE TABLE IF NOT EXISTS feedback_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at TEXT,
    host TEXT,
    datetime_minute TEXT,
    model TEXT,
    score REAL,
    threshold REAL,
    payload TEXT,
    forwarded INTEGER DEFAULT 0,
    forward_status TEXT
)
"""


def _ensure_table(conn):
    conn.execute(_DDL)


def record(host: str, datetime_minute: str, model: str,
           score: float, threshold: float, extra: dict | None = None) -> dict:
    payload = {
        "host": host, "datetime_minute": datetime_minute, "model": model,
        "score": score, "threshold": threshold, **(extra or {}),
    }
    forwarded, forward_status = 0, ""
    if config.ZABBIX_WEBHOOK_URL:
        try:
            r = requests.post(config.ZABBIX_WEBHOOK_URL, json=payload,
                              timeout=10)
            forwarded = 1 if r.ok else 0
            forward_status = f"{r.status_code}"
        except Exception as e:
            forward_status = f"hata: {e}"

    with connect_rw(config.REALTIME_DB) as conn:
        _ensure_table(conn)
        cur = conn.execute(
            """INSERT INTO feedback_log
               (received_at, host, datetime_minute, model, score, threshold,
                payload, forwarded, forward_status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (datetime.now(timezone.utc).isoformat(), host, datetime_minute,
             model, score, threshold, json.dumps(payload, ensure_ascii=False),
             forwarded, forward_status),
        )
        rowid = cur.lastrowid
    return {"id": rowid, "forwarded": bool(forwarded),
            "forward_status": forward_status}


def list_log(limit: int = 100) -> list:
    limit = max(1, min(int(limit), 1000))
    try:
        with connect_ro(config.REALTIME_DB) as conn:
            rows = conn.execute(
                "SELECT * FROM feedback_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return rows_to_dicts(rows)
    except Exception:
        return []
