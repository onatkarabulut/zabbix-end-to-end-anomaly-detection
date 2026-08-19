"""Ground truth (data/ground_truth.jsonl) okuma/yazma.

Load test scriptleri buraya otomatik kayit atar; API hem listeler hem de
manuel kayit eklemeye izin verir (ornek: elle yapilan bir anomali denemesi).
"""

import json
import os
import uuid
from datetime import datetime, timezone

from .. import config


def list_records(label: str | None = None, since_ts: int | None = None) -> list:
    if not os.path.exists(config.GROUND_TRUTH_FILE):
        return []
    out = []
    with open(config.GROUND_TRUTH_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if label and rec.get("label") != label:
                continue
            if since_ts and rec.get("start_ts", 0) < since_ts:
                continue
            out.append(rec)
    return out


def add_record(rec_type: str, label: str, host: str,
               start_ts: int, end_ts: int, metadata: dict | None = None) -> dict:
    rec = {
        "anomaly_id": f"{rec_type}-{uuid.uuid4().hex[:8]}",
        "type": rec_type,
        "host": host,
        "label": label,
        "start_ts": int(start_ts),
        "end_ts": int(end_ts),
        "metadata": dict(metadata or {}, source="api",
                         added_at=datetime.now(timezone.utc).isoformat()),
    }
    os.makedirs(os.path.dirname(config.GROUND_TRUTH_FILE), exist_ok=True)
    with open(config.GROUND_TRUTH_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return rec
