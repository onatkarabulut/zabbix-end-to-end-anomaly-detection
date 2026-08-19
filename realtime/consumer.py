"""Consumer - Redis Stream'den okuyup dakika bazli skorlar ve alert uretir.

Katlama 3+4: consumer group + XREADGROUP + XACK ile at-least-once teslimat.
XPENDING/XCLAIM ile takilmus mesajlar yeniden denenir. (itemid, clock)
watermark'i sayesinde ayni dakika iki kez skorlanmaz (idempotent).

Is akisi:
  1. XREADGROUP ile `zabbix_history` stream'inden yeni satirlari oku.
  2. Satirlari (host, dakika) bucketing yap.
  3. Tum satirlar geldiginde o dakika icin RealtimeScorer ile skorla.
  4. Esik asimi varsa alerts tablosuna yaz + Prometheus metrigini artir.
  5. XACK ile basarili islemi onayla.
"""

import argparse
import json
import os
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone

import redis

from .config import STREAM_KEY, CONSUMER_GROUP
from .item_key import ItemKeyResolver
from .scoring import RealtimeScorer

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server

    _SCORES = Counter("ml_scores_total", "skorlanan dakika")
    _ALERTS = Counter("ml_alerts_total", "uretilen alarm")
    _LAG = Gauge("ml_redis_lag_minutes", "son skor ile simdiki arasindaki gecikme")
    _XPENDING = Gauge("ml_redis_xpending_depth", "bekleyen mesaj derinligi")
    _SCORE_LAST = Gauge("ml_score_last", "son dakika skoru", ["model"])
    _PROC_TIME = Histogram("ml_consumer_processing_seconds", "islem suresi")
    PROMETHEUS_OK = True
except Exception:  # pragma: no cover
    PROMETHEUS_OK = False

DEFAULT_CONSUMER = "consumer-1"


class MinuteBuffer:
    """Gelen satirlari (host, dakika) bazinda biriktirir ve her host icin
    ham (key -> deger) serisini tutar. LSTM penceresi bu seriden uretilir.

    `max_series_min`: seri bu dakikadan eski kayitlari tutmaz. Budama
    yapilmazsa seri sinirsiz buyur (bellek sizintisi) ve skorlamadaki
    `_window_values` tum listeyi taradigi icin consumer zamanla yavaslar.
    LSTM seq_len=60 + rolling pencere 60 dk icin 180 dk fazlasiyla yeterli.
    """

    def __init__(self, max_series_min: int = 180):
        self._buckets = defaultdict(list)
        self._series = defaultdict(list)  # host -> [(epoch, {key: value})]
        self.max_series_min = max_series_min

    def add(self, host, minute_epoch, itemid, name, value):
        key = (host, minute_epoch)
        self._buckets[key].append({
            "itemid": itemid, "name": name, "value": value,
        })
        self._series[host].append((minute_epoch, {name: value}))

    def series(self, host):
        """Host'un ham serisi (artam sirali), LSTM penceresi icin."""
        return self._series[host]

    def _prune_series(self, watermark):
        """Watermark'tan max_series_min dakikadan eski kayitlari at."""
        cutoff = watermark - self.max_series_min * 60
        for host in list(self._series):
            s = self._series[host]
            kept = [(e, kv) for e, kv in s if e >= cutoff]
            if len(kept) != len(s):
                self._series[host] = kept

    def drain_minutes(self, watermark):
        """Watermark oncesi tamamlanmis dakikalari teslim eder."""
        ready = [(k, v) for k, v in self._buckets.items() if k[1] < watermark]
        for k in ready:
            del self._buckets[k[0]]
        self._prune_series(watermark)
        return ready


class AlertStore:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host TEXT,
                datetime_minute TEXT,
                model TEXT,
                score REAL,
                threshold REAL,
                source TEXT,
                created_at TEXT
            )
            """
        )
        self.conn.commit()

    def insert(self, host, minute_epoch, model, score, threshold, source="realtime"):
        minute_ts = datetime.fromtimestamp(minute_epoch, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        self.conn.execute(
            """
            INSERT INTO alerts (host, datetime_minute, model, score, threshold, source, created_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (host, minute_ts, model, float(score), float(threshold), source),
        )
        self.conn.commit()

    def close(self):
        self.conn.close()


class RealtimeConsumer:
    def __init__(self, redis_host="localhost", redis_port=6379,
                 consumer_name=DEFAULT_CONSUMER, db_path="data/realtime.db",
                 redis_client=None, item_key_resolver=None):
        self.redis = redis_client or redis.Redis(host=redis_host,
                                                 port=redis_port,
                                                 decode_responses=True)
        self.consumer = consumer_name
        self.scorer = RealtimeScorer()
        self.buffer = MinuteBuffer()
        self.alerts = AlertStore(db_path)
        self.watermark = {}
        self.item_key = item_key_resolver or ItemKeyResolver()
        self._ensure_group()

    def _ensure_group(self):
        try:
            self.redis.xgroup_create(STREAM_KEY, CONSUMER_GROUP, id="0", mkstream=True)
        except redis.exceptions.ResponseError:
            pass

    def _read_batch(self, count=500, block_ms=500):
        try:
            resp = self.redis.xreadgroup(
                CONSUMER_GROUP, self.consumer,
                {STREAM_KEY: ">"}, count=count, block=block_ms,
            )
        except redis.exceptions.ResponseError:
            self._ensure_group()
            return []
        if not resp:
            return []
        return resp[0][1]

    def _score_minute(self, host, minute_epoch, rows):
        vote = self.scorer.vote(self.buffer.series(host))
        _SCORES.inc()
        for model, res in vote["results"].items():
            _SCORE_LAST.labels(model=model).set(res["score"])
            if res["anomaly"]:
                self.alerts.insert(host, minute_epoch, model,
                                   res["score"], res["threshold"])
                _ALERTS.inc()
        self.watermark[(host, minute_epoch)] = True
        _LAG.set((int(time.time()) - minute_epoch) / 60.0)
        return vote["alert"]

    def _process_pending(self):
        """Takilmis (pending) mesajlari XPENDING ile bulup XCLAIM ile geri
        alir ve yeniden isler - at-least-once garantisi."""
        try:
            pending = self.redis.xpending_range(STREAM_KEY, CONSUMER_GROUP,
                                                min="-", max="+", count=50)
        except redis.exceptions.ResponseError:
            return
        _XPENDING.set(len(pending) if pending else 0)
        if not pending:
            return
        ids = [p["message_id"] for p in pending]
        claimed = self.redis.xclaim(STREAM_KEY, CONSUMER_GROUP, self.consumer,
                                    60000, ids, idle=60000)
        for msg in claimed:
            self._handle(msg, ack=True)

    def _handle(self, msg, ack=False):
        entry_id, fields = msg
        host = fields.get("host", "")
        clock = int(fields.get("clock", 0))
        minute = (clock // 60) * 60
        try:
            value = float(fields.get("value", 0.0))
        except (TypeError, ValueError):
            value = 0.0
        itemid = int(fields.get("itemid", 0))
        name = fields.get("name", "")
        # Feature spec Zabbix key_'ni bekler; export'ta name okunakli isimdir.
        key = self.item_key.resolve(itemid, name)
        self.buffer.add(host, minute, itemid, key, value)
        if ack:
            self.redis.xack(STREAM_KEY, CONSUMER_GROUP, entry_id)
        return entry_id

    def run_once(self, count=500):
        """Bir tur: yeni mesajlari oku, olgun dakikalari skorla."""
        messages = self._read_batch(count=count)
        new_ids = [self._handle(m) for m in messages]
        if new_ids:
            self.redis.xack(STREAM_KEY, CONSUMER_GROUP, *new_ids)
        # En guncel clock'a gore olgun dakikalari isle
        now = int(time.time())
        ready = self.buffer.drain_minutes(now - 60)
        for (host, minute_epoch), rows in ready:
            self._score_minute(host, minute_epoch, rows)
        self._process_pending()

    def run_forever(self, interval=1.0):
        print(f"[consumer] {self.consumer} calisiyor")
        while True:
            try:
                self.run_once()
            except Exception as e:
                print(f"[consumer] tur hatasi: {e}")
            time.sleep(interval)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-host", default="localhost")
    ap.add_argument("--redis-port", type=int, default=6379)
    ap.add_argument("--consumer", default=DEFAULT_CONSUMER)
    ap.add_argument("--db", default="data/realtime.db")
    ap.add_argument("--metrics-port", type=int, default=8001)
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    if PROMETHEUS_OK:
        start_http_server(args.metrics_port)
        print(f"[consumer] Prometheus metrikleri: :{args.metrics_port}/metrics")

    resolver = None
    if os.environ.get("DB_HOST"):
        resolver = ItemKeyResolver(
            host=os.environ.get("DB_HOST"),
            port=int(os.environ.get("DB_PORT", "5432")),
            user=os.environ.get("DB_USER", "zabbix"),
            password=os.environ.get("DB_PASSWORD", "zabbix_sifresi"),
            dbname=os.environ.get("DB_NAME", "zabbix"),
        )

    c = RealtimeConsumer(args.redis_host, args.redis_port, args.consumer,
                         args.db, item_key_resolver=resolver)
    if args.once:
        c.run_once()
        print("[consumer] tek tur tamam")
    else:
        c.run_forever(args.interval)


if __name__ == "__main__":
    main()