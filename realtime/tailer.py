"""Tailer - Zabbix export dosyalarini takip eder, Redis Stream'e yazar.

Katlama 1: Export dosyalari Zabbix'in HistoryExport ozelligiyle uretilir
(zabbix-export/*.ndjson). Tailer her dosyayi byte-offset checkpoint ile
takip eder ve yeni satirlari `zabbix_history` Redis stream'ine XADD eder.
Redis AOF ile kalici oldugu icin tailer restart etse bile veri kaybi olmaz.

Prometheus: :8002/metrics uzerinden ml_tailer_* metrikleri yayinlanir.
"""

import argparse
import glob
import json
import os
import time

import redis

from .checkpoint import FileCheckpoint
from .config import STREAM_KEY

try:
    from prometheus_client import Counter, Gauge, start_http_server

    _SCANNED = Counter("ml_tailer_bytes_scanned", "tailer tarafindan okunan byte")
    _LINES = Counter("ml_tailer_lines", "tailer tarafindan islenen satir")
    _ERRORS = Counter("ml_tailer_errors", "tailer hatalari")
    _FILES = Gauge("ml_tailer_files", "izlenen export dosyasi sayisi")
    PROMETHEUS_OK = True
except Exception:  # pragma: no cover
    PROMETHEUS_OK = False


def _iter_json_lines(raw: bytes):
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            _ERRORS.inc()


class ZabbixTailer:
    def __init__(self, export_dir: str, redis_host: str = "localhost",
                 redis_port: int = 6379, checkpoint_db: str = "data/realtime.db"):
        self.export_dir = export_dir
        self.redis = redis.Redis(host=redis_host, port=redis_port,
                                 decode_responses=True)
        self.checkpoint = FileCheckpoint(checkpoint_db)

    def _export_files(self):
        # Sadece history export'lari: trends-* (value_avg) ve problems-*
        # dosyalari farkli sema tasir, stream'e karistirilmaz.
        return sorted(glob.glob(os.path.join(self.export_dir, "history-*.ndjson")))

    def poll_once(self) -> int:
        """Bir tur tarama: tum export dosyalarinin yeni kisimlarini okuyup
        Redis'e yazar. Islenen satir sayisini dondurur."""
        files = self._export_files()
        _FILES.set(len(files))
        total = 0
        for path in files:
            fname = os.path.basename(path)
            try:
                offset = self.checkpoint.get(fname)
                size = os.path.getsize(path)
                if offset > size:
                    # Dosya Zabbix tarafinda rotate/truncate edilmis: ayni
                    # isimle yeni (kucuk) dosya olusmus, eski offset gecersiz.
                    # Sifirla ki veri atlanmasin. (offset lokalde de sifirlanir;
                    # aksi halde asagidaki checkpoint.set eski offset uzerine
                    # ekleyip her turda dosyanin bastan okunmasina yol acar.)
                    offset = 0
                data = self._read_new(path, offset)
                if not data:
                    continue
                n = 0
                for rec in _iter_json_lines(data):
                    self.redis.xadd(STREAM_KEY, {
                        "itemid": rec.get("itemid", 0),
                        "clock": rec.get("clock", 0),
                        "value": rec.get("value", 0.0),
                        "name": rec.get("name", ""),
                        "host": rec.get("host", {}).get("host", ""),
                    })
                    _LINES.inc()
                    n += 1
                self.checkpoint.set(fname, offset + len(data))
                _SCANNED.inc(len(data))
                total += n
            except Exception as e:  # pragma: no cover
                _ERRORS.inc()
                print(f"[tailer] hata {fname}: {e}")
        return total

    def _read_new(self, path: str, offset: int) -> bytes:
        size = os.path.getsize(path)
        if offset > size:
            return b""
        with open(path, "rb") as f:
            f.seek(offset)
            return f.read(size - offset)

    def run_forever(self, interval: float = 5.0):
        print(f"[tailer] izleniyor: {self.export_dir}")
        while True:
            try:
                n = self.poll_once()
                if n:
                    print(f"[tailer] {n} satir -> Redis")
            except Exception as e:
                _ERRORS.inc()
                print(f"[tailer] tur hatasi: {e}")
            time.sleep(interval)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--export-dir", default="zabbix-export")
    ap.add_argument("--redis-host", default="localhost")
    ap.add_argument("--redis-port", type=int, default=6379)
    ap.add_argument("--checkpoint-db", default="data/realtime.db")
    ap.add_argument("--metrics-port", type=int, default=8002)
    ap.add_argument("--interval", type=float, default=5.0)
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    if PROMETHEUS_OK:
        start_http_server(args.metrics_port)
        print(f"[tailer] Prometheus metrikleri: :{args.metrics_port}/metrics")

    tailer = ZabbixTailer(args.export_dir, args.redis_host, args.redis_port,
                          args.checkpoint_db)
    if args.once:
        print(f"[tailer] tek tur: {tailer.poll_once()} satir")
    else:
        tailer.run_forever(args.interval)


if __name__ == "__main__":
    main()