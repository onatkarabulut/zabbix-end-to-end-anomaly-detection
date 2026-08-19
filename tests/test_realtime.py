"""Faz 4 uctan uca test: tailer -> Redis Stream -> consumer -> skor -> alerts.

Gerçek Redis yerine fakeredis kullanir (docker yokken gelistirme icin).
RealtimeScorer gercek egitilmis modelleri (data/models/) kullanir.
"""

import json
import os
import sys
import tempfile
import time

import fakeredis
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from realtime.checkpoint import FileCheckpoint  # noqa: E402
from realtime.config import STREAM_KEY, CONSUMER_GROUP  # noqa: E402
from realtime.consumer import RealtimeConsumer  # noqa: E402
from realtime.item_key import ItemKeyResolver  # noqa: E402
from realtime.tailer import ZabbixTailer  # noqa: E402


@pytest.fixture()
def tmpdb(tmp_path):
    return str(tmp_path / "realtime.db")


@pytest.fixture()
def redis_fake():
    return fakeredis.FakeRedis(decode_responses=True)


def _export_line(itemid, clock, value, name="system.cpu.util"):
    return (
        f'{{"host": {{"host": "Zabbix server"}}, "itemid": {itemid}, '
        f'"name": "{name}", "clock": {clock}, "ns": 0, "value": {value}, "type": 0}}'
    )


def test_tailer_to_stream(tmp_path, redis_fake, tmpdb):
    export_dir = str(tmp_path / "export")
    os.makedirs(export_dir)
    fpath = os.path.join(export_dir, "history-history-syncer-1.ndjson")

    clock = 1786716780
    with open(fpath, "a") as f:
        f.write(_export_line(69663, clock, 2.125) + "\n")
        f.write(_export_line(69663, clock + 1, 84.97) + "\n")

    # Tailer'ı fakeredis'e yönlendir (redis client'ı monkeypatch)
    tailer = ZabbixTailer(export_dir, checkpoint_db=tmpdb)
    tailer.redis = redis_fake
    n = tailer.poll_once()
    assert n == 2
    assert redis_fake.xlen(STREAM_KEY) == 2

    # Yeni satir geldiginde sadece yeni kisim islenir (byte-offset)
    with open(fpath, "a") as f:
        f.write(_export_line(69663, clock + 2, 10.5) + "\n")
    n2 = tailer.poll_once()
    assert n2 == 1
    assert redis_fake.xlen(STREAM_KEY) == 3

    # Checkpoint ilerledi
    cp = FileCheckpoint(tmpdb)
    assert cp.get("history-history-syncer-1.ndjson") > 0


def test_consumer_scores_and_alerts(redis_fake, tmpdb):
    # Stream'e test verisi ekle
    for i in range(30):
        redis_fake.xadd(STREAM_KEY, {
            "itemid": 69663, "clock": 1786716780 + i,
            "value": float(i), "name": "system.cpu.util", "host": "Zabbix server",
        })

    consumer = RealtimeConsumer(db_path=tmpdb, redis_client=redis_fake)
    consumer.run_once()

    # consumer group stream'den okudu
    assert redis_fake.xlen(STREAM_KEY) == 30
    # dakika skorlandi (watermark dolu) ve seri birikti
    assert consumer.watermark
    assert "Zabbix server" in consumer.buffer._series
    # alerts tablosu olustu
    assert os.path.exists(tmpdb)


def test_alert_store_insert(tmpdb):
    from realtime.consumer import AlertStore
    store = AlertStore(tmpdb)
    store.insert("Zabbix server", 1786716780, "isolationforest", 1.5, 0.632)
    store.insert("Zabbix server", 1786716780, "oneclasssvm", 0.01, 0.00039)
    rows = store.conn.execute("SELECT host, model, score FROM alerts").fetchall()
    assert len(rows) == 2
    store.close()


def test_item_key_resolver_from_cache(tmp_path):
    # Gercek Zabbix export'unda name okunakli isim, model key bekler.
    cache = tmp_path / "item_key_map.json"
    cache.write_text(json.dumps({
        "42248": "system.cpu.util[,nice]",
        "42249": "system.cpu.load[all,avg1]",
        "42256": "system.cpu.intr",
    }))
    r = ItemKeyResolver(cache_path=str(cache))
    assert r.resolve(42248) == "system.cpu.util[,nice]"
    assert r.resolve(42249) == "system.cpu.load[all,avg1]"
    assert r.resolve(99999, "fallback") == "fallback"
    assert r.resolve(42256) == "system.cpu.intr"


def test_consumer_uses_key_not_display_name(redis_fake, tmpdb, tmp_path):
    # Canli export formati: itemid + okunakli name ("CPU nice time").
    cache = tmp_path / "item_key_map.json"
    cache.write_text(json.dumps({"42248": "system.cpu.util[,nice]"}))
    resolver = ItemKeyResolver(cache_path=str(cache))
    consumer = RealtimeConsumer(db_path=tmpdb, redis_client=redis_fake,
                                item_key_resolver=resolver)
    # Guncel clock kullanilir: MinuteBuffer 180 dk'dan eski kayitlari budar,
    # sabit eski timestamp serinin bosalmasina yol acar.
    base_clock = int(time.time()) - 120
    for i in range(60):
        redis_fake.xadd(STREAM_KEY, {
            "itemid": 42248, "clock": base_clock + i,
            "value": float(i), "name": "CPU nice time", "host": "Zabbix server",
        })
    consumer.run_once()
    # Seride key olarak Zabbix key'i saklanir (display name degil)
    series = consumer.buffer.series("Zabbix server")
    assert series
    assert any("system.cpu.util[,nice]" in kv for _, kv in series)
    assert all("CPU nice time" not in kv for _, kv in series)