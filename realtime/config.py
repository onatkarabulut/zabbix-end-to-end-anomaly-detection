"""Faz 4 - Realtime anomali tespit katmani.

Mimari (veri kaybi yok hikayesi, 4 katman):
  1. Tailer: Zabbix export dosyalarini byte-offset checkpoint ile takip eder
     (SQLite file_offsets tablosu). Zabbix restart/rotate edilse bile kaldigi
     yerden devam eder.
  2. Redis Stream (AOF): tailer her satiri XADD ile `zabbix_history` stream'ine
     yazar. AOF sayesinde Redis restart'inda veri kalicidir.
  3. Consumer (consumer group): XREADGROUP + XACK + XPENDING/XCLAIM ile
     at-least-once teslimat. Ayni (itemid, clock) ikilisi watermark ile
     idempotent hale getirilir (effectively-once skorlama).
  4. Skor: kayitli model (feature_spec + scaler + threshold) ile dakika bazli
     anomali skoru; esik asimi data/realtime.db `alerts` tablosuna yazilir.
"""

STREAM_KEY = "zabbix_history"
CONSUMER_GROUP = "ml-consumers"