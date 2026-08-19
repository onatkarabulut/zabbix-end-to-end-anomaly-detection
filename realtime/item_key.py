"""Zabbix `items` tablosundan itemid -> key eslemesini saglar.

Canli HistoryExport JSON'larinda item'lar okunakli `name` alaniyla gelir
("CPU utilization"), model feature_spec'i ise Zabbix `key_` bekler
("system.cpu.util"). Bu modul Zabbix PostgreSQL'inden itemid->key haritasini
ceker ve local cache'e yazar; DB erisilemezse cache'i kullanir.
"""

import json
import os

_CACHE_DEFAULT = os.path.join(
    os.path.dirname(__file__), "..", "data", "item_key_map.json"
)


class ItemKeyResolver:
    def __init__(self, host=None, port=5432, user=None, password=None,
                 dbname=None, cache_path=_CACHE_DEFAULT):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.cache_path = cache_path
        self._map = {}
        self._load()

    def _load(self):
        if self.host and self.user and self.password and self.dbname:
            try:
                self._map = self._fetch()
                self._save()
                return
            except Exception as e:  # pragma: no cover - DB yoksa cache
                print(f"[item_key] DB eslesmesi alinamadi ({e}); cache deneniyor")
        self._map = self._load_cache()

    def _fetch(self):
        import psycopg2

        conn = psycopg2.connect(
            host=self.host, port=self.port, user=self.user,
            password=self.password, dbname=self.dbname,
        )
        cur = conn.cursor()
        cur.execute("SELECT itemid, key_ FROM items")
        mapping = {str(iid): key for iid, key in cur.fetchall()}
        conn.close()
        return mapping

    def _save(self):
        try:
            os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(self._map, f)
        except OSError:  # pragma: no cover
            pass

    def _load_cache(self):
        try:
            with open(self.cache_path, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return {}

    def resolve(self, itemid, fallback=None):
        if itemid is None:
            return fallback
        return self._map.get(str(itemid), fallback or "")