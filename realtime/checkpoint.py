"""Byte-offset checkpoint - Zabbix export dosyalarinin kaldigi yerden devami."""

import sqlite3


class FileCheckpoint:
    """Her export dosyasi icin okunan byte sayisini SQLite'ta tutar.

    Neden SQLite? Tailer tek process'tir; satirlar -> Redis'e islenir.
    Checkpoint yalnizca 'hangi byte'a kadar islendik' bilgisini saklar.
    AOF + consumer group asil veri kaybi korumasidir; bu dosya-level
    restart-oncesi ilerlemeyi korur (cift katman).
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS file_offsets (
                file_name TEXT PRIMARY KEY,
                bytes_read INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        self._conn.commit()

    def get(self, file_name: str) -> int:
        cur = self._conn.execute(
            "SELECT bytes_read FROM file_offsets WHERE file_name = ?", (file_name,)
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0

    def set(self, file_name: str, bytes_read: int):
        self._conn.execute(
            """
            INSERT OR REPLACE INTO file_offsets (file_name, bytes_read)
            VALUES (?, ?)
            """,
            (file_name, bytes_read),
        )
        self._conn.commit()

    def close(self):
        self._conn.close()