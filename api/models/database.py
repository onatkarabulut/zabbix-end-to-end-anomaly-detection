"""SQLite baglanti yardimcilari.

Sorgular kisa omurlu baglantilarla yapilir (API cok is parcacikli calisir;
sqlite baglantisi thread'e baglidir). Okuma sorgulari icin immutable degil
ro mod kullanilir: pipeline ayni dosyaya es zamanli yazmaya devam eder.
"""

import os
import sqlite3
from contextlib import contextmanager


@contextmanager
def connect_ro(db_path: str):
    """Salt okunur baglanti. Dosya yoksa OperationalError firlatir."""
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"veritabani bulunamadi: {db_path}")
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def connect_rw(db_path: str):
    """Yazilabilir baglanti (feedback log gibi API'ye ait tablolar icin)."""
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def rows_to_dicts(rows) -> list:
    return [dict(r) for r in rows]
