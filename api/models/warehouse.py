"""Veri ambari (zabbix_ml.db) sorgulari: enriched / feature matrix / kapsam."""

from .. import config
from .database import connect_ro, rows_to_dicts

ENRICHED_TABLE = "ml_features_enriched"
MATRIX_TABLE = "ml_feature_matrix"

# SQL enjeksiyonuna karsi tablo adlari sabit listedendir.
_TABLES = {"enriched": ENRICHED_TABLE, "matrix": MATRIX_TABLE}


def _table(name: str) -> str:
    if name not in _TABLES:
        raise ValueError(f"bilinmeyen tablo: {name}")
    return _TABLES[name]


def get_columns(table: str = "enriched") -> list:
    tbl = _table(table)
    with connect_ro(config.WAREHOUSE_DB) as conn:
        rows = conn.execute(f"PRAGMA table_info({tbl})").fetchall()
    return [r["name"] for r in rows]


def get_hosts() -> list:
    with connect_ro(config.WAREHOUSE_DB) as conn:
        rows = conn.execute(
            f"SELECT DISTINCT host FROM {ENRICHED_TABLE} ORDER BY host"
        ).fetchall()
    return [r["host"] for r in rows]


def query_features(table: str = "enriched", host: str | None = None,
                   start: str | None = None, end: str | None = None,
                   columns: list | None = None,
                   limit: int = 500, offset: int = 0) -> dict:
    """Filtreli feature sorgusu. `columns` verilirse yalnizca o kolonlar
    (datetime_minute + host her zaman dahil) doner - genis tabloda bant
    genisligini korur."""
    tbl = _table(table)
    valid_cols = get_columns(table)

    if columns:
        unknown = [c for c in columns if c not in valid_cols]
        if unknown:
            raise ValueError(f"bilinmeyen kolon(lar): {unknown}")
        sel_cols = ["datetime_minute", "host"] + [
            c for c in columns if c not in ("datetime_minute", "host")]
        select = ", ".join(f'"{c}"' for c in sel_cols)
    else:
        select = "*"

    where, params = [], []
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

    limit = max(1, min(int(limit), 10000))
    offset = max(0, int(offset))

    with connect_ro(config.WAREHOUSE_DB) as conn:
        total = conn.execute(
            f"SELECT COUNT(*) AS n FROM {tbl}{where_sql}", params
        ).fetchone()["n"]
        rows = conn.execute(
            f"SELECT {select} FROM {tbl}{where_sql} "
            f"ORDER BY datetime_minute LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
    return {"total": total, "limit": limit, "offset": offset,
            "rows": rows_to_dicts(rows)}


def get_coverage() -> dict:
    """Veri kapsami: satir sayilari, zaman araligi, 70 dk'dan buyuk boşluklar."""
    out = {}
    with connect_ro(config.WAREHOUSE_DB) as conn:
        for key, tbl in _TABLES.items():
            try:
                r = conn.execute(
                    f"SELECT COUNT(*) AS n, MIN(datetime_minute) AS lo, "
                    f"MAX(datetime_minute) AS hi FROM {tbl}"
                ).fetchone()
                out[key] = {"rows": r["n"], "min": r["lo"], "max": r["hi"]}
            except Exception as e:
                out[key] = {"error": str(e)}

        # boşluk analizi (enriched, dakika bazli)
        gaps = []
        try:
            rows = conn.execute(
                f"SELECT DISTINCT datetime_minute FROM {ENRICHED_TABLE} "
                f"ORDER BY datetime_minute"
            ).fetchall()
            prev = None
            from datetime import datetime
            for r in rows:
                cur = datetime.fromisoformat(str(r["datetime_minute"]))
                if prev is not None:
                    delta_min = (cur - prev).total_seconds() / 60.0
                    if delta_min > 70:
                        gaps.append({"from": str(prev), "to": str(cur),
                                     "minutes": round(delta_min, 1)})
                prev = cur
        except Exception:
            pass
        out["gaps_over_70min"] = gaps
    return out
