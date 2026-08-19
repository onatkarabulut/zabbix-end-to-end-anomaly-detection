"""Enriched / feature matrix veri endpoint'leri."""

from fastapi import APIRouter, HTTPException, Query

from ..models import warehouse

router = APIRouter(prefix="/api/v1/features", tags=["features"])


@router.get("/enriched")
def enriched(host: str | None = None,
             start: str | None = Query(None, description="YYYY-MM-DD HH:MM:SS"),
             end: str | None = None,
             columns: str | None = Query(
                 None, description="virgullu kolon listesi (ops.)"),
             limit: int = 500, offset: int = 0):
    """ml_features_enriched tablosundan filtreli veri."""
    cols = [c.strip() for c in columns.split(",")] if columns else None
    try:
        return warehouse.query_features(
            "enriched", host=host, start=start, end=end,
            columns=cols, limit=limit, offset=offset)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except FileNotFoundError as e:
        raise HTTPException(503, str(e))


@router.get("/matrix")
def matrix(host: str | None = None, start: str | None = None,
           end: str | None = None, columns: str | None = None,
           limit: int = 500, offset: int = 0):
    """Ham ml_feature_matrix tablosundan filtreli veri."""
    cols = [c.strip() for c in columns.split(",")] if columns else None
    try:
        return warehouse.query_features(
            "matrix", host=host, start=start, end=end,
            columns=cols, limit=limit, offset=offset)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except FileNotFoundError as e:
        raise HTTPException(503, str(e))


@router.get("/columns")
def columns(table: str = Query("enriched", pattern="^(enriched|matrix)$")):
    """Tablo kolon listesi."""
    try:
        return {"table": table, "columns": warehouse.get_columns(table)}
    except FileNotFoundError as e:
        raise HTTPException(503, str(e))


@router.get("/hosts")
def hosts():
    """Ambardaki hostlar."""
    try:
        return {"hosts": warehouse.get_hosts()}
    except FileNotFoundError as e:
        raise HTTPException(503, str(e))


@router.get("/coverage")
def coverage():
    """Veri kapsami: satir sayilari, zaman araligi, boşluk analizi."""
    try:
        return warehouse.get_coverage()
    except FileNotFoundError as e:
        raise HTTPException(503, str(e))
