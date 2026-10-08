from fastapi import APIRouter, Query
from services.db import query

router = APIRouter()

@router.get("/")
def search_events(q: str = Query(..., min_length=2)):
    """Full-text search across events."""
    rows = query("""
        SELECT id, category, sub_type, location_name, country,
               ST_Y(geom) as lat, ST_X(geom) as lng,
               event_time, severity, summary,
               confidence, confidence_score
        FROM events
        WHERE summary ILIKE %s
           OR location_name ILIKE %s
           OR actor1 ILIKE %s
           OR actor2 ILIKE %s
           OR raw_text ILIKE %s
        ORDER BY confidence_score DESC, event_time DESC
        LIMIT 50
    """, [f"%{q}%"] * 5)

    for r in rows:
        for k, v in r.items():
            if hasattr(v, 'isoformat'):
                r[k] = v.isoformat()

    return {"results": rows, "count": len(rows), "query": q}