from fastapi import APIRouter, Query
from typing import Optional
from services.db import query, query_one

router = APIRouter()

@router.get("/")
def get_events(
    category: Optional[str] = None,
    country: Optional[str] = None,
    min_confidence: float = 0.0,
    limit: int = Query(default=100, le=500),
    offset: int = 0,
):
    """Get events with optional filters."""
    conditions = ["confidence_score >= %s"]
    params = [min_confidence]

    if category:
        conditions.append("category = %s")
        params.append(category)
    if country:
        conditions.append("country ILIKE %s")
        params.append(f"%{country}%")

    where = " AND ".join(conditions)
    params.extend([limit, offset])

    rows = query(f"""
        SELECT id, event_hash, category, sub_type,
               location_name, country, region,
               ST_Y(geom) as lat, ST_X(geom) as lng,
               geo_precision, event_time,
               actor1, actor1_type, actor2, actor2_type,
               severity, fatalities, summary,
               confidence, confidence_score, source_count,
               created_at
        FROM events
        WHERE {where}
        ORDER BY event_time DESC
        LIMIT %s OFFSET %s
    """, params)

    # Serialize datetime/enum
    for r in rows:
        for k, v in r.items():
            if hasattr(v, 'isoformat'):
                r[k] = v.isoformat()

    return {"events": rows, "count": len(rows)}


@router.get("/stats")
def get_stats():
    """Get dashboard statistics."""
    total = query_one("SELECT COUNT(*) as count FROM events")
    by_category = query("SELECT category, COUNT(*) as count FROM events GROUP BY category ORDER BY count DESC")
    by_country = query("SELECT country, COUNT(*) as count FROM events GROUP BY country ORDER BY count DESC LIMIT 10")
    by_confidence = query("""
        SELECT confidence, COUNT(*) as count 
        FROM events GROUP BY confidence ORDER BY count DESC
    """)
    avg_severity = query_one("SELECT AVG(severity)::float as avg FROM events")
    recent = query_one("SELECT MAX(event_time) as latest FROM events")

    return {
        "total_events": total["count"],
        "by_category": by_category,
        "by_country": by_country,
        "by_confidence": by_confidence,
        "avg_severity": avg_severity["avg"] if avg_severity else 0,
        "latest_event": recent["latest"].isoformat() if recent and recent["latest"] else None,
    }


@router.get("/geo")
def get_geo_events(
    min_confidence: float = 0.0,
    limit: int = Query(default=500, le=1000),
):
    """Get events with coordinates for map display."""
    rows = query("""
        SELECT id, category, sub_type, location_name, country,
               ST_Y(geom) as lat, ST_X(geom) as lng,
               event_time, severity, summary,
               confidence, confidence_score, actor1, actor2
        FROM events
        WHERE geom IS NOT NULL AND confidence_score >= %s
        ORDER BY event_time DESC
        LIMIT %s
    """, [min_confidence, limit])

    for r in rows:
        for k, v in r.items():
            if hasattr(v, 'isoformat'):
                r[k] = v.isoformat()

    return {"events": rows}


@router.get("/timeline")
def get_timeline():
    """Get event counts by day for timeline chart."""
    rows = query("""
        SELECT DATE(event_time) as date, category, COUNT(*) as count
        FROM events
        WHERE event_time IS NOT NULL
        GROUP BY DATE(event_time), category
        ORDER BY date DESC
        LIMIT 90
    """)
    for r in rows:
        if r.get("date"):
            r["date"] = r["date"].isoformat()
    return {"timeline": rows}


@router.get("/{event_id}")
def get_event(event_id: str):
    """Get a single event with its sources."""
    event = query_one("""
        SELECT id, event_hash, category, sub_type,
               location_name, country, region,
               ST_Y(geom) as lat, ST_X(geom) as lng,
               event_time, actor1, actor2, severity, fatalities,
               summary, raw_text, confidence, confidence_score, source_count,
               created_at
        FROM events WHERE id = %s
    """, [event_id])

    if not event:
        return {"error": "Event not found"}

    sources = query("""
        SELECT source_name, source_url, source_tier, source_type, published_at
        FROM event_sources WHERE event_id = %s
    """, [event_id])

    for r in [event] + sources:
        for k, v in r.items():
            if hasattr(v, 'isoformat'):
                r[k] = v.isoformat()

    return {"event": event, "sources": sources}