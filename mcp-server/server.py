import os, json, logging
from datetime import datetime, date
import psycopg2
from psycopg2.extras import RealDictCursor
from mcp.server.fastmcp import FastMCP
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ── Config ──
PG_URL = os.getenv("POSTGRES_URL", "postgresql://conflict:conflict_secret@localhost:5433/conflict_monitor")
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

# ── Init ──
mcp = FastMCP("Conflict Monitor")

# Lazy-loaded globals
_pg = None
_qdrant = None
_model = None

def get_pg():
    global _pg
    if _pg is None or _pg.closed:
        _pg = psycopg2.connect(PG_URL)
        _pg.autocommit = True
    return _pg

def get_qdrant():
    global _qdrant
    if _qdrant is None:
        _qdrant = QdrantClient(url=QDRANT_URL)
    return _qdrant

def get_model():
    global _model
    if _model is None:
        log.info("Loading embedding model...")
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model

def pg_query(sql, params=None):
    conn = get_pg()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    # Serialize
    for r in rows:
        for k, v in r.items():
            if isinstance(v, (datetime, date)):
                r[k] = v.isoformat()
            elif isinstance(v, list):
                r[k] = [str(x) for x in v]
    return rows

def pg_one(sql, params=None):
    rows = pg_query(sql, params)
    return rows[0] if rows else None


# ══════════════════════════════════════
#  TOOLS
# ══════════════════════════════════════

@mcp.tool()
def get_conflict_overview() -> str:
    """Get a high-level overview of the US-Iran conflict monitoring dashboard.
    Returns total event count, category breakdown, confidence distribution,
    and average severity. Use this first to understand the current state."""

    total = pg_one("SELECT COUNT(*) as count FROM events")
    by_cat = pg_query("SELECT category, COUNT(*) as count FROM events GROUP BY category ORDER BY count DESC")
    by_conf = pg_query("SELECT confidence, COUNT(*) as count FROM events GROUP BY confidence ORDER BY count DESC")
    avg_sev = pg_one("SELECT AVG(severity)::float as avg FROM events")
    latest = pg_one("SELECT MAX(event_time) as latest FROM events")
    by_country = pg_query("SELECT country, COUNT(*) as count FROM events GROUP BY country ORDER BY count DESC LIMIT 10")

    return json.dumps({
        "total_events": total["count"],
        "avg_severity": round(avg_sev["avg"], 1) if avg_sev["avg"] else 0,
        "latest_event_time": latest["latest"] if latest else None,
        "by_category": by_cat,
        "by_confidence": by_conf,
        "top_countries": by_country,
    }, indent=2)


@mcp.tool()
def search_events(
    query: str,
    category: str = "",
    country: str = "",
    min_severity: int = 0,
    min_confidence: float = 0.0,
    limit: int = 20
) -> str:
    """Search conflict events by keyword. Searches across summaries, locations,
    actors, and raw text. Optionally filter by category, country, severity,
    and confidence.
    
    Categories: battle, explosion_remote_violence, violence_against_civilians,
    protest, strategic_development, naval_engagement, cyber_attack
    """

    conditions = ["(summary ILIKE %s OR location_name ILIKE %s OR actor1 ILIKE %s OR actor2 ILIKE %s)"]
    params = [f"%{query}%"] * 4

    if category:
        conditions.append("category = %s")
        params.append(category)
    if country:
        conditions.append("country ILIKE %s")
        params.append(f"%{country}%")
    if min_severity > 0:
        conditions.append("severity >= %s")
        params.append(min_severity)
    if min_confidence > 0:
        conditions.append("confidence_score >= %s")
        params.append(min_confidence)

    where = " AND ".join(conditions)
    params.append(min(limit, 50))

    rows = pg_query(f"""
        SELECT id, category, sub_type, summary, location_name, country,
               ST_Y(geom) as lat, ST_X(geom) as lng,
               event_time, actor1, actor2, severity,
               confidence, confidence_score, source_count
        FROM events WHERE {where}
        ORDER BY confidence_score DESC, event_time DESC
        LIMIT %s
    """, params)

    return json.dumps({"count": len(rows), "events": rows}, indent=2)


@mcp.tool()
def get_events_near_location(
    latitude: float,
    longitude: float,
    radius_km: float = 100,
    limit: int = 20
) -> str:
    """Find conflict events within a radius of a geographic point.
    Useful for questions like 'what happened near Tehran' or 'events near the Strait of Hormuz'.
    
    Common coordinates:
    - Tehran: 35.69, 51.39
    - Isfahan: 32.65, 51.67
    - Strait of Hormuz: 26.57, 56.25
    - Bahrain (Fifth Fleet): 26.07, 50.56
    - Beirut: 33.89, 35.50
    """

    rows = pg_query("""
        SELECT id, category, sub_type, summary, location_name, country,
               ST_Y(geom) as lat, ST_X(geom) as lng,
               event_time, actor1, actor2, severity,
               confidence, confidence_score,
               ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) / 1000 as distance_km
        FROM events
        WHERE geom IS NOT NULL
          AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s)
        ORDER BY event_time DESC
        LIMIT %s
    """, [longitude, latitude, longitude, latitude, radius_km * 1000, min(limit, 50)])

    return json.dumps({
        "center": {"lat": latitude, "lng": longitude},
        "radius_km": radius_km,
        "count": len(rows),
        "events": rows
    }, indent=2)


@mcp.tool()
def get_event_details(event_id: str) -> str:
    """Get full details of a specific event including all sources and confidence history."""

    event = pg_one("""
        SELECT id, event_hash, category, sub_type, summary, raw_text,
               location_name, country, region,
               ST_Y(geom) as lat, ST_X(geom) as lng,
               event_time, actor1, actor1_type, actor2, actor2_type,
               severity, fatalities, fatalities_precision,
               confidence, confidence_score, source_count, created_at
        FROM events WHERE id = %s
    """, [event_id])

    if not event:
        return json.dumps({"error": "Event not found"})

    sources = pg_query("""
        SELECT source_name, source_url, source_tier, source_type, published_at
        FROM event_sources WHERE event_id = %s
    """, [event_id])

    conf_log = pg_query("""
        SELECT old_score, new_score, reason, created_at
        FROM confidence_log WHERE event_id = %s ORDER BY created_at
    """, [event_id])

    return json.dumps({
        "event": event,
        "sources": sources,
        "confidence_history": conf_log
    }, indent=2)


@mcp.tool()
def get_timeline(days: int = 7) -> str:
    """Get event counts grouped by day and category for the last N days.
    Useful for understanding how the conflict is evolving over time."""

    rows = pg_query("""
        SELECT DATE(event_time) as date, category, COUNT(*) as count,
               AVG(severity)::float as avg_severity
        FROM events
        WHERE event_time >= NOW() - make_interval(days := %s)
        GROUP BY DATE(event_time), category
        ORDER BY date DESC, count DESC
    """, [days])

    # Also get daily totals
    daily = pg_query("""
        SELECT DATE(event_time) as date, COUNT(*) as total,
               AVG(severity)::float as avg_severity,
               SUM(CASE WHEN fatalities IS NOT NULL THEN fatalities ELSE 0 END) as total_fatalities
        FROM events
        WHERE event_time >= NOW() - make_interval(days := %s)
        GROUP BY DATE(event_time)
        ORDER BY date DESC
    """, [days])

    return json.dumps({
        "days": days,
        "daily_summary": daily,
        "by_category": rows
    }, indent=2)


@mcp.tool()
def semantic_search(question: str, limit: int = 10) -> str:
    """Search events using semantic similarity (vector search).
    Better than keyword search for natural language questions like
    'missile attacks on civilian areas' or 'naval confrontations in the Gulf'.
    """

    model = get_model()
    embedding = model.encode(question).tolist()
    qdrant = get_qdrant()

    results = qdrant.search(
        collection_name="conflict_events",
        query_vector=embedding,
        limit=min(limit, 20),
    )

    events = []
    for r in results:
        p = r.payload
        events.append({
            "event_id": p.get("event_id", ""),
            "category": p.get("category", ""),
            "summary": p.get("summary", ""),
            "location_name": p.get("location_name", ""),
            "country": p.get("country", ""),
            "severity": p.get("severity", 0),
            "confidence_score": p.get("confidence_score", 0),
            "relevance_score": round(r.score, 4),
        })

    return json.dumps({
        "question": question,
        "count": len(events),
        "events": events
    }, indent=2)


@mcp.tool()
def get_actors_summary() -> str:
    """Get a summary of all actors involved in the conflict and their event counts.
    Shows who is doing what to whom."""

    actors1 = pg_query("""
        SELECT actor1 as actor, COUNT(*) as events_initiated,
               array_agg(DISTINCT category) as categories
        FROM events WHERE actor1 IS NOT NULL AND actor1 != ''
        GROUP BY actor1 ORDER BY events_initiated DESC LIMIT 20
    """)

    actors2 = pg_query("""
        SELECT actor2 as actor, COUNT(*) as events_targeted,
               array_agg(DISTINCT category) as categories
        FROM events WHERE actor2 IS NOT NULL AND actor2 != ''
        GROUP BY actor2 ORDER BY events_targeted DESC LIMIT 20
    """)

    return json.dumps({
        "top_initiators": actors1,
        "top_targets": actors2,
    }, indent=2)


# ══════════════════════════════════════
#  RESOURCES
# ══════════════════════════════════════

@mcp.resource("conflict://overview")
def resource_overview() -> str:
    """Current conflict overview and statistics."""
    return get_conflict_overview()

@mcp.resource("conflict://latest")
def resource_latest() -> str:
    """The 10 most recent conflict events."""
    rows = pg_query("""
        SELECT id, category, summary, location_name, country,
               event_time, severity, confidence, confidence_score
        FROM events ORDER BY event_time DESC LIMIT 10
    """)
    return json.dumps(rows, indent=2)


# ══════════════════════════════════════
#  RUN
# ══════════════════════════════════════

if __name__ == "__main__":
    log.info("🔌 Starting Conflict Monitor MCP Server...")
    mcp.run(transport="stdio")