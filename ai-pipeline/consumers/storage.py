import json, uuid, logging
from urllib.request import Request, urlopen
from urllib.error import URLError
import psycopg2
from psycopg2.extras import Json
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct

log = logging.getLogger(__name__)

COLLECTION_NAME = "conflict_events"
ES_INDEX = "conflict_events"

ES_MAPPING = {
    "mappings": {
        "properties": {
            "event_hash": {"type": "keyword"},
            "category": {"type": "keyword"},
            "sub_type": {"type": "keyword"},
            "summary": {"type": "text", "analyzer": "english"},
            "location_name": {"type": "text"},
            "country": {"type": "keyword"},
            "region": {"type": "keyword"},
            "location": {"type": "geo_point"},
            "event_time": {"type": "date"},
            "actor1": {"type": "text"},
            "actor2": {"type": "text"},
            "severity": {"type": "integer"},
            "fatalities": {"type": "integer"},
            "confidence_score": {"type": "float"},
            "source_count": {"type": "integer"},
            "source_name": {"type": "keyword"},
            "raw_text": {"type": "text", "analyzer": "english"},
        }
    }
}


class StorageWriter:
    def __init__(self, pg_url: str, es_url: str, qdrant_url: str):
        self.pg = psycopg2.connect(pg_url)
        self.pg.autocommit = True
        log.info("✅ PostgreSQL connected")

        self.es_url = es_url.rstrip("/")
        self._ensure_es_index()
        log.info("✅ Elasticsearch connected")

        self.qdrant = QdrantClient(url=qdrant_url, check_compatibility=False)
        log.info("✅ Qdrant connected")

    def _ensure_es_index(self):
        try:
            req = Request(f"{self.es_url}/{ES_INDEX}", method="HEAD")
            try:
                urlopen(req)
                log.info(f"📦 ES index {ES_INDEX} already exists")
            except URLError as e:
                if hasattr(e, 'code') and e.code == 404:
                    data = json.dumps(ES_MAPPING).encode("utf-8")
                    req = Request(f"{self.es_url}/{ES_INDEX}", data=data, method="PUT",
                                  headers={"Content-Type": "application/json"})
                    urlopen(req)
                    log.info(f"📦 Created ES index: {ES_INDEX}")
                else:
                    # Index exists but returned non-200 (e.g. 400 from product check) — try creating
                    data = json.dumps(ES_MAPPING).encode("utf-8")
                    req = Request(f"{self.es_url}/{ES_INDEX}", data=data, method="PUT",
                                  headers={"Content-Type": "application/json"})
                    try:
                        urlopen(req)
                        log.info(f"📦 Created ES index: {ES_INDEX}")
                    except Exception:
                        log.info(f"📦 ES index {ES_INDEX} likely already exists")
        except Exception as e:
            log.error(f"❌ ES index setup failed: {e}")

    def _es_index_doc(self, doc_id: str, doc: dict):
        try:
            data = json.dumps(doc, default=str).encode("utf-8")
            req = Request(f"{self.es_url}/{ES_INDEX}/_doc/{doc_id}", data=data, method="PUT",
                          headers={"Content-Type": "application/json"})
            urlopen(req)
        except Exception as e:
            log.error(f"❌ ES write failed: {e}")

    def event_exists(self, event_hash: str) -> bool:
        with self.pg.cursor() as cur:
            cur.execute("SELECT 1 FROM events WHERE event_hash = %s", (event_hash,))
            return cur.fetchone() is not None

    def add_source_to_event(self, event_hash: str, raw: dict):
        try:
            with self.pg.cursor() as cur:
                cur.execute("SELECT id, confidence_score, source_count FROM events WHERE event_hash = %s", (event_hash,))
                row = cur.fetchone()
                if not row:
                    return
                event_id, old_score, source_count = row

                cur.execute("""
                    INSERT INTO event_sources (event_id, source_name, source_url, source_tier, source_type, raw_payload)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    event_id, raw.get("source_name", ""), raw.get("source_url", ""),
                    raw.get("source_tier", "tier3"), raw.get("source", ""), Json(raw)
                ))

                new_count = source_count + 1
                boost = 0.10 if raw.get("source_tier") in ("tier1", "tier2") else 0.05
                new_score = min(old_score + boost, 1.0)

                cur.execute("""
                    UPDATE events SET confidence_score = %s, source_count = %s WHERE event_hash = %s
                """, (new_score, new_count, event_hash))

                cur.execute("""
                    INSERT INTO confidence_log (event_id, old_score, new_score, reason)
                    VALUES (%s, %s, %s, %s)
                """, (event_id, old_score, new_score,
                      f"New {raw.get('source_tier')} source: {raw.get('source_name')}"))

                log.info(f"📈 Updated confidence for {event_hash[:12]}: {old_score:.2f} → {new_score:.2f}")
        except Exception as e:
            log.error(f"Failed to add source: {e}")

    def write_event(self, enriched: dict, embedding: list):
        event_id = str(uuid.uuid4())

        # ── PostgreSQL ──
        try:
            with self.pg.cursor() as cur:
                lat = enriched.get("lat", 0)
                lng = enriched.get("lng", 0)
                geom = f"SRID=4326;POINT({lng} {lat})" if lat and lng else None

                cur.execute("""
                    INSERT INTO events (
                        id, event_hash, category, sub_type,
                        location_name, country, region, geom, geo_precision,
                        event_time, actor1, actor1_type, actor2, actor2_type,
                        severity, fatalities, summary, raw_text,
                        confidence_score, source_count
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, ST_GeomFromEWKT(%s), %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    event_id, enriched["event_hash"],
                    enriched["category"], enriched.get("sub_type"),
                    enriched["location_name"], enriched["country"],
                    enriched.get("region"), geom, enriched.get("geo_precision", "approximate"),
                    enriched.get("event_time"), enriched.get("actor1"), enriched.get("actor1_type"),
                    enriched.get("actor2"), enriched.get("actor2_type"),
                    enriched.get("severity", 5), enriched.get("fatalities"),
                    enriched.get("summary"), enriched.get("raw_text"),
                    enriched.get("confidence_score", 0), enriched.get("source_count", 1),
                ))

                cur.execute("""
                    INSERT INTO event_sources (
                        event_id, source_name, source_url, source_tier, source_type,
                        gdelt_event_id, gdelt_cameo_code, raw_payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    event_id, enriched.get("source_name"), enriched.get("source_url"),
                    enriched.get("source_tier", "tier3"), enriched.get("source"),
                    enriched.get("source_event_id"), enriched.get("gdelt_cameo_code"),
                    Json(enriched.get("raw_payload", {})),
                ))
        except Exception as e:
            log.error(f"❌ PostgreSQL write failed: {e}")
            return

        # ── Elasticsearch ──
        es_doc = {
            "event_hash": enriched["event_hash"],
            "category": enriched["category"],
            "sub_type": enriched.get("sub_type"),
            "summary": enriched.get("summary"),
            "location_name": enriched["location_name"],
            "country": enriched["country"],
            "region": enriched.get("region"),
            "event_time": enriched.get("event_time"),
            "actor1": enriched.get("actor1"),
            "actor2": enriched.get("actor2"),
            "severity": enriched.get("severity"),
            "fatalities": enriched.get("fatalities"),
            "confidence_score": enriched.get("confidence_score"),
            "source_count": enriched.get("source_count"),
            "source_name": enriched.get("source_name"),
            "raw_text": enriched.get("raw_text", "")[:5000],
        }
        lat, lng = enriched.get("lat", 0), enriched.get("lng", 0)
        if lat and lng:
            es_doc["location"] = {"lat": lat, "lon": lng}
        self._es_index_doc(event_id, es_doc)

        # ── Qdrant ──
        try:
            self.qdrant.upsert(
                collection_name=COLLECTION_NAME,
                points=[
                    PointStruct(
                        id=event_id.replace("-", "")[:32],
                        vector=embedding,
                        payload={
                            "event_id": event_id,
                            "event_hash": enriched["event_hash"],
                            "category": enriched["category"],
                            "summary": enriched.get("summary", ""),
                            "location_name": enriched["location_name"],
                            "country": enriched["country"],
                            "severity": enriched.get("severity", 5),
                            "confidence_score": enriched.get("confidence_score", 0),
                            "event_time": enriched.get("event_time", ""),
                        },
                    )
                ],
            )
        except Exception as e:
            log.error(f"❌ Qdrant write failed: {e}")