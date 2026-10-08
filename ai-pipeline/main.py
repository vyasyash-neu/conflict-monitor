import os, json, logging, time, hashlib, threading
from kafka import KafkaConsumer, KafkaProducer
from processors.classifier import EventClassifier
from processors.geocoder import Geocoder
from processors.confidence import ConfidenceScorer
from embeddings.vectorizer import EventVectorizer
from consumers.storage import StorageWriter

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
PG_URL = os.getenv("POSTGRES_URL", "postgresql://conflict:conflict_secret@localhost:5433/conflict_monitor")
ES_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")


def make_event_hash(event: dict) -> str:
    """Create a dedup hash from key event fields."""
    raw = f"{event.get('source','')}-{event.get('source_event_id','')}"
    return hashlib.sha256(raw.encode()).hexdigest()


def process_event(raw: dict, classifier: EventClassifier, geocoder: Geocoder,
                  scorer: ConfidenceScorer, vectorizer: EventVectorizer,
                  storage: StorageWriter):
    """Full processing pipeline for a single raw event."""
    try:
        event_hash = make_event_hash(raw)

        # Check if already processed
        if storage.event_exists(event_hash):
            # Existing event — update confidence with new source
            storage.add_source_to_event(event_hash, raw)
            return

        # Step 1: LLM Classification
        classification = classifier.classify(raw)

        # Step 2: Geocoding (if no coords from source)
        lat = raw.get("lat", 0)
        lng = raw.get("lng", 0)
        location_name = raw.get("location_raw", "") or classification.get("location", "")
        if (lat == 0 or lng == 0) and location_name:
            coords = geocoder.geocode(location_name)
            if coords:
                lat, lng = coords

        # Step 3: Confidence scoring
        confidence_score = scorer.score(raw, classification)

        # Step 4: Build enriched event
        enriched = {
            "event_hash": event_hash,
            "category": classification.get("category", "strategic_development"),
            "sub_type": classification.get("sub_type", ""),
            "location_name": location_name or raw.get("title", "Unknown"),
            "country": raw.get("country", "") or classification.get("country", "Unknown"),
            "region": classification.get("region", ""),
            "lat": lat,
            "lng": lng,
            "geo_precision": "exact" if raw.get("lat", 0) != 0 else "approximate",
            "event_time": raw.get("event_time", ""),
            "actor1": raw.get("actor1", "") or classification.get("actor1", ""),
            "actor1_type": classification.get("actor1_type", ""),
            "actor2": raw.get("actor2", "") or classification.get("actor2", ""),
            "actor2_type": classification.get("actor2_type", ""),
            "severity": classification.get("severity", 5),
            "fatalities": classification.get("fatalities"),
            "summary": classification.get("summary", ""),
            "raw_text": raw.get("raw_text", "") or raw.get("title", ""),
            "confidence_score": confidence_score,
            "source_count": 1,
            # Source info
            "source": raw.get("source", ""),
            "source_name": raw.get("source_name", ""),
            "source_url": raw.get("source_url", ""),
            "source_tier": raw.get("source_tier", "tier3"),
            "source_event_id": raw.get("source_event_id", ""),
            "gdelt_cameo_code": raw.get("gdelt_cameo_code", ""),
            "raw_payload": raw,
        }

        # Step 5: Generate embedding
        embedding = vectorizer.embed(enriched)

        # Step 6: Write to all three stores
        storage.write_event(enriched, embedding)

        log.info(f"✅ Processed: [{enriched['category']}] {enriched['summary'][:80]}... (confidence: {confidence_score:.2f})")

    except Exception as e:
        log.error(f"❌ Failed to process event: {e}", exc_info=True)


def consume_topic(topic: str, classifier, geocoder, scorer, vectorizer, storage):
    """Consume from a single Kafka topic."""
    log.info(f"📥 Starting consumer for topic: {topic}")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="ai-pipeline",
        consumer_timeout_ms=10000,
    )

    while True:
        try:
            for msg in consumer:
                process_event(msg.value, classifier, geocoder, scorer, vectorizer, storage)
        except Exception as e:
            log.error(f"Consumer error on {topic}: {e}")
            time.sleep(5)


def main():
    log.info("🧠 AI Processing Pipeline Starting...")
    log.info(f"   Kafka: {KAFKA_BROKER}")
    log.info(f"   Postgres: {PG_URL}")
    log.info(f"   Elasticsearch: {ES_URL}")
    log.info(f"   Qdrant: {QDRANT_URL}")

    # Wait for services
    time.sleep(5)

    # Initialize components
    classifier = EventClassifier(GROQ_API_KEY)
    geocoder = Geocoder()
    scorer = ConfidenceScorer()
    vectorizer = EventVectorizer(QDRANT_URL)
    storage = StorageWriter(PG_URL, ES_URL, QDRANT_URL)

    log.info("✅ All components initialized")

    # Consume both topics in parallel threads
    topics = ["raw-gdelt-events", "raw-news-events"]
    threads = []
    for topic in topics:
        t = threading.Thread(
            target=consume_topic,
            args=(topic, classifier, geocoder, scorer, vectorizer, storage),
            daemon=True
        )
        t.start()
        threads.append(t)
        log.info(f"   🔄 Consumer thread started for {topic}")

    # Keep main thread alive
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()