# 🌍 Conflict Intelligence Platform

A real-time, AI-powered conflict monitoring system tracking the 2026 US-Iran war. Ingests data from multiple sources, classifies events using LLMs, scores confidence through cross-source triangulation, and exposes an interactive dashboard with RAG-powered natural language querying and MCP server integration for AI agents.

![Architecture](https://img.shields.io/badge/Architecture-Event--Driven_Microservices-blue)
![Go](https://img.shields.io/badge/Go-1.22-00ADD8?logo=go)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)
![Kafka](https://img.shields.io/badge/Kafka-Redpanda-FF4500)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-PostGIS-336791?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)

---

## Problem Statement

Information about ongoing conflicts is fragmented across slow official datasets and fast but unreliable social platforms. Researchers, journalists, and the public struggle to get a timely, trustworthy picture of events.

**This platform solves that by:**
- Aggregating and normalizing multiple semi-trusted data sources (GDELT, ACLED, news, social media)
- Using AI to classify, geolocate, and score events by reliability and severity
- Presenting an up-to-date, transparent view of the conflict with explicit uncertainty indicators

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA INGESTION (Go)                        │
│                                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐   │
│  │  GDELT   │  │   RSS    │  │  ACLED   │  │ Social Media │   │
│  │ (15 min) │  │ (5 min)  │  │  (1 hr)  │  │   (3 min)    │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └──────┬───────┘   │
│       └──────────────┴─────────────┴───────────────┘           │
│                              │                                  │
└──────────────────────────────┼──────────────────────────────────┘
                               ▼
                    ┌─────────────────────┐
                    │   Kafka (Redpanda)  │
                    │  raw-gdelt-events   │
                    │  raw-news-events    │
                    │  processed-events   │
                    └──────────┬──────────┘
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                   AI PROCESSING PIPELINE (Python)                │
│                                                                  │
│  ┌───────────────┐  ┌───────────┐  ┌────────────┐  ┌─────────┐ │
│  │ LLM Classifier│  │ Geocoder  │  │ Confidence │  │Embedding│ │
│  │ (Groq/Llama)  │  │(Nominatim)│  │  Scorer    │  │(MiniLM) │ │
│  └───────┬───────┘  └─────┬─────┘  └─────┬──────┘  └────┬────┘ │
│          └────────────────┴──────────────┴───────────────┘      │
└──────────────────────────────┬───────────────────────────────────┘
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                    THREE-STORE ARCHITECTURE                       │
│                                                                  │
│  ┌──────────────────┐  ┌───────────────┐  ┌──────────────────┐  │
│  │ PostgreSQL+PostGIS│  │ Elasticsearch │  │  Qdrant (Vector) │  │
│  │ Geospatial queries│  │ Full-text     │  │  RAG / Semantic  │  │
│  │ Primary store     │  │ search        │  │  search          │  │
│  └──────────────────┘  └───────────────┘  └──────────────────┘  │
└──────────────────────────────┬───────────────────────────────────┘
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                        ACCESS LAYER                              │
│                                                                  │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────┐ │
│  │  FastAPI REST   │  │  MCP Server    │  │  React Dashboard   │ │
│  │  /api/events    │  │  7 tools for   │  │  Map + Events +    │ │
│  │  /api/rag/query │  │  Claude/Agents │  │  AI Chat           │ │
│  └────────────────┘  └────────────────┘  └────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Ingestion** | Go 1.22 | Concurrent multi-source data polling via goroutines |
| **Message Broker** | Redpanda (Kafka-compatible) | Event-driven decoupling of ingestion from processing |
| **AI/NLP** | Groq (Llama 3.3 70B) | Event classification, summarization, entity extraction |
| **Embeddings** | sentence-transformers (MiniLM) | Vector embeddings for semantic search / RAG |
| **Primary DB** | PostgreSQL + PostGIS | Structured events with geospatial queries |
| **Search Index** | Elasticsearch | Full-text search across events and articles |
| **Vector DB** | Qdrant | Semantic similarity search powering RAG |
| **API** | FastAPI (Python) | REST endpoints, WebSocket, RAG query |
| **MCP Server** | MCP SDK (Python) | 7 tools exposing data to Claude and AI agents |
| **Frontend** | React + Leaflet | Interactive map, event list, AI chat interface |
| **Infrastructure** | Docker Compose | Single-command deployment of 8 services |

---

## Data Sources

| Source | Type | Frequency | Trust Tier | Purpose |
|--------|------|-----------|-----------|---------|
| **GDELT** | Structured events | Every 15 min | Tier 2 | Near real-time event candidates (fast lane) |
| **ACLED** | Curated events | Every 1 hr | Tier 1 | Ground truth validation layer |
| **RSS News** | Article text | Every 5 min | Tier 2 | Rich text for LLM classification |
| **Social Media** | Tweets/posts | Every 3 min | Social | Weak signals, require corroboration |

**Confidence Triangulation:** Events start at low confidence from a single source and get upgraded as additional independent sources corroborate them. ACLED (tier 1) confirmation can push confidence to 0.9+.

---

## Key Features

### Event Classification
Every ingested event is processed by an LLM that extracts:
- **Category**: battle, explosion/remote violence, violence against civilians, protest, strategic development, naval engagement, cyber attack
- **Actors**: who did what to whom
- **Location**: geocoded to lat/lng coordinates
- **Severity**: 1-10 scale
- **Summary**: human-readable one-sentence description

### Confidence Scoring
Each event receives a confidence score (0.0-1.0) based on:
- Source tier weight (tier 1 = 0.40, tier 2 = 0.30, social = 0.10)
- Data completeness (has coordinates, actors, text)
- GDELT signals (number of mentions, Goldstein intensity)
- Cross-source corroboration (confidence upgrades when multiple sources confirm)

### RAG-Powered Querying
Ask natural language questions like "What happened near the Strait of Hormuz?" and get AI-generated analytical answers grounded in your event database via vector similarity search.

### MCP Server Integration
Expose your conflict database as tools for Claude or any MCP-compatible AI agent:
- `get_conflict_overview` — Dashboard statistics
- `search_events` — Keyword search with filters
- `get_events_near_location` — Geospatial proximity search
- `get_event_details` — Full event with sources and confidence history
- `get_timeline` — Daily event counts by category
- `semantic_search` — Vector-based natural language search
- `get_actors_summary` — Actor analysis (who's doing what to whom)

---

## Quick Start

### Prerequisites
- Docker Desktop
- Go 1.22+ (for local development)
- Python 3.11+ (for MCP server)
- Groq API key (free at https://console.groq.com)

### 1. Clone and configure

```bash
git clone https://github.com/vyasyash-neu/conflict-monitor.git
cd conflict-monitor
cp .env.example .env
# Edit .env with your API keys
```

### 2. Start infrastructure

```bash
docker compose up -d postgres redpanda qdrant elasticsearch
```

### 3. Create Kafka topics

```bash
docker exec redpanda rpk topic create raw-gdelt-events raw-news-events processed-events --partitions 3
```

### 4. Build and start all services

```bash
docker compose build
docker compose up -d
```

### 5. Verify everything is running

```bash
docker compose ps
# Should show 7 services: postgres, redpanda, qdrant, elasticsearch, go-ingestion, ai-pipeline, api
```

### 6. Access the dashboard

Open `frontend/index.html` in your browser, or:

```bash
cd frontend && python3 -m http.server 3000
# Visit http://localhost:3000
```

### 7. Test the API

```bash
# Health check
curl http://localhost:8000/health

# Get stats
curl http://localhost:8000/api/events/stats

# Get events for map
curl http://localhost:8000/api/events/geo

# RAG query
curl -X POST http://localhost:8000/api/rag/query \
  -H "Content-Type: application/json" \
  -d '{"question": "What is happening in Iran?"}'
```

### 8. Connect MCP Server to Claude Desktop

```bash
cd mcp-server
python3.11 -m pip install -r requirements.txt
```

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "conflict-monitor": {
      "command": "python3.11",
      "args": ["<path-to-project>/mcp-server/server.py"],
      "env": {
        "POSTGRES_URL": "postgresql://conflict:conflict_secret@localhost:5433/conflict_monitor",
        "QDRANT_URL": "http://localhost:6333"
      }
    }
  }
}
```

Restart Claude Desktop. You'll see the 🔨 hammer icon with 7 conflict monitoring tools available.

---

## Project Structure

```
conflict-monitor/
├── docker-compose.yml          # Orchestrates all 8 services
├── .env                        # API keys and credentials
├── docker/
│   └── postgres/
│       └── init.sql            # Schema with PostGIS, enums, triggers
├── go-ingestion/               # Go ingestion microservice
│   ├── Dockerfile
│   ├── cmd/main.go             # Entry point with goroutines
│   └── internal/
│       ├── gdelt/poller.go     # GDELT API poller + CSV parser
│       ├── news/rss.go         # RSS feed reader with keyword filtering
│       ├── acled/poller.go     # ACLED API with OAuth authentication
│       ├── social/twitter.go   # Twitter/Nitter social media monitor
│       ├── kafka/producer.go   # Kafka producer wrapper
│       └── models/event.go     # Unified event schema + conflict filter
├── ai-pipeline/                # Python AI processing pipeline
│   ├── Dockerfile
│   ├── main.py                 # Kafka consumer + orchestration
│   ├── processors/
│   │   ├── classifier.py       # Groq LLM event classifier
│   │   ├── geocoder.py         # Geocoding with known locations cache
│   │   └── confidence.py       # Multi-factor confidence scoring
│   ├── embeddings/
│   │   └── vectorizer.py       # sentence-transformers embeddings
│   └── consumers/
│       └── storage.py          # Writes to PostgreSQL, ES, Qdrant
├── api/                        # FastAPI REST backend
│   ├── Dockerfile
│   ├── main.py
│   ├── routers/
│   │   ├── events.py           # CRUD, stats, geo, timeline endpoints
│   │   ├── search.py           # Full-text search
│   │   └── rag.py              # RAG query with vector search + LLM
│   └── services/
│       ├── db.py               # PostgreSQL connection
│       └── vectorizer.py       # Qdrant + embedding model
├── mcp-server/                 # MCP server for Claude integration
│   ├── server.py               # 7 tools + 2 resources
│   └── requirements.txt
└── frontend/
    └── index.html              # Dashboard with map, events, AI chat
```

---

## Database Schema

The core `events` table includes:
- **Classification**: category (enum), sub_type, severity (1-10)
- **Location**: PostGIS geometry point, location name, country, region
- **Actors**: actor1, actor2 with type classifications
- **Confidence**: score (0-1), level (enum), source count
- **AI Content**: LLM-generated summary, raw source text
- **Audit**: confidence_log table tracking score changes over time

Key indexes: geospatial (GIST), temporal, category, confidence composite.

---

## Confidence Scoring Model

```
Score = Source Tier Weight (0.10-0.40)
      + Data Completeness (0-0.25)
      + GDELT Signals (0-0.20)
      + Goldstein Intensity (0-0.10)
      + News Text Bonus (0-0.10)

Levels:
  confirmed  ≥ 0.85  (ACLED validated + multiple sources)
  high       ≥ 0.65  (tier 1/2 source + good data)
  medium     ≥ 0.40  (single reliable source)
  low        ≥ 0.15  (single noisy source)
  unverified < 0.15  (social media only, uncorroborated)
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Service health check |
| GET | `/api/events/` | List events with filters (category, country, confidence) |
| GET | `/api/events/stats` | Dashboard statistics |
| GET | `/api/events/geo` | Geolocated events for map display |
| GET | `/api/events/timeline` | Daily event counts by category |
| GET | `/api/events/{id}` | Single event with sources |
| GET | `/api/search/?q=` | Full-text search |
| POST | `/api/rag/query` | RAG-powered natural language query |

---

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `GROQ_API_KEY` | Groq API key for LLM classification | Yes |
| `ACLED_USERNAME` | ACLED myACLED email | No |
| `ACLED_PASSWORD` | ACLED myACLED password | No |
| `TWITTER_BEARER_TOKEN` | Twitter API v2 bearer token | No |
| `POSTGRES_USER` | PostgreSQL username | Yes (default: conflict) |
| `POSTGRES_PASSWORD` | PostgreSQL password | Yes (default: conflict_secret) |

---

## Development

### Rebuild a single service
```bash
docker compose build <service-name>
docker compose up -d <service-name>
docker compose logs -f <service-name>
```

### Check event counts
```bash
docker exec postgres psql -U conflict -d conflict_monitor \
  -c "SELECT COUNT(*) FROM events;"
```

### Check source distribution
```bash
docker exec postgres psql -U conflict -d conflict_monitor \
  -c "SELECT source_type, COUNT(*) FROM event_sources GROUP BY source_type;"
```

### Stop everything
```bash
docker compose down
```

### Stop and remove all data
```bash
docker compose down -v
```

---

## Future Enhancements

- **Deduplication Engine**: Merge same events from multiple sources using geo proximity + time window + semantic similarity
- **Confidence Evolution UI**: Visualize how event confidence changes as new sources corroborate
- **WebSocket Live Feed**: Replace polling with real-time push via Kafka consumer in the API
- **Source Credibility Model**: Track and adjust source reliability over time based on corroboration rates
- **Event Relationship Graph**: Detect causal chains between events (strike → retaliation → diplomatic response)

---

## Author

**Yash Vyas** — MS Computer Science, Northeastern University
- [LinkedIn](https://www.linkedin.com/in/vyas-yash/)
- [GitHub](https://github.com/vyasyash-neu)

---

## License

MIT