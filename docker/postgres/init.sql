-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- for fuzzy text search

-- ── Enum Types ──
CREATE TYPE event_category AS ENUM (
    'battle',
    'explosion_remote_violence',   -- airstrikes, missile launches, shelling
    'violence_against_civilians',
    'protest',
    'riot',
    'strategic_development',       -- troop movements, naval maneuvers, diplomatic
    'naval_engagement',
    'cyber_attack',
    'humanitarian_incident'
);

CREATE TYPE confidence_level AS ENUM ('unverified', 'low', 'medium', 'high', 'confirmed');

CREATE TYPE source_tier AS ENUM ('tier1', 'tier2', 'tier3', 'social');
-- tier1: Reuters, AP, AFP, official govt/military statements
-- tier2: Major outlets (BBC, Al Jazeera, CNN, NYT)
-- tier3: Regional/local outlets, NGO reports
-- social: Verified journalist social accounts

-- ── Core Events Table ──
CREATE TABLE events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_hash      VARCHAR(64) UNIQUE NOT NULL,  -- for deduplication
    
    -- Classification
    category        event_category NOT NULL,
    sub_type        VARCHAR(100),          -- e.g., 'airstrike', 'ballistic_missile', 'drone_strike'
    
    -- Location
    location_name   VARCHAR(500) NOT NULL,
    country         VARCHAR(100) NOT NULL,
    region          VARCHAR(200),
    geom            GEOMETRY(POINT, 4326), -- lat/lng via PostGIS
    geo_precision   VARCHAR(20) DEFAULT 'approximate',  -- exact, approximate, region
    
    -- Time
    event_time      TIMESTAMPTZ NOT NULL,
    event_time_precision VARCHAR(20) DEFAULT 'day',  -- exact, hour, day
    
    -- Actors
    actor1          VARCHAR(300),          -- e.g., 'United States Military'
    actor1_type     VARCHAR(100),          -- state, rebel, militia, civilian
    actor2          VARCHAR(300),          -- target/other party
    actor2_type     VARCHAR(100),
    
    -- Impact
    severity        SMALLINT CHECK (severity BETWEEN 1 AND 10),
    fatalities      INTEGER,
    fatalities_precision VARCHAR(20) DEFAULT 'unknown', -- exact, estimated, unknown
    
    -- AI-generated content
    summary         TEXT,                  -- LLM-generated human-readable summary
    raw_text        TEXT,                  -- original source text that was processed
    
    -- Confidence & scoring
    confidence      confidence_level DEFAULT 'unverified',
    confidence_score FLOAT CHECK (confidence_score BETWEEN 0 AND 1) DEFAULT 0.0,
    source_count    INTEGER DEFAULT 1,
    
    -- Metadata
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    processing_version VARCHAR(20) DEFAULT 'v1'
);

-- ── Sources Table ──
CREATE TABLE event_sources (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID REFERENCES events(id) ON DELETE CASCADE,
    
    source_name     VARCHAR(300) NOT NULL,
    source_url      TEXT,
    source_tier     source_tier NOT NULL,
    source_type     VARCHAR(50),           -- gdelt, rss, acled, social
    
    published_at    TIMESTAMPTZ,
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    
    -- GDELT-specific
    gdelt_event_id  VARCHAR(50),
    gdelt_cameo_code VARCHAR(20),
    
    -- Raw payload from source
    raw_payload     JSONB
);

-- ── Confidence Audit Log ──
-- Tracks how confidence changed over time as new sources corroborate
CREATE TABLE confidence_log (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID REFERENCES events(id) ON DELETE CASCADE,
    old_score       FLOAT,
    new_score       FLOAT,
    reason          TEXT,     -- e.g., 'New tier1 source: Reuters confirmed'
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── Indexes ──
CREATE INDEX idx_events_geom ON events USING GIST (geom);
CREATE INDEX idx_events_time ON events (event_time DESC);
CREATE INDEX idx_events_category ON events (category);
CREATE INDEX idx_events_country ON events (country);
CREATE INDEX idx_events_confidence ON events (confidence_score DESC);
CREATE INDEX idx_events_hash ON events (event_hash);
CREATE INDEX idx_events_created ON events (created_at DESC);
CREATE INDEX idx_sources_event ON event_sources (event_id);
CREATE INDEX idx_sources_gdelt ON event_sources (gdelt_event_id);

-- Composite index for common dashboard queries
CREATE INDEX idx_events_dashboard ON events (country, category, event_time DESC, confidence_score DESC);

-- ── Helper function: Update confidence level from score ──
CREATE OR REPLACE FUNCTION update_confidence_level()
RETURNS TRIGGER AS $$
BEGIN
    NEW.confidence := CASE
        WHEN NEW.confidence_score >= 0.85 THEN 'confirmed'
        WHEN NEW.confidence_score >= 0.65 THEN 'high'
        WHEN NEW.confidence_score >= 0.40 THEN 'medium'
        WHEN NEW.confidence_score >= 0.15 THEN 'low'
        ELSE 'unverified'
    END;
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_confidence_level
    BEFORE INSERT OR UPDATE OF confidence_score ON events
    FOR EACH ROW
    EXECUTE FUNCTION update_confidence_level();

-- ── Helper function: Spatial query for nearby events ──
CREATE OR REPLACE FUNCTION find_nearby_events(
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    radius_km DOUBLE PRECISION DEFAULT 50,
    lim INTEGER DEFAULT 50
)
RETURNS SETOF events AS $$
BEGIN
    RETURN QUERY
    SELECT *
    FROM events
    WHERE ST_DWithin(
        geom::geography,
        ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography,
        radius_km * 1000
    )
    ORDER BY event_time DESC
    LIMIT lim;
END;
$$ LANGUAGE plpgsql;