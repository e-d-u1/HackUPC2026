-- =============================================================================
-- Travel Intelligence Platform — Database Schema
-- =============================================================================
-- Designed for Postgres 15+
-- Run with: psql -d your_db -f schema.sql
-- =============================================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";   -- for fuzzy name search

-- =============================================================================
-- CORE TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS cities (
    id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    wikidata_id     TEXT        UNIQUE NOT NULL,             -- e.g. "Q90"
    name            TEXT        NOT NULL,
    country         TEXT,
    country_code    CHAR(2),                                 -- ISO 3166-1 alpha-2
    lat             NUMERIC(9,6),
    lon             NUMERIC(9,6),
    population      INTEGER,
    continent       TEXT,

    -- Wikipedia content
    wiki_title      TEXT,
    wiki_url        TEXT,
    wiki_page_id    INTEGER,
    description     TEXT,                                    -- short tagline
    extract         TEXT,                                    -- 2-5 sentence summary

    -- Pipeline metadata
    needs_manual_review BOOLEAN DEFAULT FALSE,
    review_reason   TEXT,
    data_quality_score NUMERIC(3,2),                        -- 0.00–1.00

    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Controlled vocabulary for tags
-- All valid tags are defined here — LLM output is validated against this table
CREATE TABLE IF NOT EXISTS tag_vocabulary (
    tag             TEXT        PRIMARY KEY,
    category        TEXT,       -- e.g. 'nature', 'urban', 'activity', 'vibe'
    description     TEXT
);

-- City ↔ tag mapping (many-to-many)
CREATE TABLE IF NOT EXISTS city_tags (
    id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    city_id         UUID        NOT NULL REFERENCES cities(id) ON DELETE CASCADE,
    tag             TEXT        NOT NULL REFERENCES tag_vocabulary(tag),
    confidence      NUMERIC(3,2) DEFAULT 1.0,   -- 0.00–1.00 from LLM
    source          TEXT        DEFAULT 'llm',   -- 'llm' | 'manual' | 'osm'
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(city_id, tag)
);

-- Continuous vibe scores (one row per vibe dimension per city)
-- This row-per-vibe design lets you add new dimensions without schema changes
CREATE TABLE IF NOT EXISTS city_vibes (
    id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    city_id         UUID        NOT NULL REFERENCES cities(id) ON DELETE CASCADE,
    vibe            TEXT        NOT NULL,        -- e.g. 'adventure', 'relax'
    score           NUMERIC(4,3) NOT NULL        -- 0.000–1.000
                    CHECK (score >= 0 AND score <= 1),
    model           TEXT,                        -- e.g. 'claude-haiku-3-5'
    prompt_version  TEXT,                        -- for re-run tracking
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(city_id, vibe)
);

-- Enrichment audit log — track every LLM call per city
CREATE TABLE IF NOT EXISTS enrichment_log (
    id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    city_id         UUID        NOT NULL REFERENCES cities(id) ON DELETE CASCADE,
    stage           TEXT        NOT NULL,        -- 'wikidata' | 'wikipedia' | 'llm'
    model           TEXT,
    prompt_version  TEXT,
    input_tokens    INTEGER,
    output_tokens   INTEGER,
    cost_usd        NUMERIC(8,6),
    success         BOOLEAN     DEFAULT TRUE,
    error_msg       TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- SEED: TAG VOCABULARY
-- =============================================================================

INSERT INTO tag_vocabulary (tag, category, description) VALUES
-- Nature & Geography
('beach',           'nature',   'Significant beach or coastal area'),
('mountains',       'nature',   'Mountain ranges or significant elevation'),
('lakes',           'nature',   'Notable lakes or rivers'),
('desert',          'nature',   'Desert landscape'),
('jungle',          'nature',   'Tropical rainforest or jungle'),
('islands',         'nature',   'Island destination or island city'),
('volcanoes',       'nature',   'Active or notable volcanic features'),
-- Urban character
('cosmopolitan',    'urban',    'International, multicultural city'),
('historic',        'urban',    'Rich historical heritage'),
('modern',          'urban',    'Contemporary architecture and infrastructure'),
('walkable',        'urban',    'Compact, pedestrian-friendly layout'),
('megacity',        'urban',    'City with >10M population'),
-- Activities
('nightlife',       'activity', 'Active bar/club scene'),
('gastronomy',      'activity', 'Exceptional food culture'),
('shopping',        'activity', 'Major shopping destination'),
('museums',         'activity', 'High density of museums and galleries'),
('music',           'activity', 'Live music or music culture'),
('sport',           'activity', 'Notable sports scene or events'),
('wellness',        'activity', 'Spas, wellness culture'),
('outdoor',         'activity', 'Hiking, cycling, adventure sports'),
('diving',          'activity', 'Scuba diving or snorkeling'),
('skiing',          'activity', 'Skiing or snow sports'),
-- Cultural / religious
('religious',       'culture',  'Major religious sites'),
('art',             'culture',  'Street art, galleries, creative scene'),
('architecture',    'culture',  'Exceptional architecture'),
('festivals',       'culture',  'Famous festivals or events'),
('unesco',          'culture',  'UNESCO World Heritage sites'),
-- Practical
('budget',          'practical','Affordable for budget travellers'),
('luxury',          'practical','High-end experiences and hotels'),
('family',          'practical','Family-friendly destination'),
('solo',            'practical','Good for solo travellers'),
('digital_nomad',   'practical','Good infrastructure for remote workers'),
('visa_friendly',   'practical','Easy visa access for most nationalities')
ON CONFLICT (tag) DO NOTHING;

-- =============================================================================
-- INDEXES
-- =============================================================================

-- Geographic queries
CREATE INDEX IF NOT EXISTS idx_cities_coords
    ON cities (lat, lon);

CREATE INDEX IF NOT EXISTS idx_cities_country_code
    ON cities (country_code);

CREATE INDEX IF NOT EXISTS idx_cities_continent
    ON cities (continent);

-- Fuzzy name search
CREATE INDEX IF NOT EXISTS idx_cities_name_trgm
    ON cities USING GIN (name gin_trgm_ops);

-- Tag queries
CREATE INDEX IF NOT EXISTS idx_city_tags_tag
    ON city_tags (tag);

CREATE INDEX IF NOT EXISTS idx_city_tags_city_id
    ON city_tags (city_id);

-- Vibe queries (the hot path for recommendations)
CREATE INDEX IF NOT EXISTS idx_city_vibes_vibe_score
    ON city_vibes (vibe, score DESC);

CREATE INDEX IF NOT EXISTS idx_city_vibes_city_id
    ON city_vibes (city_id);

-- =============================================================================
-- VIEWS (convenience)
-- =============================================================================

-- Flat view for quick querying: one row per city with vibe scores as columns
CREATE OR REPLACE VIEW city_vibes_flat AS
SELECT
    c.id,
    c.name,
    c.country,
    c.country_code,
    c.lat,
    c.lon,
    c.population,
    c.continent,
    c.description,
    MAX(CASE WHEN cv.vibe = 'adventure' THEN cv.score END) AS vibe_adventure,
    MAX(CASE WHEN cv.vibe = 'relax'     THEN cv.score END) AS vibe_relax,
    MAX(CASE WHEN cv.vibe = 'culture'   THEN cv.score END) AS vibe_culture,
    MAX(CASE WHEN cv.vibe = 'luxury'    THEN cv.score END) AS vibe_luxury,
    MAX(CASE WHEN cv.vibe = 'budget'    THEN cv.score END) AS vibe_budget,
    MAX(CASE WHEN cv.vibe = 'romance'   THEN cv.score END) AS vibe_romance,
    MAX(CASE WHEN cv.vibe = 'family'    THEN cv.score END) AS vibe_family,
    MAX(CASE WHEN cv.vibe = 'nightlife' THEN cv.score END) AS vibe_nightlife,
    MAX(CASE WHEN cv.vibe = 'nature'    THEN cv.score END) AS vibe_nature,
    MAX(CASE WHEN cv.vibe = 'exotic'    THEN cv.score END) AS vibe_exotic
FROM cities c
LEFT JOIN city_vibes cv ON cv.city_id = c.id
GROUP BY c.id;

-- Tags aggregated per city
CREATE OR REPLACE VIEW city_tags_agg AS
SELECT
    c.id,
    c.name,
    ARRAY_AGG(ct.tag ORDER BY ct.confidence DESC) AS tags
FROM cities c
JOIN city_tags ct ON ct.city_id = c.id
GROUP BY c.id, c.name;