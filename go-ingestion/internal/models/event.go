package models

import "time"

// RawEvent is the unified schema that all source adapters normalize into
// before publishing to Kafka. The AI pipeline will enrich this further.
type RawEvent struct {
	ID            string            `json:"id"`
	Source        string            `json:"source"`          // "gdelt", "rss", "acled", "social"
	SourceEventID string           `json:"source_event_id"` // original ID from source
	SourceURL     string            `json:"source_url"`
	SourceName    string            `json:"source_name"`     // "Reuters", "Al Jazeera", etc.
	SourceTier    string            `json:"source_tier"`     // "tier1", "tier2", "tier3", "social"

	// Raw content for NLP processing
	Title    string `json:"title"`
	RawText  string `json:"raw_text"`
	Language string `json:"language"`

	// Pre-extracted fields (best-effort from source, AI will refine)
	EventType   string  `json:"event_type,omitempty"`   // CAMEO code or category hint
	LocationRaw string  `json:"location_raw,omitempty"` // raw place name string
	Country     string  `json:"country,omitempty"`
	Lat         float64 `json:"lat,omitempty"`
	Lng         float64 `json:"lng,omitempty"`

	// Actors (if available from source)
	Actor1 string `json:"actor1,omitempty"`
	Actor2 string `json:"actor2,omitempty"`

	// Timestamps
	EventTime  time.Time `json:"event_time"`
	IngestedAt time.Time `json:"ingested_at"`

	// GDELT-specific fields
	GDELTCameoCode  string  `json:"gdelt_cameo_code,omitempty"`
	GDELTGoldstein  float64 `json:"gdelt_goldstein,omitempty"`  // conflict intensity scale
	GDELTNumMentions int    `json:"gdelt_num_mentions,omitempty"`
	GDELTAvgTone    float64 `json:"gdelt_avg_tone,omitempty"`
}

// ConflictFilter defines which events we care about
type ConflictFilter struct {
	Countries    []string
	CAMEOPrefixes []string // CAMEO codes starting with 18, 19, 20 = assault, fight, use force
	MinGoldstein float64   // negative = conflict (e.g., -10 is war)
}

// DefaultIranConflictFilter returns the filter for US-Iran war events
func DefaultIranConflictFilter() ConflictFilter {
	return ConflictFilter{
		Countries: []string{
			// FIPS 10-4 codes (used by GDELT) + full names (for text matching)
			"IR", "Iran",
			"IS", "Israel",
			"US", "United States",
			"SA", "Saudi Arabia",
			"BA", "Bahrain",
			"KU", "Kuwait",
			"IZ", "Iraq",
			"LE", "Lebanon",
			"QA", "Qatar",
			"SY", "Syria",
			"YM", "Yemen",
			"JO", "Jordan",
			"AE", "United Arab Emirates",
		},
		CAMEOPrefixes: []string{
			"13",  // threaten
			"14",  // protest
			"15",  // exhibit force posture
			"17",  // coerce
			"18",  // assault
			"19",  // fight
			"20",  // use unconventional mass violence
		},
		MinGoldstein: -2.0, // anything negative indicates conflict; Goldstein scale is -10 to +10
	}
}