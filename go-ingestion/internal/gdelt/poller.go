package gdelt

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/yashvyas/conflict-monitor/go-ingestion/internal/kafka"
	"github.com/yashvyas/conflict-monitor/go-ingestion/internal/models"
)

const (
	gdeltLastUpdateURL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
)

type Poller struct {
	producer *kafka.Producer
	filter   models.ConflictFilter
	client   *http.Client
	seen     map[string]bool // dedup within polling window
}

func NewPoller(producer *kafka.Producer, filter models.ConflictFilter) *Poller {
	return &Poller{
		producer: producer,
		filter:   filter,
		client:   &http.Client{Timeout: 60 * time.Second},
		seen:     make(map[string]bool),
	}
}

// Start begins the polling loop. Runs every interval (typically 15 min to match GDELT updates).
func (p *Poller) Start(interval time.Duration) {
	log.Printf("🌐 GDELT Poller starting (interval: %s)", interval)
	log.Printf("📍 Filtering for countries: %v", p.filter.Countries)
	log.Printf("⚔️  CAMEO prefixes: %v", p.filter.CAMEOPrefixes)

	// Poll immediately on start, then on interval
	p.poll()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		p.poll()
	}
}

func (p *Poller) poll() {
	log.Println("🔄 Polling GDELT for latest events...")

	// Step 1: Get the latest export file URL
	exportURL, err := p.getLatestExportURL()
	if err != nil {
		log.Printf("❌ Failed to get latest GDELT URL: %v", err)
		return
	}
	log.Printf("📥 Fetching: %s", exportURL)

	// Step 2: Download and parse the CSV
	events, err := p.fetchAndParse(exportURL)
	if err != nil {
		log.Printf("❌ Failed to fetch/parse GDELT data: %v", err)
		return
	}

	// Step 3: Filter for conflict-relevant events
	filtered := p.applyFilter(events)
	log.Printf("📊 GDELT: %d total events → %d conflict-relevant after filtering", len(events), len(filtered))

	// Step 4: Publish to Kafka
	published := 0
	for _, e := range filtered {
		if p.seen[e.SourceEventID] {
			continue
		}
		if err := p.producer.Publish(e.SourceEventID, e); err != nil {
			log.Printf("❌ Failed to publish event %s: %v", e.SourceEventID, err)
			continue
		}
		p.seen[e.SourceEventID] = true
		published++
	}
	p.producer.Flush(5000)
	log.Printf("✅ Published %d new events to Kafka", published)

	// Trim seen map if it gets too large (keep last 50k)
	if len(p.seen) > 50000 {
		p.seen = make(map[string]bool)
	}
}

// getLatestExportURL fetches the GDELT lastupdate file to find the current export CSV URL
func (p *Poller) getLatestExportURL() (string, error) {
	resp, err := p.client.Get(gdeltLastUpdateURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	// The lastupdate.txt has 3 lines: export, mentions, gkg
	// We want the first line (export), third column (URL)
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) == 0 {
		return "", fmt.Errorf("empty lastupdate response")
	}

	fields := strings.Fields(lines[0])
	if len(fields) < 3 {
		return "", fmt.Errorf("unexpected format in lastupdate: %s", lines[0])
	}

	return fields[2], nil // URL is the 3rd field
}

// fetchAndParse downloads a zipped GDELT CSV and parses it into RawEvents
func (p *Poller) fetchAndParse(url string) ([]models.RawEvent, error) {
	resp, err := p.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("GDELT returned status %d", resp.StatusCode)
	}

	// Read entire body into memory (GDELT CSVs are small, ~1-5MB)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Open as zip archive
	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, fmt.Errorf("failed to open zip: %w", err)
	}

	var events []models.RawEvent
	for _, f := range zipReader.File {
		rc, err := f.Open()
		if err != nil {
			continue
		}

		reader := csv.NewReader(rc)
		reader.Comma = '\t'
		reader.LazyQuotes = true
		reader.FieldsPerRecord = -1

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				continue
			}
			if e, ok := p.parseRecord(record); ok {
				events = append(events, e)
			}
		}
		rc.Close()
	}
	return events, nil
}

// parseRecord converts a GDELT CSV row into a RawEvent
// GDELT v2 Event CSV columns reference:
// 0: GlobalEventID, 1-4: dates, 5-14: Actor1 info, 15-24: Actor2 info
// 25: IsRootEvent, 26: EventCode, 27: EventBaseCode, 28: EventRootCode
// 29: QuadClass, 30: GoldsteinScale, 31: NumMentions, 32: NumSources
// 33: NumArticles, 34: AvgTone
// 35-39: Actor1Geo, 40-44: Actor2Geo, 45-49: ActionGeo
// 50: DATEADDED, 51: SOURCEURL
func (p *Poller) parseRecord(r []string) (models.RawEvent, bool) {
	if len(r) < 52 {
		return models.RawEvent{}, false
	}

	eventID := r[0]
	cameoCode := r[26]
	goldstein, _ := strconv.ParseFloat(r[30], 64)
	numMentions, _ := strconv.Atoi(r[31])
	avgTone, _ := strconv.ParseFloat(r[34], 64)

	// Action geography (where the event happened)
	actionGeoType := r[45]
	actionGeoName := r[46]
	actionCountry := r[47]
	actionLat, _ := strconv.ParseFloat(r[49], 64)
	actionLng, _ := strconv.ParseFloat(r[50], 64) // col 50 can be lng or DATEADDED depending on version

	// Handle GDELT's variable column layout
	sourceURL := ""
	if len(r) > 60 {
		sourceURL = r[60]
	} else if len(r) > 57 {
		sourceURL = r[57]
	}

	// Parse event date
	dateStr := r[1] // SQLDATE (YYYYMMDD)
	eventTime, err := time.Parse("20060102", dateStr)
	if err != nil {
		eventTime = time.Now()
	}

	// Actor names
	actor1 := r[6]         // Actor1Name
	actor2 := r[16]        // Actor2Name
	actor1Country := r[7]  // Actor1CountryCode
	actor2Country := r[17] // Actor2CountryCode

	_ = actionGeoType // used for geo_precision later

	return models.RawEvent{
		ID:            uuid.New().String(),
		Source:        "gdelt",
		SourceEventID: eventID,
		SourceURL:     sourceURL,
		SourceName:    "GDELT",
		SourceTier:    "tier2",

		Title:    fmt.Sprintf("GDELT Event %s: %s → %s in %s", cameoCode, actor1, actor2, actionGeoName),
		RawText:  "", // GDELT doesn't provide article text, AI pipeline will fetch via URL
		Language: "en",

		EventType:   cameoCode,
		LocationRaw: actionGeoName,
		Country:     actionCountry,
		Lat:         actionLat,
		Lng:         actionLng,

		Actor1: formatActor(actor1, actor1Country),
		Actor2: formatActor(actor2, actor2Country),

		EventTime:  eventTime,
		IngestedAt: time.Now().UTC(),

		GDELTCameoCode:   cameoCode,
		GDELTGoldstein:   goldstein,
		GDELTNumMentions: numMentions,
		GDELTAvgTone:     avgTone,
	}, true
}

// applyFilter returns only events matching our conflict criteria
func (p *Poller) applyFilter(events []models.RawEvent) []models.RawEvent {
	var out []models.RawEvent
	for _, e := range events {
		if !p.matchesCountry(e) {
			continue
		}
		if !p.matchesCAMEO(e) {
			continue
		}
		out = append(out, e)
	}
	return out
}

func (p *Poller) matchesCountry(e models.RawEvent) bool {
	for _, c := range p.filter.Countries {
		cl := strings.ToLower(c)
		if strings.ToLower(e.Country) == cl ||
			strings.Contains(strings.ToLower(e.LocationRaw), cl) ||
			strings.Contains(strings.ToLower(e.Actor1), cl) ||
			strings.Contains(strings.ToLower(e.Actor2), cl) {
			return true
		}
	}
	return false
}

func (p *Poller) matchesCAMEO(e models.RawEvent) bool {
	for _, prefix := range p.filter.CAMEOPrefixes {
		if strings.HasPrefix(e.GDELTCameoCode, prefix) {
			return true
		}
	}
	return false
}

func formatActor(name, country string) string {
	if name == "" && country == "" {
		return "Unknown"
	}
	if name == "" {
		return country
	}
	if country != "" && !strings.Contains(strings.ToLower(name), strings.ToLower(country)) {
		return fmt.Sprintf("%s (%s)", name, country)
	}
	return name
}
