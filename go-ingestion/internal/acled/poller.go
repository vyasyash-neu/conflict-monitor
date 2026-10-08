package acled

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/yashvyas/conflict-monitor/go-ingestion/internal/kafka"
	"github.com/yashvyas/conflict-monitor/go-ingestion/internal/models"
)

const (
	acledTokenURL = "https://acleddata.com/oauth/token"
	acledDataURL  = "https://acleddata.com/api/acled/read"
)

type tokenResponse struct {
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

type ACLEDResponse struct {
	Status int          `json:"status"`
	Data   []ACLEDEvent `json:"data"`
	Count  int          `json:"count"`
}

type ACLEDEvent struct {
	EventIDCnty  string `json:"event_id_cnty"`
	EventDate    string `json:"event_date"`
	Year         string `json:"year"`
	EventType    string `json:"event_type"`
	SubEventType string `json:"sub_event_type"`
	Actor1       string `json:"actor1"`
	Actor2       string `json:"actor2"`
	Country      string `json:"country"`
	Region       string `json:"region"`
	Admin1       string `json:"admin1"`
	Location     string `json:"location"`
	Latitude     string `json:"latitude"`
	Longitude    string `json:"longitude"`
	Source       string `json:"source"`
	SourceScale  string `json:"source_scale"`
	Notes        string `json:"notes"`
	Fatalities   string `json:"fatalities"`
}

type Poller struct {
	producer     *kafka.Producer
	username     string
	password     string
	client       *http.Client
	seen         map[string]bool
	countries    []string
	accessToken  string
	refreshToken string
	tokenExpiry  time.Time
	mu           sync.Mutex
}

func NewPoller(producer *kafka.Producer, username, password string) *Poller {
	return &Poller{
		producer: producer,
		username: username,
		password: password,
		client:   &http.Client{Timeout: 60 * time.Second},
		seen:     make(map[string]bool),
		countries: []string{
			"Iran", "Israel", "Iraq", "Lebanon", "Syria",
			"Saudi Arabia", "Bahrain", "Kuwait", "Qatar",
			"United Arab Emirates", "Yemen", "Jordan",
		},
	}
}

// ── OAuth Token Management ──

func (p *Poller) authenticate() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Use refresh token if we have one
	if p.refreshToken != "" {
		if err := p.refreshAccessToken(); err == nil {
			return nil
		}
		log.Println("⚠️ ACLED refresh failed, re-authenticating...")
	}

	data := url.Values{}
	data.Set("username", p.username)
	data.Set("password", p.password)
	data.Set("grant_type", "password")
	data.Set("client_id", "acled")

	resp, err := p.client.Post(acledTokenURL, "application/x-www-form-urlencoded", bytes.NewBufferString(data.Encode()))
	if err != nil {
		return fmt.Errorf("auth request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("auth failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tok tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		return fmt.Errorf("failed to decode token: %w", err)
	}

	p.accessToken = tok.AccessToken
	p.refreshToken = tok.RefreshToken
	p.tokenExpiry = time.Now().Add(time.Duration(tok.ExpiresIn-60) * time.Second) // refresh 1 min early

	log.Println("🔑 ACLED OAuth token acquired")
	return nil
}

func (p *Poller) refreshAccessToken() error {
	data := url.Values{}
	data.Set("refresh_token", p.refreshToken)
	data.Set("grant_type", "refresh_token")
	data.Set("client_id", "acled")

	resp, err := p.client.Post(acledTokenURL, "application/x-www-form-urlencoded", bytes.NewBufferString(data.Encode()))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("refresh failed: %d", resp.StatusCode)
	}

	var tok tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		return err
	}

	p.accessToken = tok.AccessToken
	p.refreshToken = tok.RefreshToken
	p.tokenExpiry = time.Now().Add(time.Duration(tok.ExpiresIn-60) * time.Second)

	log.Println("🔑 ACLED OAuth token refreshed")
	return nil
}

func (p *Poller) getToken() (string, error) {
	p.mu.Lock()
	expired := p.accessToken == "" || time.Now().After(p.tokenExpiry)
	p.mu.Unlock()

	if expired {
		if err := p.authenticate(); err != nil {
			return "", err
		}
	}
	return p.accessToken, nil
}

// ── Polling ──

func (p *Poller) Start(interval time.Duration) {
	log.Printf("📋 ACLED Poller starting (interval: %s, OAuth auth)", interval)
	p.poll()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		p.poll()
	}
}

func (p *Poller) poll() {
	log.Println("🔄 Polling ACLED for latest events...")

	total := 0
	for _, country := range p.countries {
		events, err := p.fetchCountry(country)
		if err != nil {
			log.Printf("⚠️ ACLED fetch failed for %s: %v", country, err)
			continue
		}

		published := 0
		for _, e := range events {
			if p.seen[e.SourceEventID] {
				continue
			}
			if err := p.producer.Publish(e.SourceEventID, e); err != nil {
				log.Printf("❌ Failed to publish ACLED event: %v", err)
				continue
			}
			p.seen[e.SourceEventID] = true
			published++
		}
		total += published
	}

	p.producer.Flush(5000)
	log.Printf("✅ ACLED: Published %d new events", total)

	if len(p.seen) > 50000 {
		p.seen = make(map[string]bool)
	}
}

func (p *Poller) fetchCountry(country string) ([]models.RawEvent, error) {
	token, err := p.getToken()
	if err != nil {
		return nil, fmt.Errorf("auth failed: %w", err)
	}

	since := time.Now().AddDate(0, 0, -7).Format("2006-01-02")

	params := url.Values{}
	params.Set("_format", "json")
	params.Set("country", country)
	params.Set("event_date", since)
	params.Set("event_date_where", ">=")
	params.Set("limit", "500")

	reqURL := fmt.Sprintf("%s?%s", acledDataURL, params.Encode())
	req, _ := http.NewRequest("GET", reqURL, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 401 {
		// Token expired, force re-auth and retry
		p.mu.Lock()
		p.accessToken = ""
		p.mu.Unlock()
		return nil, fmt.Errorf("auth expired, will retry next cycle")
	}

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ACLED returned %d: %s", resp.StatusCode, string(body))
	}

	var acledResp ACLEDResponse
	if err := json.NewDecoder(resp.Body).Decode(&acledResp); err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	var events []models.RawEvent
	for _, ae := range acledResp.Data {
		events = append(events, p.toRawEvent(ae))
	}

	log.Printf("   📋 ACLED %s: %d events", country, len(events))
	return events, nil
}

func (p *Poller) toRawEvent(ae ACLEDEvent) models.RawEvent {
	lat, _ := strconv.ParseFloat(ae.Latitude, 64)
	lng, _ := strconv.ParseFloat(ae.Longitude, 64)

	eventTime, err := time.Parse("2006-01-02", ae.EventDate)
	if err != nil {
		eventTime = time.Now()
	}

	return models.RawEvent{
		ID:            uuid.New().String(),
		Source:        "acled",
		SourceEventID: ae.EventIDCnty,
		SourceURL:     "",
		SourceName:    fmt.Sprintf("ACLED (%s)", ae.Source),
		SourceTier:    "tier1",

		Title:   fmt.Sprintf("[ACLED] %s: %s in %s, %s", ae.EventType, ae.SubEventType, ae.Location, ae.Country),
		RawText: ae.Notes,

		EventType:   mapACLEDCategory(ae.EventType),
		LocationRaw: fmt.Sprintf("%s, %s, %s", ae.Location, ae.Admin1, ae.Country),
		Country:     ae.Country,
		Lat:         lat,
		Lng:         lng,

		Actor1: ae.Actor1,
		Actor2: ae.Actor2,

		EventTime:  eventTime,
		IngestedAt: time.Now().UTC(),
	}
}

func mapACLEDCategory(eventType string) string {
	et := strings.ToLower(eventType)
	switch {
	case strings.Contains(et, "battle"):
		return "battle"
	case strings.Contains(et, "explosion") || strings.Contains(et, "remote violence"):
		return "explosion_remote_violence"
	case strings.Contains(et, "violence against civilians"):
		return "violence_against_civilians"
	case strings.Contains(et, "protest"):
		return "protest"
	case strings.Contains(et, "riot"):
		return "riot"
	case strings.Contains(et, "strategic"):
		return "strategic_development"
	default:
		return "strategic_development"
	}
}
