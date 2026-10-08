package news

import (
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mmcdole/gofeed"
	"github.com/yashvyas/conflict-monitor/go-ingestion/internal/kafka"
	"github.com/yashvyas/conflict-monitor/go-ingestion/internal/models"
)

// Feed represents an RSS/Atom news feed to monitor
type Feed struct {
	Name string
	URL  string
	Tier string // "tier1", "tier2", "tier3"
}

// DefaultFeeds returns the news sources we monitor for the US-Iran conflict
func DefaultFeeds() []Feed {
	return []Feed{
		// Tier 2: Major international outlets
		{Name: "Al Jazeera", URL: "https://www.aljazeera.com/xml/rss/all.xml", Tier: "tier2"},
		{Name: "BBC World", URL: "https://feeds.bbci.co.uk/news/world/rss.xml", Tier: "tier2"},
		{Name: "NPR World", URL: "https://feeds.npr.org/1004/rss.xml", Tier: "tier2"},

		// Tier 2: Middle East focused
		{Name: "Middle East Eye", URL: "https://www.middleeasteye.net/rss", Tier: "tier2"},
	}
}

type RSSReader struct {
	producer *kafka.Producer
	feeds    []Feed
	parser   *gofeed.Parser
	seen     map[string]bool
	keywords []string
}

func NewRSSReader(producer *kafka.Producer, feeds []Feed) *RSSReader {
	return &RSSReader{
		producer: producer,
		feeds:    feeds,
		parser:   gofeed.NewParser(),
		seen:     make(map[string]bool),
		keywords: []string{
			"iran", "tehran", "isfahan", "irgc",
			"strait of hormuz", "persian gulf",
			"khamenei", "trump", "centcom",
			"airstrike", "missile", "drone strike",
			"hezbollah", "houthi",
			"operation epic fury", "operation roaring lion",
			"true promise",
			"us military", "us strikes",
			"nuclear", "enrichment",
			"bahrain", "kuwait", "qatar",
			"fifth fleet",
		},
	}
}

func (r *RSSReader) Start(interval time.Duration) {
	log.Printf("📰 RSS Reader starting (interval: %s, feeds: %d)", interval, len(r.feeds))

	r.pollAll()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		r.pollAll()
	}
}

func (r *RSSReader) pollAll() {
	total, published := 0, 0
	for _, feed := range r.feeds {
		n, p := r.pollFeed(feed)
		total += n
		published += p
	}
	log.Printf("📰 RSS: %d articles scanned, %d conflict-relevant published", total, published)
}

func (r *RSSReader) pollFeed(feed Feed) (int, int) {
	parsed, err := r.parser.ParseURL(feed.URL)
	if err != nil {
		log.Printf("⚠️  Failed to parse %s: %v", feed.Name, err)
		return 0, 0
	}

	published := 0
	for _, item := range parsed.Items {
		// Dedup by link
		if r.seen[item.Link] {
			continue
		}

		// Check if article is conflict-relevant
		text := strings.ToLower(item.Title + " " + item.Description)
		if !r.isRelevant(text) {
			continue
		}

		event := r.toRawEvent(item, feed)
		if err := r.producer.Publish(event.ID, event); err != nil {
			log.Printf("❌ Failed to publish RSS event: %v", err)
			continue
		}

		r.seen[item.Link] = true
		published++
	}

	// Trim seen map
	if len(r.seen) > 20000 {
		r.seen = make(map[string]bool)
	}

	return len(parsed.Items), published
}

func (r *RSSReader) isRelevant(text string) bool {
	matchCount := 0
	for _, kw := range r.keywords {
		if strings.Contains(text, kw) {
			matchCount++
		}
	}
	// Require at least 2 keyword matches to reduce noise
	return matchCount >= 2
}

func (r *RSSReader) toRawEvent(item *gofeed.Item, feed Feed) models.RawEvent {
	eventTime := time.Now().UTC()
	if item.PublishedParsed != nil {
		eventTime = *item.PublishedParsed
	} else if item.UpdatedParsed != nil {
		eventTime = *item.UpdatedParsed
	}

	description := item.Description
	if item.Content != "" {
		description = item.Content
	}

	return models.RawEvent{
		ID:            uuid.New().String(),
		Source:        "rss",
		SourceEventID: item.Link,
		SourceURL:     item.Link,
		SourceName:    feed.Name,
		SourceTier:    feed.Tier,

		Title:   item.Title,
		RawText: description,

		EventTime:  eventTime,
		IngestedAt: time.Now().UTC(),
	}
}
