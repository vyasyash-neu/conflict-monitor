package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/yashvyas/conflict-monitor/go-ingestion/internal/acled"
	"github.com/yashvyas/conflict-monitor/go-ingestion/internal/gdelt"
	"github.com/yashvyas/conflict-monitor/go-ingestion/internal/kafka"
	"github.com/yashvyas/conflict-monitor/go-ingestion/internal/models"
	"github.com/yashvyas/conflict-monitor/go-ingestion/internal/news"
	"github.com/yashvyas/conflict-monitor/go-ingestion/internal/social"
)

func main() {
	log.Println("🚀 Conflict Monitor - Go Ingestion Service Starting...")

	broker := getEnv("KAFKA_BROKER", "localhost:19092")
	gdeltInterval := getDuration("GDELT_POLL_INTERVAL", 15*time.Minute)
	rssInterval := getDuration("RSS_POLL_INTERVAL", 5*time.Minute)
	acledInterval := getDuration("ACLED_POLL_INTERVAL", 1*time.Hour)
	socialInterval := getDuration("SOCIAL_POLL_INTERVAL", 3*time.Minute)

	acledUsername := getEnv("ACLED_USERNAME", "")
	acledPassword := getEnv("ACLED_PASSWORD", "")
	twitterBearer := getEnv("TWITTER_BEARER_TOKEN", "")

	// ── Create Kafka Producers ──
	gdeltProducer, err := kafka.NewProducer(broker, "raw-gdelt-events")
	if err != nil {
		log.Fatalf("Failed to create GDELT producer: %v", err)
	}
	defer gdeltProducer.Close()

	newsProducer, err := kafka.NewProducer(broker, "raw-news-events")
	if err != nil {
		log.Fatalf("Failed to create news producer: %v", err)
	}
	defer newsProducer.Close()

	// ── Initialize Pollers ──
	filter := models.DefaultIranConflictFilter()
	gdeltPoller := gdelt.NewPoller(gdeltProducer, filter)
	rssReader := news.NewRSSReader(newsProducer, news.DefaultFeeds())

	// ── Launch goroutines ──
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		gdeltPoller.Start(gdeltInterval)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		rssReader.Start(rssInterval)
	}()

	// ── ACLED (if API key provided) ──
	if acledUsername != "" && acledPassword != "" {
		acledProducer, err := kafka.NewProducer(broker, "raw-news-events")
		if err != nil {
			log.Printf("⚠️ Failed to create ACLED producer: %v", err)
		} else {
			acledPoller := acled.NewPoller(acledProducer, acledUsername, acledPassword)
			wg.Add(1)
			go func() {
				defer wg.Done()
				acledPoller.Start(acledInterval)
			}()
			log.Printf("   📋 ACLED polling every %s (OAuth)", acledInterval)
		}
	} else {
		log.Println("   📋 ACLED: Skipped (no credentials set)")
	}

	// ── Social Media ──
	socialProducer, err := kafka.NewProducer(broker, "raw-news-events") // reuse news topic
	if err != nil {
		log.Printf("⚠️ Failed to create social producer: %v", err)
	} else {
		twitterPoller := social.NewTwitterPoller(socialProducer, social.DefaultAccounts(), twitterBearer)
		wg.Add(1)
		go func() {
			defer wg.Done()
			twitterPoller.Start(socialInterval)
		}()
		if twitterBearer != "" {
			log.Printf("   🐦 Twitter API polling every %s", socialInterval)
		} else {
			log.Printf("   🐦 Twitter via Nitter RSS every %s", socialInterval)
		}
	}

	log.Println("✅ All ingestion goroutines running")
	log.Printf("   📡 GDELT polling every %s", gdeltInterval)
	log.Printf("   📰 RSS polling every %s", rssInterval)

	// ── Graceful shutdown ──
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("🛑 Shutting down ingestion service...")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return fallback
}
