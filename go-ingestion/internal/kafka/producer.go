package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	kgo "github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kgo.Writer
	topic  string
}

func NewProducer(broker, topic string) (*Producer, error) {
	w := &kgo.Writer{
		Addr:         kgo.TCP(broker),
		Topic:        topic,
		Balancer:     &kgo.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kgo.RequireAll,
		Compression:  kgo.Snappy,
	}

	log.Printf("📤 Kafka producer created for topic: %s @ %s", topic, broker)
	return &Producer{writer: w, topic: topic}, nil
}

func (p *Producer) Publish(key string, event interface{}) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = p.writer.WriteMessages(ctx, kgo.Message{
		Key:   []byte(key),
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	log.Printf("✅ Published to %s: %s", p.topic, key)
	return nil
}

func (p *Producer) Flush(timeoutMs int) {
	// segmentio/kafka-go flushes on write, no explicit flush needed
}

func (p *Producer) Close() {
	if err := p.writer.Close(); err != nil {
		log.Printf("❌ Error closing producer: %v", err)
	}
}
