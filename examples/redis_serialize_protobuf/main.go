package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/nilorg/eventbus"
	"github.com/nilorg/eventbus/proto"
)

func main() {
	log.Println("=== Redis EventBus with Protobuf Serialization Test ===")

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	defer rdb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("✓ Connected to Redis successfully")

	opts := &eventbus.RedisOptions{
		Serialize:         &eventbus.ProtobufSerialize{},
		Logger:            &eventbus.StdLogger{},
		MessageMaxRetries: 3,
		SkipBadMessages:   true,
	}

	bus, err := eventbus.NewRedis(rdb, opts)
	if err != nil {
		log.Fatalf("Failed to create Redis event bus: %v", err)
	}
	log.Println("✓ Created Redis EventBus with ProtobufSerialize")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go startProtobufConsumer(ctx, bus)
	go startProtobufProducer(ctx, bus)

	log.Println("Test started. Press Ctrl+C to stop...")

	<-sigChan
	log.Println("\nShutting down...")
	cancel()
	time.Sleep(2 * time.Second)
	log.Println("Test completed")
}

func startProtobufConsumer(ctx context.Context, bus eventbus.EventBus) {
	topic := "protobuf_test_topic"

	log.Println("Starting Protobuf consumer for topic:", topic)

	err := bus.SubscribeAsync(ctx, topic, func(ctx context.Context, msg *eventbus.Message) error {
		log.Printf("[Consumer] Received message:")
		log.Printf("  - Header: %+v", msg.Header)
		log.Printf("  - Value bytes length: %d", len(msg.Value))

		var event proto.TestEvent
		if err := (&eventbus.ProtobufSerialize{}).Unmarshal(msg.Value, &event); err != nil {
			log.Printf("  - Failed to unmarshal event: %v", err)
			return err
		}

		log.Printf("  - Event: Message=%s, Count=%d", event.Message, event.Count)
		log.Println("  ✓ Successfully decoded protobuf event")
		return nil
	})

	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
	}
}

func startProtobufProducer(ctx context.Context, bus eventbus.EventBus) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	counter := 1

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			event := &proto.TestEvent{
				Message: fmt.Sprintf("Protobuf test message #%d", counter),
				Count:   int32(counter * 10),
			}

			log.Printf("[Producer] Publishing event #%d: Message=%s, Count=%d", 
				counter, event.Message, event.Count)

			if err := bus.Publish(ctx, "protobuf_test_topic", event); err != nil {
				log.Printf("Failed to publish: %v", err)
			} else {
				log.Println("  ✓ Published successfully")
			}

			counter++
			if counter > 10 {
				log.Println("Producer completed 10 messages, stopping...")
				return
			}
		}
	}
}