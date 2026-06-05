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
)

type UserEvent struct {
	UserID    string                 `msgpack:"user_id"`
	Action    string                 `msgpack:"action"`
	Timestamp int64                  `msgpack:"timestamp"`
	Metadata  map[string]interface{} `msgpack:"metadata"`
}

func main() {
	log.Println("=== Redis EventBus with MessagePack Serialization Test ===")

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
		Serialize:         &eventbus.MessagePackSerialize{},
		Logger:            &eventbus.StdLogger{},
		MessageMaxRetries: 3,
		SkipBadMessages:   true,
	}

	bus, err := eventbus.NewRedis(rdb, opts)
	if err != nil {
		log.Fatalf("Failed to create Redis event bus: %v", err)
	}
	log.Println("✓ Created Redis EventBus with MessagePackSerialize")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go startMessagePackConsumer(ctx, bus)
	go startMessagePackProducer(ctx, bus)

	log.Println("Test started. Press Ctrl+C to stop...")

	<-sigChan
	log.Println("\nShutting down...")
	cancel()
	time.Sleep(2 * time.Second)
	log.Println("Test completed")
}

func startMessagePackConsumer(ctx context.Context, bus eventbus.EventBus) {
	topic := "msgpack_test_topic"

	log.Println("Starting MessagePack consumer for topic:", topic)

	err := bus.SubscribeAsync(ctx, topic, func(ctx context.Context, msg *eventbus.Message) error {
		log.Printf("[Consumer] Received message:")
		log.Printf("  - Header: %+v", msg.Header)
		log.Printf("  - Value bytes length: %d", len(msg.Value))

		var event UserEvent
		if err := (&eventbus.MessagePackSerialize{}).Unmarshal(msg.Value, &event); err != nil {
			log.Printf("  - Failed to unmarshal event: %v", err)
			return err
		}

		log.Printf("  - Event: UserID=%s, Action=%s, Timestamp=%d", 
			event.UserID, event.Action, event.Timestamp)
		log.Printf("  - Metadata: %+v", event.Metadata)
		log.Println("  ✓ Successfully decoded msgpack event")
		return nil
	})

	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
	}
}

func startMessagePackProducer(ctx context.Context, bus eventbus.EventBus) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	counter := 1

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			event := UserEvent{
				UserID:    fmt.Sprintf("user_%d", counter),
				Action:    "login",
				Timestamp: time.Now().Unix(),
				Metadata: map[string]interface{}{
					"ip":     "192.168.1.100",
					"device": "mobile",
					"app":    fmt.Sprintf("app_v%d", counter),
				},
			}

			log.Printf("[Producer] Publishing event #%d: UserID=%s, Action=%s", 
				counter, event.UserID, event.Action)

			if err := bus.Publish(ctx, "msgpack_test_topic", event); err != nil {
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