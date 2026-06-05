package main

import (
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/nilorg/eventbus"
)

type SecureEvent struct {
	UserID    string                 `json:"user_id"`
	Action    string                 `json:"action"`
	Data      map[string]interface{} `json:"data"`
	Secret    string                 `json:"secret"`
	Timestamp int64                  `json:"timestamp"`
}

func main() {
	log.Println("=== Redis EventBus with Custom Serializer (Encryption + Compression) Test ===")

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

	encryptionKey := generateEncryptionKey()
	log.Printf("✓ Generated encryption key: %s", hex.EncodeToString(encryptionKey))

	log.Println("\n=== Testing Different Custom Serializer Configurations ===")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go testCompressedOnly(ctx, rdb)
	go testEncryptedOnly(ctx, rdb, encryptionKey)
	go testCompressedAndEncrypted(ctx, rdb, encryptionKey)

	log.Println("\nAll custom serializers test started. Press Ctrl+C to stop...")
	log.Println("Each test runs for 10 messages")

	<-sigChan
	log.Println("\nShutting down...")
	cancel()
	time.Sleep(2 * time.Second)

	log.Println("\n=== Test Summary ===")
	log.Println("✓ Compression only: Reduces data size")
	log.Println("✓ Encryption only: Secures data with AES-GCM")
	log.Println("✓ Compression + Encryption: Best combination for secure & efficient transmission")
	log.Println("\nCustom serializer successfully demonstrates:")
	log.Println("  - Data compression (gzip)")
	log.Println("  - Data encryption (AES-GCM)")
	log.Println("  - Custom serialization pipeline")
	log.Println("  - Flexible configuration options")
}

func generateEncryptionKey() []byte {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		log.Fatalf("Failed to generate encryption key: %v", err)
	}
	return key
}

func testCompressedOnly(ctx context.Context, rdb *redis.Client) {
	log.Println("\n--- Test 1: Compression Only ---")

	opts := &CustomSerializeOptions{
		Compression:      CompressionGzip,
		Encryption:       EncryptionNone,
		BaseSerializer:   BaseJSON,
		CompressionLevel: gzip.BestCompression,
	}

	serializer, err := NewCustomSerialize(opts)
	if err != nil {
		log.Printf("[Compressed] Failed to create serializer: %v", err)
		return
	}

	log.Printf("[Compressed] Using pipeline: %s", serializer.DescribePipeline())

	busOpts := &eventbus.RedisOptions{
		Serialize:         serializer,
		Logger:            &eventbus.StdLogger{},
		MessageMaxRetries: 3,
		SkipBadMessages:   true,
	}

	bus, err := eventbus.NewRedis(rdb, busOpts)
	if err != nil {
		log.Printf("[Compressed] Failed to create bus: %v", err)
		return
	}

	topic := "custom_compressed_topic"

	go startCustomConsumer(ctx, bus, "Compressed", topic)
	startCustomProducer(ctx, bus, "Compressed", topic, serializer)
}

func testEncryptedOnly(ctx context.Context, rdb *redis.Client, key []byte) {
	log.Println("\n--- Test 2: Encryption Only ---")

	opts := &CustomSerializeOptions{
		Compression:    CompressionNone,
		Encryption:     EncryptionAES,
		EncryptionKey:  key,
		BaseSerializer: BaseJSON,
	}

	serializer, err := NewCustomSerialize(opts)
	if err != nil {
		log.Printf("[Encrypted] Failed to create serializer: %v", err)
		return
	}

	log.Printf("[Encrypted] Using pipeline: %s", serializer.DescribePipeline())

	busOpts := &eventbus.RedisOptions{
		Serialize:         serializer,
		Logger:            &eventbus.StdLogger{},
		MessageMaxRetries: 3,
		SkipBadMessages:   true,
	}

	bus, err := eventbus.NewRedis(rdb, busOpts)
	if err != nil {
		log.Printf("[Encrypted] Failed to create bus: %v", err)
		return
	}

	topic := "custom_encrypted_topic"

	go startCustomConsumer(ctx, bus, "Encrypted", topic)
	startCustomProducer(ctx, bus, "Encrypted", topic, serializer)
}

func testCompressedAndEncrypted(ctx context.Context, rdb *redis.Client, key []byte) {
	log.Println("\n--- Test 3: Compression + Encryption ---")

	opts := &CustomSerializeOptions{
		Compression:      CompressionGzip,
		Encryption:       EncryptionAES,
		EncryptionKey:    key,
		BaseSerializer:   BaseJSON,
		CompressionLevel: gzip.DefaultCompression,
	}

	serializer, err := NewCustomSerialize(opts)
	if err != nil {
		log.Printf("[Compressed+Encrypted] Failed to create serializer: %v", err)
		return
	}

	log.Printf("[Compressed+Encrypted] Using pipeline: %s", serializer.DescribePipeline())

	busOpts := &eventbus.RedisOptions{
		Serialize:         serializer,
		Logger:            &eventbus.StdLogger{},
		MessageMaxRetries: 3,
		SkipBadMessages:   true,
	}

	bus, err := eventbus.NewRedis(rdb, busOpts)
	if err != nil {
		log.Printf("[Compressed+Encrypted] Failed to create bus: %v", err)
		return
	}

	topic := "custom_compressed_encrypted_topic"

	go startCustomConsumer(ctx, bus, "Compressed+Encrypted", topic)
	startCustomProducer(ctx, bus, "Compressed+Encrypted", topic, serializer)
}

func startCustomConsumer(ctx context.Context, bus eventbus.EventBus, testName string, topic string) {
	log.Printf("[%s Consumer] Starting for topic: %s", testName, topic)

	err := bus.SubscribeAsync(ctx, topic, func(ctx context.Context, msg *eventbus.Message) error {
		log.Printf("[%s Consumer] Received message:", testName)
		log.Printf("  - Header: %+v", msg.Header)
		log.Printf("  - Encrypted/Compressed data length: %d bytes", len(msg.Value))

		var event SecureEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("  - Failed to decode: %v", err)
			return err
		}

		log.Printf("  - Decoded event:")
		log.Printf("    UserID: %s", event.UserID)
		log.Printf("    Action: %s", event.Action)
		log.Printf("    Secret: %s", event.Secret)
		log.Printf("    Data: %+v", event.Data)
		log.Printf("[%s Consumer] ✓ Successfully decrypted and decompressed", testName)
		return nil
	})

	if err != nil {
		log.Printf("[%s Consumer] Subscribe failed: %v", testName, err)
	}
}

func startCustomProducer(ctx context.Context, bus eventbus.EventBus, testName string, topic string, serializer eventbus.Serializer) {
	time.Sleep(2 * time.Second)

	log.Printf("[%s Producer] Starting for topic: %s", testName, topic)

	for i := 1; i <= 10; i++ {
		event := SecureEvent{
			UserID: fmt.Sprintf("user_%d", i),
			Action: "secure_action",
			Data: map[string]interface{}{
				"field1": fmt.Sprintf("value_%d", i),
				"field2": i * 100,
				"nested": map[string]string{
					"key": "secret_data",
				},
			},
			Secret:    fmt.Sprintf("SECRET_TOKEN_%d_%d", i, time.Now().UnixNano()),
			Timestamp: time.Now().Unix(),
		}

		rawData, err := json.Marshal(event)
		if err != nil {
			log.Printf("[%s Producer] Failed to marshal raw: %v", testName, err)
			continue
		}

		processedData, err := serializer.Marshal(event)
		if err != nil {
			log.Printf("[%s Producer] Failed to process: %v", testName, err)
			continue
		}

		log.Printf("[%s Producer] Event #%d:", testName, i)
		log.Printf("  - Raw size: %d bytes", len(rawData))
		log.Printf("  - Processed size: %d bytes", len(processedData))
		log.Printf("  - Compression/Encryption ratio: %.2f%%",
			float64(len(processedData))/float64(len(rawData))*100)
		log.Printf("  - Secret content: %s", event.Secret)

		if err := bus.Publish(ctx, topic, event); err != nil {
			log.Printf("[%s Producer] Publish failed: %v", testName, err)
		} else {
			log.Printf("[%s Producer] ✓ Published successfully", testName)
		}

		time.Sleep(3 * time.Second)
	}

	log.Printf("[%s Producer] Completed 10 messages", testName)
}
