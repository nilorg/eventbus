package main

import (
	"context"
	"encoding/json"
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
	log.Println("=== Redis EventBus Serialization Comparison Test ===")

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

	serializers := map[string]eventbus.Serializer{
		"JSON":      &eventbus.JSONSerialize{},
		"Protobuf":  &eventbus.ProtobufSerialize{},
		"MessagePack": &eventbus.MessagePackSerialize{},
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("\n=== Testing Different Serializers ===")
	
	for name, serializer := range serializers {
		log.Printf("\n--- Testing %s Serializer ---", name)
		
		opts := &eventbus.RedisOptions{
			Serialize:         serializer,
			Logger:            &eventbus.StdLogger{},
			MessageMaxRetries: 3,
			SkipBadMessages:   true,
		}

		bus, err := eventbus.NewRedis(rdb, opts)
		if err != nil {
			log.Printf("Failed to create bus with %s: %v", name, err)
			continue
		}

		topic := fmt.Sprintf("comparison_test_%s", name)

		go startComparisonConsumer(ctx, bus, name, topic)
		go startComparisonProducer(ctx, bus, name, topic, serializer)
	}

	log.Println("\nAll serializers test started. Press Ctrl+C to stop...")

	<-sigChan
	log.Println("\nShutting down...")
	cancel()
	time.Sleep(2 * time.Second)

	log.Println("\n=== Summary ===")
	log.Println("Test completed successfully")
	log.Println("Check logs above for each serializer's performance")
}

func startComparisonConsumer(ctx context.Context, bus eventbus.EventBus, serializerName string, topic string) {
	log.Printf("[%s Consumer] Starting for topic: %s", serializerName, topic)

	err := bus.SubscribeAsync(ctx, topic, func(ctx context.Context, msg *eventbus.Message) error {
		log.Printf("[%s Consumer] Received:", serializerName)
		log.Printf("  - ContentType: %s", msg.Header["content_type"])
		log.Printf("  - Data length: %d bytes", len(msg.Value))
		
		if serializerName == "Protobuf" {
			var event proto.TestEvent
			if err := (&eventbus.ProtobufSerialize{}).Unmarshal(msg.Value, &event); err != nil {
				log.Printf("  - Unmarshal error: %v", err)
				return err
			}
			log.Printf("  - Decoded: Message=%s, Count=%d", event.Message, event.Count)
		} else {
			var event map[string]interface{}
			serializer := getSerializer(serializerName)
			if err := serializer.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("  - Unmarshal error: %v", err)
				return err
			}
			log.Printf("  - Decoded: %+v", event)
		}
		
		log.Printf("[%s Consumer] ✓ Success", serializerName)
		return nil
	})

	if err != nil {
		log.Printf("[%s Consumer] Subscribe failed: %v", serializerName, err)
	}
}

func startComparisonProducer(ctx context.Context, bus eventbus.EventBus, serializerName string, topic string, serializer eventbus.Serializer) {
	time.Sleep(1 * time.Second)

	log.Printf("[%s Producer] Publishing to topic: %s", serializerName, topic)

	var eventData interface{}
	if serializerName == "Protobuf" {
		eventData = &proto.TestEvent{
			Message: fmt.Sprintf("%s test event", serializerName),
			Count:   100,
		}
	} else {
		eventData = map[string]interface{}{
			"message":   fmt.Sprintf("%s test event", serializerName),
			"count":     100,
			"timestamp": time.Now().Unix(),
		}
	}

	dataBytes, err := serializer.Marshal(eventData)
	if err != nil {
		log.Printf("[%s Producer] Marshal failed: %v", serializerName, err)
		return
	}

	log.Printf("[%s Producer] Serialized size: %d bytes", serializerName, len(dataBytes))

	if err := bus.Publish(ctx, topic, eventData); err != nil {
		log.Printf("[%s Producer] Publish failed: %v", serializerName, err)
	} else {
		log.Printf("[%s Producer] ✓ Published successfully", serializerName)
	}
}

func getSerializer(name string) eventbus.Serializer {
	switch name {
	case "Protobuf":
		return &eventbus.ProtobufSerialize{}
	case "MessagePack":
		return &eventbus.MessagePackSerialize{}
	default:
		return &eventbus.JSONSerialize{}
	}
}

func compareSerializationSizes() {
	testData := map[string]interface{}{
		"user_id":   "user_123",
		"action":    "login",
		"timestamp": time.Now().Unix(),
		"metadata": map[string]interface{}{
			"ip":     "192.168.1.1",
			"device": "mobile",
			"app":    "app_v1",
		},
	}

	serializers := map[string]eventbus.Serializer{
		"JSON":      &eventbus.JSONSerialize{},
		"MessagePack": &eventbus.MessagePackSerialize{},
	}

	log.Println("\n=== Serialization Size Comparison ===")
	
	for name, serializer := range serializers {
		data, err := serializer.Marshal(testData)
		if err != nil {
			log.Printf("%s: Failed - %v", name, err)
			continue
		}
		log.Printf("%s: %d bytes", name, len(data))
		
		if name == "JSON" {
			log.Printf("  Content: %s", string(data))
		}
	}
	
	protoEvent := &proto.TestEvent{
		Message: "size comparison test",
		Count:   100,
	}
	protoData, err := (&eventbus.ProtobufSerialize{}).Marshal(protoEvent)
	if err != nil {
		log.Printf("Protobuf: Failed - %v", err)
	} else {
		log.Printf("Protobuf: %d bytes (for TestEvent struct)", len(protoData))
	}

	jsonEvent := map[string]interface{}{
		"message": "size comparison test",
		"count":   100,
	}
	jsonData, _ := json.Marshal(jsonEvent)
	log.Printf("JSON (equivalent): %d bytes", len(jsonData))
}